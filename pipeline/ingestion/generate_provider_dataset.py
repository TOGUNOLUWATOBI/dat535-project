#!/usr/bin/env python3
"""Build reference-checked NeTEx snapshots for selected providers.

Example (keeps national ZIPs unless the explicit removal option is supplied):
  python3 -u -m pipeline.ingestion.generate_provider_dataset --providers KOL AKT \
    --from-month 2026-01 --to-month 2026-09 \
    --output entur_netex_snapshots/kol_agder_2026

Each snapshot has original XML, source checksums, lineage and a validation report.
--zip-only retains the selected ZIP and materializes XML only while processing.
--xml-only keeps extracted XML without a compact ZIP. XML bytes are never rewritten.
"""

import argparse
import json
from pathlib import Path
import re
import zipfile

from pipeline.common import sha256_file, utc_now, write_json
from .netex import (download, extract_snapshot, make_url, month_range, parse_month,
                    selected_member, source_metadata, validate_snapshot,
                    verify_materialized_snapshot)


def generate(providers, dates, output, remove_national=False, resume=False, xml_only=False, zip_only=False):
    """Download, extract and validate one snapshot per date into a new dataset directory.

    The dataset manifest is saved after every step, so an interrupted run can be
    resumed with ``resume=True``; that re-verifies earlier output and reuses the
    exact source generations recorded on the first run.
    """
    if xml_only and zip_only:
        raise ValueError("Choose XML-only or ZIP-only retention, not both")
    output = output.resolve()
    prior_sources = {}
    if resume and not output.exists():
        raise FileNotFoundError(f"No existing dataset to resume: {output}")
    if output.exists():
        if not resume:
            raise FileExistsError(f"Refusing to overwrite a dataset directory: {output}")
        manifest = json.loads((output / "manifest.json").read_text())
        xml_only = manifest.get("xml_only", False)
        zip_only = manifest.get("zip_only", zip_only)
        prior_sources = json.loads((output / "source_metadata.json").read_text())
        if manifest["providers"] != providers or set(prior_sources) != {d.strftime("%Y%m%d") for d in dates}:
            raise RuntimeError("Resume providers/dates do not match the existing dataset")
        manifest["status"] = "in_progress"
    else:
        output.mkdir(parents=True)
        for child in ("national", "xml", "zip", "validation", "lineage"):
            (output / child).mkdir()
        manifest = {"created_at_utc": utc_now(), "providers": providers, "xml_only": xml_only, "zip_only": zip_only,
                    "status": "in_progress", "snapshots": [], "cleanup": "National source ZIPs removed only after successful snapshot validation" if remove_national else "National source ZIPs retained"}
    manifest_path = output / "manifest.json"
    write_json(manifest_path, manifest)
    identities = {}
    for day in dates:
        key = day.strftime("%Y%m%d")
        print(f"Source identity: {day}", flush=True)
        # A resumed run reproduces the exact previously pinned generations, not
        # whatever generation happens to be current at the time of the retry.
        identities[key] = prior_sources[key] if resume else source_metadata(key)
    hashes = [m["md5Hash"] for m in identities.values()]
    if len(hashes) != len(set(hashes)):
        raise RuntimeError("Repeated national source payloads across requested dates; refusing historical batch")
    write_json(output / "source_metadata.json", identities)
    for index, day in enumerate(dates, 1):
        key = day.strftime("%Y%m%d")
        iso = day.isoformat()
        metadata = identities[key]
        url = make_url(key) + "&generation=" + str(int(metadata["generation"]))
        raw = output / "national" / f"rb_norway-aggregated-netex-{key}.zip"
        directory = output / "xml" / iso
        compact = None if xml_only else output / "zip" / f"{'_'.join(providers)}_netex_{key}.zip"
        lineage_path = output / "lineage" / f"{iso}.json"
        row = next((r for r in manifest["snapshots"] if r["snapshot_date"] == iso), None)
        if row and row.get("xml_only", xml_only):
            compact = None
        if row and row["status"] in ("validated", "validated_with_warnings"):
            if row.get("raw_storage") == "zip":
                from pipeline.storage.zip_bronze import specification, snapshot_lock, verify_zip
                spec = specification(manifest_path, iso)
                with snapshot_lock(spec):
                    verify_zip(spec)
            else:
                verify_materialized_snapshot(directory, compact, json.loads(lineage_path.read_text()))
            print(f"[{index}/{len(dates)}] {iso}: existing validated snapshot verified; skipping download", flush=True)
            continue
        if row:
            row.update(status="in_progress")
            row.pop("error", None)
        else:
            row = {"snapshot_date": iso, "status": "in_progress", "source_url": url,
               "source_generation": metadata["generation"], "source_md5_base64": metadata["md5Hash"],
               "source_created_at": metadata.get("timeCreated"), "source_bytes": int(metadata["size"]),
               "national_zip": str(raw), "national_removed": False, "xml_directory": str(directory),
               "combined_zip": str(compact) if compact is not None else None, "xml_only": xml_only}
            manifest["snapshots"].append(row)
        write_json(manifest_path, manifest)
        print(f"[{index}/{len(dates)}] {iso}: downloading national source", flush=True)
        try:
            row["source_sha256"] = download(url, raw, metadata)
            print(f"  Extracting {', '.join(providers)} plus national stops", flush=True)
            if lineage_path.exists():
                lineage = json.loads(lineage_path.read_text())
                verify_materialized_snapshot(directory, compact, lineage)
                with zipfile.ZipFile(raw) as src:
                    expected = sorted((i.filename, i.CRC, i.file_size) for i in src.infolist()
                                      if not i.is_dir() and selected_member(i.filename, providers))
                actual = sorted((i["member"], i["source_crc32"], i["bytes"]) for i in lineage)
                if expected != actual:
                    raise RuntimeError("Stored lineage does not match verified national source")
            else:
                lineage = extract_snapshot(raw, directory, compact, providers)
                write_json(lineage_path, lineage)
            print(f"  Validating all {len(lineage)} XML files and provider references", flush=True)
            report = validate_snapshot(directory, lineage, providers, day)
            report_path = output / "validation" / f"{iso}.json"
            write_json(report_path, report)
            if report["status"] not in ("passed", "passed_with_warnings"):
                raise RuntimeError(f"Reference validation failed; see {report_path}")
            row.update(status="validated_with_warnings" if report["status"] == "passed_with_warnings" else "validated",
                       validation_report=str(report_path), xml_files=report["xml_files"],
                       provider_xml_bytes=report["provider_xml_bytes"], total_xml_bytes=report["total_xml_bytes"],
                       combined_zip_bytes=compact.stat().st_size if compact is not None else 0,
                       combined_zip_sha256=sha256_file(compact) if compact is not None else None,
                       provider_reference_occurrences=report["provider_reference_occurrences"],
                       duplicate_id_version_occurrences=sum(report["duplicate_provider_id_version_pairs"].values()))
            write_json(manifest_path, manifest)
            if zip_only:
                from pipeline.storage.zip_bronze import retain_snapshot
                retain_snapshot(manifest_path, iso, apply=True, document=manifest)
            if remove_national:
                raw.unlink()
                row["national_removed"] = True
                write_json(manifest_path, manifest)
            print(f"  VALIDATED: {report['provider_xml_bytes'] / 1e9:.3f} GB provider XML; "
                  f"{report['provider_reference_occurrences']:,} refs; national ZIP "
                  f"{'removed' if remove_national else 'retained'}", flush=True)
            if report["duplicate_provider_id_version_pairs"]:
                print(f"  SOURCE WARNING: repeated provider ID/version pairs preserved: "
                      f"{dict(report['duplicate_provider_id_version_pairs'])}", flush=True)
        except Exception as exc:
            row.update(status="failed", error=f"{type(exc).__name__}: {exc}")
            manifest["status"] = "failed"
            write_json(manifest_path, manifest)
            raise
    manifest.update(status="complete_with_warnings" if any(r["status"] == "validated_with_warnings" for r in manifest["snapshots"]) else "complete",
                    completed_at_utc=utc_now(),
                    provider_xml_bytes=sum(r["provider_xml_bytes"] for r in manifest["snapshots"]),
                    total_xml_bytes=sum(r["total_xml_bytes"] for r in manifest["snapshots"]),
                    combined_zip_bytes=sum(r["combined_zip_bytes"] for r in manifest["snapshots"]))
    write_json(manifest_path, manifest)
    print(f"COMPLETE: {manifest_path}", flush=True)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--providers", nargs="+", default=["KOL", "AKT"])
    parser.add_argument("--from-month", default="2026-01")
    parser.add_argument("--to-month", default="2026-09")
    parser.add_argument("--day", type=int, default=15)
    parser.add_argument("--output", type=Path, default=Path("entur_netex_snapshots/kol_agder_2026"))
    parser.add_argument("--remove-national-after-validation", action="store_true")
    retention = parser.add_mutually_exclusive_group()
    retention.add_argument("--xml-only", action="store_true", help="Keep validated XML without creating a compact provider ZIP")
    retention.add_argument("--zip-only", action="store_true", help="Keep a verified provider ZIP and release extracted XML")
    parser.add_argument("--resume", action="store_true", help="Resume identical providers/dates after verifying existing output checksums")
    args = parser.parse_args()
    providers = list(dict.fromkeys(p.upper() for p in args.providers))
    if any(not re.fullmatch(r"[A-Z]{3}", p) for p in providers):
        parser.error("Provider codes must have exactly three letters")
    dates = month_range(parse_month(args.from_month), parse_month(args.to_month), args.day)
    generate(providers, dates, args.output, args.remove_national_after_validation, args.resume, args.xml_only, args.zip_only)


if __name__ == "__main__":
    main()
