#!/usr/bin/env python3
"""Build reference-checked NeTEx snapshots for selected providers.

Example (keeps national ZIPs unless the explicit removal option is supplied):
  python3 -u -m pipeline.ingestion.generate_provider_dataset --providers KOL AKT \
    --from-month 2026-01 --to-month 2026-09 \
    --output entur_netex_snapshots/kol_agder_2026

Each snapshot has original XML files, a compact ZIP containing the same files,
source checksums, per-file lineage, and a validation report. No XML is rewritten.
"""

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import time
import xml.etree.ElementTree as ET
import zipfile

from .download_entur_netex import download, file_checksums, make_url, month_range, parse_month, source_metadata

NETEX = "http://www.netex.org.uk/netex"
ROOT = f"{{{NETEX}}}PublicationDelivery"


def save_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(path)


def provider_for(name, providers):
    base = Path(name).name.upper()
    if not base.endswith(".XML"):
        return None
    return next((p for p in providers if base.startswith((p + "_", "_" + p + "_"))), None)


def selected_member(name, providers):
    base = Path(name).name.upper()
    return provider_for(name, providers) is not None or base == "_STOPS.XML"


def extract_snapshot(source, directory, zip_path, providers):
    if directory.exists() or zip_path.exists() or zip_path.with_suffix(".part").exists():
        raise FileExistsError(f"Snapshot output already exists; use a new --output: {directory}")
    directory.mkdir(parents=True)
    partial = zip_path.with_suffix(".part")
    lineage = []
    with zipfile.ZipFile(source) as src, zipfile.ZipFile(
            partial, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=4) as dst:
        selected = [i for i in src.infolist() if not i.is_dir() and selected_member(i.filename, providers)]
        if len({i.filename for i in selected}) != len(selected):
            raise RuntimeError("Source contains duplicate selected member names")
        for provider in providers:
            if not any(Path(i.filename).name.upper().startswith(provider + "_") for i in selected):
                raise RuntimeError(f"Source has no line XML for {provider}")
            if not any(Path(i.filename).name.upper() == f"_{provider}_SHARED_DATA.XML" for i in selected):
                raise RuntimeError(f"Source has no ordinary shared data for {provider}")
        if not any(Path(i.filename).name.upper() == "_STOPS.XML" for i in selected):
            raise RuntimeError("Source is missing the national stop document")
        for info in selected:
            relative = Path(info.filename)
            if relative.is_absolute() or ".." in relative.parts:
                raise RuntimeError(f"Unsafe ZIP member: {info.filename}")
            target = directory / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            digest = hashlib.sha256()
            written = 0
            with src.open(info) as infile, target.open("xb") as xml, dst.open(info.filename, "w") as zipped:
                for chunk in iter(lambda: infile.read(1024 * 1024), b""):
                    xml.write(chunk)
                    zipped.write(chunk)
                    digest.update(chunk)
                    written += len(chunk)
            if written != info.file_size:
                raise RuntimeError(f"Uncompressed size mismatch: {info.filename}")
            lineage.append({"member": info.filename, "xml_path": str(target),
                            "provider": provider_for(info.filename, providers),
                            "bytes": written, "source_crc32": info.CRC, "sha256": digest.hexdigest()})
    partial.replace(zip_path)
    with zipfile.ZipFile(source) as src, zipfile.ZipFile(zip_path) as dst:
        expected = sorted((i.filename, i.CRC, i.file_size) for i in src.infolist()
                          if not i.is_dir() and selected_member(i.filename, providers))
        actual = sorted((i.filename, i.CRC, i.file_size) for i in dst.infolist())
        if actual != expected:
            raise RuntimeError("Combined ZIP members do not match the national source")
        bad = dst.testzip()
        if bad is not None:
            raise RuntimeError(f"Combined ZIP CRC failure: {bad}")
    return lineage


def verify_materialized_snapshot(directory, zip_path, lineage):
    """Resume only when both stored representations still match source lineage."""
    for item in lineage:
        path = directory / item["member"]
        if not path.is_file() or path.stat().st_size != item["bytes"] or file_checksums(path)[0] != item["sha256"]:
            raise RuntimeError(f"Existing XML does not match recorded source lineage: {path}")
    expected = sorted((i["member"], i["source_crc32"], i["bytes"]) for i in lineage)
    with zipfile.ZipFile(zip_path) as archive:
        actual = sorted((i.filename, i.CRC, i.file_size) for i in archive.infolist())
        if actual != expected or archive.testzip() is not None:
            raise RuntimeError(f"Existing combined ZIP does not match source lineage: {zip_path}")


def validate_snapshot(directory, lineage, providers, snapshot_date):
    """Check full XML and provider refs without mixing definitions across dates.

    Explicit versions must match within the same XML document. Unversioned
    provider references must resolve in the snapshot. Registry-only references
    are reported separately. This is not XSD or full calendar validation.
    """
    ids = set()
    references = Counter()
    registry_refs = Counter()
    version_failures = []
    duplicate_pairs = Counter()
    duplicate_details = []
    summaries = []
    provider_counts = {p: Counter() for p in providers}
    last_progress = time.monotonic()
    for number, item in enumerate(lineage, 1):
        name = item["member"]
        owner = item["provider"]
        meta = {"member": name, "provider": owner, "profile": None,
                "publication_timestamp": None, "entity_counts": Counter(),
                "line_modes": Counter(), "composite_validity": []}
        local_pairs = set()
        local_refs = Counter()
        stack = []
        with (directory / name).open("rb") as f:
            for event, element in ET.iterparse(f, events=("start", "end")):
                tag = element.tag.rsplit("}", 1)[-1]
                if event == "start":
                    stack.append(element)
                    if len(stack) == 1:
                        if element.tag != ROOT:
                            raise RuntimeError(f"Unexpected XML root in {name}: {element.tag}")
                        meta["profile"] = element.get("version")
                    continue
                meta["entity_counts"][tag] += 1
                if owner:
                    provider_counts[owner][tag] += 1
                ident, version, ref = element.get("id"), element.get("version"), element.get("ref")
                if ident:
                    ids.add(ident)
                    if owner:
                        pair = (ident, version)
                        if pair in local_pairs:
                            duplicate_pairs[owner] += 1
                            duplicate_details.append({"provider": owner, "member": name, "tag": tag,
                                                      "id": ident, "version": version,
                                                      "order": element.get("order")})
                        local_pairs.add(pair)
                if ref is not None:
                    key = (owner, tag, ref, version)
                    if owner:
                        references[key] += 1
                        if version is not None:
                            local_refs[(tag, ref, version)] += 1
                    else:
                        registry_refs[(tag, ref)] += 1
                if tag == "PublicationTimestamp":
                    meta["publication_timestamp"] = (element.text or "").strip()
                if tag == "TransportMode" and len(stack) > 1 and stack[-2].tag.rsplit("}", 1)[-1] in ("Line", "FlexibleLine"):
                    meta["line_modes"][(element.text or "").strip()] += 1
                if tag in ("FromDate", "ToDate") and len(stack) >= 4 and stack[-2].tag.rsplit("}", 1)[-1] == "AvailabilityCondition" and stack[-4].tag.rsplit("}", 1)[-1] == "CompositeFrame":
                    meta["composite_validity"].append({tag: (element.text or "").strip()})
                stack.pop()
                if stack:
                    stack[-1].remove(element)
                element.clear()
        for (tag, ref, version), count in local_refs.items():
            if (ref, version) not in local_pairs:
                version_failures.append({"member": name, "tag": tag, "ref": ref,
                                         "version": version, "occurrences": count})
        if not meta["profile"] or not meta["publication_timestamp"]:
            raise RuntimeError(f"Missing XML profile or publication timestamp: {name}")
        publication = datetime.fromisoformat(meta["publication_timestamp"]).date()
        if owner and meta["entity_counts"].get("Line", 0) and publication > snapshot_date + timedelta(days=1):
            raise RuntimeError(f"Historical mismatch in {name}: {publication} after {snapshot_date}")
        summaries.append(meta)
        if time.monotonic() - last_progress >= 15:
            print(f"    XML scan: {number}/{len(lineage)} files", flush=True)
            last_progress = time.monotonic()
    missing = [{"provider": owner, "tag": tag, "ref": ref, "version": version, "occurrences": count}
               for (owner, tag, ref, version), count in references.items() if ref not in ids]
    registry_missing = [{"tag": tag, "ref": ref, "occurrences": count}
                        for (tag, ref), count in registry_refs.items() if ref not in ids]
    provider_reports = {}
    for provider in providers:
        files = [m for m in summaries if m["provider"] == provider]
        provider_reports[provider] = {
            "xml_files": len(files), "xml_bytes": sum(i["bytes"] for i in lineage if i["provider"] == provider),
            "entity_counts": provider_counts[provider],
            "line_modes": sum((m["line_modes"] for m in files), Counter()),
            "publication_dates": dict(Counter(m["publication_timestamp"][:10] for m in files)),
            "reference_occurrences": sum(count for (owner, _, _, _), count in references.items() if owner == provider)}
    # Raw source repeats must remain intact. They are an explicit source-quality
    # warning, not an extraction failure or permission to deduplicate source XML.
    passed = not missing and not version_failures
    status = "failed" if not passed else "passed_with_warnings" if duplicate_pairs else "passed"
    return {"status": status, "snapshot_date": snapshot_date.isoformat(),
            "scope": "XML well-formedness, root/profile presence, future ordinary-line publication dates, provider reference existence, exact same-file versioned references, and duplicate provider ID/version pairs. Not full XSD/profile/calendar semantic validation.",
            "xml_files": len(lineage), "total_xml_bytes": sum(i["bytes"] for i in lineage),
            "provider_xml_bytes": sum(i["bytes"] for i in lineage if i["provider"]),
            "unique_ids": len(ids), "providers": provider_reports,
            "provider_reference_occurrences": sum(references.values()),
            "unresolved_provider_references": missing,
            "same_file_version_failures": version_failures,
            "duplicate_provider_id_version_pairs": duplicate_pairs,
            "duplicate_provider_id_version_details": duplicate_details,
            "unresolved_registry_only_references": registry_missing, "members": summaries}


def generate(providers, dates, output, remove_national=False, resume=False):
    output = output.resolve()
    prior_sources = {}
    if resume and not output.exists():
        raise FileNotFoundError(f"No existing dataset to resume: {output}")
    if output.exists():
        if not resume:
            raise FileExistsError(f"Refusing to overwrite a dataset directory: {output}")
        manifest = json.loads((output / "manifest.json").read_text())
        prior_sources = json.loads((output / "source_metadata.json").read_text())
        if manifest["providers"] != providers or set(prior_sources) != {d.strftime("%Y%m%d") for d in dates}:
            raise RuntimeError("Resume providers/dates do not match the existing dataset")
        manifest["status"] = "in_progress"
    else:
        output.mkdir(parents=True)
        for child in ("national", "xml", "zip", "validation", "lineage"):
            (output / child).mkdir()
        manifest = {"created_at_utc": datetime.now(timezone.utc).isoformat(), "providers": providers,
                    "status": "in_progress", "snapshots": [], "cleanup": "National source ZIPs removed only after successful snapshot validation" if remove_national else "National source ZIPs retained"}
    manifest_path = output / "manifest.json"
    save_json(manifest_path, manifest)
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
    save_json(output / "source_metadata.json", identities)
    for index, day in enumerate(dates, 1):
        key = day.strftime("%Y%m%d")
        iso = day.isoformat()
        metadata = identities[key]
        url = make_url(key) + "&generation=" + str(int(metadata["generation"]))
        raw = output / "national" / f"rb_norway-aggregated-netex-{key}.zip"
        directory = output / "xml" / iso
        compact = output / "zip" / f"{'_'.join(providers)}_netex_{key}.zip"
        lineage_path = output / "lineage" / f"{iso}.json"
        row = next((r for r in manifest["snapshots"] if r["snapshot_date"] == iso), None)
        if row and row["status"] in ("validated", "validated_with_warnings"):
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
               "national_zip": str(raw), "national_removed": False, "xml_directory": str(directory), "combined_zip": str(compact)}
            manifest["snapshots"].append(row)
        save_json(manifest_path, manifest)
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
                save_json(lineage_path, lineage)
            print(f"  Validating all {len(lineage)} XML files and provider references", flush=True)
            report = validate_snapshot(directory, lineage, providers, day)
            report_path = output / "validation" / f"{iso}.json"
            save_json(report_path, report)
            if report["status"] not in ("passed", "passed_with_warnings"):
                raise RuntimeError(f"Reference validation failed; see {report_path}")
            row.update(status="validated_with_warnings" if report["status"] == "passed_with_warnings" else "validated",
                       validation_report=str(report_path), xml_files=report["xml_files"],
                       provider_xml_bytes=report["provider_xml_bytes"], total_xml_bytes=report["total_xml_bytes"],
                       combined_zip_bytes=compact.stat().st_size, combined_zip_sha256=file_checksums(compact)[0],
                       provider_reference_occurrences=report["provider_reference_occurrences"],
                       duplicate_id_version_occurrences=sum(report["duplicate_provider_id_version_pairs"].values()))
            save_json(manifest_path, manifest)
            if remove_national:
                raw.unlink()
                row["national_removed"] = True
                save_json(manifest_path, manifest)
            print(f"  VALIDATED: {report['provider_xml_bytes'] / 1e9:.3f} GB provider XML; "
                  f"{report['provider_reference_occurrences']:,} refs; national ZIP "
                  f"{'removed' if remove_national else 'retained'}", flush=True)
            if report["duplicate_provider_id_version_pairs"]:
                print(f"  SOURCE WARNING: repeated provider ID/version pairs preserved: "
                      f"{dict(report['duplicate_provider_id_version_pairs'])}", flush=True)
        except Exception as exc:
            row.update(status="failed", error=f"{type(exc).__name__}: {exc}")
            manifest["status"] = "failed"
            save_json(manifest_path, manifest)
            raise
    manifest.update(status="complete_with_warnings" if any(r["status"] == "validated_with_warnings" for r in manifest["snapshots"]) else "complete",
                    completed_at_utc=datetime.now(timezone.utc).isoformat(),
                    provider_xml_bytes=sum(r["provider_xml_bytes"] for r in manifest["snapshots"]),
                    total_xml_bytes=sum(r["total_xml_bytes"] for r in manifest["snapshots"]),
                    combined_zip_bytes=sum(r["combined_zip_bytes"] for r in manifest["snapshots"]))
    save_json(manifest_path, manifest)
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
    parser.add_argument("--resume", action="store_true", help="Resume identical providers/dates after verifying existing output checksums")
    args = parser.parse_args()
    providers = list(dict.fromkeys(p.upper() for p in args.providers))
    if any(not re.fullmatch(r"[A-Z]{3}", p) for p in providers):
        parser.error("Provider codes must have exactly three letters")
    dates = month_range(parse_month(args.from_month), parse_month(args.to_month), args.day)
    generate(providers, dates, args.output, args.remove_national_after_validation, args.resume)


if __name__ == "__main__":
    main()
