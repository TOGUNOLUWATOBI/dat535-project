#!/usr/bin/env python3
"""Check/download Entur's date-stamped national NeTEx archives, and optionally
produce Kolumbus-focused ZIPs.

Examples:
  python3 -m pipeline.ingestion.download_entur_netex --check
  python3 -m pipeline.ingestion.download_entur_netex --download
  python3 -m pipeline.ingestion.download_entur_netex --download --extract-kolumbus
  python3 -m pipeline.ingestion.download_entur_netex --year 2025 --dates 20250728 --download --extract-kolumbus
  python3 -m pipeline.ingestion.download_entur_netex --local-zip /path/to/archive.zip
  python3 -m pipeline.ingestion.download_entur_netex --from-month 2025-06 --to-month 2026-09 --check
  python3 -m pipeline.ingestion.download_entur_netex --from-month 2025-06 --to-month 2026-09 --download --extract-kolumbus --remove-national-after-extract

Default selection is the 15th of each month, January–September 2026.
These are snapshots, NOT exhaustive records of every historical journey.
HTTP availability does not prove historical coverage: duplicate source checksums
are rejected within a batch, and ordinary KOL publication dates are checked
before extraction. Every run writes its own manifest under OUTPUT/manifests/.
Use a new --output directory to regenerate while preserving existing data.
"""

import argparse
import base64
import csv
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys
import tempfile
import urllib.error
import urllib.request
import zipfile

BASE_URL = (
    "https://storage.googleapis.com/download/storage/v1/b/sarpanit-production/o/"
    "netex%2Frb_norway-aggregated-netex-{date}.zip?alt=media"
)
METADATA_URL = (
    "https://storage.googleapis.com/storage/v1/b/sarpanit-production/o/"
    "netex%2Frb_norway-aggregated-netex-{date}.zip"
)
USER_AGENT = "dat535-kolumbus-research/1.0"


def make_url(date_string: str) -> str:
    return BASE_URL.format(date=date_string)


def headers():
    return {"User-Agent": USER_AGENT}


def source_metadata(date_string: str):
    """Read immutable source identity before treating a filename as history."""
    request = urllib.request.Request(METADATA_URL.format(date=date_string), headers=headers())
    with urllib.request.urlopen(request, timeout=35) as response:
        metadata = json.load(response)
    expected = f"netex/rb_norway-aggregated-netex-{date_string}.zip"
    if metadata.get("name") != expected or not metadata.get("generation"):
        raise RuntimeError(f"Unexpected source metadata for {expected}")
    if not metadata.get("md5Hash") or not str(metadata.get("size", "")).isdigit():
        raise RuntimeError(f"Missing source checksum or size for {expected}")
    return metadata


def file_checksums(path: Path):
    sha256 = hashlib.sha256()
    md5 = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha256.update(chunk)
            md5.update(chunk)
    return sha256.hexdigest(), base64.b64encode(md5.digest()).decode("ascii")


def validate_national_zip(path: Path, metadata=None):
    sha256, md5 = file_checksums(path)
    if metadata and (path.stat().st_size != int(metadata["size"]) or md5 != metadata["md5Hash"]):
        raise RuntimeError(f"Source size/checksum mismatch: {path}; preserve it and use a new --output")
    with zipfile.ZipFile(path) as archive:
        bad = archive.testzip()
        if bad is not None:
            raise RuntimeError(f"CRC check failed on {bad} in {path}")
    return sha256


def inspect_publication_dates(path: Path, snapshot_date: date):
    """Reject ordinary KOL XML published after the claimed historical day.

    Older publications can legitimately persist, particularly flexible services.
    This guard detects future content; it is not full NeTEx semantic validation.
    A one-day allowance accommodates export/time-zone boundaries.
    """
    import xml.etree.ElementTree as ET

    dates = set()
    ordinary = 0
    with zipfile.ZipFile(path) as archive:
        for name in archive.namelist():
            filename = Path(name).name.upper()
            if not (filename.startswith("KOL_") and filename.endswith(".XML")):
                continue
            with archive.open(name) as f:
                publication = None
                for _, element in ET.iterparse(f):
                    if element.tag == "{http://www.netex.org.uk/netex}PublicationTimestamp":
                        publication = datetime.fromisoformat((element.text or "").strip()).date()
                        break
                    element.clear()
            if publication is None:
                raise RuntimeError(f"Missing PublicationTimestamp in {path}: {name}")
            dates.add(publication.isoformat())
            if "FLEXIBLELINE" not in filename:
                ordinary += 1
                if publication > snapshot_date + timedelta(days=1):
                    raise RuntimeError(
                        f"Historical date mismatch: requested {snapshot_date}, but {name} "
                        f"was published {publication}. Raw archive preserved: {path}"
                    )
    if not ordinary:
        raise RuntimeError(f"No ordinary KOL line XML available to verify historical date: {path}")
    return sorted(dates)


def probe(url: str):
    """Probe one archive without downloading it. Returns status, size, explanation."""
    req = urllib.request.Request(url, headers=headers(), method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=35) as resp:
            return resp.status, resp.headers.get("Content-Length", ""), ""
    except urllib.error.HTTPError as e:
        return e.code, "", str(e.reason)
    except Exception as e:
        return "ERROR", "", f"{type(e).__name__}: {e}"


def download(url: str, path: Path, metadata=None):
    """Retain raw data, checking identity and CRCs before using it."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        checksum = validate_national_zip(path, metadata)
        print(f"  Verified existing national ZIP: {path}")
        return checksum
    partial = path.with_suffix(".part")
    if partial.exists():
        raise FileExistsError(f"Partial download already exists; preserve it and use a new --output: {partial}")
    try:
        request = urllib.request.Request(url, headers=headers())
        with urllib.request.urlopen(request, timeout=100) as response, partial.open("wb") as f:
            if response.status != 200:
                raise RuntimeError(f"HTTP status: {response.status}")
            shutil.copyfileobj(response, f, length=1024 * 1024)
        if not zipfile.is_zipfile(partial):
            raise RuntimeError("Downloaded content isn't a valid ZIP (possibly an error page).")
        # Publish complete downloaded bytes before semantic validation so a failed
        # date/CRC/source check leaves the evidence available for investigation.
        partial.replace(path)
        checksum = validate_national_zip(path, metadata)
        print(f"  Saved {path} ({path.stat().st_size / (1024**2):.1f} MiB)")
        return checksum
    except Exception:
        # Preserve failed/interrupted downloads rather than silently deleting them.
        raise


def is_kolumbus_member(member: str) -> bool:
    """Keep provider KOL XML and shared national stops/reference XML.

    This file-level filter is a practical first-stage extraction, NOT a
    complete XML-level, relationship-preserving NeTEx transformation.
    """
    filename = Path(member).name
    upper = filename.upper()
    if not upper.endswith(".XML"):
        return False
    if upper.startswith("KOL_"):
        return True
    # National stop registry / shared stop files can be referenced by KOL lines.
    if upper.endswith("_STOPS.XML"):
        return True
    if upper.startswith("_") and upper.endswith("_SHARED_DATA.XML"):
        return True
    return False


def extract_kolumbus(source: Path, target: Path):
    if source.resolve() == target.resolve():
        raise ValueError("Source and destination must be different")
    if target.exists():
        raise FileExistsError(f"Refusing to overwrite extracted data: {target}; use a new --output")
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(".part")
    if partial.exists():
        raise FileExistsError(f"Partial extraction already exists: {partial}; use a new --output")
    count_kol = count_shared = 0
    try:
        with zipfile.ZipFile(source) as src, zipfile.ZipFile(
            partial, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=4
        ) as dst:
            for info in src.infolist():
                if info.is_dir() or not is_kolumbus_member(info.filename):
                    continue
                # Stream individual XML members, avoiding loading a large file into RAM.
                with src.open(info) as infile, dst.open(info.filename, "w") as outfile:
                    shutil.copyfileobj(infile, outfile, length=1024 * 1024)
                if Path(info.filename).name.upper().startswith("KOL_"):
                    count_kol += 1
                else:
                    count_shared += 1
        if not count_kol:
            raise RuntimeError(
                "No KOL_*.xml files were found. Inspect the archive's filenames "
                "before deciding on a filter."
            )
        partial.replace(target)
        print(f"  Kolumbus archive: {target} ({count_kol} KOL XML + {count_shared} shared XML)")
        return count_kol, count_shared
    except Exception:
        # Keep partial output for inspection/recovery; never replace the source.
        raise


def validate_extraction_matches_source(source: Path, target: Path):
    """Prove an existing extraction belongs to this source before reusing it."""
    with zipfile.ZipFile(source) as src, zipfile.ZipFile(target) as dst:
        expected = [(i.filename, i.CRC, i.file_size) for i in src.infolist()
                    if not i.is_dir() and is_kolumbus_member(i.filename)]
        actual = [(i.filename, i.CRC, i.file_size) for i in dst.infolist() if not i.is_dir()]
        if len({i[0] for i in expected}) != len(expected) or len({i[0] for i in actual}) != len(actual):
            raise RuntimeError("Duplicate ZIP member names prevent unambiguous extraction validation")
        if sorted(expected) != sorted(actual):
            raise RuntimeError(f"Existing extraction does not match source: {target}; use a new --output")
    return validate_kolumbus_zip(target)


def month_range(start: date, end: date, day: int):
    """Yield dates with the given day for all inclusive year/month pairs."""
    first_index = start.year * 12 + start.month - 1
    last_index = end.year * 12 + end.month - 1
    if first_index > last_index:
        raise ValueError("--from-month must be on or before --to-month")
    if last_index - first_index > 59:
        raise ValueError("Selection exceeds 60 monthly snapshots; narrow the range")
    return [date(index // 12, index % 12 + 1, day)
            for index in range(first_index, last_index + 1)]


def parse_month(text: str) -> date:
    if len(text) != 7 or text[4] != "-":
        raise ValueError("Expected YYYY-MM")
    result = date.fromisoformat(text + "-01")
    return result


def validate_kolumbus_zip(path: Path):
    """Check the selected archive is readable, contains KOL XML and has valid CRCs."""
    with zipfile.ZipFile(path) as z:
        names = z.namelist()
        count_kol = sum(Path(n).name.upper().startswith("KOL_") and
                        n.upper().endswith(".XML") for n in names)
        count_shared = sum(is_kolumbus_member(n) and
                           not Path(n).name.upper().startswith("KOL_") for n in names)
        if count_kol == 0:
            raise RuntimeError(f"No Kolumbus XML files in {path}")
        bad = z.testzip()
        if bad is not None:
            raise RuntimeError(f"CRC check failed on {bad} in {path}")
    return count_kol, count_shared


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--year", type=int, default=2026)
    p.add_argument("--from-month", metavar="YYYY-MM", help="First month (inclusive); use with --to-month")
    p.add_argument("--to-month", metavar="YYYY-MM", help="Final month (inclusive); use with --from-month")
    p.add_argument("--start-month", type=int, default=1)
    p.add_argument("--end-month", type=int, default=9)
    p.add_argument("--day", type=int, default=15, help="Day of month for each monthly snapshot (default: 15)")
    p.add_argument("--dates", nargs="+", metavar="YYYYMMDD", help="Explicit snapshot dates; overrides month range")
    p.add_argument("--output", default="entur_netex_snapshots", help="Output directory")
    p.add_argument("--check", action="store_true", help="Probe for archive existence without downloading")
    p.add_argument("--download", action="store_true", help="Download the selected snapshots")
    p.add_argument("--extract-kolumbus", action="store_true", help="Create Kolumbus-focused ZIPs after download")
    p.add_argument("--remove-national-after-extract", action="store_true",
                   help="Delete each national ZIP only after the Kolumbus ZIP passes CRC validation")
    p.add_argument("--local-zip", type=Path, help="Extract a previously downloaded national NeTEx ZIP")
    args = p.parse_args()
    if args.remove_national_after_extract and not (args.download and args.extract_kolumbus):
        p.error("--remove-national-after-extract requires --download --extract-kolumbus")
    if bool(args.from_month) != bool(args.to_month):
        p.error("Use --from-month and --to-month together")
    if args.dates and args.from_month:
        p.error("Choose either --dates or --from-month/--to-month")

    out = Path(args.output)
    if args.local_zip:
        if not args.local_zip.is_file():
            p.error(f"No file: {args.local_zip}")
        if not zipfile.is_zipfile(args.local_zip):
            p.error("--local-zip must point to an actual ZIP")
        destination = out / "kolumbus" / ("KOL_" + args.local_zip.name)
        validate_national_zip(args.local_zip)
        match = re.search(r"-(\d{8})\.zip$", args.local_zip.name)
        if match:
            inspect_publication_dates(args.local_zip, datetime.strptime(match[1], "%Y%m%d").date())
        extract_kolumbus(args.local_zip, destination)
        validate_extraction_matches_source(args.local_zip, destination)
        return 0

    if args.dates:
        try:
            selected_dates = [date.fromisoformat(f"{x[:4]}-{x[4:6]}-{x[6:8]}") for x in args.dates]
            if any(len(x) != 8 or not x.isdigit() for x in args.dates):
                raise ValueError("invalid YYYYMMDD")
        except ValueError as e:
            p.error(f"--dates values must be YYYYMMDD: {e}")
    elif args.from_month:
        try:
            selected_dates = month_range(parse_month(args.from_month), parse_month(args.to_month), args.day)
        except ValueError as e:
            p.error(f"Invalid cross-year range: {e}")
    else:
        if not (1 <= args.start_month <= args.end_month <= 12):
            p.error("Months must be between 1 and 12 (start <= end)")
        try:
            selected_dates = [date(args.year, m, args.day) for m in range(args.start_month, args.end_month + 1)]
        except ValueError as e:
            p.error(f"Invalid monthly date: {e}")

    # Check-only is the safe default (no surprise multi-gigabyte downloads).
    check_only = args.check or not args.download
    out.mkdir(parents=True, exist_ok=True)
    # Each run owns a separate manifest; even --check must preserve earlier history.
    manifests = out / "manifests"
    manifests.mkdir(parents=True, exist_ok=True)
    run_time = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    with tempfile.NamedTemporaryFile(prefix=f"netex_manifest_{run_time}_", suffix=".csv",
                                     dir=manifests, delete=False) as f:
        manifest_path = Path(f.name)
    fields = ["snapshot_date", "http_status", "content_length_bytes", "url", "raw_zip", "kolumbus_zip", "kol_xml_count", "shared_xml_count", "note",
              "checked_at_utc", "source_generation", "source_md5_base64", "source_created_at",
              "raw_sha256", "kolumbus_sha256", "publication_dates", "validation_status"]
    rows = []
    metadata_by_date = {}
    metadata_errors = {}
    checksum_dates = {}
    # Compare source objects before downloading, so an alias/backfill mistake does
    # not trigger many copies of the same multi-hundred-megabyte file.
    for d in selected_dates:
        key = d.strftime("%Y%m%d")
        try:
            metadata = source_metadata(key)
            metadata_by_date[key] = metadata
            checksum_dates.setdefault(metadata["md5Hash"], set()).add(d.isoformat())
        except Exception as e:
            metadata_errors[key] = f"Source identity check failed: {type(e).__name__}: {e}"
    for d in selected_dates:
        key = d.strftime("%Y%m%d")
        url = make_url(key)
        raw = out / "national" / f"rb_norway-aggregated-netex-{key}.zip"
        kol = out / "kolumbus" / f"KOL_rb_norway-aggregated-netex-{key}.zip"
        print(f"{d.isoformat()}:", flush=True)
        status, size, note = probe(url)
        row = dict(snapshot_date=d.isoformat(), http_status=status, content_length_bytes=size, url=url,
                   raw_zip=str(raw) if raw.exists() else "", kolumbus_zip="", kol_xml_count="", shared_xml_count="", note=note,
                   checked_at_utc=datetime.now(timezone.utc).isoformat(), validation_status="metadata_only")
        print(f"  HTTP {status}" + (f" | {int(size)/(1024**2):.1f} MiB" if size.isdigit() else "") + (f" | {note}" if note else ""))
        metadata = metadata_by_date.get(key)
        if metadata:
            row.update(source_generation=metadata["generation"], source_md5_base64=metadata["md5Hash"],
                       source_created_at=metadata.get("timeCreated", ""))
            # Bind the media request to the exact source generation just inspected.
            row["url"] = url + "&generation=" + str(int(metadata["generation"]))
            duplicate_dates = sorted(checksum_dates[metadata["md5Hash"]])
            if len(duplicate_dates) > 1:
                row["validation_status"] = "rejected_duplicate_source"
                row["note"] = "Identical source checksum across requested dates: " + ", ".join(duplicate_dates)
                print(f"  REJECTED: {row['note']}")
            created = metadata.get("timeCreated", "")
            if created and datetime.fromisoformat(created).date() > d + timedelta(days=1):
                print(f"  WARNING: source object created {created}, after requested day; XML date must be checked")
        else:
            row["validation_status"] = "source_identity_failed"
            row["note"] = metadata_errors[key]
            print(f"  ERROR: {row['note']}")
        if status == 200 and not check_only and row["validation_status"] == "metadata_only":
            try:
                row["raw_sha256"] = download(row["url"], raw, metadata)
                row["raw_zip"] = str(raw)
                row["publication_dates"] = ";".join(inspect_publication_dates(raw, d))
                if args.extract_kolumbus:
                    if not kol.exists():
                        extract_kolumbus(raw, kol)
                    counts = validate_extraction_matches_source(raw, kol)
                    row["kol_xml_count"], row["shared_xml_count"] = counts
                    row["kolumbus_zip"] = str(kol)
                    row["kolumbus_sha256"] = file_checksums(kol)[0]
                    print(f"  Validated Kolumbus ZIP ({counts[0]} KOL XML files; CRC and source member match OK)")
                    if args.remove_national_after_extract and raw.exists():
                        raw.unlink()
                        print("  Deleted national ZIP after source, date and extraction validation")
                        row["note"] = "National ZIP removed after source, date and extraction validation"
                row["raw_zip"] = str(raw) if raw.exists() else ""
                row["validation_status"] = "passed_date_and_source_checks"
            except Exception as e:
                row["raw_zip"] = str(raw) if raw.exists() else ""
                row["validation_status"] = "failed"
                row["note"] = f"Download/extraction failed: {type(e).__name__}: {e}"
                print(f"  ERROR: {row['note']}")
        rows.append(row)
        # Continuously persist manifest so interrupted runs are auditable.
        with manifest_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
    print(f"Manifest: {manifest_path}")
    print("Note: each ZIP is a timetable snapshot; monthly sampling is NOT complete journey history.")
    return 0 if all(r["http_status"] == 200 and r["validation_status"] in
                    ("metadata_only", "passed_date_and_source_checks") for r in rows) else 1


if __name__ == "__main__":
    sys.exit(main())
