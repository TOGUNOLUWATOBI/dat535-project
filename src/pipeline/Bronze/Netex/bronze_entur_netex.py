# %% [markdown]
# # Bronze Ingestion: Entur NeTEx (`KOL`)
#
# Selects and downloads Kolumbus (`KOL`) NeTEx archives from Entur's public GCS export
# (`rb_kol-aggregated-netex.zip`) into `/srv/dat535/data/bronze/netex/kol/<YYYY-MM>/`
# using streamed atomic writes, source size/MD5 and ZIP validation, SHA-256 verification, and a manifest file.
# Archive contents are stored as-is; parsing and transformations happen downstream in Spark.

# %%
from __future__ import annotations

import argparse
import base64
import calendar
from datetime import date, datetime, time as dt_time, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile


# %%
# Generate weekly step dates starting on the 1st of each month, plus the final cutoff date once if needed.
def iter_monthly_step_dates(
    year: int,
    months: list[int],
    step_days: int = 7,
    cutoff_date: date | None = None,
) -> list[date]:
    if step_days < 1:
        raise ValueError("step_days must be >= 1")

    dates: list[date] = []
    for month in months:
        days_in_month = calendar.monthrange(year, month)[1]
        current = date(year, month, 1)
        month_end = date(year, month, days_in_month)

        # Skip months that start strictly after the cutoff date.
        if cutoff_date is not None and current > cutoff_date:
            continue

        effective_end = min(month_end, cutoff_date) if cutoff_date is not None else month_end
        while current <= effective_end:
            dates.append(current)
            current += timedelta(days=step_days)

    # Include the final cutoff date once if it falls within the requested year/months and wasn't already hit.
    if cutoff_date is not None and cutoff_date.year == year and cutoff_date.month in months:
        if not dates or dates[-1] != cutoff_date:
            dates.append(cutoff_date)

    return dates


# %%
# Compute SHA-256 hex digest and base64-encoded MD5 digest of a file in streaming chunks.
def compute_file_digests(path: Path) -> tuple[str, str]:
    sha256_hash = hashlib.sha256()
    md5_hash = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            sha256_hash.update(chunk)
            md5_hash.update(chunk)
    md5_b64 = base64.b64encode(md5_hash.digest()).decode("ascii")
    return sha256_hash.hexdigest(), md5_b64


# %%
# Build request headers for public Entur/GCS endpoints without bearer authentication.
def build_headers(client_name: str) -> dict[str, str]:
    return {
        "User-Agent": client_name,
        "ET-Client-Name": client_name,
    }


# %%
# Parse an ISO timestamp or date string into a timezone-aware UTC datetime for exact comparison.
def parse_timestamp_utc(raw_value: object) -> datetime | None:
    if not raw_value:
        return None
    text = str(raw_value).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        try:
            dt = datetime.combine(date.fromisoformat(text[:10]), dt_time.min)
        except ValueError:
            return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


# %%
# Query retained object versions bounded by [min_target_date, max_target_date] (keeping the latest
# baseline version at or before min_target_date so early target dates still have a valid publication).
def fetch_versions(
    codespace: str,
    headers: dict[str, str],
    min_target_date: date | None = None,
    max_target_date: date | None = None,
    versions_url_template: str = (
        "https://storage.googleapis.com/storage/v1/b/marduk-production/o"
        "?prefix=outbound%2Fnetex%2Frb_{codespace}-aggregated-netex.zip"
        "&versions=true&maxResults=1000"
        "&fields=items(name,generation,timeCreated,updated,size,md5Hash),nextPageToken"
    ),
    timeout_seconds: int = 30,
) -> tuple[int | None, list[dict[str, object]], str | None]:
    expected_name = f"outbound/netex/rb_{codespace.lower()}-aggregated-netex.zip"
    lower_bound_dt = (
        datetime.combine(min_target_date, dt_time.max, tzinfo=timezone.utc)
        if min_target_date is not None
        else None
    )
    upper_bound_dt = (
        datetime.combine(max_target_date, dt_time.max, tzinfo=timezone.utc)
        if max_target_date is not None
        else None
    )

    # Track only the single latest baseline version at or before lower_bound_dt so pre-window history isn't kept.
    best_baseline: tuple[datetime, dict[str, object]] | None = None
    in_window_items: list[tuple[datetime, dict[str, object]]] = []
    page_token: str | None = None
    last_status: int | None = None

    while True:
        base_url = versions_url_template.format(codespace=codespace.lower())
        if page_token:
            sep = "&" if "?" in base_url else "?"
            url = f"{base_url}{sep}pageToken={urllib.parse.quote(page_token)}"
        else:
            url = base_url

        try:
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                last_status = response.status
                raw = response.read().decode("utf-8")
                parsed = json.loads(raw)

                if isinstance(parsed, list):
                    raw_items = parsed
                    page_token = None
                elif isinstance(parsed, dict):
                    raw_items = parsed.get("items") or parsed.get("versions") or []
                    next_tok = parsed.get("nextPageToken")
                    page_token = str(next_tok) if next_tok else None
                else:
                    return last_status, [], "Unexpected versions payload structure"

                if not isinstance(raw_items, list):
                    return last_status, [], "Unexpected versions list type"

                for entry in raw_items:
                    if not isinstance(entry, dict):
                        continue
                    obj_name = entry.get("name")
                    if obj_name and str(obj_name) != expected_name:
                        continue

                    raw_created = (
                        entry.get("timeCreated")
                        or entry.get("updated")
                        or entry.get("creationDate")
                        or entry.get("createdAt")
                        or entry.get("publishedAt")
                        or entry.get("date")
                    )
                    pub_dt = parse_timestamp_utc(raw_created)
                    # Entries without a valid timestamp cannot satisfy cutoff bounds and are excluded.
                    if pub_dt is None:
                        continue
                    if upper_bound_dt is not None and pub_dt > upper_bound_dt:
                        continue

                    if lower_bound_dt is not None and pub_dt <= lower_bound_dt:
                        if best_baseline is None or pub_dt >= best_baseline[0]:
                            best_baseline = (pub_dt, entry)
                    else:
                        in_window_items.append((pub_dt, entry))

                if not page_token:
                    break
        except urllib.error.HTTPError as exc:
            return exc.code, [], f"HTTP {exc.code}: {exc.reason}"
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
            return None, [], str(exc)

    combined: list[tuple[datetime, dict[str, object]]] = []
    if best_baseline is not None:
        combined.append(best_baseline)
    combined.extend(in_window_items)
    combined.sort(key=lambda item: item[0])

    return last_status, [entry for _, entry in combined], None


# %%
# Resolve a requested target_date to the latest available historical publication on or before end-of-day UTC.
def resolve_publication_for_date(
    target_date: date,
    versions: list[dict[str, object]],
) -> dict[str, object] | None:
    cutoff_dt = datetime.combine(target_date, dt_time.max, tzinfo=timezone.utc)
    candidates: list[tuple[datetime, str, dict[str, object]]] = []
    for entry in versions:
        if not isinstance(entry, dict):
            continue
        generation_id = (
            entry.get("generation")
            or entry.get("importKey")
            or entry.get("import_key")
            or entry.get("id")
        )
        raw_created = (
            entry.get("timeCreated")
            or entry.get("updated")
            or entry.get("creationDate")
            or entry.get("createdAt")
            or entry.get("publishedAt")
            or entry.get("date")
        )
        pub_dt = parse_timestamp_utc(raw_created)
        if not generation_id or pub_dt is None:
            continue
        if pub_dt <= cutoff_dt:
            candidates.append((pub_dt, str(generation_id), entry))

    if not candidates:
        return None

    # Sort by full UTC timestamp so later same-day publications take precedence over earlier ones.
    candidates.sort(key=lambda item: item[0])
    pub_dt, generation_id, raw_entry = candidates[-1]
    raw_size = raw_entry.get("size")
    expected_size: int | None = None
    if raw_size is not None:
        try:
            expected_size = int(str(raw_size))
        except ValueError:
            expected_size = None
    raw_md5 = raw_entry.get("md5Hash") or raw_entry.get("md5_hash")
    expected_md5_b64 = str(raw_md5).strip() if raw_md5 else None

    return {
        "generation": generation_id,
        "publication_date": pub_dt.isoformat(),
        "expected_size": expected_size,
        "expected_md5_b64": expected_md5_b64,
        "raw_publication": raw_entry,
    }


# %%
# Stream a URL response directly into a temporary file and return HTTP status and byte count.
def stream_to_temp_file(
    url: str,
    tmp_path: Path,
    headers: dict[str, str],
    retries: int = 3,
    backoff_seconds: float = 1.5,
    timeout_seconds: int = 60,
    chunk_size: int = 1024 * 1024,
) -> tuple[int | None, int, str | None]:
    last_error: str | None = None

    for attempt in range(retries):
        bytes_written = 0
        try:
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                status = response.status
                with tmp_path.open("wb") as out_file:
                    while True:
                        chunk = response.read(chunk_size)
                        if not chunk:
                            break
                        out_file.write(chunk)
                        bytes_written += len(chunk)
                    out_file.flush()
                    os.fsync(out_file.fileno())
                return status, bytes_written, None
        except urllib.error.HTTPError as exc:
            tmp_path.unlink(missing_ok=True)
            # Non-retryable client/auth/not-found statuses return immediately.
            if exc.code in (400, 401, 403, 404, 410):
                return exc.code, 0, f"HTTP {exc.code}: {exc.reason}"
            last_error = f"HTTP {exc.code}: {exc.reason}"
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            tmp_path.unlink(missing_ok=True)
            last_error = str(exc)

        if attempt < retries - 1:
            time.sleep(backoff_seconds * (2**attempt))

    return None, 0, last_error


# %%
# Validate downloaded size, source MD5 checksum, and ZIP/XML archive integrity before publishing.
def validate_downloaded_archive(
    path: Path,
    expected_size: int | None,
    expected_md5_b64: str | None,
) -> tuple[bool, str | None, str | None, str | None]:
    if not path.exists() or path.stat().st_size == 0:
        return False, "Downloaded file is empty", None, None

    actual_size = path.stat().st_size
    if expected_size is not None and actual_size != expected_size:
        return False, f"Size mismatch: expected {expected_size} bytes, got {actual_size} bytes", None, None

    sha256_hex, actual_md5_b64 = compute_file_digests(path)
    if expected_md5_b64 is not None and actual_md5_b64 != expected_md5_b64:
        return False, f"MD5 mismatch: expected {expected_md5_b64}, got {actual_md5_b64}", None, None

    if not zipfile.is_zipfile(path):
        return False, "Payload is not a valid ZIP archive", None, None

    try:
        with zipfile.ZipFile(path, "r") as zf:
            bad_member = zf.testzip()
            if bad_member is not None:
                return False, f"Corrupt ZIP member: {bad_member}", None, None
            xml_members = [
                name for name in zf.namelist()
                if not name.endswith("/") and name.lower().endswith(".xml")
            ]
            if not xml_members:
                return False, "ZIP archive contains no .xml timetable files", None, None
    except (zipfile.BadZipFile, OSError) as exc:
        return False, f"ZIP validation error: {exc}", None, None

    return True, None, sha256_hex, actual_md5_b64


# %%
# Load existing manifest records and verify an existing downloaded file against its recorded SHA-256.
def load_manifest(manifest_path: Path) -> list[dict[str, object]]:
    if not manifest_path.exists():
        return []
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    records = data.get("records", [])
    return records if isinstance(records, list) else []


def save_manifest(
    manifest_path: Path,
    provider: str,
    records: list[dict[str, object]],
) -> None:
    payload = {
        "source": "netex",
        "provider": provider,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "records": records,
    }
    raw_bytes = json.dumps(payload, indent=2).encode("utf-8")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = manifest_path.with_name(f".{manifest_path.name}.part.{os.getpid()}")
    try:
        with tmp_path.open("wb") as handle:
            handle.write(raw_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, manifest_path)
    finally:
        tmp_path.unlink(missing_ok=True)


def find_existing_download(
    records: list[dict[str, object]],
    date_str: str,
) -> dict[str, object] | None:
    for rec in reversed(records):
        if rec.get("target_date") == date_str and rec.get("status") == "downloaded":
            file_str = rec.get("file")
            expected_sha = rec.get("sha256")
            if not isinstance(file_str, str) or not isinstance(expected_sha, str) or not expected_sha:
                continue
            file_path = Path(file_str)
            if not file_path.is_file():
                continue
            actual_sha, _ = compute_file_digests(file_path)
            if actual_sha == expected_sha:
                return rec
    return None


def upsert_record(records: list[dict[str, object]], new_record: dict[str, object]) -> None:
    date_str = str(new_record["target_date"])
    for idx, existing in enumerate(records):
        if existing.get("target_date") == date_str:
            records[idx] = new_record
            return
    records.append(new_record)


# %%
# Process target dates, selecting historical GCS generations, validating size/MD5/ZIP, and saving manifest per attempt.
def run_ingestion(
    target_dates: list[date],
    version_download_template: str,
    versions_url_template: str,
    bronze_root: Path,
    headers: dict[str, str],
    provider: str = "KOL",
    force: bool = False,
) -> tuple[Path, bool, int]:
    codespace = provider.lower()
    provider_dir = bronze_root / "netex" / codespace
    provider_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = provider_dir / "manifest.json"
    records = load_manifest(manifest_path)

    min_target_date = min(target_dates) if target_dates else None
    max_target_date = max(target_dates) if target_dates else None

    # Fetch retained versions bounded by the target date window so discovery stays scoped.
    versions_status, versions_list, versions_err = fetch_versions(
        codespace=codespace,
        headers=headers,
        min_target_date=min_target_date,
        max_target_date=max_target_date,
        versions_url_template=versions_url_template,
    )
    versions_lookup_ok = versions_status == 200 and versions_err is None

    previous_sha256: str | None = None
    for existing in records:
        if existing.get("status") == "downloaded" and existing.get("sha256"):
            previous_sha256 = str(existing["sha256"])

    any_failure = False
    downloaded_count = 0

    for target_date in target_dates:
        month_str = target_date.strftime("%Y-%m")
        date_str = target_date.isoformat()
        archive_path = provider_dir / month_str / f"{codespace}_{date_str}.zip"
        tmp_path = archive_path.with_name(f".{archive_path.name}.part.{os.getpid()}")

        # Skip re-downloading if already completed and verified on disk unless --force is set.
        existing_ok = None if force else find_existing_download(records, date_str)
        if existing_ok is not None:
            if existing_ok.get("sha256"):
                previous_sha256 = str(existing_ok["sha256"])
            downloaded_count += 1
            print(f"[{date_str}] SKIP (already downloaded: {archive_path})")
            continue

        retrieved_at = datetime.now(timezone.utc).isoformat()

        if not versions_lookup_ok:
            any_failure = True
            record: dict[str, object] = {
                "target_date": date_str,
                "month": month_str,
                "status": "failed",
                "http_status": versions_status,
                "retrieved_at": retrieved_at,
                "error": versions_err or "Versions listing lookup failed",
            }
            print(f"[{date_str}] FAILED ({versions_status or 'err'}): {record['error']}")
            upsert_record(records, record)
            save_manifest(manifest_path, provider, records)
            continue

        publication = resolve_publication_for_date(target_date, versions_list)
        if publication is None:
            record = {
                "target_date": date_str,
                "month": month_str,
                "status": "unavailable",
                "http_status": versions_status,
                "retrieved_at": retrieved_at,
                "error": f"No published NeTEx version on or before {date_str} in available versions",
            }
            print(f"[{date_str}] UNAVAILABLE: {record['error']}")
            upsert_record(records, record)
            save_manifest(manifest_path, provider, records)
            continue

        generation_id = str(publication["generation"])
        pub_date = str(publication["publication_date"])
        expected_size = publication.get("expected_size")
        expected_md5_b64 = publication.get("expected_md5_b64")
        download_url = version_download_template.format(
            codespace=codespace,
            generation=generation_id,
            import_key=generation_id,
            date=date_str,
            month=month_str,
        )

        status, byte_count, err = stream_to_temp_file(
            url=download_url,
            tmp_path=tmp_path,
            headers=headers,
        )

        if status == 200 and err is None:
            is_valid, val_err, digest, actual_md5_b64 = validate_downloaded_archive(
                path=tmp_path,
                expected_size=expected_size if isinstance(expected_size, int) else None,
                expected_md5_b64=expected_md5_b64 if isinstance(expected_md5_b64, str) else None,
            )
            if is_valid and digest is not None:
                try:
                    archive_path.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(tmp_path, archive_path)
                finally:
                    tmp_path.unlink(missing_ok=True)

                unchanged = (previous_sha256 is not None and digest == previous_sha256)
                previous_sha256 = digest
                downloaded_count += 1

                record = {
                    "target_date": date_str,
                    "month": month_str,
                    "status": "downloaded",
                    "http_status": status,
                    "generation": generation_id,
                    "publication_date": pub_date,
                    "retrieved_at": retrieved_at,
                    "file": str(archive_path),
                    "bytes": byte_count,
                    "md5_b64": actual_md5_b64,
                    "sha256": digest,
                    "unchanged_from_previous": unchanged,
                }
                print(
                    f"[{date_str}] OK (gen={generation_id}, pub={pub_date}) -> "
                    f"{archive_path} ({byte_count} bytes, unchanged={unchanged})"
                )
            else:
                tmp_path.unlink(missing_ok=True)
                any_failure = True
                record = {
                    "target_date": date_str,
                    "month": month_str,
                    "status": "failed",
                    "http_status": status,
                    "generation": generation_id,
                    "publication_date": pub_date,
                    "retrieved_at": retrieved_at,
                    "error": val_err or "Downloaded archive failed integrity validation",
                }
                print(f"[{date_str}] FAILED (gen={generation_id}): {record['error']}")
        else:
            tmp_path.unlink(missing_ok=True)
            any_failure = True
            record = {
                "target_date": date_str,
                "month": month_str,
                "status": "unavailable" if status in (404, 410) else "failed",
                "http_status": status,
                "generation": generation_id,
                "publication_date": pub_date,
                "retrieved_at": retrieved_at,
                "error": err or "Download failed",
            }
            print(f"[{date_str}] {str(record['status']).upper()} ({status or 'err'}): {record['error']}")

        upsert_record(records, record)
        save_manifest(manifest_path, provider, records)

    return manifest_path, any_failure, downloaded_count


# %%
# Parse CLI arguments and month ranges for both monthly-step backfills and single-day runs.
def parse_months(spec: str) -> list[int]:
    months: set[int] = set()
    for part in spec.split(","):
        token = part.strip()
        if not token:
            continue
        if "-" in token:
            start_s, end_s = token.split("-", 1)
            start_m, end_m = int(start_s), int(end_s)
            if not (1 <= start_m <= 12 and 1 <= end_m <= 12 and start_m <= end_m):
                raise ValueError(f"Invalid month range: {token}")
            months.update(range(start_m, end_m + 1))
        else:
            m = int(token)
            if not (1 <= m <= 12):
                raise ValueError(f"Invalid month: {token}")
            months.add(m)
    return sorted(months)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze NeTEx downloader for KOL.")
    parser.add_argument(
        "--mode",
        choices=["monthly-step", "single-day"],
        default="monthly-step",
        help="Use 'monthly-step' for per-month step backfills or 'single-day' for daily runs.",
    )
    parser.add_argument(
        "--date",
        dest="single_date",
        help="Target date in YYYY-MM-DD format when --mode single-day is used.",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2026,
        help="Target year for monthly-step mode (default: 2026).",
    )
    parser.add_argument(
        "--months",
        default="1-11",
        help="Month range or comma list for monthly-step mode, e.g. '1-11' (default: 1-11).",
    )
    parser.add_argument(
        "--step-days",
        type=int,
        default=7,
        help="Day step within each month starting on the 1st (default: 7).",
    )
    parser.add_argument(
        "--until",
        dest="cutoff_date",
        help="Optional cutoff date (YYYY-MM-DD) for monthly-step mode; defaults to today.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if target_date already has a verified Bronze ZIP on disk.",
    )
    return parser.parse_args(argv)


# %%
# Entry point for CLI, GitHub Actions, and interactive notebook execution.
def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    versions_url_template = os.environ.get(
        "KOL_NETEX_VERSIONS_URL",
        (
            "https://storage.googleapis.com/storage/v1/b/marduk-production/o"
            "?prefix=outbound%2Fnetex%2Frb_{codespace}-aggregated-netex.zip"
            "&versions=true&maxResults=1000"
            "&fields=items(name,generation,timeCreated,updated,size,md5Hash),nextPageToken"
        ),
    ).strip()
    version_download_template = os.environ.get(
        "KOL_NETEX_VERSION_URL_TEMPLATE",
        "https://storage.googleapis.com/marduk-production/outbound/netex/rb_{codespace}-aggregated-netex.zip?generation={generation}",
    ).strip()

    data_root = Path(os.environ.get("DAT535_DATA_ROOT", "/srv/dat535/data"))
    bronze_root = data_root / "bronze"
    client_name = os.environ.get("ET_CLIENT_NAME", "dat535-spark-pipeline")
    headers = build_headers(client_name)

    if args.mode == "single-day":
        if not args.single_date:
            print("Error: --date YYYY-MM-DD is required when --mode single-day.", file=sys.stderr)
            return 2
        target_dates = [date.fromisoformat(args.single_date)]
    else:
        months = parse_months(args.months)
        cutoff = date.fromisoformat(args.cutoff_date) if args.cutoff_date else date.today()
        target_dates = iter_monthly_step_dates(
            year=args.year,
            months=months,
            step_days=args.step_days,
            cutoff_date=cutoff,
        )

    manifest_path, any_failure, downloaded_count = run_ingestion(
        target_dates=target_dates,
        version_download_template=version_download_template,
        versions_url_template=versions_url_template,
        bronze_root=bronze_root,
        headers=headers,
        provider="KOL",
        force=args.force,
    )
    print(f"Manifest updated: {manifest_path}")
    should_fail = any_failure or (len(target_dates) > 0 and downloaded_count == 0)
    return 1 if should_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())