# %% [markdown]
# # Bronze Ingestion: Entur Historical `realtime_siri_et` (`KOL`)
#
# Queries Entur's BigQuery table (`ent-data-sharing-ext-prd.realtime_siri_et.realtime_siri_et`)
# filtered to Kolumbus (`dataSource = 'KOL'`) and `operatingDate = <YYYY-MM-DD>`, and streams raw
# Parquet files into `/srv/dat535/data/bronze/siri_et/kol/<YYYY-MM>/` using atomic writes,
# SHA-256 verification, incremental manifest persistence, and non-zero failure exits.
# Raw columns and values are preserved untouched for downstream Spark transformations.

# %%
from __future__ import annotations

import argparse
import calendar
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys

from google.cloud import bigquery
import pyarrow.parquet as pq


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
# Compute SHA-256 hex digest of a file in streaming chunks to keep memory usage low.
def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


# %%
# Load existing manifest records and verify an existing Parquet file against its recorded SHA-256.
def load_manifest(manifest_path: Path) -> list[dict[str, object]]:
    if not manifest_path.exists():
        return []
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    records = data.get("records", [])
    return records if isinstance(records, list) else []


def save_manifest(
    manifest_path: Path,
    provider: str,
    table_id: str,
    records: list[dict[str, object]],
) -> None:
    payload = {
        "source": "realtime_siri_et",
        "table": table_id,
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
            if sha256_file(file_path) == expected_sha:
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
# Query BigQuery for a single (provider, operatingDate) and write raw pages to a temporary Parquet file.
def fetch_siri_et_day_to_parquet(
    client: bigquery.Client,
    table_id: str,
    provider: str,
    target_date: date,
    tmp_path: Path,
) -> tuple[int, int]:
    query = f"""
        SELECT *
        FROM `{table_id}`
        WHERE dataSource = @data_source
          AND operatingDate = @operating_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("data_source", "STRING", provider.upper()),
            bigquery.ScalarQueryParameter("operating_date", "DATE", target_date),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    result = query_job.result()

    row_count = 0
    writer: pq.ParquetWriter | None = None
    tmp_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        for page in result.pages:
            page_table = page.to_arrow()
            if page_table.num_rows == 0:
                continue
            if writer is None:
                writer = pq.ParquetWriter(tmp_path, page_table.schema, compression="snappy")
            writer.write_table(page_table)
            row_count += page_table.num_rows
    finally:
        if writer is not None:
            writer.close()

    if row_count == 0:
        tmp_path.unlink(missing_ok=True)

    bytes_billed = int(query_job.total_bytes_billed or 0)
    return row_count, bytes_billed

# %%
# Process target dates, publishing verified Parquet files atomically and updating the manifest per attempt.
def run_ingestion(
    client: bigquery.Client,
    table_id: str,
    target_dates: list[date],
    bronze_root: Path,
    provider: str = "KOL",
    force: bool = False,
) -> tuple[Path, bool, int]:
    codespace = provider.lower()
    provider_dir = bronze_root / "siri_et" / codespace
    provider_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = provider_dir / "manifest.json"
    records = load_manifest(manifest_path)

    previous_sha256: str | None = None
    for existing in records:
        if existing.get("status") == "downloaded" and existing.get("sha256"):
            previous_sha256 = str(existing["sha256"])

    any_failure = False
    downloaded_count = 0

    for target_date in target_dates:
        month_str = target_date.strftime("%Y-%m")
        date_str = target_date.isoformat()
        parquet_path = provider_dir / month_str / f"{codespace}_{date_str}.parquet"
        tmp_path = parquet_path.with_name(f".{parquet_path.name}.part.{os.getpid()}")

        # Skip re-downloading if already completed and verified on disk unless --force is set.
        existing_ok = None if force else find_existing_download(records, date_str)
        if existing_ok is not None:
            if existing_ok.get("sha256"):
                previous_sha256 = str(existing_ok["sha256"])
            downloaded_count += 1
            print(f"[{date_str}] SKIP (already downloaded: {parquet_path})")
            continue

        retrieved_at = datetime.now(timezone.utc).isoformat()

        try:
            row_count, bytes_billed = fetch_siri_et_day_to_parquet(
                client=client,
                table_id=table_id,
                provider=provider,
                target_date=target_date,
                tmp_path=tmp_path,
            )

            if row_count == 0:
                tmp_path.unlink(missing_ok=True)
                record: dict[str, object] = {
                    "target_date": date_str,
                    "month": month_str,
                    "status": "unavailable",
                    "retrieved_at": retrieved_at,
                    "rows": 0,
                    "bytes_billed": bytes_billed,
                    "error": f"No rows returned for {provider.upper()} on {date_str}",
                }
                print(f"[{date_str}] UNAVAILABLE: {record['error']}")
            else:
                try:
                    parquet_path.parent.mkdir(parents=True, exist_ok=True)
                    with tmp_path.open("rb") as handle:
                        os.fsync(handle.fileno())
                    os.replace(tmp_path, parquet_path)
                finally:
                    tmp_path.unlink(missing_ok=True)

                byte_count = parquet_path.stat().st_size
                digest = sha256_file(parquet_path)
                unchanged = (previous_sha256 is not None and digest == previous_sha256)
                previous_sha256 = digest
                downloaded_count += 1

                record = {
                    "target_date": date_str,
                    "month": month_str,
                    "status": "downloaded",
                    "retrieved_at": retrieved_at,
                    "file": str(parquet_path),
                    "rows": row_count,
                    "bytes": byte_count,
                    "bytes_billed": bytes_billed,
                    "sha256": digest,
                    "unchanged_from_previous": unchanged,
                }
                print(
                    f"[{date_str}] OK ({row_count} rows, {byte_count} bytes, "
                    f"billed={bytes_billed} B) -> {parquet_path}"
                )
        except Exception as exc:
            tmp_path.unlink(missing_ok=True)
            any_failure = True
            record = {
                "target_date": date_str,
                "month": month_str,
                "status": "failed",
                "retrieved_at": retrieved_at,
                "error": str(exc),
            }
            print(f"[{date_str}] FAILED: {record['error']}")

        upsert_record(records, record)
        save_manifest(manifest_path, provider, table_id, records)

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
    parser = argparse.ArgumentParser(description="Bronze BigQuery SIRI-ET downloader for KOL.")
    parser.add_argument(
        "--mode",
        choices=["monthly-step", "single-day"],
        default="monthly-step",
        help="Use 'monthly-step' for per-month step backfills or 'single-day' for daily runs.",
    )
    parser.add_argument(
        "--date",
        dest="single_date",
        help="Target operatingDate in YYYY-MM-DD format when --mode single-day is used.",
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
        help="Re-download even if target_date already has a verified Bronze Parquet file on disk.",
    )
    return parser.parse_args(argv)


# %%
# Entry point for CLI, GitHub Actions, and interactive notebook execution.
def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    project_id = os.environ.get("GCP_PROJECT_ID", "").strip() or None
    table_id = os.environ.get(
        "KOL_SIRI_ET_BQ_TABLE",
        "ent-data-sharing-ext-prd.realtime_siri_et.realtime_siri_et",
    ).strip()

    data_root = Path(os.environ.get("DAT535_DATA_ROOT", "/srv/dat535/data"))
    bronze_root = data_root / "bronze"

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

    client = bigquery.Client(project=project_id)
    manifest_path, any_failure, downloaded_count = run_ingestion(
        client=client,
        table_id=table_id,
        target_dates=target_dates,
        bronze_root=bronze_root,
        provider="KOL",
        force=args.force,
    )
    print(f"Manifest updated: {manifest_path}")
    should_fail = any_failure or (len(target_dates) > 0 and downloaded_count == 0)
    return 1 if should_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())