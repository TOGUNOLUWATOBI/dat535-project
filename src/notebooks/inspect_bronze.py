# %% [markdown]
# # Bronze Data Inspector (`KOL` NeTEx & `realtime_siri_et`)
#
# Quick inspection utility to summarize manifests, list XML contents of NeTEx archives,
# and print Parquet schemas, row counts, null counts, and sample records for SIRI-ET.

# %%
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import zipfile

import pyarrow.parquet as pq


# %%
def inspect_manifest(manifest_path: Path, label: str) -> list[dict[str, object]]:
    print(f"\n{'=' * 70}\n{label} MANIFEST: {manifest_path}\n{'=' * 70}")
    if not manifest_path.exists():
        print("Manifest not found.")
        return []

    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    records = data.get("records", [])
    by_status: dict[str, int] = {}
    for rec in records:
        status = str(rec.get("status", "unknown"))
        by_status[status] = by_status.get(status, 0) + 1

    print(f"Updated at : {data.get('updated_at')}")
    print(f"Total dates: {len(records)} -> {by_status}")
    return records


# %%
def inspect_netex(netex_dir: Path, sample_date: str | None) -> None:
    records = inspect_manifest(netex_dir / "manifest.json", "NETEX")
    downloaded = [r for r in records if r.get("status") == "downloaded"]
    if not downloaded:
        return

    target = next(
        (r for r in downloaded if r.get("target_date") == sample_date),
        downloaded[-1],
    )
    zip_path = Path(str(target["file"]))
    print(f"\nSample NeTEx archive: {zip_path} ({zip_path.stat().st_size:,} bytes)")

    with zipfile.ZipFile(zip_path, "r") as zf:
        entries = zf.infolist()
        xml_files = [e for e in entries if e.filename.endswith(".xml")]
        print(f"Total entries: {len(entries)} | XML files: {len(xml_files)}")
        for info in entries[:15]:
            print(f"  - {info.filename} ({info.file_size:,} B)")
        if len(entries) > 15:
            print(f"  ... ({len(entries) - 15} more files)")


# %%
def inspect_siri_et(siri_dir: Path, sample_date: str | None, rows: int) -> None:
    records = inspect_manifest(siri_dir / "manifest.json", "SIRI-ET")
    downloaded = [r for r in records if r.get("status") == "downloaded"]
    if not downloaded:
        return

    total_rows = sum(int(r.get("rows", 0)) for r in downloaded)
    print(f"Total SIRI-ET rows across {len(downloaded)} files: {total_rows:,}")

    target = next(
        (r for r in downloaded if r.get("target_date") == sample_date),
        downloaded[-1],
    )
    parquet_path = Path(str(target["file"]))
    pf = pq.ParquetFile(parquet_path)

    print(f"\nSample SIRI-ET Parquet: {parquet_path}")
    print(f"Rows: {pf.metadata.num_rows:,} | Columns: {pf.metadata.num_columns} | Row groups: {pf.metadata.num_row_groups}")
    print("\n--- Schema ---")
    print(pf.schema_arrow)

    # Read first row group (or slice) for quick column stats & sample rows without loading huge files
    table = pf.read_row_group(0)
    sample = table.slice(0, rows)

    print("\n--- Non-Null Check (Row Group 0) ---")
    for col_name in table.column_names:
        col = table[col_name]
        non_null = len(col) - col.null_count
        print(f"  {col_name:<35} non-null: {non_null:>7,} / {len(col):,}")

    print(f"\n--- First {sample.num_rows} Row(s) ---")
    for idx, row in enumerate(sample.to_pylist(), start=1):
        print(f"\n[Row {idx}]")
        for k, v in row.items():
            print(f"  {k}: {v!r}")


# %%
def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect Bronze NeTEx and SIRI-ET files.")
    parser.add_argument("--date", help="Specific target_date (YYYY-MM-DD) to inspect; defaults to latest.")
    parser.add_argument("--rows", type=int, default=2, help="Number of sample SIRI-ET rows to print (default: 2).")
    args = parser.parse_args()

    data_root = Path(os.environ.get("DAT535_DATA_ROOT", "/srv/dat535/data"))
    bronze_root = data_root / "bronze"

    inspect_netex(bronze_root / "netex" / "kol", args.date)
    inspect_siri_et(bronze_root / "siri_et" / "kol", args.date, args.rows)


if __name__ == "__main__":
    main()