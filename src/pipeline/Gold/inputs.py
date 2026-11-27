"""Which Silver run is the authority for each operating date, and narrow reads of Silver tables."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from pipeline.Gold import DEFAULT_PROVIDER


@dataclass(frozen=True)
class DateAuthority:
    provider: str
    operating_date: str
    source_silver_run_id: str
    silver_manifest_path: str
    silver_manifest_sha256: str
    publication_id: str
    silver_gate_status: str
    siri_file_available: bool


@dataclass(frozen=True)
class SilverInventory:
    provider: str
    expected_dates: tuple[str, ...]
    authorities: tuple[DateAuthority, ...]
    excluded_runs: tuple[dict[str, str], ...]


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def build_silver_inventory(
    *,
    silver_runs_dir: str | Path,
    expected_dates: Sequence[str],
    provider: str = DEFAULT_PROVIDER,
    allowed_run_ids: Sequence[str] | None = None,
) -> SilverInventory:
    """Find exactly one complete, gate-passed Silver run for every expected date (or fail)."""
    provider = provider.upper()
    runs_root = Path(silver_runs_dir)
    if not runs_root.is_dir():
        raise FileNotFoundError(f"Silver runs directory does not exist: {runs_root}")
    expected = tuple(sorted({str(d).strip() for d in expected_dates if str(d).strip()}))
    if not expected:
        raise ValueError("expected_dates must not be empty.")

    excluded: list[dict[str, str]] = []
    candidates: dict[str, list[DateAuthority]] = {d: [] for d in expected}
    for run_dir in sorted(p for p in runs_root.iterdir() if p.is_dir()):
        run_id = run_dir.name
        if run_id.startswith(("diagnostic-", "failed-", "superseded-", ".")):
            excluded.append({"run_id": run_id, "reason": "excluded_by_prefix"})
            continue
        if allowed_run_ids is not None and run_id not in set(allowed_run_ids):
            excluded.append({"run_id": run_id, "reason": "not_in_allowed_run_ids"})
            continue

        manifest_path = next((p for p in (run_dir / "silver_manifest.json", run_dir / "manifest.json") if p.is_file()),
                             next(iter(sorted(run_dir.glob("*manifest*.json"))), None))
        if manifest_path is None:
            raise FileNotFoundError(f"Missing Silver manifest in candidate run directory: {run_dir}")
        manifest = _read_json(manifest_path)
        status = str(manifest.get("status") or "").strip().lower()
        if status != "complete":
            raise ValueError(f"Silver run {run_id} manifest status is '{status}', expected 'complete'.")
        run_provider = str(manifest.get("provider") or provider).strip().upper()
        if run_provider != provider:
            excluded.append({"run_id": run_id, "reason": f"provider_mismatch:{run_provider}"})
            continue

        report_path = Path(manifest.get("quality_report_path") or run_dir / "quality" / "quality_report.json")
        if not report_path.is_file():
            raise FileNotFoundError(f"Missing Silver quality report for run {run_id}: {report_path}")
        report = _read_json(report_path)
        gate = str(report.get("gate_status") or "").strip().lower()
        if gate != "passed":
            raise ValueError(f"Silver run {run_id} gate_status is '{gate}', expected 'passed'.")

        # operating_date -> (publication_id, siri_file_available) from the run's daily summaries
        dates: dict[str, tuple[str, bool]] = {}
        for row in report.get("daily_summaries") or []:
            if (row.get("provider") or provider).upper() != provider:
                continue
            day = str(row.get("operating_date") or row.get("snapshot_date") or "").strip()
            pub = str(row.get("publication_id") or "").strip()
            if day in dates and dates[day][0] != pub:
                raise ValueError(f"Inconsistent publication_id for {provider} {day} in {run_dir}: {dates[day][0]} vs {pub}")
            if day:
                dates[day] = (pub, bool(row.get("siri_file_available", False)))

        sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        for day, (pub, siri) in sorted(dates.items()):
            if day not in candidates:
                continue
            if not pub:
                raise ValueError(f"Missing publication_id for {provider} {day} in Silver run {run_id}.")
            candidates[day].append(DateAuthority(provider, day, run_id, str(manifest_path), sha, pub, gate, siri))

    missing = [d for d, c in candidates.items() if not c]
    if missing:
        raise ValueError(f"Missing authoritative Silver coverage for expected dates ({len(missing)}): {', '.join(missing)}")
    duplicates = {d: [a.source_silver_run_id for a in c] for d, c in candidates.items() if len(c) > 1}
    if duplicates:
        raise ValueError(f"Duplicate Silver date authority detected without explicit selection: {duplicates}")
    return SilverInventory(provider, expected, tuple(candidates[d][0] for d in expected), tuple(excluded))


def authority_df(spark: SparkSession, inventory: SilverInventory, *extra: str) -> DataFrame:
    """One row per date: provider, source_silver_run_id, publication_id, [extra...], operating_date."""
    types = {"siri_file_available": T.BooleanType()}
    fields = ["provider", "operating_date_str", "source_silver_run_id", "publication_id", *extra]
    rows = [(a.provider, a.operating_date, a.source_silver_run_id, a.publication_id,
             *(getattr(a, f) for f in extra)) for a in inventory.authorities]
    schema = T.StructType([T.StructField(f, types.get(f, T.StringType()), False) for f in fields])
    return (spark.createDataFrame(rows, schema)
            .withColumn("operating_date", F.to_date("operating_date_str")).drop("operating_date_str"))


def read_silver_table_for_inventory(
    spark: SparkSession,
    inventory: SilverInventory,
    table_name: str,
    columns: Sequence[str] | None = None,
) -> DataFrame:
    """Read a Silver table from every authoritative run, keeping only each date's publication."""
    by_run: dict[str, list[DateAuthority]] = {}
    for a in inventory.authorities:
        by_run.setdefault(a.source_silver_run_id, []).append(a)

    frames = []
    for run_id, auths in sorted(by_run.items()):
        path = Path(auths[0].silver_manifest_path).parent / table_name
        if not path.exists():
            raise FileNotFoundError(f"Silver table '{table_name}' missing in run '{run_id}': {path}")
        df = spark.read.parquet(str(path))
        if "operating_date" not in df.columns and "snapshot_date" in df.columns:
            df = df.withColumn("operating_date", F.to_date("snapshot_date"))
        if columns is not None:
            missing = [c for c in columns if c not in df.columns]
            if missing:
                raise KeyError(f"Columns {missing} not found in Silver table '{table_name}' ({run_id}).")
            df = df.select(*columns)

        has_pub = "publication_id" in df.columns
        if "operating_date" in df.columns:
            keep = None
            for a in auths:
                cond = F.col("operating_date").cast("string") == F.lit(a.operating_date)
                if has_pub:
                    cond = cond & (F.col("publication_id") == F.lit(a.publication_id))
                keep = cond if keep is None else keep | cond
            df = df.filter(keep)
        elif has_pub:
            df = df.filter(F.col("publication_id").isin(sorted({a.publication_id for a in auths})))
        if "provider" in df.columns:
            df = df.filter(F.col("provider") == F.lit(inventory.provider))
        frames.append(df.withColumn("source_silver_run_id", F.lit(run_id)))

    merged = frames[0]
    for df in frames[1:]:
        merged = merged.unionByName(df, allowMissingColumns=True)
    return merged
