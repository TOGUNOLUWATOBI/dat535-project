"""Explicit Silver run inventory, date authority resolution, and narrow reads."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Iterable, Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F

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

    def by_date(self) -> dict[str, DateAuthority]:
        return {auth.operating_date: auth for auth in self.authorities}


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _find_manifest_path(run_dir: Path) -> Path | None:
    candidates = [
        run_dir / "silver_manifest.json",
        run_dir / "manifest.json",
    ]
    for c in candidates:
        if c.is_file():
            return c
    matches = sorted(run_dir.glob("*manifest*.json"))
    return matches[0] if matches else None


def _extract_dates_and_publications(
    run_dir: Path,
    manifest_data: dict,
    provider: str,
) -> dict[str, tuple[str, bool]]:
    """Return mapping of operating_date -> (publication_id, siri_file_available)."""
    mapping: dict[str, tuple[str, bool]] = {}

    q_report_raw = manifest_data.get("quality_report_path")
    q_report_path = Path(q_report_raw) if q_report_raw else (run_dir / "quality" / "quality_report.json")
    if q_report_path.is_file():
        q_data = json.loads(q_report_path.read_text(encoding="utf-8"))
        for row in q_data.get("daily_summaries") or []:
            row_provider = (row.get("provider") or provider).upper()
            if row_provider != provider.upper():
                continue
            op_date = str(row.get("operating_date") or row.get("snapshot_date") or "").strip()
            pub_id = str(row.get("publication_id") or "").strip()
            siri_avail = bool(row.get("siri_file_available", False))
            if op_date:
                if op_date in mapping and mapping[op_date][0] != pub_id:
                    raise ValueError(
                        f"Inconsistent publication_id for {provider} {op_date} in {run_dir}: "
                        f"{mapping[op_date][0]} vs {pub_id}"
                    )
                mapping[op_date] = (pub_id, siri_avail)

    if mapping:
        return mapping

    # Fallback if daily_summaries is absent: inspect selection/entries if present
    sel = manifest_data.get("input_selection") or {}
    dates = sel.get("requested_dates") or manifest_data.get("requested_dates") or []
    for d in dates:
        ds = str(d).strip()
        if ds:
            mapping[ds] = ("", False)
    return mapping


def build_silver_inventory(
    *,
    silver_runs_dir: str | Path,
    expected_dates: Sequence[str],
    provider: str = DEFAULT_PROVIDER,
    allowed_run_ids: Sequence[str] | None = None,
) -> SilverInventory:
    """Resolve and validate a strict 1:1 (provider, operating_date) Silver authority map."""
    provider_norm = provider.upper()
    runs_root = Path(silver_runs_dir)
    if not runs_root.is_dir():
        raise FileNotFoundError(f"Silver runs directory does not exist: {runs_root}")

    expected_clean = tuple(sorted({str(d).strip() for d in expected_dates if str(d).strip()}))
    if not expected_clean:
        raise ValueError("expected_dates must not be empty.")

    allowed_set = set(allowed_run_ids) if allowed_run_ids is not None else None
    excluded: list[dict[str, str]] = []
    date_to_candidates: dict[str, list[DateAuthority]] = {d: [] for d in expected_clean}

    for entry in sorted(runs_root.iterdir()):
        if not entry.is_dir():
            continue
        run_id = entry.name
        if run_id.startswith(("diagnostic-", "failed-", "superseded-", ".")):
            excluded.append({"run_id": run_id, "reason": "excluded_by_prefix"})
            continue
        if allowed_set is not None and run_id not in allowed_set:
            excluded.append({"run_id": run_id, "reason": "not_in_allowed_run_ids"})
            continue

        manifest_path = _find_manifest_path(entry)
        if manifest_path is None:
            raise FileNotFoundError(f"Missing Silver manifest in candidate run directory: {entry}")

        manifest_sha = _sha256_file(manifest_path)
        manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))

        manifest_status = str(manifest_data.get("status") or "").strip().lower()
        if manifest_status != "complete":
            raise ValueError(
                f"Silver run {run_id} manifest status is '{manifest_status}', expected 'complete'."
            )

        manifest_provider = str(manifest_data.get("provider") or provider_norm).strip().upper()
        if manifest_provider != provider_norm:
            excluded.append({"run_id": run_id, "reason": f"provider_mismatch:{manifest_provider}"})
            continue

        q_report_raw = manifest_data.get("quality_report_path")
        q_report_path = Path(q_report_raw) if q_report_raw else (entry / "quality" / "quality_report.json")
        if not q_report_path.is_file():
            raise FileNotFoundError(f"Missing Silver quality report for run {run_id}: {q_report_path}")
        q_data = json.loads(q_report_path.read_text(encoding="utf-8"))
        gate_status = str(q_data.get("gate_status") or "").strip().lower()
        if gate_status != "passed":
            raise ValueError(
                f"Silver run {run_id} gate_status is '{gate_status}', expected 'passed'."
            )

        date_pub_map = _extract_dates_and_publications(entry, manifest_data, provider_norm)
        for op_date, (pub_id, siri_avail) in sorted(date_pub_map.items()):
            if op_date not in date_to_candidates:
                continue
            if not pub_id:
                raise ValueError(
                    f"Missing publication_id for {provider_norm} {op_date} in Silver run {run_id}."
                )
            date_to_candidates[op_date].append(
                DateAuthority(
                    provider=provider_norm,
                    operating_date=op_date,
                    source_silver_run_id=run_id,
                    silver_manifest_path=str(manifest_path),
                    silver_manifest_sha256=manifest_sha,
                    publication_id=pub_id,
                    silver_gate_status=gate_status,
                    siri_file_available=siri_avail,
                )
            )

    missing_dates = [d for d, cands in date_to_candidates.items() if len(cands) == 0]
    if missing_dates:
        raise ValueError(
            f"Missing authoritative Silver coverage for expected dates ({len(missing_dates)}): "
            f"{', '.join(missing_dates)}"
        )

    duplicate_dates = {d: [c.source_silver_run_id for c in cands] for d, cands in date_to_candidates.items() if len(cands) > 1}
    if duplicate_dates:
        raise ValueError(
            f"Duplicate Silver date authority detected without explicit selection: {duplicate_dates}"
        )

    resolved = tuple(date_to_candidates[d][0] for d in expected_clean)
    return SilverInventory(
        provider=provider_norm,
        expected_dates=expected_clean,
        authorities=resolved,
        excluded_runs=tuple(excluded),
    )


def authority_lookup_df(spark: SparkSession, inventory: SilverInventory) -> DataFrame:
    """Create a small Spark DataFrame of authoritative (operating_date, source_silver_run_id, publication_id)."""
    rows = [
        (
            a.provider,
            a.operating_date,
            a.source_silver_run_id,
            a.publication_id,
            a.siri_file_available,
        )
        for a in inventory.authorities
    ]
    return spark.createDataFrame(
        rows,
        schema=[
            "provider",
            "operating_date_str",
            "source_silver_run_id",
            "publication_id",
            "siri_file_available",
        ],
    )


def read_silver_table_for_inventory(
    spark: SparkSession,
    inventory: SilverInventory,
    table_name: str,
    columns: Sequence[str] | None = None,
) -> DataFrame:
    """Read a Silver table across authoritative runs, filtering to each date's publication_id."""
    by_run: dict[str, list[DateAuthority]] = {}
    for auth in inventory.authorities:
        by_run.setdefault(auth.source_silver_run_id, []).append(auth)

    frames: list[DataFrame] = []
    for run_id, auths in sorted(by_run.items()):
        table_path = Path(auths[0].silver_manifest_path).parent / table_name
        if not table_path.exists():
            raise FileNotFoundError(f"Silver table '{table_name}' missing in run '{run_id}': {table_path}")

        df = spark.read.parquet(str(table_path))
        if "operating_date" not in df.columns and "snapshot_date" in df.columns:
            df = df.withColumn("operating_date", F.to_date(F.col("snapshot_date")))
        if columns is not None:
            available = set(df.columns)
            projected = [c for c in columns if c in available]
            missing_cols = [c for c in columns if c not in available]
            if missing_cols:
                raise KeyError(
                    f"Columns {missing_cols} not found in Silver table '{table_name}' ({run_id})."
                )
            df = df.select(*projected)

        has_op_date = "operating_date" in df.columns
        has_pub_id = "publication_id" in df.columns
        if has_op_date:
            pair_cond = None
            for a in auths:
                cond = F.col("operating_date").cast("string") == F.lit(a.operating_date)
                if has_pub_id:
                    cond = cond & (F.col("publication_id") == F.lit(a.publication_id))
                pair_cond = cond if pair_cond is None else (pair_cond | cond)
            if pair_cond is not None:
                df = df.filter(pair_cond)
        elif has_pub_id:
            pub_ids = sorted({a.publication_id for a in auths if a.publication_id})
            if pub_ids:
                df = df.filter(F.col("publication_id").isin(pub_ids))

        if "provider" in df.columns:
            df = df.filter(F.col("provider") == F.lit(inventory.provider))

        df = df.withColumn("source_silver_run_id", F.lit(run_id))
        frames.append(df)

    if not frames:
        raise ValueError(f"No Silver DataFrames constructed for table '{table_name}'.")

    merged = frames[0]
    for nxt in frames[1:]:
        merged = merged.unionByName(nxt, allowMissingColumns=True)
    return merged


def enrich_uniquely(
    fact_df: DataFrame,
    dim_df: DataFrame,
    join_keys: Iterable[str],
    select_cols: Sequence[str],
) -> DataFrame:
    """Enrich fact_df from dim_df after verifying uniqueness on join_keys to prevent multiplication."""
    keys = list(join_keys)
    if not keys:
        raise ValueError("join_keys must not be empty for dimension enrichment.")

    dim_projected = dim_df.select(*dict.fromkeys(keys + list(select_cols))).dropDuplicates(keys)
    # Guard against duplicate keys in dimension before joining
    total_dim = dim_df.select(*keys).count()
    distinct_dim = dim_projected.count()
    if total_dim != distinct_dim:
        raise ValueError(
            f"Dimension enrichment key {keys} is not unique: {total_dim} rows vs {distinct_dim} distinct keys."
        )

    fact_count = fact_df.count()
    enriched = fact_df.join(dim_projected, on=keys, how="left")
    after_count = enriched.count()
    if after_count != fact_count:
        raise ValueError(
            f"Fact row count changed during enrichment on {keys}: {fact_count} -> {after_count}"
        )
    return enriched
