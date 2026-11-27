"""daily_data_quality, the Gold quality gate, the manifest, and atomic publication."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

from pyspark.sql import DataFrame, SparkSession, functions as F

from pipeline.Gold import (
    ARRIVAL_WITHIN_LIMIT_MAX_SEC, BASELINES, BUSINESS_AGREEMENT_TOLERANCE_SEC, DEFAULT_PERCENTILE_ACCURACY,
    DEFAULT_SHUFFLE_PARTITIONS, DEFAULT_TIMEZONE, DEPARTURE_EARLY_MAX_EXCLUSIVE_SEC, DEPARTURE_WITHIN_WINDOW_MAX_SEC,
    EVENT_TYPES, GOLD_SCHEMA_VERSION, GOLD_TABLES, METRIC_POLICY_VERSION, SEASON_WINDOWS_2026,
    SEVERE_DELAY_MIN_EXCLUSIVE_SEC, TIMESTAMP_IDENTITY_TOLERANCE_SEC, add_version_columns, count_if, lower_text, ratio,
)
from pipeline.Gold.event_metrics import SLICES, normalize_journey_key_expr
from pipeline.Gold.inputs import SilverInventory, authority_df, read_silver_table_for_inventory

DAY = ["provider", "operating_date", "publication_id"]
QUALITY_NOTES = [
    "Analysis covers 53 selected operating dates across Jan-Nov 2026 (sampled cadence), not every calendar day.",
    "winter-2026 is defined as January-February 2026 within this study scope.",
    "Weekday composition varies across months and seasons; descriptive differences do not prove weather or causal effects.",
]


def build_daily_data_quality(spark: SparkSession, inventory: SilverInventory, journey_perf_df: DataFrame, *,
                             gold_run_id: str) -> DataFrame:
    """One row per date: how much of the timetable and the live feed matched, and why events were excluded."""
    read = lambda table, columns: read_silver_table_for_inventory(spark, inventory, table, columns)
    status = lower_text("service_status")
    planned = read("journey_service_dates", [*DAY, "service_status"]).groupBy(*DAY).agg(
        count_if(status.isin("scheduled", "active", "operating"), "scheduled_journey_count"),
        count_if(~status.isin("scheduled", "active", "operating", "not_scheduled", "inactive"),
                 "calendar_unresolved_journey_count"))
    calls = read("netex_stop_calls", [*DAY, "passing_time_id"]).groupBy(*DAY).agg(
        F.count(F.lit(1)).cast("long").alias("planned_call_count"),
        F.countDistinct("passing_time_id").cast("long").alias("coverage_eligible_planned_call_count"))
    dates = read("reconciled_stop_calls", [*DAY, "et_canonical_id"]).dropDuplicates(["provider", "et_canonical_id"])
    siri = (read("siri_et_stop_calls", ["provider", "et_canonical_id", "service_journey_id", "dated_service_journey_id",
                                        "duplicate_status"])
            .withColumn("journey_key", normalize_journey_key_expr())
            .join(dates, on=["provider", "et_canonical_id"], how="inner")
            .groupBy(*DAY).agg(
                F.count(F.lit(1)).cast("long").alias("raw_reported_row_count"),
                F.countDistinct("et_canonical_id").cast("long").alias("canonical_reported_call_count"),
                count_if(F.col("journey_key").isNull(), "unkeyed_reported_call_count"),
                count_if(lower_text("duplicate_status").isin("conflict", "conflicting", "duplicate"),
                         "conflicting_reported_record_count")))

    slice_cols = sorted({c for spec in SLICES.values() for c in (spec[0], spec[2])})
    reconciled = read("reconciled_stop_calls", [*DAY, "reconciliation_status", "planned_call_id", "et_canonical_id",
                                                "resolved_line_id", "line_ref", *slice_cols])
    rec_status = F.col("reconciliation_status")
    planned_line, reported_line = F.col("resolved_line_id"), F.col("line_ref")
    matching = reconciled.groupBy(*DAY).agg(
        F.countDistinct(F.when(rec_status == "matched", F.col("planned_call_id"))).cast("long")
        .alias("matched_coverage_eligible_planned_call_count"),
        F.countDistinct(F.when(rec_status == "matched", F.col("et_canonical_id"))).cast("long")
        .alias("matched_reported_call_count"),
        *(count_if(rec_status == s, f"{s}_call_count") for s in
          ["planned_only", "reported_only", "ambiguous_planned", "ambiguous_reported"]),
        count_if(F.col("planned_call_id").isNull(), "plan_unresolved_call_count"),
        count_if(planned_line.isNotNull() & reported_line.isNotNull() & (planned_line != reported_line),
                 "line_mismatch_count"),
        count_if(planned_line.isNull() & reported_line.isNull(), "unresolved_label_count"))

    out = (authority_df(spark, inventory, "silver_gate_status", "siri_file_available")
           .join(planned, on=DAY, how="left").join(calls, on=DAY, how="left")
           .join(siri, on=DAY, how="left").join(matching, on=DAY, how="left"))
    for c in ["scheduled_journey_count", "calendar_unresolved_journey_count", "planned_call_count",
              "coverage_eligible_planned_call_count", "matched_coverage_eligible_planned_call_count",
              "canonical_reported_call_count", "raw_reported_row_count", "matched_reported_call_count",
              "unkeyed_reported_call_count", "planned_only_call_count", "reported_only_call_count",
              "ambiguous_planned_call_count", "ambiguous_reported_call_count", "plan_unresolved_call_count",
              "conflicting_reported_record_count", "line_mismatch_count", "unresolved_label_count"]:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(0).cast("long")))

    # metric_populations: per (baseline, event) how many events were measured or excluded, and why.
    no_reasons = F.array().cast("array<string>")
    populations = []
    for (baseline, event), (elig_col, _, excl_col, _) in SLICES.items():
        eligible = F.coalesce(F.col(elig_col).cast("boolean"), F.lit(False))
        reasons = F.coalesce(F.col(excl_col), no_reasons)
        rows = reconciled.select(*DAY, F.lit(baseline).alias("baseline"), F.lit(event).alias("event_type"),
                                 eligible.alias("_eligible"),
                                 F.when(~eligible, reasons).otherwise(no_reasons).alias("_reasons"),
                                 (~eligible & (F.size(reasons) == 0)).alias("_no_reason"))
        keys = [*DAY, "baseline", "event_type"]
        reason_counts = (rows.withColumn("reason_code", F.explode_outer("_reasons")).filter(F.col("reason_code").isNotNull())
                         .groupBy(*keys, "reason_code").agg(F.count(F.lit(1)).cast("long").alias("event_count"))
                         .groupBy(*keys).agg(F.collect_list(F.struct(F.col("reason_code").cast("string").alias("reason_code"),
                                                                     F.col("event_count"))).alias("exclusion_reason_counts")))
        populations.append(
            rows.groupBy(*keys).agg(F.count(F.lit(1)).cast("long").alias("population_event_count"),
                                    count_if(F.col("_eligible"), "eligible_event_count"),
                                    count_if(~F.col("_eligible"), "excluded_event_count"),
                                    count_if(F.col("_no_reason"), "no_supplied_reason_count"))
            .join(reason_counts, on=keys, how="left")
            .select(*DAY, F.struct(
                F.col("baseline").cast("string").alias("baseline"), F.col("event_type").cast("string").alias("event_type"),
                "population_event_count", "eligible_event_count", "excluded_event_count", "no_supplied_reason_count",
                F.coalesce("exclusion_reason_counts", F.array().cast(
                    "array<struct<reason_code:string,event_count:bigint>>")).alias("exclusion_reason_counts"),
            ).alias("_population")))
    by_day = populations[0]
    for df in populations[1:]:
        by_day = by_day.unionByName(df)
    by_day = by_day.groupBy(*DAY).agg(F.collect_list("_population").alias("metric_populations"))

    out = (out.join(by_day, on=DAY, how="left")
           .withColumn("planned_call_matching_coverage",
                       ratio("matched_coverage_eligible_planned_call_count", "coverage_eligible_planned_call_count"))
           .withColumn("reported_call_matching_coverage",
                       ratio("matched_reported_call_count", "canonical_reported_call_count")))
    return add_version_columns(out, gold_run_id, ("gold_run_id", "gold_schema_version", "metric_policy_version"))


def run_gold_gates(inventory: SilverInventory, tables: dict[str, DataFrame]) -> tuple[list[str], dict]:
    """Structural checks that must pass before publishing. Returns (hard failures, check values)."""
    failures: list[str] = []
    expected = set(inventory.expected_dates)
    if expected != {a.operating_date for a in inventory.authorities} or len(inventory.authorities) != len(expected):
        failures.append(f"Date authority mismatch: expected {len(expected)} dates, resolved {len(inventory.authorities)}.")

    quality, trips, network = tables["daily_data_quality"], tables["journey_performance"], tables["daily_network_punctuality"]
    quality_dates = {str(r["operating_date"]) for r in quality.select("operating_date").collect()}
    trip_keys = trips.select("provider", "operating_date", "journey_key").dropDuplicates().count()
    checks = {"expected_date_count": len(expected), "daily_data_quality_rows": quality.count(),
              "journey_performance_rows": trips.count(), "journey_performance_distinct_keys": trip_keys,
              "daily_network_punctuality_rows": network.count(),
              "expected_network_punctuality_rows": len(expected) * len(EVENT_TYPES) * len(BASELINES)}
    if checks["daily_data_quality_rows"] != len(expected) or quality_dates != expected:
        failures.append(f"daily_data_quality date set mismatch: rows={checks['daily_data_quality_rows']}, "
                        f"distinct_dates={len(quality_dates)} vs expected={len(expected)}.")
    if checks["journey_performance_rows"] != trip_keys:
        failures.append(f"journey_performance grain duplicate detected: rows={checks['journey_performance_rows']} "
                        f"vs distinct_keys={trip_keys}.")
    if checks["daily_network_punctuality_rows"] != checks["expected_network_punctuality_rows"]:
        failures.append(f"daily_network_punctuality row count mismatch: got {checks['daily_network_punctuality_rows']}, "
                        f"expected {checks['expected_network_punctuality_rows']}.")

    # Every measured event is early, on time or late (arrivals: on time or late); severe is a subset of late.
    c = F.col
    for message, bad in [
        ("Departure category conservation failed on {} network rows.",
         (c("event_type") == "departure") & (c("early_count") + c("within_window_count") + c("late_count") != c("eligible_event_count"))),
        ("Arrival category conservation failed on {} network rows.",
         (c("event_type") == "arrival") & (c("within_window_count") + c("late_count") != c("eligible_event_count"))),
        ("severe_delay_count > late_count on {} network rows.", c("severe_delay_count") > c("late_count")),
        (f"Timestamp identity check (> {TIMESTAMP_IDENTITY_TOLERANCE_SEC}s) failed on {{}} network rows.",
         (c("baseline") == "netex_planned") & (c("timestamp_identity_failed_count") > 0)),
    ]:
        n = network.filter(bad).count()
        if n > 0:
            failures.append(message.format(n))

    # Network, line and stop tables must contain the same eligible events.
    totals = {name: int(tables[f"daily_{name}_punctuality"].agg(F.sum("eligible_event_count")).first()[0] or 0)
              for name in ("network", "line", "stop")}
    checks["reconciliation_eligible_totals"] = totals
    if len(set(totals.values())) != 1:
        failures.append(f"Eligible event count mismatch across network ({totals['network']}), line ({totals['line']}), "
                        f"and stop ({totals['stop']}).")
    return failures, checks


def _git_revision(repo_root: Path) -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=str(repo_root),
                                       stderr=subprocess.DEVNULL, text=True).strip() or "unknown"
    except Exception:
        return "unknown"


def _tree_fingerprint(path: Path) -> str:
    """Hash of every file's relative path and size."""
    h = hashlib.sha256()
    for p in sorted(path.rglob("*")):
        if p.is_file():
            h.update(str(p.relative_to(path)).encode("utf-8"))
            h.update(str(p.stat().st_size).encode("utf-8"))
    return h.hexdigest()


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def publish_gold_outputs(*, spark: SparkSession, inventory: SilverInventory, tables: dict[str, DataFrame],
                         gold_run_id: str, gold_runs_dir: str | Path, repo_root: Path, started_at_utc: str,
                         percentile_accuracy: int = DEFAULT_PERCENTILE_ACCURACY,
                         shuffle_partitions: int = DEFAULT_SHUFFLE_PARTITIONS) -> Path:
    """Run the gate, write the 9 tables, reports and manifest to a staging folder, then rename it into place."""
    missing = [name for name in GOLD_TABLES if name not in tables]
    if missing:
        raise KeyError(f"Missing required Gold tables prior to publication: {missing}")
    failures, checks = run_gold_gates(inventory, tables)
    gate = "failed" if failures else "passed"

    runs_root = Path(gold_runs_dir)
    final_dir, staged_dir = runs_root / gold_run_id, runs_root / f".staging-{gold_run_id}"
    shutil.rmtree(staged_dir, ignore_errors=True)
    (staged_dir / "quality").mkdir(parents=True)

    table_meta = {}
    for name in GOLD_TABLES:
        tables[name].write.mode("overwrite").parquet(str(staged_dir / name))
        rows = spark.read.parquet(str(staged_dir / name)).count()
        table_meta[name] = {"path": str(final_dir / name), "row_count": int(rows), "columns": tables[name].columns}

    _write_json(staged_dir / "quality" / "input_inventory.json", {
        "provider": inventory.provider, "expected_dates": list(inventory.expected_dates),
        "excluded_runs": list(inventory.excluded_runs), "authorities": [asdict(a) for a in inventory.authorities]})
    _write_json(staged_dir / "quality" / "quality_report.json", {
        "gold_run_id": gold_run_id, "gate_status": gate, "hard_failures": failures,
        "quality_notes": QUALITY_NOTES, "checks": checks})
    _write_json(staged_dir / "gold_manifest.json", {
        "status": "complete" if gate == "passed" else "failed",
        "gold_run_id": gold_run_id,
        "gold_schema_version": GOLD_SCHEMA_VERSION,
        "metric_policy_version": METRIC_POLICY_VERSION,
        "started_at_utc": started_at_utc,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "provider": inventory.provider,
        "coverage_kind": "sampled_operating_dates",
        "expected_operating_dates": list(inventory.expected_dates),
        "published_operating_dates": list(inventory.expected_dates),
        "season_windows_2026": SEASON_WINDOWS_2026,
        "date_authority": {a.operating_date: {"source_silver_run_id": a.source_silver_run_id,
                                              "publication_id": a.publication_id,
                                              "silver_manifest_sha256": a.silver_manifest_sha256}
                           for a in inventory.authorities},
        "metric_policy": {
            "timezone": DEFAULT_TIMEZONE,
            "departure_early_max_exclusive_sec": DEPARTURE_EARLY_MAX_EXCLUSIVE_SEC,
            "departure_within_window_max_sec": DEPARTURE_WITHIN_WINDOW_MAX_SEC,
            "arrival_within_limit_max_sec": ARRIVAL_WITHIN_LIMIT_MAX_SEC,
            "severe_delay_min_exclusive_sec": SEVERE_DELAY_MIN_EXCLUSIVE_SEC,
            "timestamp_identity_tolerance_sec": TIMESTAMP_IDENTITY_TOLERANCE_SEC,
            "business_agreement_tolerance_sec": BUSINESS_AGREEMENT_TOLERANCE_SEC,
        },
        "percentile_method": {"function": "percentile_approx", "accuracy": int(percentile_accuracy)},
        "execution": {"spark_master": spark.sparkContext.master, "shuffle_partitions": int(shuffle_partitions),
                      "auto_broadcast_join_threshold": -1, "session_timezone": "UTC"},
        "code_revision": _git_revision(repo_root),
        "output_tables": table_meta,
        "gate_status": gate,
        "hard_failures": failures,
        "quality_notes": QUALITY_NOTES,
        "quality_report_path": str(final_dir / "quality" / "quality_report.json"),
        "artifact_tree_fingerprint": _tree_fingerprint(staged_dir),
    })

    if failures:
        raise RuntimeError(f"Gold quality gate failed for run '{gold_run_id}'; staged output kept at {staged_dir}: {failures}")
    shutil.rmtree(final_dir, ignore_errors=True)
    staged_dir.rename(final_dir)
    return final_dir
