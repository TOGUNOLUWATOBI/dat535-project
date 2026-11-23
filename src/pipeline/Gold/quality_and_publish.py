"""Daily data quality, structural gates, manifests, and atomic publication for Gold."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from pipeline.Gold import (
    ARRIVAL_WITHIN_LIMIT_MAX_SEC,
    BASELINES,
    BUSINESS_AGREEMENT_TOLERANCE_SEC,
    DEFAULT_PERCENTILE_ACCURACY,
    DEFAULT_PROVIDER,
    DEFAULT_SHUFFLE_PARTITIONS,
    DEFAULT_TIMEZONE,
    DEPARTURE_EARLY_MAX_EXCLUSIVE_SEC,
    DEPARTURE_WITHIN_WINDOW_MAX_SEC,
    EVENT_TYPES,
    GOLD_SCHEMA_VERSION,
    GOLD_TABLES,
    METRIC_POLICY_VERSION,
    SEASON_WINDOWS_2026,
    SEVERE_DELAY_MIN_EXCLUSIVE_SEC,
    TIMESTAMP_IDENTITY_TOLERANCE_SEC,
)
from pipeline.Gold.event_metrics import normalize_journey_key_expr
from pipeline.Gold.inputs import SilverInventory, read_silver_table_for_inventory


@dataclass
class GateResult:
    passed: bool
    hard_failures: list[str]
    quality_notes: list[str]
    checks: dict[str, Any]


def _git_revision(repo_root: Path) -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(repo_root),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out or "unknown"
    except Exception:
        return "unknown"


def _dir_sha256_summary(path: Path) -> str:
    h = hashlib.sha256()
    if not path.exists():
        return ""
    for p in sorted(path.rglob("*")):
        if p.is_file():
            h.update(str(p.relative_to(path)).encode("utf-8"))
            h.update(str(p.stat().st_size).encode("utf-8"))
    return h.hexdigest()


def build_daily_data_quality(
    spark: SparkSession,
    inventory: SilverInventory,
    journey_perf_df: DataFrame,
    *,
    gold_run_id: str,
) -> DataFrame:
    """Build dense daily_data_quality table (1 row per provider + operating_date)."""
    jsd_df = read_silver_table_for_inventory(
        spark,
        inventory,
        table_name="journey_service_dates",
        columns=["provider", "operating_date", "publication_id", "service_status"],
    )
    nsc_df = read_silver_table_for_inventory(
        spark,
        inventory,
        table_name="netex_stop_calls",
        columns=["provider", "operating_date", "publication_id", "passing_time_id"],
    ).withColumn("planned_call_id", F.col("passing_time_id"))
    siri_raw = read_silver_table_for_inventory(
        spark,
        inventory,
        table_name="siri_et_stop_calls",
        columns=[
            "provider",
            "et_canonical_id",
            "service_journey_id",
            "dated_service_journey_id",
            "duplicate_status",
        ],
    ).withColumn("journey_key", normalize_journey_key_expr("dated_service_journey_id", "service_journey_id"))
    rec_date_map = read_silver_table_for_inventory(
        spark,
        inventory,
        table_name="reconciled_stop_calls",
        columns=["provider", "operating_date", "publication_id", "et_canonical_id"],
    ).dropDuplicates(["provider", "et_canonical_id"])
    siri_df = siri_raw.join(rec_date_map, on=["provider", "et_canonical_id"], how="inner").withColumn(
        "has_conflicting_variants",
        F.lower(F.coalesce(F.col("duplicate_status").cast("string"), F.lit(""))).isin(
            "conflict", "conflicting", "duplicate"
        ),
    )

    rec_df = read_silver_table_for_inventory(
        spark,
        inventory,
        table_name="reconciled_stop_calls",
        columns=[
            "provider",
            "operating_date",
            "publication_id",
            "reconciliation_status",
            "planned_call_id",
            "et_canonical_id",
            "resolved_line_id",
            "line_ref",
            "eligible_reported_departure_delay",
            "exclusion_reported_departure_delay",
            "eligible_reported_arrival_delay",
            "exclusion_reported_arrival_delay",
            "eligible_netex_departure_comparison",
            "exclusion_netex_departure_comparison",
            "eligible_netex_arrival_comparison",
            "exclusion_netex_arrival_comparison",
        ],
    )

    jsd_agg = jsd_df.groupBy("provider", "operating_date", "publication_id").agg(
        F.sum(
            F.when(
                F.lower(F.coalesce(F.col("service_status").cast("string"), F.lit(""))).isin(
                    "scheduled", "active", "operating"
                ),
                F.lit(1),
            ).otherwise(F.lit(0))
        )
        .cast("long")
        .alias("scheduled_journey_count"),
        F.sum(
            F.when(
                ~F.lower(F.coalesce(F.col("service_status").cast("string"), F.lit(""))).isin(
                    "scheduled", "active", "operating", "not_scheduled", "inactive"
                ),
                F.lit(1),
            ).otherwise(F.lit(0))
        )
        .cast("long")
        .alias("calendar_unresolved_journey_count"),
    )

    nsc_agg = nsc_df.groupBy("provider", "operating_date", "publication_id").agg(
        F.count(F.lit(1)).cast("long").alias("planned_call_count"),
        F.countDistinct("planned_call_id").cast("long").alias("coverage_eligible_planned_call_count"),
    )

    siri_agg = siri_df.groupBy("provider", "operating_date", "publication_id").agg(
        F.count(F.lit(1)).cast("long").alias("raw_reported_row_count"),
        F.countDistinct("et_canonical_id").cast("long").alias("canonical_reported_call_count"),
        F.sum(F.when(F.col("journey_key").isNull(), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("unkeyed_reported_call_count"),
        F.sum(F.when(F.coalesce(F.col("has_conflicting_variants"), F.lit(False)), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("conflicting_reported_record_count"),
    )

    rec_agg = rec_df.groupBy("provider", "operating_date", "publication_id").agg(
        F.countDistinct(
            F.when(F.col("reconciliation_status") == F.lit("matched"), F.col("planned_call_id"))
        )
        .cast("long")
        .alias("matched_coverage_eligible_planned_call_count"),
        F.countDistinct(
            F.when(F.col("reconciliation_status") == F.lit("matched"), F.col("et_canonical_id"))
        )
        .cast("long")
        .alias("matched_reported_call_count"),
        F.sum(F.when(F.col("reconciliation_status") == F.lit("planned_only"), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("planned_only_call_count"),
        F.sum(F.when(F.col("reconciliation_status") == F.lit("reported_only"), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("reported_only_call_count"),
        F.sum(F.when(F.col("reconciliation_status") == F.lit("ambiguous_planned"), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("ambiguous_planned_call_count"),
        F.sum(F.when(F.col("reconciliation_status") == F.lit("ambiguous_reported"), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("ambiguous_reported_call_count"),
        F.sum(F.when(F.col("planned_call_id").isNull(), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("plan_unresolved_call_count"),
        F.sum(
            F.when(
                F.col("resolved_line_id").isNotNull()
                & F.col("line_ref").isNotNull()
                & (F.col("resolved_line_id") != F.col("line_ref")),
                F.lit(1),
            ).otherwise(F.lit(0))
        )
        .cast("long")
        .alias("line_mismatch_count"),
        F.sum(
            F.when(
                F.col("resolved_line_id").isNull() & F.col("line_ref").isNull(),
                F.lit(1),
            ).otherwise(F.lit(0))
        )
        .cast("long")
        .alias("unresolved_label_count"),
    )

    dense_dates = spark.createDataFrame(
        [
            (
                a.provider,
                a.operating_date,
                a.source_silver_run_id,
                a.publication_id,
                a.silver_gate_status,
                bool(a.siri_file_available),
            )
            for a in inventory.authorities
        ],
        schema=T.StructType(
            [
                T.StructField("provider", T.StringType(), False),
                T.StructField("operating_date_str", T.StringType(), False),
                T.StructField("source_silver_run_id", T.StringType(), False),
                T.StructField("publication_id", T.StringType(), False),
                T.StructField("silver_gate_status", T.StringType(), False),
                T.StructField("siri_file_available", T.BooleanType(), False),
            ]
        ),
    ).withColumn("operating_date", F.to_date("operating_date_str")).drop("operating_date_str")

    joined = (
        dense_dates.join(jsd_agg, on=["provider", "operating_date", "publication_id"], how="left")
        .join(nsc_agg, on=["provider", "operating_date", "publication_id"], how="left")
        .join(siri_agg, on=["provider", "operating_date", "publication_id"], how="left")
        .join(rec_agg, on=["provider", "operating_date", "publication_id"], how="left")
    )

    long_cols = [
        "scheduled_journey_count",
        "calendar_unresolved_journey_count",
        "planned_call_count",
        "coverage_eligible_planned_call_count",
        "matched_coverage_eligible_planned_call_count",
        "canonical_reported_call_count",
        "raw_reported_row_count",
        "matched_reported_call_count",
        "unkeyed_reported_call_count",
        "planned_only_call_count",
        "reported_only_call_count",
        "ambiguous_planned_call_count",
        "ambiguous_reported_call_count",
        "plan_unresolved_call_count",
        "conflicting_reported_record_count",
        "line_mismatch_count",
        "unresolved_label_count",
    ]
    out = joined
    for c in long_cols:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(0).cast("long")))

    # Summarize per-date exclusion populations including no_supplied_reason_count
    slice_specs = [
        ("et_aimed", "departure", "eligible_reported_departure_delay", "exclusion_reported_departure_delay"),
        ("et_aimed", "arrival", "eligible_reported_arrival_delay", "exclusion_reported_arrival_delay"),
        ("netex_planned", "departure", "eligible_netex_departure_comparison", "exclusion_netex_departure_comparison"),
        ("netex_planned", "arrival", "eligible_netex_arrival_comparison", "exclusion_netex_arrival_comparison"),
    ]

    per_date_slices: list[DataFrame] = []
    for bl, ev, elig_c, excl_c in slice_specs:
        elig_b = F.coalesce(F.col(elig_c).cast("boolean"), F.lit(False))
        excl_arr = F.coalesce(F.col(excl_c), F.array().cast("array<string>"))
        is_excl = ~elig_b
        no_reason = is_excl & (F.size(excl_arr) == 0)

        base_s = rec_df.select(
            "provider",
            "operating_date",
            "publication_id",
            F.lit(bl).alias("baseline"),
            F.lit(ev).alias("event_type"),
            F.when(elig_b, F.lit(1)).otherwise(F.lit(0)).alias("_elig"),
            F.when(is_excl, F.lit(1)).otherwise(F.lit(0)).alias("_excl"),
            F.when(no_reason, F.lit(1)).otherwise(F.lit(0)).alias("_no_reason"),
            F.when(is_excl, excl_arr).otherwise(F.array().cast("array<string>")).alias("_reasons"),
        )

        # Reason counts per (provider, operating_date, publication_id, baseline, event_type, reason_code)
        exploded_reasons = (
            base_s.withColumn("reason_code", F.explode_outer("_reasons"))
            .filter(F.col("reason_code").isNotNull())
            .groupBy("provider", "operating_date", "publication_id", "baseline", "event_type", "reason_code")
            .agg(F.count(F.lit(1)).cast("long").alias("event_count"))
            .groupBy("provider", "operating_date", "publication_id", "baseline", "event_type")
            .agg(
                F.collect_list(
                    F.struct(
                        F.col("reason_code").cast("string").alias("reason_code"),
                        F.col("event_count").cast("long").alias("event_count"),
                    )
                ).alias("exclusion_reason_counts")
            )
        )

        slice_agg = (
            base_s.groupBy("provider", "operating_date", "publication_id", "baseline", "event_type")
            .agg(
                F.count(F.lit(1)).cast("long").alias("population_event_count"),
                F.sum("_elig").cast("long").alias("eligible_event_count"),
                F.sum("_excl").cast("long").alias("excluded_event_count"),
                F.sum("_no_reason").cast("long").alias("no_supplied_reason_count"),
            )
            .join(
                exploded_reasons,
                on=["provider", "operating_date", "publication_id", "baseline", "event_type"],
                how="left",
            )
            .withColumn(
                "exclusion_reason_counts",
                F.coalesce(
                    F.col("exclusion_reason_counts"),
                    F.array().cast("array<struct<reason_code:string,event_count:bigint>>"),
                ),
            )
            .select(
                "provider",
                "operating_date",
                "publication_id",
                F.struct(
                    F.col("baseline").cast("string").alias("baseline"),
                    F.col("event_type").cast("string").alias("event_type"),
                    F.col("population_event_count"),
                    F.col("eligible_event_count"),
                    F.col("excluded_event_count"),
                    F.col("no_supplied_reason_count"),
                    F.col("exclusion_reason_counts"),
                ).alias("_mp_struct"),
            )
        )
        per_date_slices.append(slice_agg)

    all_slices = per_date_slices[0]
    for nxt in per_date_slices[1:]:
        all_slices = all_slices.unionByName(nxt)

    mp_by_date = all_slices.groupBy("provider", "operating_date", "publication_id").agg(
        F.collect_list("_mp_struct").alias("metric_populations")
    )

    return (
        out.join(mp_by_date, on=["provider", "operating_date", "publication_id"], how="left")
        .withColumn(
            "planned_call_matching_coverage",
            F.when(
                F.col("coverage_eligible_planned_call_count") > 0,
                F.col("matched_coverage_eligible_planned_call_count").cast("double")
                / F.col("coverage_eligible_planned_call_count").cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "reported_call_matching_coverage",
            F.when(
                F.col("canonical_reported_call_count") > 0,
                F.col("matched_reported_call_count").cast("double")
                / F.col("canonical_reported_call_count").cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("gold_run_id", F.lit(gold_run_id).cast("string"))
        .withColumn("gold_schema_version", F.lit(GOLD_SCHEMA_VERSION).cast("string"))
        .withColumn("metric_policy_version", F.lit(METRIC_POLICY_VERSION).cast("string"))
    )


def run_gold_gates(
    inventory: SilverInventory,
    tables: dict[str, DataFrame],
) -> GateResult:
    """Execute structural Gold gates before atomic publication."""
    hard_failures: list[str] = []
    quality_notes: list[str] = []
    checks: dict[str, Any] = {}

    expected_dates = set(inventory.expected_dates)
    auth_dates = {a.operating_date for a in inventory.authorities}
    if expected_dates != auth_dates or len(inventory.authorities) != len(expected_dates):
        hard_failures.append(
            f"Date authority mismatch: expected {len(expected_dates)} dates, resolved {len(auth_dates)}."
        )
    checks["expected_date_count"] = len(expected_dates)

    # Check 1: daily_data_quality dense date set & population conservation
    dq_df = tables["daily_data_quality"]
    dq_dates = {str(r["operating_date"]) for r in dq_df.select("operating_date").collect()}
    dq_count = dq_df.count()
    checks["daily_data_quality_rows"] = dq_count
    if dq_count != len(expected_dates) or dq_dates != expected_dates:
        hard_failures.append(
            f"daily_data_quality date set mismatch: rows={dq_count}, distinct_dates={len(dq_dates)} vs expected={len(expected_dates)}."
        )

    # Check 2: journey_performance grain uniqueness
    jp_df = tables["journey_performance"]
    jp_total = jp_df.count()
    jp_distinct = jp_df.select("provider", "operating_date", "journey_key").dropDuplicates().count()
    checks["journey_performance_rows"] = jp_total
    checks["journey_performance_distinct_keys"] = jp_distinct
    if jp_total != jp_distinct:
        hard_failures.append(
            f"journey_performance grain duplicate detected: rows={jp_total} vs distinct_keys={jp_distinct}."
        )

    # Check 3: daily_network_punctuality exact count (4 rows per date) & conservation rules
    net_df = tables["daily_network_punctuality"]
    net_count = net_df.count()
    expected_net = len(expected_dates) * len(EVENT_TYPES) * len(BASELINES)
    checks["daily_network_punctuality_rows"] = net_count
    checks["expected_network_punctuality_rows"] = expected_net
    if net_count != expected_net:
        hard_failures.append(
            f"daily_network_punctuality row count mismatch: got {net_count}, expected {expected_net}."
        )

    # Conservation and severe <= late checks on network punctuality
    bad_dep_sum = net_df.filter(
        (F.col("event_type") == F.lit("departure"))
        & ((F.col("early_count") + F.col("within_window_count") + F.col("late_count")) != F.col("eligible_event_count"))
    ).count()
    bad_arr_sum = net_df.filter(
        (F.col("event_type") == F.lit("arrival"))
        & ((F.col("within_window_count") + F.col("late_count")) != F.col("eligible_event_count"))
    ).count()
    bad_severe = net_df.filter(F.col("severe_delay_count") > F.col("late_count")).count()
    bad_ts_id = net_df.filter(
        (F.col("baseline") == F.lit("netex_planned")) & (F.col("timestamp_identity_failed_count") > 0)
    ).count()

    if bad_dep_sum > 0:
        hard_failures.append(f"Departure category conservation failed on {bad_dep_sum} network rows.")
    if bad_arr_sum > 0:
        hard_failures.append(f"Arrival category conservation failed on {bad_arr_sum} network rows.")
    if bad_severe > 0:
        hard_failures.append(f"severe_delay_count > late_count on {bad_severe} network rows.")
    if bad_ts_id > 0:
        hard_failures.append(f"Timestamp identity check (> {TIMESTAMP_IDENTITY_TOLERANCE_SEC}s) failed on {bad_ts_id} network rows.")

    # Check 4: line and stop eligible counts reconcile to network totals
    line_df = tables["daily_line_punctuality"]
    stop_df = tables["daily_stop_punctuality"]
    net_elig = net_df.agg(F.sum("eligible_event_count").alias("s")).collect()[0]["s"] or 0
    line_elig = line_df.agg(F.sum("eligible_event_count").alias("s")).collect()[0]["s"] or 0
    stop_elig = stop_df.agg(F.sum("eligible_event_count").alias("s")).collect()[0]["s"] or 0
    checks["reconciliation_eligible_totals"] = {
        "network": int(net_elig),
        "line": int(line_elig),
        "stop": int(stop_elig),
    }
    if not (int(net_elig) == int(line_elig) == int(stop_elig)):
        hard_failures.append(
            f"Eligible event count mismatch across network ({net_elig}), line ({line_elig}), and stop ({stop_elig})."
        )

    # Non-blocking quality notes
    quality_notes.extend(
        [
            "Analysis covers 53 selected operating dates across Jan-Nov 2026 (sampled cadence), not every calendar day.",
            "winter-2026 is defined as January-February 2026 within this study scope.",
            "Weekday composition varies across months and seasons; descriptive differences do not prove weather or causal effects.",
        ]
    )

    return GateResult(
        passed=len(hard_failures) == 0,
        hard_failures=hard_failures,
        quality_notes=quality_notes,
        checks=checks,
    )


def publish_gold_outputs(
    *,
    spark: SparkSession,
    inventory: SilverInventory,
    tables: dict[str, DataFrame],
    gold_run_id: str,
    gold_runs_dir: str | Path,
    repo_root: Path,
    started_at_utc: str,
    percentile_accuracy: int = DEFAULT_PERCENTILE_ACCURACY,
    shuffle_partitions: int = DEFAULT_SHUFFLE_PARTITIONS,
) -> Path:
    """Run gates, write all 9 tables + reports + manifest into a temp staging dir, and atomically publish."""
    for name in GOLD_TABLES:
        if name not in tables:
            raise KeyError(f"Missing required Gold table '{name}' prior to publication.")

    gate_result = run_gold_gates(inventory, tables)
    runs_root = Path(gold_runs_dir)
    runs_root.mkdir(parents=True, exist_ok=True)
    final_dir = runs_root / gold_run_id
    staged_dir = runs_root / f".staging-{gold_run_id}"

    if staged_dir.exists():
        shutil.rmtree(staged_dir)
    staged_dir.mkdir(parents=True, exist_ok=True)
    (staged_dir / "quality").mkdir(parents=True, exist_ok=True)

    table_meta: dict[str, Any] = {}
    for table_name in GOLD_TABLES:
        df = tables[table_name]
        out_path = staged_dir / table_name
        df.write.mode("overwrite").parquet(str(out_path))
        row_cnt = spark.read.parquet(str(out_path)).count()
        table_meta[table_name] = {
            "path": str(final_dir / table_name),
            "row_count": int(row_cnt),
            "columns": df.columns,
        }

    inventory_payload = {
        "provider": inventory.provider,
        "expected_dates": list(inventory.expected_dates),
        "excluded_runs": list(inventory.excluded_runs),
        "authorities": [
            {
                "provider": a.provider,
                "operating_date": a.operating_date,
                "source_silver_run_id": a.source_silver_run_id,
                "silver_manifest_path": a.silver_manifest_path,
                "silver_manifest_sha256": a.silver_manifest_sha256,
                "publication_id": a.publication_id,
                "silver_gate_status": a.silver_gate_status,
                "siri_file_available": a.siri_file_available,
            }
            for a in inventory.authorities
        ],
    }
    inv_path = staged_dir / "quality" / "input_inventory.json"
    inv_path.write_text(json.dumps(inventory_payload, indent=2), encoding="utf-8")

    report_payload = {
        "gold_run_id": gold_run_id,
        "gate_status": "passed" if gate_result.passed else "failed",
        "hard_failures": gate_result.hard_failures,
        "quality_notes": gate_result.quality_notes,
        "checks": gate_result.checks,
    }
    q_report_path = staged_dir / "quality" / "quality_report.json"
    q_report_path.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")

    completed_at_utc = datetime.now(timezone.utc).isoformat()
    manifest_payload = {
        "status": "complete" if gate_result.passed else "failed",
        "gold_run_id": gold_run_id,
        "gold_schema_version": GOLD_SCHEMA_VERSION,
        "metric_policy_version": METRIC_POLICY_VERSION,
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
        "provider": inventory.provider,
        "coverage_kind": "sampled_operating_dates",
        "expected_operating_dates": list(inventory.expected_dates),
        "published_operating_dates": list(inventory.expected_dates),
        "season_windows_2026": SEASON_WINDOWS_2026,
        "date_authority": {
            a.operating_date: {
                "source_silver_run_id": a.source_silver_run_id,
                "publication_id": a.publication_id,
                "silver_manifest_sha256": a.silver_manifest_sha256,
            }
            for a in inventory.authorities
        },
        "metric_policy": {
            "timezone": DEFAULT_TIMEZONE,
            "departure_early_max_exclusive_sec": DEPARTURE_EARLY_MAX_EXCLUSIVE_SEC,
            "departure_within_window_max_sec": DEPARTURE_WITHIN_WINDOW_MAX_SEC,
            "arrival_within_limit_max_sec": ARRIVAL_WITHIN_LIMIT_MAX_SEC,
            "severe_delay_min_exclusive_sec": SEVERE_DELAY_MIN_EXCLUSIVE_SEC,
            "timestamp_identity_tolerance_sec": TIMESTAMP_IDENTITY_TOLERANCE_SEC,
            "business_agreement_tolerance_sec": BUSINESS_AGREEMENT_TOLERANCE_SEC,
        },
        "percentile_method": {
            "function": "percentile_approx",
            "accuracy": int(percentile_accuracy),
        },
        "execution": {
            "spark_master": spark.sparkContext.master,
            "shuffle_partitions": int(shuffle_partitions),
            "auto_broadcast_join_threshold": -1,
            "session_timezone": "UTC",
        },
        "code_revision": _git_revision(repo_root),
        "output_tables": table_meta,
        "gate_status": "passed" if gate_result.passed else "failed",
        "hard_failures": gate_result.hard_failures,
        "quality_notes": gate_result.quality_notes,
        "quality_report_path": str(final_dir / "quality" / "quality_report.json"),
        "artifact_tree_fingerprint": _dir_sha256_summary(staged_dir),
    }
    manifest_path = staged_dir / "gold_manifest.json"
    manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")

    if not gate_result.passed:
        raise RuntimeError(
            f"Gold quality gate failed for run '{gold_run_id}'; staged output kept at {staged_dir}: "
            f"{gate_result.hard_failures}"
        )

    if final_dir.exists():
        shutil.rmtree(final_dir)
    staged_dir.rename(final_dir)
    return final_dir
