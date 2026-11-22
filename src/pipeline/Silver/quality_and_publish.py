"""Quality reporting, verification gates, and atomic publication for Silver."""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import SILVER_TABLES, SILVER_VERSION
from .input_selection import InputSelectionResult, write_selection_report

DATED_SILVER_TABLES: frozenset[str] = frozenset(
    {
        "service_calendar",
        "journey_service_dates",
        "netex_stop_calls",
        "siri_et_stop_calls",
        "reconciled_stop_calls",
    }
)


def _prepare_tmp_dir(output_dir: Path) -> Path:
    """Create a fresh sibling directory owned by this run before building tables."""
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(
            f"Target Silver run directory already exists and is non-empty: {output_dir}"
        )
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = output_dir.parent / f".{output_dir.name}.tmp.{os.getpid()}"
    tmp_dir.mkdir()
    return tmp_dir


def _write_and_read_table(
    spark: SparkSession,
    tmp_dir: Path,
    tbl_name: str,
    df: DataFrame,
    unpersist_source: bool = True,
) -> tuple[DataFrame, int, list[dict[str, Any]]]:
    """Write once, verify the Parquet read-back, and release the completed source."""
    tbl_path = tmp_dir / tbl_name
    writer = df.write.mode("errorifexists")
    if tbl_name in DATED_SILVER_TABLES and "operating_date" in df.columns:
        writer = writer.partitionBy("operating_date")
    writer.parquet(str(tbl_path))

    # Empty partitioned outputs have no data-file footer from which to infer a schema.
    written_df = spark.read.schema(df.schema).parquet(str(tbl_path))
    row_count = written_df.count()
    schema_meta = [
        {"name": f.name, "type": f.dataType.simpleString(), "nullable": f.nullable}
        for f in written_df.schema.fields
    ]
    if unpersist_source and df.is_cached:
        df.unpersist(blocking=True)
    return written_df, row_count, schema_meta


def build_daily_quality_summary(
    tables: dict[str, DataFrame],
    publication_lookup_df: DataFrame,
) -> DataFrame:
    """Aggregate per-(provider, operating_date) quality and population counts."""
    base = publication_lookup_df.select(
        "provider",
        "publication_id",
        "snapshot_date",
        F.col("operating_date").cast("date").alias("operating_date"),
        "staging_run",
        "siri_file_available",
        "selection_status",
    ).dropDuplicates()

    cal_df = tables["service_calendar"]
    cal_agg = cal_df.groupBy("provider", "publication_id", "operating_date").agg(
        F.count(F.lit(1)).alias("calendar_day_types_evaluated"),
        F.sum(F.when(F.col("calendar_state") == "active", 1).otherwise(0)).alias(
            "calendar_day_types_active"
        ),
        F.sum(F.when(F.col("calendar_state") == "inactive", 1).otherwise(0)).alias(
            "calendar_day_types_inactive"
        ),
        F.sum(F.when(F.col("calendar_state") == "unresolved", 1).otherwise(0)).alias(
            "calendar_day_types_unresolved"
        ),
    )

    jsd_df = tables["journey_service_dates"]
    jsd_agg = jsd_df.groupBy("provider", "publication_id", "operating_date").agg(
        F.count(F.lit(1)).alias("journey_definitions_evaluated"),
        F.sum(F.when(F.col("service_status") == "scheduled", 1).otherwise(0)).alias(
            "journeys_scheduled"
        ),
        F.sum(F.when(F.col("service_status") == "not_scheduled", 1).otherwise(0)).alias(
            "journeys_not_scheduled"
        ),
        F.sum(F.when(F.col("service_status") == "unresolved", 1).otherwise(0)).alias(
            "journeys_calendar_unresolved"
        ),
    )

    nc_df = tables["netex_stop_calls"]
    nc_agg = nc_df.groupBy("provider", "publication_id", "operating_date").agg(
        F.count(F.lit(1)).alias("netex_planned_calls_total"),
        F.sum(F.when(F.col("planned_call_status") == "resolved", 1).otherwise(0)).alias(
            "netex_planned_calls_resolved"
        ),
        F.sum(F.when(F.col("is_flexible_window") == F.lit(True), 1).otherwise(0)).alias(
            "netex_planned_calls_flexible"
        ),
        F.sum(
            F.when(
                ~F.col("planned_call_status").isin("resolved", "flexible_service_window"), 1
            ).otherwise(0)
        ).alias("netex_planned_calls_unresolved"),
    )

    et_df = tables["siri_et_stop_calls"]
    et_agg = et_df.groupBy("provider", "operating_date").agg(
        F.coalesce(F.sum("source_occurrences"), F.lit(0)).alias("siri_et_raw_rows"),
        F.count(F.lit(1)).alias("siri_et_canonical_rows"),
        F.sum(
            F.when(F.col("duplicate_status") == "collapsed_exact_duplicates", 1).otherwise(0)
        ).alias("siri_et_collapsed_duplicate_groups"),
        F.sum(
            F.when(F.col("duplicate_status") == "conflicting_candidate_key", 1).otherwise(0)
        ).alias("siri_et_conflicting_candidate_rows"),
        F.sum(F.when(F.col("departure_time_kind") == "recorded", 1).otherwise(0)).alias(
            "siri_et_recorded_departures"
        ),
        F.sum(F.when(F.col("departure_time_kind") == "estimated", 1).otherwise(0)).alias(
            "siri_et_estimated_departures"
        ),
        F.sum(F.when(F.col("arrival_time_kind") == "recorded", 1).otherwise(0)).alias(
            "siri_et_recorded_arrivals"
        ),
        F.sum(F.when(F.col("arrival_time_kind") == "estimated", 1).otherwise(0)).alias(
            "siri_et_estimated_arrivals"
        ),
    )

    rec_df = tables["reconciled_stop_calls"]
    rec_agg = rec_df.groupBy("provider", "publication_id", "operating_date").agg(
        F.count(F.lit(1)).alias("reconciled_total_rows"),
        F.sum(F.when(F.col("reconciliation_status") == "matched", 1).otherwise(0)).alias(
            "reconciled_matched"
        ),
        F.sum(F.when(F.col("reconciliation_status") == "planned_only", 1).otherwise(0)).alias(
            "reconciled_planned_only"
        ),
        F.sum(F.when(F.col("reconciliation_status") == "reported_only", 1).otherwise(0)).alias(
            "reconciled_reported_only"
        ),
        F.sum(F.when(F.col("reconciliation_status") == "ambiguous", 1).otherwise(0)).alias(
            "reconciled_ambiguous"
        ),
        F.sum(F.when(F.col("reconciliation_status") == "plan_unresolved", 1).otherwise(0)).alias(
            "reconciled_plan_unresolved"
        ),
        F.sum(F.when(F.col("eligible_reported_departure_delay"), 1).otherwise(0)).alias(
            "eligible_reported_departure_delay"
        ),
        F.sum(F.when(F.col("eligible_reported_arrival_delay"), 1).otherwise(0)).alias(
            "eligible_reported_arrival_delay"
        ),
        F.sum(F.when(F.col("eligible_netex_departure_comparison"), 1).otherwise(0)).alias(
            "eligible_netex_departure_comparison"
        ),
        F.sum(F.when(F.col("eligible_netex_arrival_comparison"), 1).otherwise(0)).alias(
            "eligible_netex_arrival_comparison"
        ),
        F.sum(F.when(F.col("eligible_planned_call_coverage"), 1).otherwise(0)).alias(
            "eligible_planned_call_coverage"
        ),
        F.sum(F.when(F.col("eligible_reported_cancellation_analysis"), 1).otherwise(0)).alias(
            "eligible_reported_cancellation_analysis"
        ),
    )

    summary = (
        base.join(cal_agg, on=["provider", "publication_id", "operating_date"], how="left")
        .join(jsd_agg, on=["provider", "publication_id", "operating_date"], how="left")
        .join(nc_agg, on=["provider", "publication_id", "operating_date"], how="left")
        .join(et_agg, on=["provider", "operating_date"], how="left")
        .join(rec_agg, on=["provider", "publication_id", "operating_date"], how="left")
    )

    count_cols = [
        c
        for c in summary.columns
        if c
        not in (
            "provider",
            "publication_id",
            "snapshot_date",
            "operating_date",
            "staging_run",
            "siri_file_available",
            "selection_status",
        )
    ]
    for col_name in count_cols:
        summary = summary.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)).cast("long"))

    return summary.orderBy("operating_date")


def validate_silver_integrity(
    table_counts: dict[str, int],
    selection: InputSelectionResult,
    daily_summary_rows: list[dict[str, Any]],
    written_reconciled_df: DataFrame,
) -> tuple[list[str], list[str]]:
    """Enforce hard structural gates against both planned and reported input populations."""
    hard_failures: list[str] = list(selection.hard_failures)
    quality_notes: list[str] = list(selection.warnings)

    for tbl in SILVER_TABLES:
        if tbl not in table_counts:
            hard_failures.append(f"missing_output_table:{tbl}")

    if selection.records:
        for def_tbl in ("lines", "routes", "journey_patterns", "journey_pattern_stops", "journeys"):
            if table_counts.get(def_tbl, 0) <= 0:
                hard_failures.append(f"empty_definition_table:{def_tbl}")

    written_nc = table_counts.get("netex_stop_calls", 0)
    written_et_canon = table_counts.get("siri_et_stop_calls", 0)
    written_rec = table_counts.get("reconciled_stop_calls", 0)

    daily_nc = sum(int(r.get("netex_planned_calls_total", 0) or 0) for r in daily_summary_rows)
    daily_et_canon = sum(int(r.get("siri_et_canonical_rows", 0) or 0) for r in daily_summary_rows)
    daily_rec = sum(int(r.get("reconciled_total_rows", 0) or 0) for r in daily_summary_rows)

    if daily_nc != written_nc:
        hard_failures.append(
            f"netex_stop_calls_count_mismatch:daily_sum={daily_nc} vs written={written_nc}"
        )
    if daily_et_canon != written_et_canon:
        hard_failures.append(
            f"siri_et_canonical_count_mismatch:daily_sum={daily_et_canon} vs written={written_et_canon}"
        )
    if daily_rec != written_rec:
        hard_failures.append(
            f"reconciled_stop_calls_count_mismatch:daily_sum={daily_rec} vs written={written_rec}"
        )

    # Validate both populations in one action over the written ID columns.
    rec_stats = written_reconciled_df.select("planned_call_id", "et_canonical_id").agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(
            F.when(
                F.col("planned_call_id").isNull() & F.col("et_canonical_id").isNull(), 1
            ).otherwise(0)
        ).alias("orphan_rows"),
        F.count("planned_call_id").alias("total_p_refs"),
        F.countDistinct("planned_call_id").alias("distinct_p_refs"),
        F.count("et_canonical_id").alias("total_e_refs"),
        F.countDistinct("et_canonical_id").alias("distinct_e_refs"),
    ).first()
    if rec_stats["rows"] != written_rec:
        hard_failures.append(
            f"reconciled_written_count_mismatch:verified={rec_stats['rows']} vs written={written_rec}"
        )
    orphan_rows = int(rec_stats["orphan_rows"] or 0)
    total_p_refs = int(rec_stats["total_p_refs"])
    distinct_p_refs = int(rec_stats["distinct_p_refs"])
    total_e_refs = int(rec_stats["total_e_refs"])
    distinct_e_refs = int(rec_stats["distinct_e_refs"])
    if orphan_rows > 0:
        hard_failures.append(f"reconciled_orphan_rows_without_ids:{orphan_rows}")
    if total_p_refs != distinct_p_refs:
        hard_failures.append(
            f"duplicate_planned_call_in_reconciled:rows={total_p_refs} vs distinct={distinct_p_refs}"
        )
    if distinct_p_refs != written_nc:
        hard_failures.append(
            f"planned_call_loss_or_mismatch:reconciled_distinct={distinct_p_refs} vs netex_stop_calls={written_nc}"
        )
    if total_e_refs != distinct_e_refs:
        hard_failures.append(
            f"duplicate_et_call_in_reconciled:rows={total_e_refs} vs distinct={distinct_e_refs}"
        )
    if distinct_e_refs != written_et_canon:
        hard_failures.append(
            f"et_call_loss_or_mismatch:reconciled_distinct={distinct_e_refs} vs siri_et_stop_calls={written_et_canon}"
        )

    total_ambiguous = sum(int(r.get("reconciled_ambiguous", 0) or 0) for r in daily_summary_rows)
    unres_cal = sum(int(r.get("journeys_calendar_unresolved", 0) or 0) for r in daily_summary_rows)
    if unres_cal > 0:
        quality_notes.append(f"journeys_with_unresolved_calendar:{unres_cal}")
    if total_ambiguous > 0:
        quality_notes.append(f"ambiguous_stop_visit_records:{total_ambiguous}")

    conflicting_et = sum(
        int(r.get("siri_et_conflicting_candidate_rows", 0) or 0) for r in daily_summary_rows
    )
    if conflicting_et > 0:
        quality_notes.append(f"siri_et_conflicting_candidate_key_rows:{conflicting_et}")

    if written_et_canon == 0 and any(r.siri_file_available for r in selection.records):
        hard_failures.append("siri_et_zero_canonical_rows_despite_available_files")

    return hard_failures, quality_notes


def write_and_publish_silver(
    spark: SparkSession,
    output_dir: Path,
    tables: dict[str, DataFrame],
    selection: InputSelectionResult,
    publication_lookup_df: DataFrame,
    started_at_utc: str,
    spark_master: str,
    config_meta: dict[str, Any],
    *,
    tmp_dir: Path,
    table_counts: dict[str, int],
    table_schemas: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    """Validate Parquet-backed tables and atomically publish their existing temp directory."""
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(
            f"Target Silver run directory already exists and is non-empty: {output_dir}"
        )

    try:
        for tbl_name in SILVER_TABLES:
            if (
                tbl_name not in tables
                or tbl_name not in table_counts
                or tbl_name not in table_schemas
                or not (tmp_dir / tbl_name).is_dir()
            ):
                raise ValueError(f"Silver table was not materialized: {tbl_name}")

        daily_summary_df = build_daily_quality_summary(tables, publication_lookup_df)
        quality_dir = tmp_dir / "quality"
        quality_dir.mkdir(parents=True, exist_ok=True)

        daily_summary_path = quality_dir / "daily_quality_summary"
        daily_summary_df.write.mode("overwrite").parquet(str(daily_summary_path))
        written_daily_df = spark.read.parquet(str(daily_summary_path))
        daily_rows = [row.asDict(recursive=True) for row in written_daily_df.collect()]

        for row in daily_rows:
            if row.get("operating_date") is not None:
                row["operating_date"] = str(row["operating_date"])

        hard_failures, quality_notes = validate_silver_integrity(
            table_counts=table_counts,
            selection=selection,
            daily_summary_rows=daily_rows,
            written_reconciled_df=tables["reconciled_stop_calls"],
        )

        # Also save a standalone input selection audit artifact in quality/
        write_selection_report(selection, quality_dir / "input_selection.json")

        completed_at_utc = datetime.utcnow().isoformat() + "Z"
        gate_passed = len(hard_failures) == 0

        quality_report = {
            "silver_version": SILVER_VERSION,
            "provider": selection.provider,
            "generated_at_utc": completed_at_utc,
            "gate_status": "passed" if gate_passed else "failed",
            "hard_failures": hard_failures,
            "data_quality_notes": quality_notes,
            "run_totals": {
                "requested_dates": len(selection.requested_dates),
                "siri_et_files_available": sum(
                    1 for r in selection.records if r.siri_file_available
                ),
                "table_row_counts": table_counts,
                "journey_definitions_evaluated": sum(
                    r["journey_definitions_evaluated"] for r in daily_rows
                ),
                "journeys_scheduled": sum(r["journeys_scheduled"] for r in daily_rows),
                "journeys_not_scheduled": sum(r["journeys_not_scheduled"] for r in daily_rows),
                "journeys_calendar_unresolved": sum(
                    r["journeys_calendar_unresolved"] for r in daily_rows
                ),
                "netex_planned_calls_total": sum(r["netex_planned_calls_total"] for r in daily_rows),
                "netex_planned_calls_resolved": sum(
                    r["netex_planned_calls_resolved"] for r in daily_rows
                ),
                "netex_planned_calls_flexible": sum(
                    r["netex_planned_calls_flexible"] for r in daily_rows
                ),
                "siri_et_raw_rows": sum(r["siri_et_raw_rows"] for r in daily_rows),
                "siri_et_canonical_rows": sum(r["siri_et_canonical_rows"] for r in daily_rows),
                "siri_et_collapsed_duplicate_groups": sum(
                    r["siri_et_collapsed_duplicate_groups"] for r in daily_rows
                ),
                "siri_et_conflicting_candidate_rows": sum(
                    r["siri_et_conflicting_candidate_rows"] for r in daily_rows
                ),
                "reconciled_matched": sum(r["reconciled_matched"] for r in daily_rows),
                "reconciled_planned_only": sum(r["reconciled_planned_only"] for r in daily_rows),
                "reconciled_reported_only": sum(r["reconciled_reported_only"] for r in daily_rows),
                "reconciled_ambiguous": sum(r["reconciled_ambiguous"] for r in daily_rows),
                "reconciled_plan_unresolved": sum(r["reconciled_plan_unresolved"] for r in daily_rows),
                "eligible_reported_departure_delay": sum(
                    r["eligible_reported_departure_delay"] for r in daily_rows
                ),
                "eligible_reported_arrival_delay": sum(
                    r["eligible_reported_arrival_delay"] for r in daily_rows
                ),
                "eligible_netex_departure_comparison": sum(
                    r["eligible_netex_departure_comparison"] for r in daily_rows
                ),
                "eligible_netex_arrival_comparison": sum(
                    r["eligible_netex_arrival_comparison"] for r in daily_rows
                ),
                "eligible_planned_call_coverage": sum(
                    r["eligible_planned_call_coverage"] for r in daily_rows
                ),
                "eligible_reported_cancellation_analysis": sum(
                    r["eligible_reported_cancellation_analysis"] for r in daily_rows
                ),
            },
            "daily_summaries": daily_rows,
        }

        quality_report_path = quality_dir / "quality_report.json"
        with quality_report_path.open("w", encoding="utf-8") as fh:
            json.dump(quality_report, fh, indent=2)

        manifest = {
            "status": "complete" if gate_passed else "failed_quality_gate",
            "silver_version": SILVER_VERSION,
            "provider": selection.provider,
            "spark_master": spark_master,
            "started_at_utc": started_at_utc,
            "completed_at_utc": completed_at_utc,
            "output_dir": str(output_dir),
            "config": config_meta,
            "input_selection": selection.to_dict(),
            "output_table_rows": table_counts,
            "output_table_schemas": table_schemas,
            "quality_report_path": str(output_dir / "quality" / "quality_report.json"),
            "gate_status": quality_report["gate_status"],
            "hard_failures": hard_failures,
            "data_quality_notes": quality_notes,
        }

        manifest_path = tmp_dir / "silver_manifest.json"
        with manifest_path.open("w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2)

        if not gate_passed:
            raise RuntimeError(f"Silver publication gate failed: {hard_failures}")

        output_dir.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp_dir, output_dir)
        return manifest
    except Exception:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)
        raise
