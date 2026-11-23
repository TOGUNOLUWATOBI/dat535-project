"""Shared event projection and punctuality/agreement aggregation for Gold."""

from __future__ import annotations

from typing import Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from pipeline.Gold import (
    ARRIVAL_WITHIN_LIMIT_MAX_SEC,
    BASELINES,
    BUSINESS_AGREEMENT_TOLERANCE_SEC,
    DEFAULT_PERCENTILE_ACCURACY,
    DEPARTURE_EARLY_MAX_EXCLUSIVE_SEC,
    DEPARTURE_WITHIN_WINDOW_MAX_SEC,
    EVENT_TYPES,
    GOLD_SCHEMA_VERSION,
    METRIC_POLICY_VERSION,
    SEVERE_DELAY_MIN_EXCLUSIVE_SEC,
    TIMESTAMP_IDENTITY_TOLERANCE_SEC,
)
from pipeline.Gold.inputs import SilverInventory, read_silver_table_for_inventory


def normalize_journey_key_expr(
    dated_journey_col: str = "dated_service_journey_id",
    service_journey_col: str = "service_journey_id",
) -> F.Column:
    """Derive journey_key from explicit journey IDs without date-only fallback."""
    dsj = F.trim(F.col(dated_journey_col).cast("string"))
    sj = F.trim(F.col(service_journey_col).cast("string"))
    dsj_valid = dsj.isNotNull() & (dsj != "")
    sj_valid = sj.isNotNull() & (sj != "")
    return (
        F.when(dsj_valid & sj_valid, F.concat_ws("::", F.lit("dsj"), dsj, F.lit("sj"), sj))
        .when(dsj_valid, F.concat_ws("::", F.lit("dsj"), dsj))
        .when(sj_valid, F.concat_ws("::", F.lit("sj"), sj))
        .otherwise(F.lit(None).cast("string"))
    )


def normalize_transport_mode_expr(col_name: str) -> F.Column:
    """Normalize transport mode without silently collapsing unknown/non-bus values into bus."""
    raw = F.lower(F.trim(F.col(col_name).cast("string")))
    return (
        F.when(raw.isNull() | (raw == ""), F.lit("unknown"))
        .when(raw.isin("bus", "coach"), F.lit("bus"))
        .when(raw.isin("water", "ferry", "boat", "passenger_ship", "high_speed_ferry"), F.lit("water"))
        .otherwise(raw)
    )


def _project_single_slice(
    df: DataFrame,
    *,
    baseline: str,
    event_type: str,
    elig_col: str,
    delay_col: str,
    excl_col: str,
    line_col: str,
    mode_col: str,
    aimed_ts_col: str | None = None,
    planned_ts_col: str | None = None,
    reported_ts_col: str | None = None,
    disagreement_col: str | None = None,
) -> DataFrame:
    """Project one (baseline, event_type) slice from reconciled_stop_calls."""
    elig_expr = F.coalesce(F.col(elig_col).cast("boolean"), F.lit(False))
    delay_expr = F.col(delay_col).cast("double")
    excl_expr = F.coalesce(F.col(excl_col), F.array().cast("array<string>"))

    # Separate plan-comparison eligibility from recorded-time punctuality eligibility:
    # For netex_planned, both planned and SIRI aimed timestamps present enables schedule agreement check.
    if baseline == "netex_planned" and aimed_ts_col and planned_ts_col:
        aimed_epoch = F.col(aimed_ts_col).cast("double")
        planned_epoch = F.col(planned_ts_col).cast("double")
        rep_epoch = F.col(reported_ts_col).cast("double") if reported_ts_col else F.lit(None).cast("double")

        has_both_planned = aimed_epoch.isNotNull() & planned_epoch.isNotNull()
        disagreement_val = (
            F.col(disagreement_col).cast("double")
            if disagreement_col and disagreement_col in df.columns
            else F.when(has_both_planned, aimed_epoch - planned_epoch).otherwise(F.lit(None).cast("double"))
        )
        abs_disagreement_val = F.when(
            disagreement_val.isNotNull(), F.abs(disagreement_val)
        ).otherwise(F.lit(None).cast("double"))
        plan_agreed_expr = F.when(
            disagreement_val.isNotNull(),
            F.abs(disagreement_val) <= F.lit(BUSINESS_AGREEMENT_TOLERANCE_SEC),
        ).otherwise(F.lit(None).cast("boolean"))

        # Check D_NeTEx == D_ET + G within numerical identity tolerance when all three timestamps exist
        has_all_three = aimed_epoch.isNotNull() & planned_epoch.isNotNull() & rep_epoch.isNotNull()
        d_et_calc = rep_epoch - aimed_epoch
        d_netex_calc = rep_epoch - planned_epoch
        g_calc = aimed_epoch - planned_epoch
        identity_failed_expr = F.when(
            has_all_three,
            F.abs(d_netex_calc - (d_et_calc + g_calc)) > F.lit(TIMESTAMP_IDENTITY_TOLERANCE_SEC),
        ).otherwise(F.lit(None).cast("boolean"))
    else:
        has_both_planned = F.lit(False)
        disagreement_val = F.lit(None).cast("double")
        abs_disagreement_val = F.lit(None).cast("double")
        plan_agreed_expr = F.lit(None).cast("boolean")
        has_all_three = F.lit(False)
        identity_failed_expr = F.lit(None).cast("boolean")

    journey_key_expr = normalize_journey_key_expr("dated_service_journey_id", "service_journey_id")
    line_id_expr = F.when(
        F.trim(F.col(line_col).cast("string")) != "",
        F.trim(F.col(line_col).cast("string")),
    ).otherwise(F.lit(None).cast("string"))
    line_res_expr = F.when(line_id_expr.isNotNull(), F.lit("resolved")).otherwise(F.lit("unknown"))

    quay_id_expr = F.when(
        F.trim(F.col("quay_id").cast("string")) != "",
        F.trim(F.col("quay_id").cast("string")),
    ).otherwise(F.lit(None).cast("string"))
    quay_res_expr = F.when(quay_id_expr.isNotNull(), F.lit("resolved")).otherwise(F.lit("unknown"))

    return df.select(
        F.col("provider").cast("string").alias("provider"),
        F.col("operating_date").cast("date").alias("operating_date"),
        F.col("source_silver_run_id").cast("string").alias("source_silver_run_id"),
        F.col("publication_id").cast("string").alias("publication_id"),
        F.col("et_canonical_id").cast("string").alias("et_canonical_id"),
        journey_key_expr.alias("journey_key"),
        line_id_expr.alias("line_id"),
        line_res_expr.alias("line_resolution_status"),
        quay_id_expr.alias("quay_id"),
        quay_res_expr.alias("quay_resolution_status"),
        normalize_transport_mode_expr(mode_col).alias("transport_mode"),
        F.lit(event_type).cast("string").alias("event_type"),
        F.lit(baseline).cast("string").alias("baseline"),
        F.when(elig_expr, delay_expr).otherwise(F.lit(None).cast("double")).alias("delay_sec"),
        elig_expr.alias("eligible"),
        excl_expr.alias("exclusion_reason_codes"),
        has_both_planned.alias("has_plan_comparison"),
        plan_agreed_expr.alias("plan_agreed"),
        disagreement_val.alias("plan_disagreement_sec"),
        abs_disagreement_val.alias("abs_plan_disagreement_sec"),
        has_all_three.alias("ts_identity_checked"),
        identity_failed_expr.alias("ts_identity_failed"),
    )


def build_logical_event_projection(
    spark: SparkSession,
    inventory: SilverInventory,
    *,
    baselines: Sequence[str] = BASELINES,
    event_types: Sequence[str] = EVENT_TYPES,
) -> DataFrame:
    """Build narrow logical arrival/departure event rows across requested baselines and event types."""
    needed_cols = [
        "provider",
        "operating_date",
        "publication_id",
        "et_canonical_id",
        "service_journey_id",
        "dated_service_journey_id",
        "resolved_line_id",
        "line_ref",
        "resolved_transport_mode",
        "quay_id",
        "aimed_arrival_utc",
        "planned_arrival_utc",
        "reported_arrival_utc",
        "aimed_departure_utc",
        "planned_departure_utc",
        "reported_departure_utc",
        "reported_arrival_delay_sec",
        "reported_departure_delay_sec",
        "netex_arrival_deviation_sec",
        "netex_departure_deviation_sec",
        "planned_arrival_disagreement_sec",
        "planned_departure_disagreement_sec",
        "eligible_reported_departure_delay",
        "exclusion_reported_departure_delay",
        "eligible_reported_arrival_delay",
        "exclusion_reported_arrival_delay",
        "eligible_netex_departure_comparison",
        "exclusion_netex_departure_comparison",
        "eligible_netex_arrival_comparison",
        "exclusion_netex_arrival_comparison",
    ]
    base_df = read_silver_table_for_inventory(
        spark,
        inventory,
        table_name="reconciled_stop_calls",
        columns=needed_cols,
    )

    slices: list[DataFrame] = []
    for baseline in baselines:
        if baseline == "et_aimed":
            line_col = "line_ref"
            mode_col = "resolved_transport_mode"
            if "departure" in event_types:
                slices.append(
                    _project_single_slice(
                        base_df,
                        baseline="et_aimed",
                        event_type="departure",
                        elig_col="eligible_reported_departure_delay",
                        delay_col="reported_departure_delay_sec",
                        excl_col="exclusion_reported_departure_delay",
                        line_col=line_col,
                        mode_col=mode_col,
                    )
                )
            if "arrival" in event_types:
                slices.append(
                    _project_single_slice(
                        base_df,
                        baseline="et_aimed",
                        event_type="arrival",
                        elig_col="eligible_reported_arrival_delay",
                        delay_col="reported_arrival_delay_sec",
                        excl_col="exclusion_reported_arrival_delay",
                        line_col=line_col,
                        mode_col=mode_col,
                    )
                )
        elif baseline == "netex_planned":
            line_col = "resolved_line_id"
            mode_col = "resolved_transport_mode"
            if "departure" in event_types:
                slices.append(
                    _project_single_slice(
                        base_df,
                        baseline="netex_planned",
                        event_type="departure",
                        elig_col="eligible_netex_departure_comparison",
                        delay_col="netex_departure_deviation_sec",
                        excl_col="exclusion_netex_departure_comparison",
                        line_col=line_col,
                        mode_col=mode_col,
                        aimed_ts_col="aimed_departure_utc",
                        planned_ts_col="planned_departure_utc",
                        reported_ts_col="reported_departure_utc",
                        disagreement_col="planned_departure_disagreement_sec",
                    )
                )
            if "arrival" in event_types:
                slices.append(
                    _project_single_slice(
                        base_df,
                        baseline="netex_planned",
                        event_type="arrival",
                        elig_col="eligible_netex_arrival_comparison",
                        delay_col="netex_arrival_deviation_sec",
                        excl_col="exclusion_netex_arrival_comparison",
                        line_col=line_col,
                        mode_col=mode_col,
                        aimed_ts_col="aimed_arrival_utc",
                        planned_ts_col="planned_arrival_utc",
                        reported_ts_col="reported_arrival_utc",
                        disagreement_col="planned_arrival_disagreement_sec",
                    )
                )
        else:
            raise ValueError(f"Unsupported baseline: {baseline}")

    if not slices:
        raise ValueError("No event slices selected.")

    out = slices[0]
    for s in slices[1:]:
        out = out.unionByName(s)
    return out


def aggregate_punctuality(
    events_df: DataFrame,
    group_cols: Sequence[str],
    *,
    gold_run_id: str,
    percentile_accuracy: int = DEFAULT_PERCENTILE_ACCURACY,
    include_lineage_cols: bool = True,
) -> DataFrame:
    """Shared aggregation across network, line, stop, and period punctuality outputs."""
    grp = list(group_cols)
    is_elig = F.col("eligible") & F.col("delay_sec").isNotNull()
    is_dep = F.col("event_type") == F.lit("departure")
    is_arr = F.col("event_type") == F.lit("arrival")

    early_cond = is_elig & (F.col("delay_sec") < F.lit(DEPARTURE_EARLY_MAX_EXCLUSIVE_SEC))
    dep_within_cond = (
        is_elig
        & is_dep
        & (F.col("delay_sec") >= F.lit(DEPARTURE_EARLY_MAX_EXCLUSIVE_SEC))
        & (F.col("delay_sec") <= F.lit(DEPARTURE_WITHIN_WINDOW_MAX_SEC))
    )
    arr_within_cond = is_elig & is_arr & (F.col("delay_sec") <= F.lit(ARRIVAL_WITHIN_LIMIT_MAX_SEC))
    within_cond = dep_within_cond | arr_within_cond

    dep_late_cond = is_elig & is_dep & (F.col("delay_sec") > F.lit(DEPARTURE_WITHIN_WINDOW_MAX_SEC))
    arr_late_cond = is_elig & is_arr & (F.col("delay_sec") > F.lit(ARRIVAL_WITHIN_LIMIT_MAX_SEC))
    late_cond = dep_late_cond | arr_late_cond

    severe_cond = is_elig & (F.col("delay_sec") > F.lit(SEVERE_DELAY_MIN_EXCLUSIVE_SEC))
    elig_delay_expr = F.when(is_elig, F.col("delay_sec")).otherwise(F.lit(None).cast("double"))
    pos_delay_expr = F.when(
        is_elig & (F.col("delay_sec") > F.lit(0.0)), F.col("delay_sec")
    ).otherwise(F.lit(0.0))

    is_netex = F.col("baseline") == F.lit("netex_planned")
    plan_comp_cond = is_netex & F.col("has_plan_comparison")
    plan_agree_cond = plan_comp_cond & F.coalesce(F.col("plan_agreed"), F.lit(False))
    ts_id_chk_cond = is_netex & F.col("ts_identity_checked")
    ts_id_fail_cond = ts_id_chk_cond & F.coalesce(F.col("ts_identity_failed"), F.lit(False))

    agg_exprs = [
        F.count(F.lit(1)).cast("long").alias("population_event_count"),
        F.sum(F.when(is_elig, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("eligible_event_count"),
        F.sum(F.when(~is_elig, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("excluded_event_count"),
        F.countDistinct(F.when(is_elig, F.col("journey_key"))).cast("long").alias("contributing_journey_count"),
        F.sum(F.when(early_cond, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("early_count"),
        F.sum(F.when(within_cond, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("within_window_count"),
        F.sum(F.when(late_cond, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("late_count"),
        F.sum(F.when(severe_cond, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("severe_delay_count"),
        F.coalesce(F.sum(elig_delay_expr), F.lit(0.0)).cast("double").alias("delay_sum_sec"),
        F.coalesce(F.sum(pos_delay_expr), F.lit(0.0)).cast("double").alias("positive_delay_sum_sec"),
        F.expr(f"percentile_approx(CASE WHEN eligible AND delay_sec IS NOT NULL THEN delay_sec END, 0.5, {int(percentile_accuracy)})")
        .cast("double")
        .alias("median_delay_sec"),
        F.expr(f"percentile_approx(CASE WHEN eligible AND delay_sec IS NOT NULL THEN delay_sec END, 0.9, {int(percentile_accuracy)})")
        .cast("double")
        .alias("p90_delay_sec"),
        F.expr(f"percentile_approx(CASE WHEN eligible AND delay_sec IS NOT NULL THEN delay_sec END, 0.95, {int(percentile_accuracy)})")
        .cast("double")
        .alias("p95_delay_sec"),
        # Timetable agreement metrics (populated on netex_planned rows; null on et_aimed)
        F.sum(F.when(plan_comp_cond, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("_plan_comp_cnt"),
        F.sum(F.when(plan_agree_cond, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("_plan_agree_cnt"),
        F.avg(F.when(plan_comp_cond, F.col("plan_disagreement_sec"))).cast("double").alias("_mean_plan_disagree_sec"),
        F.expr(
            f"percentile_approx(CASE WHEN baseline = 'netex_planned' AND has_plan_comparison THEN abs_plan_disagreement_sec END, 0.5, {int(percentile_accuracy)})"
        )
        .cast("double")
        .alias("_med_abs_plan_disagree_sec"),
        F.expr(
            f"percentile_approx(CASE WHEN baseline = 'netex_planned' AND has_plan_comparison THEN abs_plan_disagreement_sec END, 0.95, {int(percentile_accuracy)})"
        )
        .cast("double")
        .alias("_p95_abs_plan_disagree_sec"),
        F.sum(F.when(ts_id_chk_cond, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("_ts_id_chk_cnt"),
        F.sum(F.when(ts_id_fail_cond, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("_ts_id_fail_cnt"),
    ]
    if include_lineage_cols:
        agg_exprs.extend(
            [
                F.first("source_silver_run_id", ignorenulls=True).alias("source_silver_run_id"),
                F.first("publication_id", ignorenulls=True).alias("publication_id"),
            ]
        )

    grouped = events_df.groupBy(*grp).agg(*agg_exprs)

    elig_pos = F.col("eligible_event_count") > F.lit(0)
    netex_row = F.col("baseline") == F.lit("netex_planned")
    comp_pos = F.col("_plan_comp_cnt") > F.lit(0)

    out = grouped.withColumn(
        "mean_delay_sec",
        F.when(elig_pos, F.col("delay_sum_sec") / F.col("eligible_event_count").cast("double")).otherwise(
            F.lit(None).cast("double")
        ),
    ).withColumn(
        "within_window_rate",
        F.when(
            elig_pos, F.col("within_window_count").cast("double") / F.col("eligible_event_count").cast("double")
        ).otherwise(F.lit(None).cast("double")),
    ).withColumn(
        "early_rate",
        F.when(elig_pos, F.col("early_count").cast("double") / F.col("eligible_event_count").cast("double")).otherwise(
            F.lit(None).cast("double")
        ),
    ).withColumn(
        "late_rate",
        F.when(elig_pos, F.col("late_count").cast("double") / F.col("eligible_event_count").cast("double")).otherwise(
            F.lit(None).cast("double")
        ),
    ).withColumn(
        "severe_delay_rate",
        F.when(
            elig_pos, F.col("severe_delay_count").cast("double") / F.col("eligible_event_count").cast("double")
        ).otherwise(F.lit(None).cast("double")),
    ).withColumn(
        "plan_comparison_count",
        F.when(netex_row, F.col("_plan_comp_cnt")).otherwise(F.lit(None).cast("long")),
    ).withColumn(
        "plan_agreement_count",
        F.when(netex_row, F.col("_plan_agree_cnt")).otherwise(F.lit(None).cast("long")),
    ).withColumn(
        "plan_agreement_rate",
        F.when(
            netex_row & comp_pos,
            F.col("_plan_agree_cnt").cast("double") / F.col("_plan_comp_cnt").cast("double"),
        ).otherwise(F.lit(None).cast("double")),
    ).withColumn(
        "mean_plan_disagreement_sec",
        F.when(netex_row & comp_pos, F.col("_mean_plan_disagree_sec")).otherwise(F.lit(None).cast("double")),
    ).withColumn(
        "median_abs_plan_disagreement_sec",
        F.when(netex_row & comp_pos, F.col("_med_abs_plan_disagree_sec")).otherwise(F.lit(None).cast("double")),
    ).withColumn(
        "p95_abs_plan_disagreement_sec",
        F.when(netex_row & comp_pos, F.col("_p95_abs_plan_disagree_sec")).otherwise(F.lit(None).cast("double")),
    ).withColumn(
        "timestamp_identity_checked_count",
        F.when(netex_row, F.col("_ts_id_chk_cnt")).otherwise(F.lit(None).cast("long")),
    ).withColumn(
        "timestamp_identity_failed_count",
        F.when(netex_row, F.col("_ts_id_fail_cnt")).otherwise(F.lit(None).cast("long")),
    ).withColumn(
        "gold_schema_version", F.lit(GOLD_SCHEMA_VERSION).cast("string")
    ).withColumn(
        "metric_policy_version", F.lit(METRIC_POLICY_VERSION).cast("string")
    ).withColumn(
        "gold_run_id", F.lit(gold_run_id).cast("string")
    ).drop(
        "_plan_comp_cnt",
        "_plan_agree_cnt",
        "_mean_plan_disagree_sec",
        "_med_abs_plan_disagree_sec",
        "_p95_abs_plan_disagree_sec",
        "_ts_id_chk_cnt",
        "_ts_id_fail_cnt",
    )
    return out


def build_daily_network_punctuality(
    spark: SparkSession,
    inventory: SilverInventory,
    events_df: DataFrame,
    *,
    gold_run_id: str,
    percentile_accuracy: int = DEFAULT_PERCENTILE_ACCURACY,
) -> DataFrame:
    """Build dense daily_network_punctuality (212 rows for 53 dates x 2 events x 2 baselines)."""
    dense_grid = (
        spark.createDataFrame(
            [
                (
                    auth.provider,
                    auth.operating_date,
                    auth.source_silver_run_id,
                    auth.publication_id,
                )
                for auth in inventory.authorities
            ],
            schema=T.StructType(
                [
                    T.StructField("provider", T.StringType(), False),
                    T.StructField("operating_date_str", T.StringType(), False),
                    T.StructField("source_silver_run_id", T.StringType(), False),
                    T.StructField("publication_id", T.StringType(), False),
                ]
            ),
        )
        .withColumn("operating_date", F.to_date(F.col("operating_date_str")))
        .drop("operating_date_str")
        .crossJoin(
            spark.createDataFrame(
                [(ev, bl) for ev in EVENT_TYPES for bl in BASELINES],
                schema=T.StructType(
                    [
                        T.StructField("event_type", T.StringType(), False),
                        T.StructField("baseline", T.StringType(), False),
                    ]
                ),
            )
        )
    )

    agg_df = aggregate_punctuality(
        events_df,
        group_cols=[
            "provider",
            "operating_date",
            "source_silver_run_id",
            "publication_id",
            "event_type",
            "baseline",
        ],
        gold_run_id=gold_run_id,
        percentile_accuracy=percentile_accuracy,
        include_lineage_cols=False,
    )

    joined = dense_grid.join(
        agg_df.drop("gold_schema_version", "metric_policy_version", "gold_run_id"),
        on=["provider", "operating_date", "source_silver_run_id", "publication_id", "event_type", "baseline"],
        how="left",
    )

    zero_long_cols = [
        "population_event_count",
        "eligible_event_count",
        "excluded_event_count",
        "contributing_journey_count",
        "early_count",
        "within_window_count",
        "late_count",
        "severe_delay_count",
    ]
    zero_double_cols = ["delay_sum_sec", "positive_delay_sum_sec"]

    out = joined
    for c in zero_long_cols:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(0).cast("long")))
    for c in zero_double_cols:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(0.0).cast("double")))

    out = (
        out.withColumn(
            "plan_comparison_count",
            F.when(
                F.col("baseline") == F.lit("netex_planned"),
                F.coalesce(F.col("plan_comparison_count"), F.lit(0).cast("long")),
            ).otherwise(F.lit(None).cast("long")),
        )
        .withColumn(
            "plan_agreement_count",
            F.when(
                F.col("baseline") == F.lit("netex_planned"),
                F.coalesce(F.col("plan_agreement_count"), F.lit(0).cast("long")),
            ).otherwise(F.lit(None).cast("long")),
        )
        .withColumn(
            "timestamp_identity_checked_count",
            F.when(
                F.col("baseline") == F.lit("netex_planned"),
                F.coalesce(F.col("timestamp_identity_checked_count"), F.lit(0).cast("long")),
            ).otherwise(F.lit(None).cast("long")),
        )
        .withColumn(
            "timestamp_identity_failed_count",
            F.when(
                F.col("baseline") == F.lit("netex_planned"),
                F.coalesce(F.col("timestamp_identity_failed_count"), F.lit(0).cast("long")),
            ).otherwise(F.lit(None).cast("long")),
        )
        .withColumn("gold_schema_version", F.lit(GOLD_SCHEMA_VERSION).cast("string"))
        .withColumn("metric_policy_version", F.lit(METRIC_POLICY_VERSION).cast("string"))
        .withColumn("gold_run_id", F.lit(gold_run_id).cast("string"))
    )
    return out


def build_daily_line_punctuality(
    spark: SparkSession,
    inventory: SilverInventory,
    events_df: DataFrame,
    *,
    gold_run_id: str,
    percentile_accuracy: int = DEFAULT_PERCENTILE_ACCURACY,
) -> DataFrame:
    """Build daily_line_punctuality from shared event projection and aggregate_punctuality."""
    line_dim = read_silver_table_for_inventory(
        spark,
        inventory,
        table_name="lines",
        columns=["publication_id", "line_id", "public_code", "line_name"],
    )
    dim_keys = ["publication_id", "line_id"]
    line_dim_non_null = line_dim.filter(F.col("line_id").isNotNull())
    line_dim_dedup = line_dim_non_null.dropDuplicates(dim_keys)
    total_dim = line_dim_non_null.select(*dim_keys).count()
    distinct_dim = line_dim_dedup.count()
    if total_dim != distinct_dim:
        raise ValueError(
            f"Line dimension is not unique on {dim_keys}: {total_dim} rows vs {distinct_dim} distinct keys."
        )

    line_labels = line_dim_dedup.select(
        *dim_keys,
        F.col("public_code").cast("string").alias("line_public_code"),
        F.col("line_name").cast("string").alias("line_name"),
    )

    agg_df = aggregate_punctuality(
        events_df,
        group_cols=[
            "provider",
            "operating_date",
            "source_silver_run_id",
            "publication_id",
            "transport_mode",
            "line_id",
            "line_resolution_status",
            "event_type",
            "baseline",
        ],
        gold_run_id=gold_run_id,
        percentile_accuracy=percentile_accuracy,
        include_lineage_cols=False,
    )

    pre_count = agg_df.count()
    joined = agg_df.join(line_labels, on=dim_keys, how="left")
    post_count = joined.count()
    if post_count != pre_count:
        raise ValueError(
            f"Line label enrichment changed row count on {dim_keys}: {pre_count} -> {post_count}"
        )

    return joined.select(
        "gold_run_id",
        "gold_schema_version",
        "metric_policy_version",
        "provider",
        "operating_date",
        "source_silver_run_id",
        "publication_id",
        "transport_mode",
        "line_id",
        "line_resolution_status",
        "line_public_code",
        "line_name",
        "event_type",
        "baseline",
        "population_event_count",
        "eligible_event_count",
        "excluded_event_count",
        "contributing_journey_count",
        "early_count",
        "within_window_count",
        "late_count",
        "severe_delay_count",
        "delay_sum_sec",
        "positive_delay_sum_sec",
        "mean_delay_sec",
        "median_delay_sec",
        "p90_delay_sec",
        "p95_delay_sec",
        "within_window_rate",
        "early_rate",
        "late_rate",
        "severe_delay_rate",
        "plan_comparison_count",
        "plan_agreement_count",
        "plan_agreement_rate",
        "mean_plan_disagreement_sec",
        "median_abs_plan_disagreement_sec",
        "p95_abs_plan_disagreement_sec",
        "timestamp_identity_checked_count",
        "timestamp_identity_failed_count",
    )


def build_daily_stop_punctuality(
    events_df: DataFrame,
    *,
    gold_run_id: str,
    percentile_accuracy: int = DEFAULT_PERCENTILE_ACCURACY,
) -> DataFrame:
    """Build daily_stop_punctuality keyed by quay_id without inventing absent-date rows."""
    agg_df = aggregate_punctuality(
        events_df,
        group_cols=[
            "provider",
            "operating_date",
            "source_silver_run_id",
            "publication_id",
            "transport_mode",
            "quay_id",
            "quay_resolution_status",
            "event_type",
            "baseline",
        ],
        gold_run_id=gold_run_id,
        percentile_accuracy=percentile_accuracy,
        include_lineage_cols=False,
    )

    return agg_df.select(
        "gold_run_id",
        "gold_schema_version",
        "metric_policy_version",
        "provider",
        "operating_date",
        "source_silver_run_id",
        "publication_id",
        "transport_mode",
        "quay_id",
        "quay_resolution_status",
        "event_type",
        "baseline",
        "population_event_count",
        "eligible_event_count",
        "excluded_event_count",
        "contributing_journey_count",
        "early_count",
        "within_window_count",
        "late_count",
        "severe_delay_count",
        "delay_sum_sec",
        "positive_delay_sum_sec",
        "mean_delay_sec",
        "median_delay_sec",
        "p90_delay_sec",
        "p95_delay_sec",
        "within_window_rate",
        "early_rate",
        "late_rate",
        "severe_delay_rate",
        "plan_comparison_count",
        "plan_agreement_count",
        "plan_agreement_rate",
        "mean_plan_disagreement_sec",
        "median_abs_plan_disagreement_sec",
        "p95_abs_plan_disagreement_sec",
        "timestamp_identity_checked_count",
        "timestamp_identity_failed_count",
    )
