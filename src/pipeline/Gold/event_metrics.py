"""Stop events (one row per stop call x event x baseline) and the punctuality tables built from them."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from pipeline.Gold import (
    ARRIVAL_WITHIN_LIMIT_MAX_SEC, BASELINES, BUSINESS_AGREEMENT_TOLERANCE_SEC, DEFAULT_PERCENTILE_ACCURACY,
    DEPARTURE_EARLY_MAX_EXCLUSIVE_SEC, DEPARTURE_WITHIN_WINDOW_MAX_SEC, EVENT_TYPES, SEVERE_DELAY_MIN_EXCLUSIVE_SEC,
    TIMESTAMP_IDENTITY_TOLERANCE_SEC, add_version_columns, count_if, ratio,
)
from pipeline.Gold.inputs import SilverInventory, authority_df, read_silver_table_for_inventory

# (baseline, event) -> reconciled_stop_calls columns: eligible flag, delay, exclusion reasons, line ID.
SLICES = {
    ("et_aimed", "departure"): ("eligible_reported_departure_delay", "reported_departure_delay_sec",
                                "exclusion_reported_departure_delay", "line_ref"),
    ("et_aimed", "arrival"): ("eligible_reported_arrival_delay", "reported_arrival_delay_sec",
                              "exclusion_reported_arrival_delay", "line_ref"),
    ("netex_planned", "departure"): ("eligible_netex_departure_comparison", "netex_departure_deviation_sec",
                                     "exclusion_netex_departure_comparison", "resolved_line_id"),
    ("netex_planned", "arrival"): ("eligible_netex_arrival_comparison", "netex_arrival_deviation_sec",
                                   "exclusion_netex_arrival_comparison", "resolved_line_id"),
}
# Output columns of every punctuality table, after its key columns.
METRIC_COLUMNS = [
    "population_event_count", "eligible_event_count", "excluded_event_count", "contributing_journey_count",
    "early_count", "within_window_count", "late_count", "severe_delay_count", "delay_sum_sec",
    "positive_delay_sum_sec", "mean_delay_sec", "median_delay_sec", "p90_delay_sec", "p95_delay_sec",
    "within_window_rate", "early_rate", "late_rate", "severe_delay_rate", "plan_comparison_count",
    "plan_agreement_count", "plan_agreement_rate", "mean_plan_disagreement_sec",
    "median_abs_plan_disagreement_sec", "p95_abs_plan_disagreement_sec", "timestamp_identity_checked_count",
    "timestamp_identity_failed_count",
]


def null(type_name: str):
    return F.lit(None).cast(type_name)


def normalize_journey_key_expr(dated_journey_col="dated_service_journey_id", service_journey_col="service_journey_id"):
    """journey_key from the dated and/or plain journey ID (never from the date alone)."""
    dsj, sj = (F.trim(F.col(c).cast("string")) for c in (dated_journey_col, service_journey_col))
    dsj_ok, sj_ok = dsj.isNotNull() & (dsj != ""), sj.isNotNull() & (sj != "")
    return (F.when(dsj_ok & sj_ok, F.concat_ws("::", F.lit("dsj"), dsj, F.lit("sj"), sj))
            .when(dsj_ok, F.concat_ws("::", F.lit("dsj"), dsj))
            .when(sj_ok, F.concat_ws("::", F.lit("sj"), sj))
            .otherwise(F.lit(None).cast("string")))


def normalize_transport_mode_expr(col_name: str):
    """bus / water / unknown; other values are kept as they are (never guessed to be bus)."""
    raw = F.lower(F.trim(F.col(col_name).cast("string")))
    return (F.when(raw.isNull() | (raw == ""), F.lit("unknown"))
            .when(raw.isin("bus", "coach"), F.lit("bus"))
            .when(raw.isin("water", "ferry", "boat", "passenger_ship", "high_speed_ferry"), F.lit("water"))
            .otherwise(raw))


def non_empty(col: str):
    """Trimmed string, NULL when empty."""
    value = F.trim(F.col(col).cast("string"))
    return F.when(value != "", value).otherwise(F.lit(None).cast("string"))


def _slice(df: DataFrame, baseline: str, event: str) -> DataFrame:
    """Event rows for one (baseline, event) slice."""
    elig_col, delay_col, excl_col, line_col = SLICES[(baseline, event)]
    eligible = F.coalesce(F.col(elig_col).cast("boolean"), F.lit(False))
    if baseline == "netex_planned":
        # Compare the live schedule with the printed timetable, and check delay_netex == delay_et + gap.
        aimed, planned, reported = (F.col(f"{k}_{event}_utc").cast("double") for k in ("aimed", "planned", "reported"))
        gap = F.col(f"planned_{event}_disagreement_sec").cast("double")
        has_plan = aimed.isNotNull() & planned.isNotNull()
        has_all = has_plan & reported.isNotNull()
        plan = [has_plan, F.abs(gap) <= F.lit(BUSINESS_AGREEMENT_TOLERANCE_SEC), gap, F.abs(gap), has_all,
                F.when(has_all, F.abs((reported - planned) - ((reported - aimed) + (aimed - planned)))
                       > F.lit(TIMESTAMP_IDENTITY_TOLERANCE_SEC))]
    else:
        plan = [F.lit(False), null("boolean"), null("double"), null("double"), F.lit(False), null("boolean")]
    line_id, quay_id = non_empty(line_col), non_empty("quay_id")
    resolved = lambda c: F.when(c.isNotNull(), F.lit("resolved")).otherwise(F.lit("unknown"))
    return df.select(
        F.col("provider").cast("string").alias("provider"),
        F.col("operating_date").cast("date").alias("operating_date"),
        F.col("source_silver_run_id").cast("string").alias("source_silver_run_id"),
        F.col("publication_id").cast("string").alias("publication_id"),
        F.col("et_canonical_id").cast("string").alias("et_canonical_id"),
        normalize_journey_key_expr().alias("journey_key"),
        line_id.alias("line_id"), resolved(line_id).alias("line_resolution_status"),
        quay_id.alias("quay_id"), resolved(quay_id).alias("quay_resolution_status"),
        normalize_transport_mode_expr("resolved_transport_mode").alias("transport_mode"),
        F.lit(event).alias("event_type"), F.lit(baseline).alias("baseline"),
        F.when(eligible, F.col(delay_col).cast("double")).otherwise(null("double")).alias("delay_sec"),
        eligible.alias("eligible"),
        F.coalesce(F.col(excl_col), F.array().cast("array<string>")).alias("exclusion_reason_codes"),
        *(c.alias(n) for c, n in zip(plan, ["has_plan_comparison", "plan_agreed", "plan_disagreement_sec",
                                            "abs_plan_disagreement_sec", "ts_identity_checked", "ts_identity_failed"])),
    )


def build_logical_event_projection(spark: SparkSession, inventory: SilverInventory) -> DataFrame:
    """All arrival/departure events for both baselines, from reconciled_stop_calls."""
    columns = ["provider", "operating_date", "publication_id", "et_canonical_id", "service_journey_id",
               "dated_service_journey_id", "resolved_line_id", "line_ref", "resolved_transport_mode", "quay_id"]
    for event in ("arrival", "departure"):
        columns += [f"{k}_{event}_utc" for k in ("aimed", "planned", "reported")] + [f"planned_{event}_disagreement_sec"]
    columns += sorted({c for spec in SLICES.values() for c in spec[:3]})
    calls = read_silver_table_for_inventory(spark, inventory, "reconciled_stop_calls", columns)
    events = [_slice(calls, b, e) for b in BASELINES for e in ("departure", "arrival")]
    out = events[0]
    for df in events[1:]:
        out = out.unionByName(df)
    return out


def aggregate_punctuality(events_df: DataFrame, group_cols, *, gold_run_id: str,
                          percentile_accuracy: int = DEFAULT_PERCENTILE_ACCURACY) -> DataFrame:
    """Counts, delay percentiles, rates and timetable agreement per group (shared by every punctuality table)."""
    delay, event = F.col("delay_sec"), F.col("event_type")
    eligible = F.col("eligible") & delay.isNotNull()
    dep, arr = eligible & (event == "departure"), eligible & (event == "arrival")
    netex = F.col("baseline") == F.lit("netex_planned")
    compared = netex & F.col("has_plan_comparison")
    checked = netex & F.col("ts_identity_checked")

    def pct(column: str, q: float, where: str):
        sql = f"percentile_approx(CASE WHEN {where} THEN {column} END, {q}, {int(percentile_accuracy)})"
        return F.expr(sql).cast("double")

    in_delay = "eligible AND delay_sec IS NOT NULL"
    in_plan = "baseline = 'netex_planned' AND has_plan_comparison"
    out = events_df.groupBy(*group_cols).agg(
        F.count(F.lit(1)).cast("long").alias("population_event_count"),
        count_if(eligible, "eligible_event_count"),
        count_if(~eligible, "excluded_event_count"),
        F.countDistinct(F.when(eligible, F.col("journey_key"))).cast("long").alias("contributing_journey_count"),
        count_if(eligible & (delay < DEPARTURE_EARLY_MAX_EXCLUSIVE_SEC), "early_count"),
        count_if((dep & (delay >= DEPARTURE_EARLY_MAX_EXCLUSIVE_SEC) & (delay <= DEPARTURE_WITHIN_WINDOW_MAX_SEC))
                 | (arr & (delay <= ARRIVAL_WITHIN_LIMIT_MAX_SEC)), "within_window_count"),
        count_if((dep & (delay > DEPARTURE_WITHIN_WINDOW_MAX_SEC)) | (arr & (delay > ARRIVAL_WITHIN_LIMIT_MAX_SEC)),
                 "late_count"),
        count_if(eligible & (delay > SEVERE_DELAY_MIN_EXCLUSIVE_SEC), "severe_delay_count"),
        F.coalesce(F.sum(F.when(eligible, delay).otherwise(null("double"))), F.lit(0.0)).cast("double").alias("delay_sum_sec"),
        F.coalesce(F.sum(F.when(eligible & (delay > 0.0), delay).otherwise(F.lit(0.0))), F.lit(0.0))
        .cast("double").alias("positive_delay_sum_sec"),
        pct("delay_sec", 0.5, in_delay).alias("median_delay_sec"),
        pct("delay_sec", 0.9, in_delay).alias("p90_delay_sec"),
        pct("delay_sec", 0.95, in_delay).alias("p95_delay_sec"),
        count_if(compared, "_compared"),
        count_if(compared & F.coalesce(F.col("plan_agreed"), F.lit(False)), "_agreed"),
        F.avg(F.when(compared, F.col("plan_disagreement_sec"))).cast("double").alias("_mean_gap"),
        pct("abs_plan_disagreement_sec", 0.5, in_plan).alias("_median_gap"),
        pct("abs_plan_disagreement_sec", 0.95, in_plan).alias("_p95_gap"),
        count_if(checked, "_checked"),
        count_if(checked & F.coalesce(F.col("ts_identity_failed"), F.lit(False)), "_failed"),
    )
    for rate, count in [("mean_delay_sec", "delay_sum_sec"), ("within_window_rate", "within_window_count"),
                        ("early_rate", "early_count"), ("late_rate", "late_count"),
                        ("severe_delay_rate", "severe_delay_count")]:
        out = out.withColumn(rate, ratio(count, "eligible_event_count"))
    # Timetable-agreement metrics exist only on netex_planned rows.
    netex_row, has_comparisons = F.col("baseline") == F.lit("netex_planned"), F.col("_compared") > 0
    for name, value, needs_comparisons in [
        ("plan_comparison_count", "_compared", False), ("plan_agreement_count", "_agreed", False),
        ("plan_agreement_rate", ratio("_agreed", "_compared"), True), ("mean_plan_disagreement_sec", "_mean_gap", True),
        ("median_abs_plan_disagreement_sec", "_median_gap", True), ("p95_abs_plan_disagreement_sec", "_p95_gap", True),
        ("timestamp_identity_checked_count", "_checked", False), ("timestamp_identity_failed_count", "_failed", False),
    ]:
        value = F.col(value) if isinstance(value, str) else value
        out = out.withColumn(name, F.when(netex_row & has_comparisons if needs_comparisons else netex_row, value))
    out = out.drop("_compared", "_agreed", "_mean_gap", "_median_gap", "_p95_gap", "_checked", "_failed")
    return add_version_columns(out, gold_run_id)


def build_daily_network_punctuality(spark, inventory, events_df, *, gold_run_id, percentile_accuracy=DEFAULT_PERCENTILE_ACCURACY):
    """Every date x event x baseline, also when nothing was measured (212 rows for 53 dates)."""
    keys = ["provider", "operating_date", "source_silver_run_id", "publication_id", "event_type", "baseline"]
    grid = authority_df(spark, inventory).crossJoin(
        spark.createDataFrame([(e, b) for e in EVENT_TYPES for b in BASELINES], "event_type string, baseline string"))
    metrics = aggregate_punctuality(events_df, keys, gold_run_id=gold_run_id, percentile_accuracy=percentile_accuracy)
    out = grid.join(metrics.drop("gold_schema_version", "metric_policy_version", "gold_run_id"), on=keys, how="left")
    for c in METRIC_COLUMNS[:8]:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(0).cast("long")))
    for c in ("delay_sum_sec", "positive_delay_sum_sec"):
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(0.0).cast("double")))
    for c in ("plan_comparison_count", "plan_agreement_count", "timestamp_identity_checked_count",
              "timestamp_identity_failed_count"):
        out = out.withColumn(c, F.when(F.col("baseline") == "netex_planned", F.coalesce(F.col(c), F.lit(0).cast("long"))))
    return add_version_columns(out, gold_run_id)


def line_labels(spark: SparkSession, inventory: SilverInventory) -> DataFrame:
    """(publication_id, line_id) -> line_public_code, line_name; fails if a line appears twice."""
    keys = ["publication_id", "line_id"]
    lines = read_silver_table_for_inventory(spark, inventory, "lines", [*keys, "public_code", "line_name"])
    lines = lines.filter(F.col("line_id").isNotNull())
    unique = lines.dropDuplicates(keys)
    if lines.select(*keys).count() != unique.count():
        raise ValueError(f"Line dimension is not unique on {keys}.")
    return unique.select(*keys, F.col("public_code").cast("string").alias("line_public_code"),
                         F.col("line_name").cast("string").alias("line_name"))


def _daily_keyed(events_df, key_cols, gold_run_id, percentile_accuracy):
    keys = ["provider", "operating_date", "source_silver_run_id", "publication_id", "transport_mode", *key_cols,
            "event_type", "baseline"]
    return aggregate_punctuality(events_df, keys, gold_run_id=gold_run_id, percentile_accuracy=percentile_accuracy)


def _select_daily(df, key_cols):
    return df.select("gold_run_id", "gold_schema_version", "metric_policy_version", "provider", "operating_date",
                     "source_silver_run_id", "publication_id", "transport_mode", *key_cols, "event_type", "baseline",
                     *METRIC_COLUMNS)


def build_daily_line_punctuality(spark, inventory, events_df, *, gold_run_id, percentile_accuracy=DEFAULT_PERCENTILE_ACCURACY):
    """Punctuality per date and line, with the line's public number and name."""
    metrics = _daily_keyed(events_df, ["line_id", "line_resolution_status"], gold_run_id, percentile_accuracy)
    joined = metrics.join(line_labels(spark, inventory), on=["publication_id", "line_id"], how="left")
    if joined.count() != metrics.count():
        raise ValueError("Line label enrichment changed the row count.")
    return _select_daily(joined, ["line_id", "line_resolution_status", "line_public_code", "line_name"])


def build_daily_stop_punctuality(events_df, *, gold_run_id, percentile_accuracy=DEFAULT_PERCENTILE_ACCURACY):
    """Punctuality per date and quay (only quays that had events)."""
    metrics = _daily_keyed(events_df, ["quay_id", "quay_resolution_status"], gold_run_id, percentile_accuracy)
    return _select_daily(metrics, ["quay_id", "quay_resolution_status"])
