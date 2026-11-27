"""Trip-level performance, line service (scheduled, tracked, cancelled) and season/month rollups."""

from __future__ import annotations

import calendar
from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from pipeline.Gold import (
    DEFAULT_PERCENTILE_ACCURACY, SEASON_WINDOWS_2026, add_version_columns, count_if, lower_text, ratio,
)
from pipeline.Gold.event_metrics import (
    aggregate_punctuality, line_labels, non_empty, normalize_journey_key_expr, normalize_transport_mode_expr, null,
)
from pipeline.Gold.inputs import SilverInventory, authority_df, read_silver_table_for_inventory

JOURNEY = ["provider", "operating_date", "publication_id", "journey_key"]
PERIOD = ["period_type", "period_label", "period_start_date", "period_end_date"]
CONFLICT = ("conflict", "conflicting", "duplicate")
SCHEDULED = ("scheduled", "active", "operating")


def _read(spark, inventory, table, columns, keyed=True):
    """Narrow Silver read, with journey_key added (timetable tables have no dated journey ID)."""
    df = read_silver_table_for_inventory(spark, inventory, table, columns)
    if keyed and "dated_service_journey_id" not in df.columns:
        df = df.withColumn("dated_service_journey_id", null("string"))
    return df.withColumn("journey_key", normalize_journey_key_expr()) if keyed else df


def _not_estimated(time_col: str, kind_col: str):
    return F.col(time_col).isNotNull() & (lower_text(kind_col) != "estimated")


def _first_where(cond, col: str, name: str):
    return F.first(F.when(cond, F.col(col)), ignorenulls=True).alias(name)


def _rename(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    return df.select(*JOURNEY, *(F.col(old).alias(new) for old, new in mapping.items()))


def _endpoint_status(resolved, origin, terminal):
    return (F.when(~resolved, null("string")).when(origin & terminal, F.lit("both"))
            .when(origin, F.lit("origin_only")).when(terminal, F.lit("terminal_only")).otherwise(F.lit("neither")))


def build_journey_performance(spark: SparkSession, inventory: SilverInventory, *, gold_run_id: str) -> DataFrame:
    """One row per (provider, operating_date, journey_key): planned vs reported trip, first and last stop."""
    planned = _read(spark, inventory, "journey_service_dates", [
        "provider", "operating_date", "publication_id", "service_journey_id", "resolved_line_id",
        "resolved_transport_mode", "service_status"])
    calls = _read(spark, inventory, "netex_stop_calls", [
        "provider", "operating_date", "publication_id", "service_journey_id", "passing_time_id", "quay_id",
        "passing_time_index", "planned_departure_utc", "planned_arrival_utc"])
    siri = _read(spark, inventory, "siri_et_stop_calls", [
        "provider", "et_canonical_id", "service_journey_id", "dated_service_journey_id", "line_ref", "stop_point_ref",
        "aimed_departure_utc", "aimed_arrival_utc", "reported_departure_utc", "reported_arrival_utc",
        "departure_time_kind", "arrival_time_kind", "journey_cancellation_state", "stop_cancellation_state",
        "duplicate_status"])
    reconciled = _read(spark, inventory, "reconciled_stop_calls", [
        "provider", "operating_date", "publication_id", "et_canonical_id", "planned_call_id", "service_journey_id",
        "dated_service_journey_id", "reconciliation_status", "reported_departure_delay_sec",
        "reported_arrival_delay_sec", "netex_departure_deviation_sec", "netex_arrival_deviation_sec",
        "eligible_reported_departure_delay", "eligible_reported_arrival_delay",
        "eligible_netex_departure_comparison", "eligible_netex_arrival_comparison"])
    dates = authority_df(spark, inventory)

    # SIRI rows get their operating date from the reconciled calls.
    siri = (siri.join(reconciled.select("provider", "operating_date", "publication_id", "et_canonical_id")
                      .dropDuplicates(["provider", "et_canonical_id"]), on=["provider", "et_canonical_id"], how="left")
            .join(dates, on=["provider", "operating_date", "publication_id"], how="inner")
            .filter(F.col("journey_key").isNotNull()))
    reconciled = reconciled.filter(F.col("journey_key").isNotNull())

    # First and last planned stop of each trip.
    order = F.coalesce(F.col("passing_time_index").cast("long"), F.lit(0).cast("long"))
    trip = Window.partitionBy(*JOURNEY)
    calls = (calls.filter(F.col("journey_key").isNotNull())
             .withColumn("_first", F.row_number().over(trip.orderBy(order.asc(), F.col("passing_time_id").asc())) == 1)
             .withColumn("_last", F.row_number().over(trip.orderBy(order.desc(), F.col("passing_time_id").desc())) == 1)
             .withColumn("_n", F.count(F.lit(1)).over(trip)))
    endpoints = (calls.filter(F.col("_first") | F.col("_last")).groupBy(*JOURNEY).agg(
        F.max("_n").cast("long").alias("planned_call_count"),
        _first_where(F.col("_first"), "passing_time_id", "origin_planned_call_id"),
        _first_where(F.col("_last"), "passing_time_id", "terminal_planned_call_id"),
        _first_where(F.col("_first"), "quay_id", "origin_quay_id"),
        _first_where(F.col("_last"), "quay_id", "terminal_quay_id"),
        _first_where(F.col("_first"), "planned_departure_utc", "origin_planned_departure_utc"),
        _first_where(F.col("_last"), "planned_arrival_utc", "terminal_planned_arrival_utc"),
    ).withColumn("endpoint_resolution_status",
                 F.when(F.col("origin_planned_call_id").isNotNull() & F.col("terminal_planned_call_id").isNotNull(),
                        F.lit("resolved")).otherwise(F.lit("unresolved"))))

    cancellation = F.col("journey_cancellation_state")
    reported = siri.groupBy(*JOURNEY).agg(
        F.countDistinct("et_canonical_id").cast("long").alias("reported_canonical_call_count"),
        count_if(_not_estimated("reported_arrival_utc", "arrival_time_kind"), "recorded_arrival_count"),
        count_if(_not_estimated("reported_departure_utc", "departure_time_kind"), "recorded_departure_count"),
        count_if((lower_text("arrival_time_kind") == "estimated") | (lower_text("departure_time_kind") == "estimated"),
                 "estimated_call_count"),
        count_if(lower_text("duplicate_status").isin(*CONFLICT), "conflicting_reported_call_count"),
        F.first("line_ref", ignorenulls=True).alias("reported_line_id"),
        F.max(F.when(cancellation == "cancelled", 1).otherwise(0)).alias("_cancelled"),
        F.max(F.when(cancellation == "not_cancelled", 1).otherwise(0)).alias("_not_cancelled"),
    )
    matched = reconciled.groupBy(*JOURNEY).agg(F.countDistinct(
        F.when(F.col("reconciliation_status") == "matched", F.col("planned_call_id"))).cast("long").alias("matched_call_count"))
    planned = planned.filter(F.col("journey_key").isNotNull()).groupBy(*JOURNEY).agg(*(
        F.first(c, ignorenulls=True).alias(a) for c, a in [
            ("service_journey_id", "service_journey_id"), ("dated_service_journey_id", "dated_service_journey_id"),
            ("resolved_line_id", "planned_line_id"), ("resolved_transport_mode", "transport_mode"),
            ("service_status", "service_status")]))

    trips = (planned.select(*JOURNEY).union(reported.select(*JOURNEY)).dropDuplicates(JOURNEY)
             .join(dates, on=JOURNEY[:3], how="inner")
             .join(planned, on=JOURNEY, how="left").join(endpoints, on=JOURNEY, how="left")
             .join(reported, on=JOURNEY, how="left").join(matched, on=JOURNEY, how="left"))

    # Reported and reconciled values at the first and last stop.
    at_stop = siri.filter(F.col("stop_point_ref").isNotNull()).groupBy(*JOURNEY, "stop_point_ref").agg(*(
        F.first(c, ignorenulls=True).alias(c) for c in [
            "aimed_departure_utc", "aimed_arrival_utc", "reported_departure_utc", "reported_arrival_utc",
            "departure_time_kind", "arrival_time_kind"]))
    flag = lambda c: F.max(F.when(F.coalesce(F.col(c), F.lit(False)), 1).otherwise(0)).alias(c)
    at_call = reconciled.filter(F.col("planned_call_id").isNotNull()).groupBy(*JOURNEY, "planned_call_id").agg(
        *(F.first(c, ignorenulls=True).alias(c) for c in [
            "reported_departure_delay_sec", "reported_arrival_delay_sec", "netex_departure_deviation_sec",
            "netex_arrival_deviation_sec"]),
        *(flag(c) for c in ["eligible_reported_departure_delay", "eligible_reported_arrival_delay",
                            "eligible_netex_departure_comparison", "eligible_netex_arrival_comparison"]))
    trips = (
        trips.join(_rename(at_stop, {"stop_point_ref": "origin_quay_id",
                                     "aimed_departure_utc": "origin_aimed_departure_utc",
                                     "reported_departure_utc": "origin_reported_departure_utc",
                                     "departure_time_kind": "origin_departure_time_kind"}),
                   on=[*JOURNEY, "origin_quay_id"], how="left")
        .join(_rename(at_stop, {"stop_point_ref": "terminal_quay_id",
                                "aimed_arrival_utc": "terminal_aimed_arrival_utc",
                                "reported_arrival_utc": "terminal_reported_arrival_utc",
                                "arrival_time_kind": "terminal_arrival_time_kind"}),
              on=[*JOURNEY, "terminal_quay_id"], how="left")
        .join(_rename(at_call, {"planned_call_id": "origin_planned_call_id",
                                "reported_departure_delay_sec": "_et_origin", "netex_departure_deviation_sec": "_nx_origin",
                                "eligible_reported_departure_delay": "_et_origin_ok",
                                "eligible_netex_departure_comparison": "_nx_origin_ok"}),
              on=[*JOURNEY, "origin_planned_call_id"], how="left")
        .join(_rename(at_call, {"planned_call_id": "terminal_planned_call_id",
                                "reported_arrival_delay_sec": "_et_terminal", "netex_arrival_deviation_sec": "_nx_terminal",
                                "eligible_reported_arrival_delay": "_et_terminal_ok",
                                "eligible_netex_arrival_comparison": "_nx_terminal_ok"}),
              on=[*JOURNEY, "terminal_planned_call_id"], how="left"))

    status = lower_text("service_status")
    is_scheduled = status.isin(*SCHEDULED)
    has_reports = F.coalesce(F.col("reported_canonical_call_count"), F.lit(0)) > 0
    resolved = F.col("endpoint_resolution_status") == "resolved"
    state = (F.when((F.col("_cancelled") == 1) & (F.col("_not_cancelled") == 1), F.lit("conflicting"))
             .when(F.col("_cancelled") == 1, F.lit("cancelled"))
             .when(F.col("_not_cancelled") == 1, F.lit("not_cancelled")).otherwise(F.lit("unknown")))

    def delays(prefix: str):
        """Origin delay, terminal delay, their change and the eligibility flags for one baseline."""
        origin_ok = resolved & (F.coalesce(F.col(f"_{prefix}_origin_ok"), F.lit(0)) == 1)
        terminal_ok = resolved & (F.coalesce(F.col(f"_{prefix}_terminal_ok"), F.lit(0)) == 1)
        origin = F.when(origin_ok, F.col(f"_{prefix}_origin").cast("double")).otherwise(null("double"))
        terminal = F.when(terminal_ok, F.col(f"_{prefix}_terminal").cast("double")).otherwise(null("double"))
        both = origin_ok & terminal_ok
        name = {"et": "et", "nx": "netex"}[prefix]
        return [origin_ok.cast("boolean").alias(f"{name}_origin_departure_eligible"),
                terminal_ok.cast("boolean").alias(f"{name}_terminal_arrival_eligible"),
                both.cast("boolean").alias(f"{name}_endpoint_pair_eligible"),
                origin.alias(f"{name}_origin_departure_delay_sec"),
                terminal.alias(f"{name}_terminal_arrival_delay_sec"),
                F.when(both, terminal - origin).otherwise(null("double")).alias(f"{name}_delay_change_sec")]

    planned_line, reported_line = F.col("planned_line_id"), F.col("reported_line_id")
    dsj, sj = F.col("dated_service_journey_id"), F.col("service_journey_id")
    text = lambda c: F.col(c).cast("string").alias(c)
    return add_version_columns(trips, gold_run_id, ("gold_run_id", "gold_schema_version", "metric_policy_version")).select(
        "gold_run_id", "gold_schema_version", "metric_policy_version",
        text("provider"), F.col("operating_date").cast("date").alias("operating_date"),
        *(text(c) for c in ["source_silver_run_id", "publication_id", "journey_key", "service_journey_id",
                            "dated_service_journey_id"]),
        F.when(dsj.isNotNull() & sj.isNotNull(), F.lit("resolved_both"))
        .when(dsj.isNotNull(), F.lit("resolved_dated_journey")).when(sj.isNotNull(), F.lit("resolved_service_journey"))
        .otherwise(F.lit("unresolved")).alias("journey_identity_status"),
        normalize_transport_mode_expr("transport_mode").alias("transport_mode"),
        text("planned_line_id"), text("reported_line_id"),
        F.when(planned_line.isNotNull() & reported_line.isNotNull(),
               F.when(planned_line == reported_line, F.lit("agreement")).otherwise(F.lit("mismatch")))
        .when(planned_line.isNotNull() | reported_line.isNotNull(), F.lit("one_sided"))
        .otherwise(F.lit("unknown")).alias("line_consistency_status"),
        F.when(status == "", F.lit("no_planned_definition")).when(is_scheduled, F.lit("scheduled"))
        .when(status.isin("not_scheduled", "inactive"), F.lit("not_scheduled")).otherwise(F.lit("unresolved"))
        .alias("planned_service_status"),
        has_reports.cast("boolean").alias("has_reported_records"),
        (has_reports & ~is_scheduled).cast("boolean").alias("reported_outside_resolved_schedule"),
        (is_scheduled & ~has_reports).cast("boolean").alias("scheduled_without_et"),
        *(F.coalesce(F.col(c), F.lit(0)).cast("long").alias(c) for c in [
            "planned_call_count", "matched_call_count", "reported_canonical_call_count", "recorded_arrival_count",
            "recorded_departure_count", "estimated_call_count", "conflicting_reported_call_count"]),
        *(text(c) for c in ["endpoint_resolution_status", "origin_planned_call_id", "terminal_planned_call_id",
                            "origin_quay_id", "terminal_quay_id"]),
        _endpoint_status(resolved,
                         F.col("origin_reported_departure_utc").isNotNull() | F.col("origin_aimed_departure_utc").isNotNull(),
                         F.col("terminal_reported_arrival_utc").isNotNull() | F.col("terminal_aimed_arrival_utc").isNotNull()
                         ).alias("endpoint_record_presence_status"),
        _endpoint_status(resolved, _not_estimated("origin_reported_departure_utc", "origin_departure_time_kind"),
                         _not_estimated("terminal_reported_arrival_utc", "terminal_arrival_time_kind")
                         ).alias("endpoint_observation_status"),
        "origin_planned_departure_utc", "terminal_planned_arrival_utc", "origin_aimed_departure_utc",
        "terminal_aimed_arrival_utc", "origin_reported_departure_utc", "terminal_reported_arrival_utc",
        text("origin_departure_time_kind"), text("terminal_arrival_time_kind"),
        *delays("et"), *delays("nx"),
        state.alias("journey_cancellation_state"),
        state.isin("cancelled", "not_cancelled").cast("boolean").alias("journey_cancellation_eligible"),
        F.array_remove(F.array(
            F.when(has_reports & ~is_scheduled, F.lit("reported_outside_resolved_schedule")),
            F.when(is_scheduled & ~has_reports, F.lit("scheduled_without_et")),
            F.when(~resolved, F.lit("endpoint_unresolved")),
            F.when(state == "conflicting", F.lit("conflicting_journey_cancellation")),
            F.when(state == "unknown", F.lit("unknown_journey_cancellation")),
        ), null("string")).alias("quality_reason_codes"),
    )


def build_daily_line_service(spark: SparkSession, inventory: SilverInventory, journey_perf_df: DataFrame, *,
                             gold_run_id: str) -> DataFrame:
    """Per date and line: scheduled trips, trips with real-time data, journey and stop cancellations."""
    siri = _read(spark, inventory, "siri_et_stop_calls",
                 ["provider", "et_canonical_id", "line_ref", "stop_cancellation_state", "duplicate_status"], keyed=False)
    dates = _read(spark, inventory, "reconciled_stop_calls",
                  ["provider", "operating_date", "publication_id", "et_canonical_id"], keyed=False)
    siri = siri.join(dates.dropDuplicates(["provider", "et_canonical_id"]), on=["provider", "et_canonical_id"], how="inner")
    labels = line_labels(spark, inventory)

    trips = journey_perf_df.withColumn("line_id", F.coalesce("planned_line_id", "reported_line_id")).withColumn(
        "line_resolution_status", F.when(F.col("line_id").isNotNull(), F.lit("resolved")).otherwise(F.lit("unknown")))
    scheduled = F.col("planned_service_status") == "scheduled"
    tracked = F.col("has_reported_records")
    state = F.col("journey_cancellation_state")
    counts = {
        "scheduled_journey_count": scheduled,
        "scheduled_with_et_journey_count": scheduled & tracked,
        "scheduled_without_et_journey_count": scheduled & ~tracked,
        "plan_unresolved_journey_count": F.col("planned_service_status") == "unresolved",
        "reported_journey_count": tracked,
        "reported_outside_resolved_schedule_count": F.col("reported_outside_resolved_schedule"),
        "reported_with_recorded_time_journey_count":
            tracked & ((F.col("recorded_arrival_count") > 0) | (F.col("recorded_departure_count") > 0)),
        "reported_with_both_recorded_endpoints_count": tracked & (F.col("endpoint_observation_status") == "both"),
        "journey_cancelled_count": state == "cancelled",
        "journey_not_cancelled_count": state == "not_cancelled",
        "journey_cancellation_unknown_count": state == "unknown",
        "journey_cancellation_conflicting_count": state == "conflicting",
        "journey_partially_unknown_true_count": state == "partially_unknown_true",
        "journey_partially_unknown_false_count": state == "partially_unknown_false",
        "journey_cancellation_eligible_count": F.col("journey_cancellation_eligible"),
    }
    per_line = trips.groupBy("provider", "operating_date", "source_silver_run_id", "publication_id", "transport_mode",
                             "line_id", "line_resolution_status").agg(*(count_if(c, n) for n, c in counts.items()))

    stop_state, conflict = lower_text("stop_cancellation_state"), lower_text("duplicate_status").isin(*CONFLICT)
    known = ("cancelled", "not_cancelled", "false", "active")
    stops = siri.withColumn("line_id", non_empty("line_ref")).groupBy(
        "provider", "operating_date", "publication_id", "line_id").agg(
        count_if(stop_state == "cancelled", "stop_cancelled_count"),
        count_if(stop_state.isin(*known[1:]), "stop_not_cancelled_count"),
        count_if(~stop_state.isin(*known) & ~conflict, "stop_unknown_count"),
        count_if(conflict, "stop_cancellation_conflicting_candidate_count"))

    out = (per_line.join(stops, on=["provider", "operating_date", "publication_id", "line_id"], how="left")
           .join(labels, on=["publication_id", "line_id"], how="left"))
    for c in ["stop_cancelled_count", "stop_not_cancelled_count", "stop_unknown_count",
              "stop_cancellation_conflicting_candidate_count"]:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(0).cast("long")))
    stop_known = F.col("stop_cancelled_count") + F.col("stop_not_cancelled_count")
    journey_known = F.col("journey_cancelled_count") + F.col("journey_not_cancelled_count")
    out = (out.withColumn("stop_cancellation_eligible_count", stop_known.cast("long"))
           .withColumn("scheduled_journey_reporting_coverage",
                       ratio("scheduled_with_et_journey_count", "scheduled_journey_count"))
           .withColumn("journey_cancellation_rate", ratio("journey_cancelled_count", journey_known))
           .withColumn("stop_cancellation_rate", ratio("stop_cancelled_count", stop_known)))
    return add_version_columns(out, gold_run_id, ("gold_run_id", "gold_schema_version", "metric_policy_version"))


# ---------------------------------------------------------------- seasons and months

def _period_of_date(spark: SparkSession, inventory: SilverInventory) -> DataFrame:
    """operating_date -> its month and its season (with start and end dates)."""
    rows = []
    for a in inventory.authorities:
        d = date.fromisoformat(a.operating_date)
        rows.append((d, "month", f"{d:%Y-%m}", d.replace(day=1),
                     d.replace(day=calendar.monthrange(d.year, d.month)[1])))
        for label, (start, end) in SEASON_WINDOWS_2026.items():
            if date.fromisoformat(start) <= d <= date.fromisoformat(end):
                rows.append((d, "season", label, date.fromisoformat(start), date.fromisoformat(end)))
    return spark.createDataFrame(rows, ["operating_date", *PERIOD])


def _period_sample(spark, inventory, periods: DataFrame, events_df: DataFrame | None = None) -> DataFrame:
    """Which sampled days each period is based on: count, first/last date, days with SIRI, weekdays."""
    siri = spark.createDataFrame([(date.fromisoformat(a.operating_date), 1 if a.siri_file_available else 0)
                                  for a in inventory.authorities], ["operating_date", "_siri"])
    days = periods.join(siri, on="operating_date", how="left")
    extra = []
    if events_df is not None:
        measured = events_df.filter(F.col("eligible")).select("operating_date").dropDuplicates().withColumn("_measured", F.lit(1))
        days = days.join(measured, on="operating_date", how="left")
        extra = [F.sum(F.coalesce(F.col("_measured"), F.lit(0))).cast("long").alias("dates_with_eligible_events_count")]
    weekday = F.element_at(F.array(*(F.lit(d) for d in ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"])),
                           F.dayofweek("operating_date"))
    weekdays = (periods.select(*PERIOD, "operating_date").dropDuplicates().withColumn("_day", weekday)
                .groupBy(*PERIOD, "_day").agg(F.countDistinct("operating_date").cast("long").alias("_n"))
                .groupBy(*PERIOD).agg(F.map_from_entries(F.collect_list(F.struct("_day", "_n"))).alias("weekday_date_counts")))
    return days.groupBy(*PERIOD).agg(
        F.min("operating_date").alias("first_selected_date"),
        F.max("operating_date").alias("last_selected_date"),
        F.countDistinct("operating_date").cast("long").alias("selected_date_count"),
        F.sum(F.coalesce(F.col("_siri"), F.lit(0))).cast("long").alias("dates_with_siri_count"),
        *extra,
    ).join(weekdays, on=PERIOD, how="left")


def build_period_punctuality(spark: SparkSession, inventory: SilverInventory, events_df: DataFrame, *,
                             gold_run_id: str, include_line: bool,
                             percentile_accuracy: int = DEFAULT_PERCENTILE_ACCURACY) -> DataFrame:
    """Punctuality per season and month, for the whole network or per line."""
    periods = _period_of_date(spark, inventory)
    keys = ["provider", *PERIOD, "transport_mode", *(["line_id", "line_resolution_status"] if include_line else []),
            "event_type", "baseline"]
    out = aggregate_punctuality(events_df.join(periods, on="operating_date", how="inner"), keys,
                                gold_run_id=gold_run_id, percentile_accuracy=percentile_accuracy)
    out = out.join(_period_sample(spark, inventory, periods, events_df), on=PERIOD, how="left")
    if include_line:
        names = (read_silver_table_for_inventory(spark, inventory, "lines",
                                                 ["publication_id", "line_id", "public_code", "line_name"])
                 .filter(F.col("line_id").isNotNull()).groupBy("line_id").agg(
                     F.first("public_code", ignorenulls=True).cast("string").alias("line_public_code"),
                     F.first("line_name", ignorenulls=True).cast("string").alias("line_name")))
        out = out.join(names, on="line_id", how="left")
    return out


def build_period_line_service(spark: SparkSession, inventory: SilverInventory, daily_line_service_df: DataFrame, *,
                              gold_run_id: str) -> DataFrame:
    """daily_line_service added up per season and month."""
    periods = _period_of_date(spark, inventory)
    summed = ["scheduled_journey_count", "scheduled_with_et_journey_count", "scheduled_without_et_journey_count",
              "plan_unresolved_journey_count", "reported_journey_count", "reported_outside_resolved_schedule_count",
              "reported_with_recorded_time_journey_count", "reported_with_both_recorded_endpoints_count",
              "journey_cancelled_count", "journey_not_cancelled_count", "journey_cancellation_unknown_count",
              "journey_cancellation_conflicting_count", "stop_cancelled_count", "stop_not_cancelled_count"]
    evaluated = (F.col("scheduled_journey_count") > 0) | (F.col("plan_unresolved_journey_count") == 0)
    out = (daily_line_service_df.join(periods, on="operating_date", how="inner")
           .groupBy("provider", *PERIOD, "transport_mode", "line_id", "line_resolution_status").agg(
               F.first("line_public_code", ignorenulls=True).alias("line_public_code"),
               F.first("line_name", ignorenulls=True).alias("line_name"),
               *(F.sum(c).cast("long").alias(c) for c in summed),
               F.countDistinct(F.when(F.col("scheduled_journey_count") > 0, F.col("operating_date")))
               .cast("long").alias("dates_line_scheduled_count"),
               F.countDistinct(F.when(evaluated, F.col("operating_date"))).cast("long").alias("evaluated_date_count"))
           .join(_period_sample(spark, inventory, periods), on=PERIOD, how="left"))
    out = (out.withColumn("scheduled_journeys_per_evaluated_date", ratio("scheduled_journey_count", "evaluated_date_count"))
           .withColumn("journey_cancellation_rate", ratio("journey_cancelled_count",
                                                          F.col("journey_cancelled_count") + F.col("journey_not_cancelled_count")))
           .withColumn("stop_cancellation_rate", ratio("stop_cancelled_count",
                                                       F.col("stop_cancelled_count") + F.col("stop_not_cancelled_count"))))
    return add_version_columns(out, gold_run_id, ("gold_run_id", "gold_schema_version", "metric_policy_version"))
