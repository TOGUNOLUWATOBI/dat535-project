"""Journey-first stop-call reconciliation and metric eligibility for Silver."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _diff_seconds_col(lhs_ts, rhs_ts):
    """Signed timestamp difference (lhs - rhs) in seconds preserving fractional precision."""
    return F.when(
        lhs_ts.isNotNull() & rhs_ts.isNotNull(),
        F.unix_micros(lhs_ts) / F.lit(1_000_000.0) - F.unix_micros(rhs_ts) / F.lit(1_000_000.0),
    ).otherwise(F.lit(None).cast("double"))


def build_reconciled_stop_calls(
    netex_stop_calls_df: DataFrame,
    siri_et_stop_calls_df: DataFrame,
    journey_service_dates_df: DataFrame,
    publication_lookup_df: DataFrame,
) -> DataFrame:
    """Reconcile planned NeTEx stop calls and canonical SIRI ET stop calls."""
    selected_dates = publication_lookup_df.select(
        "publication_id",
        "provider",
        "snapshot_date",
        F.col("operating_date").cast("date").alias("operating_date"),
        "siri_file_available",
    ).dropDuplicates()

    jsd_lookup = (
        journey_service_dates_df.withColumn(
            "_jsd_rank",
            F.row_number().over(
                Window.partitionBy("publication_id", "operating_date", "service_journey_id").orderBy(
                    F.when(F.col("service_status") == "scheduled", F.lit(0))
                    .when(F.col("service_status") == "unresolved", F.lit(1))
                    .otherwise(F.lit(2)),
                    F.col("service_journey_version").desc_nulls_last(),
                )
            ),
        )
        .filter(F.col("_jsd_rank") == 1)
        .select(
            "publication_id",
            "operating_date",
            "service_journey_id",
            F.col("service_status").alias("journey_service_status"),
            F.col("reason_codes").alias("journey_service_reason_codes"),
        )
    )

    p_base = (
        netex_stop_calls_df.withColumn(
            "_p_order_key",
            F.coalesce(
                F.col("pattern_point_index").cast("long"),
                F.col("passing_time_order_raw").cast("long"),
                F.col("passing_time_index").cast("long"),
            ),
        )
        .withColumn(
            "_p_seq_rank",
            F.dense_rank().over(
                Window.partitionBy("publication_id", "operating_date", "service_journey_id").orderBy(
                    "_p_order_key", "passing_time_index"
                )
            ),
        )
        .withColumn(
            "_p_quay_visit_cnt",
            F.when(
                F.col("quay_id").isNotNull(),
                F.count(F.lit(1)).over(
                    Window.partitionBy(
                        "publication_id", "operating_date", "service_journey_id", "quay_id"
                    )
                ),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "_p_visit_nr",
            F.when(
                F.col("quay_id").isNotNull(),
                F.dense_rank().over(
                    Window.partitionBy(
                        "publication_id", "operating_date", "service_journey_id", "quay_id"
                    ).orderBy("_p_order_key", "passing_time_index")
                ),
            ).otherwise(F.lit(None).cast("int")),
        )
        .withColumn(
            "planned_call_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("publication_id"),
                    F.date_format(F.col("operating_date"), "yyyy-MM-dd"),
                    F.coalesce(F.col("service_journey_id"), F.lit("")),
                    F.coalesce(F.col("passing_time_index").cast("string"), F.lit("")),
                    F.coalesce(F.col("stop_point_in_pattern_ref"), F.lit("")),
                ),
                256,
            ),
        )
    )

    e_base = siri_et_stop_calls_df.withColumn(
        "_e_order_key",
        F.coalesce(F.col("sequence_nr"), F.lit(9223372036854775807)),
    ).withColumn(
        "_e_seq_rank",
        F.dense_rank().over(
            Window.partitionBy("provider", "operating_date", "service_journey_id").orderBy(
                "_e_order_key",
                "aimed_departure_utc",
                "aimed_arrival_utc",
                "et_canonical_id",
            )
        ),
    ).withColumn(
        "_e_quay_visit_cnt",
        F.when(
            F.col("stop_point_ref").isNotNull(),
            F.count(F.lit(1)).over(
                Window.partitionBy(
                    "provider", "operating_date", "service_journey_id", "stop_point_ref"
                )
            ),
        ).otherwise(F.lit(0)),
    ).withColumn(
        "_e_visit_nr",
        F.when(
            F.col("stop_point_ref").isNotNull(),
            F.dense_rank().over(
                Window.partitionBy(
                    "provider", "operating_date", "service_journey_id", "stop_point_ref"
                ).orderBy(
                    "_e_order_key",
                    "aimed_departure_utc",
                    "aimed_arrival_utc",
                    "et_canonical_id",
                )
            ),
        ).otherwise(F.lit(None).cast("int")),
    )

    # Single-event timestamp errors do not disqualify stop/journey matching or the other event
    matchable_planned_statuses = [
        "resolved",
        "resolved_with_arrival_time_error",
        "resolved_with_departure_time_error",
    ]
    p_sched = p_base.filter(F.col("planned_call_status").isin(matchable_planned_statuses))
    p_non_sched_or_unresolved = p_base.filter(
        ~F.col("planned_call_status").isin(matchable_planned_statuses)
    )

    raw_pairs = (
        p_sched.alias("p")
        .join(
            e_base.alias("e"),
            on=(
                (F.col("p.provider") == F.col("e.provider"))
                & (F.col("p.operating_date") == F.col("e.operating_date"))
                & (F.col("p.service_journey_id") == F.col("e.service_journey_id"))
                & F.col("p.quay_id").isNotNull()
                & (F.col("p.quay_id") == F.col("e.stop_point_ref"))
            ),
            how="inner",
        )
        .withColumn(
            "_is_repeated_quay",
            (F.col("p._p_quay_visit_cnt") > 1) | (F.col("e._e_quay_visit_cnt") > 1),
        )
        .withColumn(
            "_visit_and_order_consistent",
            F.when(
                ~F.col("_is_repeated_quay"),
                F.lit(True),
            ).otherwise(
                (F.col("p._p_visit_nr") == F.col("e._e_visit_nr"))
                & (F.col("p._p_quay_visit_cnt") == F.col("e._e_quay_visit_cnt"))
                & (F.abs(F.col("p._p_seq_rank") - F.col("e._e_seq_rank")) <= F.lit(2))
            ),
        )
    )

    consistent_scored = (
        raw_pairs.filter(F.col("_visit_and_order_consistent"))
        .withColumn(
            "_p_cand_cnt",
            F.count(F.lit(1)).over(Window.partitionBy("p.planned_call_id")),
        )
        .withColumn(
            "_e_cand_cnt",
            F.count(F.lit(1)).over(Window.partitionBy("e.et_canonical_id")),
        )
    )

    accepted_pairs = consistent_scored.filter(
        (F.col("_p_cand_cnt") == 1) & (F.col("_e_cand_cnt") == 1)
    )

    matched_p_ids = accepted_pairs.select(F.col("p.planned_call_id").alias("_m_p_id")).distinct()
    matched_e_ids = accepted_pairs.select(F.col("e.et_canonical_id").alias("_m_e_id")).distinct()

    all_candidate_p_ids = raw_pairs.select(F.col("p.planned_call_id").alias("_c_p_id")).distinct()
    all_candidate_e_ids = raw_pairs.select(F.col("e.et_canonical_id").alias("_c_e_id")).distinct()

    ambiguous_p_ids = (
        all_candidate_p_ids.join(
            matched_p_ids, F.col("_c_p_id") == F.col("_m_p_id"), "left_anti"
        )
        .select(F.col("_c_p_id").alias("_a_p_id"))
        .distinct()
    )
    ambiguous_e_ids = (
        all_candidate_e_ids.join(
            matched_e_ids, F.col("_c_e_id") == F.col("_m_e_id"), "left_anti"
        )
        .select(F.col("_c_e_id").alias("_a_e_id"))
        .distinct()
    )

    ambiguous_planned_df = (
        p_sched.join(ambiguous_p_ids, F.col("planned_call_id") == F.col("_a_p_id"), "inner")
        .drop("_a_p_id")
        .join(
            selected_dates,
            on=["publication_id", "provider", "snapshot_date", "operating_date"],
            how="left",
        )
        .withColumn("reconciliation_status", F.lit("ambiguous"))
        .withColumn("et_canonical_id", F.lit(None).cast("string"))
        .withColumn("dated_service_journey_id", F.lit(None).cast("string"))
        .withColumn("operator_ref", F.col("resolved_operator_ref"))
        .withColumn("line_ref", F.col("resolved_line_id"))
        .withColumn("sequence_nr", F.lit(None).cast("long"))
        .withColumn("extra_journey", F.lit(None).cast("boolean"))
        .withColumn("extra_call", F.lit(None).cast("boolean"))
        .withColumn("journey_cancellation", F.lit(None).cast("boolean"))
        .withColumn("journey_cancellation_state", F.lit("unknown"))
        .withColumn("stop_cancellation", F.lit(None).cast("boolean"))
        .withColumn("stop_cancellation_state", F.lit("unknown"))
        .withColumn("estimated_flag", F.lit(None).cast("boolean"))
        .withColumn("arrival_time_kind", F.lit("missing"))
        .withColumn("departure_time_kind", F.lit("missing"))
        .withColumn("aimed_arrival_utc", F.lit(None).cast("timestamp"))
        .withColumn("reported_arrival_utc", F.lit(None).cast("timestamp"))
        .withColumn("aimed_departure_utc", F.lit(None).cast("timestamp"))
        .withColumn("reported_departure_utc", F.lit(None).cast("timestamp"))
        .withColumn("reported_arrival_delay_sec", F.lit(None).cast("double"))
        .withColumn("reported_departure_delay_sec", F.lit(None).cast("double"))
        .withColumn("source_occurrences", F.lit(None).cast("long"))
        .withColumn("candidate_key_variant_count", F.lit(None).cast("int"))
        .withColumn("duplicate_status", F.lit(None).cast("string"))
        .withColumn("et_quality_reason_codes", F.array().cast("array<string>"))
        .withColumn("siri_source_file", F.lit(None).cast("string"))
        .withColumn("siri_source_sha256", F.lit(None).cast("string"))
    )

    planned_only_df = (
        p_sched.join(matched_p_ids, F.col("planned_call_id") == F.col("_m_p_id"), "left_anti")
        .join(ambiguous_p_ids, F.col("planned_call_id") == F.col("_a_p_id"), "left_anti")
        .join(
            selected_dates,
            on=["publication_id", "provider", "snapshot_date", "operating_date"],
            how="left",
        )
        .withColumn("reconciliation_status", F.lit("planned_only"))
        .withColumn("et_canonical_id", F.lit(None).cast("string"))
        .withColumn("dated_service_journey_id", F.lit(None).cast("string"))
        .withColumn("operator_ref", F.col("resolved_operator_ref"))
        .withColumn("line_ref", F.col("resolved_line_id"))
        .withColumn("sequence_nr", F.lit(None).cast("long"))
        .withColumn("extra_journey", F.lit(None).cast("boolean"))
        .withColumn("extra_call", F.lit(None).cast("boolean"))
        .withColumn("journey_cancellation", F.lit(None).cast("boolean"))
        .withColumn("journey_cancellation_state", F.lit("unknown"))
        .withColumn("stop_cancellation", F.lit(None).cast("boolean"))
        .withColumn("stop_cancellation_state", F.lit("unknown"))
        .withColumn("estimated_flag", F.lit(None).cast("boolean"))
        .withColumn("arrival_time_kind", F.lit("missing"))
        .withColumn("departure_time_kind", F.lit("missing"))
        .withColumn("aimed_arrival_utc", F.lit(None).cast("timestamp"))
        .withColumn("reported_arrival_utc", F.lit(None).cast("timestamp"))
        .withColumn("aimed_departure_utc", F.lit(None).cast("timestamp"))
        .withColumn("reported_departure_utc", F.lit(None).cast("timestamp"))
        .withColumn("reported_arrival_delay_sec", F.lit(None).cast("double"))
        .withColumn("reported_departure_delay_sec", F.lit(None).cast("double"))
        .withColumn("source_occurrences", F.lit(None).cast("long"))
        .withColumn("candidate_key_variant_count", F.lit(None).cast("int"))
        .withColumn("duplicate_status", F.lit(None).cast("string"))
        .withColumn("et_quality_reason_codes", F.array().cast("array<string>"))
        .withColumn("siri_source_file", F.lit(None).cast("string"))
        .withColumn("siri_source_sha256", F.lit(None).cast("string"))
    )

    plan_unresolved_df = (
        p_non_sched_or_unresolved.join(
            selected_dates,
            on=["publication_id", "provider", "snapshot_date", "operating_date"],
            how="left",
        )
        .withColumn("reconciliation_status", F.lit("plan_unresolved"))
        .withColumn("et_canonical_id", F.lit(None).cast("string"))
        .withColumn("dated_service_journey_id", F.lit(None).cast("string"))
        .withColumn("operator_ref", F.col("resolved_operator_ref"))
        .withColumn("line_ref", F.col("resolved_line_id"))
        .withColumn("sequence_nr", F.lit(None).cast("long"))
        .withColumn("extra_journey", F.lit(None).cast("boolean"))
        .withColumn("extra_call", F.lit(None).cast("boolean"))
        .withColumn("journey_cancellation", F.lit(None).cast("boolean"))
        .withColumn("journey_cancellation_state", F.lit("unknown"))
        .withColumn("stop_cancellation", F.lit(None).cast("boolean"))
        .withColumn("stop_cancellation_state", F.lit("unknown"))
        .withColumn("estimated_flag", F.lit(None).cast("boolean"))
        .withColumn("arrival_time_kind", F.lit("missing"))
        .withColumn("departure_time_kind", F.lit("missing"))
        .withColumn("aimed_arrival_utc", F.lit(None).cast("timestamp"))
        .withColumn("reported_arrival_utc", F.lit(None).cast("timestamp"))
        .withColumn("aimed_departure_utc", F.lit(None).cast("timestamp"))
        .withColumn("reported_departure_utc", F.lit(None).cast("timestamp"))
        .withColumn("reported_arrival_delay_sec", F.lit(None).cast("double"))
        .withColumn("reported_departure_delay_sec", F.lit(None).cast("double"))
        .withColumn("source_occurrences", F.lit(None).cast("long"))
        .withColumn("candidate_key_variant_count", F.lit(None).cast("int"))
        .withColumn("duplicate_status", F.lit(None).cast("string"))
        .withColumn("et_quality_reason_codes", F.array().cast("array<string>"))
        .withColumn("siri_source_file", F.lit(None).cast("string"))
        .withColumn("siri_source_sha256", F.lit(None).cast("string"))
    )

    matched_df = (
        accepted_pairs.select(
            F.col("p.publication_id").alias("publication_id"),
            F.col("p.provider").alias("provider"),
            F.col("p.snapshot_date").alias("snapshot_date"),
            F.col("p.operating_date").alias("operating_date"),
            F.col("p.service_journey_id").alias("service_journey_id"),
            F.col("p.service_journey_version").alias("service_journey_version"),
            F.col("p.resolved_line_id").alias("resolved_line_id"),
            F.col("p.resolved_public_code").alias("resolved_public_code"),
            F.col("p.resolved_transport_mode").alias("resolved_transport_mode"),
            F.col("p.resolved_route_id").alias("resolved_route_id"),
            F.col("p.resolved_journey_pattern_id").alias("resolved_journey_pattern_id"),
            F.col("p.resolved_operator_ref").alias("resolved_operator_ref"),
            F.col("p.passing_time_index").alias("passing_time_index"),
            F.col("p.pattern_point_index").alias("pattern_point_index"),
            F.col("p.stop_point_in_pattern_ref").alias("stop_point_in_pattern_ref"),
            F.col("p.scheduled_stop_point_ref").alias("scheduled_stop_point_ref"),
            F.col("p.stop_point_name").alias("stop_point_name"),
            F.col("p.quay_id").alias("quay_id"),
            F.col("p.stop_assignment_status").alias("stop_assignment_status"),
            F.col("p.is_flexible_window").alias("is_flexible_window"),
            F.col("p.planned_call_status").alias("planned_call_status"),
            F.col("p.planned_arrival_utc").alias("planned_arrival_utc"),
            F.col("p.planned_arrival_time_error").alias("planned_arrival_time_error"),
            F.col("p.planned_departure_utc").alias("planned_departure_utc"),
            F.col("p.planned_departure_time_error").alias("planned_departure_time_error"),
            F.col("p.planned_call_id").alias("planned_call_id"),
            F.col("e.et_canonical_id").alias("et_canonical_id"),
            F.col("e.dated_service_journey_id").alias("dated_service_journey_id"),
            F.col("e.operator_ref").alias("operator_ref"),
            F.col("e.line_ref").alias("line_ref"),
            F.col("e.sequence_nr").alias("sequence_nr"),
            F.col("e.extra_journey").alias("extra_journey"),
            F.col("e.extra_call").alias("extra_call"),
            F.col("e.journey_cancellation").alias("journey_cancellation"),
            F.col("e.journey_cancellation_state").alias("journey_cancellation_state"),
            F.col("e.stop_cancellation").alias("stop_cancellation"),
            F.col("e.stop_cancellation_state").alias("stop_cancellation_state"),
            F.col("e.estimated_flag").alias("estimated_flag"),
            F.col("e.arrival_time_kind").alias("arrival_time_kind"),
            F.col("e.departure_time_kind").alias("departure_time_kind"),
            F.col("e.aimed_arrival_utc").alias("aimed_arrival_utc"),
            F.col("e.reported_arrival_utc").alias("reported_arrival_utc"),
            F.col("e.aimed_departure_utc").alias("aimed_departure_utc"),
            F.col("e.reported_departure_utc").alias("reported_departure_utc"),
            F.col("e.reported_arrival_delay_sec").alias("reported_arrival_delay_sec"),
            F.col("e.reported_departure_delay_sec").alias("reported_departure_delay_sec"),
            F.col("e.source_occurrences").alias("source_occurrences"),
            F.col("e.candidate_key_variant_count").alias("candidate_key_variant_count"),
            F.col("e.duplicate_status").alias("duplicate_status"),
            F.col("e.quality_reason_codes").alias("et_quality_reason_codes"),
            F.col("e.siri_source_file").alias("siri_source_file"),
            F.col("e.siri_source_sha256").alias("siri_source_sha256"),
        )
        .join(
            selected_dates,
            on=["publication_id", "provider", "snapshot_date", "operating_date"],
            how="left",
        )
        .withColumn("reconciliation_status", F.lit("matched"))
    )

    unmatched_or_amb_et = (
        e_base.join(matched_e_ids, F.col("et_canonical_id") == F.col("_m_e_id"), "left_anti")
        .join(
            selected_dates.select(
                "provider",
                "operating_date",
                "publication_id",
                "snapshot_date",
                "siri_file_available",
            ),
            on=["provider", "operating_date"],
            how="left",
        )
        .join(ambiguous_e_ids, F.col("et_canonical_id") == F.col("_a_e_id"), "left")
        .withColumn(
            "reconciliation_status",
            F.when(F.col("_a_e_id").isNotNull(), F.lit("ambiguous")).otherwise(
                F.lit("reported_only")
            ),
        )
        .drop("_a_e_id")
        .withColumn("service_journey_version", F.lit(None).cast("string"))
        .withColumn("resolved_line_id", F.lit(None).cast("string"))
        .withColumn("resolved_public_code", F.lit(None).cast("string"))
        .withColumn("resolved_transport_mode", F.lit(None).cast("string"))
        .withColumn("resolved_route_id", F.lit(None).cast("string"))
        .withColumn("resolved_journey_pattern_id", F.lit(None).cast("string"))
        .withColumn("resolved_operator_ref", F.lit(None).cast("string"))
        .withColumn("passing_time_index", F.lit(None).cast("int"))
        .withColumn("pattern_point_index", F.lit(None).cast("int"))
        .withColumn("stop_point_in_pattern_ref", F.lit(None).cast("string"))
        .withColumn("scheduled_stop_point_ref", F.lit(None).cast("string"))
        .withColumn("quay_id", F.col("stop_point_ref"))
        .withColumn("stop_assignment_status", F.lit(None).cast("string"))
        .withColumn("is_flexible_window", F.lit(False))
        .withColumn("planned_call_status", F.lit(None).cast("string"))
        .withColumn("planned_arrival_utc", F.lit(None).cast("timestamp"))
        .withColumn("planned_arrival_time_error", F.lit(None).cast("string"))
        .withColumn("planned_departure_utc", F.lit(None).cast("timestamp"))
        .withColumn("planned_departure_time_error", F.lit(None).cast("string"))
        .withColumn("planned_call_id", F.lit(None).cast("string"))
        .withColumnRenamed("quality_reason_codes", "et_quality_reason_codes")
    )

    union_cols = [
        "publication_id",
        "provider",
        "snapshot_date",
        "operating_date",
        "siri_file_available",
        "reconciliation_status",
        "planned_call_id",
        "et_canonical_id",
        "service_journey_id",
        "service_journey_version",
        "dated_service_journey_id",
        "resolved_line_id",
        "line_ref",
        "resolved_public_code",
        "resolved_transport_mode",
        "resolved_route_id",
        "resolved_journey_pattern_id",
        "resolved_operator_ref",
        "operator_ref",
        "passing_time_index",
        "pattern_point_index",
        "sequence_nr",
        "stop_point_in_pattern_ref",
        "scheduled_stop_point_ref",
        "quay_id",
        "stop_point_name",
        "stop_assignment_status",
        "is_flexible_window",
        "planned_call_status",
        "extra_journey",
        "extra_call",
        "journey_cancellation",
        "journey_cancellation_state",
        "stop_cancellation",
        "stop_cancellation_state",
        "estimated_flag",
        "arrival_time_kind",
        "departure_time_kind",
        "planned_arrival_utc",
        "planned_arrival_time_error",
        "aimed_arrival_utc",
        "reported_arrival_utc",
        "planned_departure_utc",
        "planned_departure_time_error",
        "aimed_departure_utc",
        "reported_departure_utc",
        "reported_arrival_delay_sec",
        "reported_departure_delay_sec",
        "source_occurrences",
        "candidate_key_variant_count",
        "duplicate_status",
        "et_quality_reason_codes",
        "siri_source_file",
        "siri_source_sha256",
    ]

    combined = (
        matched_df.select(*union_cols)
        .unionByName(ambiguous_planned_df.select(*union_cols))
        .unionByName(planned_only_df.select(*union_cols))
        .unionByName(plan_unresolved_df.select(*union_cols))
        .unionByName(unmatched_or_amb_et.select(*union_cols))
        .join(
            jsd_lookup,
            on=["publication_id", "operating_date", "service_journey_id"],
            how="left",
        )
    )

    with_metrics_and_eligibility = (
        combined.withColumn(
            "netex_arrival_deviation_sec",
            F.when(
                F.col("planned_arrival_time_error").isNull(),
                _diff_seconds_col(F.col("reported_arrival_utc"), F.col("planned_arrival_utc")),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "netex_departure_deviation_sec",
            F.when(
                F.col("planned_departure_time_error").isNull(),
                _diff_seconds_col(F.col("reported_departure_utc"), F.col("planned_departure_utc")),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "planned_arrival_disagreement_sec",
            F.when(
                F.col("planned_arrival_time_error").isNull(),
                _diff_seconds_col(F.col("aimed_arrival_utc"), F.col("planned_arrival_utc")),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "planned_departure_disagreement_sec",
            F.when(
                F.col("planned_departure_time_error").isNull(),
                _diff_seconds_col(F.col("aimed_departure_utc"), F.col("planned_departure_utc")),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "_not_cancelled",
            (F.col("journey_cancellation_state") == "not_cancelled")
            & (F.col("stop_cancellation_state") == "not_cancelled"),
        )
        .withColumn(
            "_dup_ok",
            F.col("duplicate_status").isin("unique", "collapsed_exact_duplicates"),
        )
        .withColumn(
            "_not_flexible",
            ~F.coalesce(F.col("is_flexible_window"), F.lit(False)),
        )
        .withColumn(
            "eligible_reported_departure_delay",
            (F.col("departure_time_kind") == "recorded")
            & F.col("aimed_departure_utc").isNotNull()
            & F.col("_dup_ok")
            & F.col("_not_cancelled")
            & F.col("_not_flexible"),
        )
        .withColumn(
            "exclusion_reported_departure_delay",
            F.array_compact(
                F.array(
                    F.when(
                        F.col("departure_time_kind") == "missing",
                        F.lit("missing_recorded_departure"),
                    ),
                    F.when(F.col("departure_time_kind") == "estimated", F.lit("estimated_time")),
                    F.when(
                        F.col("departure_time_kind") == "unresolved",
                        F.lit("unknown_time_classification"),
                    ),
                    F.when(
                        F.col("aimed_departure_utc").isNull(), F.lit("missing_aimed_departure")
                    ),
                    F.when(
                        F.col("duplicate_status") == "conflicting_candidate_key",
                        F.lit("conflicting_reported_record"),
                    ),
                    F.when(
                        F.col("journey_cancellation_state").isin(
                            "cancelled", "conflicting_across_stops"
                        ),
                        F.lit("cancelled_journey"),
                    ),
                    F.when(F.col("stop_cancellation_state") == "cancelled", F.lit("cancelled_stop")),
                    F.when(
                        F.col("stop_cancellation_state") == "unknown",
                        F.lit("unknown_stop_cancellation"),
                    ),
                    F.when(
                        F.coalesce(F.col("is_flexible_window"), F.lit(False)),
                        F.lit("flexible_service_window"),
                    ),
                )
            ),
        )
        .withColumn(
            "eligible_reported_arrival_delay",
            (F.col("arrival_time_kind") == "recorded")
            & F.col("aimed_arrival_utc").isNotNull()
            & F.col("_dup_ok")
            & F.col("_not_cancelled")
            & F.col("_not_flexible"),
        )
        .withColumn(
            "exclusion_reported_arrival_delay",
            F.array_compact(
                F.array(
                    F.when(
                        F.col("arrival_time_kind") == "missing", F.lit("missing_recorded_arrival")
                    ),
                    F.when(F.col("arrival_time_kind") == "estimated", F.lit("estimated_time")),
                    F.when(
                        F.col("arrival_time_kind") == "unresolved",
                        F.lit("unknown_time_classification"),
                    ),
                    F.when(F.col("aimed_arrival_utc").isNull(), F.lit("missing_aimed_arrival")),
                    F.when(
                        F.col("duplicate_status") == "conflicting_candidate_key",
                        F.lit("conflicting_reported_record"),
                    ),
                    F.when(
                        F.col("journey_cancellation_state").isin(
                            "cancelled", "conflicting_across_stops"
                        ),
                        F.lit("cancelled_journey"),
                    ),
                    F.when(F.col("stop_cancellation_state") == "cancelled", F.lit("cancelled_stop")),
                    F.when(
                        F.col("stop_cancellation_state") == "unknown",
                        F.lit("unknown_stop_cancellation"),
                    ),
                    F.when(
                        F.coalesce(F.col("is_flexible_window"), F.lit(False)),
                        F.lit("flexible_service_window"),
                    ),
                )
            ),
        )
        .withColumn(
            "eligible_netex_departure_comparison",
            (F.col("reconciliation_status") == "matched")
            & (F.col("departure_time_kind") == "recorded")
            & F.col("planned_departure_utc").isNotNull()
            & F.col("planned_departure_time_error").isNull()
            & F.col("_dup_ok")
            & F.col("_not_cancelled")
            & F.col("_not_flexible"),
        )
        .withColumn(
            "exclusion_netex_departure_comparison",
            F.array_compact(
                F.array(
                    F.when(
                        F.col("reconciliation_status") == "planned_only",
                        F.lit("unmatched_planned_call"),
                    ),
                    F.when(
                        F.col("reconciliation_status") == "reported_only",
                        F.lit("unmatched_reported_call"),
                    ),
                    F.when(
                        F.col("reconciliation_status") == "ambiguous",
                        F.lit("ambiguous_stop_visit"),
                    ),
                    F.when(
                        F.col("reconciliation_status") == "plan_unresolved",
                        F.lit("unresolved_calendar"),
                    ),
                    F.when(
                        F.col("departure_time_kind") == "missing",
                        F.lit("missing_recorded_departure"),
                    ),
                    F.when(F.col("departure_time_kind") == "estimated", F.lit("estimated_time")),
                    F.when(
                        F.col("planned_departure_utc").isNull(), F.lit("missing_netex_departure")
                    ),
                    F.when(
                        F.col("planned_departure_time_error").isNotNull(),
                        F.concat(F.lit("planned_departure_error:"), F.col("planned_departure_time_error")),
                    ),
                    F.when(
                        F.col("duplicate_status") == "conflicting_candidate_key",
                        F.lit("conflicting_reported_record"),
                    ),
                    F.when(F.col("stop_cancellation_state") == "cancelled", F.lit("cancelled_stop")),
                    F.when(
                        F.coalesce(F.col("is_flexible_window"), F.lit(False)),
                        F.lit("flexible_service_window"),
                    ),
                )
            ),
        )
        .withColumn(
            "eligible_netex_arrival_comparison",
            (F.col("reconciliation_status") == "matched")
            & (F.col("arrival_time_kind") == "recorded")
            & F.col("planned_arrival_utc").isNotNull()
            & F.col("planned_arrival_time_error").isNull()
            & F.col("_dup_ok")
            & F.col("_not_cancelled")
            & F.col("_not_flexible"),
        )
        .withColumn(
            "exclusion_netex_arrival_comparison",
            F.array_compact(
                F.array(
                    F.when(
                        F.col("reconciliation_status") == "planned_only",
                        F.lit("unmatched_planned_call"),
                    ),
                    F.when(
                        F.col("reconciliation_status") == "reported_only",
                        F.lit("unmatched_reported_call"),
                    ),
                    F.when(
                        F.col("reconciliation_status") == "ambiguous",
                        F.lit("ambiguous_stop_visit"),
                    ),
                    F.when(
                        F.col("reconciliation_status") == "plan_unresolved",
                        F.lit("unresolved_calendar"),
                    ),
                    F.when(
                        F.col("arrival_time_kind") == "missing", F.lit("missing_recorded_arrival")
                    ),
                    F.when(F.col("arrival_time_kind") == "estimated", F.lit("estimated_time")),
                    F.when(F.col("planned_arrival_utc").isNull(), F.lit("missing_netex_arrival")),
                    F.when(
                        F.col("planned_arrival_time_error").isNotNull(),
                        F.concat(F.lit("planned_arrival_error:"), F.col("planned_arrival_time_error")),
                    ),
                    F.when(
                        F.col("duplicate_status") == "conflicting_candidate_key",
                        F.lit("conflicting_reported_record"),
                    ),
                    F.when(F.col("stop_cancellation_state") == "cancelled", F.lit("cancelled_stop")),
                    F.when(
                        F.coalesce(F.col("is_flexible_window"), F.lit(False)),
                        F.lit("flexible_service_window"),
                    ),
                )
            ),
        )
        .withColumn(
            "eligible_planned_call_coverage",
            F.col("planned_call_id").isNotNull()
            & (
                F.col("planned_call_status").isin(
                    "resolved",
                    "resolved_with_arrival_time_error",
                    "resolved_with_departure_time_error",
                )
            )
            & F.col("quay_id").isNotNull(),
        )
        .withColumn(
            "exclusion_planned_call_coverage",
            F.array_compact(
                F.array(
                    F.when(F.col("planned_call_id").isNull(), F.lit("not_a_planned_call")),
                    F.when(
                        F.col("planned_call_status") == "flexible_service_window",
                        F.lit("flexible_service_window"),
                    ),
                    F.when(
                        F.col("planned_call_status") == "unresolved_quay",
                        F.lit("unresolved_quay"),
                    ),
                    F.when(
                        F.col("planned_call_status").isin(
                            "unresolved_pattern_stop", "ambiguous_pattern_stop"
                        ),
                        F.lit("unresolved_pattern_stop"),
                    ),
                    F.when(
                        F.col("planned_call_status") == "invalid_planned_timestamps",
                        F.lit("invalid_planned_timestamps"),
                    ),
                )
            ),
        )
        .withColumn(
            "eligible_reported_cancellation_analysis",
            F.col("et_canonical_id").isNotNull()
            & F.col("_dup_ok")
            & (
                F.col("journey_cancellation_state").isin("cancelled", "not_cancelled")
                | F.col("stop_cancellation_state").isin("cancelled", "not_cancelled")
            ),
        )
        .withColumn(
            "exclusion_reported_cancellation_analysis",
            F.array_compact(
                F.array(
                    F.when(F.col("et_canonical_id").isNull(), F.lit("missing_reported_record")),
                    F.when(
                        F.col("duplicate_status") == "conflicting_candidate_key",
                        F.lit("conflicting_reported_record"),
                    ),
                    F.when(
                        F.col("journey_cancellation_state") == "conflicting_across_stops",
                        F.lit("conflicting_journey_cancellation"),
                    ),
                    F.when(
                        (F.col("journey_cancellation_state") == "unknown")
                        & (F.col("stop_cancellation_state") == "unknown"),
                        F.lit("unknown_cancellation_status"),
                    ),
                )
            ),
        )
        .drop("_not_cancelled", "_dup_ok", "_not_flexible")
    )

    return with_metrics_and_eligibility