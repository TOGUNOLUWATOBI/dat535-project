"""Journey performance, line service, and monthly/seasonal period rollups for Gold."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from pipeline.Gold import (
    DEFAULT_PERCENTILE_ACCURACY,
    GOLD_SCHEMA_VERSION,
    METRIC_POLICY_VERSION,
    SEASON_WINDOWS_2026,
)
from pipeline.Gold.event_metrics import (
    aggregate_punctuality,
    normalize_journey_key_expr,
    normalize_transport_mode_expr,
)
from pipeline.Gold.inputs import SilverInventory, read_silver_table_for_inventory


def build_journey_performance(
    spark: SparkSession,
    inventory: SilverInventory,
    *,
    gold_run_id: str,
) -> DataFrame:
    """Build journey_performance at (provider, operating_date, journey_key) grain."""
    jsd_df = (
        read_silver_table_for_inventory(
            spark,
            inventory,
            table_name="journey_service_dates",
            columns=[
                "provider",
                "operating_date",
                "publication_id",
                "service_journey_id",
                "resolved_line_id",
                "resolved_transport_mode",
                "service_status",
            ],
        )
        .withColumn("dated_service_journey_id", F.lit(None).cast("string"))
        .withColumn("journey_key", normalize_journey_key_expr("dated_service_journey_id", "service_journey_id"))
    )

    nsc_df = (
        read_silver_table_for_inventory(
            spark,
            inventory,
            table_name="netex_stop_calls",
            columns=[
                "provider",
                "operating_date",
                "publication_id",
                "service_journey_id",
                "passing_time_id",
                "quay_id",
                "passing_time_index",
                "planned_departure_utc",
                "planned_arrival_utc",
            ],
        )
        .withColumn("dated_service_journey_id", F.lit(None).cast("string"))
        .withColumn("journey_key", normalize_journey_key_expr("dated_service_journey_id", "service_journey_id"))
    )

    siri_df = read_silver_table_for_inventory(
        spark,
        inventory,
        table_name="siri_et_stop_calls",
        columns=[
            "provider",
            "et_canonical_id",
            "service_journey_id",
            "dated_service_journey_id",
            "line_ref",
            "stop_point_ref",
            "aimed_departure_utc",
            "aimed_arrival_utc",
            "reported_departure_utc",
            "reported_arrival_utc",
            "departure_time_kind",
            "arrival_time_kind",
            "journey_cancellation_state",
            "stop_cancellation_state",
            "duplicate_status",
        ],
    ).withColumn("journey_key", normalize_journey_key_expr("dated_service_journey_id", "service_journey_id"))

    rec_df = read_silver_table_for_inventory(
        spark,
        inventory,
        table_name="reconciled_stop_calls",
        columns=[
            "provider",
            "operating_date",
            "publication_id",
            "et_canonical_id",
            "planned_call_id",
            "service_journey_id",
            "dated_service_journey_id",
            "reconciliation_status",
            "reported_departure_delay_sec",
            "reported_arrival_delay_sec",
            "netex_departure_deviation_sec",
            "netex_arrival_deviation_sec",
            "eligible_reported_departure_delay",
            "eligible_reported_arrival_delay",
            "eligible_netex_departure_comparison",
            "eligible_netex_arrival_comparison",
        ],
    ).withColumn("journey_key", normalize_journey_key_expr("dated_service_journey_id", "service_journey_id"))

    inv_meta = spark.createDataFrame(
        [
            (a.provider, a.operating_date, a.source_silver_run_id, a.publication_id)
            for a in inventory.authorities
        ],
        schema=["provider", "operating_date_str", "source_silver_run_id", "publication_id"],
    ).withColumn("operating_date", F.to_date("operating_date_str")).drop("operating_date_str")

    siri_with_date = siri_df.join(
        rec_df.select("provider", "operating_date", "publication_id", "et_canonical_id").dropDuplicates(
            ["provider", "et_canonical_id"]
        ),
        on=["provider", "et_canonical_id"],
        how="left",
    ).join(inv_meta, on=["provider", "operating_date", "publication_id"], how="inner")

    nsc_keyed = nsc_df.filter(F.col("journey_key").isNotNull())
    seq_col = F.coalesce(F.col("passing_time_index").cast("long"), F.lit(0).cast("long"))
    w_asc = Window.partitionBy("provider", "operating_date", "publication_id", "journey_key").orderBy(
        seq_col.asc(), F.col("passing_time_id").asc()
    )
    w_desc = Window.partitionBy("provider", "operating_date", "publication_id", "journey_key").orderBy(
        seq_col.desc(), F.col("passing_time_id").desc()
    )
    w_cnt = Window.partitionBy("provider", "operating_date", "publication_id", "journey_key")

    nsc_ranked = (
        nsc_keyed.withColumn("_rn_asc", F.row_number().over(w_asc))
        .withColumn("_rn_desc", F.row_number().over(w_desc))
        .withColumn("_nsc_cnt", F.count(F.lit(1)).over(w_cnt))
    )

    endpoints_df = (
        nsc_ranked.filter((F.col("_rn_asc") == 1) | (F.col("_rn_desc") == 1))
        .groupBy("provider", "operating_date", "publication_id", "journey_key")
        .agg(
            F.max("_nsc_cnt").cast("long").alias("planned_call_count"),
            F.first(F.when(F.col("_rn_asc") == 1, F.col("passing_time_id")), ignorenulls=True).alias(
                "origin_planned_call_id"
            ),
            F.first(F.when(F.col("_rn_desc") == 1, F.col("passing_time_id")), ignorenulls=True).alias(
                "terminal_planned_call_id"
            ),
            F.first(F.when(F.col("_rn_asc") == 1, F.col("quay_id")), ignorenulls=True).alias("origin_quay_id"),
            F.first(F.when(F.col("_rn_desc") == 1, F.col("quay_id")), ignorenulls=True).alias("terminal_quay_id"),
            F.first(F.when(F.col("_rn_asc") == 1, F.col("planned_departure_utc")), ignorenulls=True).alias(
                "origin_planned_departure_utc"
            ),
            F.first(F.when(F.col("_rn_desc") == 1, F.col("planned_arrival_utc")), ignorenulls=True).alias(
                "terminal_planned_arrival_utc"
            ),
        )
        .withColumn(
            "endpoint_resolution_status",
            F.when(
                F.col("origin_planned_call_id").isNotNull() & F.col("terminal_planned_call_id").isNotNull(),
                F.lit("resolved"),
            ).otherwise(F.lit("unresolved")),
        )
    )

    siri_keyed = siri_with_date.filter(F.col("journey_key").isNotNull())
    has_rec_arr = F.col("reported_arrival_utc").isNotNull() & (
        F.lower(F.coalesce(F.col("arrival_time_kind").cast("string"), F.lit(""))) != F.lit("estimated")
    )
    has_rec_dep = F.col("reported_departure_utc").isNotNull() & (
        F.lower(F.coalesce(F.col("departure_time_kind").cast("string"), F.lit(""))) != F.lit("estimated")
    )
    is_est = (
        F.lower(F.coalesce(F.col("arrival_time_kind").cast("string"), F.lit(""))) == F.lit("estimated")
    ) | (
        F.lower(F.coalesce(F.col("departure_time_kind").cast("string"), F.lit(""))) == F.lit("estimated")
    )
    is_conflict_var = F.lower(F.coalesce(F.col("duplicate_status").cast("string"), F.lit(""))).isin(
        "conflict", "conflicting", "duplicate"
    )

    siri_journey_agg = siri_keyed.groupBy("provider", "operating_date", "publication_id", "journey_key").agg(
        F.countDistinct("et_canonical_id").cast("long").alias("reported_canonical_call_count"),
        F.sum(F.when(has_rec_arr, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("recorded_arrival_count"),
        F.sum(F.when(has_rec_dep, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("recorded_departure_count"),
        F.sum(F.when(is_est, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("estimated_call_count"),
        F.sum(F.when(is_conflict_var, F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("conflicting_reported_call_count"),
        F.first("line_ref", ignorenulls=True).alias("reported_line_id"),
        F.max(F.when(F.col("journey_cancellation_state") == F.lit("cancelled"), F.lit(1)).otherwise(F.lit(0))).alias(
            "_has_cancelled"
        ),
        F.max(
            F.when(F.col("journey_cancellation_state") == F.lit("not_cancelled"), F.lit(1)).otherwise(F.lit(0))
        ).alias("_has_not_cancelled"),
    )

    rec_keyed = rec_df.filter(F.col("journey_key").isNotNull())
    rec_journey_agg = rec_keyed.groupBy("provider", "operating_date", "publication_id", "journey_key").agg(
        F.countDistinct(
            F.when(F.col("reconciliation_status") == F.lit("matched"), F.col("planned_call_id"))
        )
        .cast("long")
        .alias("matched_call_count"),
    )

    jsd_keyed = jsd_df.filter(F.col("journey_key").isNotNull()).groupBy(
        "provider", "operating_date", "publication_id", "journey_key"
    ).agg(
        F.first("service_journey_id", ignorenulls=True).alias("service_journey_id"),
        F.first("dated_service_journey_id", ignorenulls=True).alias("dated_service_journey_id"),
        F.first("resolved_line_id", ignorenulls=True).alias("planned_line_id"),
        F.first("resolved_transport_mode", ignorenulls=True).alias("transport_mode"),
        F.first("service_status", ignorenulls=True).alias("service_status"),
    )

    all_keys = (
        jsd_keyed.select("provider", "operating_date", "publication_id", "journey_key")
        .union(siri_journey_agg.select("provider", "operating_date", "publication_id", "journey_key"))
        .dropDuplicates(["provider", "operating_date", "publication_id", "journey_key"])
    )

    joined = (
        all_keys.join(inv_meta, on=["provider", "operating_date", "publication_id"], how="inner")
        .join(jsd_keyed, on=["provider", "operating_date", "publication_id", "journey_key"], how="left")
        .join(endpoints_df, on=["provider", "operating_date", "publication_id", "journey_key"], how="left")
        .join(siri_journey_agg, on=["provider", "operating_date", "publication_id", "journey_key"], how="left")
        .join(rec_journey_agg, on=["provider", "operating_date", "publication_id", "journey_key"], how="left")
    )

    status_norm = F.lower(F.coalesce(F.col("service_status").cast("string"), F.lit("")))
    is_scheduled = status_norm.isin("scheduled", "active", "operating")
    planned_service_status_expr = (
        F.when(status_norm == F.lit(""), F.lit("no_planned_definition"))
        .when(is_scheduled, F.lit("scheduled"))
        .when(status_norm.isin("not_scheduled", "inactive"), F.lit("not_scheduled"))
        .otherwise(F.lit("unresolved"))
    )

    has_rep = F.coalesce(F.col("reported_canonical_call_count"), F.lit(0)) > F.lit(0)
    outside_sched = has_rep & (~is_scheduled)

    j_cancel_state = (
        F.when((F.col("_has_cancelled") == 1) & (F.col("_has_not_cancelled") == 1), F.lit("conflicting"))
        .when(F.col("_has_cancelled") == 1, F.lit("cancelled"))
        .when(F.col("_has_not_cancelled") == 1, F.lit("not_cancelled"))
        .otherwise(F.lit("unknown"))
    )

    ep_resolved = F.col("endpoint_resolution_status") == F.lit("resolved")

    siri_ep = (
        siri_keyed.filter(F.col("stop_point_ref").isNotNull())
        .groupBy("provider", "operating_date", "publication_id", "journey_key", "stop_point_ref")
        .agg(
            F.first("aimed_departure_utc", ignorenulls=True).alias("_ep_aimed_dep"),
            F.first("aimed_arrival_utc", ignorenulls=True).alias("_ep_aimed_arr"),
            F.first("reported_departure_utc", ignorenulls=True).alias("_ep_rep_dep"),
            F.first("reported_arrival_utc", ignorenulls=True).alias("_ep_rep_arr"),
            F.first("departure_time_kind", ignorenulls=True).alias("_ep_dep_kind"),
            F.first("arrival_time_kind", ignorenulls=True).alias("_ep_arr_kind"),
        )
    )

    rec_ep = (
        rec_keyed.filter(F.col("planned_call_id").isNotNull())
        .groupBy("provider", "operating_date", "publication_id", "journey_key", "planned_call_id")
        .agg(
            F.first("reported_departure_delay_sec", ignorenulls=True).alias("_ep_et_dep_delay"),
            F.first("reported_arrival_delay_sec", ignorenulls=True).alias("_ep_et_arr_delay"),
            F.first("netex_departure_deviation_sec", ignorenulls=True).alias("_ep_nx_dep_delay"),
            F.first("netex_arrival_deviation_sec", ignorenulls=True).alias("_ep_nx_arr_delay"),
            F.max(
                F.when(F.coalesce(F.col("eligible_reported_departure_delay"), F.lit(False)), 1).otherwise(0)
            ).alias("_ep_et_dep_elig"),
            F.max(
                F.when(F.coalesce(F.col("eligible_reported_arrival_delay"), F.lit(False)), 1).otherwise(0)
            ).alias("_ep_et_arr_elig"),
            F.max(
                F.when(F.coalesce(F.col("eligible_netex_departure_comparison"), F.lit(False)), 1).otherwise(0)
            ).alias("_ep_nx_dep_elig"),
            F.max(
                F.when(F.coalesce(F.col("eligible_netex_arrival_comparison"), F.lit(False)), 1).otherwise(0)
            ).alias("_ep_nx_arr_elig"),
        )
    )

    siri_o = siri_ep.select(
        F.col("provider").alias("_so_p"),
        F.col("operating_date").alias("_so_d"),
        F.col("publication_id").alias("_so_pub"),
        F.col("journey_key").alias("_so_jk"),
        F.col("stop_point_ref").alias("_so_sp"),
        F.col("_ep_aimed_dep").alias("origin_aimed_departure_utc"),
        F.col("_ep_rep_dep").alias("origin_reported_departure_utc"),
        F.col("_ep_dep_kind").alias("origin_departure_time_kind"),
    )
    siri_t = siri_ep.select(
        F.col("provider").alias("_st_p"),
        F.col("operating_date").alias("_st_d"),
        F.col("publication_id").alias("_st_pub"),
        F.col("journey_key").alias("_st_jk"),
        F.col("stop_point_ref").alias("_st_sp"),
        F.col("_ep_aimed_arr").alias("terminal_aimed_arrival_utc"),
        F.col("_ep_rep_arr").alias("terminal_reported_arrival_utc"),
        F.col("_ep_arr_kind").alias("terminal_arrival_time_kind"),
    )
    rec_o = rec_ep.select(
        F.col("provider").alias("_ro_p"),
        F.col("operating_date").alias("_ro_d"),
        F.col("publication_id").alias("_ro_pub"),
        F.col("journey_key").alias("_ro_jk"),
        F.col("planned_call_id").alias("_ro_cid"),
        F.col("_ep_et_dep_delay").alias("_et_o_dep_delay"),
        F.col("_ep_nx_dep_delay").alias("_nx_o_dep_delay"),
        F.col("_ep_et_dep_elig").alias("_et_o_dep_elig"),
        F.col("_ep_nx_dep_elig").alias("_nx_o_dep_elig"),
    )
    rec_t = rec_ep.select(
        F.col("provider").alias("_rt_p"),
        F.col("operating_date").alias("_rt_d"),
        F.col("publication_id").alias("_rt_pub"),
        F.col("journey_key").alias("_rt_jk"),
        F.col("planned_call_id").alias("_rt_cid"),
        F.col("_ep_et_arr_delay").alias("_et_t_arr_delay"),
        F.col("_ep_nx_arr_delay").alias("_nx_t_arr_delay"),
        F.col("_ep_et_arr_elig").alias("_et_t_arr_elig"),
        F.col("_ep_nx_arr_elig").alias("_nx_t_arr_elig"),
    )

    enriched = (
        joined.join(
            siri_o,
            (joined["provider"] == siri_o["_so_p"])
            & (joined["operating_date"] == siri_o["_so_d"])
            & (joined["publication_id"] == siri_o["_so_pub"])
            & (joined["journey_key"] == siri_o["_so_jk"])
            & (joined["origin_quay_id"] == siri_o["_so_sp"]),
            how="left",
        )
        .join(
            siri_t,
            (joined["provider"] == siri_t["_st_p"])
            & (joined["operating_date"] == siri_t["_st_d"])
            & (joined["publication_id"] == siri_t["_st_pub"])
            & (joined["journey_key"] == siri_t["_st_jk"])
            & (joined["terminal_quay_id"] == siri_t["_st_sp"]),
            how="left",
        )
        .join(
            rec_o,
            (joined["provider"] == rec_o["_ro_p"])
            & (joined["operating_date"] == rec_o["_ro_d"])
            & (joined["publication_id"] == rec_o["_ro_pub"])
            & (joined["journey_key"] == rec_o["_ro_jk"])
            & (joined["origin_planned_call_id"] == rec_o["_ro_cid"]),
            how="left",
        )
        .join(
            rec_t,
            (joined["provider"] == rec_t["_rt_p"])
            & (joined["operating_date"] == rec_t["_rt_d"])
            & (joined["publication_id"] == rec_t["_rt_pub"])
            & (joined["journey_key"] == rec_t["_rt_jk"])
            & (joined["terminal_planned_call_id"] == rec_t["_rt_cid"]),
            how="left",
        )
    )

    o_present = F.col("origin_reported_departure_utc").isNotNull() | F.col("origin_aimed_departure_utc").isNotNull()
    t_present = F.col("terminal_reported_arrival_utc").isNotNull() | F.col("terminal_aimed_arrival_utc").isNotNull()

    o_usable_rec = F.col("origin_reported_departure_utc").isNotNull() & (
        F.lower(F.coalesce(F.col("origin_departure_time_kind").cast("string"), F.lit(""))) != F.lit("estimated")
    )
    t_usable_rec = F.col("terminal_reported_arrival_utc").isNotNull() & (
        F.lower(F.coalesce(F.col("terminal_arrival_time_kind").cast("string"), F.lit(""))) != F.lit("estimated")
    )

    ep_presence_expr = (
        F.when(~ep_resolved, F.lit(None).cast("string"))
        .when(o_present & t_present, F.lit("both"))
        .when(o_present, F.lit("origin_only"))
        .when(t_present, F.lit("terminal_only"))
        .otherwise(F.lit("neither"))
    )

    ep_obs_expr = (
        F.when(~ep_resolved, F.lit(None).cast("string"))
        .when(o_usable_rec & t_usable_rec, F.lit("both"))
        .when(o_usable_rec, F.lit("origin_only"))
        .when(t_usable_rec, F.lit("terminal_only"))
        .otherwise(F.lit("neither"))
    )

    et_o_elig = ep_resolved & (F.coalesce(F.col("_et_o_dep_elig"), F.lit(0)) == 1)
    et_t_elig = ep_resolved & (F.coalesce(F.col("_et_t_arr_elig"), F.lit(0)) == 1)
    et_pair_elig = et_o_elig & et_t_elig

    nx_o_elig = ep_resolved & (F.coalesce(F.col("_nx_o_dep_elig"), F.lit(0)) == 1)
    nx_t_elig = ep_resolved & (F.coalesce(F.col("_nx_t_arr_elig"), F.lit(0)) == 1)
    nx_pair_elig = nx_o_elig & nx_t_elig

    et_o_delay = F.when(et_o_elig, F.col("_et_o_dep_delay").cast("double")).otherwise(F.lit(None).cast("double"))
    et_t_delay = F.when(et_t_elig, F.col("_et_t_arr_delay").cast("double")).otherwise(F.lit(None).cast("double"))
    et_delta = F.when(et_pair_elig, et_t_delay - et_o_delay).otherwise(F.lit(None).cast("double"))

    nx_o_delay = F.when(nx_o_elig, F.col("_nx_o_dep_delay").cast("double")).otherwise(F.lit(None).cast("double"))
    nx_t_delay = F.when(nx_t_elig, F.col("_nx_t_arr_delay").cast("double")).otherwise(F.lit(None).cast("double"))
    nx_delta = F.when(nx_pair_elig, nx_t_delay - nx_o_delay).otherwise(F.lit(None).cast("double"))

    j_cancel_elig = j_cancel_state.isin("cancelled", "not_cancelled")

    line_cons_expr = (
        F.when(
            F.col("planned_line_id").isNotNull() & F.col("reported_line_id").isNotNull(),
            F.when(F.col("planned_line_id") == F.col("reported_line_id"), F.lit("agreement")).otherwise(
                F.lit("mismatch")
            ),
        )
        .when(F.col("planned_line_id").isNotNull() | F.col("reported_line_id").isNotNull(), F.lit("one_sided"))
        .otherwise(F.lit("unknown"))
    )

    journey_id_status_expr = (
        F.when(
            F.col("dated_service_journey_id").isNotNull() & F.col("service_journey_id").isNotNull(),
            F.lit("resolved_both"),
        )
        .when(F.col("dated_service_journey_id").isNotNull(), F.lit("resolved_dated_journey"))
        .when(F.col("service_journey_id").isNotNull(), F.lit("resolved_service_journey"))
        .otherwise(F.lit("unresolved"))
    )

    reasons_expr = F.array_remove(
        F.array(
            F.when(outside_sched, F.lit("reported_outside_resolved_schedule")),
            F.when(is_scheduled & (~has_rep), F.lit("scheduled_without_et")),
            F.when(~ep_resolved, F.lit("endpoint_unresolved")),
            F.when(j_cancel_state == F.lit("conflicting"), F.lit("conflicting_journey_cancellation")),
            F.when(j_cancel_state == F.lit("unknown"), F.lit("unknown_journey_cancellation")),
        ),
        F.lit(None).cast("string"),
    )

    return enriched.select(
        F.lit(gold_run_id).cast("string").alias("gold_run_id"),
        F.lit(GOLD_SCHEMA_VERSION).cast("string").alias("gold_schema_version"),
        F.lit(METRIC_POLICY_VERSION).cast("string").alias("metric_policy_version"),
        joined["provider"].cast("string").alias("provider"),
        joined["operating_date"].cast("date").alias("operating_date"),
        joined["source_silver_run_id"].cast("string").alias("source_silver_run_id"),
        joined["publication_id"].cast("string").alias("publication_id"),
        joined["journey_key"].cast("string").alias("journey_key"),
        joined["service_journey_id"].cast("string").alias("service_journey_id"),
        joined["dated_service_journey_id"].cast("string").alias("dated_service_journey_id"),
        journey_id_status_expr.alias("journey_identity_status"),
        normalize_transport_mode_expr("transport_mode").alias("transport_mode"),
        joined["planned_line_id"].cast("string").alias("planned_line_id"),
        joined["reported_line_id"].cast("string").alias("reported_line_id"),
        line_cons_expr.alias("line_consistency_status"),
        planned_service_status_expr.alias("planned_service_status"),
        has_rep.cast("boolean").alias("has_reported_records"),
        outside_sched.cast("boolean").alias("reported_outside_resolved_schedule"),
        (is_scheduled & (~has_rep)).cast("boolean").alias("scheduled_without_et"),
        F.coalesce(joined["planned_call_count"], F.lit(0)).cast("long").alias("planned_call_count"),
        F.coalesce(joined["matched_call_count"], F.lit(0)).cast("long").alias("matched_call_count"),
        F.coalesce(joined["reported_canonical_call_count"], F.lit(0)).cast("long").alias("reported_canonical_call_count"),
        F.coalesce(joined["recorded_arrival_count"], F.lit(0)).cast("long").alias("recorded_arrival_count"),
        F.coalesce(joined["recorded_departure_count"], F.lit(0)).cast("long").alias("recorded_departure_count"),
        F.coalesce(joined["estimated_call_count"], F.lit(0)).cast("long").alias("estimated_call_count"),
        F.coalesce(joined["conflicting_reported_call_count"], F.lit(0)).cast("long").alias("conflicting_reported_call_count"),
        joined["endpoint_resolution_status"].cast("string").alias("endpoint_resolution_status"),
        joined["origin_planned_call_id"].cast("string").alias("origin_planned_call_id"),
        joined["terminal_planned_call_id"].cast("string").alias("terminal_planned_call_id"),
        joined["origin_quay_id"].cast("string").alias("origin_quay_id"),
        joined["terminal_quay_id"].cast("string").alias("terminal_quay_id"),
        ep_presence_expr.alias("endpoint_record_presence_status"),
        ep_obs_expr.alias("endpoint_observation_status"),
        joined["origin_planned_departure_utc"].alias("origin_planned_departure_utc"),
        joined["terminal_planned_arrival_utc"].alias("terminal_planned_arrival_utc"),
        siri_o["origin_aimed_departure_utc"].alias("origin_aimed_departure_utc"),
        siri_t["terminal_aimed_arrival_utc"].alias("terminal_aimed_arrival_utc"),
        siri_o["origin_reported_departure_utc"].alias("origin_reported_departure_utc"),
        siri_t["terminal_reported_arrival_utc"].alias("terminal_reported_arrival_utc"),
        siri_o["origin_departure_time_kind"].cast("string").alias("origin_departure_time_kind"),
        siri_t["terminal_arrival_time_kind"].cast("string").alias("terminal_arrival_time_kind"),
        et_o_elig.cast("boolean").alias("et_origin_departure_eligible"),
        et_t_elig.cast("boolean").alias("et_terminal_arrival_eligible"),
        et_pair_elig.cast("boolean").alias("et_endpoint_pair_eligible"),
        et_o_delay.alias("et_origin_departure_delay_sec"),
        et_t_delay.alias("et_terminal_arrival_delay_sec"),
        et_delta.alias("et_delay_change_sec"),
        nx_o_elig.cast("boolean").alias("netex_origin_departure_eligible"),
        nx_t_elig.cast("boolean").alias("netex_terminal_arrival_eligible"),
        nx_pair_elig.cast("boolean").alias("netex_endpoint_pair_eligible"),
        nx_o_delay.alias("netex_origin_departure_delay_sec"),
        nx_t_delay.alias("netex_terminal_arrival_delay_sec"),
        nx_delta.alias("netex_delay_change_sec"),
        j_cancel_state.alias("journey_cancellation_state"),
        j_cancel_elig.cast("boolean").alias("journey_cancellation_eligible"),
        reasons_expr.alias("quality_reason_codes"),
    )


def build_daily_line_service(
    spark: SparkSession,
    inventory: SilverInventory,
    journey_perf_df: DataFrame,
    *,
    gold_run_id: str,
) -> DataFrame:
    """Build daily_line_service at (provider, operating_date, transport_mode, line_id, line_resolution_status)."""
    siri_df = read_silver_table_for_inventory(
        spark,
        inventory,
        table_name="siri_et_stop_calls",
        columns=[
            "provider",
            "et_canonical_id",
            "line_ref",
            "stop_cancellation_state",
            "duplicate_status",
        ],
    )
    rec_map = read_silver_table_for_inventory(
        spark,
        inventory,
        table_name="reconciled_stop_calls",
        columns=["provider", "operating_date", "publication_id", "et_canonical_id"],
    ).dropDuplicates(["provider", "et_canonical_id"])
    siri_dated = siri_df.join(rec_map, on=["provider", "et_canonical_id"], how="inner")

    line_dim = read_silver_table_for_inventory(
        spark,
        inventory,
        table_name="lines",
        columns=["publication_id", "line_id", "public_code", "line_name"],
    )
    dim_keys = ["publication_id", "line_id"]
    line_non_null = line_dim.filter(F.col("line_id").isNotNull())
    line_dedup = line_non_null.dropDuplicates(dim_keys)
    if line_non_null.select(*dim_keys).count() != line_dedup.count():
        raise ValueError("Line dimension not unique on (publication_id, line_id).")

    jp_with_line = journey_perf_df.withColumn(
        "line_id", F.coalesce(F.col("planned_line_id"), F.col("reported_line_id"))
    ).withColumn(
        "line_resolution_status",
        F.when(F.col("line_id").isNotNull(), F.lit("resolved")).otherwise(F.lit("unknown")),
    )

    is_sched = F.col("planned_service_status") == F.lit("scheduled")
    is_unres = F.col("planned_service_status") == F.lit("unresolved")
    has_et = F.col("has_reported_records")
    has_both_rec_ep = F.col("endpoint_observation_status") == F.lit("both")
    has_rec_time = (F.col("recorded_arrival_count") > 0) | (F.col("recorded_departure_count") > 0)

    j_agg = jp_with_line.groupBy(
        "provider",
        "operating_date",
        "source_silver_run_id",
        "publication_id",
        "transport_mode",
        "line_id",
        "line_resolution_status",
    ).agg(
        F.sum(F.when(is_sched, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("scheduled_journey_count"),
        F.sum(F.when(is_sched & has_et, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("scheduled_with_et_journey_count"),
        F.sum(F.when(is_sched & (~has_et), F.lit(1)).otherwise(F.lit(0))).cast("long").alias(
            "scheduled_without_et_journey_count"
        ),
        F.sum(F.when(is_unres, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("plan_unresolved_journey_count"),
        F.sum(F.when(has_et, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("reported_journey_count"),
        F.sum(F.when(F.col("reported_outside_resolved_schedule"), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("reported_outside_resolved_schedule_count"),
        F.sum(F.when(has_et & has_rec_time, F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("reported_with_recorded_time_journey_count"),
        F.sum(F.when(has_et & has_both_rec_ep, F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("reported_with_both_recorded_endpoints_count"),
        F.sum(F.when(F.col("journey_cancellation_state") == F.lit("cancelled"), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("journey_cancelled_count"),
        F.sum(F.when(F.col("journey_cancellation_state") == F.lit("not_cancelled"), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("journey_not_cancelled_count"),
        F.sum(F.when(F.col("journey_cancellation_state") == F.lit("unknown"), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("journey_cancellation_unknown_count"),
        F.sum(F.when(F.col("journey_cancellation_state") == F.lit("conflicting"), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("journey_cancellation_conflicting_count"),
        F.sum(
            F.when(F.col("journey_cancellation_state") == F.lit("partially_unknown_true"), F.lit(1)).otherwise(F.lit(0))
        )
        .cast("long")
        .alias("journey_partially_unknown_true_count"),
        F.sum(
            F.when(F.col("journey_cancellation_state") == F.lit("partially_unknown_false"), F.lit(1)).otherwise(F.lit(0))
        )
        .cast("long")
        .alias("journey_partially_unknown_false_count"),
        F.sum(F.when(F.col("journey_cancellation_eligible"), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("journey_cancellation_eligible_count"),
    )

    stop_state_lower = F.lower(F.coalesce(F.col("stop_cancellation_state").cast("string"), F.lit("")))
    is_conflict_var = F.lower(F.coalesce(F.col("duplicate_status").cast("string"), F.lit(""))).isin(
        "conflict", "conflicting", "duplicate"
    )
    stop_agg = (
        siri_dated.withColumn(
            "line_id",
            F.when(F.trim(F.col("line_ref").cast("string")) != "", F.trim(F.col("line_ref").cast("string"))),
        )
        .groupBy("provider", "operating_date", "publication_id", "line_id")
        .agg(
            F.sum(F.when(stop_state_lower == F.lit("cancelled"), F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("stop_cancelled_count"),
            F.sum(F.when(stop_state_lower.isin("not_cancelled", "false", "active"), F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("stop_not_cancelled_count"),
            F.sum(
                F.when(
                    (~stop_state_lower.isin("cancelled", "not_cancelled", "false", "active")) & (~is_conflict_var),
                    F.lit(1),
                ).otherwise(F.lit(0))
            )
            .cast("long")
            .alias("stop_unknown_count"),
            F.sum(F.when(is_conflict_var, F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("stop_cancellation_conflicting_candidate_count"),
        )
    )

    labels = line_dedup.select(
        "publication_id",
        "line_id",
        F.col("public_code").cast("string").alias("line_public_code"),
        F.col("line_name").cast("string").alias("line_name"),
    )

    combined = j_agg.join(
        stop_agg,
        on=["provider", "operating_date", "publication_id", "line_id"],
        how="left",
    ).join(labels, on=["publication_id", "line_id"], how="left")

    for c in [
        "stop_cancelled_count",
        "stop_not_cancelled_count",
        "stop_unknown_count",
        "stop_cancellation_conflicting_candidate_count",
    ]:
        combined = combined.withColumn(c, F.coalesce(F.col(c), F.lit(0).cast("long")))

    stop_elig_expr = F.col("stop_cancelled_count") + F.col("stop_not_cancelled_count")
    j_denom_expr = F.col("journey_cancelled_count") + F.col("journey_not_cancelled_count")

    return (
        combined.withColumn("stop_cancellation_eligible_count", stop_elig_expr.cast("long"))
        .withColumn(
            "scheduled_journey_reporting_coverage",
            F.when(
                F.col("scheduled_journey_count") > 0,
                F.col("scheduled_with_et_journey_count").cast("double") / F.col("scheduled_journey_count").cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "journey_cancellation_rate",
            F.when(
                j_denom_expr > 0,
                F.col("journey_cancelled_count").cast("double") / j_denom_expr.cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "stop_cancellation_rate",
            F.when(
                stop_elig_expr > 0,
                F.col("stop_cancelled_count").cast("double") / stop_elig_expr.cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("gold_run_id", F.lit(gold_run_id).cast("string"))
        .withColumn("gold_schema_version", F.lit(GOLD_SCHEMA_VERSION).cast("string"))
        .withColumn("metric_policy_version", F.lit(METRIC_POLICY_VERSION).cast("string"))
    )


def _period_metadata_df(spark: SparkSession, inventory: SilverInventory) -> DataFrame:
    """Build (operating_date, period_type, period_label, period_start_date, period_end_date) rows."""
    rows: list[tuple[date, str, str, date, date]] = []
    for auth in inventory.authorities:
        d = date.fromisoformat(auth.operating_date)
        m_label = f"{d.year:04d}-{d.month:02d}"
        m_start = date(d.year, d.month, 1)
        if d.month == 12:
            m_end = date(d.year, 12, 31)
        else:
            m_end = date.fromordinal(date(d.year, d.month + 1, 1).toordinal() - 1)
        rows.append((d, "month", m_label, m_start, m_end))

        for s_label, (s_start_s, s_end_s) in SEASON_WINDOWS_2026.items():
            s_start = date.fromisoformat(s_start_s)
            s_end = date.fromisoformat(s_end_s)
            if s_start <= d <= s_end:
                rows.append((d, "season", s_label, s_start, s_end))

    return spark.createDataFrame(
        rows,
        schema=["operating_date", "period_type", "period_label", "period_start_date", "period_end_date"],
    )


def _weekday_date_counts_df(spark: SparkSession, p_map: DataFrame) -> DataFrame:
    """Compute distinct selected operating_date counts per weekday for each period."""
    distinct_period_dates = p_map.select(
        "period_type", "period_label", "period_start_date", "period_end_date", "operating_date"
    ).dropDuplicates()

    # Spark dayofweek: 1=Sun, 2=Mon, ..., 7=Sat
    with_dow = distinct_period_dates.withColumn("_spark_dow", F.dayofweek(F.col("operating_date")))
    spark_dow_expr = (
        F.when(F.col("_spark_dow") == 2, F.lit("Mon"))
        .when(F.col("_spark_dow") == 3, F.lit("Tue"))
        .when(F.col("_spark_dow") == 4, F.lit("Wed"))
        .when(F.col("_spark_dow") == 5, F.lit("Thu"))
        .when(F.col("_spark_dow") == 6, F.lit("Fri"))
        .when(F.col("_spark_dow") == 7, F.lit("Sat"))
        .otherwise(F.lit("Sun"))
    )
    return (
        with_dow.withColumn("_dow_name", spark_dow_expr)
        .groupBy("period_type", "period_label", "period_start_date", "period_end_date", "_dow_name")
        .agg(F.countDistinct("operating_date").cast("long").alias("_dow_cnt"))
        .groupBy("period_type", "period_label", "period_start_date", "period_end_date")
        .agg(
            F.map_from_entries(
                F.collect_list(F.struct(F.col("_dow_name"), F.col("_dow_cnt")))
            ).alias("weekday_date_counts")
        )
    )


def build_period_punctuality(
    spark: SparkSession,
    inventory: SilverInventory,
    events_df: DataFrame,
    *,
    gold_run_id: str,
    include_line: bool,
    percentile_accuracy: int = DEFAULT_PERCENTILE_ACCURACY,
) -> DataFrame:
    """Build period_line_punctuality or period_network_punctuality across month and season windows."""
    p_map = _period_metadata_df(spark, inventory)
    wd_counts = _weekday_date_counts_df(spark, p_map)

    siri_by_date = spark.createDataFrame(
        [(date.fromisoformat(a.operating_date), 1 if a.siri_file_available else 0) for a in inventory.authorities],
        schema=["operating_date", "_has_siri"],
    )
    elig_dates_df = (
        events_df.filter(F.col("eligible"))
        .select("operating_date")
        .dropDuplicates()
        .withColumn("_has_elig_events", F.lit(1))
    )

    period_cov = (
        p_map.join(siri_by_date, on="operating_date", how="left")
        .join(elig_dates_df, on="operating_date", how="left")
        .groupBy("period_type", "period_label", "period_start_date", "period_end_date")
        .agg(
            F.min("operating_date").alias("first_selected_date"),
            F.max("operating_date").alias("last_selected_date"),
            F.countDistinct("operating_date").cast("long").alias("selected_date_count"),
            F.sum(F.coalesce(F.col("_has_siri"), F.lit(0))).cast("long").alias("dates_with_siri_count"),
            F.sum(F.coalesce(F.col("_has_elig_events"), F.lit(0)))
            .cast("long")
            .alias("dates_with_eligible_events_count"),
        )
        .join(wd_counts, on=["period_type", "period_label", "period_start_date", "period_end_date"], how="left")
    )

    events_with_period = events_df.join(p_map, on="operating_date", how="inner")
    group_cols = [
        "provider",
        "period_type",
        "period_label",
        "period_start_date",
        "period_end_date",
        "transport_mode",
    ]
    if include_line:
        group_cols.extend(["line_id", "line_resolution_status"])
    group_cols.extend(["event_type", "baseline"])

    agg = aggregate_punctuality(
        events_with_period,
        group_cols=group_cols,
        gold_run_id=gold_run_id,
        percentile_accuracy=percentile_accuracy,
        include_lineage_cols=False,
    )

    joined = agg.join(
        period_cov,
        on=["period_type", "period_label", "period_start_date", "period_end_date"],
        how="left",
    )

    if include_line:
        line_dim = (
            read_silver_table_for_inventory(
                spark,
                inventory,
                table_name="lines",
                columns=["publication_id", "line_id", "public_code", "line_name"],
            )
            .filter(F.col("line_id").isNotNull())
            .groupBy("line_id")
            .agg(
                F.first("public_code", ignorenulls=True).cast("string").alias("line_public_code"),
                F.first("line_name", ignorenulls=True).cast("string").alias("line_name"),
            )
        )
        joined = joined.join(line_dim, on="line_id", how="left")

    return joined


def build_period_line_service(
    spark: SparkSession,
    inventory: SilverInventory,
    daily_line_service_df: DataFrame,
    *,
    gold_run_id: str,
) -> DataFrame:
    """Build period_line_service across month and season windows."""
    p_map = _period_metadata_df(spark, inventory)
    wd_counts = _weekday_date_counts_df(spark, p_map)

    siri_by_date = spark.createDataFrame(
        [(date.fromisoformat(a.operating_date), 1 if a.siri_file_available else 0) for a in inventory.authorities],
        schema=["operating_date", "_has_siri"],
    )

    period_cov = (
        p_map.join(siri_by_date, on="operating_date", how="left")
        .groupBy("period_type", "period_label", "period_start_date", "period_end_date")
        .agg(
            F.min("operating_date").alias("first_selected_date"),
            F.max("operating_date").alias("last_selected_date"),
            F.countDistinct("operating_date").cast("long").alias("selected_date_count"),
            F.sum(F.coalesce(F.col("_has_siri"), F.lit(0))).cast("long").alias("dates_with_siri_count"),
        )
        .join(wd_counts, on=["period_type", "period_label", "period_start_date", "period_end_date"], how="left")
    )

    dls_with_period = daily_line_service_df.join(p_map, on="operating_date", how="inner")
    has_valid_plan_eval = (F.col("scheduled_journey_count") > 0) | (F.col("plan_unresolved_journey_count") == 0)

    grouped = (
        dls_with_period.groupBy(
            "provider",
            "period_type",
            "period_label",
            "period_start_date",
            "period_end_date",
            "transport_mode",
            "line_id",
            "line_resolution_status",
        )
        .agg(
            F.first("line_public_code", ignorenulls=True).alias("line_public_code"),
            F.first("line_name", ignorenulls=True).alias("line_name"),
            F.sum("scheduled_journey_count").cast("long").alias("scheduled_journey_count"),
            F.sum("scheduled_with_et_journey_count").cast("long").alias("scheduled_with_et_journey_count"),
            F.sum("scheduled_without_et_journey_count").cast("long").alias("scheduled_without_et_journey_count"),
            F.sum("plan_unresolved_journey_count").cast("long").alias("plan_unresolved_journey_count"),
            F.sum("reported_journey_count").cast("long").alias("reported_journey_count"),
            F.sum("reported_outside_resolved_schedule_count")
            .cast("long")
            .alias("reported_outside_resolved_schedule_count"),
            F.sum("reported_with_recorded_time_journey_count")
            .cast("long")
            .alias("reported_with_recorded_time_journey_count"),
            F.sum("reported_with_both_recorded_endpoints_count")
            .cast("long")
            .alias("reported_with_both_recorded_endpoints_count"),
            F.sum("journey_cancelled_count").cast("long").alias("journey_cancelled_count"),
            F.sum("journey_not_cancelled_count").cast("long").alias("journey_not_cancelled_count"),
            F.sum("journey_cancellation_unknown_count").cast("long").alias("journey_cancellation_unknown_count"),
            F.sum("journey_cancellation_conflicting_count").cast("long").alias("journey_cancellation_conflicting_count"),
            F.sum("stop_cancelled_count").cast("long").alias("stop_cancelled_count"),
            F.sum("stop_not_cancelled_count").cast("long").alias("stop_not_cancelled_count"),
            F.countDistinct(F.when(F.col("scheduled_journey_count") > 0, F.col("operating_date")))
            .cast("long")
            .alias("dates_line_scheduled_count"),
            F.countDistinct(F.when(has_valid_plan_eval, F.col("operating_date")))
            .cast("long")
            .alias("evaluated_date_count"),
        )
        .join(
            period_cov,
            on=["period_type", "period_label", "period_start_date", "period_end_date"],
            how="left",
        )
    )

    j_denom = F.col("journey_cancelled_count") + F.col("journey_not_cancelled_count")
    s_denom = F.col("stop_cancelled_count") + F.col("stop_not_cancelled_count")

    return (
        grouped.withColumn(
            "scheduled_journeys_per_evaluated_date",
            F.when(
                F.col("evaluated_date_count") > 0,
                F.col("scheduled_journey_count").cast("double") / F.col("evaluated_date_count").cast("double"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "journey_cancellation_rate",
            F.when(j_denom > 0, F.col("journey_cancelled_count").cast("double") / j_denom.cast("double")).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "stop_cancellation_rate",
            F.when(s_denom > 0, F.col("stop_cancelled_count").cast("double") / s_denom.cast("double")).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn("gold_run_id", F.lit(gold_run_id).cast("string"))
        .withColumn("gold_schema_version", F.lit(GOLD_SCHEMA_VERSION).cast("string"))
        .withColumn("metric_policy_version", F.lit(METRIC_POLICY_VERSION).cast("string"))
    )
