"""Publication-scoped NeTEx definition resolution for the DAT535 Silver layer."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from . import DEFAULT_PROVIDER


def _occurrence_struct_col():
    """Preserve exact source occurrence identity so repeated IDs remain traceable."""
    return F.struct(
        F.col("staging_run").alias("staging_run"),
        F.col("snapshot_date").alias("snapshot_date"),
        F.col("source_zip").alias("source_zip"),
        F.col("source_zip_sha256").alias("source_zip_sha256"),
        F.col("source_generation").alias("source_generation"),
        F.col("publication_date").alias("publication_date"),
        F.col("xml_publication_timestamp").alias("xml_publication_timestamp"),
        F.col("source_member").alias("source_member"),
        F.col("member_index").alias("member_index"),
        F.col("source_xml_sha256").alias("source_xml_sha256"),
        F.col("composite_frame_index").alias("composite_frame_index"),
        F.col("composite_frame_id").alias("composite_frame_id"),
        F.col("composite_frame_version").alias("composite_frame_version"),
        F.col("parent_frame_index").alias("parent_frame_index"),
        F.col("parent_frame_id").alias("parent_frame_id"),
        F.col("parent_frame_version").alias("parent_frame_version"),
        F.col("entity_type").alias("entity_type"),
        F.col("entity_index").alias("entity_index"),
    )


def _load_staged_table(
    spark: SparkSession,
    run_dirs: list[str],
    table_name: str,
    publication_lookup: DataFrame,
    provider: str = DEFAULT_PROVIDER,
) -> DataFrame:
    """Read a staged table across selected run directories and attach publication_id."""
    paths = [f"{r}/{table_name}" for r in run_dirs]
    base_path = str(run_dirs[0]).rsplit("/", 1)[0]
    df = (
        spark.read.option("basePath", base_path)
        .parquet(*paths)
        .filter(F.upper(F.coalesce(F.col("provider"), F.lit(provider))) == provider.upper())
    )
    if "entity_type" not in df.columns:
        df = df.withColumn("entity_type", F.lit(table_name))
    if "parent_frame_index" not in df.columns:
        df = df.withColumn("parent_frame_index", F.lit(None).cast("int"))
    if "parent_frame_id" not in df.columns:
        df = df.withColumn("parent_frame_id", F.lit(None).cast("string"))
    if "parent_frame_version" not in df.columns:
        df = df.withColumn("parent_frame_version", F.lit(None).cast("string"))

    staging_run_from_path = F.element_at(F.split(F.input_file_name(), "/"), -3)
    df = df.withColumn("staging_run", staging_run_from_path)

    return df.join(
        publication_lookup.select("staging_run", "snapshot_date", "publication_id"),
        on=["staging_run", "snapshot_date"],
        how="inner",
    )


def _match_ref_expr(ref_id_col, ref_ver_col, target_id_col, target_ver_col):
    """Match a NeTEx reference within scope, honoring explicit reference version when present."""
    return (
        ref_id_col.isNotNull()
        & (ref_id_col == target_id_col)
        & (
            ref_ver_col.isNull()
            | (F.trim(ref_ver_col) == "")
            | target_ver_col.isNull()
            | (ref_ver_col == target_ver_col)
        )
    )


def build_lines(
    spark: SparkSession,
    run_dirs: list[str],
    publication_lookup: DataFrame,
    provider: str = DEFAULT_PROVIDER,
) -> DataFrame:
    """Build publication-scoped line definitions without losing source provenance."""
    raw_lines = _load_staged_table(spark, run_dirs, "lines", publication_lookup, provider)

    dedup_win = Window.partitionBy(
        "publication_id",
        "provider",
        "source_member",
        "member_index",
        "composite_frame_index",
        "parent_frame_index",
        "entity_type",
        "entity_index",
    ).orderBy(F.col("snapshot_date").desc())

    lines_df = (
        raw_lines.withColumn("_rn", F.row_number().over(dedup_win))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("line_id", F.col("record._id"))
        .withColumn("line_version", F.col("record._version"))
        .withColumn("line_name", F.col("record.Name"))
        .withColumn("public_code", F.col("record.PublicCode"))
        .withColumn("private_code", F.col("record.PrivateCode"))
        .withColumn("transport_mode", F.col("record.TransportMode"))
        .withColumn("operator_ref", F.col("record.OperatorRef._ref"))
        .withColumn("operator_version", F.col("record.OperatorRef._version"))
        .withColumn("represented_by_group_ref", F.col("record.RepresentedByGroupRef._ref"))
        .withColumn(
            "line_definition_status",
            F.when(
                F.col("line_id").isNull() | (F.trim(F.col("line_id")) == ""),
                F.lit("missing_line_id"),
            ).otherwise(F.lit("resolved")),
        )
        .withColumn("source_occurrence", _occurrence_struct_col())
    )

    return lines_df.select(
        "publication_id",
        "provider",
        "snapshot_date",
        "line_id",
        "line_version",
        "line_name",
        "public_code",
        "private_code",
        "transport_mode",
        "operator_ref",
        "operator_version",
        "represented_by_group_ref",
        "line_definition_status",
        "source_occurrence",
    )


def build_routes(
    spark: SparkSession,
    run_dirs: list[str],
    publication_lookup: DataFrame,
    lines_df: DataFrame,
    provider: str = DEFAULT_PROVIDER,
) -> DataFrame:
    """Resolve routes to publication-scoped lines while detecting missing or ambiguous targets."""
    raw_routes = _load_staged_table(spark, run_dirs, "routes", publication_lookup, provider)

    route_occ = (
        raw_routes.withColumn("route_id", F.col("record._id"))
        .withColumn("route_version", F.col("record._version"))
        .withColumn("route_name", F.col("record.Name"))
        .withColumn("route_short_name", F.col("record.ShortName"))
        .withColumn("direction_type", F.col("record.DirectionType"))
        .withColumn("line_ref", F.col("record.LineRef._ref"))
        .withColumn("line_ref_version", F.col("record.LineRef._version"))
        .withColumn("flexible_line_ref", F.col("record.FlexibleLineRef._ref"))
        .withColumn("flexible_line_ref_version", F.col("record.FlexibleLineRef._version"))
        .withColumn("source_occurrence", _occurrence_struct_col())
        .withColumn(
            "_occ_id",
            F.sha2(
                F.to_json(
                    F.struct(
                        F.col("publication_id").alias("publication_id"),
                        F.col("source_occurrence").alias("source_occurrence"),
                    )
                ),
                256,
            ),
        )
    )

    line_targets = lines_df.select(
        F.col("publication_id").alias("_t_pub_id"),
        F.col("line_id").alias("_t_line_id"),
        F.col("line_version").alias("_t_line_ver"),
        F.col("line_name").alias("_t_line_name"),
        F.col("public_code").alias("_t_public_code"),
        F.col("transport_mode").alias("_t_transport_mode"),
        F.col("operator_ref").alias("_t_operator_ref"),
    ).dropDuplicates()

    joined = route_occ.join(
        line_targets,
        on=(
            (F.col("publication_id") == F.col("_t_pub_id"))
            & _match_ref_expr(
                F.col("line_ref"),
                F.col("line_ref_version"),
                F.col("_t_line_id"),
                F.col("_t_line_ver"),
            )
        ),
        how="left",
    )

    agg_win = Window.partitionBy("_occ_id")
    counted = joined.withColumn(
        "line_match_count",
        F.sum(F.when(F.col("_t_line_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))).over(agg_win),
    )

    pick_win = Window.partitionBy("_occ_id").orderBy(
        F.col("_t_line_id").asc_nulls_last(),
        F.col("_t_line_ver").asc_nulls_last(),
    )

    resolved = (
        counted.withColumn("_rn", F.row_number().over(pick_win))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_occ_id", "_t_pub_id")
        .withColumn(
            "resolved_line_id",
            F.when(F.col("line_match_count") == 1, F.col("_t_line_id")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "resolved_line_version",
            F.when(F.col("line_match_count") == 1, F.col("_t_line_ver")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "resolved_line_name",
            F.when(F.col("line_match_count") == 1, F.col("_t_line_name")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "resolved_public_code",
            F.when(F.col("line_match_count") == 1, F.col("_t_public_code")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "resolved_transport_mode",
            F.when(F.col("line_match_count") == 1, F.col("_t_transport_mode")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "resolved_operator_ref",
            F.when(F.col("line_match_count") == 1, F.col("_t_operator_ref")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "route_resolution_status",
            F.when(F.col("line_match_count") == 1, F.lit("resolved"))
            .when(F.col("line_match_count") > 1, F.lit("ambiguous_line_ref"))
            .when(
                F.col("line_ref").isNull() & F.col("flexible_line_ref").isNotNull(),
                F.lit("flexible_line_ref_only"),
            )
            .when(F.col("line_ref").isNull(), F.lit("missing_line_ref"))
            .otherwise(F.lit("unresolved_line_ref")),
        )
    )

    return resolved.select(
        "publication_id",
        "provider",
        "snapshot_date",
        "route_id",
        "route_version",
        "route_name",
        "route_short_name",
        "direction_type",
        "line_ref",
        "line_ref_version",
        "flexible_line_ref",
        "flexible_line_ref_version",
        "resolved_line_id",
        "resolved_line_version",
        "resolved_line_name",
        "resolved_public_code",
        "resolved_transport_mode",
        "resolved_operator_ref",
        "line_match_count",
        "route_resolution_status",
        "source_occurrence",
    )


def build_journey_patterns_and_stops(
    spark: SparkSession,
    run_dirs: list[str],
    publication_lookup: DataFrame,
    routes_df: DataFrame,
    provider: str = DEFAULT_PROVIDER,
) -> tuple[DataFrame, DataFrame]:
    """Build journey_patterns and journey_pattern_stops with grouped ambiguity counts."""
    raw_jps = _load_staged_table(spark, run_dirs, "journey_patterns", publication_lookup, provider)
    raw_jpp = _load_staged_table(
        spark, run_dirs, "journey_pattern_points", publication_lookup, provider
    )
    raw_ssps = _load_staged_table(
        spark, run_dirs, "scheduled_stop_points", publication_lookup, provider
    )
    raw_sas = _load_staged_table(
        spark, run_dirs, "stop_assignments", publication_lookup, provider
    )

    jp_occ = (
        raw_jps.withColumn("journey_pattern_id", F.col("record._id"))
        .withColumn("journey_pattern_version", F.col("record._version"))
        .withColumn("journey_pattern_name", F.col("record.Name"))
        .withColumn("route_ref", F.col("record.RouteRef._ref"))
        .withColumn("route_ref_version", F.col("record.RouteRef._version"))
        .withColumn("source_occurrence", _occurrence_struct_col())
        .withColumn(
            "_jp_occ_id",
            F.sha2(
                F.to_json(
                    F.struct(
                        F.col("publication_id").alias("publication_id"),
                        F.col("source_occurrence").alias("source_occurrence"),
                    )
                ),
                256,
            ),
        )
    )

    route_targets = routes_df.select(
        F.col("publication_id").alias("_r_pub_id"),
        F.col("route_id").alias("_r_route_id"),
        F.col("route_version").alias("_r_route_ver"),
        F.col("resolved_line_id").alias("_r_line_id"),
        F.col("resolved_public_code").alias("_r_public_code"),
        F.col("resolved_transport_mode").alias("_r_transport_mode"),
        F.col("resolved_operator_ref").alias("_r_operator_ref"),
        F.col("direction_type").alias("_r_direction_type"),
    ).dropDuplicates()

    jp_joined = jp_occ.join(
        route_targets,
        on=(
            (F.col("publication_id") == F.col("_r_pub_id"))
            & _match_ref_expr(
                F.col("route_ref"),
                F.col("route_ref_version"),
                F.col("_r_route_id"),
                F.col("_r_route_ver"),
            )
        ),
        how="left",
    )

    jp_cnt_win = Window.partitionBy("_jp_occ_id")
    jp_pick_win = Window.partitionBy("_jp_occ_id").orderBy(
        F.col("_r_route_id").asc_nulls_last(),
        F.col("_r_route_ver").asc_nulls_last(),
    )

    journey_patterns_df = (
        jp_joined.withColumn(
            "route_match_count",
            F.sum(F.when(F.col("_r_route_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))).over(
                jp_cnt_win
            ),
        )
        .withColumn("_rn", F.row_number().over(jp_pick_win))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn(
            "resolved_route_id",
            F.when(F.col("route_match_count") == 1, F.col("_r_route_id")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "resolved_route_version",
            F.when(F.col("route_match_count") == 1, F.col("_r_route_ver")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "resolved_line_id",
            F.when(F.col("route_match_count") == 1, F.col("_r_line_id")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "resolved_public_code",
            F.when(F.col("route_match_count") == 1, F.col("_r_public_code")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "resolved_transport_mode",
            F.when(F.col("route_match_count") == 1, F.col("_r_transport_mode")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "resolved_operator_ref",
            F.when(F.col("route_match_count") == 1, F.col("_r_operator_ref")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "resolved_direction_type",
            F.when(F.col("route_match_count") == 1, F.col("_r_direction_type")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "journey_pattern_resolution_status",
            F.when(F.col("route_match_count") == 1, F.lit("resolved"))
            .when(F.col("route_match_count") > 1, F.lit("ambiguous_route_ref"))
            .when(F.col("route_ref").isNull(), F.lit("missing_route_ref"))
            .otherwise(F.lit("unresolved_route_ref")),
        )
    )

    sa_clean = (
        raw_sas.withColumn("scheduled_stop_point_ref", F.col("record.ScheduledStopPointRef._ref"))
        .withColumn(
            "scheduled_stop_point_ref_ver", F.col("record.ScheduledStopPointRef._version")
        )
        .withColumn("quay_ref", F.col("record.QuayRef._ref"))
        .withColumn("quay_ref_version", F.col("record.QuayRef._version"))
        .withColumn("stop_place_ref", F.col("record.StopPlaceRef._ref"))
        .withColumn("flexible_stop_place_ref", F.col("record.FlexibleStopPlaceRef._ref"))
        .filter(F.col("scheduled_stop_point_ref").isNotNull())
        .select(
            "publication_id",
            "scheduled_stop_point_ref",
            "scheduled_stop_point_ref_ver",
            "quay_ref",
            "quay_ref_version",
            "stop_place_ref",
            "flexible_stop_place_ref",
        )
        .dropDuplicates()
    )

    ssp_clean = (
        raw_ssps.withColumn("ssp_id", F.col("record._id"))
        .withColumn("ssp_version", F.col("record._version"))
        .withColumn("stop_point_name", F.col("record.Name"))
        .filter(F.col("ssp_id").isNotNull())
        .select("publication_id", "ssp_id", "ssp_version", "stop_point_name")
        .dropDuplicates()
    )

    jpp_base = (
        raw_jpp.withColumn("pattern_stop_point_id", F.col("record._id"))
        .withColumn("pattern_stop_point_version", F.col("record._version"))
        .withColumn("source_order_raw", F.col("record._order"))
        .withColumn("source_order_int", F.col("record._order").cast("int"))
        .withColumn("scheduled_stop_point_ref", F.col("record.ScheduledStopPointRef._ref"))
        .withColumn(
            "scheduled_stop_point_ref_version", F.col("record.ScheduledStopPointRef._version")
        )
        .withColumn("for_boarding_raw", F.col("record.ForBoarding"))
        .withColumn("for_alighting_raw", F.col("record.ForAlighting"))
        .withColumn("source_occurrence", _occurrence_struct_col())
        .withColumn(
            "_stop_occ_id",
            F.sha2(
                F.to_json(
                    F.struct(
                        F.col("publication_id").alias("publication_id"),
                        F.col("source_occurrence").alias("source_occurrence"),
                        F.col("pattern_stop_point_id").alias("pattern_stop_point_id"),
                        F.col("source_order_int").alias("source_order_int"),
                    )
                ),
                256,
            ),
        )
    )

    jpp_with_ssp = jpp_base.join(
        ssp_clean,
        on=(
            (jpp_base["publication_id"] == ssp_clean["publication_id"])
            & _match_ref_expr(
                jpp_base["scheduled_stop_point_ref"],
                jpp_base["scheduled_stop_point_ref_version"],
                ssp_clean["ssp_id"],
                ssp_clean["ssp_version"],
            )
        ),
        how="left",
    ).select(
        jpp_base["*"],
        ssp_clean["stop_point_name"].alias("_ssp_name"),
    )

    jpp_with_sa = jpp_with_ssp.join(
        sa_clean,
        on=(
            (jpp_with_ssp["publication_id"] == sa_clean["publication_id"])
            & _match_ref_expr(
                jpp_with_ssp["scheduled_stop_point_ref"],
                jpp_with_ssp["scheduled_stop_point_ref_version"],
                sa_clean["scheduled_stop_point_ref"],
                sa_clean["scheduled_stop_point_ref_ver"],
            )
        ),
        how="left",
    ).select(
        jpp_with_ssp["*"],
        sa_clean["quay_ref"].alias("_sa_quay_ref"),
        sa_clean["quay_ref_version"].alias("_sa_quay_ver"),
        sa_clean["stop_place_ref"].alias("_sa_stop_place_ref"),
        sa_clean["flexible_stop_place_ref"].alias("_sa_flex_stop_place_ref"),
    )

    # Direct grouped countDistinct per _stop_occ_id (collect_set already returns an array)
    sa_counts_df = jpp_with_sa.groupBy("_stop_occ_id").agg(
        F.countDistinct(
            F.coalesce(
                F.col("_sa_quay_ref"),
                F.col("_sa_flex_stop_place_ref"),
                F.col("_sa_stop_place_ref"),
            )
        )
        .cast("int")
        .alias("stop_assignment_match_count"),
        F.countDistinct(F.col("_sa_quay_ref"))
        .cast("int")
        .alias("quay_candidate_count"),
    )

    pick_sa_win = Window.partitionBy("_stop_occ_id").orderBy(
        F.col("_sa_quay_ref").asc_nulls_last(),
        F.col("_sa_quay_ver").asc_nulls_last(),
        F.col("_sa_flex_stop_place_ref").asc_nulls_last(),
    )

    jps_counted = (
        jpp_with_sa.join(sa_counts_df, on="_stop_occ_id", how="left")
        .withColumn("stop_assignment_match_count", F.coalesce(F.col("stop_assignment_match_count"), F.lit(0)))
        .withColumn("quay_candidate_count", F.coalesce(F.col("quay_candidate_count"), F.lit(0)))
    )

    journey_pattern_stops_df = (
        jps_counted.withColumn("_rn", F.row_number().over(pick_sa_win))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_stop_occ_id")
        .withColumn(
            "quay_id",
            F.when(
                (F.col("quay_candidate_count") == 1) & (F.col("stop_assignment_match_count") == 1),
                F.col("_sa_quay_ref"),
            ).otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "quay_version",
            F.when(
                (F.col("quay_candidate_count") == 1) & (F.col("stop_assignment_match_count") == 1),
                F.col("_sa_quay_ver"),
            ).otherwise(F.lit(None).cast("string")),
        )
        .withColumn("stop_point_name", F.col("_ssp_name"))
        .withColumn(
            "stop_assignment_status",
            F.when(
                (F.col("quay_candidate_count") == 1) & (F.col("stop_assignment_match_count") == 1),
                F.lit("resolved"),
            )
            .when(F.col("stop_assignment_match_count") > 1, F.lit("ambiguous"))
            .when(
                (F.col("stop_assignment_match_count") == 0)
                & F.col("_sa_flex_stop_place_ref").isNotNull(),
                F.lit("flexible_stop_place_only"),
            )
            .when(F.col("stop_assignment_match_count") == 0, F.lit("missing_stop_assignment"))
            .otherwise(F.lit("unresolved")),
        )
        .select(
            "publication_id",
            "provider",
            "snapshot_date",
            "journey_pattern_id",
            "journey_pattern_version",
            "point_index",
            "pattern_stop_point_id",
            "pattern_stop_point_version",
            "source_order_raw",
            "source_order_int",
            "scheduled_stop_point_ref",
            "scheduled_stop_point_ref_version",
            "stop_point_name",
            "quay_id",
            "quay_version",
            "stop_assignment_match_count",
            "quay_candidate_count",
            "stop_assignment_status",
            "for_boarding_raw",
            "for_alighting_raw",
            "source_occurrence",
        )
    )

    jp_out = journey_patterns_df.drop(
        "_jp_occ_id",
        "_r_route_id",
        "_r_route_ver",
        "_r_line_id",
        "_r_public_code",
        "_r_transport_mode",
        "_r_operator_ref",
        "_r_direction_type",
    ).select(
        "publication_id",
        "provider",
        "snapshot_date",
        "journey_pattern_id",
        "journey_pattern_version",
        "journey_pattern_name",
        "route_ref",
        "route_ref_version",
        "resolved_route_id",
        "resolved_route_version",
        "resolved_line_id",
        "resolved_public_code",
        "resolved_transport_mode",
        "resolved_operator_ref",
        "resolved_direction_type",
        "route_match_count",
        "journey_pattern_resolution_status",
        "source_occurrence",
    )

    return jp_out, journey_pattern_stops_df


def build_journeys(
    spark: SparkSession,
    run_dirs: list[str],
    publication_lookup: DataFrame,
    journey_patterns_df: DataFrame,
    lines_df: DataFrame,
    provider: str = DEFAULT_PROVIDER,
) -> DataFrame:
    """Resolve service journey definitions to patterns, lines, operators, and day types."""
    raw_sjs = _load_staged_table(spark, run_dirs, "service_journeys", publication_lookup, provider)

    sj_occ = (
        raw_sjs.withColumn("service_journey_id", F.col("record._id"))
        .withColumn("service_journey_version", F.col("record._version"))
        .withColumn("journey_name", F.col("record.Name"))
        .withColumn("private_code", F.col("record.PrivateCode"))
        .withColumn("departure_time_raw", F.col("record.DepartureTime"))
        .withColumn("departure_day_offset_raw", F.col("record.DepartureDayOffset"))
        .withColumn("journey_pattern_ref", F.col("record.JourneyPatternRef._ref"))
        .withColumn("journey_pattern_ref_version", F.col("record.JourneyPatternRef._version"))
        .withColumn("line_ref", F.col("record.LineRef._ref"))
        .withColumn("line_ref_version", F.col("record.LineRef._version"))
        .withColumn("operator_ref", F.col("record.OperatorRef._ref"))
        .withColumn("operator_ref_version", F.col("record.OperatorRef._version"))
        .withColumn(
            "day_type_refs",
            F.transform(
                F.coalesce(F.col("record.dayTypes.DayTypeRef"), F.array()),
                lambda x: F.struct(
                    x["_ref"].alias("day_type_id"),
                    x["_version"].alias("day_type_version"),
                ),
            ),
        )
        .withColumn(
            "validity_conditions", F.col("record.validityConditions.AvailabilityCondition")
        )
        .withColumn("source_occurrence", _occurrence_struct_col())
        .withColumn(
            "_sj_occ_id",
            F.sha2(
                F.to_json(
                    F.struct(
                        F.col("publication_id").alias("publication_id"),
                        F.col("source_occurrence").alias("source_occurrence"),
                    )
                ),
                256,
            ),
        )
    )

    jp_targets = journey_patterns_df.select(
        F.col("publication_id").alias("_jp_pub_id"),
        F.col("journey_pattern_id").alias("_jp_id"),
        F.col("journey_pattern_version").alias("_jp_ver"),
        F.col("resolved_route_id").alias("_jp_route_id"),
        F.col("resolved_line_id").alias("_jp_line_id"),
        F.col("resolved_public_code").alias("_jp_public_code"),
        F.col("resolved_transport_mode").alias("_jp_transport_mode"),
        F.col("resolved_operator_ref").alias("_jp_route_operator_ref"),
    ).dropDuplicates()

    line_targets = lines_df.select(
        F.col("publication_id").alias("_l_pub_id"),
        F.col("line_id").alias("_l_id"),
        F.col("line_version").alias("_l_ver"),
        F.col("public_code").alias("_l_public_code"),
        F.col("transport_mode").alias("_l_transport_mode"),
        F.col("operator_ref").alias("_l_operator_ref"),
    ).dropDuplicates()

    joined_jp = sj_occ.join(
        jp_targets,
        on=(
            (sj_occ["publication_id"] == jp_targets["_jp_pub_id"])
            & _match_ref_expr(
                sj_occ["journey_pattern_ref"],
                sj_occ["journey_pattern_ref_version"],
                jp_targets["_jp_id"],
                jp_targets["_jp_ver"],
            )
        ),
        how="left",
    )

    joined_all = joined_jp.join(
        line_targets,
        on=(
            (F.col("publication_id") == F.col("_l_pub_id"))
            & _match_ref_expr(
                F.coalesce(F.col("line_ref"), F.col("_jp_line_id")),
                F.col("line_ref_version"),
                F.col("_l_id"),
                F.col("_l_ver"),
            )
        ),
        how="left",
    )

    cnt_win = Window.partitionBy("_sj_occ_id")
    pick_win = Window.partitionBy("_sj_occ_id").orderBy(
        F.col("_jp_id").asc_nulls_last(),
        F.col("_l_id").asc_nulls_last(),
    )

    resolved = (
        joined_all.withColumn(
            "pattern_match_count",
            F.sum(F.when(F.col("_jp_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))).over(cnt_win),
        )
        .withColumn("_rn", F.row_number().over(pick_win))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_sj_occ_id", "_jp_pub_id", "_l_pub_id")
        .withColumn(
            "resolved_journey_pattern_id",
            F.when(F.col("pattern_match_count") == 1, F.col("_jp_id")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "resolved_route_id",
            F.when(F.col("pattern_match_count") == 1, F.col("_jp_route_id")).otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "resolved_line_id",
            F.coalesce(
                F.when(F.col("pattern_match_count") == 1, F.col("_jp_line_id")),
                F.col("_l_id"),
            ),
        )
        .withColumn(
            "resolved_public_code",
            F.coalesce(
                F.when(F.col("pattern_match_count") == 1, F.col("_jp_public_code")),
                F.col("_l_public_code"),
            ),
        )
        .withColumn(
            "resolved_transport_mode",
            F.coalesce(
                F.when(F.col("pattern_match_count") == 1, F.col("_jp_transport_mode")),
                F.col("_l_transport_mode"),
            ),
        )
        .withColumn(
            "resolved_operator_ref",
            F.coalesce(
                F.col("operator_ref"),
                F.when(F.col("pattern_match_count") == 1, F.col("_jp_route_operator_ref")),
                F.col("_l_operator_ref"),
            ),
        )
        .withColumn(
            "journey_resolution_status",
            F.when(F.col("service_journey_id").isNull(), F.lit("missing_journey_id"))
            .when(F.col("pattern_match_count") > 1, F.lit("ambiguous_journey_pattern_ref"))
            .when(
                (F.col("pattern_match_count") == 1) & F.col("resolved_line_id").isNotNull(),
                F.lit("resolved"),
            )
            .when(F.col("pattern_match_count") == 1, F.lit("resolved_pattern_missing_line"))
            .when(F.col("journey_pattern_ref").isNull(), F.lit("missing_journey_pattern_ref"))
            .otherwise(F.lit("unresolved_journey_pattern_ref")),
        )
    )

    return resolved.select(
        "publication_id",
        "provider",
        "snapshot_date",
        "service_journey_id",
        "service_journey_version",
        "journey_name",
        "private_code",
        "departure_time_raw",
        "departure_day_offset_raw",
        "journey_pattern_ref",
        "journey_pattern_ref_version",
        "line_ref",
        "line_ref_version",
        "operator_ref",
        "operator_ref_version",
        "resolved_journey_pattern_id",
        "resolved_route_id",
        "resolved_line_id",
        "resolved_public_code",
        "resolved_transport_mode",
        "resolved_operator_ref",
        "pattern_match_count",
        "day_type_refs",
        "validity_conditions",
        "journey_resolution_status",
        "source_occurrence",
    )


def build_netex_definitions(
    spark: SparkSession,
    run_dirs: list[str],
    publication_lookup: DataFrame,
    provider: str = DEFAULT_PROVIDER,
) -> dict[str, DataFrame]:
    """Build all five publication-scoped NeTEx definition tables."""
    lines_df = build_lines(spark, run_dirs, publication_lookup, provider=provider)
    routes_df = build_routes(spark, run_dirs, publication_lookup, lines_df, provider=provider)
    jp_df, jps_df = build_journey_patterns_and_stops(
        spark, run_dirs, publication_lookup, routes_df, provider=provider
    )
    journeys_df = build_journeys(
        spark, run_dirs, publication_lookup, jp_df, lines_df, provider=provider
    )
    return {
        "lines": lines_df,
        "routes": routes_df,
        "journey_patterns": jp_df,
        "journey_pattern_stops": jps_df,
        "journeys": journeys_df,
    }