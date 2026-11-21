"""Canonical SIRI ET stop-call preparation and classification for the DAT535 Silver layer."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from . import DEFAULT_PROVIDER, DEFAULT_TIMEZONE


def _classify_time_kind(ts_col, est_col):
    """Classify arrival or departure timestamp separately using Entur's estimated flag."""
    return (
        F.when(ts_col.isNull(), F.lit("missing"))
        .when(est_col == F.lit(False), F.lit("recorded"))
        .when(est_col == F.lit(True), F.lit("estimated"))
        .otherwise(F.lit("unresolved"))
    )


def build_siri_et_stop_calls(
    spark: SparkSession,
    siri_lookup: DataFrame,
    provider: str = DEFAULT_PROVIDER,
    timezone_name: str = DEFAULT_TIMEZONE,
) -> DataFrame:
    """Build canonical siri_et_stop_calls with duplicate accounting and flag checks."""
    available_rows = (
        siri_lookup.filter(F.col("siri_source_file").isNotNull())
        .select("operating_date", "siri_source_file", "siri_source_sha256")
        .dropDuplicates()
    )

    file_paths = [r["siri_source_file"] for r in available_rows.collect()]
    if not file_paths:
        return _empty_siri_et_df(spark)

    raw_et = (
        spark.read.parquet(*file_paths)
        .withColumn("_input_file_uri", F.input_file_name())
        .withColumn(
            "provider",
            F.upper(F.coalesce(F.col("dataSource"), F.lit(provider))),
        )
        .filter(F.col("provider") == provider.upper())
        .withColumn("operating_date", F.col("operatingDate").cast("date"))
        .withColumn("operating_date_str", F.date_format(F.col("operating_date"), "yyyy-MM-dd"))
    )

    normalized_et = raw_et.withColumn(
        "_norm_file_path",
        F.regexp_replace(F.col("_input_file_uri"), r"^file:(/+)", "/"),
    )

    with_prov = normalized_et.join(
        available_rows.select(
            F.col("operating_date").alias("_sel_op_date"),
            F.col("siri_source_file"),
            F.col("siri_source_sha256"),
        ),
        on=(
            (normalized_et["operating_date_str"] == F.col("_sel_op_date"))
            & (normalized_et["_norm_file_path"] == F.col("siri_source_file"))
        ),
        how="inner",
    ).drop("_input_file_uri", "_norm_file_path", "_sel_op_date")

    staged_et = (
        with_prov.withColumn("line_ref", F.col("lineRef"))
        .withColumn("direction_ref", F.col("directionRef"))
        .withColumn("day_of_the_week", F.col("dayOfTheWeek").cast("int"))
        .withColumn("service_journey_id", F.col("serviceJourneyId"))
        .withColumn("dated_service_journey_id", F.col("datedServiceJourneyId"))
        .withColumn("operator_ref", F.col("operatorRef"))
        .withColumn("vehicle_mode", F.col("vehicleMode"))
        .withColumn("extra_journey", F.col("extraJourney").cast("boolean"))
        .withColumn("journey_cancellation", F.col("journeyCancellation").cast("boolean"))
        .withColumn("stop_point_ref", F.col("stopPointRef"))
        .withColumn("sequence_nr", F.col("sequenceNr").cast("long"))
        .withColumn("stop_point_name", F.col("stopPointName"))
        .withColumn("origin_name", F.col("originName"))
        .withColumn("destination_name", F.col("destinationName"))
        .withColumn("extra_call", F.col("extraCall").cast("boolean"))
        .withColumn("stop_cancellation", F.col("stopCancellation").cast("boolean"))
        .withColumn("estimated_flag", F.col("estimated").cast("boolean"))
        .withColumn("recorded_at_utc", F.col("recordedAtTime").cast("timestamp"))
        .withColumn("aimed_arrival_utc", F.col("aimedArrivalTime").cast("timestamp"))
        .withColumn("reported_arrival_utc", F.col("arrivalTime").cast("timestamp"))
        .withColumn("aimed_departure_utc", F.col("aimedDepartureTime").cast("timestamp"))
        .withColumn("reported_departure_utc", F.col("departureTime").cast("timestamp"))
        .withColumn("data_source", F.col("dataSource"))
        .withColumn("data_source_name", F.col("dataSourceName"))
    )

    exact_payload_cols = [
        "provider",
        "operating_date",
        "operating_date_str",
        "siri_source_file",
        "siri_source_sha256",
        "line_ref",
        "direction_ref",
        "day_of_the_week",
        "service_journey_id",
        "dated_service_journey_id",
        "operator_ref",
        "vehicle_mode",
        "extra_journey",
        "journey_cancellation",
        "stop_point_ref",
        "sequence_nr",
        "stop_point_name",
        "origin_name",
        "destination_name",
        "extra_call",
        "stop_cancellation",
        "estimated_flag",
        "aimed_arrival_utc",
        "reported_arrival_utc",
        "aimed_departure_utc",
        "reported_departure_utc",
        "data_source",
        "data_source_name",
    ]

    collapsed = staged_et.groupBy(*exact_payload_cols).agg(
        F.count(F.lit(1)).cast("long").alias("source_occurrences"),
        F.min("recorded_at_utc").alias("first_recorded_at_utc"),
        F.max("recorded_at_utc").alias("latest_recorded_at_utc"),
    )

    effective_journey_key = F.coalesce(
        F.col("dated_service_journey_id"),
        F.concat_ws(":", F.col("operating_date_str"), F.col("service_journey_id")),
    )

    cand_win = Window.partitionBy(
        "provider",
        "operating_date",
        effective_journey_key,
        "sequence_nr",
    )

    journey_win = Window.partitionBy(
        "provider",
        "operating_date",
        effective_journey_key,
    )

    with_dup_and_journey_checks = (
        collapsed.withColumn(
            "candidate_key_variant_count",
            F.count(F.lit(1)).over(cand_win).cast("int"),
        )
        .withColumn(
            "candidate_key_total_occurrences",
            F.sum("source_occurrences").over(cand_win).cast("long"),
        )
        .withColumn(
            "duplicate_status",
            F.when(
                F.col("dated_service_journey_id").isNull() & F.col("service_journey_id").isNull(),
                F.lit("missing_journey_key"),
            )
            .when(F.col("sequence_nr").isNull(), F.lit("missing_sequence_nr"))
            .when(F.col("candidate_key_variant_count") > 1, F.lit("conflicting_candidate_key"))
            .when(F.col("source_occurrences") > 1, F.lit("collapsed_exact_duplicates"))
            .otherwise(F.lit("unique")),
        )
        .withColumn(
            "_has_j_cancel_true",
            F.max(F.when(F.col("journey_cancellation") == F.lit(True), 1).otherwise(0)).over(
                journey_win
            ),
        )
        .withColumn(
            "_has_j_cancel_false",
            F.max(F.when(F.col("journey_cancellation") == F.lit(False), 1).otherwise(0)).over(
                journey_win
            ),
        )
        .withColumn(
            "_has_j_cancel_null",
            F.max(F.when(F.col("journey_cancellation").isNull(), 1).otherwise(0)).over(journey_win),
        )
        .withColumn(
            "journey_cancellation_state",
            F.when(
                (F.col("_has_j_cancel_true") == 1) & (F.col("_has_j_cancel_false") == 1),
                F.lit("conflicting_across_stops"),
            )
            .when(
                (F.col("_has_j_cancel_true") == 1) & (F.col("_has_j_cancel_null") == 1),
                F.lit("partially_unknown_true"),
            )
            .when(
                (F.col("_has_j_cancel_false") == 1) & (F.col("_has_j_cancel_null") == 1),
                F.lit("partially_unknown_false"),
            )
            .when(F.col("_has_j_cancel_true") == 1, F.lit("cancelled"))
            .when(F.col("_has_j_cancel_false") == 1, F.lit("not_cancelled"))
            .otherwise(F.lit("unknown")),
        )
        .withColumn(
            "stop_cancellation_state",
            F.when(F.col("stop_cancellation") == F.lit(True), F.lit("cancelled"))
            .when(F.col("stop_cancellation") == F.lit(False), F.lit("not_cancelled"))
            .otherwise(F.lit("unknown")),
        )
        .withColumn(
            "arrival_time_kind",
            _classify_time_kind(F.col("reported_arrival_utc"), F.col("estimated_flag")),
        )
        .withColumn(
            "departure_time_kind",
            _classify_time_kind(F.col("reported_departure_utc"), F.col("estimated_flag")),
        )
        .withColumn(
            "aimed_arrival_local_oslo",
            F.from_utc_timestamp(F.col("aimed_arrival_utc"), timezone_name),
        )
        .withColumn(
            "reported_arrival_local_oslo",
            F.from_utc_timestamp(F.col("reported_arrival_utc"), timezone_name),
        )
        .withColumn(
            "aimed_departure_local_oslo",
            F.from_utc_timestamp(F.col("aimed_departure_utc"), timezone_name),
        )
        .withColumn(
            "reported_departure_local_oslo",
            F.from_utc_timestamp(F.col("reported_departure_utc"), timezone_name),
        )
        .withColumn(
            "reported_arrival_delay_sec",
            F.when(
                F.col("reported_arrival_utc").isNotNull() & F.col("aimed_arrival_utc").isNotNull(),
                F.unix_micros(F.col("reported_arrival_utc")) / F.lit(1_000_000.0)
                - F.unix_micros(F.col("aimed_arrival_utc")) / F.lit(1_000_000.0),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "reported_departure_delay_sec",
            F.when(
                F.col("reported_departure_utc").isNotNull()
                & F.col("aimed_departure_utc").isNotNull(),
                F.unix_micros(F.col("reported_departure_utc")) / F.lit(1_000_000.0)
                - F.unix_micros(F.col("aimed_departure_utc")) / F.lit(1_000_000.0),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "quality_reason_codes",
            F.array_compact(
                F.array(
                    F.when(
                        F.col("duplicate_status") == "conflicting_candidate_key",
                        F.lit("conflicting_reported_record"),
                    ),
                    F.when(
                        F.col("duplicate_status") == "missing_journey_key",
                        F.lit("missing_service_journey_id"),
                    ),
                    F.when(
                        F.col("duplicate_status") == "missing_sequence_nr",
                        F.lit("missing_sequence_nr"),
                    ),
                    F.when(
                        F.col("stop_point_ref").isNull(),
                        F.lit("missing_stop_point_ref"),
                    ),
                    F.when(
                        F.col("journey_cancellation_state") == "conflicting_across_stops",
                        F.lit("conflicting_journey_cancellation"),
                    ),
                    F.when(
                        F.col("journey_cancellation_state").isin(
                            "unknown", "partially_unknown_true", "partially_unknown_false"
                        ),
                        F.lit("unknown_journey_cancellation"),
                    ),
                    F.when(
                        F.col("stop_cancellation_state") == "unknown",
                        F.lit("unknown_stop_cancellation"),
                    ),
                    F.when(
                        F.col("arrival_time_kind") == "estimated",
                        F.lit("estimated_arrival_time"),
                    ),
                    F.when(
                        F.col("arrival_time_kind") == "unresolved",
                        F.lit("unknown_arrival_time_classification"),
                    ),
                    F.when(
                        F.col("departure_time_kind") == "estimated",
                        F.lit("estimated_departure_time"),
                    ),
                    F.when(
                        F.col("departure_time_kind") == "unresolved",
                        F.lit("unknown_departure_time_classification"),
                    ),
                    F.when(
                        F.col("extra_journey") == F.lit(True),
                        F.lit("extra_journey"),
                    ),
                    F.when(
                        F.col("extra_call") == F.lit(True),
                        F.lit("extra_call"),
                    ),
                )
            ),
        )
        .withColumn(
            "et_canonical_id",
            F.sha2(
                F.concat_ws(
                    "\x1f",
                    F.coalesce(F.col("provider"), F.lit("<NULL>")),
                    F.coalesce(F.col("operating_date_str"), F.lit("<NULL>")),
                    F.coalesce(F.col("siri_source_file"), F.lit("<NULL>")),
                    F.coalesce(F.col("siri_source_sha256"), F.lit("<NULL>")),
                    F.coalesce(F.col("line_ref"), F.lit("<NULL>")),
                    F.coalesce(F.col("direction_ref"), F.lit("<NULL>")),
                    F.coalesce(F.col("day_of_the_week").cast("string"), F.lit("<NULL>")),
                    F.coalesce(F.col("service_journey_id"), F.lit("<NULL>")),
                    F.coalesce(F.col("dated_service_journey_id"), F.lit("<NULL>")),
                    F.coalesce(F.col("operator_ref"), F.lit("<NULL>")),
                    F.coalesce(F.col("vehicle_mode"), F.lit("<NULL>")),
                    F.coalesce(F.col("extra_journey").cast("string"), F.lit("<NULL>")),
                    F.coalesce(F.col("journey_cancellation").cast("string"), F.lit("<NULL>")),
                    F.coalesce(F.col("stop_point_ref"), F.lit("<NULL>")),
                    F.coalesce(F.col("sequence_nr").cast("string"), F.lit("<NULL>")),
                    F.coalesce(F.col("stop_point_name"), F.lit("<NULL>")),
                    F.coalesce(F.col("origin_name"), F.lit("<NULL>")),
                    F.coalesce(F.col("destination_name"), F.lit("<NULL>")),
                    F.coalesce(F.col("extra_call").cast("string"), F.lit("<NULL>")),
                    F.coalesce(F.col("stop_cancellation").cast("string"), F.lit("<NULL>")),
                    F.coalesce(F.col("estimated_flag").cast("string"), F.lit("<NULL>")),
                    F.coalesce(F.col("aimed_arrival_utc").cast("string"), F.lit("<NULL>")),
                    F.coalesce(F.col("reported_arrival_utc").cast("string"), F.lit("<NULL>")),
                    F.coalesce(F.col("aimed_departure_utc").cast("string"), F.lit("<NULL>")),
                    F.coalesce(F.col("reported_departure_utc").cast("string"), F.lit("<NULL>")),
                    F.coalesce(F.col("data_source"), F.lit("<NULL>")),
                    F.coalesce(F.col("data_source_name"), F.lit("<NULL>")),
                ),
                256,
            ),
        )
        .drop("_has_j_cancel_true", "_has_j_cancel_false", "_has_j_cancel_null")
    )

    return with_dup_and_journey_checks.select(
        "et_canonical_id",
        "provider",
        "operating_date",
        "day_of_the_week",
        "line_ref",
        "direction_ref",
        "service_journey_id",
        "dated_service_journey_id",
        "operator_ref",
        "vehicle_mode",
        "stop_point_ref",
        "sequence_nr",
        "stop_point_name",
        "origin_name",
        "destination_name",
        "extra_journey",
        "extra_call",
        "journey_cancellation",
        "journey_cancellation_state",
        "stop_cancellation",
        "stop_cancellation_state",
        "estimated_flag",
        "arrival_time_kind",
        "departure_time_kind",
        "aimed_arrival_utc",
        "reported_arrival_utc",
        "aimed_arrival_local_oslo",
        "reported_arrival_local_oslo",
        "reported_arrival_delay_sec",
        "aimed_departure_utc",
        "reported_departure_utc",
        "aimed_departure_local_oslo",
        "reported_departure_local_oslo",
        "reported_departure_delay_sec",
        "first_recorded_at_utc",
        "latest_recorded_at_utc",
        "source_occurrences",
        "candidate_key_variant_count",
        "candidate_key_total_occurrences",
        "duplicate_status",
        "quality_reason_codes",
        "data_source",
        "data_source_name",
        "siri_source_file",
        "siri_source_sha256",
    )


def _empty_siri_et_df(spark: SparkSession) -> DataFrame:
    """Return an empty DataFrame matching build_siri_et_stop_calls output columns."""
    schema = T.StructType(
        [
            T.StructField("et_canonical_id", T.StringType(), False),
            T.StructField("provider", T.StringType(), False),
            T.StructField("operating_date", T.DateType(), True),
            T.StructField("day_of_the_week", T.IntegerType(), True),
            T.StructField("line_ref", T.StringType(), True),
            T.StructField("direction_ref", T.StringType(), True),
            T.StructField("service_journey_id", T.StringType(), True),
            T.StructField("dated_service_journey_id", T.StringType(), True),
            T.StructField("operator_ref", T.StringType(), True),
            T.StructField("vehicle_mode", T.StringType(), True),
            T.StructField("stop_point_ref", T.StringType(), True),
            T.StructField("sequence_nr", T.LongType(), True),
            T.StructField("stop_point_name", T.StringType(), True),
            T.StructField("origin_name", T.StringType(), True),
            T.StructField("destination_name", T.StringType(), True),
            T.StructField("extra_journey", T.BooleanType(), True),
            T.StructField("extra_call", T.BooleanType(), True),
            T.StructField("journey_cancellation", T.BooleanType(), True),
            T.StructField("journey_cancellation_state", T.StringType(), False),
            T.StructField("stop_cancellation", T.BooleanType(), True),
            T.StructField("stop_cancellation_state", T.StringType(), False),
            T.StructField("estimated_flag", T.BooleanType(), True),
            T.StructField("arrival_time_kind", T.StringType(), False),
            T.StructField("departure_time_kind", T.StringType(), False),
            T.StructField("aimed_arrival_utc", T.TimestampType(), True),
            T.StructField("reported_arrival_utc", T.TimestampType(), True),
            T.StructField("aimed_arrival_local_oslo", T.TimestampType(), True),
            T.StructField("reported_arrival_local_oslo", T.TimestampType(), True),
            T.StructField("reported_arrival_delay_sec", T.DoubleType(), True),
            T.StructField("aimed_departure_utc", T.TimestampType(), True),
            T.StructField("reported_departure_utc", T.TimestampType(), True),
            T.StructField("aimed_departure_local_oslo", T.TimestampType(), True),
            T.StructField("reported_departure_local_oslo", T.TimestampType(), True),
            T.StructField("reported_departure_delay_sec", T.DoubleType(), True),
            T.StructField("first_recorded_at_utc", T.TimestampType(), True),
            T.StructField("latest_recorded_at_utc", T.TimestampType(), True),
            T.StructField("source_occurrences", T.LongType(), False),
            T.StructField("candidate_key_variant_count", T.IntegerType(), False),
            T.StructField("candidate_key_total_occurrences", T.LongType(), False),
            T.StructField("duplicate_status", T.StringType(), False),
            T.StructField("quality_reason_codes", T.ArrayType(T.StringType()), False),
            T.StructField("data_source", T.StringType(), True),
            T.StructField("data_source_name", T.StringType(), True),
            T.StructField("siri_source_file", T.StringType(), True),
            T.StructField("siri_source_sha256", T.StringType(), True),
        ]
    )
    return spark.createDataFrame([], schema)