"""Validated Silver projections and bounded service dates for one snapshot."""

from datetime import date, timedelta
import json
from pathlib import Path

from pipeline.ingestion.generate_provider_dataset import save_json
from pipeline.processing.bronze.xml_to_parquet import utc_now
from .calendars import expand_calendars


def occurrence(role):
    from pyspark.sql import functions as F
    names = ["snapshot_date", "provider", "source_member", "source_xml_sha256", "composite_frame_id",
             "parent_frame_id", "parent_frame_version", "parent_frame_index", "entity_type", "entity_index"]
    return F.sha2(F.to_json(F.struct(F.lit(role).alias("role"), *[F.col(c) for c in names])), 256)


def seconds(time, offset):
    """NeTEx clock seconds plus signed operating-day offset; preserve raw strings."""
    from pyspark.sql import functions as F
    clock = F.col(time)
    valid = clock.rlike(r"^([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]+)?$")
    off = F.when(F.col(offset).isNull(), F.lit(0)).otherwise(F.col(offset).try_cast("int"))
    return F.when(valid & off.isNotNull(), F.substring(clock, 1, 2).cast("int") * 3600
                  + F.substring(clock, 4, 2).cast("int") * 60
                  + F.substring(clock, 7, 20).cast("double") + off * 86400)


def lookup(source, target, ref, version, prefix, local=True, provider=True, extra=()):
    """Resolve at most one target; ambiguous references remain explicit failures."""
    from pyspark.sql import functions as F, Window
    target = target.select(*[F.col(c).alias(prefix + c) for c in target.columns])
    condition = F.col(ref) == F.col(prefix + "entity_id")
    condition &= (F.col(version).isNull() | F.col(version).isin("any", "*")
                  | (F.col(version) == F.col(prefix + "entity_version")))
    if provider: condition &= F.col("provider") == F.col(prefix + "provider")
    if local: condition &= F.col("source_member") == F.col(prefix + "source_member")
    for left, right in extra: condition &= F.col(left) == F.col(prefix + right)
    result = source.join(target, condition, "left")
    window = Window.partitionBy("occurrence_key")
    result = result.withColumn(prefix + "matches", F.count(prefix + "occurrence_key").over(window))
    result = result.withColumn("_candidate_row", F.row_number().over(
        window.orderBy(F.col(prefix + "occurrence_key").asc_nulls_last())))
    return result.where("_candidate_row=1").drop("_candidate_row").withColumn(
        prefix + "resolved", F.col(prefix + "matches") == 1)


def build_silver(spark, staging, output, service_from=None, service_to=None):
    from pyspark.sql import functions as F, Window
    from pyspark.sql.types import DateType, StringType, StructField, StructType
    staging, output = Path(staging).resolve(), Path(output).resolve()
    source = json.loads((staging / "metadata/manifest.json").read_text())
    if source.get("status") != "complete" or source.get("scope") != "complete_snapshot":
        raise ValueError("Silver requires complete snapshot staging, including shared definitions")
    if output.is_relative_to(staging) or staging.is_relative_to(output):
        raise ValueError("Silver output must be separate from staging")
    first = date.fromisoformat(str(service_from or source["snapshot_date"]))
    last = date.fromisoformat(str(service_to)) if service_to else first + timedelta(days=6)
    if last < first or (last-first).days > 365: raise ValueError("Service window must contain 1–366 days")
    output.mkdir(parents=True, exist_ok=False); (output / "metadata").mkdir()
    report = {"schema_version": 1, "layer": "silver", "status": "in_progress",
              "snapshot_date": source["snapshot_date"], "providers": source["providers"],
              "source_manifest": str(staging / "metadata/manifest.json"), "started_at_utc": utc_now(),
              "service_from": str(first), "service_to": str(last), "tables": {}, "quality": {}}
    path = output / "metadata/manifest.json"; save_json(path, report)
    def read(name): return spark.read.parquet(str(staging / source["tables"][name]["path"]))
    def write(name, frame):
        target = output / name
        frame.write.mode("errorifexists").option("compression", "snappy").parquet(str(target))
        saved = spark.read.parquet(str(target)); count = saved.count()
        report["tables"][name] = {"path": name, "rows": count, "schema": json.loads(saved.schema.json()),
            "parquet_bytes": sum(p.stat().st_size for p in target.glob("*.parquet"))}
        save_json(path, report); print(f"silver/{name}: {count:,} rows", flush=True)
        return saved
    def flatten(name, fields):
        raw = read(name)
        return raw.select("snapshot_date", "provider", "source_member", "source_xml_sha256",
            "valid_from", "valid_to", occurrence(name).alias("occurrence_key"),
            F.col("record._id").alias("entity_id"), F.col("record._version").alias("entity_version"),
            *[F.col(expression).alias(label) for label, expression in fields.items()])
    def canonical(name, frame, payload):
        # Definitions with identical keys and payloads can be collapsed; conflicting
        # definitions are withheld. Original occurrences remain in staging.
        keys = ["provider", "entity_id", "entity_version"]
        counts = frame.groupBy(*keys).agg(F.countDistinct(F.to_json(F.struct(*payload))).alias("variants"),
                                         F.count("*").alias("source_occurrences"))
        joined = frame.join(counts, keys, "inner")
        conflicts = joined.where("variants>1")
        conflict_count = conflicts.count()
        report["quality"][name + "_conflicting_occurrences"] = conflict_count
        if conflict_count: write("quarantine_" + name, conflicts)
        window = Window.partitionBy(*keys).orderBy("occurrence_key")
        return joined.where("variants=1").withColumn("_rank", F.row_number().over(window)).where("_rank=1").drop("_rank", "variants")
    try:
        lines = write("lines", flatten("lines", {"name": "record.Name", "public_code": "record.PublicCode",
            "transport_mode": "record.TransportMode", "line_kind": "entity_type"}))
        routes = flatten("routes", {"name": "record.Name", "direction": "record.DirectionType",
            "line_ref": "record.LineRef._ref", "line_ref_version": "record.LineRef._version",
            "flexible_line_ref": "record.FlexibleLineRef._ref", "flexible_line_ref_version": "record.FlexibleLineRef._version"})
        routes = write("routes", routes.withColumn("line_ref",F.coalesce("line_ref","flexible_line_ref"))
                       .withColumn("line_ref_version",F.coalesce("line_ref_version","flexible_line_ref_version")))
        patterns = flatten("journey_patterns", {"name": "record.Name", "route_ref": "record.RouteRef._ref",
                                               "route_ref_version": "record.RouteRef._version"})
        patterns = lookup(patterns, routes.select("provider", "source_member", "entity_id", "entity_version",
            "occurrence_key", "line_ref", "line_ref_version", "direction"), "route_ref", "route_ref_version", "route_")
        patterns = write("journey_patterns", patterns)
        raw_points = read("journey_pattern_points")
        points = raw_points.select("snapshot_date", "provider", "source_member", "source_xml_sha256",
            occurrence("journey_patterns").alias("pattern_key"), "point_index",
            F.col("record._id").alias("entity_id"), F.col("record._version").alias("entity_version"),
            F.col("record._order").try_cast("int").alias("source_order"),
            F.col("record.ScheduledStopPointRef._ref").alias("scheduled_stop_ref"),
            F.col("record.ScheduledStopPointRef._version").alias("scheduled_stop_version"),
            F.col("record.ForBoarding").alias("for_boarding_raw"), F.col("record.ForAlighting").alias("for_alighting_raw"))
        points = write("journey_pattern_points", points.withColumn("occurrence_key", F.sha2(
            F.to_json(F.struct("pattern_key", "point_index")), 256)))
        stops = flatten("scheduled_stop_points", {"name": "record.Name"})
        stops = write("scheduled_stop_points", canonical("scheduled_stop_points", stops, ["name"]))
        assignments = flatten("stop_assignments", {"scheduled_stop_ref": "record.ScheduledStopPointRef._ref",
            "scheduled_stop_version": "record.ScheduledStopPointRef._version", "quay_ref": "record.QuayRef._ref",
            "quay_version": "record.QuayRef._version", "stop_place_ref": "record.StopPlaceRef._ref",
            "flexible_stop_place_ref": "record.FlexibleStopPlaceRef._ref"})
        assignments = (assignments.drop("entity_id", "entity_version")
            .withColumnRenamed("scheduled_stop_ref", "entity_id").withColumnRenamed("scheduled_stop_version", "entity_version")
            .withColumn("entity_version", F.coalesce("entity_version", F.lit(""))))
        assignments = write("stop_assignments", canonical("stop_assignments", assignments,
                            ["quay_ref", "quay_version", "stop_place_ref", "flexible_stop_place_ref"]))
        raw_quays = read("quays")
        quays = raw_quays.select("snapshot_date", "provider", "source_member", "source_xml_sha256",
            F.sha2(F.to_json(F.struct("source_xml_sha256", "stop_place_id", "quay_index")),256).alias("occurrence_key"),
            F.col("record._id").alias("entity_id"), F.col("record._version").alias("entity_version"),
            "stop_place_id", F.col("record.Name").alias("name"),
            F.col("record.Centroid.Location.Latitude").try_cast("double").alias("latitude"),
            F.col("record.Centroid.Location.Longitude").try_cast("double").alias("longitude"))
        quays = write("quays", canonical("quays", quays, ["stop_place_id", "name", "latitude", "longitude"]))
        raw_places = read("stop_places")
        places = raw_places.select("snapshot_date", "source_member", "source_xml_sha256",
            F.col("_id").alias("stop_place_id"), F.col("_version").alias("stop_place_version"),
            F.col("Name").alias("name"), F.col("TransportMode").alias("transport_mode"),
            F.col("Centroid.Location.Latitude").try_cast("double").alias("latitude"),
            F.col("Centroid.Location.Longitude").try_cast("double").alias("longitude"))
        write("stop_places", places)
        flex = flatten("flexible_stop_places", {"name": "record.Name"})
        flex = write("flexible_stop_places", canonical("flexible_stop_places", flex, ["name"]))
        # Calendar definitions retain their raw schema and provenance. Only the
        # small reference tables are collected for date/exception resolution.
        calendar_frames = {n: read(n) for n in ("day_types", "operating_periods", "operating_days", "day_type_assignments")}
        day_rows = calendar_frames["day_types"].collect()
        calendar, calendar_errors = expand_calendars(day_rows, calendar_frames["operating_periods"].collect(),
            calendar_frames["operating_days"].collect(), calendar_frames["day_type_assignments"].collect(), first, last)
        schema = StructType([StructField("provider", StringType()), StructField("day_type_id", StringType()),
                             StructField("day_type_version", StringType()), StructField("service_date", DateType())])
        calendar = write("calendar_dates", spark.createDataFrame(calendar, schema).dropDuplicates())
        issue_schema = StructType([StructField(n, StringType()) for n in ("provider", "day_type_id", "day_type_version", "reason")])
        issues = write("calendar_issues", spark.createDataFrame(calendar_errors, issue_schema))
        dtypes = calendar_frames["day_types"].select("provider", "source_member",
            occurrence("day_types").alias("occurrence_key"), F.col("record._id").alias("entity_id"),
            F.col("record._version").alias("entity_version"))
        raw_journeys = read("service_journeys")
        journeys = flatten("service_journeys", {"line_ref": "record.LineRef._ref", "line_ref_version": "record.LineRef._version",
            "pattern_ref": "record.JourneyPatternRef._ref", "pattern_ref_version": "record.JourneyPatternRef._version",
            "departure_time_raw": "record.DepartureTime", "departure_day_offset_raw": "record.DepartureDayOffset",
            "day_type_refs": "record.dayTypes.DayTypeRef", "journey_kind": "entity_type"})
        patterns_small = patterns.select("provider", "source_member", "entity_id", "entity_version", "occurrence_key",
            "route_resolved", "route_entity_id", "route_entity_version", "route_line_ref", "route_line_ref_version", "route_direction")
        journeys = lookup(journeys, patterns_small, "pattern_ref", "pattern_ref_version", "pattern_")
        journeys = journeys.withColumn("effective_line_ref", F.coalesce("line_ref", "pattern_route_line_ref"))
        journeys = journeys.withColumn("effective_line_version", F.coalesce("line_ref_version", "pattern_route_line_ref_version"))
        journeys = lookup(journeys, lines.select("provider", "source_member", "entity_id", "entity_version", "occurrence_key",
            "public_code", "transport_mode", "line_kind"), "effective_line_ref", "effective_line_version", "line_")
        journeys = write("journey_definitions", journeys.withColumn("is_flexible", F.coalesce(
            (F.col("line_line_kind") == "FlexibleLine") | (F.col("journey_kind") == "FlexibleServiceJourney"),F.lit(False))))
        refs = journeys.select(F.col("occurrence_key").alias("journey_key"), "provider", "source_member",
            F.posexplode("day_type_refs").alias("day_type_index", "ref"))
        refs = (refs.withColumn("occurrence_key", F.sha2(F.to_json(F.struct("journey_key", "day_type_index")),256))
                .withColumn("day_type_ref", F.col("ref._ref")).withColumn("day_type_ref_version", F.col("ref._version")))
        refs = lookup(refs, dtypes, "day_type_ref", "day_type_ref_version", "day_", local=False)
        error_days = issues.select(F.col("provider").alias("error_provider"), F.col("day_type_id").alias("error_id"),
                                  F.col("day_type_version").alias("error_version")).dropDuplicates()
        refs = refs.join(error_days, (refs.provider == error_days.error_provider)
            & (refs.day_entity_id == error_days.error_id) & refs.day_entity_version.eqNullSafe(error_days.error_version), "left")
        refs = write("journey_day_types", refs.withColumn("calendar_resolved", F.col("day_resolved") & F.col("error_id").isNull()))
        calendar_flags = refs.groupBy("journey_key").agg(F.min(F.col("calendar_resolved").cast("int")).alias("calendar_ok"))
        dated = refs.where("calendar_resolved").join(calendar, (refs.provider == calendar.provider)
            & (refs.day_entity_id == calendar.day_type_id) & refs.day_entity_version.eqNullSafe(calendar.day_type_version), "inner")
        dated = dated.select("journey_key", "service_date").dropDuplicates()
        raw_times = read("passing_times")
        times = raw_times.select("snapshot_date", "provider", "source_member", "source_xml_sha256",
            occurrence("service_journeys").alias("journey_key"), "passing_time_index",
            F.col("record._id").alias("passing_time_id"), F.col("record._version").alias("passing_time_version"),
            F.col("record.StopPointInJourneyPatternRef._ref").alias("point_ref"),
            F.col("record.StopPointInJourneyPatternRef._version").alias("point_ref_version"),
            F.col("record.ArrivalTime").alias("arrival_time_raw"), F.col("record.DepartureTime").alias("departure_time_raw"),
            F.col("record.EarliestDepartureTime").alias("earliest_departure_time_raw"),
            F.col("record.LatestArrivalTime").alias("latest_arrival_time_raw"),
            F.col("record.ArrivalDayOffset").alias("arrival_day_offset_raw"),
            F.col("record.DepartureDayOffset").alias("departure_day_offset_raw"))
        j_context = journeys.select(F.col("occurrence_key").alias("journey_key"), "is_flexible",
            F.col("pattern_occurrence_key").alias("pattern_key"), F.col("line_occurrence_key").alias("line_key"))
        times = times.join(j_context, "journey_key", "left").withColumn("occurrence_key",
            F.sha2(F.to_json(F.struct("journey_key", "passing_time_index")),256))
        times = lookup(times, points.select("provider", "source_member", "occurrence_key", "entity_id", "entity_version",
            "pattern_key", "scheduled_stop_ref", "scheduled_stop_version", "source_order"),
            "point_ref", "point_ref_version", "point_", extra=(("pattern_key", "pattern_key"),))
        times = lookup(times, stops.select("provider", "occurrence_key", "entity_id", "entity_version", "name"),
            "point_scheduled_stop_ref", "point_scheduled_stop_version", "stop_", local=False)
        # Assignment reference versions can be absent; a unique stop ID is sufficient.
        times = times.withColumn("assignment_ref_version", F.lit(None).cast("string"))
        times = lookup(times, assignments.select("provider", "occurrence_key", "entity_id", "entity_version",
            "quay_ref", "quay_version", "stop_place_ref", "flexible_stop_place_ref"),
            "point_scheduled_stop_ref", "assignment_ref_version", "assignment_", local=False)
        times = lookup(times, quays.select("provider", "occurrence_key", "entity_id", "entity_version", "stop_place_id", "name", "latitude", "longitude"),
            "assignment_quay_ref", "assignment_quay_version", "quay_", local=False, provider=False)
        times = times.withColumn("flex_version", F.lit(None).cast("string"))
        times = lookup(times, flex.select("provider", "occurrence_key", "entity_id", "entity_version", "name"),
            "assignment_flexible_stop_place_ref", "flex_version", "flex_", local=False)
        times = times.withColumn("arrival_seconds", seconds("arrival_time_raw", "arrival_day_offset_raw"))
        times = times.withColumn("departure_seconds", seconds("departure_time_raw", "departure_day_offset_raw"))
        order = Window.partitionBy("journey_key").orderBy("passing_time_index")
        times = times.withColumn("previous_seconds", F.lag(F.coalesce("departure_seconds", "arrival_seconds")).over(order))
        invalid_time = ((F.col("arrival_time_raw").isNotNull() & F.col("arrival_seconds").isNull())
            | (F.col("departure_time_raw").isNotNull() & F.col("departure_seconds").isNull())
            | (F.col("arrival_time_raw").isNull() & F.col("departure_time_raw").isNull() & ~F.col("is_flexible")))
        reasons = [(~F.col("point_resolved"), "unresolved_pattern_point"),
                   (~F.col("stop_resolved"), "unresolved_scheduled_stop"),
                   (~F.col("assignment_resolved"), "unresolved_stop_assignment"),
                   (~(F.col("quay_resolved") | F.col("flex_resolved")), "unresolved_physical_stop"),
                   (invalid_time, "invalid_time"),
                   (F.col("departure_seconds") < F.col("arrival_seconds"), "arrival_after_departure"),
                   (F.coalesce("arrival_seconds", "departure_seconds") < F.col("previous_seconds"), "non_monotonic_time")]
        times = times.withColumn("reason_codes", F.filter(F.array(*[F.when(c,F.lit(r)) for c,r in reasons]), lambda x:x.isNotNull()))
        times = write("passing_times", times.withColumn("is_valid", F.size("reason_codes") == 0))
        write("quarantine_passing_times", times.where("NOT is_valid"))
        time_flags = times.groupBy("journey_key").agg(F.count("*").alias("passing_time_count"),
            F.sum((~F.col("is_valid")).cast("long")).alias("invalid_passing_times"),
            F.min("departure_seconds").alias("first_departure_seconds"),
            F.max(F.coalesce("arrival_seconds", "departure_seconds")).alias("last_arrival_seconds"))
        final = journeys.withColumnRenamed("occurrence_key", "journey_key").join(time_flags,"journey_key","left").join(calendar_flags,"journey_key","left")
        conditions = [(~F.col("line_resolved"), "unresolved_line"), (~F.col("pattern_resolved"), "unresolved_pattern"),
                      (~F.coalesce("pattern_route_resolved",F.lit(False)), "unresolved_route"),
                      (F.coalesce("calendar_ok",F.lit(0)) == 0, "unresolved_calendar"),
                      (F.coalesce("invalid_passing_times",F.lit(0)) > 0, "invalid_passing_times"),
                      (F.col("passing_time_count").isNull() & ~F.col("is_flexible"), "missing_passing_times")]
        final = final.withColumn("reason_codes", F.filter(F.array(*[F.when(c,F.lit(r)) for c,r in conditions]),lambda x:x.isNotNull()))
        final = write("journeys", final.withColumn("is_valid", F.size("reason_codes")==0))
        write("quarantine_journeys", final.where("NOT is_valid"))
        dated = dated.join(final.select("journey_key", "provider", "snapshot_date", "line_entity_id", "line_entity_version",
            "line_public_code", "line_transport_mode", "is_flexible", "is_valid", "first_departure_seconds", "last_arrival_seconds"),"journey_key","inner")
        dated = write("journey_service_dates", dated)
        input_counts = {n:source["tables"][n]["rows"] for n in ("lines", "service_journeys", "passing_times")}
        if report["tables"]["lines"]["rows"] != input_counts["lines"] or report["tables"]["journeys"]["rows"] != input_counts["service_journeys"] or report["tables"]["passing_times"]["rows"] != input_counts["passing_times"]:
            raise RuntimeError("Silver joins changed source occurrence counts")
        report["quality"].update(input_counts=input_counts,
            quarantined_journeys=report["tables"]["quarantine_journeys"]["rows"],
            quarantined_passing_times=report["tables"]["quarantine_passing_times"]["rows"],
            calendar_issues=len(calendar_errors),
            gold_eligible_service_occurrences=dated.where("is_valid AND NOT is_flexible").count())
        report.update(status="complete", completed_at_utc=utc_now())
        save_json(path,report); return report
    except Exception as error:
        report.update(status="failed", completed_at_utc=utc_now(), error=f"{type(error).__name__}: {error}")
        save_json(path,report); raise
