"""Complete snapshot projection, including shared calendars and national stops."""

import json
from pathlib import Path

from pipeline.ingestion.generate_provider_dataset import save_json
from .netex_schema import complete_frame_schema, stop_place_schema
from .staging_sources import load_snapshot_sources, verify_sources
from .xml_to_parquet import add_context, projected_tables, utc_now

CONTEXT = ["snapshot_date", "provider", "source_file", "source_member", "source_xml_sha256",
           "source_profile", "publication_timestamp", "valid_from", "valid_to"]


def reference_tables(frames):
    from pyspark.sql import functions as F
    common = CONTEXT + ["composite_frame_id", "composite_frame_version"]
    base = frames.select(*CONTEXT, F.col("_id").alias("composite_frame_id"),
                         F.col("_version").alias("composite_frame_version"), "frames")
    def expand(frame, container, kinds):
        parent = base.select(*common, F.posexplode("frames." + frame).alias("parent_frame_index", "parent"))
        parent = parent.select(*common, "parent_frame_index", F.col("parent._id").alias("parent_frame_id"),
                               F.col("parent._version").alias("parent_frame_version"),
                               F.col("parent." + container).alias("items"))
        keys = common + ["parent_frame_index", "parent_frame_id", "parent_frame_version"]
        result = None
        for kind in kinds:
            rows = parent.select(*keys, F.posexplode("items." + kind).alias("entity_index", "record"))
            rows = rows.withColumn("entity_type", F.lit(kind))
            result = rows if result is None else result.unionByName(rows)
        return result
    tables = {}
    for name, frame, container, kinds in (
        ("routes", "ServiceFrame", "routes", ("Route",)),
        ("journey_patterns", "ServiceFrame", "journeyPatterns", ("JourneyPattern",)),
        ("scheduled_stop_points", "ServiceFrame", "scheduledStopPoints", ("ScheduledStopPoint",)),
        ("stop_assignments", "ServiceFrame", "stopAssignments", ("PassengerStopAssignment", "FlexibleStopAssignment")),
        ("day_types", "ServiceCalendarFrame", "dayTypes", ("DayType",)),
        ("operating_periods", "ServiceCalendarFrame", "operatingPeriods", ("OperatingPeriod",)),
        ("operating_days", "ServiceCalendarFrame", "operatingDays", ("OperatingDay",)),
        ("day_type_assignments", "ServiceCalendarFrame", "dayTypeAssignments", ("DayTypeAssignment",)),
        ("flexible_stop_places", "SiteFrame", "flexibleStopPlaces", ("FlexibleStopPlace",)),
    ): tables[name] = expand(frame, container, kinds)
    patterns = tables["journey_patterns"]
    tables["journey_pattern_points"] = patterns.select(
        *[c for c in patterns.columns if c != "record"],
        F.col("record._id").alias("journey_pattern_id"), F.col("record._version").alias("journey_pattern_version"),
        F.posexplode("record.pointsInSequence.StopPointInJourneyPattern").alias("point_index", "record"))
    return tables


def convert_snapshot(spark, manifest_path, output, snapshot_date=None, selection=None):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    selection = selection or load_snapshot_sources(manifest_path, snapshot_date)
    output = Path(output).resolve()
    source_root = Path(selection["source_root"]).resolve()
    if output.is_relative_to(source_root) or source_root.is_relative_to(output):
        raise ValueError("Staging output must be separate from Bronze")
    output.mkdir(parents=True, exist_ok=False)
    metadata = output / "metadata"; metadata.mkdir()
    # Spark hides underscore-prefixed input files even when explicitly listed.
    # Read-only symbolic aliases expose these files without copying or renaming
    # Bronze. Source provenance continues to identify the original raw paths.
    inputs = output / "inputs"; inputs.mkdir()
    for group in ("files", "stop_files"):
        selection[group] = [dict(item) for item in selection[group]]
        for i, item in enumerate(selection[group]):
            if Path(item["source_file"]).name.startswith("_"):
                alias = inputs / f"{group}-{i}.xml"
                alias.symlink_to(item["source_file"])
                item["read_file"] = str(alias)
    report = {"schema_version": 2, "layer": "staging", "status": "in_progress",
              "started_at_utc": utc_now(), "spark_version": spark.version, **selection, "tables": {}}
    path = metadata / "manifest.json"
    save_json(path, report)
    frames = stops = None
    def reader(tag, schema, files):
        return (spark.read.format("xml").option("rowTag", tag).option("mode", "FAILFAST")
                .option("ignoreNamespace", "false").option("ignoreSurroundingSpaces", "false")
                .option("wildcardColName", "_unmapped_xml").schema(schema).load(files))
    try:
        raw = reader("CompositeFrame", complete_frame_schema(), [s.get("read_file", s["source_file"]) for s in selection["files"]])
        frames = add_context(spark, raw, selection, metadata / "frame_inventory.jsonl")
        frames = (frames.withColumn("valid_from", F.expr("try_element_at(validityConditions.AvailabilityCondition, 1).FromDate"))
                  .withColumn("valid_to", F.expr("try_element_at(validityConditions.AvailabilityCondition, 1).ToDate"))
                  .persist(StorageLevel.DISK_ONLY))
        tables = projected_tables(frames, ("valid_from", "valid_to"))
        tables.update(reference_tables(frames))
        raw_stops = reader("StopPlace", stop_place_schema(), [s.get("read_file", s["source_file"]) for s in selection["stop_files"]])
        stops = add_context(spark, raw_stops, {**selection, "files": selection["stop_files"]},
                            metadata / "stop_inventory.jsonl").drop("source_uri").persist(StorageLevel.DISK_ONLY)
        tables["stop_places"] = stops
        stop_context = [c for c in CONTEXT if c not in ("valid_from", "valid_to")]
        tables["quays"] = stops.select(*stop_context, F.col("_id").alias("stop_place_id"),
            F.col("_version").alias("stop_place_version"), F.posexplode("quays.Quay").alias("quay_index", "record"))
        for name, table in tables.items():
            expected = selection["expected_rows"][name]
            actual = table.count()
            if actual != expected: raise RuntimeError(f"{name}: {actual} rows; Bronze records {expected}")
            identity = "_id" if name in ("frames", "stop_places") else "record._id"
            if table.where(F.col(identity).isNull()).limit(1).count():
                raise RuntimeError(f"{name}: missing entity IDs")
            target = output / name
            table.write.mode("errorifexists").option("compression", "snappy").parquet(str(target))
            saved = spark.read.parquet(str(target))
            if saved.count() != actual: raise RuntimeError(f"{name}: read-back count mismatch")
            report["tables"][name] = {"path": name, "rows": actual,
                "schema": json.loads(saved.schema.json()),
                "parquet_bytes": sum(f.stat().st_size for f in target.rglob("*.parquet"))}
            save_json(path, report)
            print(f"staging/{name}: {actual:,} rows verified", flush=True)
        verify_sources(selection["files"] + selection["stop_files"])
        report.update(status="complete", completed_at_utc=utc_now(), source_checksums_unchanged=True)
        save_json(path, report)
        return report
    except Exception as error:
        report.update(status="failed", completed_at_utc=utc_now(), error=f"{type(error).__name__}: {error}")
        save_json(path, report)
        raise
    finally:
        if frames is not None: frames.unpersist()
        if stops is not None: stops.unpersist()
