"""Stage one validated snapshot: NeTEx XML to entity Parquet tables with source context.

Spark 4's built-in XML reader parses provider CompositeFrames (line and shared
files) and the national StopPlaces with explicit schemas. ``posexplode`` turns
each nested entity list into rows that keep their parent frame and source
position, so no occurrence is merged or dropped here. Every table's row count
must equal the count recorded by Bronze validation, and is checked again after
the Parquet read-back. This is staging, not Silver: values stay raw strings.
"""

import json
from pathlib import Path

from pipeline.common import utc_now, write_json
from pipeline.storage.zip_bronze import with_snapshot_xml
from .netex_schema import complete_frame_schema, stop_place_schema
from .staging_sources import load_snapshot_sources, verify_sources

# Source context carried on every staged frame and entity row.
CONTEXT = ["snapshot_date", "provider", "source_file", "source_member", "source_xml_sha256",
           "source_profile", "publication_timestamp", "valid_from", "valid_to"]

# Output table -> (frame type, container element, entity element names).
ENTITY_TABLES = {
    "lines": ("ServiceFrame", "lines", ("Line", "FlexibleLine")),
    "service_journeys": ("TimetableFrame", "vehicleJourneys", ("ServiceJourney", "FlexibleServiceJourney")),
    "routes": ("ServiceFrame", "routes", ("Route",)),
    "journey_patterns": ("ServiceFrame", "journeyPatterns", ("JourneyPattern",)),
    "scheduled_stop_points": ("ServiceFrame", "scheduledStopPoints", ("ScheduledStopPoint",)),
    "stop_assignments": ("ServiceFrame", "stopAssignments", ("PassengerStopAssignment", "FlexibleStopAssignment")),
    "day_types": ("ServiceCalendarFrame", "dayTypes", ("DayType",)),
    "operating_periods": ("ServiceCalendarFrame", "operatingPeriods", ("OperatingPeriod",)),
    "operating_days": ("ServiceCalendarFrame", "operatingDays", ("OperatingDay",)),
    "day_type_assignments": ("ServiceCalendarFrame", "dayTypeAssignments", ("DayTypeAssignment",)),
    "flexible_stop_places": ("SiteFrame", "flexibleStopPlaces", ("FlexibleStopPlace",)),
}


def add_context(spark, raw, selection, inventory_path):
    """Join each parsed row to its source file's provider, hash and publication metadata.

    The inventory is also written to ``inventory_path`` as JSON lines, so the
    staged output records exactly which files were read.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    names = ("source_file", "source_member", "provider", "source_xml_sha256",
             "source_profile", "publication_timestamp")
    schema = StructType([StructField(name, StringType(), False) for name in (*names, "read_file")])
    with inventory_path.open("x", encoding="utf-8") as handle:
        for item in selection["files"]:
            value = {name: item[name] for name in names}
            value["read_file"] = item.get("read_file", item["source_file"])
            handle.write(json.dumps(value, ensure_ascii=False) + "\n")
    inventory = spark.read.schema(schema).json(str(inventory_path))
    # Hadoop and pathlib escape reserved URI characters differently. Decode the
    # local file URI for matching, preserving literal '+' rather than treating
    # it as a URL-encoded space. Keep Spark's original URI for diagnostics.
    local_path = F.regexp_replace(F.input_file_name(), "^file:/+", "/")
    local_path = F.url_decode(F.regexp_replace(local_path, r"\+", "%2B"))
    framed = (raw.withColumn("source_uri", F.input_file_name()).withColumn("read_file", local_path)
              .join(inventory, "read_file", "left").drop("read_file"))
    missing = framed.where(F.col("provider").isNull()).select("source_uri").limit(3).collect()
    if missing:
        raise RuntimeError(f"Spark source paths did not match the source inventory: {[r.source_uri for r in missing]}")
    return framed.withColumn("snapshot_date", F.lit(selection["snapshot_date"]).cast("date"))


def entity_tables(frames):
    """Expand CompositeFrames into one table per entity type, plus ordered child tables.

    Each entity row keeps the CompositeFrame and parent frame IDs/versions and
    zero-based ``parent_frame_index``/``entity_index`` source positions; the
    entity itself stays nested under ``record``. An ID/version pair alone is
    not treated as a unique occurrence.
    """
    from pyspark.sql import functions as F

    common = CONTEXT + ["composite_frame_id", "composite_frame_version"]
    base = frames.select(*CONTEXT, F.col("_id").alias("composite_frame_id"),
                         F.col("_version").alias("composite_frame_version"), "frames")

    def expand(frame_type, container, kinds):
        parent = base.select(*common, F.posexplode("frames." + frame_type).alias("parent_frame_index", "parent"))
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

    tables = {"frames": frames.drop("source_uri")}
    tables.update({name: expand(*spec) for name, spec in ENTITY_TABLES.items()})
    # Ordered children carry their parent's ID/version and their own index.
    journeys = tables["service_journeys"]
    keys = common + ["parent_frame_index", "parent_frame_id", "parent_frame_version", "entity_index", "entity_type"]
    tables["passing_times"] = journeys.select(
        *keys, F.col("record._id").alias("service_journey_id"),
        F.col("record._version").alias("service_journey_version"),
        F.posexplode("record.passingTimes.TimetabledPassingTime").alias("passing_time_index", "record"))
    patterns = tables["journey_patterns"]
    tables["journey_pattern_points"] = patterns.select(
        *[c for c in patterns.columns if c != "record"],
        F.col("record._id").alias("journey_pattern_id"), F.col("record._version").alias("journey_pattern_version"),
        F.posexplode("record.pointsInSequence.StopPointInJourneyPattern").alias("point_index", "record"))
    return tables


@with_snapshot_xml
def convert_snapshot(spark, manifest_path, output, snapshot_date=None, selection=None):
    """Write every staging table for one snapshot to ``output`` and return its manifest.

    ``output`` must be a new directory outside Bronze. A failure leaves the
    partial output and a ``failed`` manifest in place for inspection.
    """
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    selection = selection or load_snapshot_sources(manifest_path, snapshot_date)
    output = Path(output).resolve()
    source_root = Path(selection["source_root"]).resolve()
    if output.is_relative_to(source_root) or source_root.is_relative_to(output):
        raise ValueError("Staging output must be separate from Bronze")
    output.mkdir(parents=True, exist_ok=False)
    metadata = output / "metadata"
    metadata.mkdir()
    # Spark hides underscore-prefixed input files even when explicitly listed.
    # Read-only symbolic aliases expose these files without copying or renaming
    # Bronze. Source provenance continues to identify the original raw paths.
    inputs = output / "inputs"
    inputs.mkdir()
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
    write_json(path, report)
    frames = stops = None

    def reader(tag, schema, files):
        # FAILFAST: a malformed XML record stops the job instead of becoming a null row.
        return (spark.read.format("xml").option("rowTag", tag).option("mode", "FAILFAST")
                .option("ignoreNamespace", "false").option("ignoreSurroundingSpaces", "false")
                .option("wildcardColName", "_unmapped_xml").schema(schema).load(files))

    try:
        raw = reader("CompositeFrame", complete_frame_schema(),
                     [s.get("read_file", s["source_file"]) for s in selection["files"]])
        frames = add_context(spark, raw, selection, metadata / "frame_inventory.jsonl")
        frames = (frames.withColumn("valid_from", F.expr("try_element_at(validityConditions.AvailabilityCondition, 1).FromDate"))
                  .withColumn("valid_to", F.expr("try_element_at(validityConditions.AvailabilityCondition, 1).ToDate"))
                  .persist(StorageLevel.DISK_ONLY))
        tables = entity_tables(frames)
        raw_stops = reader("StopPlace", stop_place_schema(),
                           [s.get("read_file", s["source_file"]) for s in selection["stop_files"]])
        stops = add_context(spark, raw_stops, {**selection, "files": selection["stop_files"]},
                            metadata / "stop_inventory.jsonl").drop("source_uri").persist(StorageLevel.DISK_ONLY)
        tables["stop_places"] = stops
        stop_context = [c for c in CONTEXT if c not in ("valid_from", "valid_to")]
        tables["quays"] = stops.select(*stop_context, F.col("_id").alias("stop_place_id"),
                                       F.col("_version").alias("stop_place_version"),
                                       F.posexplode("quays.Quay").alias("quay_index", "record"))
        for name, table in tables.items():
            expected = selection["expected_rows"][name]
            actual = table.count()
            if actual != expected:
                raise RuntimeError(f"{name}: {actual} rows; Bronze records {expected}")
            identity = "_id" if name in ("frames", "stop_places") else "record._id"
            if table.where(F.col(identity).isNull()).limit(1).count():
                raise RuntimeError(f"{name}: missing entity IDs")
            target = output / name
            table.write.mode("errorifexists").option("compression", "snappy").parquet(str(target))
            saved = spark.read.parquet(str(target))
            if saved.count() != actual:
                raise RuntimeError(f"{name}: read-back count mismatch")
            report["tables"][name] = {"path": name, "rows": actual,
                                      "schema": json.loads(saved.schema.json()),
                                      "parquet_bytes": sum(f.stat().st_size for f in target.rglob("*.parquet"))}
            write_json(path, report)
            print(f"staging/{name}: {actual:,} rows verified", flush=True)
        # The XML must still match its Bronze hashes after Spark has read it.
        verify_sources(selection["files"] + selection["stop_files"])
        report.update(status="complete", completed_at_utc=utc_now(), source_checksums_unchanged=True)
        write_json(path, report)
        return report
    except Exception as error:
        report.update(status="failed", completed_at_utc=utc_now(), error=f"{type(error).__name__}: {error}")
        write_json(path, report)
        raise
    finally:
        if frames is not None:
            frames.unpersist()
        if stops is not None:
            stops.unpersist()
