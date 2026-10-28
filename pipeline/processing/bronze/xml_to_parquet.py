"""Read validated provider NeTEx with Spark 4 and write staging Parquet.

This first projection retains CompositeFrame context and expands lines,
service journeys and their ordered passing times. It is not Silver, and does
not yet model stops, routes, journey patterns or service-calendar definitions.
"""

import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys

from pipeline.ingestion.generate_provider_dataset import save_json
from .netex_schema import composite_frame_schema
from .staging_sources import load_sources, verify_sources


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def add_context(spark, raw, selection, inventory_path):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    names = ("source_file", "source_member", "provider", "source_xml_sha256",
             "source_profile", "publication_timestamp")
    schema = StructType([StructField(name, StringType(), False) for name in names])
    with inventory_path.open("x", encoding="utf-8") as handle:
        for item in selection["files"]:
            handle.write(json.dumps({name: item[name] for name in names}, ensure_ascii=False) + "\n")
    inventory = spark.read.schema(schema).json(str(inventory_path))
    # Hadoop and pathlib escape reserved URI characters differently. Decode the
    # local file URI for matching, preserving literal '+' rather than treating
    # it as a URL-encoded space. Keep Spark's original URI for diagnostics.
    local_path = F.regexp_replace(F.input_file_name(), "^file:/+", "/")
    local_path = F.url_decode(F.regexp_replace(local_path, r"\+", "%2B"))
    framed = (raw.withColumn("source_uri", F.input_file_name()).withColumn("source_file", local_path)
              .join(inventory, "source_file", "left"))
    missing = framed.where(F.col("provider").isNull()).select("source_uri").limit(3).collect()
    if missing:
        raise RuntimeError(f"Spark source paths did not match the source inventory: {[r.source_uri for r in missing]}")
    return framed.withColumn("snapshot_date", F.lit(selection["snapshot_date"]).cast("date"))


def projected_tables(frames):
    from pyspark.sql import functions as F

    context = ["snapshot_date", "provider", "source_file", "source_member",
               "source_xml_sha256", "source_profile", "publication_timestamp"]
    base = frames.select(*context, F.col("_id").alias("composite_frame_id"),
                         F.col("_version").alias("composite_frame_version"), "frames")
    common = context + ["composite_frame_id", "composite_frame_version"]

    def expand(frame_type, container, entities):
        parents = base.select(*common, F.posexplode(F.col("frames." + frame_type))
                              .alias("parent_frame_index", "parent"))
        scoped = parents.select(*common, "parent_frame_index",
                                F.col("parent._id").alias("parent_frame_id"),
                                F.col("parent._version").alias("parent_frame_version"),
                                F.col("parent." + container).alias("items"))
        keys = common + ["parent_frame_index", "parent_frame_id", "parent_frame_version"]
        result = None
        for entity in entities:
            rows = scoped.select(*keys, F.posexplode(F.col("items." + entity)).alias("entity_index", "record"))
            rows = rows.withColumn("entity_type", F.lit(entity))
            result = rows if result is None else result.unionByName(rows)
        return result

    lines = expand("ServiceFrame", "lines", ("Line", "FlexibleLine"))
    journeys = expand("TimetableFrame", "vehicleJourneys", ("ServiceJourney", "FlexibleServiceJourney"))
    keys = common + ["parent_frame_index", "parent_frame_id", "parent_frame_version", "entity_index", "entity_type"]
    passing = journeys.select(*keys, F.col("record._id").alias("service_journey_id"),
                              F.col("record._version").alias("service_journey_version"),
                              F.posexplode("record.passingTimes.TimetabledPassingTime")
                              .alias("passing_time_index", "record"))
    return {"frames": frames.drop("source_uri"), "lines": lines,
            "service_journeys": journeys, "passing_times": passing}


def convert(spark, manifest_path, output, snapshot_date=None, sample_per_provider=None):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    if int(spark.version.split(".")[0]) < 4:
        raise RuntimeError("This job requires Spark 4 or newer with the built-in XML reader")
    selection = load_sources(manifest_path, snapshot_date, sample_per_provider)
    output = Path(output).resolve()
    source_root = Path(selection["source_root"]).resolve()
    if output.is_relative_to(source_root) or source_root.is_relative_to(output):
        raise ValueError("Staging output must be separate from the Bronze source tree")
    output.mkdir(parents=True, exist_ok=False)
    metadata = output / "metadata"
    metadata.mkdir()
    manifest = {
        "schema_version": 1, "status": "in_progress", "layer": "staging",
        "started_at_utc": utc_now(), "spark_version": spark.version,
        **selection, "tables": {},
        "projection": "Provider CompositeFrames, lines, service journeys and ordered passing times; shared stop/calendar definitions and Silver joins remain to implement.",
    }
    report_path = metadata / "manifest.json"
    save_json(report_path, manifest)
    frames = None
    try:
        raw = (spark.read.format("xml").option("rowTag", "CompositeFrame")
               .option("mode", "FAILFAST").option("ignoreNamespace", "false")
               .option("ignoreSurroundingSpaces", "false")
               .option("attributePrefix", "_").option("wildcardColName", "_unmapped_xml")
               .schema(composite_frame_schema()).load([s["source_file"] for s in selection["files"]]))
        frames = add_context(spark, raw, selection, metadata / "source_inventory.jsonl")
        frames = frames.persist(StorageLevel.DISK_ONLY)
        for name, table in projected_tables(frames).items():
            expected = selection["expected_rows"][name]
            actual = table.count()
            if actual != expected:
                raise RuntimeError(f"{name}: Spark read {actual} rows; Bronze validation records {expected}")
            identity = "_id" if name == "frames" else "record._id"
            if table.where(F.col(identity).isNull()).limit(1).count():
                raise RuntimeError(f"{name}: missing source entity IDs")
            target = output / name
            table.write.mode("errorifexists").option("compression", "snappy").parquet(str(target))
            readback = spark.read.parquet(str(target))
            if readback.count() != expected:
                raise RuntimeError(f"{name}: Parquet read-back row count differs from XML")
            manifest["tables"][name] = {
                "path": name, "rows": actual, "schema": json.loads(readback.schema.json()),
                "parquet_bytes": sum(p.stat().st_size for p in target.rglob("*.parquet")),
            }
            save_json(report_path, manifest)
            print(f"{name}: {actual:,} rows written and read back", flush=True)
        verify_sources(selection["files"])
        manifest.update(status="complete", completed_at_utc=utc_now(), source_checksums_unchanged=True)
        save_json(report_path, manifest)
        return manifest
    except Exception as error:
        manifest.update(status="failed", completed_at_utc=utc_now(), error=f"{type(error).__name__}: {error}")
        save_json(report_path, manifest)
        raise
    finally:
        if frames is not None:
            frames.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True, help="Complete Bronze run or historical dataset manifest")
    parser.add_argument("--date", type=date.fromisoformat, help="Required for a multi-snapshot historical manifest")
    parser.add_argument("--output", type=Path, required=True, help="New staging directory outside Bronze")
    parser.add_argument("--sample-per-provider", type=int, help="Test with N smallest provider files containing journeys; omit for all provider line files")
    parser.add_argument("--master", default="local[2]", help="Spark master; default: local[2]")
    args = parser.parse_args()
    spark = None
    try:
        from pyspark.sql import SparkSession
        spark = (SparkSession.builder.appName("DAT535-NeTEx-XML-to-Parquet")
                 .master(args.master).config("spark.sql.session.timeZone", "Europe/Oslo").getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        result = convert(spark, args.manifest, args.output, args.date, args.sample_per_provider)
        print(f"{result['status']}: {args.output / 'metadata/manifest.json'}", flush=True)
        return 0
    except Exception as error:
        print(f"XML-to-Parquet failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
