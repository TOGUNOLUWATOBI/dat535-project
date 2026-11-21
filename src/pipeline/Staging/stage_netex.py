"""Stage Bronze NeTEx ZIP snapshots into structured Parquet entity tables.

Reads verified Bronze NeTEx ZIPs from ``manifest.json``, materializes tagged
archive/member extraction once on executors, parses ``PublicationDelivery``
documents (``dataObjects.CompositeFrame`` and ``dataObjects.StopPlace``) using
explicit schemas from ``schemas.py``, preserves source provenance and element
indices, enforces member-parse accounting and pre/post-write count checks,
verifies captured ZIP hashes, preserves failed runs under a marked directory
without publishing to the target path, and publishes via a single rename into a
fresh destination directory.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import time

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from schemas import complete_frame_schema, stop_place_schema
from zip_xml_reader import extract_zip_members_partition, sha256_file


CONTEXT_COLS = [
    "snapshot_date",
    "provider",
    "source_zip",
    "source_zip_sha256",
    "source_generation",
    "publication_date",
    "source_member",
    "member_index",
    "source_xml_sha256",
]

ENTITY_SPECS = {
    "lines": ("ServiceFrame", "lines", ("Line", "FlexibleLine")),
    "routes": ("ServiceFrame", "routes", ("Route",)),
    "journey_patterns": ("ServiceFrame", "journeyPatterns", ("JourneyPattern",)),
    "scheduled_stop_points": ("ServiceFrame", "scheduledStopPoints", ("ScheduledStopPoint",)),
    "stop_assignments": (
        "ServiceFrame",
        "stopAssignments",
        ("PassengerStopAssignment", "FlexibleStopAssignment"),
    ),
    "service_journeys": (
        "TimetableFrame",
        "vehicleJourneys",
        ("ServiceJourney", "FlexibleServiceJourney"),
    ),
    "day_types": ("ServiceCalendarFrame", "dayTypes", ("DayType",)),
    "operating_periods": ("ServiceCalendarFrame", "operatingPeriods", ("OperatingPeriod",)),
    "operating_days": ("ServiceCalendarFrame", "operatingDays", ("OperatingDay",)),
    "day_type_assignments": ("ServiceCalendarFrame", "dayTypeAssignments", ("DayTypeAssignment",)),
    "flexible_stop_places": ("SiteFrame", "flexibleStopPlaces", ("FlexibleStopPlace",)),
}

TAGGED_SCHEMA = StructType([
    StructField("kind", StringType(), False),
    StructField("snapshot_date", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("source_zip", StringType(), True),
    StructField("source_zip_sha256", StringType(), True),
    StructField("source_generation", StringType(), True),
    StructField("publication_date", StringType(), True),
    StructField("source_member", StringType(), True),
    StructField("member_index", IntegerType(), True),
    StructField("member_bytes", LongType(), True),
    StructField("source_xml_sha256", StringType(), True),
    StructField("is_stops_file", BooleanType(), True),
    StructField("xml_content", StringType(), True),
    StructField("stage", StringType(), True),
    StructField("error", StringType(), True),
])

FAILURE_SCHEMA = StructType([
    StructField("snapshot_date", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("source_zip", StringType(), True),
    StructField("source_zip_sha256", StringType(), True),
    StructField("source_generation", StringType(), True),
    StructField("publication_date", StringType(), True),
    StructField("source_member", StringType(), True),
    StructField("member_index", IntegerType(), True),
    StructField("stage", StringType(), True),
    StructField("error", StringType(), True),
])


def publication_delivery_wrapper_schema(
    payload_field: str,
    payload_element_schema: StructType,
) -> StructType:
    """Wrap payload array under PublicationDelivery -> dataObjects -> <payload_field>."""
    s = StringType()
    data_objects = StructType([
        StructField(payload_field, ArrayType(payload_element_schema), True),
        StructField("_unmapped_xml", ArrayType(s), True),
    ])
    return StructType([
        StructField("_version", s, True),
        StructField("PublicationTimestamp", s, True),
        StructField("ParticipantRef", s, True),
        StructField("dataObjects", data_objects, True),
        StructField("_unmapped_xml", ArrayType(s), True),
        StructField("_corrupt_record", s, True),
    ])


def select_verified_zips(manifest_path: Path, dates: set[str] | None = None) -> list[dict[str, str]]:
    """Load verified downloaded ZIP entries from Bronze manifest and verify requested dates."""
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    records = payload.get("records", [])
    provider = str(payload.get("provider", "KOL"))
    selected: list[dict[str, str]] = []
    seen_dates: set[str] = set()

    for rec in records:
        if rec.get("status") != "downloaded":
            continue
        target_date = str(rec.get("target_date", ""))
        if dates is not None and target_date not in dates:
            continue
        zip_path = Path(str(rec["file"]))
        expected_sha = str(rec.get("sha256", "")).strip()
        if not expected_sha:
            raise ValueError(f"Missing recorded sha256 in Bronze manifest for {target_date}: {zip_path}")
        if not zip_path.is_file():
            raise FileNotFoundError(f"Bronze ZIP missing on disk: {zip_path}")
        actual_sha = sha256_file(zip_path)
        if actual_sha != expected_sha:
            raise RuntimeError(
                f"Bronze ZIP SHA-256 mismatch for {zip_path}: expected {expected_sha}, got {actual_sha}"
            )
        seen_dates.add(target_date)
        selected.append({
            "target_date": target_date,
            "provider": provider,
            "file": str(zip_path.resolve()),
            "sha256": expected_sha,
            "generation": str(rec.get("generation", "")),
            "publication_date": str(rec.get("publication_date", "")),
        })

    if dates is not None:
        missing_dates = sorted(dates - seen_dates)
        if missing_dates:
            raise ValueError(
                f"Requested --dates missing verified downloaded manifest entries: {', '.join(missing_dates)}"
            )

    if not selected:
        raise ValueError("No verified downloaded Bronze NeTEx ZIPs matched selection.")
    return selected


def verify_captured_zips(captured_zips: list[dict[str, str]]) -> None:
    """Re-verify the exact paths and expected hashes captured at job start."""
    for item in captured_zips:
        zip_path = Path(item["file"])
        expected_sha = item["sha256"]
        if not expected_sha:
            raise ValueError(f"Captured record missing sha256: {zip_path}")
        if not zip_path.is_file():
            raise FileNotFoundError(f"Captured Bronze ZIP missing before publication: {zip_path}")
        actual_sha = sha256_file(zip_path)
        if actual_sha != expected_sha:
            raise RuntimeError(
                f"Captured Bronze ZIP changed during run ({zip_path}): expected {expected_sha}, got {actual_sha}"
            )


def parse_xml_column(
    df,
    row_tag: str,
    wrapper_schema: StructType,
    payload_field: str,
    parse_stage: str,
):
    """Parse xml_content in-place while retaining provenance and rejecting missing payload."""
    xml_opts = {
        "rowTag": row_tag,
        "mode": "PERMISSIVE",
        "ignoreNamespace": "false",
        "ignoreSurroundingSpaces": "false",
        "wildcardColName": "_unmapped_xml",
        "columnNameOfCorruptRecord": "_corrupt_record",
    }
    parsed_df = (
        df.withColumn("parsed_doc", F.from_xml(F.col("xml_content"), wrapper_schema, xml_opts))
        .drop("xml_content")
    )
    payload_col = F.col(f"parsed_doc.dataObjects.{payload_field}")
    invalid_expr = (
        F.col("parsed_doc").isNull()
        | F.col("parsed_doc._corrupt_record").isNotNull()
        | F.col("parsed_doc.dataObjects").isNull()
        | payload_col.isNull()
        | (F.size(payload_col) == 0)
    )

    bad_df = parsed_df.filter(invalid_expr).select(
        "snapshot_date",
        "provider",
        "source_zip",
        "source_zip_sha256",
        "source_generation",
        "publication_date",
        "source_member",
        "member_index",
        F.lit(parse_stage).alias("stage"),
        F.coalesce(
            F.col("parsed_doc._corrupt_record"),
            F.lit(f"Missing or empty PublicationDelivery.dataObjects.{payload_field}"),
        ).alias("error"),
    )
    ok_df = parsed_df.filter(~invalid_expr)
    return ok_df, bad_df


def build_frame_and_entity_tables(persisted_ok_frames_df):
    """Expand PublicationDelivery.dataObjects.CompositeFrame into staged entity tables."""
    exploded_frames = persisted_ok_frames_df.select(
        *CONTEXT_COLS,
        F.col("parsed_doc.PublicationTimestamp").alias("xml_publication_timestamp"),
        F.posexplode("parsed_doc.dataObjects.CompositeFrame").alias("composite_frame_index", "cf"),
    )

    frame_cols = CONTEXT_COLS + [
        "xml_publication_timestamp",
        "composite_frame_index",
        "composite_frame_id",
        "composite_frame_version",
    ]

    base = (
        exploded_frames.select(
            *CONTEXT_COLS,
            "xml_publication_timestamp",
            "composite_frame_index",
            F.col("cf._id").alias("composite_frame_id"),
            F.col("cf._version").alias("composite_frame_version"),
            F.col("cf._created").alias("xml_frame_created"),
            F.col("cf.validityConditions").alias("validity_conditions"),
            F.col("cf.frames").alias("frames"),
        )
        .persist(StorageLevel.DISK_ONLY)
    )

    tables = {"composite_frames": base.drop("frames")}

    def expand(frame_type: str, container: str, kinds: tuple[str, ...]):
        parent = (
            base.select(
                *frame_cols,
                F.posexplode(f"frames.{frame_type}").alias("parent_frame_index", "parent"),
            )
            .select(
                *frame_cols,
                "parent_frame_index",
                F.col("parent._id").alias("parent_frame_id"),
                F.col("parent._version").alias("parent_frame_version"),
                F.col(f"parent.{container}").alias("items"),
            )
        )
        keys = frame_cols + ["parent_frame_index", "parent_frame_id", "parent_frame_version"]
        out = None
        for kind in kinds:
            rows = (
                parent.select(*keys, F.posexplode(f"items.{kind}").alias("entity_index", "record"))
                .withColumn("entity_type", F.lit(kind))
            )
            out = rows if out is None else out.unionByName(rows)
        return out

    for table_name, spec in ENTITY_SPECS.items():
        tables[table_name] = expand(*spec)

    journeys = tables["service_journeys"]
    j_keys = frame_cols + [
        "parent_frame_index",
        "parent_frame_id",
        "parent_frame_version",
        "entity_index",
        "entity_type",
    ]
    tables["passing_times"] = journeys.select(
        *j_keys,
        F.col("record._id").alias("service_journey_id"),
        F.col("record._version").alias("service_journey_version"),
        F.posexplode("record.passingTimes.TimetabledPassingTime").alias("passing_time_index", "record"),
    )

    patterns = tables["journey_patterns"]
    p_keys = [c for c in patterns.columns if c != "record"]
    tables["journey_pattern_points"] = patterns.select(
        *p_keys,
        F.col("record._id").alias("journey_pattern_id"),
        F.col("record._version").alias("journey_pattern_version"),
        F.posexplode("record.pointsInSequence.StopPointInJourneyPattern").alias("point_index", "record"),
    )
    return base, tables


def _safe_unpersist(df) -> None:
    if df is not None:
        try:
            df.unpersist()
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage Bronze NeTEx ZIPs into Parquet tables via Spark.")
    parser.add_argument(
        "--dates",
        help="Optional comma-separated YYYY-MM-DD dates; defaults to all downloaded in manifest.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="New final staging output directory (must not already exist).",
    )
    parser.add_argument(
        "--zip-slices",
        type=int,
        default=4,
        help="RDD partitions for ZIP member extraction (default: 4).",
    )
    parser.add_argument(
        "--repartition-members",
        type=int,
        default=0,
        help="Optional repartition count for extracted XML members (default: 0).",
    )
    args = parser.parse_args()

    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.monotonic()

    data_root = Path(os.environ.get("DAT535_DATA_ROOT", "/srv/dat535/data"))
    manifest_path = data_root / "bronze" / "netex" / "kol" / "manifest.json"
    final_out = Path(args.output_dir).resolve()
    if final_out.exists():
        raise FileExistsError(f"Destination already exists; refusing overwrite/non-atomic replacement: {final_out}")

    final_out.parent.mkdir(parents=True, exist_ok=True)
    staged_tmp = final_out.parent / f".{final_out.name}.tmp.{os.getpid()}"
    if staged_tmp.exists():
        shutil.rmtree(staged_tmp)
    staged_tmp.mkdir(parents=True, exist_ok=True)

    date_filter = {d.strip() for d in args.dates.split(",") if d.strip()} if args.dates else None
    zips = select_verified_zips(manifest_path, date_filter)

    spark = (
        SparkSession.builder
        .appName("dat535-netex-staging")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    tagged_df = None
    ok_frames_df = None
    base_frames_df = None
    ok_stops_df = None

    try:
        master_url = spark.sparkContext.master

        def to_tagged_row(pair):
            kind, payload = pair
            return {
                "kind": kind,
                "snapshot_date": payload.get("snapshot_date"),
                "provider": payload.get("provider"),
                "source_zip": payload.get("source_zip"),
                "source_zip_sha256": payload.get("source_zip_sha256"),
                "source_generation": payload.get("source_generation"),
                "publication_date": payload.get("publication_date"),
                "source_member": payload.get("source_member"),
                "member_index": payload.get("member_index"),
                "member_bytes": payload.get("member_bytes"),
                "source_xml_sha256": payload.get("source_xml_sha256"),
                "is_stops_file": payload.get("is_stops_file"),
                "xml_content": payload.get("xml_content"),
                "stage": payload.get("stage"),
                "error": payload.get("error"),
            }

        # Persist tagged extraction once on disk so ZIPs are read and extracted once.
        tagged_df = (
            spark.createDataFrame(
                spark.sparkContext
                .parallelize(zips, numSlices=min(args.zip_slices, max(1, len(zips))))
                .mapPartitions(extract_zip_members_partition)
                .map(to_tagged_row),
                schema=TAGGED_SCHEMA,
            )
            .persist(StorageLevel.DISK_ONLY)
        )

        members_df = tagged_df.filter(F.col("kind") == "member").select(
            "snapshot_date",
            "provider",
            "source_zip",
            "source_zip_sha256",
            "source_generation",
            "publication_date",
            "source_member",
            "member_index",
            "member_bytes",
            "source_xml_sha256",
            "is_stops_file",
            "xml_content",
        )
        if args.repartition_members > 0:
            members_df = members_df.repartition(args.repartition_members)

        archive_failures_df = tagged_df.filter(F.col("kind") == "archive_failure").select(
            *[f.name for f in FAILURE_SCHEMA.fields]
        )
        member_read_failures_df = tagged_df.filter(F.col("kind") == "member_failure").select(
            *[f.name for f in FAILURE_SCHEMA.fields]
        )

        provider_members = members_df.filter(~F.col("is_stops_file"))
        stops_members = members_df.filter(F.col("is_stops_file"))

        frame_doc_schema = publication_delivery_wrapper_schema("CompositeFrame", complete_frame_schema())
        ok_frames_raw, bad_frames_df = parse_xml_column(
            provider_members, "PublicationDelivery", frame_doc_schema, "CompositeFrame", "xml_parse_frame"
        )
        ok_frames_df = ok_frames_raw.persist(StorageLevel.DISK_ONLY)
        base_frames_df, staged_tables = build_frame_and_entity_tables(ok_frames_df)

        stops_count = stops_members.count()
        table_status: dict[str, str] = {name: "staged" for name in staged_tables}

        parse_failures_union = bad_frames_df
        ok_stops_count = 0
        if stops_count > 0:
            stops_doc_schema = publication_delivery_wrapper_schema("StopPlace", stop_place_schema())
            ok_stops_raw, bad_stops_df = parse_xml_column(
                stops_members, "PublicationDelivery", stops_doc_schema, "StopPlace", "xml_parse_stops"
            )
            ok_stops_df = ok_stops_raw.persist(StorageLevel.DISK_ONLY)
            parse_failures_union = parse_failures_union.unionByName(bad_stops_df)
            ok_stops_count = ok_stops_df.count()
            staged_tables["stop_places"] = (
                ok_stops_df.select(
                    *CONTEXT_COLS,
                    F.posexplode("parsed_doc.dataObjects.StopPlace").alias("stop_place_index", "sp"),
                )
                .select(*CONTEXT_COLS, "stop_place_index", F.col("sp.*"))
            )
            staged_tables["quays"] = staged_tables["stop_places"].select(
                *CONTEXT_COLS,
                "stop_place_index",
                F.col("_id").alias("stop_place_id"),
                F.col("_version").alias("stop_place_version"),
                F.posexplode("quays.Quay").alias("quay_index", "record"),
            )
            table_status["stop_places"] = "staged"
            table_status["quays"] = "staged"
        else:
            table_status["stop_places"] = "absent_input"
            table_status["quays"] = "absent_input"

        all_failures_df = (
            archive_failures_df
            .unionByName(member_read_failures_df)
            .unionByName(parse_failures_union)
        )

        extracted_member_count = members_df.count()
        ok_frame_members_count = ok_frames_df.count()
        parse_failed_count = parse_failures_union.count()
        parsed_ok_members_count = ok_frame_members_count + ok_stops_count

        if extracted_member_count != parsed_ok_members_count + parse_failed_count:
            raise AssertionError(
                f"Member parse reconciliation failed: extracted={extracted_member_count}, "
                f"parsed_ok={parsed_ok_members_count}, parse_failed={parse_failed_count}"
            )

        archive_failure_count = archive_failures_df.count()
        member_read_failure_count = member_read_failures_df.count()
        total_failure_count = all_failures_df.count()

        table_counts: dict[str, int] = {}
        for name, df in staged_tables.items():
            pre_write_count = df.count()
            table_path = staged_tmp / name
            df.write.mode("errorifexists").parquet(str(table_path))
            post_write_count = spark.read.parquet(str(table_path)).count()
            if pre_write_count != post_write_count:
                raise AssertionError(
                    f"Row count mismatch for {name}: pre_write={pre_write_count}, post_write={post_write_count}"
                )
            table_counts[name] = post_write_count

        _safe_unpersist(base_frames_df)
        base_frames_df = None
        _safe_unpersist(ok_frames_df)
        ok_frames_df = None
        _safe_unpersist(ok_stops_df)
        ok_stops_df = None

        if table_status.get("stop_places") == "absent_input":
            table_counts["stop_places"] = 0
            table_counts["quays"] = 0

        if total_failure_count > 0:
            all_failures_df.write.mode("overwrite").parquet(str(staged_tmp / "_failures"))

        _safe_unpersist(tagged_df)
        tagged_df = None

        verify_captured_zips(zips)

        has_any_failure = total_failure_count > 0
        failed_diagnostics_dir = final_out.parent / f".{final_out.name}.failed.{os.getpid()}"
        summary: dict[str, object] = {
            "status": "incomplete" if has_any_failure else "complete",
            "spark_master": master_url,
            "started_at_utc": started_at,
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "elapsed_sec": round(time.monotonic() - t0, 2),
            "input_zips": len(zips),
            "extracted_members": extracted_member_count,
            "parsed_ok_members": parsed_ok_members_count,
            "parse_failed_members": parse_failed_count,
            "archive_failures": archive_failure_count,
            "member_read_failures": member_read_failure_count,
            "total_failures": total_failure_count,
            "table_status": table_status,
            "output_table_rows": table_counts,
        }
        if has_any_failure:
            summary["failed_diagnostics_dir"] = str(failed_diagnostics_dir)

        (staged_tmp / "staging_manifest.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

        if has_any_failure:
            staged_tmp.rename(failed_diagnostics_dir)
            raise RuntimeError(
                f"Staging incomplete due to {total_failure_count} failure(s) "
                f"(archive={archive_failure_count}, member_read={member_read_failure_count}, parse={parse_failed_count}); "
                f"diagnostics preserved at {failed_diagnostics_dir}"
            )

        staged_tmp.rename(final_out)
        print(json.dumps(summary, indent=2))
    finally:
        for cached in (base_frames_df, ok_frames_df, ok_stops_df, tagged_df):
            _safe_unpersist(cached)
        spark.stop()


if __name__ == "__main__":
    main()