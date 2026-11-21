"""CLI entrypoint for building the DAT535 Silver layer from NeTEx Staging and SIRI ET Bronze."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from . import (
    DEFAULT_NETEX_STAGING_RUNS_DIR,
    DEFAULT_PROVIDER,
    DEFAULT_SILVER_RUNS_DIR,
    DEFAULT_SIRI_ET_BRONZE_DIR,
    DEFAULT_TIMEZONE,
    SILVER_VERSION,
)
from .calendar_and_calls import (
    build_journey_service_dates,
    build_netex_stop_calls,
    build_service_calendar,
)
from .input_selection import parse_iso_date_str, select_inputs, write_selection_report
from .netex_definitions import build_netex_definitions
from .quality_and_publish import write_and_publish_silver
from .reconciliation import build_reconciled_stop_calls
from .siri_et_calls import build_siri_et_stop_calls


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build DAT535 Silver reconciliation tables from staged NeTEx and SIRI ET Bronze."
    )
    parser.add_argument(
        "--provider",
        default=DEFAULT_PROVIDER,
        help=f"Provider codespace (default: {DEFAULT_PROVIDER})",
    )
    parser.add_argument(
        "--dates",
        default=None,
        help="Optional comma-separated operating dates (YYYY-MM-DD). Defaults to all selected snapshots.",
    )
    parser.add_argument(
        "--dates-file",
        default=None,
        help="Optional file containing operating dates (one per line or comma-separated).",
    )
    parser.add_argument(
        "--staging-runs-dir",
        default=str(DEFAULT_NETEX_STAGING_RUNS_DIR),
        help=f"Directory holding quarterly NeTEx staging runs (default: {DEFAULT_NETEX_STAGING_RUNS_DIR})",
    )
    parser.add_argument(
        "--siri-et-dir",
        default=str(DEFAULT_SIRI_ET_BRONZE_DIR),
        help=f"Directory holding SIRI ET Bronze Parquet files (default: {DEFAULT_SIRI_ET_BRONZE_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Target Silver run directory (must not already be non-empty).",
    )
    parser.add_argument(
        "--timezone",
        default=DEFAULT_TIMEZONE,
        help=f"Source civil timezone for NeTEx clock times (default: {DEFAULT_TIMEZONE})",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=4,
        help="Spark SQL shuffle partitions for constrained VM execution (default: 4).",
    )
    return parser.parse_args(argv)


def _collect_requested_dates(args: argparse.Namespace) -> list[str] | None:
    tokens: list[str] = []
    if args.dates:
        tokens.extend(part.strip() for part in args.dates.split(",") if part.strip())
    if args.dates_file:
        p = Path(args.dates_file)
        if not p.is_file():
            raise FileNotFoundError(f"Dates file not found: {p}")
        for line in p.read_text(encoding="utf-8").splitlines():
            for part in line.split(","):
                if part.strip():
                    tokens.append(part.strip())
    if not tokens:
        return None
    return sorted({parse_iso_date_str(t) for t in tokens})


def _create_spark_session(shuffle_partitions: int) -> SparkSession:
    spark = (
        SparkSession.builder.appName("dat535-silver-reconciliation")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main(argv: list[str] | None = None) -> int:
    started_at_utc = datetime.utcnow().isoformat() + "Z"
    args = parse_args(argv)

    requested_dates = _collect_requested_dates(args)
    staging_runs_dir = Path(args.staging_runs_dir)
    siri_et_dir = Path(args.siri_et_dir)
    output_dir = Path(args.output_dir)

    selection = select_inputs(
        dates=requested_dates,
        provider=args.provider,
        staging_runs_dir=staging_runs_dir,
        siri_et_dir=siri_et_dir,
    )

    if selection.hard_failures:
        raise RuntimeError(f"Input selection hard failures: {selection.hard_failures}")

    spark = _create_spark_session(shuffle_partitions=args.shuffle_partitions)
    spark_master = os.environ.get("SPARK_MASTER_URL") or spark.sparkContext.master

    try:
        pub_rows = [
            (
                r.staging_run,
                r.snapshot_date,
                r.operating_date,
                r.provider,
                r.publication_id,
                r.siri_source_file,
                r.siri_source_sha256,
                r.siri_file_available,
                r.selection_status,
            )
            for r in selection.records
        ]
        pub_schema = T.StructType(
            [
                T.StructField("staging_run", T.StringType(), False),
                T.StructField("snapshot_date", T.StringType(), False),
                T.StructField("operating_date", T.StringType(), False),
                T.StructField("provider", T.StringType(), False),
                T.StructField("publication_id", T.StringType(), False),
                T.StructField("siri_source_file", T.StringType(), True),
                T.StructField("siri_source_sha256", T.StringType(), True),
                T.StructField("siri_file_available", T.BooleanType(), False),
                T.StructField("selection_status", T.StringType(), False),
            ]
        )
        publication_lookup_df = spark.createDataFrame(pub_rows, schema=pub_schema).cache()
        publication_lookup_df.count()

        selected_run_dirs = sorted(
            {r.staging_run_dir for r in selection.records if r.staging_run_dir}
        )

        # 1. Resolved publication-scoped NeTEx definitions
        defs = build_netex_definitions(
            spark=spark,
            run_dirs=selected_run_dirs,
            publication_lookup=publication_lookup_df,
            provider=args.provider,
        )
        lines_df = defs["lines"].cache()
        routes_df = defs["routes"].cache()
        jp_df = defs["journey_patterns"].cache()
        jps_df = defs["journey_pattern_stops"].cache()
        journeys_df = defs["journeys"].cache()

        # 2. Calendar & dated planned stop calls
        service_calendar_df = build_service_calendar(
            spark=spark,
            run_dirs=selected_run_dirs,
            publication_lookup=publication_lookup_df,
            provider=args.provider,
        ).cache()

        journey_service_dates_df = build_journey_service_dates(
            journeys_df=journeys_df,
            service_calendar_df=service_calendar_df,
            publication_lookup=publication_lookup_df,
        ).cache()

        netex_stop_calls_df = build_netex_stop_calls(
            spark=spark,
            run_dirs=selected_run_dirs,
            publication_lookup=publication_lookup_df,
            journey_service_dates_df=journey_service_dates_df,
            journey_pattern_stops_df=jps_df,
            provider=args.provider,
            timezone_name=args.timezone,
        ).cache()

        # 3. Canonical SIRI ET stop calls
        siri_et_stop_calls_df = build_siri_et_stop_calls(
            spark=spark,
            siri_lookup=publication_lookup_df,
            provider=args.provider,
            timezone_name=args.timezone,
        ).cache()

        # 4. Reconciled stop calls + metric eligibility
        reconciled_stop_calls_df = build_reconciled_stop_calls(
            netex_stop_calls_df=netex_stop_calls_df,
            siri_et_stop_calls_df=siri_et_stop_calls_df,
            journey_service_dates_df=journey_service_dates_df,
            publication_lookup_df=publication_lookup_df,
        ).cache()

        all_tables = {
            "lines": lines_df,
            "routes": routes_df,
            "journey_patterns": jp_df,
            "journey_pattern_stops": jps_df,
            "journeys": journeys_df,
            "service_calendar": service_calendar_df,
            "journey_service_dates": journey_service_dates_df,
            "netex_stop_calls": netex_stop_calls_df,
            "siri_et_stop_calls": siri_et_stop_calls_df,
            "reconciled_stop_calls": reconciled_stop_calls_df,
        }

        manifest = write_and_publish_silver(
            spark=spark,
            output_dir=output_dir,
            tables=all_tables,
            selection=selection,
            publication_lookup_df=publication_lookup_df,
            started_at_utc=started_at_utc,
            spark_master=spark_master,
            config_meta={
                "silver_version": SILVER_VERSION,
                "provider": args.provider,
                "timezone": args.timezone,
                "spark_session_timezone": "UTC",
                "shuffle_partitions": args.shuffle_partitions,
                "default_silver_runs_dir": str(DEFAULT_SILVER_RUNS_DIR),
            },
        )

        # Also save a standalone input selection audit artifact in quality/
        write_selection_report(selection, output_dir / "quality" / "input_selection.json")

        for df in all_tables.values():
            df.unpersist()
        publication_lookup_df.unpersist()

        print(json.dumps(manifest, indent=2, default=str))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())