"""CLI entrypoint for building the Gold analytical layer."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Sequence

from pyspark.sql import SparkSession

from pipeline.Gold import (
    DEFAULT_GOLD_RUNS_DIR,
    DEFAULT_PERCENTILE_ACCURACY,
    DEFAULT_PROVIDER,
    DEFAULT_SHUFFLE_PARTITIONS,
    DEFAULT_SILVER_RUNS_DIR,
)
from pipeline.Gold.event_metrics import (
    build_daily_line_punctuality,
    build_daily_network_punctuality,
    build_daily_stop_punctuality,
    build_logical_event_projection,
)
from pipeline.Gold.inputs import build_silver_inventory
from pipeline.Gold.journey_metrics import (
    build_daily_line_service,
    build_journey_performance,
    build_period_line_service,
    build_period_punctuality,
)
from pipeline.Gold.quality_and_publish import build_daily_data_quality, publish_gold_outputs


def _create_spark_session(app_name: str, shuffle_partitions: int, driver_memory: str = "2g") -> SparkSession:
    return (
        SparkSession.builder.master("local[1]").appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(int(shuffle_partitions)))
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def run_gold_pipeline(
    *,
    gold_run_id: str,
    expected_dates: Sequence[str],
    provider: str = DEFAULT_PROVIDER,
    silver_runs_dir: str | Path = DEFAULT_SILVER_RUNS_DIR,
    gold_runs_dir: str | Path = DEFAULT_GOLD_RUNS_DIR,
    allowed_run_ids: Sequence[str] | None = None,
    shuffle_partitions: int = DEFAULT_SHUFFLE_PARTITIONS,
    percentile_accuracy: int = DEFAULT_PERCENTILE_ACCURACY,
    driver_memory: str = "2g",
) -> Path:
    started_at_utc = datetime.now(timezone.utc).isoformat()
    repo_root = Path(__file__).resolve().parents[3]

    inventory = build_silver_inventory(
        silver_runs_dir=silver_runs_dir,
        expected_dates=expected_dates,
        provider=provider,
        allowed_run_ids=allowed_run_ids,
    )

    import shutil

    stage_dir = Path(f"/tmp/gold_stage_{gold_run_id}")
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)

    spark = _create_spark_session(f"gold-{gold_run_id}", shuffle_partitions, driver_memory=driver_memory)
    try:
        raw_events = build_logical_event_projection(spark, inventory)
        raw_events.write.mode("overwrite").parquet(str(stage_dir / "events"))
        events_df = spark.read.parquet(str(stage_dir / "events"))

        raw_jp = build_journey_performance(spark, inventory, gold_run_id=gold_run_id)
        raw_jp.write.mode("overwrite").parquet(str(stage_dir / "journey_performance"))
        journey_perf_df = spark.read.parquet(str(stage_dir / "journey_performance"))

        raw_dls = build_daily_line_service(spark, inventory, journey_perf_df, gold_run_id=gold_run_id)
        raw_dls.write.mode("overwrite").parquet(str(stage_dir / "daily_line_service"))
        dls_df = spark.read.parquet(str(stage_dir / "daily_line_service"))

        tables = {
            "journey_performance": journey_perf_df,
            "daily_line_service": dls_df,
            "daily_line_punctuality": build_daily_line_punctuality(
                spark, inventory, events_df, gold_run_id=gold_run_id, percentile_accuracy=percentile_accuracy
            ),
            "daily_stop_punctuality": build_daily_stop_punctuality(
                events_df, gold_run_id=gold_run_id, percentile_accuracy=percentile_accuracy
            ),
            "daily_network_punctuality": build_daily_network_punctuality(
                spark, inventory, events_df, gold_run_id=gold_run_id, percentile_accuracy=percentile_accuracy
            ),
            "daily_data_quality": build_daily_data_quality(
                spark, inventory, journey_perf_df, gold_run_id=gold_run_id
            ),
            "period_line_punctuality": build_period_punctuality(
                spark,
                inventory,
                events_df,
                gold_run_id=gold_run_id,
                include_line=True,
                percentile_accuracy=percentile_accuracy,
            ),
            "period_network_punctuality": build_period_punctuality(
                spark,
                inventory,
                events_df,
                gold_run_id=gold_run_id,
                include_line=False,
                percentile_accuracy=percentile_accuracy,
            ),
            "period_line_service": build_period_line_service(
                spark, inventory, dls_df, gold_run_id=gold_run_id
            ),
        }

        return publish_gold_outputs(
            spark=spark,
            inventory=inventory,
            tables=tables,
            gold_run_id=gold_run_id,
            gold_runs_dir=gold_runs_dir,
            repo_root=repo_root,
            started_at_utc=started_at_utc,
            percentile_accuracy=percentile_accuracy,
            shuffle_partitions=shuffle_partitions,
        )
    finally:
        spark.stop()
        shutil.rmtree(stage_dir, ignore_errors=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Kolumbus Gold analytical layer.")
    parser.add_argument("--gold-run-id", required=True, help="Immutable identifier for this Gold run.")
    parser.add_argument("--provider", default=DEFAULT_PROVIDER, help="Provider code (default: KOL).")
    parser.add_argument("--silver-runs-dir", default=DEFAULT_SILVER_RUNS_DIR, help="Silver runs root directory.")
    parser.add_argument("--gold-runs-dir", default=DEFAULT_GOLD_RUNS_DIR, help="Gold runs root directory.")
    parser.add_argument("--dates", nargs="+", help="Explicit list of expected operating dates (YYYY-MM-DD).")
    parser.add_argument("--dates-file", help="JSON file containing a list of operating dates or object with 'dates'.")
    parser.add_argument("--allowed-run-ids", nargs="+", help="Optional explicit allowlist of Silver run IDs.")
    parser.add_argument("--shuffle-partitions", type=int, default=DEFAULT_SHUFFLE_PARTITIONS)
    parser.add_argument("--percentile-accuracy", type=int, default=DEFAULT_PERCENTILE_ACCURACY)
    parser.add_argument("--driver-memory", default="2g")

    args = parser.parse_args(argv)
    dates: list[str] = []
    if args.dates:
        dates.extend(args.dates)
    if args.dates_file:
        raw = json.loads(Path(args.dates_file).read_text(encoding="utf-8"))
        if isinstance(raw, list):
            dates.extend(str(x) for x in raw)
        elif isinstance(raw, dict) and isinstance(raw.get("dates"), list):
            dates.extend(str(x) for x in raw["dates"])

    if not dates:
        parser.error("Provide --dates and/or --dates-file.")

    published = run_gold_pipeline(
        gold_run_id=args.gold_run_id,
        expected_dates=dates,
        provider=args.provider,
        silver_runs_dir=args.silver_runs_dir,
        gold_runs_dir=args.gold_runs_dir,
        allowed_run_ids=args.allowed_run_ids,
        shuffle_partitions=args.shuffle_partitions,
        percentile_accuracy=args.percentile_accuracy,
        driver_memory=args.driver_memory,
    )
    print(f"Published Gold run to: {published}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
