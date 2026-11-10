"""Planned-service metrics reconciled with validated Silver service dates."""

from datetime import date
import json
from pathlib import Path

from pipeline.ingestion.generate_provider_dataset import save_json
from pipeline.processing.bronze.xml_to_parquet import utc_now


def build_gold(spark, silver, output, thresholds=None):
    """Publish Gold only if the Silver input passes the quality gate (warn still publishes)."""
    from pyspark.sql import functions as F
    from pipeline.quality.silver_profile import evaluate, profile_silver
    silver, output = Path(silver).resolve(), Path(output).resolve()
    source = json.loads((silver / "metadata/manifest.json").read_text())
    if source.get("status") != "complete": raise ValueError("Gold requires complete validated Silver")
    if output.is_relative_to(silver) or silver.is_relative_to(output):
        raise ValueError("Gold output must be separate from Silver")
    output.mkdir(parents=True, exist_ok=False); (output / "metadata").mkdir()
    report = {"schema_version": 1, "layer": "gold", "status": "in_progress",
              "snapshot_date": source["snapshot_date"], "providers": source["providers"],
              "service_from": source["service_from"], "service_to": source["service_to"],
              "service_window_policy": source.get("service_window_policy", "relative"),
              "history_role": source.get("history_role", "primary"),
              "source_calendar_coverage": source.get("source_calendar_coverage"),
              "source_manifest": str(silver / "metadata/manifest.json"),
              "started_at_utc": utc_now(), "tables": {},
              "meaning": "Planned fixed timetable service as observed in this snapshot; no actual operations or ridership."}
    path = output / "metadata/manifest.json"; save_json(path, report)
    def read(name): return spark.read.parquet(str(silver / source["tables"][name]["path"]))
    def write(name, frame):
        target = output / name
        expected = frame.count()
        frame.write.mode("errorifexists").option("compression", "snappy").parquet(str(target))
        saved = spark.read.parquet(str(target))
        if saved.count() != expected: raise RuntimeError(f"{name}: read-back count mismatch")
        report["tables"][name] = {"path": name, "rows": expected, "schema": json.loads(saved.schema.json()),
            "parquet_bytes": sum(f.stat().st_size for f in target.glob("*.parquet"))}
        save_json(path,report); print(f"gold/{name}: {expected:,} rows",flush=True)
        return saved
    try:
        # Quality gate: profile Silver and stop before writing any table on "fail".
        profile = profile_silver(spark, silver)
        save_json(output / "metadata/quality_profile.json", profile)
        gate = evaluate(profile, thresholds)
        report["quality_gate"] = {k: gate[k] for k in ("status", "gate_version", "findings", "metrics")}
        save_json(path, report)
        if gate["status"] == "fail":
            failed = sorted({f"{f['provider']}:{f['rule']}" for f in gate["findings"] if f["level"] == "fail"})
            raise RuntimeError(f"Quality gate failed, Gold not published: {failed}")
        dates = read("journey_service_dates")
        valid = dates.where("is_valid AND NOT is_flexible")
        total, eligible = dates.count(), valid.count()
        keys = ["snapshot_date", "provider", "line_entity_id", "line_entity_version", "line_public_code", "line_transport_mode"]
        daily = valid.groupBy(*keys,"service_date").agg(F.count("*").alias("scheduled_departures"),
            F.min("first_departure_seconds").alias("first_departure_seconds"),
            F.max("first_departure_seconds").alias("last_departure_seconds"))
        daily = daily.withColumn("departure_span_hours", (F.col("last_departure_seconds")-F.col("first_departure_seconds"))/3600)
        daily = write("daily_line_service",daily)
        reconciled = daily.agg(F.coalesce(F.sum("scheduled_departures"),F.lit(0)).alias("n")).first().n
        if reconciled != eligible: raise RuntimeError("Gold departures do not reconcile to eligible Silver service dates")
        days = (date.fromisoformat(source["service_to"])-date.fromisoformat(source["service_from"])).days+1
        coverage = source.get("source_calendar_coverage") or {
            p: {"from": source["service_from"], "to": source["service_to"]} for p in source["providers"]}
        providers = spark.createDataFrame([(p, (coverage.get(p) or {}).get("from"),
                                            (coverage.get(p) or {}).get("to")) for p in source["providers"]],
                                          "provider string, covered_from string, covered_to string")
        calendar = spark.range(days).select(F.date_add(F.lit(source["service_from"]).cast("date"),
                                                       F.col("id").cast("int")).alias("service_date"))
        network = daily.groupBy("provider", "service_date").agg(
            F.sum("scheduled_departures").alias("scheduled_departures"),
            F.countDistinct("line_entity_id").alias("active_lines"),
            F.min("first_departure_seconds").alias("first_departure_seconds"),
            F.max("last_departure_seconds").alias("last_departure_seconds"))
        network = calendar.crossJoin(providers).join(network,["provider","service_date"],"left")
        network = network.withColumn("is_source_covered", F.coalesce(
            (F.col("service_date") >= F.to_date("covered_from"))
            & (F.col("service_date") <= F.to_date("covered_to")),F.lit(False)))
        for column in ("scheduled_departures","active_lines"):
            network = network.withColumn(column,F.when(F.col("is_source_covered"),F.coalesce(F.col(column),F.lit(0)))
                                          .otherwise(F.col(column)))
        write("daily_network_service",network.drop("covered_from","covered_to")
              .withColumn("snapshot_date",F.lit(source["snapshot_date"]).cast("date")))
        summary = daily.groupBy(*keys).agg(F.sum("scheduled_departures").alias("scheduled_departures"),
            F.countDistinct("service_date").alias("days_with_service"),
            F.min("first_departure_seconds").alias("earliest_departure_seconds"),
            F.max("last_departure_seconds").alias("latest_departure_seconds"))
        summary = summary.withColumn("window_days",F.lit(days)).withColumn("average_departures_per_calendar_day",F.col("scheduled_departures")/days)
        write("line_window_summary",summary)
        active = valid.select("journey_key").dropDuplicates()
        coverage = (read("passing_times").where("is_valid").join(active,"journey_key","inner")
            .select("snapshot_date","provider","line_key","point_scheduled_stop_ref","assignment_quay_ref", "assignment_flexible_stop_place_ref").dropDuplicates())
        write("line_stop_coverage",coverage)
        write("excluded_service_dates", dates.where("NOT is_valid OR is_flexible"))
        report.update(status="complete", completed_at_utc=utc_now(), reconciliation={
            "silver_service_occurrences":total,"eligible_fixed_service_occurrences":eligible,
            "excluded_service_occurrences":total-eligible,"gold_scheduled_departures":reconciled,
            "counts_match": True})
        save_json(path,report); return report
    except Exception as error:
        report.update(status="failed",completed_at_utc=utc_now(),error=f"{type(error).__name__}: {error}")
        save_json(path,report); raise
