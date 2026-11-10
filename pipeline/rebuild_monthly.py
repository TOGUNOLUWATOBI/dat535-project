"""Rebuild registered plans for whole months, preserving every previous attempt.

Calendar-support jobs fill early-month gaps without changing the primary
observation dates used by route history. The support plan records exact source
manifests and the requested months; coverage must pass before completion.
"""

import argparse
from datetime import date, timedelta
import json
import os
from pathlib import Path
import sqlite3
import sys
from uuid import uuid4

from pipeline.ingestion.generate_provider_dataset import save_json
from pipeline.processing.windows import month_bounds
from pipeline.run_snapshot import run_snapshot


def manifest_jobs(path, role="primary"):
    path = Path(path).resolve()
    manifest = json.loads(path.read_text())
    rows = manifest.get("snapshots", [manifest])
    return [{"manifest": str(path), "snapshot_date": row["snapshot_date"], "history_role": role}
            for row in rows if row["status"] in ("validated", "validated_with_warnings", "complete", "complete_with_warnings")]


def registered_jobs(root):
    state = Path(root) / "pipeline_state.sqlite3"
    if not state.is_file():
        return []
    with sqlite3.connect(state.resolve().as_uri() + "?mode=ro", uri=True) as db:
        rows = db.execute("""SELECT s.snapshot_date,s.source_manifest,a.output_path FROM snapshots s
            JOIN stage_attempts a USING(snapshot_key) WHERE a.stage='gold' AND a.status='complete'
            ORDER BY a.finished_at DESC""").fetchall()
    jobs = {}
    for day, manifest, output in rows:
        data = json.loads((Path(output) / "metadata/manifest.json").read_text())
        if data.get("history_role", "primary") == "primary" and day not in jobs:
            jobs[day] = {"manifest": manifest, "snapshot_date": day, "history_role": "primary"}
    return list(jobs.values())


def support_jobs(path):
    path = Path(path).resolve()
    plan = json.loads(path.read_text())
    if plan.get("status") != "complete" or not plan.get("jobs"):
        raise ValueError("A completed, nonempty support plan is required")
    jobs = []
    for row in plan["jobs"]:
        first, last = month_bounds(row["service_from"])
        if (str(first), str(last)) != (row["service_from"], row["service_to"]):
            raise ValueError("Each support job must cover a complete calendar month")
        source = Path(row["manifest"])
        jobs.append({**row, "manifest": str(source if source.is_absolute() else path.parent / source),
                     "history_role": "calendar_support"})
    if len({j["snapshot_date"] for j in jobs}) != len(jobs):
        raise ValueError("Support export dates must be unique")
    return jobs


def audit_coverage(manifests, months, expected_providers=("AKT", "KOL")):
    results = {}
    for month in sorted(set(months)):
        first, last = month_bounds(month + "-01")
        candidates = [m for m in manifests if m["service_from"] <= str(last) and m["service_to"] >= str(first)]
        providers = sorted(expected_providers)
        absent = []
        for n in range((last - first).days + 1):
            day = str(first + timedelta(days=n))
            if not any(m["service_from"] <= day <= m["service_to"] and all(
                p in m["providers"] and ((m.get("source_calendar_coverage") or {}).get(p) or {}).get("from", "9999") <= day
                <= ((m.get("source_calendar_coverage") or {}).get(p) or {}).get("to", "0000") for p in providers)
                       for m in candidates):
                absent.append(day)
        missing = {p: absent for p in providers} if absent else {}
        results[month] = {"days": (last - first).days + 1, "providers": providers,
                          "missing_dates": missing, "complete": bool(providers) and not missing}
    return results


def ensure_support_months(jobs, plan_path, remove_national=False):
    """Extend a historical support plan for registered later primary months."""
    jobs = list(jobs)
    plan_path = Path(plan_path)
    support = support_jobs(plan_path)
    primary_months = {j["snapshot_date"][:7] for j in jobs}
    supporting_months = {j["service_from"][:7] for j in support}
    for month in sorted(primary_months - supporting_months):
        from pipeline.ingestion.prepare_monthly_support import prepare
        from pipeline.ingestion.download_entur_netex import parse_month
        providers = json.loads(plan_path.read_text())["providers"]
        extra = prepare(plan_path.resolve().parent / "extra_months" / month,
                        parse_month(month), parse_month(month), providers,
                        remove_national=remove_national,
                        excluded_dates=[j["snapshot_date"] for j in jobs])
        support.extend(support_jobs(extra))
    return support


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--primary-manifest", type=Path, action="append", default=[])
    parser.add_argument("--support-plan", type=Path, required=True)
    parser.add_argument("--remove-national-after-validation", action="store_true",
                        help="Only for newly prepared extra-month support archives; failed sources stay")
    parser.add_argument("--skip-history", action="store_true", help="For isolated verification runs only")
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args(argv)
    jobs = {(j["snapshot_date"], j["history_role"]): j for j in registered_jobs(args.data_root)}
    for path in args.primary_manifest:
        jobs.update({(j["snapshot_date"], j["history_role"]): j for j in manifest_jobs(path)})
    support = ensure_support_months(jobs.values(), args.support_plan, args.remove_national_after_validation)
    if {j["snapshot_date"] for j in support} & {day for day, _ in jobs}:
        raise ValueError("Support sources must use dates distinct from the primary observation sample")
    jobs.update({(j["snapshot_date"], j["history_role"]): j for j in support})
    target = args.data_root / "rebuilds" / f"monthly-{uuid4().hex[:12]}"
    target.mkdir(parents=True)
    report = {"status": "in_progress", "jobs": list(jobs.values()), "outputs": []}
    save_json(target / "manifest.json", report)
    spark = None
    try:
        from pyspark.sql import SparkSession
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        spark = (SparkSession.builder.appName("DAT535-full-month-rebuild").master(args.master)
                 .config("spark.sql.session.timeZone", "Europe/Oslo").config("spark.sql.shuffle.partitions", "4")
                 .config("spark.ui.showConsoleProgress", "false").getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        manifests = []
        for n, job in enumerate(jobs.values(), 1):
            print(f"[{n}/{len(jobs)}] Full month: {job['snapshot_date']} ({job['history_role']})", flush=True)
            outputs = run_snapshot(spark, job["manifest"], job["snapshot_date"], args.data_root,
                                   service_from=job.get("service_from"), service_to=job.get("service_to"),
                                   history_role=job["history_role"])
            report["outputs"].append({**job, "stages": outputs})
            manifests.append(json.loads((Path(outputs["gold"]["path"]) / "metadata/manifest.json").read_text()))
            save_json(target / "manifest.json", report)
        report["coverage"] = audit_coverage(manifests, [j["service_from"][:7] for j in support])
        if not all(v["complete"] for v in report["coverage"].values()):
            raise RuntimeError("Full-month coverage audit failed; missing dates are recorded in the rebuild manifest")
        if not args.skip_history:
            from pipeline.analysis.route_history import run
            path, _, _ = run(spark, args.data_root, mappings_path=Path("config/route_renumberings.json"),
                             discontinuations_path=Path("config/route_discontinuations.json"))
            report["history"] = str(path)
        report["status"] = "complete"
        save_json(target / "manifest.json", report)
        print(f"COMPLETE: {target / 'manifest.json'}", flush=True)
        return 0
    except Exception as error:
        report.update(status="failed", error=f"{type(error).__name__}: {error}")
        save_json(target / "manifest.json", report)
        print(report["error"], file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
