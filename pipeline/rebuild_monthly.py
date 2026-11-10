"""Rebuild registered plans for whole months, preserving every previous attempt.

Calendar-support jobs fill early-month gaps without changing the primary
observation dates used by route history. The support plan records exact source
manifests and the requested months; coverage must pass before completion.
"""

import argparse
from contextlib import closing
from datetime import timedelta
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from uuid import uuid4

from pipeline.common import write_json
from pipeline.processing.windows import month_bounds
from pipeline.spark_runtime import error_text, memory_size, worker_environment


def manifest_jobs(path, role="primary"):
    """One job per validated snapshot in an ingestion or historical dataset manifest."""
    path = Path(path).resolve()
    manifest = json.loads(path.read_text())
    rows = manifest.get("snapshots", [manifest])
    return [{"manifest": str(path), "snapshot_date": row["snapshot_date"], "history_role": role}
            for row in rows if row["status"] in ("validated", "validated_with_warnings", "complete", "complete_with_warnings")]


def registered_jobs(root):
    """Primary snapshots that already have a complete Gold output in the checkpoint registry."""
    state = Path(root) / "pipeline_state.sqlite3"
    if not state.is_file():
        return []
    # closing(): a sqlite3 connection's own context manager commits but does not close.
    with closing(sqlite3.connect(state.resolve().as_uri() + "?mode=ro", uri=True)) as db:
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
    """Calendar-support jobs from a completed support plan; each must cover one whole month."""
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
    """Per month, the dates not covered by any Gold output for every expected provider.

    A date counts as covered only if one output's service window and each
    provider's source calendar validity both include it.
    """
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


def run_worker(task, target, data_root, master, driver_memory, job=None):
    """Wait for one worker to exit fully before launching the next JVM."""
    target = Path(target).resolve()
    command = [sys.executable, "-B", "-u", "-m", "pipeline.batch_worker", "--task", task,
               "--data-root", str(Path(data_root).resolve()), "--master", master, "--result", str(target)]
    if job is not None:
        job_path = target.with_name(target.stem + "-input.json")
        write_json(job_path, job)
        command.extend(["--job", str(job_path)])
    process = subprocess.run(command, cwd=Path(__file__).resolve().parent.parent,
                             env=worker_environment(driver_memory), check=False)
    result = json.loads(target.read_text()) if target.is_file() else {}
    if process.returncode != 0 or result.get("status") != "complete":
        raise RuntimeError(f"{task} worker exited with code {process.returncode}: "
                           f"{result.get('error', 'worker stopped before recording completion')}; see {target}")
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--primary-manifest", type=Path, action="append", default=[])
    parser.add_argument("--support-plan", type=Path, required=True)
    parser.add_argument("--remove-national-after-validation", action="store_true",
                        help="Only for newly prepared extra-month support archives; failed sources stay")
    parser.add_argument("--skip-history", action="store_true", help="For isolated verification runs only")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--driver-memory", default=os.environ.get("DAT535_DRIVER_MEMORY", "2g"),
                        type=memory_size, help="Java heap per sequential worker, set before JVM startup (default: 2g)")
    args = parser.parse_args(argv)
    args.data_root = args.data_root.resolve()
    target = args.data_root / "rebuilds" / f"monthly-{uuid4().hex[:12]}"
    target.mkdir(parents=True)
    (target / "workers").mkdir()
    report = {"status": "preparing", "phase": "support", "jobs": [], "outputs": [], "driver_memory": args.driver_memory,
              "execution": "one_sequential_process_per_snapshot"}
    write_json(target / "manifest.json", report)
    try:
        jobs = {(j["snapshot_date"], j["history_role"]): j for j in registered_jobs(args.data_root)}
        for path in args.primary_manifest:
            jobs.update({(j["snapshot_date"], j["history_role"]): j for j in manifest_jobs(path)})
        support = ensure_support_months(jobs.values(), args.support_plan, args.remove_national_after_validation)
        if {j["snapshot_date"] for j in support} & {day for day, _ in jobs}:
            raise ValueError("Support sources must use dates distinct from the primary observation sample")
        jobs.update({(j["snapshot_date"], j["history_role"]): j for j in support})
        report.update(status="in_progress", phase="snapshots", jobs=list(jobs.values()))
        write_json(target / "manifest.json", report)
        manifests = []
        for n, job in enumerate(jobs.values(), 1):
            print(f"[{n}/{len(jobs)}] Full month: {job['snapshot_date']} ({job['history_role']})", flush=True)
            report["current_job"] = job
            write_json(target / "manifest.json", report)
            result = run_worker("snapshot", target / "workers" / f"snapshot-{n:03d}.json", args.data_root,
                                args.master, args.driver_memory, job)
            outputs = result["stages"]
            report["outputs"].append({**job, "stages": outputs, "runtime": result["runtime"]})
            manifests.append(json.loads((Path(outputs["gold"]["path"]) / "metadata/manifest.json").read_text()))
            write_json(target / "manifest.json", report)
        report.pop("current_job", None)
        report["phase"] = "coverage"
        write_json(target / "manifest.json", report)
        report["coverage"] = audit_coverage(manifests, [j["service_from"][:7] for j in support])
        if not all(v["complete"] for v in report["coverage"].values()):
            raise RuntimeError("Full-month coverage audit failed; missing dates are recorded in the rebuild manifest")
        if not args.skip_history:
            report["phase"] = "history"
            write_json(target / "manifest.json", report)
            result = run_worker("history", target / "workers/history.json", args.data_root,
                                args.master, args.driver_memory)
            report["history"] = result["history"]
        report["status"] = "complete"
        report["phase"] = "complete"
        report.pop("current_job", None)
        write_json(target / "manifest.json", report)
        print(f"COMPLETE: {target / 'manifest.json'}", flush=True)
        return 0
    except Exception as error:
        report.update(status="failed", error=error_text(error))
        write_json(target / "manifest.json", report)
        print(report["error"], file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
