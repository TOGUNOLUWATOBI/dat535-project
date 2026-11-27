"""Run pipeline layers in a fresh directory; reuse upstream published inputs read-only.

Use --dry-run to inspect the exact commands without starting Spark or creating outputs.
The production batch wrappers are deliberately avoided because they remove existing runs.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from zoneinfo import ZoneInfo


LAYERS = ("bronze", "staging", "silver", "gold")
REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class Command:
    name: str
    argv: list[str]


def selected_dates(value: str, source_root: Path, *, now: datetime | None = None) -> list[str]:
    if value.strip() == "yesterday":
        today = (now or datetime.now(ZoneInfo("Europe/Oslo"))).astimezone(ZoneInfo("Europe/Oslo")).date()
        values = [(today - timedelta(days=1)).isoformat()]
    elif value.strip() == "manifest":
        payload = json.loads((source_root / "bronze/netex/kol/manifest.json").read_text())
        values = [r["target_date"] for r in payload["records"] if r.get("status") == "downloaded"]
    else:
        values = [v.strip() for v in value.split(",") if v.strip()]
    dates = sorted({date.fromisoformat(v).isoformat() for v in values})
    if not dates:
        raise ValueError("Select at least one operating date.")
    return dates


def build_plan(repo: Path, source: Path, root: Path, dates: list[str], start: str,
               python: str, gui_python: str, *, environment: str = "dev") -> tuple[list[Command], Path, Path]:
    """Commands use absolute checkout paths and new run paths; no existing-run deletion."""
    if start not in LAYERS:
        raise ValueError(f"Unknown start layer: {start}")
    if environment not in ("dev", "prod"):
        raise ValueError(f"Unknown environment: {environment}")
    if not dates:
        raise ValueError("No dates selected")
    source, root, repo = source.resolve(), root.resolve(), repo.resolve()
    protected = [source / p for p in ("bronze", "staging", "silver", "gold", "metadata")]
    if root == source or source.is_relative_to(root) or any(root.is_relative_to(p) for p in protected):
        raise ValueError("Development output must be separate from published input directories.")
    first = LAYERS.index(start)
    staging = (root if first <= 1 else source) / "staging/netex/kol/runs"
    silver = (root if first <= 2 else source) / "silver/kol/runs"
    siri = (root if first == 0 else source) / "bronze/siri_et/kol"
    gold = root / f"gold/kol/runs/{environment}-gold"
    commands: list[Command] = []
    spark_submit = str(Path(python).parent / "spark-submit")
    spark = [spark_submit, "--master", "local[1]", "--driver-memory", "2g",
             "--conf", "spark.sql.shuffle.partitions=4",
             "--conf", "spark.sql.autoBroadcastJoinThreshold=-1",
             "--conf", "spark.sql.inMemoryColumnarStorage.batchSize=1"]

    if first == 0:
        for day in dates:
            for label, script in (("netex", "Netex/bronze_entur_netex.py"),
                                  ("siri-et", "SIRI_ET/bronze_entur_siri_et.py")):
                commands.append(Command(f"bronze-{label}-{day}", [python, str(repo / "src/pipeline/Bronze" / script),
                                                                 "--mode", "single-day", "--date", day]))
    if first <= 1:
        quarters: dict[int, list[str]] = {}
        for day in dates:
            quarters.setdefault((date.fromisoformat(day).month - 1) // 3 + 1, []).append(day)
        # Silver currently discovers these four exact quarterly directory names.
        for quarter, days in sorted(quarters.items()):
            name = f"batch-2026-q{quarter}"
            commands.append(Command(f"staging-q{quarter}", spark + [
                "--py-files", ",".join(str(repo / "src/pipeline/Staging" / f)
                                        for f in ("schemas.py", "zip_xml_reader.py")),
                str(repo / "src/pipeline/Staging/stage_netex.py"),
                "--dates", ",".join(days), "--output-dir", str(staging / name),
                "--zip-slices", str(len(days))]))
    if first <= 2:
        # Keep Silver batches bounded on the 7.8 GiB VM, even for a 53-date selection.
        for offset in range(0, len(dates), 5):
            name = f"{environment}-silver-{offset // 5 + 1:03d}"
            commands.append(Command(name, spark + [str(root / "launch_silver.py"),
                "--dates", ",".join(dates[offset:offset + 5]),
                "--staging-runs-dir", str(staging), "--siri-et-dir", str(siri),
                "--output-dir", str(silver / name), "--shuffle-partitions", "4"]))
    # Bound Gold scan tasks and retain its four shuffle partitions on the small VM.
    gold_spark = spark + ["--conf", "spark.sql.files.maxPartitionBytes=16777216",
                          "--conf", "spark.sql.adaptive.coalescePartitions.enabled=false"]
    commands.append(Command("gold", gold_spark + [str(root / "launch_gold.py"),
        "--gold-run-id", gold.name, "--dates", *dates,
        "--silver-runs-dir", str(silver), "--gold-runs-dir", str(gold.parent)]))
    commands.append(Command("gui-smoke", [gui_python, str(repo / ".github/scripts/smoke_dev_gui.py")]))
    return commands, gold, silver


def execute(commands: list[Command], repo: Path, root: Path, env: dict[str, str]) -> None:
    """Persist timings and logs even when a layer fails; stop before downstream layers."""
    metadata = root / "metadata"
    metadata.mkdir(exist_ok=True)
    report = {"code_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
              "status": "running", "steps": []}
    report_path = metadata / "run.json"
    report_path.write_text(json.dumps(report, indent=2))
    try:
        for command in commands:
            started = time.monotonic()
            result = {"name": command.name, "status": "running"}
            report["steps"].append(result)
            try:
                print(f"Starting {command.name}", flush=True)
                with (metadata / f"{command.name}.log").open("w") as log:
                    with subprocess.Popen(command.argv, cwd=repo, env=env, stdout=subprocess.PIPE,
                                          stderr=subprocess.STDOUT, text=True) as process:
                        for line in process.stdout:
                            log.write(line)
                            print(line, end="", flush=True)
                        returncode = process.wait()
                result.update(status="complete" if returncode == 0 else "failed", returncode=returncode)
                if returncode:
                    raise subprocess.CalledProcessError(returncode, command.argv)
            except Exception:
                result["status"] = "failed"
                raise
            finally:
                result["elapsed_sec"] = round(time.monotonic() - started, 3)
                report_path.write_text(json.dumps(report, indent=2))
        report["status"] = "complete"
    except Exception:
        report["status"] = "failed"
        raise
    finally:
        report_path.write_text(json.dumps(report, indent=2))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path("/srv/dat535/data"))
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--start-layer", choices=LAYERS, default="bronze")
    parser.add_argument("--environment", choices=("dev", "prod"), default="dev")
    parser.add_argument("--dates", default="yesterday")
    parser.add_argument("--gui-python", default="/home/ubuntu/dat535-gui-env/bin/python")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    source, root = args.source_root.resolve(), args.run_root.resolve()
    dates = selected_dates(args.dates, source)
    commands, gold, silver = build_plan(REPO_ROOT, source, root, dates, args.start_layer,
                                        sys.executable, args.gui_python, environment=args.environment)
    if args.dry_run:
        print(json.dumps({"dates": dates, "commands": [asdict(c) for c in commands]}, indent=2))
        return 0
    root.mkdir(parents=True, exist_ok=False)
    # Expose paths early so failed runs can still attach their logs as artifacts.
    if output := os.environ.get("GITHUB_OUTPUT"):
        with Path(output).open("a") as handle:
            for key, path in (("run_root", root), ("gold_dir", gold), ("silver_runs_dir", silver)):
                handle.write(f"{key}={path}\n")
    if args.start_layer == "staging":
        (root / "bronze").symlink_to(source / "bronze", target_is_directory=True)
    for layer in ("silver", "gold"):
        (root / f"launch_{layer}.py").write_text(
            f'import runpy\nrunpy.run_module("pipeline.{layer.title()}.build_{layer}", run_name="__main__")\n')
    env = dict(os.environ, DAT535_DATA_ROOT=str(root), PYTHONPATH=str(REPO_ROOT / "src"),
               PYSPARK_PYTHON=sys.executable, DAT535_GOLD_RUN=str(gold), DAT535_SILVER_RUNS=str(silver))
    env["PATH"] = str(Path(sys.executable).parent) + os.pathsep + env.get("PATH", "")
    for key in ("SPARK_HOME", "PYSPARK_DRIVER_PYTHON", "PYSPARK_DRIVER_PYTHON_OPTS"):
        env.pop(key, None)
    execute(commands, REPO_ROOT, root, env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
