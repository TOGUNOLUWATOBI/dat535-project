"""Run one or all validated snapshots through checkpointed staging, Silver and Gold."""

import argparse
from datetime import date
import json
import os
from pathlib import Path
import shutil
import sys

from pipeline.checkpoints import Checkpoints, digest, identity
from pipeline.processing.bronze.staging_sources import load_snapshot_sources


def implementation(*paths):
    root = Path(__file__).resolve().parent.parent
    return identity([(p, digest(root / p)) for p in paths])


def run_snapshot(spark, manifest, day, data_root, state_path=None, through="gold", service_from=None, service_to=None):
    from pipeline.processing.bronze.snapshot_to_parquet import convert_snapshot
    from pipeline.processing.silver.build import build_silver
    from pipeline.processing.gold.build import build_gold
    data_root = Path(data_root).resolve()
    if data_root.is_relative_to(Path(manifest).resolve().parent):
        raise ValueError("Persistent data root must not be inside the source manifest directory")
    # Check every input hash before trusting any previously completed stage.
    selection = load_snapshot_sources(manifest, day)
    state = Checkpoints(state_path or data_root / "pipeline_state.sqlite3")
    try:
        key = state.register(selection)
        with state.lock(key):
            versions = {
                "staging": implementation("pipeline/processing/bronze/snapshot_to_parquet.py", "pipeline/processing/bronze/netex_schema.py",
                    "pipeline/processing/bronze/xml_to_parquet.py", "pipeline/processing/bronze/staging_sources.py"),
                "silver": implementation("pipeline/processing/silver/build.py","pipeline/processing/silver/calendars.py"),
                "gold": implementation("pipeline/processing/gold/build.py"),
            }
            outputs = {}
            def destination(layer): return data_root / layer / "entur" / selection["snapshot_date"] / key[:16]
            # Leave headroom for Spark's shuffle and system services. No cleanup is automatic.
            data_root.mkdir(parents=True,exist_ok=True)
            if shutil.disk_usage(data_root).free < 800 * 1024**2:
                raise RuntimeError("Less than 800 MiB free: add storage before starting another snapshot; no raw data was removed")
            staging, staged_sha, reused = state.execute(key,"staging",key,versions["staging"],destination("staging"),
                lambda output: convert_snapshot(spark,manifest,output,day,selection))
            outputs["staging"] = {"path":str(staging),"reused":reused}
            if through == "staging": return outputs
            silver_input = identity([staged_sha,str(service_from),str(service_to)])
            silver, silver_sha, reused = state.execute(key,"silver",silver_input,versions["silver"],destination("silver"),
                lambda output:build_silver(spark,staging,output,service_from,service_to))
            outputs["silver"] = {"path":str(silver),"reused":reused}
            if through == "silver": return outputs
            gold, _, reused = state.execute(key,"gold",silver_sha,versions["gold"],destination("gold"),
                lambda output:build_gold(spark,silver,output))
            outputs["gold"] = {"path":str(gold),"reused":reused}
            return outputs
    finally: state.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest",type=Path,required=True)
    group=parser.add_mutually_exclusive_group()
    group.add_argument("--date",type=date.fromisoformat)
    group.add_argument("--all-snapshots",action="store_true")
    parser.add_argument("--data-root",type=Path,required=True)
    parser.add_argument("--state",type=Path)
    parser.add_argument("--through",choices=("staging","silver","gold"),default="gold")
    parser.add_argument("--service-from",type=date.fromisoformat)
    parser.add_argument("--service-to",type=date.fromisoformat)
    parser.add_argument("--master",default="local[2]")
    args=parser.parse_args()
    spark=None
    try:
        manifest=json.loads(args.manifest.read_text())
        days=[args.date]
        if args.all_snapshots:
            days=[date.fromisoformat(s["snapshot_date"]) for s in manifest.get("snapshots",[])
                  if s["status"] in ("validated","validated_with_warnings")]
            if not days: raise ValueError("No validated historical snapshots found")
        from pyspark.sql import SparkSession
        if args.master.startswith("local"):
            os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        spark=(SparkSession.builder.appName("DAT535-checkpointed-pipeline").master(args.master)
            .config("spark.sql.session.timeZone","Europe/Oslo").config("spark.sql.shuffle.partitions","4")
            .config("spark.ui.showConsoleProgress","false").getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        for day in days:
            print(json.dumps({"snapshot":str(day),"outputs":run_snapshot(spark,args.manifest,day,args.data_root,args.state,
                args.through,args.service_from,args.service_to)},indent=2),flush=True)
        return 0
    except Exception as error:
        print(f"Pipeline failed: {type(error).__name__}: {error}",file=sys.stderr); return 1
    finally:
        if spark is not None: spark.stop()


if __name__ == "__main__":sys.exit(main())
