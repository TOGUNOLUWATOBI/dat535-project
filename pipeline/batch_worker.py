"""One Spark JVM per snapshot/history operation for the monthly rebuild.

``rebuild_monthly`` starts this module as a separate process for every job, so
each job gets a fresh driver heap and a JVM failure cannot affect the next job.
The outcome is written to ``--result`` as JSON, including after a failure.
"""

import argparse
import json
import os
from pathlib import Path
import sys

from pipeline.common import write_json
from pipeline.spark_runtime import error_text, start_spark, stop_spark


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", choices=("snapshot", "history"), required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--job", type=Path)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args(argv)
    if args.result.exists():
        parser.error("Worker result already exists")
    if args.task == "snapshot" and args.job is None:
        parser.error("Snapshot task requires --job")
    spark = None
    result = {"status": "running", "task": args.task, "pid": os.getpid()}
    write_json(args.result, result)
    try:
        spark = start_spark(f"DAT535-monthly-{args.task}", args.master)
        result["runtime"] = {"driver_memory": spark.sparkContext.getConf().get("spark.driver.memory"),
                             "max_heap_bytes": spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory()}
        print(f"Fresh {args.task} worker {os.getpid()}: driver heap {result['runtime']['driver_memory']}", flush=True)
        if args.task == "snapshot":
            from pipeline.run_snapshot import run_snapshot
            job = json.loads(args.job.read_text())
            result["stages"] = run_snapshot(spark, job["manifest"], job["snapshot_date"], args.data_root,
                                           service_from=job.get("service_from"), service_to=job.get("service_to"),
                                           history_role=job["history_role"])
        else:
            from pipeline.analysis.route_history import run
            path, _, _ = run(spark, args.data_root, mappings_path=Path("config/route_renumberings.json"),
                             discontinuations_path=Path("config/route_discontinuations.json"))
            result["history"] = str(path)
        result["status"] = "complete"
        write_json(args.result, result)
        return 0
    except Exception as error:
        result.update(status="failed", error=error_text(error))
        write_json(args.result, result)
        print(result["error"], file=sys.stderr)
        return 1
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    raise SystemExit(main())
