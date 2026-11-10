from contextlib import redirect_stderr
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

from pipeline.checkpoints import Checkpoints
from pipeline.rebuild_monthly import main, run_worker
from pipeline.spark_runtime import error_text, stop_spark, worker_environment


class DeadGatewayError(Exception):
    def __str__(self):
        raise ConnectionRefusedError("JVM stopped")


class WorkerTests(unittest.TestCase):
    def test_heap_is_set_before_launch_and_worker_cannot_attach_to_an_old_jvm(self):
        with patch.dict(os.environ, {"PYSPARK_GATEWAY_PORT": "123", "PYSPARK_GATEWAY_SECRET": "secret",
                                    "PYSPARK_SUBMIT_ARGS": "--driver-memory 1g --conf spark.driver.memory=512m --conf 'spark.local.dir=/tmp/a b' pyspark-shell"}):
            environment = worker_environment("2g")
            self.assertNotIn("PYSPARK_GATEWAY_PORT", environment)
            self.assertNotIn("PYSPARK_GATEWAY_SECRET", environment)
            self.assertEqual(shlex.split(environment["PYSPARK_SUBMIT_ARGS"]),
                             ["--driver-memory", "2g", "--conf", "spark.local.dir=/tmp/a b", "pyspark-shell"])
            self.assertEqual(os.environ["PYSPARK_GATEWAY_PORT"], "123")
        with self.assertRaises(ValueError):
            worker_environment("unbounded")

    def test_dead_gateway_cannot_prevent_checkpoint_failure_from_being_recorded(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = Checkpoints(root / "state.sqlite3")
            try:
                key = state.register({"snapshot_date": "2026-04-01", "providers": ["KOL"],
                                      "source_manifest": "manifest.json", "files": [], "stop_files": []})
                def fail(output):
                    raise DeadGatewayError()
                with state.lock(key), self.assertRaises(DeadGatewayError):
                    state.execute(key, "staging", key, "unchanged", root / "outputs", fail)
                saved = state.status()[0]
                self.assertEqual(saved["status"], "failed")
                self.assertIn("DeadGatewayError", saved["error"])
            finally:
                state.close()
        with redirect_stderr(io.StringIO()):
            spark = Mock(); spark.stop.side_effect = DeadGatewayError()
            stop_spark(spark)

    def test_failed_child_and_partial_progress_are_recorded_without_starting_later_jobs(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            jobs = [{"manifest": str(root / "source.json"), "snapshot_date": day, "history_role": "primary"}
                    for day in ("2026-01-15", "2026-02-15")]
            gold = root / "gold/metadata"; gold.mkdir(parents=True)
            (gold / "manifest.json").write_text(json.dumps({"service_from": "2026-01-01", "service_to": "2026-01-31"}))
            successful = {"stages": {"gold": {"path": str(gold.parent), "reused": True}}, "runtime": {"driver_memory": "2g"}}
            with patch("pipeline.rebuild_monthly.registered_jobs", return_value=jobs), \
                    patch("pipeline.rebuild_monthly.ensure_support_months", return_value=[]), \
                    patch("pipeline.rebuild_monthly.run_worker", side_effect=[successful, DeadGatewayError()]) as worker, \
                    redirect_stderr(io.StringIO()):
                self.assertEqual(main(["--data-root", str(root), "--support-plan", "unused.json"]), 1)
            report = json.loads(next(root.glob("rebuilds/monthly-*/manifest.json")).read_text())
            self.assertEqual(report["status"], "failed")
            self.assertEqual(len(report["outputs"]), 1)
            self.assertIn("DeadGatewayError", report["error"])
            self.assertEqual(worker.call_count, 2)

    def test_failure_during_support_preparation_also_has_a_report(self):
        with tempfile.TemporaryDirectory() as temp:
            with patch("pipeline.rebuild_monthly.registered_jobs", return_value=[]), \
                    patch("pipeline.rebuild_monthly.ensure_support_months", side_effect=RuntimeError("support unavailable")), \
                    redirect_stderr(io.StringIO()):
                self.assertEqual(main(["--data-root", temp, "--support-plan", "unused.json"]), 1)
            report = json.loads(next(Path(temp).glob("rebuilds/monthly-*/manifest.json")).read_text())
            self.assertEqual(report["status"], "failed")
            self.assertIn("support unavailable", report["error"])

    def test_worker_exit_code_is_checked_even_if_result_claims_completion(self):
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "worker.json"
            target.write_text(json.dumps({"status": "complete"}))
            with patch("pipeline.rebuild_monthly.subprocess.run", return_value=subprocess.CompletedProcess([], 1)) as child:
                with self.assertRaisesRegex(RuntimeError, "code 1"):
                    run_worker("snapshot", target, Path(temp), "local[2]", "2g", {"snapshot_date": "2026-04-01"})
            self.assertIn("pipeline.batch_worker", child.call_args.args[0])
            self.assertEqual(shlex.split(child.call_args.kwargs["env"]["PYSPARK_SUBMIT_ARGS"])[:2], ["--driver-memory", "2g"])


if __name__ == "__main__":
    unittest.main()
