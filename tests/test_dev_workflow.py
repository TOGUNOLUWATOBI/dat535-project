"""Development runner boundaries and failure reporting, without Spark or cloud access."""

import contextlib
from datetime import datetime, timezone
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("dev_runner", ROOT / ".github/scripts/run_dev_pipeline.py")
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


class DevWorkflowTests(unittest.TestCase):
    def plan(self, start="bronze", dates=None):
        return runner.build_plan(ROOT, Path("/srv/dat535/data"), Path("/srv/dat535/data/dev/runs/test"),
                                 dates or ["2026-01-01"], start,
                                 "/home/ubuntu/spark-env/bin/python", "/home/ubuntu/dat535-gui-env/bin/python")

    def test_yesterday_uses_oslo_date_at_utc_midnight_and_year_boundary(self):
        # 23:30 UTC is already the next calendar day in Oslo, in winter and summer.
        for now, expected in ((datetime(2026, 11, 27, 23, 30, tzinfo=timezone.utc), "2026-11-27"),
                              (datetime(2026, 6, 1, 22, 30, tzinfo=timezone.utc), "2026-06-01"),
                              (datetime(2026, 12, 31, 23, 30, tzinfo=timezone.utc), "2026-12-31")):
            with self.subTest(now=now):
                self.assertEqual(runner.selected_dates("yesterday", ROOT, now=now), [expected])

    def test_explicit_dates_are_validated_sorted_and_deduplicated(self):
        self.assertEqual(runner.selected_dates("2026-03-29, 2026-01-01,2026-03-29", ROOT),
                         ["2026-01-01", "2026-03-29"])
        for value in ("", "2026-02-30", "2026-01-01; echo injected"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                runner.selected_dates(value, ROOT)

    def test_manifest_uses_only_downloaded_dates(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bronze/netex/kol/manifest.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({"records": [
                {"status": "downloaded", "target_date": "2026-01-08"},
                {"status": "failed", "target_date": "2026-01-15"},
            ]}))
            self.assertEqual(runner.selected_dates("manifest", Path(tmp)), ["2026-01-08"])

    def test_full_plan_runs_every_layer_in_order_with_bounded_silver_batches(self):
        dates = ["2026-01-01", "2026-01-08", "2026-01-15", "2026-01-22", "2026-01-29", "2026-04-01"]
        commands, gold, silver = self.plan(dates=dates)
        self.assertEqual(len([c for c in commands if c.name.startswith("bronze-")]), 12)
        staging = [c for c in commands if c.name.startswith("staging-")]
        self.assertEqual([c.name for c in staging], ["staging-q1", "staging-q2"])
        for c in staging:
            self.assertIn("batch-2026-q", c.argv[c.argv.index("--output-dir") + 1])
            self.assertEqual(c.argv[c.argv.index("--driver-memory") + 1], "2g")
        batches = [c for c in commands if c.name.startswith("dev-silver-")]
        self.assertEqual([len(c.argv[c.argv.index("--dates") + 1].split(",")) for c in batches], [5, 1])
        self.assertEqual([c.name for c in commands[-2:]], ["gold", "gui-smoke"])
        self.assertTrue(gold.is_relative_to(Path("/srv/dat535/data/dev")))
        self.assertTrue(silver.is_relative_to(Path("/srv/dat535/data/dev")))
        self.assertFalse(any("--force" in c.argv or "rm" in c.argv for c in commands))

    def test_downstream_start_reuses_only_published_upstream_inputs(self):
        for start in ("staging", "silver", "gold"):
            with self.subTest(start=start):
                commands, _, silver = self.plan(start)
                self.assertFalse(any(c.name.startswith("bronze-") for c in commands))
                if start in ("silver", "gold"):
                    self.assertFalse(any(c.name.startswith("staging-") for c in commands))
                if start == "gold":
                    self.assertEqual(silver, Path("/srv/dat535/data/silver/kol/runs"))
                    self.assertEqual([c.name for c in commands], ["gold", "gui-smoke"])
                if start == "silver":
                    c = commands[0]
                    self.assertEqual(c.argv[c.argv.index("--staging-runs-dir") + 1],
                                     "/srv/dat535/data/staging/netex/kol/runs")

    def test_published_directories_cannot_be_used_as_output_roots(self):
        for root in ("/srv/dat535/data", "/srv/dat535", "/srv/dat535/data/bronze/test",
                     "/srv/dat535/data/silver/kol/runs/test", "/srv/dat535/data/gold/kol/runs/test"):
            with self.subTest(root=root), self.assertRaises(ValueError):
                runner.build_plan(ROOT, Path("/srv/dat535/data"), Path(root), ["2026-01-01"],
                                  "bronze", sys.executable, sys.executable)

    def test_dry_run_has_no_output_or_spark_side_effects(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()) as stdout:
            root = Path(tmp) / "new-run"
            self.assertEqual(runner.main(["--run-root", str(root), "--dates", "2026-01-01", "--dry-run"]), 0)
            self.assertFalse(root.exists())
            plan = json.loads(stdout.getvalue())
            self.assertEqual(plan["commands"][-1]["name"], "gui-smoke")

    def test_failure_preserves_logs_timings_and_stops_downstream_execution(self):
        commands = [runner.Command("bronze", [sys.executable, "-c", "print('downloaded')"]),
                    runner.Command("staging", [sys.executable, "-c", "print('failed'); raise SystemExit(3)"]),
                    runner.Command("silver", [sys.executable, "-c", "print('must not run')"])]
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()), \
                patch.object(runner.subprocess, "check_output", return_value="test-revision\n"):
            with self.assertRaises(subprocess.CalledProcessError):
                runner.execute(commands, ROOT, Path(tmp), dict(os.environ))
            report = json.loads((Path(tmp) / "metadata/run.json").read_text())
            self.assertEqual(report["status"], "failed")
            self.assertEqual([s["name"] for s in report["steps"]], ["bronze", "staging"])
            self.assertEqual(report["steps"][1]["returncode"], 3)
            self.assertTrue(all("elapsed_sec" in s for s in report["steps"]))
            self.assertFalse((Path(tmp) / "metadata/silver.log").exists())


if __name__ == "__main__":
    unittest.main()
