import importlib.util
import os
from pathlib import Path
import tempfile
import unittest

from pipeline.rebuild_monthly import run_worker
from pipeline.storage.zip_bronze import retain_snapshot
from tests.processing.test_snapshot_pipeline import complete_fixture


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "Install processing dependencies for Spark integration")
class BatchWorkerSparkTests(unittest.TestCase):
    def test_fresh_jvms_use_the_heap_budget_and_reuse_verified_zip_backed_checkpoints(self):
        java = Path("/Users/bebs/Library/Java/JavaVirtualMachines/ms-17.0.15/Contents/Home")
        if java.exists():
            os.environ.setdefault("JAVA_HOME", str(java))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = complete_fixture(root / "bronze", full_month=True)
            retain_snapshot(manifest, apply=True)
            job = {"manifest": str(manifest), "snapshot_date": "2026-01-15", "history_role": "primary"}
            first = run_worker("snapshot", root / "first.json", root / "outputs", "local[2]", "2g", job)
            second = run_worker("snapshot", root / "second.json", root / "outputs", "local[2]", "2g", job)
            self.assertNotEqual(first["pid"], second["pid"])
            self.assertEqual(first["runtime"]["driver_memory"], "2g")
            self.assertEqual(second["runtime"]["driver_memory"], "2g")
            self.assertGreaterEqual(second["runtime"]["max_heap_bytes"], 1900 * 1024**2)
            self.assertTrue(all(stage["reused"] for stage in second["stages"].values()))
            self.assertEqual(first["stages"]["gold"]["path"], second["stages"]["gold"]["path"])
            self.assertFalse((root / "bronze/xml/2026-01-15").exists())


if __name__ == "__main__":
    unittest.main()
