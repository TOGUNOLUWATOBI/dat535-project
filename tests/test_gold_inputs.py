"""Real temporary Silver manifests and Parquet reads; no published VM data."""

from datetime import date
import hashlib
from pathlib import Path
import tempfile
import unittest

from tests.fixtures import SparkTestCase, available, silver_run


@unittest.skipUnless(available("pyspark"), "Install requirements.txt for Gold imports")
class GoldInventoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pipeline.Gold.inputs import build_silver_inventory
        cls.build = staticmethod(build_silver_inventory)

    def test_complete_authority_has_manifest_hash_and_ignores_diagnostic_copies(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            run = silver_run(root)
            silver_run(root, "diagnostic-copy")
            result = self.build(silver_runs_dir=root, expected_dates=["2026-01-01"])
            authority, = result.authorities
            self.assertEqual(authority.source_silver_run_id, run.name)
            self.assertEqual(authority.silver_manifest_sha256,
                             hashlib.sha256((run / "silver_manifest.json").read_bytes()).hexdigest())
            self.assertEqual(result.excluded_runs[0]["reason"], "excluded_by_prefix")

    def test_missing_failed_and_duplicate_date_authorities_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            with self.assertRaisesRegex(ValueError, "Missing authoritative Silver"):
                self.build(silver_runs_dir=root, expected_dates=["2026-01-01"])
            silver_run(root, gate="failed")
            with self.assertRaisesRegex(ValueError, "gate_status"):
                self.build(silver_runs_dir=root, expected_dates=["2026-01-01"])
            silver_run(root)
            silver_run(root, "second-run")
            with self.assertRaisesRegex(ValueError, "Duplicate Silver date authority"):
                self.build(silver_runs_dir=root, expected_dates=["2026-01-01"])
            selected = self.build(silver_runs_dir=root, expected_dates=["2026-01-01"],
                                  allowed_run_ids=["second-run"])
            self.assertEqual(selected.authorities[0].source_silver_run_id, "second-run")


class GoldReaderTests(SparkTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.run = silver_run(self.root)
        from pipeline.Gold.inputs import build_silver_inventory
        self.inventory = build_silver_inventory(silver_runs_dir=self.root, expected_dates=["2026-01-01"])
        self.spark.createDataFrame([
            ("KOL", "2026-01-01", "pub-1", "keep"),
            ("KOL", "2026-01-02", "pub-2", "wrong-date"),
            ("OTHER", "2026-01-01", "pub-1", "wrong-provider"),
        ], "provider string, snapshot_date string, publication_id string, value string").write.parquet(str(self.run / "sample"))

    def test_derived_operating_date_and_lineage_are_available(self):
        from pipeline.Gold.inputs import read_silver_table_for_inventory
        frame = read_silver_table_for_inventory(self.spark, self.inventory, "sample",
                    ["provider", "operating_date", "publication_id", "value"])
        row, = frame.collect()
        self.assertEqual(row.value, "keep")
        self.assertEqual(row.operating_date, date(2026, 1, 1))
        self.assertEqual(row.source_silver_run_id, self.run.name)

    def test_missing_requested_columns_fail_explicitly(self):
        from pipeline.Gold.inputs import read_silver_table_for_inventory
        with self.assertRaisesRegex(KeyError, "missing_column"):
            read_silver_table_for_inventory(self.spark, self.inventory, "sample", ["missing_column"])

    @unittest.expectedFailure
    def test_authority_filters_apply_even_when_filter_columns_are_not_projected(self):
        """Known defect: Gold currently projects away its date/provider filters."""
        from pipeline.Gold.inputs import read_silver_table_for_inventory
        frame = read_silver_table_for_inventory(self.spark, self.inventory, "sample", ["value"])
        self.assertEqual([row.value for row in frame.collect()], ["keep"])
