"""Current quarterly input selection, date authority and provenance checks."""

import hashlib
from pathlib import Path
import tempfile
import unittest

from pipeline.Silver.input_selection import discover_siri_et_files, parse_iso_date_str, select_inputs
from tests.fixtures import write_json


class SilverInputTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.staging, self.siri = self.root / "staging", self.root / "siri"
        self.staging.mkdir()
        self.siri.mkdir()

    def stage(self, name="batch-2026-q1", days=("2026-01-01",), **extra):
        return write_json(self.staging / name / "staging_manifest.json", {
            "status": "complete", "total_failures": 0, "requested_dates": list(days), **extra})

    def select(self, days=("2026-01-01",)):
        return select_inputs(days, staging_runs_dir=self.staging, siri_et_dir=self.siri)

    def test_selection_binds_date_to_siri_hash_and_ignores_diagnostic_runs(self):
        self.stage()
        self.stage("diagnostic-copy")
        path = self.siri / "kol_2026-01-01.parquet"
        path.write_bytes(b"sample bytes for provenance hashing")
        result = self.select()
        self.assertEqual(result.hard_failures, ())
        self.assertEqual(result.selected_staging_runs, ("batch-2026-q1",))
        self.assertIn("diagnostic-copy", result.ignored_run_dirs)
        record, = result.records
        self.assertEqual(record.selection_status, "selected_verified")
        self.assertEqual(record.siri_source_sha256, hashlib.sha256(path.read_bytes()).hexdigest())
        self.assertEqual(record.operating_date, "2026-01-01")

    def test_missing_siri_is_explicit_and_does_not_remove_planned_day(self):
        self.stage()
        result = self.select()
        self.assertEqual(result.hard_failures, ())
        self.assertEqual(len(result.records), 1)
        self.assertFalse(result.records[0].siri_file_available)
        self.assertIn("missing_siri_et_file", result.records[0].issue_codes)
        self.assertEqual(len(result.warnings), 1)

    def test_missing_and_duplicate_planned_dates_are_hard_failures(self):
        missing = self.select()
        self.assertIn("missing_netex_snapshot", missing.hard_failures[0])
        self.stage()
        self.stage("batch-2026-q2")
        duplicate = self.select()
        self.assertIn("duplicate_snapshot_across_runs", duplicate.hard_failures[0])
        self.assertEqual(duplicate.records[0].selection_status, "duplicate_snapshot_error")

    def test_incomplete_or_failed_staging_cannot_be_selected(self):
        for extra in ({"status": "running"}, {"total_failures": 1}):
            with self.subTest(extra=extra):
                self.stage(**extra)
                with self.assertRaises(RuntimeError):
                    self.select()

    def test_duplicate_siri_files_fail_instead_of_choosing_arbitrarily(self):
        for name in ("kol_2026-01-01.parquet", "copy_2026-01-01.parquet"):
            (self.siri / name).write_bytes(b"content")
        with self.assertRaisesRegex(RuntimeError, "Duplicate SIRI"):
            discover_siri_et_files(self.siri)

    def test_invalid_dates_are_rejected(self):
        for value in ("2026-02-30", "01-01-2026", "2026-1-1"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_iso_date_str(value)
