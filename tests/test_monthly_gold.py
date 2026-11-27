"""Monthly publication policy and durable Gold outputs, without XML or Spark."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests.fixtures import service_fixture


@unittest.skipUnless(importlib.util.find_spec("pandas") and importlib.util.find_spec("pyarrow"),
                     "Install requirements.txt for monthly Gold tests")
class MonthlyGoldTests(unittest.TestCase):
    def setUp(self):
        self.daily, self.items = service_fixture()
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))

    def register(self, daily=None, items=None, version="fixture"):
        from pipeline.checkpoints import Checkpoints
        from pipeline.common import write_json
        daily = self.daily if daily is None else daily
        items = self.items if items is None else items
        state = Checkpoints(self.root / "pipeline_state.sqlite3")
        try:
            for item in items:
                key = state.register({"snapshot_date": item["snapshot_date"], "providers": ["AKT", "KOL"],
                                      "source_manifest": "unchanged-bronze.json", "files": [], "stop_files": []})

                def save(output):
                    (output / "metadata").mkdir(parents=True)
                    (output / "daily_line_service").mkdir()
                    part = daily[daily.snapshot_date == item["snapshot_date"]]
                    part.to_parquet(output / "daily_line_service/part-00000.parquet", index=False)
                    write_json(output / "metadata/manifest.json", {**item["gold_manifest"],
                        "status": "complete", "snapshot_date": item["snapshot_date"],
                        "service_from": item["service_from"], "service_to": item["service_to"],
                        "quality_gate": {"status": "pass"},
                        "tables": {"daily_line_service": {"path": "daily_line_service", "rows": len(part)}}})
                with state.lock(key):
                    state.execute(key, "gold", key, version, self.root / "gold/entur" / key[:16], save)
        finally:
            state.close()

    def test_overlap_chooses_whole_export_without_backfill_or_filter_dependence(self):
        import pandas as pd
        from pipeline.processing.monthly import select_plans
        newer = dict(self.items[0], snapshot_date="2026-01-16", service_from="2026-01-16", service_to="2026-01-22")
        future = dict(newer, snapshot_date="2026-01-25", service_from="2026-01-15", service_to="2026-01-31")
        future["gold_manifest"] = {"providers": ["AKT", "KOL"], "source_calendar_coverage": {
            p: {"from": "2026-01-25", "to": "2026-01-31"} for p in ("AKT", "KOL")}}
        row = self.daily.iloc[[0]].assign(snapshot_date="2026-01-16", service_date="2026-01-16", scheduled_departures=100)
        hindsight = row.assign(snapshot_date="2026-01-25", service_date="2026-01-15", scheduled_departures=777)
        lines, calendar = select_plans(pd.concat([self.daily, row, hindsight]), [*self.items, newer, future])
        dates = pd.to_datetime(lines.service_date).dt.strftime("%Y-%m-%d")
        departures = lines.groupby(dates).scheduled_departures.sum()
        self.assertEqual(departures["2026-01-15"], 17)
        self.assertEqual(departures["2026-01-16"], 100)
        self.assertEqual(lines[dates == "2026-01-16"].line_entity_id.nunique(), 1)
        coverage = calendar.assign(service_date=pd.to_datetime(calendar.service_date).dt.strftime("%Y-%m-%d"))
        coverage = coverage.set_index("service_date")
        self.assertFalse(coverage.covered.get("2026-01-23", False))
        self.assertEqual(coverage.loc["2026-01-16", "source_export"], "2026-01-16")
        self.assertEqual(lines[(dates == "2026-01-16") & (lines.provider == "KOL")].scheduled_departures.sum(), 0)

    def test_coverage_requires_both_providers_and_includes_days_before_publication(self):
        from pipeline.processing.monthly import select_plans
        item = {**self.items[0], "service_from": "2026-01-01", "service_to": "2026-01-31"}
        _, calendar = select_plans(self.daily[self.daily.snapshot_date == item["snapshot_date"]], [item])
        self.assertEqual(calendar.covered.sum(), 31)
        self.assertTrue((calendar.source_export == "2026-01-15").all())
        item["gold_manifest"] = {"providers": ["AKT", "KOL"], "source_calendar_coverage": {
            "AKT": {"from": "2026-01-01", "to": "2026-01-31"},
            "KOL": {"from": "2026-01-15", "to": "2026-01-21"}}}
        _, calendar = select_plans(self.daily[self.daily.snapshot_date == item["snapshot_date"]], [item])
        self.assertEqual(calendar.covered.sum(), 7)

    def test_saved_parquet_reconciles_is_reused_and_needs_no_silver(self):
        import pandas as pd
        from pipeline.processing.monthly import publish, read_monthly
        self.register()
        output, report, reused = publish(self.root)
        self.assertFalse(reused)
        self.assertEqual(report["reconciliation"]["gold_scheduled_departures"], 89)
        lines, coverage = read_monthly(self.root)
        self.assertEqual(int(lines.scheduled_departures.sum()), 89)
        self.assertEqual(len(coverage), 59)
        self.assertEqual(coverage.covered.sum(), 14)
        self.assertFalse((self.root / "silver").exists())
        again, _, reused = publish(self.root)
        self.assertTrue(reused)
        self.assertEqual(again, output)
        pd.testing.assert_frame_equal(lines, read_monthly(self.root)[0])

    def test_new_gold_refuses_stale_monthly_then_publishes_new_output(self):
        from pipeline.processing.monthly import publish, read_monthly
        self.register()
        old, _, _ = publish(self.root)
        changed = self.daily.copy()
        changed.loc[0, "scheduled_departures"] = 20
        self.register(changed, version="corrected")
        with self.assertRaisesRegex(ValueError, "outdated"):
            read_monthly(self.root)
        new, report, reused = publish(self.root)
        self.assertFalse(reused)
        self.assertNotEqual(new, old)
        self.assertTrue(old.exists())
        self.assertEqual(report["reconciliation"]["gold_scheduled_departures"], 99)

    def test_failed_build_keeps_previous_pointer_and_records_failure(self):
        from pipeline.processing.monthly import publish
        self.register()
        publish(self.root)
        pointer = self.root / "gold/entur/monthly/current.json"
        previous = pointer.read_bytes()
        self.register(version="next")
        with patch("pandas.DataFrame.to_parquet", side_effect=OSError("disk full")):
            with self.assertRaisesRegex(OSError, "disk full"):
                publish(self.root)
        self.assertEqual(pointer.read_bytes(), previous)
        reports = [json.loads(p.read_text()) for p in pointer.parent.glob("monthly-*/metadata/manifest.json")]
        self.assertEqual(sorted(m["status"] for m in reports), ["complete", "failed"])

    def test_damaged_monthly_is_refused_then_rebuilt_without_overwriting(self):
        from pipeline.processing.monthly import publish, read_monthly
        self.register()
        old, _, _ = publish(self.root)
        parquet = old / "daily_line_service/part-00000.parquet"
        parquet.write_bytes(b"damaged")
        with self.assertRaisesRegex(ValueError, "saved hashes"):
            read_monthly(self.root)
        new, _, reused = publish(self.root)
        self.assertFalse(reused)
        self.assertNotEqual(new, old)
        self.assertEqual(parquet.read_bytes(), b"damaged")
        self.assertEqual(int(read_monthly(self.root)[0].scheduled_departures.sum()), 89)

    def test_damaged_source_checkpoint_is_not_accepted_as_new_data(self):
        from pipeline.processing.monthly import publish, snapshots
        self.register()
        output = Path(snapshots(self.root)[0]["gold"])
        (output / "daily_line_service/part-00000.parquet").write_bytes(b"damaged")
        with self.assertRaisesRegex(ValueError, "checkpoint"):
            publish(self.root)
        self.assertFalse((self.root / "gold/entur/monthly/current.json").exists())

    def test_inputs_changing_during_build_prevent_publication(self):
        from pipeline.processing.monthly import publish, snapshots
        self.register()
        items = snapshots(self.root, verify=True)
        with patch("pipeline.processing.monthly.snapshots", side_effect=[items, []]):
            with self.assertRaisesRegex(RuntimeError, "changed during monthly processing"):
                publish(self.root)
        base = self.root / "gold/entur/monthly"
        self.assertFalse((base / "current.json").exists())
        report = json.loads(next(base.glob("monthly-*/metadata/manifest.json")).read_text())
        self.assertEqual(report["status"], "failed")

    def test_missing_publication_shows_actionable_gui_error_without_writing(self):
        from pipeline.processing.monthly import read_monthly
        with self.assertRaisesRegex(ValueError, "scripts/run.sh monthly"):
            read_monthly(self.root)
        self.assertFalse((self.root / "gold").exists())


if __name__ == "__main__":
    unittest.main()
