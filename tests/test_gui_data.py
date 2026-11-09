import importlib.util
import unittest


@unittest.skipUnless(importlib.util.find_spec("pandas"), "pandas is needed for the GUI data helpers")
class GuiDataTests(unittest.TestCase):
    def test_clock_keeps_overnight_times_on_the_operating_day(self):
        from app.data import clock
        self.assertEqual(clock(6 * 3600 + 5 * 60), "06:05")
        self.assertEqual(clock(86400 + 15 * 60), "00:15 +1d")
        self.assertEqual(clock(None), "–")

    def test_gate_status_reports_rules_and_older_outputs(self):
        from app.data import gate_status
        self.assertEqual(gate_status({}), "not recorded")
        self.assertEqual(gate_status({"quality_gate": {"status": "pass", "findings": []}}), "pass")
        warn = {"quality_gate": {"status": "warn", "findings": [{"rule": "calendar_issues"}, {"rule": "calendar_issues"}]}}
        self.assertEqual(gate_status(warn), "warn (calendar_issues)")

    def test_evidence_files_load_with_sources(self):
        from app.data import evidence
        renumberings, discontinuations = evidence()
        self.assertTrue(renumberings.source_url.str.startswith("https://").all())
        self.assertIn("AKT:Line:1056", set(discontinuations.line_id))


if __name__ == "__main__":
    unittest.main()
