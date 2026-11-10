import json
from pathlib import Path
import tempfile
import unittest

from pipeline.quality.silver_profile import DEFAULT_THRESHOLDS, evaluate, read_thresholds


def profile(journeys=100, quarantined=0, ambiguous=0, eligible=50, calendar=None, essential_missing=0):
    return {"providers": ["AKT"], "expected_providers": ["AKT"], "observed_providers": ["AKT"],
            "volumes": {"AKT": {"lines_staging": 10, "lines_silver": 10,
                                "journeys_staging": journeys, "passing_times_staging": 1000,
                                "journeys_silver": journeys, "journeys_quarantined": quarantined,
                                "passing_times_silver": 1000, "passing_times_quarantined": 0}},
            "service": {"AKT": {"service_occurrences": eligible, "eligible_fixed": eligible, "rejected_invalid": 0}},
            "missing": {"AKT": {"lines": 10, "essential_physical_stop": essential_missing, "optional_stop_name": 999}},
            "references": {"AKT": {"ambiguous_line": ambiguous, "unresolved_line": 0}},
            "duplicates": {"_all": {"quays_conflicting_withheld": 0}},
            "calendar": {"AKT": calendar or {}}}


class QualityGateTests(unittest.TestCase):
    def test_clean_snapshot_passes(self):
        self.assertEqual(evaluate(profile())["status"], "pass")

    def test_any_rejection_warns_and_more_than_one_percent_fails(self):
        self.assertEqual(evaluate(profile(quarantined=1, journeys=1000))["status"], "warn")
        result = evaluate(profile(quarantined=2, journeys=100))
        self.assertEqual(result["status"], "fail")
        self.assertEqual([(f["rule"], f["level"]) for f in result["findings"]], [("journey_quarantine_rate", "fail")])

    def test_ambiguous_references_and_empty_service_fail(self):
        self.assertEqual(evaluate(profile(ambiguous=1))["status"], "fail")
        self.assertEqual(evaluate(profile(eligible=0))["findings"][0]["rule"], "eligible_service_occurrences_min")

    def test_optional_fields_do_not_count_but_essential_ones_do(self):
        self.assertEqual(evaluate(profile())["metrics"]["AKT"]["essential_missing_rate"], 0.0)
        self.assertEqual(evaluate(profile(essential_missing=50))["status"], "fail")

    def test_calendar_issues_warn(self):
        self.assertEqual(evaluate(profile(calendar={"unsupported_rule": 2}))["status"], "warn")

    def test_empty_or_missing_selection_cannot_pass(self):
        empty = profile()
        empty.update(providers=[], expected_providers=[], observed_providers=[])
        self.assertEqual(evaluate(empty)["status"], "fail")
        del empty["expected_providers"]
        self.assertEqual(evaluate(empty)["status"], "fail")

    def test_missing_provider_fails_even_if_empty_windows_are_allowed(self):
        sample = profile()
        sample["expected_providers"] = ["AKT", "KOL"]
        result = evaluate(sample, {"eligible_service_occurrences_min": {"fail_below": 0}})
        self.assertEqual(result["status"], "fail")
        self.assertIn(("KOL", "missing_expected_provider"),
                      {(f["provider"], f["rule"]) for f in result["findings"]})
        sample.update(providers=["AKT", "KOL"], observed_providers=[])
        self.assertEqual(evaluate(sample)["status"], "fail")

    def test_unexpected_provider_fails(self):
        sample = profile()
        sample["observed_providers"].append("OTHER")
        self.assertEqual(evaluate(sample)["findings"][0]["rule"], "unexpected_provider")

    def test_source_rows_cannot_disappear_even_when_service_counts_reconcile(self):
        sample = profile()
        sample["volumes"]["AKT"]["journeys_staging"] += 1
        self.assertEqual(evaluate(sample)["status"], "fail")

    def test_calendar_impact_uses_journeys_before_date_expansion(self):
        sample = profile(calendar={"unsupported_rule": 1})
        sample["calendar_impact"] = {"AKT": {"affected_journeys": 2}}
        result = evaluate(sample)
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["metrics"]["AKT"]["calendar_affected_journey_rate"], 0.02)

    def test_threshold_file_is_validated(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "rules.json"
            path.write_text(json.dumps({"rules": {"journey_quarantine_rate": {"warn": 0.0, "fail": 0.5}}}))
            self.assertEqual(read_thresholds(path)["journey_quarantine_rate"]["fail"], 0.5)
            path.write_text(json.dumps({"rules": {"journey_quarantine": {"fail": 0.5}}}))
            with self.assertRaises(ValueError):
                read_thresholds(path)
        self.assertEqual(read_thresholds(None), DEFAULT_THRESHOLDS)

    def test_project_threshold_file_matches_defaults(self):
        path = Path(__file__).resolve().parents[2] / "config/quality_thresholds.json"
        self.assertEqual(read_thresholds(path), DEFAULT_THRESHOLDS)


if __name__ == "__main__":
    unittest.main()
