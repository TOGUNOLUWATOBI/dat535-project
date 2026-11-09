import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

from pipeline.analysis.route_history import DEFAULTS, HistoryInputError, read_mappings

JAN, FEB, MAR = "2026-01-15", "2026-02-15", "2026-03-15"
DATES = [JAN, FEB, MAR]

# One fixture network, modelled on the real AKT change of 22 June 2026:
#   AKT:Line:1    bus 1, unchanged stops, departures halve in March (service change only)
#   AKT:Line:1055 bus 55, replaced by AKT:Line:55 (same number, extended route, new ID).
#                 The new ID is published in February, one snapshot before the old one is gone.
#   AKT:Line:1056 bus 56, discontinued in March; stop F was only served by it.
#   AKT:Line:9    bus 9, retired in March; AKT:Line:99 reuses number 9 on unrelated stops.
NETWORK = {
    JAN: [("AKT:Line:1", "1", ["A", "B", "C"], 100),
          ("AKT:Line:1055", "55", ["A", "B", "C", "D"], 40),
          ("AKT:Line:1056", "56", ["C", "F"], 20),
          ("AKT:Line:9", "9", ["P", "Q"], 10)],
    FEB: [("AKT:Line:1", "1", ["A", "B", "C"], 100),
          ("AKT:Line:1055", "55", ["A", "B", "C", "D"], 40),
          ("AKT:Line:55", "55", ["A", "B", "C", "D", "E"], 6),
          ("AKT:Line:1056", "56", ["C", "F"], 20),
          ("AKT:Line:9", "9", ["P", "Q"], 10)],
    MAR: [("AKT:Line:1", "1", ["A", "B", "C"], 50),
          ("AKT:Line:55", "55", ["A", "B", "C", "D", "E"], 44),
          ("AKT:Line:99", "9", ["R", "S"], 12)],
}
MAPPINGS = [{"provider": "AKT", "old_line_id": "AKT:Line:1055", "new_line_id": "AKT:Line:55",
             "to_snapshot": MAR, "verified": True, "verified_by": "test", "note": "fixture"}]


class ReadMappingsTests(unittest.TestCase):
    def test_missing_file_means_no_mappings(self):
        self.assertEqual(read_mappings(None), ([], None))
        self.assertEqual(read_mappings("/nonexistent/route_renumberings.json"), ([], None))

    def test_entries_are_read_and_incomplete_entries_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "map.json"
            path.write_text(json.dumps(MAPPINGS))
            rows, sha = read_mappings(path)
            self.assertEqual(rows, [("AKT", "AKT:Line:1055", "AKT:Line:55", True, "test", "fixture", None, MAR)])
            self.assertEqual(len(sha), 64)
            path.write_text(json.dumps([{"provider": "AKT", "old_line_id": "AKT:Line:1"}]))
            with self.assertRaises(HistoryInputError):
                read_mappings(path)

    def test_project_mapping_file_is_valid(self):
        rows, _ = read_mappings(Path(__file__).resolve().parents[2] / "config/route_renumberings.json")
        self.assertTrue(rows)
        self.assertNotIn("AKT:Line:1056", {r[1] for r in rows}, "Line 56 was discontinued, not renumbered")


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "Install processing dependencies for Spark integration")
class RouteHistorySparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.pop("SPARK_HOME", None)
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[2]").appName("DAT535-route-history-tests")
                     .config("spark.ui.enabled", "false").config("spark.ui.showConsoleProgress", "false")
                     .config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "Europe/Oslo")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # ---- helpers --------------------------------------------------------

    def lines_frame(self):
        from pipeline.analysis.route_history import finish_lines
        rows = [(day, "AKT", line, [code], [f"Route {code}"], ["bus"], ["Line"], [f"p-{line}-{len(stops)}"],
                 sorted(stops), 1, deps, 7, 6 * 3600, 22 * 3600)
                for day, entries in NETWORK.items() for line, code, stops, deps in entries]
        schema = ("snapshot_date string, provider string, line_id string, public_codes array<string>, "
                  "names array<string>, transport_modes array<string>, line_kinds array<string>, "
                  "pattern_signatures array<string>, stop_ids array<string>, journeys_published long, "
                  "window_departures long, days_with_service long, first_departure_seconds int, "
                  "last_departure_seconds int")
        return finish_lines(self.spark.createDataFrame(rows, schema))

    def mapping_rows(self):
        return [("AKT", m["old_line_id"], m["new_line_id"], True, m["verified_by"], m["note"], None, m["to_snapshot"])
                for m in MAPPINGS]

    def candidates(self, options=None):
        from pipeline.analysis.route_history import detect_changes, renumbering_candidates
        options = {**DEFAULTS, **(options or {})}
        lines = self.lines_frame()
        transitions, _ = detect_changes(self.spark, lines, DATES, options)
        return renumbering_candidates(self.spark, transitions, options, self.mapping_rows(), lines, DATES)

    # ---- tests ----------------------------------------------------------

    def test_changes_between_snapshots(self):
        from pipeline.analysis.route_history import detect_changes
        transitions, changes = detect_changes(self.spark, self.lines_frame(), DATES, DEFAULTS)
        status = {(r.to_snapshot, r.line_id): r.status for r in transitions.collect()}
        self.assertEqual(status[(FEB, "AKT:Line:55")], "added")
        self.assertEqual(status[(MAR, "AKT:Line:1055")], "retired")
        self.assertEqual(status[(MAR, "AKT:Line:1056")], "retired")
        self.assertEqual(status[(MAR, "AKT:Line:1")], "service_changed")
        self.assertEqual(status[(FEB, "AKT:Line:1")], "unchanged")
        kinds = {(r.to_snapshot, r.line_id, r.change_type) for r in changes.collect()}
        self.assertIn((MAR, "AKT:Line:1", "departures_decreased"), kinds)
        self.assertIn((MAR, "AKT:Line:55", "departures_increased"), kinds)
        self.assertNotIn((FEB, "AKT:Line:1", "departures_decreased"), kinds)

    def test_replacement_published_one_snapshot_early_is_a_candidate(self):
        rows = {(r.old_line_id, r.new_line_id): r for r in self.candidates().collect()}
        pair = rows[("AKT:Line:1055", "AKT:Line:55")]
        self.assertEqual((pair.to_snapshot, pair.new_line_since, pair.match_type),
                         (MAR, FEB, "same_code_new_line_id"))
        self.assertTrue(pair.verified)
        self.assertEqual(pair.stop_similarity, 0.8)
        # Comparing only with lines added in the same transition would miss the early replacement.
        from pipeline.analysis.route_history import detect_changes, renumbering_candidates
        transitions, _ = detect_changes(self.spark, self.lines_frame(), DATES, DEFAULTS)
        same_transition_only = renumbering_candidates(self.spark, transitions, DEFAULTS, self.mapping_rows())
        self.assertNotIn(("AKT:Line:1055", "AKT:Line:55"),
                         {(r.old_line_id, r.new_line_id) for r in same_transition_only.collect()})

    def test_reused_bus_number_on_unrelated_stops_is_not_a_candidate(self):
        pairs = {(r.old_line_id, r.new_line_id) for r in self.candidates().collect()}
        self.assertNotIn(("AKT:Line:9", "AKT:Line:99"), pairs)
        self.assertNotIn(("AKT:Line:1056", "AKT:Line:55"), pairs)
        loose = {(r.old_line_id, r.new_line_id) for r in self.candidates({"same_code_similarity": 0.0}).collect()}
        self.assertIn(("AKT:Line:9", "AKT:Line:99"), loose)

    def test_verified_mapping_is_kept_when_automatic_rules_miss_it(self):
        from pipeline.analysis.route_history import detect_changes, renumbering_candidates
        lines = self.lines_frame()
        transitions, _ = detect_changes(self.spark, lines, DATES, DEFAULTS)
        rows = self.mapping_rows() + [("AKT", "AKT:Line:9", "AKT:Line:99", True, "test", "announced", None, MAR),
                                      ("AKT", "AKT:Line:9", "AKT:Line:99", True, "test", "wrong month", None, FEB)]
        found = renumbering_candidates(self.spark, transitions, DEFAULTS, rows[:2], lines, DATES).collect()
        manual = [r for r in found if r.match_type == "verified_mapping"]
        self.assertEqual([(r.old_line_id, r.new_line_id, r.from_snapshot, r.to_snapshot, r.verified, r.stop_similarity)
                          for r in manual], [("AKT:Line:9", "AKT:Line:99", FEB, MAR, True, 0.0)])
        self.assertEqual(sum(r.match_type != "verified_mapping" for r in found), len(self.candidates().collect()))
        # A mapping that names a snapshot where the new line does not exist yet stays unmatched.
        misplaced = renumbering_candidates(self.spark, transitions, DEFAULTS, [rows[2]], lines, DATES).collect()
        self.assertNotIn("verified_mapping", {r.match_type for r in misplaced})

    def test_versions_link_verified_renumbering_and_cover_every_snapshot(self):
        from pipeline.analysis.route_history import build_versions
        lines = self.lines_frame()
        versions, covered = build_versions(lines, DATES, self.candidates())
        rows = {(r.line_id, r.version_number): r for r in versions.collect()}
        new55 = rows[("AKT:Line:55", 1)]
        self.assertEqual((new55.valid_from, new55.last_observed, new55.is_current), (FEB, MAR, True))
        self.assertEqual(new55.renumbered_from_line_id, "AKT:Line:1055")
        old55 = rows[("AKT:Line:1055", 1)]
        self.assertEqual((old55.superseded_in, old55.is_current, old55.renumbered_from_line_id), (MAR, False, None))
        self.assertEqual(rows[("AKT:Line:1", 1)].snapshots_observed, 3, "Departure changes do not create versions")
        self.assertEqual(sum(r.snapshots_observed for r in rows.values()), lines.count())
        self.assertEqual(covered.count(), lines.count())

    def test_structure_change_creates_a_new_version(self):
        from pipeline.analysis.route_history import build_versions, finish_lines
        base = self.lines_frame().where("line_id = 'AKT:Line:1'").drop(
            "public_code", "name", "transport_mode", "stop_count", "pattern_count", "stop_set_hash",
            "pattern_set_hash", "has_conflicting_definitions", "attribute_hash")
        from pyspark.sql import functions as F
        moved = finish_lines(base.withColumn("stop_ids", F.when(F.col("snapshot_date") == MAR,
                                                                 F.array(*[F.lit(s) for s in "ABCX"]))
                                             .otherwise(F.col("stop_ids"))))
        versions, _ = build_versions(moved, DATES)
        found = sorted((r.version_number, r.valid_from, r.changed_after, r.version_reason) for r in versions.collect())
        self.assertEqual(found, [(1, JAN, None, "first_seen"), (2, MAR, FEB, "structure_changed")])

    def test_full_history_run_reconciles_with_gold(self):
        from pipeline.analysis.route_history import build_history
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            inputs = [self.write_snapshot(root, day) for day in DATES]
            mapping = root / "map.json"
            mapping.write_text(json.dumps(MAPPINGS))
            report = build_history(self.spark, inputs, root / "history", mappings_path=mapping, identity_key="test")
            self.assertEqual(report["status"], "complete")
            checks = report["checks"]
            self.assertTrue(all(s["ok"] for s in checks["snapshots"]))
            gold = sum(d for entries in NETWORK.values() for _, _, _, d in entries)
            self.assertEqual(checks["service_date_departures"], {"gold": gold, "versioned": gold})
            self.assertEqual(checks["service_dates_without_version"], 0)
            self.assertEqual((checks["verified_mappings_matched"], checks["verified_mappings_unmatched"]), (1, 0))
            summary = {r["to_snapshot"]: r for r in report["summary"]}
            self.assertEqual((summary[MAR]["lines_added"], summary[MAR]["lines_retired"]), (1, 3))
            lost = self.spark.read.parquet(str(root / "history/stop_coverage_changes")).where(
                "change_type = 'stop_lost_service'").collect()
            self.assertIn(("NSR:StopPlace:F", "Stop F"), {(r.stop_id, r.stop_name) for r in lost})
            self.assertTrue((root / "history/metadata/inventory.json").is_file())

    # ---- Silver/Gold fixture writer --------------------------------------

    def write_snapshot(self, root, day):
        silver, gold = root / "silver" / day, root / "gold" / day
        entries = NETWORK[day]
        stop = lambda s: f"NSR:StopPlace:{s}"
        tables = {
            (silver, "lines"): ([("AKT", line, f"Route {code}", code, "bus", "Line") for line, code, _, _ in entries],
                                "provider string, entity_id string, name string, public_code string, "
                                "transport_mode string, line_kind string"),
            (silver, "journeys"): ([(f"j-{line}", "AKT", line, True) for line, _, _, _ in entries],
                                   "journey_key string, provider string, line_entity_id string, is_valid boolean"),
            (silver, "passing_times"): ([(f"j-{line}", i, stop(s), None, None, None, True)
                                         for line, _, stops, _ in entries for i, s in enumerate(stops)],
                                        "journey_key string, passing_time_index int, quay_stop_place_id string, "
                                        "assignment_flexible_stop_place_ref string, assignment_quay_ref string, "
                                        "point_scheduled_stop_ref string, is_valid boolean"),
            (silver, "stop_places"): ([(stop(s), f"Stop {s}") for s in "ABCDEFPQRS"],
                                      "stop_place_id string, name string"),
            (gold, "line_window_summary"): ([("AKT", line, deps, 2, 6 * 3600, 22 * 3600) for line, _, _, deps in entries],
                                            "provider string, line_entity_id string, scheduled_departures long, "
                                            "days_with_service long, earliest_departure_seconds int, "
                                            "latest_departure_seconds int"),
            (gold, "daily_line_service"): ([("AKT", line, f"{day[:8]}{d}", part, 6 * 3600, 22 * 3600)
                                            for line, _, _, deps in entries
                                            for d, part in (("15", deps // 2), ("16", deps - deps // 2))],
                                           "provider string, line_entity_id string, service_date string, "
                                           "scheduled_departures long, first_departure_seconds int, "
                                           "last_departure_seconds int"),
        }
        for (base, name), (rows, schema) in tables.items():
            self.spark.createDataFrame(rows, schema).coalesce(1).write.parquet(str(base / name))
        for base, names in ((silver, ["lines", "journeys", "passing_times", "stop_places"]),
                            (gold, ["line_window_summary", "daily_line_service"])):
            (base / "metadata").mkdir()
            (base / "metadata/manifest.json").write_text(json.dumps(
                {"tables": {n: {"path": n} for n in names}, "service_from": day, "service_to": f"{day[:8]}21"}))
        return {"snapshot_date": day, "providers": ["AKT"], "gold": str(gold), "silver": str(silver),
                "gold_manifest_sha256": "g", "silver_manifest_sha256": "s",
                "service_from": day, "service_to": f"{day[:8]}21"}


if __name__ == "__main__":
    unittest.main()
