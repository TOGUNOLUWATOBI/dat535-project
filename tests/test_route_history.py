import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from pipeline.analysis.route_history import DEFAULTS, HistoryInputError, read_discontinuations, read_mappings
from tests.fixtures import spark_session

JAN, FEB, MAR = "2026-01-15", "2026-02-15", "2026-03-15"
DATES = [JAN, FEB, MAR]
URL = "https://example.org/notice"

# One fixture network, modelled on the real AKT change of 22 June 2026:
#   AKT:Line:1    bus 1, unchanged stops, departures halve in March (service change only)
#   AKT:Line:1055 bus 55, replaced by AKT:Line:55 (same number, extended route, new ID).
#                 The new ID is published in February, one snapshot before the old one is gone.
#   AKT:Line:1056 bus 56, discontinued in March; stop F was only served by it.
#   AKT:Line:9    bus 9, gone in March; AKT:Line:99 reuses number 9 on unrelated stops.
#   AKT:Line:B1   a boat, unchanged; excluded from bus-only totals.
NETWORK = {
    JAN: [("AKT:Line:1", "1", ["A", "B", "C"], 100),
          ("AKT:Line:1055", "55", ["A", "B", "C", "D"], 40),
          ("AKT:Line:1056", "56", ["C", "F"], 20),
          ("AKT:Line:9", "9", ["P", "Q"], 10),
          ("AKT:Line:B1", "B1", ["W", "X"], 4)],
    FEB: [("AKT:Line:1", "1", ["A", "B", "C"], 100),
          ("AKT:Line:1055", "55", ["A", "B", "C", "D"], 40),
          ("AKT:Line:55", "55", ["A", "B", "C", "D", "E"], 6),
          ("AKT:Line:1056", "56", ["C", "F"], 20),
          ("AKT:Line:9", "9", ["P", "Q"], 10),
          ("AKT:Line:B1", "B1", ["W", "X"], 4)],
    MAR: [("AKT:Line:1", "1", ["A", "B", "C"], 50),
          ("AKT:Line:55", "55", ["A", "B", "C", "D", "E"], 44),
          ("AKT:Line:99", "9", ["R", "S"], 12),
          ("AKT:Line:B1", "B1", ["W", "X"], 4)],
}
WATER = {"AKT:Line:B1"}
MAPPINGS = [{"provider": "AKT", "old_line_id": "AKT:Line:1055", "new_line_id": "AKT:Line:55",
             "from_snapshot": FEB, "to_snapshot": MAR, "verified": True, "verified_by": "test",
             "source_url": URL, "note": "fixture"}]
DISCONTINUED = [{"provider": "AKT", "line_id": "AKT:Line:1056", "to_snapshot": MAR, "verified": True,
                 "verified_by": "test", "source_url": URL, "note": "fixture"}]


def mode(line):
    return "water" if line in WATER else "bus"


class EvidenceFileTests(unittest.TestCase):
    def read(self, reader, entries):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "evidence.json"
            path.write_text(json.dumps(entries))
            return reader(path)

    def test_missing_file_means_no_evidence(self):
        self.assertEqual(read_mappings(None), ([], None))
        self.assertEqual(read_mappings("/nonexistent/route_renumberings.json"), ([], None))

    def test_verified_entry_is_read(self):
        rows, sha = self.read(read_mappings, MAPPINGS)
        self.assertEqual(rows, [("AKT", "AKT:Line:1055", "AKT:Line:55", True, "test", "fixture", FEB, MAR)])
        self.assertEqual(len(sha), 64)

    def test_entry_without_verified_is_an_unverified_candidate(self):
        rows, _ = self.read(read_mappings, [{"provider": "AKT", "old_line_id": "AKT:Line:1",
                                             "new_line_id": "AKT:Line:2"}])
        self.assertIs(rows[0][3], False)

    def test_invalid_entries_are_refused(self):
        base = MAPPINGS[0]
        bad = {
            "string verified": {**base, "verified": "false"},
            "numeric verified": {**base, "verified": 1},
            "no verifier": {k: v for k, v in base.items() if k != "verified_by"},
            "no source": {k: v for k, v in base.items() if k != "source_url"},
            "source not a URL": {**base, "source_url": "AKT website"},
            "no transition": {k: v for k, v in base.items() if k != "to_snapshot"},
            "misspelt field": {**base, "verifed": True},
            "other provider": {**base, "new_line_id": "KOL:Line:55"},
            "bad date": {**base, "to_snapshot": "15.03.2026"},
            "reversed dates": {**base, "from_snapshot": MAR, "to_snapshot": FEB},
            "missing line": {k: v for k, v in base.items() if k != "old_line_id"},
        }
        for label, entry in bad.items():
            with self.subTest(label), self.assertRaises(HistoryInputError):
                self.read(read_mappings, [entry])
        with self.assertRaises(HistoryInputError):
            self.read(read_mappings, [base, dict(base)])
        with self.assertRaises(HistoryInputError):
            self.read(read_discontinuations, [{**DISCONTINUED[0], "verified": "yes"}])

    def test_project_evidence_files_are_valid(self):
        root = Path(__file__).resolve().parents[1] / "config"
        mappings, _ = read_mappings(root / "route_renumberings.json")
        stops, _ = read_discontinuations(root / "route_discontinuations.json")
        self.assertTrue(mappings and all(r[3] for r in mappings))
        self.assertNotIn("AKT:Line:1056", {r[1] for r in mappings}, "Line 56 was discontinued, not renumbered")
        self.assertIn("AKT:Line:1056", {r[1] for r in stops})


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "Install processing dependencies for Spark integration")
class RouteHistorySparkTests(unittest.TestCase):
    BROADCAST_KEYS = ("spark.sql.autoBroadcastJoinThreshold", "spark.sql.adaptive.autoBroadcastJoinThreshold")

    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()
        cls.saved = {key: cls.spark.conf.get(key, None) for key in cls.BROADCAST_KEYS}
        from pipeline.analysis.route_history import configure_history_session
        configure_history_session(cls.spark)

    @classmethod
    def tearDownClass(cls):
        # The session is shared: put back the broadcast settings history turned off.
        for key, value in cls.saved.items():
            if value is None:
                cls.spark.conf.unset(key)
            else:
                cls.spark.conf.set(key, value)

    # ---- helpers --------------------------------------------------------

    def lines_frame(self):
        from pipeline.analysis.route_history import finish_lines
        rows = [(day, "AKT", line, [code], [f"Route {code}"], [mode(line)], ["Line"], [f"p-{line}-{len(stops)}"],
                 sorted(stops), sorted(stops), 1, deps, 7, 6 * 3600, 22 * 3600)
                for day, entries in NETWORK.items() for line, code, stops, deps in entries]
        schema = ("snapshot_date string, provider string, line_id string, public_codes array<string>, "
                  "names array<string>, transport_modes array<string>, line_kinds array<string>, "
                  "pattern_signatures array<string>, stop_ids array<string>, window_stop_ids array<string>, "
                  "journeys_published long, window_departures long, days_with_service long, "
                  "first_departure_seconds int, last_departure_seconds int")
        return finish_lines(self.spark.createDataFrame(rows, schema))

    def mapping_rows(self, entries=MAPPINGS):
        return [("AKT", m["old_line_id"], m["new_line_id"], m["verified"], m["verified_by"], m["note"],
                 m.get("from_snapshot"), m["to_snapshot"]) for m in entries]

    def discontinuation_rows(self):
        return [("AKT", d["line_id"], True, d["verified_by"], d["note"], d.get("from_snapshot"), d["to_snapshot"])
                for d in DISCONTINUED]

    def transitions(self):
        from pipeline.analysis.route_history import detect_changes
        return detect_changes(self.spark, self.lines_frame(), DATES, DEFAULTS)[0]

    def candidates(self, options=None, mappings=MAPPINGS):
        from pipeline.analysis.route_history import renumbering_candidates
        options = {**DEFAULTS, **(options or {})}
        return renumbering_candidates(self.spark, self.transitions(), options, self.mapping_rows(mappings),
                                      self.lines_frame(), DATES)

    # ---- change detection and candidates --------------------------------

    def test_changes_between_snapshots(self):
        from pipeline.analysis.route_history import detect_changes
        transitions, changes = detect_changes(self.spark, self.lines_frame(), DATES, DEFAULTS)
        status = {(r.to_snapshot, r.line_id): r.status for r in transitions.collect()}
        self.assertEqual(status[(FEB, "AKT:Line:55")], "appeared")
        self.assertEqual(status[(MAR, "AKT:Line:1055")], "disappeared")
        self.assertEqual(status[(MAR, "AKT:Line:1056")], "disappeared")
        self.assertEqual(status[(MAR, "AKT:Line:1")], "service_changed")
        self.assertEqual(status[(FEB, "AKT:Line:1")], "unchanged")
        kinds = {(r.to_snapshot, r.line_id, r.change_type) for r in changes.collect()}
        self.assertIn((MAR, "AKT:Line:1", "departures_decreased"), kinds)
        self.assertIn((MAR, "AKT:Line:55", "departures_increased"), kinds)
        self.assertIn((MAR, "AKT:Line:1056", "line_disappeared"), kinds)
        self.assertNotIn((FEB, "AKT:Line:1", "departures_decreased"), kinds)

    def test_replacement_published_one_snapshot_early_is_a_candidate(self):
        rows = {(r.old_line_id, r.new_line_id): r for r in self.candidates().collect()}
        pair = rows[("AKT:Line:1055", "AKT:Line:55")]
        self.assertEqual((pair.to_snapshot, pair.new_line_since, pair.match_type),
                         (MAR, FEB, "same_code_new_line_id"))
        self.assertTrue(pair.verified)
        self.assertEqual(pair.stop_similarity, 0.8)
        # Line 55 appeared in FEB, one snapshot before line 1055 disappeared (MAR): only the
        # lookback window pairs them; comparing lines added in the same transition would not.
        self.assertLess(pair.new_line_since, pair.to_snapshot)

    def test_reused_bus_number_on_unrelated_stops_is_not_a_candidate(self):
        pairs = {(r.old_line_id, r.new_line_id) for r in self.candidates().collect()}
        self.assertNotIn(("AKT:Line:9", "AKT:Line:99"), pairs)
        self.assertNotIn(("AKT:Line:1056", "AKT:Line:55"), pairs)
        loose = {(r.old_line_id, r.new_line_id) for r in self.candidates({"same_code_similarity": 0.0}).collect()}
        self.assertIn(("AKT:Line:9", "AKT:Line:99"), loose)

    def test_mapping_applies_only_to_the_transition_it_names(self):
        wrong_start = [{**MAPPINGS[0], "from_snapshot": JAN}]
        rows = {(r.old_line_id, r.new_line_id): r for r in self.candidates(mappings=wrong_start).collect()}
        pair = rows[("AKT:Line:1055", "AKT:Line:55")]
        self.assertFalse(pair.verified, "A mapping for another transition must not verify this one")
        self.assertEqual(pair.match_type, "same_code_new_line_id", "The candidate itself must not be dropped")
        unverified = [{**MAPPINGS[0], "verified": False}]
        self.assertFalse(any(r.verified for r in self.candidates(mappings=unverified).collect()))

    def test_verified_mapping_is_kept_when_automatic_rules_miss_it(self):
        extra = [{**MAPPINGS[0], "old_line_id": "AKT:Line:9", "new_line_id": "AKT:Line:99"}]
        found = self.candidates(mappings=MAPPINGS + extra).collect()
        manual = [r for r in found if r.match_type == "verified_mapping"]
        self.assertEqual([(r.old_line_id, r.new_line_id, r.from_snapshot, r.to_snapshot, r.verified, r.stop_similarity)
                          for r in manual], [("AKT:Line:9", "AKT:Line:99", FEB, MAR, True, 0.0)])
        self.assertEqual(sum(r.match_type != "verified_mapping" for r in found), len(self.candidates().collect()))
        misplaced = [{**extra[0], "from_snapshot": JAN, "to_snapshot": FEB}]
        self.assertNotIn("verified_mapping", {r.match_type for r in self.candidates(mappings=misplaced).collect()})

    # ---- interpretation and versions ------------------------------------

    def test_disappearance_is_only_explained_with_verified_evidence(self):
        from pipeline.analysis.route_history import interpret_transitions
        result = interpret_transitions(self.spark, self.transitions(), self.candidates(), self.discontinuation_rows())
        meaning = {(r.to_snapshot, r.line_id): r.interpretation for r in result.collect()}
        self.assertEqual(meaning[(MAR, "AKT:Line:1055")], "renumbered_verified")
        self.assertEqual(meaning[(MAR, "AKT:Line:1056")], "discontinued_verified")
        self.assertEqual(meaning[(MAR, "AKT:Line:9")], "not_observed")
        self.assertEqual(meaning[(FEB, "AKT:Line:55")], "replacement_verified")
        self.assertEqual(meaning[(MAR, "AKT:Line:99")], "first_observed")
        self.assertIsNone(meaning[(MAR, "AKT:Line:1")])
        bare = interpret_transitions(self.spark, self.transitions(), self.candidates(mappings=[]), [])
        self.assertEqual({r.interpretation for r in bare.where("status = 'disappeared'").collect()}, {"not_observed"})

    def test_versions_use_observation_dates_and_link_verified_renumbering(self):
        from pipeline.analysis.route_history import build_versions
        lines = self.lines_frame()
        versions, covered = build_versions(lines, DATES, self.candidates())
        rows = {(r.line_id, r.version_number): r for r in versions.collect()}
        new55 = rows[("AKT:Line:55", 1)]
        self.assertEqual((new55.first_observed, new55.previous_snapshot, new55.last_observed, new55.observed_in_latest),
                         (FEB, JAN, MAR, True))
        self.assertEqual(new55.renumbered_from_line_id, "AKT:Line:1055")
        old55 = rows[("AKT:Line:1055", 1)]
        self.assertEqual((old55.first_observed, old55.previous_snapshot, old55.version_reason), (JAN, None, "first_observed"))
        self.assertEqual((old55.first_not_observed, old55.observed_in_latest, old55.renumbered_from_line_id),
                         (MAR, False, None))
        self.assertNotIn("valid_from", versions.columns)
        self.assertEqual(rows[("AKT:Line:1", 1)].snapshots_observed, 3, "Departure changes do not create versions")
        self.assertEqual(sum(r.snapshots_observed for r in rows.values()), lines.count())
        self.assertEqual(covered.count(), lines.count())

    def test_structure_change_creates_a_new_version(self):
        from pipeline.analysis.route_history import build_versions, finish_lines
        from pyspark.sql import functions as F
        base = self.lines_frame().where("line_id = 'AKT:Line:1'").select(
            "snapshot_date", "provider", "line_id", "public_codes", "names", "transport_modes", "line_kinds",
            "pattern_signatures", "stop_ids", "window_stop_ids", "journeys_published", "window_departures",
            "days_with_service", "first_departure_seconds", "last_departure_seconds")
        moved = finish_lines(base.withColumn("stop_ids", F.when(F.col("snapshot_date") == MAR,
                                                                 F.array(*[F.lit(s) for s in "ABCX"]))
                                             .otherwise(F.col("stop_ids"))))
        versions, _ = build_versions(moved, DATES)
        found = sorted((r.version_number, r.first_observed, r.previous_snapshot, r.version_reason)
                       for r in versions.collect())
        self.assertEqual(found, [(1, JAN, None, "first_observed"), (2, MAR, FEB, "structure_changed")])

    # ---- full run on Silver/Gold fixtures -------------------------------

    def test_full_history_run_reconciles_with_gold(self):
        from pipeline.analysis.route_history import build_history
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            inputs = [self.write_snapshot(root, day) for day in DATES]
            mapping, stops_file = root / "map.json", root / "stops.json"
            mapping.write_text(json.dumps(MAPPINGS))
            stops_file.write_text(json.dumps(DISCONTINUED))
            report = build_history(self.spark, inputs, root / "history", mappings_path=mapping,
                                   identity_key="test", discontinuations_path=stops_file)
            self.assertEqual(report["status"], "complete")
            self.assertEqual(report["runtime"]["sql"]["spark.sql.autoBroadcastJoinThreshold"], "-1")
            self.assertEqual(report["runtime"]["sql"]["spark.sql.adaptive.autoBroadcastJoinThreshold"], "-1")
            self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(), 0)
            checks = report["checks"]
            self.assertTrue(all(s["ok"] for s in checks["snapshots"]))
            gold = sum(d for entries in NETWORK.values() for _, _, _, d in entries)
            self.assertEqual(checks["service_date_departures"], {"gold": gold, "versioned": gold})
            self.assertEqual(checks["service_dates_without_version"], 0)
            self.assertEqual((checks["verified_mappings_matched"], checks["verified_mappings_unmatched"]), (1, []))
            self.assertEqual(checks["verified_discontinuations_matched"], 1)
            self.assertEqual((checks["window_stops_outside_definitions"], checks["window_stop_scope_mismatches"]), (0, 0))
            self.assertEqual(checks["interpretations"]["not_observed"], 1)  # AKT:Line:9
            march = {r["to_snapshot"]: r for r in report["summary"]}[MAR]
            self.assertEqual((march["lines_appeared"], march["lines_disappeared"]), (1, 3))
            self.assertEqual((march["lines_disappeared_renumbered_verified"],
                              march["lines_disappeared_discontinued_verified"],
                              march["lines_disappeared_unexplained"]), (1, 1, 1))
            self.assertEqual((march["window_departures_before"], march["bus_window_departures_before"]), (180, 176))

            read = lambda name: self.spark.read.parquet(str(root / "history" / name))
            lost = {(r.measure, r.stop_id) for r in read("stop_coverage_changes").where("change_type = 'stop_lost'").collect()}
            self.assertIn(("window_service", "NSR:StopPlace:F"), lost)
            self.assertIn(("definitions", "NSR:StopPlace:F"), lost)
            # Stop G is in a published pattern of line 1, but no journey using it operates in the window.
            g = read("stop_coverage").where(f"stop_id = 'NSR:StopPlace:G' AND snapshot_date = '{MAR}'").first()
            self.assertEqual((g.lines_definitions, g.lines_window_service, g.stop_name), (1, 0, "Stop G"))
            line1 = read("line_snapshots").where(f"line_id = 'AKT:Line:1' AND snapshot_date = '{MAR}'").first()
            self.assertIn("NSR:StopPlace:G", line1.stop_ids)
            self.assertNotIn("NSR:StopPlace:G", line1.window_stop_ids)
            modes = {(r.snapshot_date, r.transport_mode): r.window_departures for r in read("network_snapshots").collect()}
            self.assertEqual((modes[(MAR, "bus")], modes[(MAR, "water")]), (106, 4))
            self.assertTrue((root / "history/metadata/inventory.json").is_file())

    def test_unmatched_verified_evidence_fails_the_run(self):
        from pipeline.analysis.route_history import build_history
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            inputs = [self.write_snapshot(root, day) for day in DATES]
            stops_file = root / "stops.json"
            stops_file.write_text(json.dumps([{**DISCONTINUED[0], "line_id": "AKT:Line:1"}]))
            with self.assertRaisesRegex(RuntimeError, "discontinuations not found"):
                build_history(self.spark, inputs, root / "history", identity_key="test",
                              discontinuations_path=stops_file)
            manifest = json.loads((root / "history/metadata/manifest.json").read_text())
            self.assertEqual(manifest["status"], "failed")

    def test_failed_table_write_releases_temporary_cache(self):
        from unittest.mock import patch
        from pipeline.analysis.route_history import build_history
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            inputs = [self.write_snapshot(root, day) for day in DATES]
            with patch("pyspark.sql.readwriter.DataFrameWriter.parquet", side_effect=RuntimeError("write failed")):
                with self.assertRaisesRegex(RuntimeError, "write failed"):
                    build_history(self.spark, inputs, root / "history")
            self.assertEqual(self.spark.sparkContext._jsc.sc().getPersistentRDDs().size(), 0)
            report = json.loads((root / "history/metadata/manifest.json").read_text())
            self.assertEqual(report["status"], "failed")

    def test_saved_stop_coverage_scales_without_broadcasts(self):
        """79,200 coverage rows and 636,933 national names: larger than the failed job."""
        from pyspark.sql import functions as F
        from pipeline.analysis.route_history import stop_coverage, stop_coverage_changes
        dates = [f"2026-{month:02}-15" for month in range(1, 12)]
        date_values = F.array(*[F.lit(day) for day in dates])
        base = (self.spark.range(11 * 2 * 300)
                .withColumn("month_index", F.floor(F.col("id") / 600).cast("int"))
                .withColumn("line_number", (F.col("id") % 300).cast("int")))
        lines = (base.withColumn("snapshot_date", F.element_at(date_values, F.col("month_index") + 1))
                 .withColumn("provider", F.when(F.col("id") % 600 < 300, "AKT").otherwise("KOL"))
                 .withColumn("line_id", F.concat("provider", F.lit(":Line:"), F.col("line_number")))
                 .withColumn("public_code", F.col("line_number").cast("string"))
                 .withColumn("stop_ids", F.transform(
                     F.sequence(F.col("line_number") * 12, F.col("line_number") * 12 + 11),
                     lambda n: F.concat(F.lit("NSR:StopPlace:"), n)))
                 .withColumn("window_stop_ids", F.when(F.col("month_index") % 2 == 0, F.col("stop_ids"))
                             .otherwise(F.slice("stop_ids", 1, 11))))
        names = (self.spark.range(11 * 57903)
                 .withColumn("snapshot_date", F.element_at(date_values, (F.floor(F.col("id") / 57903) + 1).cast("int")))
                 .withColumn("stop_id", F.concat(F.lit("NSR:StopPlace:"), F.col("id") % 57903))
                 .withColumn("name", F.concat(F.lit("Stop "), F.col("id") % 57903))
                 .select("snapshot_date", "stop_id", "name"))
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            stop_coverage(lines, names).write.parquet(str(root / "coverage"))
            saved = self.spark.read.parquet(str(root / "coverage"))
            self.assertEqual(saved.count(), 79200)
            changes = stop_coverage_changes(saved, dates, self.spark)
            changes.coalesce(1).write.parquet(str(root / "changes"))
            result = self.spark.read.parquet(str(root / "changes"))
            counts = {(r.measure, r.change_type): r["count"]
                      for r in result.groupBy("measure", "change_type").count().collect()}
            self.assertEqual(counts, {("window_service", "stop_lost"): 3000,
                                      ("window_service", "stop_gained"): 3000})
            plan = changes._jdf.queryExecution().executedPlan().toString()
            self.assertNotIn("BroadcastExchange", plan)
            self.assertNotIn("Range (", plan)  # only saved coverage, not the original range/name joins

    # ---- Silver/Gold fixture writer --------------------------------------

    def write_snapshot(self, root, day):
        silver, gold = root / "silver" / day, root / "gold" / day
        entries = NETWORK[day]
        stop = lambda s: f"NSR:StopPlace:{s}"
        # Line 1 also publishes a pattern A-G whose journey has no operating date in the window.
        journeys = [(f"j-{line}", line, stops) for line, _, stops, _ in entries]
        journeys.append(("j-unused", "AKT:Line:1", ["A", "G"]))
        tables = {
            (silver, "lines"): ([("AKT", line, f"Route {code}", code, mode(line), "Line") for line, code, _, _ in entries],
                                "provider string, entity_id string, name string, public_code string, "
                                "transport_mode string, line_kind string"),
            (silver, "journeys"): ([(key, "AKT", line, True) for key, line, _ in journeys],
                                   "journey_key string, provider string, line_entity_id string, is_valid boolean"),
            (silver, "passing_times"): ([(key, i, stop(s), None, None, None, True)
                                         for key, _, stops in journeys for i, s in enumerate(stops)],
                                        "journey_key string, passing_time_index int, quay_stop_place_id string, "
                                        "assignment_flexible_stop_place_ref string, assignment_quay_ref string, "
                                        "point_scheduled_stop_ref string, is_valid boolean"),
            (silver, "journey_service_dates"): ([(f"j-{line}", "AKT", line, f"{day[:8]}{d}", True, False)
                                                 for line, _, _, _ in entries for d in ("15", "16")],
                                                "journey_key string, provider string, line_entity_id string, "
                                                "service_date string, is_valid boolean, is_flexible boolean"),
            (silver, "stop_places"): ([(stop(s), f"Stop {s}") for s in "ABCDEFGPQRSWX"],
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
        for base, names in ((silver, ["lines", "journeys", "passing_times", "journey_service_dates", "stop_places"]),
                            (gold, ["line_window_summary", "daily_line_service"])):
            (base / "metadata").mkdir()
            (base / "metadata/manifest.json").write_text(json.dumps(
                {"tables": {n: {"path": n} for n in names}, "service_from": day, "service_to": f"{day[:8]}21"}))
        return {"snapshot_date": day, "providers": ["AKT"], "gold": str(gold), "silver": str(silver),
                "gold_manifest_sha256": "g", "silver_manifest_sha256": "s",
                "service_from": day, "service_to": f"{day[:8]}21"}


if __name__ == "__main__":
    unittest.main()
