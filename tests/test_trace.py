from datetime import date
import json
from pathlib import Path
import tempfile
import unittest

from pipeline.quality.trace import (clock_seconds, compare_passing_times, operating_dates,
                                    source_calendar, source_journey)
from pipeline.quality.source_checks import SourceIndex, SourceReferenceError


class IndependentTraceTests(unittest.TestCase):
    def test_times_keep_the_operating_day(self):
        self.assertEqual(clock_seconds("23:55:00", None), 86100)
        self.assertEqual(clock_seconds("00:10:00", "1"), 87000)
        self.assertIsNone(clock_seconds(None, "1"))

    def test_periods_weekdays_dates_and_exceptions(self):
        calendar = ({"W": {0, 1, 2, 3, 4}, "S": set(range(7))},
                    {"P": {"from": "2026-02-01T00:00:00", "to": "2026-02-28T00:00:00", "from_day": None, "to_day": None}},
                    {"D": "2026-02-21"},
                    {"W": [{"date": None, "period": "P", "day": None, "available": True},
                           {"date": "2026-02-20", "period": None, "day": None, "available": False}],
                     "S": [{"date": None, "period": None, "day": "D", "available": True}]})
        dates, exceptions = operating_dates(["W"], calendar, date(2026, 2, 15), date(2026, 2, 21))
        self.assertEqual(dates, ["2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19"])
        self.assertEqual(exceptions, ["2026-02-20"])
        self.assertEqual(operating_dates(["S"], calendar, date(2026, 2, 15), date(2026, 2, 21))[0], ["2026-02-21"])

    def test_explicit_available_date_overrides_an_unavailable_period(self):
        calendar = ({"W": set(range(7))},
                    {"P": {"from": "2026-01-15", "to": "2026-01-21", "from_day": None, "to_day": None}},
                    {}, {"W": [{"date": None, "period": "P", "day": None, "available": False},
                               {"date": "2026-01-16", "period": None, "day": None, "available": True}]})
        self.assertEqual(operating_dates(["W"], calendar, date(2026, 1, 15), date(2026, 1, 21))[0], ["2026-01-16"])

    def test_missing_day_type_is_not_assumed_to_run_every_day(self):
        with self.assertRaises(SourceReferenceError):
            operating_dates(["missing"], ({}, {}, {}, {}), date(2026, 1, 15), date(2026, 1, 21))


class IndependentSourceChecksTests(unittest.TestCase):
    def setUp(self):
        from tests.fixtures import complete_fixture
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        manifest = complete_fixture(self.root)
        data = json.loads(manifest.read_text())
        self.xml = self.root / data["xml_directory"]
        self.akt = sorted(self.xml.glob("AKT_*.xml")) + [self.xml / "_AKT_shared_data.xml"]
        self.kol = sorted(self.xml.glob("KOL_*.xml")) + [self.xml / "_KOL_shared_data.xml"]

    def test_pattern_point_to_quay_to_national_stop_place(self):
        index = SourceIndex(self.akt)
        path = self.xml / "AKT_007.xml"
        journey = source_journey(path, "AKT:Journey:repeat", "1")
        stops = index.physical_stops(path, (journey["pattern"], journey["pattern_version"]),
                                     journey["passing_times"], [self.xml / "_stops.xml"])
        self.assertEqual([s["scheduled_stop_ref"] for s in stops], ["AKT:Stop:1", "AKT:Stop:2"])
        self.assertEqual([s["quay_ref"] for s in stops], ["NSR:Quay:1", "NSR:Quay:2"])
        self.assertEqual([s["stop_place_id"] for s in stops], ["NSR:StopPlace:1"] * 2)

    def test_whole_line_recount_preserves_repeated_journey_ids_in_different_files(self):
        index = SourceIndex(self.akt)
        counts = index.line_departures("AKT:Line:1", source_calendar(self.akt), operating_dates,
                                       date(2026, 1, 15), date(2026, 1, 21))
        self.assertEqual(counts["source_journey_occurrences"], 2)
        self.assertEqual(sum(counts["fixed_departures_by_date"].values()), 10)
        self.assertNotIn("2026-01-16", counts["fixed_departures_by_date"])

    def test_flexible_stops_and_occurrences_are_separate_from_fixed_departures(self):
        shared = self.xml / "_KOL_shared_data.xml"
        payload = shared.read_text()
        for quay in (1, 2):
            payload = payload.replace(f'<QuayRef ref="NSR:Quay:{quay}"/>',
                                      '<FlexibleStopPlaceRef ref="KOL:FlexibleStopPlace:1"/>')
        payload = payload.replace('</frames>', '<SiteFrame id="KOL:SiteFrame:1"><flexibleStopPlaces>'
                                  '<FlexibleStopPlace id="KOL:FlexibleStopPlace:1" version="1"/>'
                                  '</flexibleStopPlaces></SiteFrame></frames>')
        shared.write_text(payload)
        index = SourceIndex(self.kol)
        path = self.xml / "KOL_HentMeg.xml"
        journey = source_journey(path, "KOL:Journey:repeat")
        stops = index.physical_stops(path, (journey["pattern"], journey["pattern_version"]),
                                     journey["passing_times"], [self.xml / "_stops.xml"])
        self.assertEqual([s["flexible_stop_ref"] for s in stops], ["KOL:FlexibleStopPlace:1"] * 2)
        counts = index.line_departures("KOL:FlexibleLine:1", source_calendar(self.kol), operating_dates,
                                       date(2026, 1, 15), date(2026, 1, 21))
        self.assertEqual(counts["fixed_departures_by_date"], {})
        self.assertEqual(sum(counts["flexible_occurrences_by_date"].values()), 5)

    def test_missing_and_ambiguous_assignments_are_refused(self):
        index = SourceIndex(self.akt)
        path = self.xml / "AKT_007.xml"
        journey = source_journey(path, "AKT:Journey:repeat")
        args = (path, (journey["pattern"], journey["pattern_version"]),
                journey["passing_times"], [self.xml / "_stops.xml"])
        original = index.shared["assignments"][0]
        index.shared["assignments"].append({**original, "quay": ("NSR:Quay:other", None)})
        with self.assertRaisesRegex(SourceReferenceError, "Stop assignment"):
            index.physical_stops(*args)
        index.shared["assignments"] = []
        with self.assertRaisesRegex(SourceReferenceError, "Stop assignment"):
            index.physical_stops(*args)

    def test_ambiguous_journey_definition_is_not_selected_by_file_order(self):
        path = self.xml / "AKT_007.xml"
        import copy
        import xml.etree.ElementTree as ET
        ns = "{http://www.netex.org.uk/netex}"
        tree = ET.parse(path)
        container = tree.find(f".//{ns}vehicleJourneys")
        container.append(copy.deepcopy(container[0]))
        tree.write(path, encoding="utf-8")
        with self.assertRaisesRegex(SourceReferenceError, "found 2"):
            source_journey(path, "AKT:Journey:repeat", "1")

    def test_wrong_quay_or_flexible_window_cannot_pass_when_times_still_match(self):
        source = [{"arrival_seconds": None, "departure_seconds": None,
                   "earliest_raw": "09:00:00", "latest_raw": "13:00:00"}]
        physical = [{"point_ref": "P", "point_version": "1", "scheduled_stop_ref": "S",
                     "scheduled_stop_version": "1", "quay_ref": "Q", "resolved_quay_version": "2",
                     "stop_place_id": "SP", "flexible_stop_ref": None}]
        silver = [{"arrival_seconds": None, "departure_seconds": None, "point_ref": "P",
                   "point_entity_version": "1", "point_scheduled_stop_ref": "S", "stop_entity_version": "1",
                   "assignment_quay_ref": "wrong", "quay_entity_version": "2", "quay_stop_place_id": "SP",
                   "assignment_flexible_stop_place_ref": None,
                   "earliest_departure_time_raw": "13:00:00", "latest_arrival_time_raw": "09:00:00"}]
        checks = compare_passing_times(source, silver, physical)
        self.assertTrue(checks["times_match"])
        self.assertFalse(checks["physical_stops_match"])
        self.assertFalse(checks["flexible_windows_match"])
        del silver[0]["earliest_departure_time_raw"]
        self.assertFalse(compare_passing_times(source, silver, physical)["flexible_window_fields_present"])


if __name__ == "__main__":
    unittest.main()
