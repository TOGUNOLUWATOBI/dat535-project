"""Silver calendar decisions and Oslo daylight-saving boundaries on tiny fixtures."""

import unittest

from tests.fixtures import available


@unittest.skipUnless(available("pyspark"), "Install requirements.txt for Silver calendar imports")
class CalendarTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pipeline.Silver import calendar_and_calls
        cls.calendar = calendar_and_calls

    def props(self, days):
        return [{"DaysOfWeek": days, **{key: None for key in
                 ("WeeksOfMonth", "MonthOfYear", "DayOfYear", "HolidayTypes", "Seasons")}}]

    def period(self):
        return {"assignment_kind": "operating_period", "from_date": "2026-01-01",
                "to_date": "2026-01-31", "is_available_raw": "true"}

    def test_period_boundaries_are_inclusive_and_weekdays_are_applied(self):
        evaluate = self.calendar._eval_day_type_for_date
        self.assertEqual(evaluate("2026-01-01", self.props("Weekdays"), [self.period()], [])[0], "active")
        self.assertEqual(evaluate("2026-01-31", self.props("Everyday"), [self.period()], [])[0], "active")
        self.assertEqual(evaluate("2026-01-03", self.props("Weekdays"), [self.period()], [])[0], "inactive")
        self.assertEqual(evaluate("2026-02-01", [], [self.period()], [])[0], "inactive")

    def test_explicit_cancellation_overrides_period_and_conflicts_stay_unresolved(self):
        evaluate = self.calendar._eval_day_type_for_date
        exclude = {"assignment_kind": "date", "target_date": "2026-01-01", "is_available_raw": "false"}
        self.assertEqual(evaluate("2026-01-01", [], [self.period(), exclude], [])[0], "inactive")
        include = {**exclude, "is_available_raw": "true"}
        result = evaluate("2026-01-01", [], [exclude, include], [])
        self.assertEqual(result[0], "unresolved")
        self.assertIn("conflicting_date_specific_assignments", result[1])

    def test_unsupported_calendar_properties_are_not_assumed_active(self):
        props = self.props("Everyday")
        props[0]["HolidayTypes"] = "NationalHoliday"
        result = self.calendar._eval_day_type_for_date("2026-01-01", props, [self.period()], [])
        self.assertEqual(result[0], "unresolved")
        self.assertIn("unsupported_property_of_day:HolidayTypes", result[1])

    def test_day_offset_and_extended_clock_hour_resolve_to_same_utc_instant(self):
        convert = self.calendar._local_clock_to_utc
        explicit = convert("2026-01-01", "01:10:00", "1")
        extended = convert("2026-01-01", "25:10:00", "0")
        self.assertEqual(explicit, extended)
        self.assertEqual(explicit[0], "2026-01-02T00:10:00+00:00")
        self.assertIsNone(explicit[3])

    def test_nonexistent_and_ambiguous_oslo_wall_times_have_explicit_errors(self):
        convert = self.calendar._local_clock_to_utc
        self.assertEqual(convert("2026-03-29", "02:30:00", "0")[3], "nonexistent_dst_local_time")
        self.assertEqual(convert("2026-10-25", "02:30:00", "0")[3], "ambiguous_dst_local_time")

    def test_missing_times_and_bad_components_do_not_create_timestamps(self):
        convert = self.calendar._local_clock_to_utc
        self.assertEqual(convert("2026-01-01", None, None), (None, None, None, None))
        self.assertEqual(convert("2026-01-01", "10:60:00", "0")[3], "invalid_time_components")
        self.assertEqual(convert("2026-01-01", "10:00:00", "bad")[3], "invalid_day_offset")
