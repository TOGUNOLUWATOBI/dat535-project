from datetime import date
import unittest

from pipeline.quality.trace import clock_seconds, operating_dates


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


if __name__ == "__main__":
    unittest.main()
