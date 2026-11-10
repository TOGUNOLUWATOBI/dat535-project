import importlib.util
import unittest


def service_fixture():
    import pandas as pd
    rows = [
        ("2026-01-15", "AKT", "shared", "10", "bus", "2026-01-15", 10),
        ("2026-01-15", "AKT", "shared", "10", "bus", "2026-01-16", 10),
        ("2026-01-15", "AKT", "water", "B1", "water", "2026-01-17", 5),
        ("2026-01-15", "KOL", "shared", "10", "bus", "2026-01-15", 7),
        ("2026-01-15", "KOL", "water", "B2", "water", "2026-01-16", 2),
        ("2026-02-15", "AKT", "shared", "10", "bus", "2026-02-15", 30),
        ("2026-02-15", "AKT", "shared", "10", "bus", "2026-02-16", 5),
        ("2026-02-15", "AKT", "new", "20", "bus", "2026-02-16", 9),
        ("2026-02-15", "KOL", "shared", "10", "bus", "2026-02-15", 7),
        ("2026-02-15", "KOL", "water", "B2", "water", "2026-02-15", 4),
    ]
    frame = pd.DataFrame(rows, columns=["snapshot_date", "provider", "line_entity_id", "line_public_code",
                                        "line_transport_mode", "service_date", "scheduled_departures"])
    frame["first_departure_seconds"], frame["last_departure_seconds"] = 21600, 79200
    items = [{"snapshot_date": d, "service_from": d, "service_to": d[:8] + "21",
              "gold_manifest": {"providers": ["AKT", "KOL"]}} for d in ["2026-01-15", "2026-02-15"]]
    return frame, items


@unittest.skipUnless(importlib.util.find_spec("pandas"), "Install GUI dependencies to test Gold analysis")
class GoldAnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.daily, self.items = service_fixture()

    def test_mode_and_operator_filters_apply_to_all_counts(self):
        from app.analytics import network_series
        series = network_series(self.daily, self.items, ["AKT"], "bus")
        self.assertEqual(list(series.departures), [20, 44])
        self.assertEqual(list(series.active_lines), [1, 2])
        water = network_series(self.daily, self.items, ["AKT"], "water")
        self.assertEqual(list(water.departures), [5, 0])
        self.assertEqual(list(water.active_lines), [1, 0])

    def test_comparison_reconciles_and_keeps_provider_identity(self):
        from app.analytics import compare_exports
        result = compare_exports(self.daily, "2026-01-15", "2026-02-15")
        self.assertEqual((result.before.sum(), result.after.sum(), result.change.sum()), (34, 55, 21))
        same = result[result.line_entity_id == "shared"].set_index("provider")
        self.assertEqual((same.loc["AKT", "change"], same.loc["KOL", "change"]), (15, 0))
        self.assertEqual(same.loc["AKT", "change_pct"], 75.0)

    def test_zero_baselines_and_lost_service_are_not_interpreted_as_renumbering(self):
        import pandas as pd
        from app.analytics import compare_exports
        result = compare_exports(self.daily, "2026-01-15", "2026-02-15", ["AKT"])
        new = result[result.line_entity_id == "new"].iloc[0]
        lost = result[result.line_entity_id == "water"].iloc[0]
        self.assertTrue(pd.isna(new.change_pct))
        self.assertEqual(new.service_status, "New service in window")
        self.assertEqual((lost.change_pct, lost.service_status), (-100.0, "No service in later window"))

    def test_zero_service_days_remain_in_calendar_window(self):
        import pandas as pd
        from app.analytics import daily_profile, select_service
        profile = daily_profile(select_service(self.daily, ["AKT"], "bus"), self.items[0])
        self.assertEqual(len(profile), 7)
        self.assertEqual(profile.departures.sum(), 20)
        self.assertEqual((profile.departures == 0).sum(), 5)
        self.assertTrue(pd.isna(profile.loc[2, "first"]))

    def test_weekdays_are_aligned_instead_of_comparing_the_first_day_of_each_window(self):
        from app.analytics import weekday_comparison
        profile = weekday_comparison(self.daily, self.items, "2026-01-15", "2026-02-15", ["AKT"], "bus")
        first = profile[profile.snapshot_date == "2026-01-15"].set_index("weekday")
        second = profile[profile.snapshot_date == "2026-02-15"].set_index("weekday")
        self.assertEqual(first.loc["Thursday", "departures"], 10)
        self.assertEqual(first.loc["Sunday", "departures"], 0)
        self.assertEqual(second.loc["Monday", "departures"], 14)
        self.assertEqual(second.loc["Sunday", "departures"], 30)

    def test_empty_filter_and_reverse_comparison(self):
        from app.analytics import compare_exports
        self.assertTrue(compare_exports(self.daily, "2026-01-15", "2026-02-15", []).empty)
        with self.assertRaises(ValueError):
            compare_exports(self.daily, "2026-02-15", "2026-01-15")

    def test_source_date_outside_declared_window_is_not_silently_dropped(self):
        from app.analytics import daily_profile
        self.daily.loc[0, "service_date"] = "2026-01-01"
        with self.assertRaises(ValueError):
            daily_profile(self.daily, self.items[0])
