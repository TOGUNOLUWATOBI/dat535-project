import importlib.util
import unittest


def service_fixture():
    """Partial windows exercise unknown dates versus covered dates with zero service."""
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
        from app.analytics import monthly_series
        series = monthly_series(self.daily, self.items, ["AKT"], "bus")
        self.assertEqual(list(series.departures), [20, 44])
        self.assertEqual(list(series.active_lines), [1, 2])
        water = monthly_series(self.daily, self.items, ["AKT"], "water")
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
        from app.analytics import daily_month
        profiles = [daily_month(self.daily, self.items, month, ["AKT"], "bus") for month in ("2026-01", "2026-02")]
        first, second = [frame[frame.covered].groupby("weekday").departures.mean() for frame in profiles]
        self.assertEqual(first.loc["Thursday"], 10)
        self.assertEqual(first.loc["Sunday"], 0)
        self.assertEqual(second.loc["Monday"], 14)
        self.assertEqual(second.loc["Sunday"], 30)

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

    def test_month_distinguishes_unknown_dates_from_covered_zero_service(self):
        import pandas as pd
        from app.analytics import daily_month
        result = daily_month(self.daily, self.items, "2026-01").set_index("service_date")
        self.assertEqual(len(result), 31)
        self.assertEqual(result.covered.sum(), 7)
        self.assertTrue(pd.isna(result.loc["2026-01-01", "departures"]))
        self.assertEqual(result.loc["2026-01-18", "departures"], 0)
        self.assertEqual(result.loc["2026-01-15", "active_lines"], 2)
        self.assertEqual(result.departures.sum(), 34)
        water = daily_month(self.daily, self.items, "2026-02", ["AKT"], "water")
        self.assertEqual(water.covered.sum(), 7)
        self.assertEqual(water.loc[water.covered, "departures"].sum(), 0)

    def test_overlapping_exports_choose_one_whole_valid_plan_without_old_line_backfill(self):
        import pandas as pd
        from app.analytics import daily_month
        newer = dict(self.items[0], snapshot_date="2026-01-16", service_from="2026-01-16", service_to="2026-01-22")
        future = dict(newer, snapshot_date="2026-01-25", service_from="2026-01-15", service_to="2026-01-31")
        future["gold_manifest"]={"providers":["AKT","KOL"],"source_calendar_coverage":{
            p:{"from":"2026-01-25","to":"2026-01-31"} for p in ("AKT","KOL")}}
        row = self.daily.iloc[[0]].assign(snapshot_date="2026-01-16", service_date="2026-01-16", scheduled_departures=100)
        hindsight = row.assign(snapshot_date="2026-01-25", service_date="2026-01-15", scheduled_departures=777)
        daily = pd.concat([self.daily, row, hindsight], ignore_index=True)
        result = daily_month(daily, [*self.items, newer, future], "2026-01").set_index("service_date")
        self.assertEqual(result.loc["2026-01-15", "departures"], 17)
        self.assertEqual(result.loc["2026-01-16", "departures"], 100)
        self.assertEqual(result.loc["2026-01-16", "active_lines"], 1)
        self.assertEqual(result.loc["2026-01-16", "source_export"], "2026-01-16")
        self.assertFalse(result.loc["2026-01-23", "covered"])

    def test_month_plan_includes_days_before_export_and_the_last_day(self):
        from app.analytics import daily_month
        item={**self.items[0],"service_from":"2026-01-01","service_to":"2026-01-31"}
        result=daily_month(self.daily,[item],"2026-01")
        self.assertEqual(result.covered.sum(),31)
        self.assertEqual(result.source_export.iloc[0],"2026-01-15")
        self.assertEqual(result.source_export.iloc[-1],"2026-01-15")

    def test_monthly_overview_uses_the_same_daily_plans_and_calendar_denominator(self):
        from app.analytics import monthly_series, daily_month
        series=monthly_series(self.daily,self.items)
        for month,expected in [('2026-01',34),('2026-02',55)]:
            part=series[series.month==month]
            profile=daily_month(self.daily,self.items,month)
            self.assertEqual(part.departures.sum(),expected)
            self.assertEqual(part.departures.sum(),profile.departures.sum())
            self.assertTrue((part.covered_days==7).all())
            self.assertAlmostEqual(part.mean_per_day.sum(),expected/7)

    def test_month_with_no_published_coverage_includes_all_leap_month_dates(self):
        from app.analytics import daily_month
        result = daily_month(self.daily, self.items, "2028-02")
        self.assertEqual(len(result), 29)
        self.assertFalse(result.covered.any())
        self.assertTrue(result.departures.isna().all())

    def test_exclusions_keep_valid_on_demand_separate_from_invalid_and_unknown(self):
        import pandas as pd
        from app.analytics import exclusion_reasons
        rows = pd.DataFrame({"is_valid": [True, False, False, None, True, False],
                             "is_flexible": [True, False, True, True, False, None]})
        result = exclusion_reasons(rows)
        self.assertEqual(list(result.reason), ["Valid on-demand service", "Invalid service occurrence",
                                               "Invalid service occurrence", "Not classified", "Not classified", "Not classified"])
        self.assertNotIn("reason", rows.columns)
        self.assertEqual(exclusion_reasons(pd.DataFrame({"journey": [1]})).reason.iloc[0], "Not classified")

    def test_current_month_cutoff_keeps_future_plans_out_of_totals_and_exports(self):
        import pandas as pd
        from app.analytics import daily_month, monthly_plan_rows, monthly_series
        daily, items = current_month_fixture()
        original = daily.copy(deep=True)
        profile = daily_month(daily, items, "2026-11", through="2026-11-10")
        self.assertEqual(len(profile), 10)
        self.assertEqual(profile.departures.sum(), 30)
        self.assertEqual(profile.service_date.max(), pd.Timestamp("2026-11-10"))
        rows, _ = monthly_plan_rows(daily, items, "2026-11", through="2026-11-10")
        self.assertTrue((rows.service_date <= pd.Timestamp("2026-11-10")).all())
        series = monthly_series(daily, items, through="2026-11-10").set_index("month")
        self.assertEqual(series.loc["2026-11", "departures"], 30)
        self.assertEqual(series.loc["2026-11", "calendar_days"], 30)
        self.assertEqual(series.loc["2026-11", "period_days"], 10)
        self.assertEqual(series.loc["2026-11", "period_end"], "2026-11-10")
        self.assertEqual(series.loc["2026-10", "departures"], 170)
        self.assertEqual(daily_month(daily, items, "2026-11").departures.sum(), 330)
        pd.testing.assert_frame_equal(daily, original)

    def test_unfinished_month_comparison_uses_matching_elapsed_date_ranges(self):
        from app.analytics import compare_months, comparison_cutoffs
        result = compare_months(*current_month_fixture(), "2026-10", "2026-11", through="2026-11-10")
        self.assertEqual((result.before.sum(), result.after.sum(), result.change.sum()), (20, 30, 10))
        self.assertEqual(result.earlier_period_end.iloc[0], "2026-10-10")
        self.assertEqual(result.later_period_end.iloc[0], "2026-11-10")
        full = compare_months(*current_month_fixture(), "2026-10", "2026-11")
        self.assertEqual((full.before.sum(), full.after.sum()), (170, 330))
        cutoffs = comparison_cutoffs("2028-02", "2028-03", "2028-03-30")
        self.assertEqual([v.day for v in cutoffs.values()], [29, 29])
        self.assertEqual(comparison_cutoffs("2026-10", "2026-11", "2026-11-30"),
                         {"2026-10": "2026-11-30", "2026-11": "2026-11-30"})

    def test_future_only_month_returns_an_empty_period_with_a_display_cutoff(self):
        from app.analytics import daily_month, monthly_series
        daily, items = current_month_fixture()
        self.assertTrue(daily_month(daily, items, "2026-11", through="2026-10-31").empty)
        series = monthly_series(daily, items, through="2026-09-30")
        self.assertTrue(series.empty)
        self.assertIn("month", series.columns)

    def test_line_profile_respects_the_same_cutoff(self):
        from app.analytics import daily_profile
        daily, items = current_month_fixture()
        profile = daily_profile(daily, items[1], through="2026-11-10")
        self.assertEqual(len(profile), 10)
        self.assertEqual(profile.departures.sum(), 30)

    def test_today_uses_norwegian_date_at_the_utc_midnight_boundary(self):
        from datetime import date, datetime, timezone
        from unittest.mock import patch
        from app.analytics import today_in_oslo
        instant = datetime(2026, 11, 9, 23, 30, tzinfo=timezone.utc)
        with patch("app.analytics.datetime") as clock:
            clock.now.side_effect = lambda tz: instant.astimezone(tz)
            self.assertEqual(today_in_oslo(), date(2026, 11, 10))


def current_month_fixture():
    """Future service is deliberately large so leaking it changes visible totals."""
    import pandas as pd
    rows = []
    for month, source, dates in [
        ("2026-10", "2026-10-15", [(1, 5), (10, 15), (11, 50), (31, 100)]),
        ("2026-11", "2026-11-09", [(1, 10), (10, 20), (11, 100), (30, 200)]),
    ]:
        rows.extend((source, "AKT", "line", "10", "bus", f"{month}-{day:02d}", count) for day, count in dates)
    daily = pd.DataFrame(rows, columns=["snapshot_date", "provider", "line_entity_id", "line_public_code",
                                       "line_transport_mode", "service_date", "scheduled_departures"])
    daily["first_departure_seconds"], daily["last_departure_seconds"] = 21600, 79200
    items = [{"snapshot_date": source, "service_from": month + "-01", "service_to": month + end,
              "gold_manifest": {"providers": ["AKT"]}}
             for month, source, end in [("2026-10", "2026-10-15", "-31"), ("2026-11", "2026-11-09", "-30")]]
    return daily, items
