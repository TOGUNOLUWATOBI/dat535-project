"""Exercise visible filter results with published-data fixtures, without Spark."""

import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

from tests.test_gui_analytics import service_fixture


@unittest.skipUnless(importlib.util.find_spec("streamlit") and importlib.util.find_spec("pandas"),
                     "Install requirements-gui.txt for GUI interaction tests")
class GuiInteractionTests(unittest.TestCase):
    def setUp(self):
        import streamlit as st
        st.cache_data.clear()
        self.daily, self.items = service_fixture()

    def tearDown(self):
        import streamlit as st
        st.cache_data.clear()

    def run_app(self, items, daily):
        from app import data
        from streamlit.testing.v1 import AppTest
        self.enterContext(patch.object(data, "snapshots", return_value=items))
        self.enterContext(patch.object(data, "gold_table", return_value=daily))
        self.enterContext(patch.object(data, "History", side_effect=SystemExit("No route history yet")))
        path = Path(__file__).resolve().parents[1] / "app/gold_app.py"
        app = AppTest.from_file(str(path), default_timeout=20).run()
        self.assertFalse(app.exception)
        return app

    def metrics(self, app):
        self.assertFalse(app.exception)
        return {metric.label: metric.value for metric in app.metric}

    def test_filters_update_cards_and_comparison_when_service_is_zero(self):
        app = self.run_app(self.items, self.daily)
        self.assertEqual(self.metrics(app)["Planned departures"], "55")
        app.selectbox(key="overview_operator").set_value("AKT").run()
        app.selectbox(key="overview_mode").set_value("Boat").run()
        self.assertEqual(self.metrics(app)["Planned departures"], "0")
        self.assertEqual(self.metrics(app)["Lines with service"], "0")

        app.sidebar.radio[0].set_value("Compare months").run()
        app.selectbox(key="compare_operator").set_value("AKT").run()
        app.selectbox(key="compare_mode").set_value("Boat").run()
        values = self.metrics(app)
        self.assertEqual((values["Earlier departures"], values["Later departures"], values["Change"]),
                         ("5", "0", "-100.0%"))
        app.radio(key="compare_lines").set_value("New service in window").run()
        self.assertFalse(app.exception)
        self.assertTrue(any("No lines match" in info.value for info in app.info))

    def test_single_export_without_history_still_has_a_working_overview(self):
        first = self.items[0]
        app = self.run_app([first], self.daily[self.daily.snapshot_date == first["snapshot_date"]])
        self.assertEqual(self.metrics(app)["Planned departures"], "34")
        app.sidebar.radio[0].set_value("Compare months").run()
        self.assertFalse(app.exception)
        self.assertTrue(any("at least two published months" in info.value for info in app.info))
        app.sidebar.radio[0].set_value("One line").run()
        self.assertFalse(app.exception)
        self.assertTrue(any("Route history is not available" in info.value for info in app.info))

    def test_daily_view_shows_partial_month_coverage_and_zero_service_filters(self):
        app = self.run_app(self.items, self.daily)
        app.sidebar.radio[0].set_value("Daily service").run()
        values = self.metrics(app)
        self.assertEqual(values["Dates with published coverage"], "7 / 28")
        self.assertEqual(values["Departures on covered dates"], "55")
        self.assertTrue(any("21 dates have no published coverage" in info.value for info in app.info))
        app.selectbox(key="daily_operator").set_value("AKT").run()
        app.selectbox(key="daily_mode").set_value("Boat").run()
        self.assertEqual(self.metrics(app)["Departures on covered dates"], "0")
        self.assertEqual(self.metrics(app)["Dates with published coverage"], "7 / 28")
        app.selectbox(key="daily_month").set_value("2026-01").run()
        self.assertEqual(self.metrics(app)["Dates with published coverage"], "7 / 31")
        self.assertEqual(self.metrics(app)["Departures on covered dates"], "5")

    def test_exclusion_details_separate_on_demand_from_invalid(self):
        import pandas as pd
        from app import data
        self.items[-1]["gold_manifest"].update(
            tables={"excluded_service_dates": {"path": "excluded_service_dates"}},
            reconciliation={"excluded_service_occurrences": 23, "counts_match": True})
        app = self.run_app(self.items, self.daily)
        excluded = pd.DataFrame({"provider": ["KOL"] * 23, "is_valid": [True] * 23,
                                 "is_flexible": [True] * 23})
        with patch.object(data, "gold_table", return_value=excluded):
            app.sidebar.radio[0].set_value("Data checks").run()
        values = self.metrics(app)
        self.assertEqual(values["Valid on-demand occurrences"], "23")
        self.assertEqual(values["Invalid occurrences"], "0")
        self.assertEqual(values["Unclassified occurrences"], "0")


if __name__ == "__main__":
    unittest.main()
