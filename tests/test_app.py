"""Every GUI page renders without errors against the published Gold run (skipped when absent)."""

import importlib.util
import os
from pathlib import Path
import unittest

GOLD = Path(os.environ.get("DAT535_GOLD_RUN", "/srv/dat535/data/gold/kol/runs/gold-kol-53d-2026-v2"))
PAGES = ["📊 Overview", "🚏 Lines", "📍 Stops", "🔄 Changes", "🩺 Data quality"]


@unittest.skipUnless(importlib.util.find_spec("streamlit") and (GOLD / "gold_manifest.json").is_file(),
                     "Needs Streamlit and the Gold run")
class GoldAppTests(unittest.TestCase):
    def open(self, page, period="winter-2026", mode="bus", event="departure"):
        from streamlit.testing.v1 import AppTest
        app = AppTest.from_file("src/app/gold_app.py", default_timeout=180).run()
        for index, value in enumerate([page, mode, event]):
            app.sidebar.radio[index].set_value(value)
        app.sidebar.selectbox[0].set_value(period)
        return app.run()

    def test_every_page_and_filter_combination_renders(self):
        for page in PAGES:
            for filters in [("winter-2026", "bus", "departure"), ("2026-03", "water", "arrival")]:
                with self.subTest(page=page, filters=filters):
                    self.assertEqual([e.value for e in self.open(page, *filters).exception], [])

    def test_unknown_rate_shows_dash_not_zero(self):
        metrics = {m.label: m.value for m in self.open(PAGES[0]).metric}
        self.assertEqual(metrics["Trips cancelled"], "—")  # no bus trip has a known cancellation state

    def test_stops_have_names_and_line_filter_narrows_them(self):
        app = self.open(PAGES[2])
        everything = len(app.dataframe[0].value)
        self.assertNotIn("(unnamed)", list(app.dataframe[0].value["Stop"])[:20])
        import pandas as pd
        lines = pd.read_parquet(GOLD / "period_line_service", columns=["line_id", "transport_mode"])
        line = lines[lines.transport_mode == "bus"].line_id.dropna().iloc[0]
        next(s for s in app.selectbox if s.label == "Only stops on line").set_value(line).run()
        self.assertEqual([e.value for e in app.exception], [])
        self.assertLess(len(app.dataframe[0].value), everything)

    def test_changes_page_finds_known_2026_changes(self):
        app = self.open(PAGES[3])
        started = list(app.dataframe[0].value["Line"])
        self.assertIn("12", started)  # line 12 started with the summer timetable on 1 July
        self.assertNotIn("1", started)  # line 1 only got a new internal ID: not a new line
        rerouted = " ".join(app.dataframe[1].value["Line · changed between"])
        self.assertIn("Line 1 · ", rerouted)  # ... but its route did change
        self.assertEqual([e.value for e in app.exception], [])


if __name__ == "__main__":
    unittest.main()
