"""Smoke-test each GUI page against the selected development dates, without deploying it."""

import json
import os
from pathlib import Path

from streamlit.testing.v1 import AppTest


def main() -> None:
    gold = Path(os.environ["DAT535_GOLD_RUN"])
    manifest = json.loads((gold / "gold_manifest.json").read_text())
    if manifest.get("status") != "complete" or manifest.get("gate_status") != "passed":
        raise RuntimeError("GUI smoke checks require a complete, gate-passed Gold run.")
    month = manifest["published_operating_dates"][0][:7]
    app = AppTest.from_file("src/app/gold_app.py", default_timeout=300).run()
    if app.exception:
        raise RuntimeError([e.value for e in app.exception])
    # The existing test_app suite asserts facts about the full historical dataset.
    # Development smoke runs may contain just one date, so exercise pages without
    # asserting that a particular historical route change must be present.
    app.sidebar.selectbox[0].set_value(month)
    for page in ("📊 Overview", "🚏 Lines", "📍 Stops", "🔄 Changes", "🩺 Data quality"):
        app.sidebar.radio[0].set_value(page)
        app.run()
        if app.exception:
            raise RuntimeError(f"{page}: {[e.value for e in app.exception]}")
        print(f"GUI smoke passed: {page}", flush=True)


if __name__ == "__main__":
    main()
