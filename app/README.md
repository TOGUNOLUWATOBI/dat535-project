# Gold explorer (Streamlit)

A read-only interface to the published Gold and route-history outputs. It
reads only completed, registered checkpoints under the data root and never
writes to it. Pandas reads the small Gold Parquet tables directly, so no Spark
session is needed.

```bash
python3 -m venv ~/dat535-gui-env
~/dat535-gui-env/bin/pip install -r requirements-gui.txt
bash scripts/run_gui.sh          # http://localhost:8501 (VS Code forwards the port)
```

`DAT535_DATA_ROOT` (default `/srv/dat535/data`), `DAT535_GUI_PORT` (default
8501) and `DAT535_GUI_PYTHON` can be overridden. The app listens on
127.0.0.1 only. Open it through SSH or VS Code port forwarding.

| Page | Shows | Source |
| --- | --- | --- |
| Overview | Select an export, operator and mode; see departures, active lines and a trend with calendar spacing | Gold `daily_line_service` |
| Compare exports | Any earlier/later pair: totals, percentage change, weekday profiles, line gains/losses and CSV | Gold `daily_line_service`, optional route-history names |
| One line | Search by number or place; service, observed versions, changes and stops in separate tabs; daily profile and CSV | Gold `daily_line_service`, route history |
| What changed | Consecutive exports: lines appeared/disappeared, verified explanations and sources, largest departure changes, stops no longer served | Route history, `config/route_*.json` |
| Replaced lines | A verified replacement: old/new departures and definition stops, with the operator source | Route history, `config/route_renumberings.json` |
| Data checks | Quality-gate results and reconciliation, CSV, optional route-history checks and Silver report | Gold manifests, route history, `docs/quality_report.md` |

The comparison matches lines by operator and NeTEx ID. It does not infer a
replacement from similar numbers or a change in departures. Percentage change
is undefined when the earlier total is zero. Weekday profiles average over
calendar days, including days with no service. A new or missing service in a
seven-day window does not establish that a route opened or closed; holidays
and seasonal plans also affect counts.

Overview, comparisons and data checks work without route history. A single
published export can be viewed; comparisons need two. The GUI reads Gold from
`pipeline_state.sqlite3` in read-only mode and does not require the Silver
files. Exports must share a provider selection and service-window definition.
The history pages use the latest complete history output and show its last
observation date. If history is older than Gold, the app shows a notice.

Results are cached for 60 seconds and checked again on interaction after the
cache expires. **Refresh data** clears the cache immediately. The GUI does not
trigger ingestion or processing.

Gold outputs built before the quality gate show the gate as *not recorded*.
The next processing run records it. Manual ZIP upload (T55) is not part of this
app; see [manual ingestion](../docs/manual_ingestion.md).

`app/data.py` holds data access, `app/analytics.py` holds the comparison and
calendar calculations, and `app/gold_app.py` holds the pages. Run their unit
tests in the GUI environment:

```bash
~/dat535-gui-env/bin/python -m unittest tests.test_gui_data tests.test_gui_analytics tests.test_gui_app -v
```
