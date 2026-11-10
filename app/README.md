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

The workflow calls `bash scripts/run_gui.sh --restart` after successful
processing and route history. The same command can be run manually: it stops
this app on its configured port, starts it in the background and writes
`DAT535_DATA_ROOT/metadata/gui.log`. After a VM reboot, run the command or a
successful workflow to bring the GUI back.

| Page | Shows | Source |
| --- | --- | --- |
| Overview | Select a month, operator and mode; departures, active lines, daily mean and coverage | Reconstructed Gold daily plans |
| Daily service | Select a month, operator and mode; departures and active lines by operating date, coverage and source export, daily CSV | Gold `daily_line_service`, published window metadata |
| Compare months | Monthly totals, percentage change, daily means, weekday profiles, line gains/losses and CSV | Reconstructed Gold daily plans, optional route-history names |
| One line | Search by number or place; service, observed versions, changes and stops in separate tabs; daily profile and CSV | Gold `daily_line_service`, route history |
| What changed | Consecutive exports: lines appeared/disappeared, verified explanations and sources, largest departure changes, stops no longer served | Route history, `config/route_*.json` |
| Replaced lines | A verified replacement: old/new departures and definition stops, with the operator source | Route history, `config/route_renumberings.json` |
| Data checks | Quality-gate results, reconciliation and retained exclusions split into valid on-demand / invalid / unclassified, CSV | Gold manifests, `excluded_service_dates`, optional route history and Silver report |

The comparison matches lines by operator and NeTEx ID. It does not infer a
replacement from similar numbers or a change in departures. Percentage change
is undefined when the earlier total is zero. Weekday profiles average over
calendar days, including days with no service. A new or missing service in a
service window does not establish that a route opened or closed; holidays
and seasonal plans also affect counts.

Daily service includes every calendar date in the selected month. A date is
covered only if a published output's window and source-calendar validity include
it for the full provider selection. Overlapping windows use the latest such
export for the whole date, even if observed later; missing lines are not
backfilled from older plans. Selection is independent of UI filters.
An uncovered date stays blank, while covered dates without matching service
are zero. The mean uses covered dates only, and the coverage card shows the
denominator. These stitched plans may come from different exports; they are
not actual operations or a single month's timetable release.

Processing now defaults to full calendar months. Older seven-day outputs need
the [full-month rebuild](../docs/full_month_processing.md), including validated
support exports for dates discarded from the 15th-of-month files. That rebuild
preserves old files and records a provider/date coverage audit. Supporting sources
do not change the original route-history observation sample.

The exclusion breakdown reads the retained rows of the selected Gold output.
One occurrence is one journey on one operating date, not one XML file. Valid
on-demand availability windows are separate from fixed departures. If older
outputs lack complete type/validity flags, the GUI reports their reasons as
unclassified instead of guessing. The historical seven-day January rerun recorded
23 valid on-demand occurrences and no invalid occurrences; the older local January
output has incomplete type flags and should not be used to infer their reasons.
Those seven-day counts are not the full-month totals.

Overview, daily service, comparisons and data checks work without route history. A single
published export can be viewed; comparisons need two months. The GUI reads Gold from
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
~/dat535-gui-env/bin/python -m unittest tests.gui.test_data tests.gui.test_analytics tests.gui.test_app -v
```
