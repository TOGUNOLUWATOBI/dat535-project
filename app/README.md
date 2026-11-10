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
| Overview | Planned departures per snapshot and provider, all modes / bus / water | Gold `line_window_summary` |
| Line explorer | One line: departures per snapshot and per day, first/last bus, versions, changes, stops served in the window | Gold `daily_line_service`, route history |
| Network changes | One step between exports: lines appeared/disappeared with the verified interpretation and source, largest departure changes, stops no longer served | Route history, `config/route_*.json` |
| Before / after | A verified renumbering: old vs new line departures and stops, with the operator source | Route history, `config/route_renumberings.json` |
| Data quality | Gold quality-gate result and reconciliation per snapshot, route-history checks, the Silver before/after table | Gold manifests, route history, `docs/quality_report.md` |

Gold outputs built before the quality gate show the gate as *not recorded*.
The next processing run records it. Manual ZIP upload (T55) is not part of this
app; see [manual ingestion](../docs/manual_ingestion.md).

`app/data.py` holds the data access, which is tested in `tests/test_gui_data.py`;
`app/gold_app.py` holds the pages.
