# DAT535 — Kolumbus and Agder timetable history

A data engineering pipeline for Entur's national NeTEx timetable exports,
filtered to **Kolumbus** (`KOL`, Rogaland) and **Agder Kollektivtrafikk**
(`AKT`). It keeps the raw source with full provenance, builds validated Parquet
tables with Spark (Bronze → staging → Silver → Gold), tracks how routes change
between exports, and shows the result in a Streamlit GUI.

**Data.** Nine historical exports dated the 15th of each month (January–September
2026), calendar-support exports that fill whole service months, and scheduled
ingestion from November 2026. On 10 November 2026 the school-server rebuild
completed 22 jobs, passed full-month coverage for January–November and produced
route history ([verified coverage](docs/documentation.md#verified-full-month-coverage)).

These are **published plans**, not records of what vehicles actually ran. The
GUI shows service dates up to today (Norway) by default; future plans can be
switched on with **Include future timetable plans**.

## Quick start

Requires Python 3.11+ and, for Spark, Java 17.

```bash
# Environments (Spark processing and the GUI are kept separate on the server)
python3 -m venv ~/spark-env      && ~/spark-env/bin/pip install -r requirements.txt
python3 -m venv ~/dat535-gui-env && ~/dat535-gui-env/bin/pip install -r requirements.txt

# Process an ingested snapshot, build route history, start the GUI
bash scripts/run.sh processing --manifest /path/to/metadata/manifest.json --data-root /srv/dat535/data
bash scripts/run.sh history --data-root /srv/dat535/data
bash scripts/run_gui.sh                          # http://localhost:8501
```

## Pipeline

| Layer | What it holds |
| --- | --- |
| **Bronze** | Selected KOL + AKT XML and national stops in a checksum-verified ZIP, with source identity, lineage and validation reports |
| **Staging** | NeTEx entities as Parquet, one row per source occurrence, with file and frame context |
| **Silver** | Resolved references, typed times, expanded calendars; invalid rows kept with reason codes and copied to `quarantine_*` |
| **Gold** | Daily line and network service, reconciled to Silver; published only if the [quality gate](docs/documentation.md#data-quality) passes |
| **History** | Line versions, appearances, removals, route and stop changes, and operator-verified renumberings |

Every stage is checkpointed in SQLite: a completed output is reused only when
its input, code and files are unchanged, and failed attempts are kept as
evidence. Transport facts live in Parquet; SQLite holds only processing state.

## Commands

All Spark work goes through `scripts/run.sh` (run it without arguments for the
list). It uses `~/spark-env` and the project's Java 17; override with
`DAT535_SPARK_PYTHON` and `DAT535_JAVA_METADATA`.

| Command | Purpose |
| --- | --- |
| `processing` | One snapshot through staging, Silver and Gold |
| `rebuild` | Whole-month plans for every registered snapshot, one Spark process per job |
| `history` | Route changes across all completed snapshots |
| `inspect` | Read route history: `summary`, `candidates`, `month`, `line`, `date` |
| `quality-report` | Before/after Silver quality table and gate results |
| `quality-trace` | Trace real journeys from source XML through Silver and Gold |
| `tests` | The full test suite, including Spark tests |

Ingestion needs only the standard library:

```bash
# Historical months (use a new output directory)
python3 -B -u -m pipeline.ingestion.generate_provider_dataset \
  --providers KOL AKT --from-month 2026-01 --to-month 2026-09 \
  --output entur_netex_snapshots/kol_akt_rebuild --zip-only --remove-national-after-validation

# Yesterday's export (what the scheduled workflow runs)
python3 -B -u -m pipeline.ingestion.poll_entur_netex --zip-only --remove-national-after-validation

# A ZIP you already have
python3 -B -u -m pipeline.ingestion.ingest_local_zip --source /path/to/source.zip --date 2026-01-15 --zip-only
```

National ZIPs are removed only after the selected data validates; failed
sources and original uploads are always kept. Storage tools:
`python3 -m pipeline.storage.zip_bronze --help` and
`python3 -m pipeline.storage.maintenance --help`.

## Automation

[`ingest-netex.yml`](.github/workflows/ingest-netex.yml) runs on pushes to `main`,
manual dispatch and cron `23 8 */3 * *` (08:23 UTC on days 1, 4, 7, … of each
month) on a self-hosted runner labelled `self-hosted, linux, dat535`. The
repository variable `DAT535_DATA_ROOT` must point to persistent storage outside
the checkout.

Each run checks free space, runs the ingestion tests, ingests yesterday's
Oslo-dated export, consolidates verified duplicate storage, runs the Spark tests,
processes the snapshot, updates route history and restarts the GUI. Any failure stops the run before the GUI restarts. Only
metadata reports are uploaded to GitHub.

## GUI

A read-only Streamlit app over Gold and route history; it reads Parquet with
pandas and never starts Spark. `bash scripts/run_gui.sh --restart` runs it in the
background (log: `$DAT535_DATA_ROOT/metadata/gui.log`). It listens on
`0.0.0.0:8501` (all interfaces, including localhost). VS Code or SSH port
forwarding to `http://localhost:8501` still works. For direct school-server
access, open `http://152.94.163.144:8501` when inbound TCP 8501 is allowed by
the school/cloud and host firewalls. The app has no login. Pages:
Overview, Daily service, Compare months, One line, What changed, Replaced lines
and Data checks. See the [GUI guide](docs/documentation.md#gui).

## Configuration

| File | Purpose |
| --- | --- |
| `config/quality_thresholds.json` | Rules that decide whether Gold may publish |
| `config/route_renumberings.json` | Line replacements, with operator source evidence |
| `config/route_discontinuations.json` | Discontinued lines, with operator source evidence |
| `.streamlit/config.toml` | GUI theme |

Only entries marked `verified: true` are shown as confirmed, and a verified entry
that matches nothing fails route history. Changing the thresholds creates a new
Gold attempt and keeps the old one. Never commit credentials.

## Repository layout

```text
app/            Streamlit pages, Gold readers and analysis helpers
config/         Quality thresholds and route-change evidence
pipeline/
  ingestion/    Historical, scheduled, manual and calendar-support ingestion
  processing/   bronze/ (XML → staging), silver/, gold/, service windows
  storage/      ZIP retention, temporary XML restore, verified deduplication
  quality/      Publication gate and independent source-to-Gold checks
  analysis/     Route history and its inspection CLI
  run_snapshot.py, rebuild_monthly.py, batch_worker.py
  checkpoints.py, spark_runtime.py, common.py
scripts/        run.sh (Spark commands) and run_gui.sh
tests/          Unit and Spark integration tests; fixtures.py holds shared test data
docs/           documentation.md (all guides, evidence and journal); evidence/ generated reports
```

Data, Parquet, databases, logs and virtual environments are not in Git. Local
runs write to the ignored `data/`; the school server uses `/srv/dat535/data`:

```text
/srv/dat535/data/
├── bronze/entur/           Raw ZIPs, uploads, lineage and validation
├── staging/entur/          Entity Parquet with source context
├── silver/entur/           Cleaned records, calendars, quarantine_* tables
├── gold/entur/             Published metrics and route history
├── rebuilds/monthly-*/     Batch progress and worker results
├── metadata/               Maintenance reports and GUI log
└── pipeline_state.sqlite3  Checkpoint registry
```

## Tests

```bash
python3 -B -m unittest discover -s tests -t . -v   # skips Spark/GUI tests when PySpark/Streamlit are missing
bash scripts/run.sh tests -v                       # everything, with Spark
~/dat535-gui-env/bin/python -B -m unittest tests.test_app tests.test_analytics tests.test_data -v
```

## Documentation

Everything is in one file, [docs/documentation.md](docs/documentation.md):

| Section | Covers |
| --- | --- |
| [Project definition](docs/documentation.md#project-definition) | Scope, terms and claims |
| [Ingestion and raw storage](docs/documentation.md#ingestion-and-raw-storage) | Scheduled runs, runner setup, manual uploads, ZIP storage |
| [Processing](docs/documentation.md#processing) | Checkpoints, tables, calendars, full-month rebuild |
| [Data quality](docs/documentation.md#data-quality) | Measurements, publication gate, journey traces |
| [Route history](docs/documentation.md#route-history) | Change detection and verified evidence |
| [GUI](docs/documentation.md#gui) | Pages and how figures are calculated |
| [Evidence](docs/documentation.md#evidence) | Dated verification records |
| [Task plan](docs/documentation.md#task-plan) and [journal](docs/documentation.md#project-journal) | Original plan, dated decisions and results |

The generated [quality report](docs/evidence/quality_report.md) and
[journey traces](docs/evidence/quality_traces.md) stay as separate files: the
`quality-report` and `quality-trace` commands write them and the GUI shows the
report. Dated records keep their original scope; older seven-day results are not
evidence for the full-month totals.
