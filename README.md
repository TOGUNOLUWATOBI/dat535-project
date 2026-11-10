# DAT535 — Kolumbus and Agder timetable pipeline

A data engineering pipeline for Entur NeTEx timetable exports, filtered to
Kolumbus (`KOL`, Rogaland) and Agder Kollektivtrafikk (`AKT`). It preserves source
provenance, builds validated Parquet tables with Spark, and presents planned
service and route changes in Streamlit.

The original historical sample contains nine exports dated the 15th of each
month, January–September 2026. Additional calendar-support exports reconstruct
whole service months; scheduled ingestion adds later observations. On 10 November
2026, the school-server rebuild completed **22 jobs**, passed full-month coverage
for **January–November**, and produced route history. November includes future
published plans. These data do not establish which vehicles actually operated.
See [verified coverage](docs/monthly_coverage.md).

The GUI defaults to service dates through today in Norway. Future dates remain
in the validated dataset and can be viewed with **Include future timetable plans**;
the current month is not presented as a completed month by default.

## What is implemented

- Historical acquisition, manual ZIP ingestion and scheduled API polling.
- Source-generation pinning, checksums, XML/reference validation and lineage.
- ZIP-backed Bronze storage with temporary XML extraction during processing.
- Spark staging, reference resolution, service calendars, quarantine and Gold metrics.
- Verified checkpoints and a sequential worker per snapshot with a 2 GiB driver heap.
- Route changes, line versions and operator-sourced evidence for confirmed replacements.
- A read-only GUI with daily service, monthly comparison, line history and data checks.

Transport facts live in Parquet. SQLite holds processing state and the manual
upload catalog; a separate serving database remains planned. XML/reference and
publication checks are implemented; full NeTEx XSD/profile validation remains
outstanding.

## Repository layout

```text
app/                      Streamlit pages, Gold readers and analysis helpers
config/                   Quality thresholds and verified route-change evidence
pipeline/
  ingestion/              Historical, polling, manual and calendar-support ingestion
  processing/             Bronze staging, Silver transformations and Gold tables
  storage/                ZIP retention, restoration and storage maintenance
  quality/                Publication gates and independent source-to-Gold checks
  analysis/               Route-history construction and inspection
  checkpoints.py          SQLite attempt registry and output verification
  run_snapshot.py         One checkpointed snapshot through staging/Silver/Gold
  rebuild_monthly.py      Whole-month batch preparation, processing and coverage audit
  batch_worker.py         One Spark process for a batch operation
  spark_runtime.py        Heap configuration and JVM failure handling
scripts/                  Commands for processing, tests, inspection and the GUI
tests/                    Ingestion, processing, storage, quality, GUI and runtime tests
docs/                     Operations, project plan, decisions and validation evidence
data/                     Ignored local outputs; README and active-layer placeholders only
```

The original local dataset under `entur_netex_snapshots/` is ignored by Git.
The school server stores its datasets under `/srv/dat535/data`, outside the
checkout. Raw datasets, Parquet, databases, logs and Python environments do not
belong in Git. See the [directory map](docs/directory_structure.md).

## Ingestion

Run from the project root with Python 3.11 or newer. Acquisition uses the
standard library. Choose a new output directory for a historical build:

```bash
python3 -B -u -m pipeline.ingestion.generate_provider_dataset \
  --providers KOL AKT --from-month 2026-01 --to-month 2026-09 \
  --output entur_netex_snapshots/kol_akt_rebuild \
  --zip-only --remove-national-after-validation
```

For new API observations or a manual upload:

```bash
python3 -B -u -m pipeline.ingestion.poll_entur_netex \
  --zip-only --remove-national-after-validation

python3 -B -u -m pipeline.ingestion.ingest_local_zip \
  --source /path/to/source.zip --date 2026-01-15 --zip-only
```

A selected ZIP retains the original KOL + AKT XML and required national stops.
National ZIP removal happens only after successful validation. Failed sources
and original manual uploads remain. Historical `--resume` verifies the recorded
source and outputs before reuse. The older `download_entur_netex` CLI remains a
low-level download/Kolumbus-only diagnostic tool; the combined generator above
is the normal project ingestion path.

See [manual ingestion](docs/manual_ingestion.md),
[ZIP storage and migration](docs/storage_maintenance.md) and
[full-month rebuilding](docs/full_month_processing.md).

## Processing and GUI on Spark

Processing uses PySpark 4.2.0 and Java 17, with dependencies in
`requirements-processing.txt`. The school scripts use `~/spark-env` and the
configured Java runtime. The GUI has its own `~/dat535-gui-env`, installed from
`requirements-gui.txt`; it reads Parquet without starting Spark.

```bash
bash scripts/run_processing.sh \
  --manifest /path/to/ingestion/metadata/manifest.json \
  --data-root /srv/dat535/data

bash scripts/run_route_history.sh --data-root /srv/dat535/data
bash scripts/run_gui.sh
```

Forward server port **8501** through VS Code Remote SSH or SSH and open
`http://localhost:8501`. `bash scripts/run_gui.sh --restart` stops this app on its
configured port and starts it in the background. Its log is
`DAT535_DATA_ROOT/metadata/gui.log`. See the [GUI guide](app/README.md).

## GitHub Actions

[The workflow](.github/workflows/ingest-netex.yml) runs on pushes to `main`, manual
dispatches and cron `23 8 */3 * *`: 08:23 UTC on days 1, 4, 7, and so on each
month. This resets monthly; it is not an exact 72-hour interval. The self-hosted
runner requires labels `self-hosted`, `linux`, `dat535`, and repository variable
`DAT535_DATA_ROOT` pointing to writable persistent storage outside its checkout.

Successful runs ingest yesterday's Oslo-dated archive, consolidate verified
storage duplicates, test and process the snapshot, update history, then restart
the GUI. Only metadata reports are uploaded to GitHub. Failed processing does
not restart the GUI. See [workflow and runner setup](docs/scheduled_ingestion.md).

## Verification and project records

```bash
python3 -B -m unittest discover -s tests -t . -v
bash scripts/run_tests.sh -v
```

The first command runs dependency-free tests and skips optional integrations
when their dependencies are absent. The second uses the configured Spark
environment and requires PySpark. For GUI tests, use the GUI environment:

```bash
~/dat535-gui-env/bin/python -B -m unittest discover -s tests/gui -t . -v
```

[Processing contracts](docs/processing_pipeline.md),
[data quality](docs/data_quality.md), [route history](docs/route_history.md),
[project task plan](docs/task_readme.md), and the
[project journal](docs/project_journal.txt) describe implementation and decisions.
Dated validation reports and journal entries retain their historical scope;
older seven-day results are not evidence for the rebuilt full-month totals.
