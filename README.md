# DAT535 — Kolumbus and Agder timetable pipeline

Historical Entur NeTEx snapshots for Kolumbus (`KOL`) and Agder Kollektivtrafikk
(`AKT`), sampled on the 15th of each month from January through September 2026.
The data describes published planned services.

## Project structure

```text
pipeline/                 Python code
  ingestion/              Historical acquisition and scheduled snapshot collection
  processing/             XML-to-Parquet staging; planned Silver and Gold processing
  storage/                Planned database publication
  analysis/               Planned historical comparisons
app/                      Planned Streamlit GUI
config/                   Configuration conventions
scripts/                  Operational notes and future deployment helpers
tests/ingestion/          Acquisition and generation tests
docs/                     Task plan, project journal, directory map and audits
entur_netex_snapshots/    Existing original NeTEx dataset
data/                     Local development outputs; server data uses external storage
```

See the [full directory tree](docs/directory_structure.md),
[task plan](docs/task_readme.md), and [project journal](docs/project_journal.txt).
Historical acquisition and an automatic GitHub Actions workflow for the school's
self-hosted Linux runner are implemented. Complete snapshot staging and the
first checkpointed Silver/Gold processing slice are implemented. Historical
route-change modeling, database publication and the GUI remain to build.
The runner is online on `spark`; all 57 current tests passed there with
PySpark 4.2.0 and a project-specific Java 17 runtime.
The workflow is published on `main`. Full manual and automatic push-triggered
Actions ingestion passed on `dat535-spark`: [successful push run](https://github.com/TOGUNOLUWATOBI/dat535-project/actions/runs/40537981353).
Scheduled ingestion was also verified overnight on 27–28 October:
[successful scheduled run](https://github.com/TOGUNOLUWATOBI/dat535-project/actions/runs/40638229520).
The five-minute test schedule produced three successful runs hours apart, so
the configured interval is not demonstrated by the observed run history.

## Current dataset

[Open the dataset documentation](entur_netex_snapshots/kol_agder_2026/README.md).
There are nine snapshots and 4,866 XML files: **4.15 GB of provider XML**, or
**7.96 GB including national stop registries**. Compact copies total **586 MB**.
The XML and ZIPs represent the same selected source data.

The dataset retains its simple `xml/`, `zip/`, and `metadata/` layout. Provider
reference and same-file version checks passed. Agder's July–September snapshots
contain 182 repeated ID/version occurrences, preserved and flagged for
preprocessing. Registry-only reference diagnostics are recorded separately.
Full NeTEx XSD/profile validation and calendar expansion remain outstanding.

## Acquisition commands

Run from the project root with Python 3.11 or newer. The implemented tools use
the standard library and need no package installation. Document the external
dependencies when the Spark, GUI and database parts are implemented.

```sh
python3 -B -m pipeline.ingestion.generate_provider_dataset --help
python3 -B -m pipeline.ingestion.download_entur_netex --help
python3 -B -m pipeline.ingestion.poll_entur_netex --help
python3 -B -m pipeline.ingestion.ingest_local_zip --help
```

To generate a fresh copy, use a **new** output directory:

```sh
python3 -B -u -m pipeline.ingestion.generate_provider_dataset \
  --providers KOL AKT \
  --from-month 2026-01 --to-month 2026-09 \
  --output entur_netex_snapshots/kol_agder_2026_rebuild \
  --remove-national-after-validation
```

The final option removes only the national ZIP downloaded for each successfully
validated snapshot. Omit it to keep those archives. Original selected XML,
checksums and lineage are retained, including source-quality warnings.

For an interrupted build, add `--resume` with the same providers, dates, and
working output directory. Existing material is verified against its recorded
lineage. The already completed, tidied delivery under `kol_agder_2026/` should be
reproduced into a new directory rather than resumed as a working build.

## Manual ZIP ingestion

Import an existing national or KOL + AKT ZIP without downloading it again:

```sh
python3 -B -u -m pipeline.ingestion.ingest_local_zip \
  --source entur_netex_snapshots/kol_agder_2026/zip/KOL_AKT_netex_20260115.zip \
  --date 2026-01-15
```

The input is preserved. The new run retains the original ZIP, selected XML,
checksums, lineage and validation results. Repeated uploads with the same ZIP
bytes, date and providers refer to the validated first import. See
[manual ingestion and duplicate handling](docs/manual_ingestion.md).

## Automatic ingestion

The live workflow on `main` requests ingestion at **08:23 UTC on days 1, 4, 7,
and so on each month** (`23 8 */3 * *`), using the previous day's archive in Oslo.
This resets at month boundaries rather than keeping an exact 72-hour interval. It collects KOL,
AKT and shared reference XML. GitHub triggers the job; a
**self-hosted Linux runner on the school server** executes it. A manual
**Run workflow** option is included. See [runner setup, storage and validation details](docs/scheduled_ingestion.md).

Every push to `main` also triggers the full ingestion workflow automatically.

```sh
python3 -B -m pipeline.ingestion.poll_entur_netex --check-only
python3 -B -u -m pipeline.ingestion.poll_entur_netex
```

Local development runs default to `data/bronze/entur/`. Scheduled runs use
`DAT535_DATA_ROOT/bronze/entur/` on persistent server storage, outside the checkout.
Set the repository variable `DAT535_DATA_ROOT` to an existing writable server
directory, and register the runner with labels `self-hosted`, `linux`, `dat535`.
Raw data stays on the server; only a report copy is uploaded as a GitHub artifact.
No automatic server-data deletion is configured.
Repeated runs can retain the same daily archive more than once; cross-run
duplicate detection is not implemented yet.

## XML to Parquet

The complete checkpointed processing command and current Silver/Gold contracts
are documented in [processing pipeline](docs/processing_pipeline.md). It adds
shared calendar/stop projections, validated joins, quarantine tables and
reconciled planned-service metrics. Historical route-change modeling and the GUI
remain to build.

The first Spark job reads provider CompositeFrames directly from NeTEx XML and
writes staging Parquet for frames, lines, service journeys and ordered passing
times. It retains source/frame context and checks counts against Bronze
validation and Parquet read-back. See [XML-to-Parquet processing](docs/xml_to_parquet.md)
for commands, schema scope and remaining projections.

Ingestion uses only the standard library; processing requires PySpark 4.2.0
(`requirements-processing.txt`) and Java 17 or newer.

## Verification

```sh
python3 -B -m unittest discover -s tests -v
```

Dataset files and generated outputs are ignored by Git. The source investigation
is recorded in the [audit](docs/audit/2026-10-22/README.md) and
[downloader follow-up](docs/audit/2026-10-22/downloader_followup.md). The duplicated,
misdated 2025 payloads are excluded from the project dataset.
