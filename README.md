# DAT535 — Kolumbus and Agder timetable pipeline

Historical Entur NeTEx snapshots for Kolumbus (`KOL`) and Agder Kollektivtrafikk
(`AKT`), sampled on the 15th of each month from January through September 2026.
The data describes published planned services.

## Project structure

```text
pipeline/                 Python code
  ingestion/              Historical acquisition and scheduled snapshot collection
  processing/             Planned Bronze, Silver and Gold processing
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
Historical acquisition and a weekly GitHub Actions workflow for the school's
self-hosted Linux runner are implemented. Parsing into tables, PySpark
transformations, database storage and the GUI remain to be built. The runner is
online on `spark`; all 25 tests and a full manual CLI ingestion passed there.
The first GitHub Actions execution awaits the workflow being merged into `main`.

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

## Weekly ingestion

GitHub Actions is configured to collect KOL + AKT and shared reference XML every
Monday at **08:23 UTC**, using Sunday's archive. GitHub triggers the job; a
**self-hosted Linux runner on the school server** executes it. A manual
**Run workflow** option is included. See [runner setup, storage and validation details](docs/scheduled_ingestion.md).

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

## Verification

```sh
python3 -B -m unittest discover -s tests -v
```

Dataset files and generated outputs are ignored by Git. The source investigation
is recorded in the [audit](docs/audit/2026-10-22/README.md) and
[downloader follow-up](docs/audit/2026-10-22/downloader_followup.md). The duplicated,
misdated 2025 payloads are excluded from the project dataset.
