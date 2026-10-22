# DAT535 — Kolumbus and Agder timetable pipeline

Historical Entur NeTEx snapshots for Kolumbus (`KOL`) and Agder Kollektivtrafikk
(`AKT`), sampled on the 15th of each month from January through September 2026.
The data describes published planned services.

## Project structure

```text
pipeline/                 Python code
  ingestion/              Download and dataset generation tools
  processing/             Planned Bronze, Silver and Gold processing
  storage/                Planned database publication
  analysis/               Planned historical comparisons
app/                      Planned Streamlit GUI
config/                   Configuration conventions
scripts/                  Planned scheduling and deployment scripts
tests/ingestion/          Acquisition and generation tests
docs/                     Task plan, project journal, directory map and audits
entur_netex_snapshots/    Existing original NeTEx dataset
data/                     Future raw ingestion, staging, Silver, Gold and quarantine
```

See the [full directory tree](docs/directory_structure.md),
[task plan](docs/task_readme.md), and [project journal](docs/project_journal.txt).
Source acquisition is implemented; parsing into tables, PySpark transformations,
database storage, API polling, scheduling, and the GUI remain to be built.

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

## Verification

```sh
python3 -B -m unittest discover -s tests -v
```

Dataset files and generated outputs are ignored by Git. The source investigation
is recorded in the [audit](docs/audit/2026-10-22/README.md) and
[downloader follow-up](docs/audit/2026-10-22/downloader_followup.md). The duplicated,
misdated 2025 payloads are excluded from the project dataset.
