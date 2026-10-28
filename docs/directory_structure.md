# Project directory structure

```text
dat535-project/
├── README.md
├── .gitignore
├── requirements-processing.txt       PySpark 4.2.0 for staging jobs
├── .github/
│   ├── actionlint.yaml                Recognize the custom school runner label
│   └── workflows/
│       └── ingest-netex.yml           Automatic jobs on the school Linux runner
├── pipeline/                         Python package
│   ├── ingestion/                    Implemented source acquisition tools
│   │   ├── download_entur_netex.py
│   │   ├── generate_provider_dataset.py
│   │   ├── ingest_local_zip.py        Import and validate existing ZIPs
│   │   └── poll_entur_netex.py        Collect one new dated snapshot
│   ├── processing/                   Spark staging and planned Silver/Gold processing
│   │   ├── bronze/                   XML-to-Parquet conversion and source lineage
│   │   │   ├── xml_to_parquet.py      Spark conversion and Parquet read-back checks
│   │   │   ├── netex_schema.py        Explicit nested entity schemas
│   │   │   └── staging_sources.py     Validated manifest and source selection
│   │   ├── silver/                   Types, calendars, joins and quality rules
│   │   └── gold/                     Analytical aggregations
│   ├── storage/                      Planned database publication
│   └── analysis/                     Planned historical service comparisons
├── app/                              Planned Streamlit GUI
├── config/                           Configuration conventions
├── scripts/                          Operational notes and future deployment helpers
├── tests/
│   ├── ingestion/                    Acquisition/generation regression tests
│   └── processing/                   Source selection and actual Spark tests
├── docs/
│   ├── task_readme.md                 Project scope and task breakdown
│   ├── project_journal.txt            Running account of decisions and progress
│   ├── directory_structure.md         This directory map
│   ├── scheduled_ingestion.md         School runner setup and persistent storage
│   ├── manual_ingestion.md            Existing ZIP imports and repeat uploads
│   ├── xml_to_parquet.md              Spark staging schemas, checks and usage
│   └── audit/                        Historical investigation and build evidence
├── entur_netex_snapshots/             Existing source dataset, ignored by Git
│   └── kol_agder_2026/
│       ├── README.md
│       ├── xml/                      Original XML grouped by snapshot date
│       ├── zip/                      Nine compact copies of the XML snapshots
│       └── metadata/                 Manifests, checksums, lineage and reports
└── data/                             Local development outputs, ignored by Git
    ├── README.md
    ├── bronze/entur/                 New ingestion runs with raw XML and metadata
    ├── staging/                      Parsed entity tables
    ├── silver/                       Validated structured data
    ├── gold/                         Analytical outputs
    └── quarantine/                   Rejected data with reasons
```

Python package directories also contain `__init__.py`. Data output directories
contain `.gitkeep` placeholders. Initial Spark staging is implemented; further
entity parsing, Silver/Gold transformations, database storage, analysis and the
GUI remain to build. Automatic ingestion is configured in
GitHub Actions for a self-hosted Linux runner. The runner and persistent storage
are configured on `spark`; all 25 tests and a full manual CLI ingestion passed
there. The workflow is now on `main`, and a full manual Actions run passed.
Three successful `schedule` runs were verified overnight on 27–28 October.
The earlier five-minute test produced runs hours apart. The current schedule
requests 08:23 UTC on days 1, 4, 7, and so on each month; it resets monthly.

Scheduled runs write outside the repository, under the configured absolute
`DAT535_DATA_ROOT/bronze/entur/` on the school server. That location retains raw
archives, XML, compact ZIPs and metadata across checkouts. The local `data/`
directory above is the development default, not the scheduled storage location.

## Working in the structure

Run commands from the project root. The existing Python tools have moved into
`pipeline/ingestion/` and are invoked as modules:

```sh
python3 -B -m pipeline.ingestion.download_entur_netex --help
python3 -B -m pipeline.ingestion.generate_provider_dataset --help
python3 -B -m pipeline.ingestion.poll_entur_netex --help
python3 -B -m pipeline.ingestion.ingest_local_zip --help
python3 -B -m pipeline.processing.bronze.xml_to_parquet --help
python3 -B -m unittest discover -s tests -v
```

The source dataset stays in its existing location; it has not been moved or
re-extracted. Process the dates listed in its `metadata/manifest.json` rather than
scanning the entire project for XML files. Keep original source XML unchanged and
write derived tables into `data/`.

Manual ZIP imports keep an exact uploaded copy, selected XML and validation
metadata in a new Bronze run. A shared `_manual_ingestions.sqlite3` metadata
index detects repeated manual uploads. See [manual ingestion](manual_ingestion.md).
The new manual path passed all 40 ingestion tests locally on Python 3.11 and
3.14; the real January ZIP and a repeat upload were also checked locally.

Developer A's main implementation area is `pipeline/processing/` and
`pipeline/analysis/`. Developer B's main areas are `pipeline/ingestion/`,
`pipeline/storage/`, `app/`, `config/`, and `scripts/`. Both contribute to Silver
and its tests, as described in the project plan.

Current acquisition code uses the Python standard library and runs directly from
the project root without package installation. Spark processing dependencies are
listed in `requirements-processing.txt`; the new staging job requires PySpark
4.2.0 and Java 17 or newer. Streamlit and database drivers remain to add. Configuration
loading, database storage, downstream orchestration and GUI execution are still
planned work. See [automatic ingestion](scheduled_ingestion.md) for the implemented
schedule and persistent server storage.

Audit reports describe the project at the time of each investigation. Historical
paths and commands in those records may refer to the earlier layout; current
commands and paths are documented here and in the root README.
