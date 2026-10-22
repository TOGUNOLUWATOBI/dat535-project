# Project directory structure

```text
dat535-project/
├── README.md
├── .gitignore
├── pipeline/                         Python package
│   ├── ingestion/                    Implemented source acquisition tools
│   │   ├── download_entur_netex.py
│   │   └── generate_provider_dataset.py
│   ├── processing/                   Planned parsing and PySpark processing
│   │   ├── bronze/                   NeTEx entities and source lineage
│   │   ├── silver/                   Types, calendars, joins and quality rules
│   │   └── gold/                     Analytical aggregations
│   ├── storage/                      Planned database publication
│   └── analysis/                     Planned historical service comparisons
├── app/                              Planned Streamlit GUI
├── config/                           Configuration conventions
├── scripts/                          Planned cron/deployment entry scripts
├── tests/
│   └── ingestion/                    Acquisition/generation regression tests
├── docs/
│   ├── task_readme.md                 Project scope and task breakdown
│   ├── project_journal.txt            Running account of decisions and progress
│   ├── directory_structure.md         This directory map
│   └── audit/                        Historical investigation and build evidence
├── entur_netex_snapshots/             Existing source dataset, ignored by Git
│   └── kol_agder_2026/
│       ├── README.md
│       ├── xml/                      Original XML grouped by snapshot date
│       ├── zip/                      Nine compact copies of the XML snapshots
│       └── metadata/                 Manifests, checksums, lineage and reports
└── data/                             Future generated pipeline outputs
    ├── README.md
    ├── bronze/                       New raw API responses/manual uploads
    ├── staging/                      Parsed entity tables
    ├── silver/                       Validated structured data
    ├── gold/                         Analytical outputs
    └── quarantine/                   Rejected data with reasons
```

Python package directories also contain `__init__.py`. Data output directories
contain `.gitkeep` placeholders. The processing, storage, analysis, GUI, and
scheduling directories are reserved for upcoming implementation.

## Working in the structure

Run commands from the project root. The existing Python tools have moved into
`pipeline/ingestion/` and are invoked as modules:

```sh
python3 -B -m pipeline.ingestion.download_entur_netex --help
python3 -B -m pipeline.ingestion.generate_provider_dataset --help
python3 -B -m unittest discover -s tests -v
```

The source dataset stays in its existing location; it has not been moved or
re-extracted. Process the dates listed in its `metadata/manifest.json` rather than
scanning the entire project for XML files. Keep original source XML unchanged and
write derived tables into `data/`.

Developer A's main implementation area is `pipeline/processing/` and
`pipeline/analysis/`. Developer B's main areas are `pipeline/ingestion/`,
`pipeline/storage/`, `app/`, `config/`, and `scripts/`. Both contribute to Silver
and its tests, as described in the project plan.

Current acquisition code uses the Python standard library and runs directly from
the project root without package installation. Introduce a dependency file when
PySpark, Streamlit, and the chosen database driver are needed. Configuration
loading, database storage, scheduled jobs and GUI execution are still planned
work.

Audit reports describe the project at the time of each investigation. Historical
paths and commands in those records may refer to the earlier layout; current
commands and paths are documented here and in the root README.
