# Project directory structure

The repository contains source, configuration, operational commands, tests and
human-readable evidence. Large source datasets, processing outputs and local
environments are ignored by Git. This map describes the current implementation;
older journal entries retain the paths used at the time.

```text
dat535-project/
├── README.md
├── .gitignore
├── requirements-processing.txt      PySpark 4.2.0
├── requirements-gui.txt             Streamlit, pandas, PyArrow and Altair
├── .github/
│   ├── actionlint.yaml              School runner label for workflow linting
│   └── workflows/ingest-netex.yml    Ingest → process → history → GUI restart
├── .streamlit/config.toml           GUI theme
├── app/
│   ├── gold_app.py                  Pages and interaction
│   ├── analytics.py                 Daily/monthly analysis and comparisons
│   ├── data.py                      Read-only Gold/history access
│   └── README.md
├── config/
│   ├── quality_thresholds.json       Gold publication rules
│   ├── route_renumberings.json       Sourced route replacement evidence
│   ├── route_discontinuations.json   Sourced discontinuation evidence
│   └── README.md
├── pipeline/
│   ├── run_snapshot.py              Checkpointed staging → Silver → Gold
│   ├── rebuild_monthly.py           Calendar support, sequential batch and coverage
│   ├── batch_worker.py              One Spark process per batch operation
│   ├── spark_runtime.py             Heap settings and safe failure reporting
│   ├── checkpoints.py               Snapshot identities, locks and verified reuse
│   ├── ingestion/
│   │   ├── download_entur_netex.py   Source HTTP helpers and older KOL diagnostics
│   │   ├── generate_provider_dataset.py
│   │   ├── ingest_local_zip.py
│   │   ├── poll_entur_netex.py
│   │   └── prepare_monthly_support.py
│   ├── processing/
│   │   ├── bronze/                  Explicit schemas, source selection and XML projection
│   │   ├── silver/                  References, calendars, cleaning and quarantine
│   │   ├── gold/                    Reconciled planned-service metrics
│   │   └── windows.py               Service-window rules
│   ├── storage/
│   │   ├── zip_bronze.py            ZIP migration, verification and temporary extraction
│   │   └── maintenance.py           Verified hardlink consolidation and legacy cleanup
│   ├── quality/                     Profiles, gates and independent XML traces
│   └── analysis/                    Route-history construction and inspection
├── scripts/                         Processing, rebuild, inspection, testing and GUI commands
├── tests/
│   ├── ingestion/                   Source acquisition and manual-import regressions
│   ├── processing/                  XML, calendars and real Spark integration
│   ├── analysis/                    History, route versions and evidence rules
│   ├── storage/                     ZIP retention, restoration and maintenance
│   ├── quality/                     Publication gates and independent XML checks
│   ├── gui/                         Gold readers, analysis and Streamlit interactions
│   └── test_*.py                    Checkpoints, workers and service-window contracts
├── docs/                            Operations, project plan, journal and validation evidence
└── data/                            Ignored local outputs
    ├── README.md
    ├── bronze/
    ├── staging/
    ├── silver/
    └── gold/
```

Python package markers (`__init__.py`) are retained where needed for imports
and recursive unittest discovery. Rejected records are `quarantine_*` tables
inside each Silver output, rather than a separate top-level quarantine folder.

## Persistent data on the school server

```text
/srv/dat535/data/
├── bronze/entur/                    Selected raw ZIPs, lineage and validation
├── staging/entur/                   Parsed entity Parquet and source context
├── silver/entur/                    Cleaned and quarantined records
├── gold/entur/                      Published metrics and route history
├── rebuilds/monthly-*/              Batch progress and worker results
├── metadata/                       Cleanup reports and operational logs
└── pipeline_state.sqlite3           Checkpoint registry, not transport facts
```

ZIP-backed raw XML is restored at its original paths for one operation and
released after verification. Original manual uploads and failed sources remain.
See [storage maintenance](storage_maintenance.md) before changing raw retention.

Run commands from the project root. Use the module paths in the root
[README](../README.md), [scripts guide](../scripts/README.md) and
[processing contracts](processing_pipeline.md). Storage maintenance now lives
at `python3 -m pipeline.storage.maintenance`; historical journal commands may
refer to its earlier `pipeline.bronze_storage` location.

The local `entur_netex_snapshots/kol_agder_2026/` directory is preserved and
ignored. It is not included when cloning the GitHub repository. Local generated
`data/` contents and virtual environments are also preserved, not deleted by
repository cleanup. [The task plan](task_readme.md) records team responsibilities.
