# Pipeline outputs

These are local development output directories. On the school server, the same
layers live under `/srv/dat535/data`, outside the checkout.

| Directory | Contents |
| --- | --- |
| `bronze/` | Original selected XML stored in ZIPs, uploads, lineage and validation |
| `staging/` | Parsed NeTEx entity Parquet with source/frame context |
| `silver/` | Resolved, typed records, calendars and `quarantine_*` tables |
| `gold/` | Reconciled planned-service metrics and route history |

New selected raw snapshots can use `--zip-only`; processing temporarily restores
their XML and releases the verified extraction afterwards. Successful national
ZIPs are removed only with the explicit cleanup option; failed sources and
original manual uploads remain. See [storage](../docs/storage_maintenance.md).

The checkpoint registry, batch progress, reports and GUI logs also live under
the chosen data root. SQLite stores metadata; timetable facts remain in Parquet.
Rejected records live beside each Silver run, with reasons and source lineage.
There is no writer for a separate top-level `quarantine/` directory.

The original local dataset at `entur_netex_snapshots/kol_agder_2026/` remains
untouched and ignored. Generated data, logs and databases are not tracked by Git;
only this README and the active layer placeholders are kept in a fresh checkout.
See [processing contracts](../docs/processing_pipeline.md) and
[full-month rebuilding](../docs/full_month_processing.md).
