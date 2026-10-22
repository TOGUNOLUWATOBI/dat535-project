# Pipeline outputs

These directories reserve storage locations for the pipeline work still to build:

| Directory | Intended contents |
| --- | --- |
| `bronze/` | Raw API responses and manually uploaded source files, with ingestion metadata |
| `staging/` | Parsed NeTEx entity tables before Silver transformations |
| `silver/` | Typed, connected and validated structured datasets |
| `gold/` | Published analytical tables and export files |
| `quarantine/` | Rejected records/files with reason codes and source lineage |

The existing NeTEx source dataset remains under
[`entur_netex_snapshots/kol_agder_2026/`](../entur_netex_snapshots/kol_agder_2026/).
Keeping that location preserves its current metadata paths. The directories here
are placeholders; creating them does not mean those pipeline stages are complete.

Generated data is ignored by Git. The README and directory placeholders remain
available in a checkout.
