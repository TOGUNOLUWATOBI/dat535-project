# Pipeline outputs

These directories hold or reserve storage locations for pipeline outputs:

| Directory | Intended contents |
| --- | --- |
| `bronze/` | Raw API responses and manually uploaded source files, with ingestion metadata |
| `staging/` | Parsed NeTEx entity tables before Silver transformations |
| `silver/` | Typed, connected and validated structured datasets |
| `gold/` | Published analytical tables and export files |
| `quarantine/` | Rejected records/files with reason codes and source lineage |

Local development snapshot collection writes new runs into `bronze/entur/`, each
with original source data, selected XML/ZIP, lineage and a validation report.
The GitHub workflow runs on the school's self-hosted Linux runner and writes to
`DAT535_DATA_ROOT/bronze/entur/` on persistent server storage outside the checkout.
National sources, selected XML/ZIP and reports stay there, including failed runs.
Only a report copy is uploaded to GitHub. The runner is registered on `spark`,
with `DAT535_DATA_ROOT=/srv/dat535/data`. Setup checks are stored there under
`bronze/entur/setup-check-20261024/` (metadata only) and
`bronze/entur/manual-full-20261023-20261024/` (complete manual ingestion of the
23 October archive). Original archives and selected XML remain on the server.

The existing NeTEx source dataset remains under
[`entur_netex_snapshots/kol_agder_2026/`](../entur_netex_snapshots/kol_agder_2026/).
Keeping that location preserves its current metadata paths. Staging now has a
first Spark projection for provider frames, lines, service journeys and passing
times; see [XML-to-Parquet processing](../docs/xml_to_parquet.md). The complete
processing command also writes shared-reference staging, Silver, Gold and
per-run `quarantine_*` tables. The separate top-level `quarantine/` directory is
still reserved; current rejected records stay beside their Silver run. See
[processing/checkpoint contracts](../docs/processing_pipeline.md).

Generated data is ignored by Git. The README and directory placeholders remain
available in a checkout.
