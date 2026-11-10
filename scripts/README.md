# Operations and scheduling

These scripts are the operational entry points for the pipeline and GUI.
Keep reusable ingestion, storage, and processing logic in `pipeline/`.

Automatic ingestion is scheduled with GitHub Actions in
[`ingest-netex.yml`](../.github/workflows/ingest-netex.yml), rather than a local
machine's crontab. GitHub sends the job to a self-hosted Linux runner on the
school server, which calls `pipeline.ingestion.poll_entur_netex` as a module.
Pushes to `main` also trigger the full ingestion workflow automatically.
Scheduled output is kept under the configured `DAT535_DATA_ROOT`, outside the
runner checkout. Only a report copy is uploaded to GitHub.
See the [setup and storage notes](../docs/scheduled_ingestion.md). Downstream
processing can be run with `bash scripts/run_processing.sh`; see the
[checkpointed processing instructions](../docs/processing_pipeline.md).
The development workflow calls this after validated ingestion. Its change must
reach `main` before automatic runs use it.

After successful processing and route history, the workflow calls
`bash scripts/run_gui.sh --restart`. This stops the existing GUI on its configured
port and starts a background instance. It listens on loopback; use SSH forwarding
for port 8501. The log is `DAT535_DATA_ROOT/metadata/gui.log`. The same script
without `--restart` runs in the foreground for manual use.

ZIP migration and storage maintenance are Python modules, not extra shell helpers:
`python3 -m pipeline.storage.zip_bronze --help` and
`python3 -m pipeline.storage.maintenance --help`.

Route changes across the completed snapshots are built with
`bash scripts/run_route_history.sh --data-root /srv/dat535/data` and shown with
`bash scripts/inspect_route_history.sh --data-root /srv/dat535/data summary`; see
[route history](../docs/route_history.md).

Silver quality evidence: `bash scripts/run_quality_report.sh --data-root /srv/dat535/data --markdown docs/quality_report.md`
writes the before/after table, and `bash scripts/run_quality_trace.sh --data-root /srv/dat535/data`
traces real journeys from XML to Gold. `bash scripts/run_tests.sh` runs all tests with Spark.
See [data quality](../docs/data_quality.md).
