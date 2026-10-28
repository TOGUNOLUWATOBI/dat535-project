# Operations and scheduling

Place deployment helpers and maintenance commands here when needed.
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
