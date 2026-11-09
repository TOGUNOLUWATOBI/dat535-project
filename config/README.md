# Configuration

Place environment-specific pipeline settings here as ingestion and processing
are implemented. Source/provider selection and output paths are currently CLI
arguments to the acquisition tools. The cron expression is in
`.github/workflows/ingest-netex.yml`; there is no separate scheduling config.

For the school's self-hosted Linux runner, set GitHub repository variable
`DAT535_DATA_ROOT` to an existing persistent server directory, for example
`/srv/dat535/data`. It must be writable by the runner account and separate from
the code checkout and runner temporary directory. Scheduled runs write into
`DAT535_DATA_ROOT/bronze/entur/`. The workflow requires runner labels
`self-hosted`, `linux` and `dat535`; see [server setup](../docs/scheduled_ingestion.md).

Future configuration covers source endpoints, schedules, storage paths, Spark
settings, and database connection settings. Store credentials in environment
variables or local ignored files.
