# Configuration

| File | Purpose |
| --- | --- |
| `quality_thresholds.json` | Rules that decide whether Gold may publish |
| `route_renumberings.json` | Candidate or verified line replacements, with source evidence |
| `route_discontinuations.json` | Candidate or verified discontinuations, with source evidence |

Only entries explicitly marked `verified: true` are presented as confirmed.
An unmatched verified mapping fails route-history publication. Changing quality
thresholds changes Gold's implementation identity and requires a new publication
attempt; the previous outputs remain. See [data quality](../docs/data_quality.md)
and [route history](../docs/route_history.md).

Provider selection and source/output paths are command-line arguments. The
schedule is in `.github/workflows/ingest-netex.yml`; GUI appearance is in
`.streamlit/config.toml`. The runner's repository variable `DAT535_DATA_ROOT`
must point to writable persistent data outside its checkout, normally
`/srv/dat535/data`. Never commit credentials: use environment variables, GitHub
secrets or ignored local files. See [runner setup](../docs/scheduled_ingestion.md).
