# DAT535 — Kolumbus punctuality

A Spark medallion pipeline that measures how punctual **Kolumbus** (`KOL`,
Rogaland) buses and boats are in 2026. It compares the times vehicles actually
reported (Entur **SIRI ET** real-time data) with the **live schedule** they
aimed for and with the **printed timetable** (Entur **NeTEx**), on 53 sampled
operating days from 2026-01-01 to 2026-11-19.

```text
Bronze  ─►  Staging  ─►  Silver  ─►  Gold  ─►  GUI
raw NeTEx ZIPs     NeTEx XML as      planned and reported     punctuality,      Streamlit
+ SIRI ET Parquet  entity Parquet    stop calls, reconciled   service, quality  app
```

All data lives on the school server under `/srv/dat535/data/`
(`bronze/`, `staging/`, `silver/`, `gold/`); nothing is stored in Git.

## Repository layout

```text
src/
  app/              gold_app.py: read-only Streamlit GUI over Gold; run_gui.sh starts it
  infrastructure/   Spark standalone cluster setup (setup_spark.sh, README)
  notebooks/        inspect_bronze.py (Bronze inspection), inspect_gold.ipynb (Gold walkthrough)
  pipeline/
    Bronze/         NeTEx (Entur GCS) and SIRI ET (BigQuery) downloaders
    Staging/        NeTEx XML → entity Parquet (stage_netex.py + scripts/)
    Silver/         Calendars, planned and reported stop calls, reconciliation (build_silver.py + scripts/)
    Gold/           Punctuality, service and quality tables (build_gold.py + scripts/)
tests/test_app.py   Every GUI page renders against the Gold run
.github/workflows/  gui.yml: test and redeploy the GUI on the server
docs/               Project definition and report (PDF)
```

## Running the layers

Requires Python 3.11 with `~/spark-env` (PySpark 4.2) and Java 17 for Spark,
and `~/dat535-gui-env` for the GUI (`pip install -r requirements.txt` in each).

| Layer | Command |
| --- | --- |
| Spark cluster | `bash src/infrastructure/setup_spark.sh start` (see `src/infrastructure/README.md`) |
| Bronze | `python src/pipeline/Bronze/Netex/bronze_entur_netex.py --mode monthly-step --months 1-11`<br>`python src/pipeline/Bronze/SIRI_ET/bronze_entur_siri_et.py --mode monthly-step --months 1-11` |
| Staging | `bash src/pipeline/Staging/scripts/stage_netex_2026_quarters.sh` |
| Silver | `bash src/pipeline/Silver/scripts/run_silver_2026.sh <run-id> [dates]` |
| Gold | `bash src/pipeline/Gold/scripts/run_gold_2026.sh <run-id>` |
| GUI | `bash src/app/run_gui.sh --restart` |

Both Bronze downloaders take `--mode single-day --date YYYY-MM-DD` for one day
and skip dates already verified on disk unless `--force` is given.

## Gold

`src/pipeline/Gold/` reads the published Silver runs, picks one authoritative
run per date and writes one immutable run to `gold/kol/runs/<run-id>/`. The
current run is `gold-kol-53d-2026-v2` (53 of 53 dates, quality gate passed).

| Table | One row per |
| --- | --- |
| `daily_network_punctuality` / `period_network_punctuality` | date, or season/month × mode; × event × baseline |
| `daily_line_punctuality` / `period_line_punctuality` | the same per line |
| `daily_stop_punctuality` | date × quay × event × baseline |
| `daily_line_service` / `period_line_service` | line: scheduled, tracked and cancelled trips |
| `journey_performance` | date × trip: first-stop and last-stop delay |
| `daily_data_quality` | date: matching coverage, live-feed availability, exclusion reasons |

`baseline` is `et_aimed` (live schedule) or `netex_planned` (printed timetable);
`event_type` is `departure` or `arrival`. A departure is on time when 0–5 min
late (earlier counts as early), an arrival when at most 5 min late; very late
is more than 15 min. The rules are stored in `gold_manifest.json`. Rates with
nothing to measure are `NULL`, not 0. See `src/notebooks/inspect_gold.ipynb` for the
walkthrough.

**Known issues in `gold-kol-53d-2026-v2`:**

- Real-time trips are not linked to timetable trips. The trip key uses the
  dated trip ID when present (`dsj::…::sj::…`), which real-time rows always
  have and timetable trips never have (`sj::…`). So `journey_performance` has
  no first/last-stop delays and `scheduled_with_et_journey_count` is 0.
  Stop-level punctuality is not affected.
- 2,586 quay-days differ from the timetable by more than an hour (up to 6 h),
  most likely a date/time matching issue. It drags the *mean* NeTEx delay far
  below zero; medians and rates are not affected.

## GUI

`src/app/gold_app.py` is a read-only Streamlit app over the Gold run. It never
starts Spark: small tables are read with pandas, the large
`daily_stop_punctuality` with PyArrow filter pushdown. Stop names and the stops
each line serves come from Silver, since Gold only stores quay IDs.

`bash src/app/run_gui.sh --restart` runs it in the background on
`0.0.0.0:8501` (log: `/srv/dat535/data/metadata/gui.log`). Open
`http://152.94.163.144:8501`, or forward port 8501 over SSH/VS Code. No login.
Override `DAT535_GOLD_RUN`, `DAT535_SILVER_RUNS`, `DAT535_GUI_PORT` and
`DAT535_GUI_PYTHON` as needed.

The sidebar filters every page by time period (a season or a month), bus or
boat (never blended), and departures or arrivals. Times are measured against
the live schedule; the Overview and each line's detail also show the printed
timetable side by side.

| Page | Answers |
| --- | --- |
| 📊 Overview | How punctual is Kolumbus? On time, late, very late, typical and worst-5% delay, trips tracked and cancelled; live schedule vs timetable; the year by season or month; bus vs boat; day by day |
| 🚏 Lines | Which lines perform worst or best? Searchable ranking, then one line's departures/arrivals and daily trend |
| 📍 Stops | Where do delays build up? Named quays ranked by late rate, for a period or one day, optionally on one line |
| 🔄 Changes | When did lines start or end, and when did a line's route change (stops added or dropped)? Read from Silver's planned stop calls; dated to the first sampled day a change shows up |
| 🩺 Data quality | Can the numbers be trusted? Quality gate, live feed, matching coverage, why stops were not measurable |

Every period number is shown with its sample (days, date range, weekdays),
because each month is only 4–5 sampled days.

## Deployment

`.github/workflows/prod.yml` runs daily at **07:15 UTC** and after every push to
`main`, including merged pull requests, using that exact commit. It replaces the
GUI-only workflow and
runs **Bronze NeTEx + SIRI ET → Staging → Silver → Gold → all GUI page checks**
for **yesterday in Europe/Oslo**. Only the new day's Bronze, Staging and Silver
are processed; Gold includes the existing Silver history plus that day. Each
build writes new outputs under
`/srv/dat535/data/prod/runs/gh-<run-id>-<attempt>/`, with Gold run ID `prod-gold`.
After all checks pass, it restarts the production GUI on **8501** against those
outputs and checks that the port answers. After checks pass, new Bronze files
and manifest records are added to `/srv/dat535/data/bronze/`, new Silver days
are registered under `/srv/dat535/data/silver/kol/runs/`, and the cumulative
Gold run is registered under `/srv/dat535/data/gold/kol/runs/`. Staging outputs
remain in the dated run directory, recorded in `metadata/production_runs/`.
Registered Silver and Gold paths link to successful production run directories;
retain those directories while they are referenced by history or the live GUI.
Existing dates are retained; reruns reuse existing Silver authority and keep
the original Bronze files. Failed builds keep their logs and leave the running
GUI alone.
Pushes to the development branch validate the production workflow without
starting a production build or deployment.

Manual runs can select dates and a starting layer. Production deployment is
allowed only from `main` and must be selected for manual runs. Branch runs can
test the complete pipeline without changing the live GUI. Development and
production share a processing concurrency group to avoid competing Spark builds
on the VM, and use separate checkout and data directories for each run. The
running GUI keeps its original checkout while the next build is tested. Both workflows keep
logs and elapsed times as seven-day artifacts. Historical upstream files are
reused; cumulative Gold is rebuilt from all authoritative Silver dates, so its
runtime grows with history.
Gold uses a 1 GB driver heap with 16 MiB scan partitions and four shuffle
partitions to leave memory for the running GUI and Actions runner.

The `dat535` runner service must be running on the server:
`sudo systemctl start actions.runner.TOGUNOLUWATOBI-dat535-project.dat535-spark.service`.

### Daily development workflow

`.github/workflows/dev.yml` is a separate **Dev workflow** for
`ft/further-refactoring-and-cleanup`:

- Every day at **07:15 UTC** (08:15 Oslo in winter, 09:15 in summer), it runs
  **Bronze NeTEx + SIRI ET → Staging → Silver → Gold → GUI smoke checks**.
  The default date is **yesterday in Europe/Oslo**, calculated when the run starts.
- Branch pushes run syntax, workflow and orchestration checks. They do not
  trigger another cloud download or full Spark build.
- Manual runs can select `bronze`, `staging`, `silver` or `gold` as the starting
  layer. All downstream layers still run. Supply `yesterday`, comma-separated
  dates, or `manifest` for the existing Bronze manifest's downloaded dates.
- Each execution writes to a fresh
  `/srv/dat535/data/dev/runs/gh-<run-id>-<attempt>/`. Starting below Bronze reads
  the existing published upstream inputs; it does not overwrite them. Silver
  backfills are split into at most five dates per batch.
- Scheduled successes deploy the development GUI on **8502**. Manual runs can
  opt into deployment. The main GUI on **8501** uses the production workflow.
  Development uses a separate checkout directory and the exact validated commit.
- Layer logs and elapsed times are attached as seven-day GitHub artifacts and
  recorded in the run's `metadata/run.json`. Failed runs retain their diagnostics;
  a failed build does not deploy its GUI. Data directories remain on the VM and
  need to be included in the project's disk-retention policy, together with the
  per-run checkout directories on the runner.

GitHub schedules and the manual Run workflow button require the workflow to be
registered on the **default branch**. Add the workflow there after review; it
explicitly checks out the development branch, not main's pipeline code. See
[GitHub's workflow-trigger documentation](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow).
The self-hosted `dat535` runner must have the documented Spark/GUI environments,
Java and Google Application Default Credentials with BigQuery access. Set the
repository variable `GCP_PROJECT_ID` if its billing project is not supplied by
those credentials. The development workflow uses the existing VM credential file
`/home/ubuntu/.config/gcp/dat535-bq-key.json`; the repository variable
`GOOGLE_APPLICATION_CREDENTIALS` can override this path. The runner user must be
able to read the file. Credential contents stay on the VM.

Each development run is an isolated **previous-day snapshot**. Production keeps
the historical dates and includes the new day in its Gold build. Existing Gold calculations and gates are unchanged,
including the currently fixed 2026 season windows; extending seasonal analytics
to December and subsequent years requires a separate Gold-policy update.

Preview the commands without creating outputs or launching Spark:

```bash
python .github/scripts/run_dev_pipeline.py --start-layer staging \
  --dates 2026-01-01 --run-root /tmp/dat535-dev-preview --dry-run
```

## Tests

```bash
python -B -m unittest discover -s tests -t . -v
```

The suite uses temporary fixtures and mocked cloud calls. Install `requirements.txt`
and Java 17 to run the Spark and Parquet tests. Historical GUI integration tests
are opt-in. See [tests/README.md](tests/README.md) for commands, coverage and the
five known Gold regressions currently recorded as expected failures.

## Data licence

Data from Entur, under the Norwegian Licence for Open Government Data (NLOD).
