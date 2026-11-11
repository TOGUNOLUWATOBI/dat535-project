# DAT535 documentation

Everything about the Kolumbus and Agder timetable pipeline in one place. The
[repository README](../README.md) has the quick start and command summary.

1. [Project definition](#project-definition) — scope, terms and claims
2. [Ingestion and raw storage](#ingestion-and-raw-storage) — scheduled runs, runner, manual uploads, ZIP storage
3. [Processing](#processing) — checkpoints, tables, calendars, full-month rebuild
4. [Data quality](#data-quality) — measurements, publication gate, journey traces
5. [Route history](#route-history) — change detection and verified evidence
6. [GUI](#gui) — pages and how figures are calculated
7. [Evidence](#evidence) — dated verification records
8. [Task plan](#task-plan) — original six-week breakdown
9. [Project journal](#project-journal) — dated decisions and results

Two generated reports stay as separate files because the commands write them and
the GUI reads one: [quality_report.md](evidence/quality_report.md) and
[quality_traces.md](evidence/quality_traces.md).

## Project definition

*Draft for team agreement.*

This page fixes what the project claims and what its words mean. The report,
Gold tables, route history and demo should all use these definitions. Items
marked **Decision** need agreement from both team members.

### Purpose

Keep a reproducible history of the **planned public-transport timetables**
that Kolumbus (Rogaland) and AKT (Agder) publish through Entur, and show how
the published network changes between exports.

Questions the outputs answer:

1. Which lines appeared, disappeared, changed route or changed bus number
   between two exports, and which of these changes are confirmed by an operator
   source?
2. How many planned departures, and which stops, does each network serve in a
   full calendar month, with daily means and weekday profiles, and how does that change over the year
   (holidays, summer timetables, network redesigns)?
3. For a given line and date, which version of the line applies and how many
   departures are planned?

**Decision: consumer.** Proposed: a transport analyst or journalist who wants
to know what changed in the published network and how sure we are of it. They
use the route-history CLI (`scripts/run.sh inspect`) and an
analytical report or notebook with fixed queries, charts and verified case
studies. A Streamlit GUI is optional and comes after these.

### Scope

| Item | Definition |
| --- | --- |
| Source | Entur national NeTEx timetable exports, filtered to `KOL` and `AKT` |
| Licence | Norwegian Licence for Open Government Data (NLOD). Credit "Data from Entur" with a link to the licence; Entur gives no accuracy warranty |
| Historical sample | Nine exports dated the 15th of each month, January – September 2026 |
| Ongoing sample | Scheduled ingestion on the server; first processed scheduled snapshot 8 November 2026 |
| Excluded | Misdated 2025 payloads that were byte-identical to later files (see README) |
| Modes | **Decision.** Proposed: multimodal (bus and water), with bus-only figures reported separately. In the historical seven-day baseline, about 8.5% of departures were boats (44,534 of 521,185); this is not the rebuilt monthly total |
| Service type | Fixed scheduled services. Flexible (on-demand) services are kept in Silver but excluded from departure totals |

Do not write "bus departures" for an all-mode total. Use "planned departures",
or filter `transport_mode = 'bus'`.

### What the dates mean

| Term | Meaning |
| --- | --- |
| **Snapshot date** | The date of the export we downloaded. It is an *observation* date: what had been published by then |
| **Service window** | Full calendar month by default. Dates are bounded by source validity; auxiliary validated exports fill early-month gaps. Older seven-day outputs require the full-month rebuild |
| **Service date** | The operating day of a journey in the window. Times after midnight belong to the previous service date (e.g. 26:45) |
| **first_observed / last_observed** | First and last snapshot in which a line version was published |
| **previous_snapshot** | The snapshot before `first_observed`; the change was published after it |
| **Effective date** | When a change started for passengers. Only known from an operator source quoted in the evidence files, never inferred from snapshots |

A difference between two snapshots means that the published data changed in
that interval. An export can announce a future timetable or correct an earlier
one. A line missing from an export is "no longer observed". It is called
renumbered or discontinued only with a verified source.

### What the counts mean

| Measure | Definition |
| --- | --- |
| Line | `(provider, NeTEx line ID)`. A new ID is a new line unless a verified renumbering links it |
| Stop | National stop place (`NSR:StopPlace`). Platform (quay) changes inside one stop place are not route changes |
| Planned departures | Fixed, valid journey occurrences with a service date in the window |
| Stops in route definitions | Stops in a line's published journey patterns (route structure) |
| Stops served in the window | Stops of fixed journeys that operate on a date in the window |

The two stop measures differ a lot in holiday weeks. In February 2026, AKT
publishes 3,744 stops but serves 2,269 in the window. Reports must say which
measure they use.

### Claims we make and do not make

We claim:

* The outputs reproduce exactly what each export published for the window,
  with reconciled counts from Silver to Gold to route history.
* Verified route changes match quoted operator notices.

We do not claim:

* That buses ran as planned (no real-time data).
* Effective dates of changes, except where an operator source states them.
* Actual daily observations between snapshots. Monthly plans are reconstructed from dated exports, with source provenance for each service date.
* Multi-node scalability. Processing runs on one server (`local[2]`).

### Why Spark

Each export is nested XML. The nine historical snapshots hold 263,757 journey
occurrences and 8,296,954 passing-time occurrences. Silver needs large joins
(passing times to patterns, stops and calendars) and window operations that
are repeated for every snapshot, and the whole history is reprocessed when
rules change. Spark gives the schema-aware XML parsing, distributed joins and
checkpointed reruns this needs. Pandas on one machine would need hand-written
chunking.

### Open decisions

1. Consumer and main deliverable (proposed above).
2. Multimodal versus bus-only scope (proposed: multimodal, bus reported separately).
3. Quality thresholds that stop or warn before Gold is published. Proposed and
   implemented: any rejection warns, more than 1% (or any ambiguous reference)
   stops. See [data quality](#data-quality) and `config/quality_thresholds.json`.

## Ingestion and raw storage

### Automatic ingestion on the school Linux server

The historical January–September 2026 dataset is our starting data. The workflow
in [ingest-netex.yml](../.github/workflows/ingest-netex.yml) collects new data
using the previous day's dated Entur archive in Europe/Oslo. The live workflow
on `main` uses `23 8 */3 * *`: 08:23 UTC on days 1, 4, 7, and so on each month.
The day-of-month sequence resets monthly, so this is not an exact rolling
72-hour interval. Using the previous day avoids depending on the completion
time of that morning's export.

Every push to `main` also triggers full ingestion. The push trigger was verified
on 27 October; scheduled ingestion was verified overnight on 27–28 October.
The earlier five-minute test produced runs hours apart; the new schedule is
a requested trigger time, not a guarantee of exact delivery.

GitHub handles the timer and sends the job to a **self-hosted Linux runner on
the school server**. The server executes the Python command and keeps the data
on its persistent disk. The workflow targets runner labels
`[self-hosted, linux, dat535]`. See [GitHub's runner label documentation](https://docs.github.com/en/actions/how-tos/manage-runners/self-hosted-runners/use-in-a-workflow).

This polls Entur's public archive metadata/download API for planned timetables.
It does not collect real-time vehicle positions or delays. The workflow runs ingestion, verifies duplicate-storage consolidation, tests
Spark transformations, and updates Silver, Gold and route history. Database
publication remains separate.

#### Current installation — 24 October 2026

| Setting | Installed value |
| --- | --- |
| Server SSH alias | `spark` |
| Operating system | Ubuntu 24.04.4 LTS, x86_64 |
| Python | 3.11.15 |
| GitHub repository | `TOGUNOLUWATOBI/dat535-project` |
| Runner | `dat535-spark`, version 2.338.1 |
| Labels | `self-hosted`, `Linux`, `X64`, `dat535` |
| Runner directory | `/home/ubuntu/dat535-runner` |
| Service account | `ubuntu` |
| Persistent storage | `/srv/dat535/data` |
| Repository variable | `DAT535_DATA_ROOT=/srv/dat535/data` |
| Systemd service | `actions.runner.TOGUNOLUWATOBI-dat535-project.dat535-spark.service` |

The service is enabled at boot and GitHub reports the runner online. All 25
ingestion tests passed on the server. A live metadata-only check for the
23 October archive succeeded, writing its manifest to
`/srv/dat535/data/bronze/entur/setup-check-20261024/metadata/manifest.json`.
The existing `group04` runner on the same server remains active.

The ingestion setup was merged into `main` on 26 October via
[PR #1](https://github.com/TOGUNOLUWATOBI/dat535-project/pull/1). Full manual
command execution on the server and a manual GitHub Actions run have passed.

#### GitHub Actions verification — 26–27 October 2026

The complete workflow was dispatched through GitHub Actions on `main` and
succeeded on `dat535-spark`: [manual Actions run 40388358483](https://github.com/TOGUNOLUWATOBI/dat535-project/actions/runs/40388358483).
The job ran from 19:37:16 to 19:38:08 UTC. Storage preflight, checkout, all 25
tests, source acquisition, XML/reference validation and report upload passed.

The saved manifest records `github_event_name=workflow_dispatch` and
`runner_name=dat535-spark`. It contains the 25 October snapshot: 532 XML files,
399,331,448 provider XML bytes and 1,269,673 provider reference occurrences.
There were zero unresolved provider references and zero same-file version
failures. The source's 65 repeated AKT ID/version occurrences were preserved as
warnings, giving ingestion status `complete_with_warnings`.

Automatic scheduling was still unverified during the checks on 27 October, with no
`event=schedule` runs. Temporary five-minute and single-date probes did not
produce a scheduled run. Minimal controls on both the school runner and a
GitHub-hosted runner passed when dispatched manually, but neither received a
scheduled event. This establishes where the evidence stops; it does not prove
the underlying cause is a GitHub service fault.

The temporary probe workflows have been removed from `main` and archived
outside the local workflow directory. On 27 October the production schedule was
changed from Mondays to hourly at the user's request, then to a five-minute
test directly on `main`. No automatic
downloads were observed during the earlier checks, and no raw data was removed.
A manual dispatch proves that the
Actions job executes, but a successful run with `event=schedule` is still
needed to confirm automatic ingestion. Both event type and runner name are
recorded in ingestion manifests.

On 27 October we added `push: branches: [main]`. Publishing that workflow change
automatically started [run 40537981353](https://github.com/TOGUNOLUWATOBI/dat535-project/actions/runs/40537981353)
with `event=push`, at commit `c4fdda51e9bf043562eee53fb5a05fb2788835c9`.
The job ran on `dat535-spark` from 16:08:39 to 16:09:31 UTC and all steps passed,
including tests, full ingestion and report upload. Its downloaded manifest
independently records `github_event_name=push`, the 26 October snapshot,
532 XML files and ingestion status `complete_with_warnings`. The 65 repeated
AKT ID/version occurrences were preserved as source warnings. This verifies
automatic ingestion through a push event; a successful cron run was still
needed at that point.

#### Scheduled ingestion verified — 28 October 2026

The overnight history contains three successful runs with `event=schedule`:
27 October at 21:51, and 28 October at 01:05 and 06:28, all in Europe/Oslo.
Each job started about two seconds after its run was created and took roughly
53 seconds on `dat535-spark`. These recorded jobs were not waiting on the runner.

The latest [scheduled run 40638229520](https://github.com/TOGUNOLUWATOBI/dat535-project/actions/runs/40638229520)
passed all steps. Its downloaded manifest independently records
`github_event_name=schedule`, the 27 October snapshot and 532 XML files.
Reference validation found no unresolved provider references or same-file
version failures. The 65 repeated AKT ID/version occurrences were preserved as
warnings, resulting in `complete_with_warnings`.

This confirms automatic scheduled ingestion. It does not confirm delivery at
the requested five-minute interval: the observed runs were hours apart.
GitHub documents possible scheduler delays and dropped runs under load, but
these observations do not establish the cause of this repository's sparse
schedule delivery. A small verification record is saved in
`audit/2026-10-28/actions_scheduled_verification.json`.

#### Full manual CLI test — 24 October 2026

The following command was run from
`/home/ubuntu/dat535-runner/project-check` on `spark`, at commit `3e9e434`:

```sh
python3 -B -u -m pipeline.ingestion.poll_entur_netex \
  --date 2026-10-23 \
  --output /srv/dat535/data/bronze/entur/manual-full-20261023-20261024
```

It completed in 43.5 seconds with status `complete_with_warnings`:

| Result | Value |
| --- | --- |
| Original national ZIP retained | 282,505,026 bytes |
| Selected XML documents | 532: 322 KOL, 209 AKT and national stops |
| Provider XML | 409,277,177 bytes |
| XML including national stops | 822,336,592 bytes |
| Compact selected ZIP | 63,454,934 bytes |
| Provider reference occurrences checked | 1,296,889 |
| Unresolved provider references | 0 |
| Same-file version failures | 0 |
| Repeated AKT ID/version occurrences | 65, preserved as source warnings |

After completion we independently verified source size/MD5/SHA-256, all selected
XML hashes, selected ZIP CRCs and source member names/sizes/CRCs. A second call
with the same output path was refused with `FileExistsError`; the complete file
inventory was unchanged. No data was removed. Registry-only diagnostics remain
separate, and full XSD/calendar validation is still outside these checks.

Evidence is saved on the server as
`metadata/manual_ingestion_test.json` inside that run. A small local copy is at
`docs/audit/2026-10-24/manual_ingestion_test.json` (local evidence, ignored by Git).
This evidence covers the manual CLI. The later manual GitHub Actions dispatch
also passed, as recorded above. A later existing-ZIP CLI/service was implemented
and tested locally on 28 October; see manual ZIP ingestion.
The GUI upload interface is still to build.

To check the runner from a shell on `spark`:

```sh
sudo systemctl status actions.runner.TOGUNOLUWATOBI-dat535-project.dat535-spark.service
sudo journalctl -u actions.runner.TOGUNOLUWATOBI-dat535-project.dat535-spark.service -n 50 --no-pager
```

#### Set up the school server

1. Install Git, Bash and Python 3.11 or newer, including system time-zone data,
   on Linux. Run the runner under a dedicated account with write access to our
   data directory. The ingestion code uses only the Python standard library.
2. In the repository, open **Settings → Actions → Runners → New self-hosted
   runner**. Select Linux and the server's architecture, then follow GitHub's
   current download and registration commands. Add the custom label `dat535`.
   Use a current runner version; `actions/checkout@v5` requires 2.327.1 or newer.
   See [adding a runner](https://docs.github.com/en/actions/how-tos/manage-runners/self-hosted-runners/add-runners).
3. Install the runner as a service so it starts after a reboot. For a Linux
   server using systemd, run these commands from the registered runner directory:

   ```sh
   sudo ./svc.sh install
   sudo ./svc.sh start
   sudo ./svc.sh status
   ```

   The account running the service must be the account with access to the data.
   See [GitHub's service setup](https://docs.github.com/en/actions/how-tos/manage-runners/self-hosted-runners/configure-the-application).
4. Prepare a persistent data directory outside the runner's checkout and
   temporary directories. For example, ask the server administrator to create
   `/srv/dat535/data` and give the runner account write access. The actual
   school storage mount can be used instead; this path is an example.
5. In **Settings → Secrets and variables → Actions → Variables**, add the
   repository variable `DAT535_DATA_ROOT` with that absolute path. It must already
   exist on the server. This is a path setting, not a secret.

The workflow checks the Python version, storage path and write access **before
checkout**. Missing storage or a path inside the checkout/temporary directory
fails the job. This matters because checkout's normal cleanup can remove ignored
files; source data belongs outside that tree. See the [checkout cleanup setting](https://github.com/actions/checkout/blob/v5/action.yml).

The server needs outbound access to GitHub and Entur's Google Storage endpoints.
It must be powered on with the runner service online to execute the job. Without
an available runner, GitHub queues the job; it does not run it on another school
machine without matching labels. See [runner routing and connectivity](https://docs.github.com/en/actions/reference/runners/self-hosted-runners).

#### Enable the automatic workflow

1. Keep the ingestion workflow, Python module and tests on the default branch,
   `main`. The initial setup has already been merged there.
2. In **Actions**, open **Ingest Entur NeTEx**, then **Run workflow**. Leave the
   date blank to use yesterday in Oslo. Choose `check_only` for a small metadata
   check first, then run normally to collect and validate an actual snapshot.
3. Check the run's logs, server output directory and downloadable report.
   Subsequent scheduled runs use the same command automatically. No Entur API key
   is needed. Runner registration and the storage variable must be configured
   before the first run.

GitHub schedules require the workflow on the default branch. They may start
late, and public repositories can have inactive schedules disabled after 60 days
without repository activity. See [GitHub's schedule documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).

#### What each run does

1. Run the ingestion tests before accessing the source.
2. Request metadata for one date, recording its generation, source creation time,
   expected size and MD5. Download that exact object generation.
3. Verify source size, MD5, SHA-256 and the national ZIP's member CRCs.
4. Select `KOL_*`, `AKT_*`, their provider shared XML, and the full `_stops.xml`.
   Keep the original selected XML bytes in a verified compact ZIP (`--zip-only`).
5. Reuse our XML parsing, publication-date and provider reference checks. Record
   source repeats as warnings; missing provider references fail the run.
6. Remove the fetched national ZIP only after extraction, validation and compact-ZIP verification pass.
   Keep the selected ZIP, metadata, source URL, generation and checksums. Consolidate
   checksum-verified identical files through hardlinks, preserving every path.
   Failed or interrupted sources are retained. Upload a copy of the report to GitHub.
   Failed validation marks the server manifest as failed and fails the job;
   downstream processing must accept only `complete` or `complete_with_warnings`
   manifests.
7. Test and process the snapshot through staging, Silver and Gold. Restore its
   XML temporarily from the selected ZIP and release that verified working copy
   afterwards. Update route history, then call `scripts/run_gui.sh --restart`.
   Failed processing does not restart the GUI; metadata reports still upload.

The checks are not full NeTEx XSD, profile or calendar validation. Registry-only
reference diagnostics are reported separately from provider reference failures.

Network timeouts, incomplete transfers, HTTP 408/429 and server errors get up to
three attempts with backoff. `Retry-After` is respected up to 60 seconds. A 404
fails clearly; we do not silently choose a different date. Each download attempt
uses its own path, keeping interrupted bytes. Checksum or validation errors are
fatal and retain the evidence. GitHub concurrency prevents overlapping runs.

#### Persistent storage

With `DAT535_DATA_ROOT=/srv/dat535/data`, each execution writes:

```text
/srv/dat535/data/
└── bronze/entur/run-RUN_ID-ATTEMPT/
    ├── metadata/       Manifest, source metadata, lineage and validation report
    ├── national/       Failed/interrupted sources; successful ZIP removed after validation
    ├── zip/            Retained KOL + AKT XML and national stops, compressed unchanged
    └── xml/DATE/       Temporary extraction during validation/processing
```

A metadata-only check creates just `metadata/`; it does not claim ZIP or XML
validation. These server directories are retained across runs and reboots,
subject to the school's disk and mount configuration. Successful scheduled runs
remove their national ZIP after validation, recording `national_removed=true`.
The workflow uses `--zip-only --remove-national-after-validation`: selected raw
XML stays inside its ZIP, metadata has no automatic expiry, and processing
removes its verified extracted copy when the snapshot finishes or raises an error.
An interrupted process can leave a cache that is verified and recovered on the
next run. New unique ZIPs still consume space. See storage maintenance. Full ingestion checks
for 2 GiB free before checkout/download. Storage capacity and backups still need
to account for growth in selected data. A removed national ZIP can currently
be re-downloaded from its saved source URL/generation while Entur retains it;
this is not a separately verified Google Drive backup.

The only GitHub artifact is `entur-report-RUN_ID-ATTEMPT`, retained for 30 days.
It contains a copy of the metadata, which also stays on the server. Raw national
archives and selected ZIPs are not uploaded to GitHub. Expiry of a report artifact
does not remove any server data. A report upload failure fails the workflow but
leaves the server's data and manifest intact.

The existing historical dataset remains where it is. Copying that dataset to
school storage is a separate setup step; the workflow does not move it or add
it to Git. The runner and storage setup above has been completed on the server.

Each workflow execution is a new observation, named by run ID and attempt. A
manual rerun can collect the same payload again. There is no cross-run duplicate
lookup yet. The content-based `ingestion_id` lets later database/storage work
recognize identical national payloads for the same provider selection. The
schedule still requests yesterday's dated archive, so it can download
and retain that same archive repeatedly within a day. It does not backfill
missed days. A three-day schedule samples one source day per execution; it does
not automatically collect all three intervening daily archives.

#### Run the same command locally

From the project root with Python 3.11 or newer, without installing packages:

```sh
python3 -B -m pipeline.ingestion.poll_entur_netex --check-only
python3 -B -u -m pipeline.ingestion.poll_entur_netex
```

By default, a new directory is created under `data/bronze/entur/`:

```text
RUN/
├── metadata/       Manifest, source metadata, per-file lineage and validation
├── national/       Original national ZIP and any interrupted attempts
├── xml/DATE/       Unmodified selected XML
└── zip/            Compact copy of the selected XML
```

For a particular source day, use `--date YYYY-MM-DD`. For a custom destination,
use `--output PATH` with a new directory. Existing output directories are refused.
Add `--remove-national-after-validation` to discard the national ZIP after a
successful run. Add `--xml-only` to avoid creating a compact provider ZIP.
Without those options, manual polling retains both ZIP representations.
Paths in the manifest and lineage are relative to this run directory. Keep the
folders together when copying a run. To restore loose XML from its compact copy,
extract the selected ZIP into `xml/DATE/`.

On the school server, manual commands should explicitly use the persistent
location as well, for example:

```sh
python3 -B -m pipeline.ingestion.poll_entur_netex --check-only \
  --output /srv/dat535/data/bronze/entur/manual-source-check-01
```

The unit tests use small ZIP fixtures for full acquisition, filtering and
reference validation, including transient failures. They passed locally and on
the school server. A full manual CLI ingestion of the 23 October archive also
passed on `spark`, with the warnings recorded above. A complete manual run
through GitHub Actions also passed. Automatic cron verification is tracked in
the Actions verification section above.

### Manual ZIP ingestion

`pipeline.ingestion.ingest_local_zip` imports an existing national Entur ZIP
or a previously selected KOL + AKT ZIP. It uses no network requests and needs
only Python 3.11 or newer. The same `ingest_zip` function can be called by the
future upload GUI.

From the cloned project directory:

```sh
python3 -B -u -m pipeline.ingestion.ingest_local_zip \
  --source entur_netex_snapshots/kol_agder_2026/zip/KOL_AKT_netex_20260115.zip \
  --date 2026-01-15 --zip-only
```

The date is required. It is a user-supplied claim, not inferred from the filename
or verified against Entur's archive metadata. Future ordinary-line publication
dates are checked, but that alone cannot prove an upload's historical date.

For an existing archive on the school server, after deploying this module:

```sh
python3 -B -u -m pipeline.ingestion.ingest_local_zip \
  --source /srv/dat535/data/bronze/entur/kol_akt_2026_01_09/zip/selected_netex_20260115.zip \
  --date 2026-01-15 \
  --output /srv/dat535/data/bronze/entur/manual-january-check-001 \
  --zip-only
```

An explicit output directory must be new. Without `--output`, a unique run is
created below `data/bronze/entur/`. Server invocations should supply persistent
storage outside the checkout.

The selected ZIP filename can differ between direct ingestion and migration.
Use the `combined_zip` path recorded in that snapshot's manifest rather than
guessing the name. The server example above uses the migrated January archive.

#### What is kept and checked

The input is never modified. A new import copies its original ZIP byte for byte,
records SHA-256, MD5 and byte size, checks every member's CRC, and rejects unsafe
paths, symbolic links and duplicate member names. It then reuses the existing
provider extraction and reference validator. Selected XML is not rewritten.

With `--zip-only`, the selected XML stays inside its verified ZIP; its expanded
working copy is released after validation. Processing restores it temporarily.
The original uploaded archive remains. Without this option the API retains both
the selected ZIP and expanded XML for compatibility with earlier callers.

The archive must contain line XML and ordinary shared data for every requested
provider, plus `_stops.xml`. Flexible shared data is retained when present.
Missing references, malformed XML, bad CRCs or missing required files fail the
import. Repeated source ID/version occurrences remain source warnings. These
are XML/reference checks, not full NeTEx XSD, profile or calendar validation.

```text
bronze/entur/
├── _manual_ingestions.sqlite3       Metadata index for successful manual imports
└── manual-RUN/
    ├── uploads/original.zip        Exact uploaded bytes
    ├── xml/DATE/                   Original selected XML and national stops
    ├── zip/                        Compact selected snapshot
    └── metadata/
        ├── manifest.json           Source checksum, date, providers and status
        ├── lineage.json            Member paths, sizes, CRCs and SHA-256 hashes
        └── validation.json         XML and reference checks
```

Output paths in a run's metadata are relative to its directory. Failed imports
keep their copied ZIP, partial outputs and a failed manifest; they are not
registered as successful. Downstream processing must accept only `complete` or
`complete_with_warnings` imports. Retrying a failure requires a new run directory.

#### Repeat uploads

The duplicate key is **original ZIP SHA-256 + claimed snapshot date + provider
selection**. Renaming the same file or reordering provider arguments does not
create another successful import. Different dates and provider selections are
separate observations; a new date still goes through validation.

The catalog checks the previous raw archive, selected ZIP and materialized XML
before treating it as a duplicate. A duplicate creates a new metadata-only run
with status `duplicate`; `duplicate_of` points to the original manifest relative
to the new run. No second ZIP copy or XML extraction is created. A damaged or
missing original fails clearly rather than being silently reused.

The default catalog is `_manual_ingestions.sqlite3` in the output directory's
parent. Use `--catalog /path/to/shared.sqlite3` when imports use different parent
directories but should share duplicate checks. The catalog holds a write lock
through import and registration, so concurrent imports cannot register the same
key twice. Another importer waits up to 120 seconds, then fails if it cannot
acquire the lock. SQLite releases the lock and rolls back uncommitted catalog
changes when a process exits; existing run files remain available for inspection.

This is a small ingestion metadata index, not the analytical database. Keep the
catalog and its imports in place: the index records absolute manifest paths.
Relocating them needs a catalog migration or a new explicitly chosen catalog.

Duplicate detection currently covers this manual path only. It does not scan
the earlier historical directories or stop repeated API downloads. Repacked ZIPs
with identical XML but different ZIP bytes are also separate inputs. Entity-level
deduplication and shared API/manual ingestion identity remain later work.

#### Verification — 28 October 2026

All 40 ingestion tests pass on Python 3.11 and 3.14. Tests cover national and
selected ZIPs, retained references, unsafe paths, bad CRCs, missing dependencies,
failed retries, modified previous outputs and concurrent duplicate uploads.

We also imported the real January ZIP locally and repeated the upload. See the
ignored audit record at `audit/2026-10-28/manual_zip_ingestion_verification.json`
for the counts, original checksum and duplicate result. This is a test of existing
January data, not additional historical coverage. The module has not yet been
deployed to the school server or connected to a GUI.

### Server storage maintenance

#### Current policy: selected-provider ZIPs

The school VM has a 38 GiB root disk. After preparing full-month calendar
support, the server reported 23 GiB in Bronze and 3.5 GiB free. Successful
national ZIP removal was working; permanently extracted XML was the main cost.
The user approved moving existing validated snapshots to compressed ZIP storage.

Keep each snapshot's selected KOL + AKT XML and national stops inside a compact
ZIP. Keep manifests, source identity, lineage and validation reports beside it.
ZIP compression preserves the exact original XML bytes and NeTEx references.
Staging, Silver, Gold and the checkpoint registry remain local and unchanged by
the migration. National archives include other providers and are a separate
retention decision; original manual uploads are never deleted by this tool.

#### Migrate existing XML without downloading again

Use the branch containing ZIP storage support, and let existing ingestion and
processing jobs finish before migration. Old checkouts do not know how to restore
ZIP-backed XML. Do not delete entire run folders or manually change their manifests.

Preview (no XML or manifests changed):

```bash
cd ~/dat535-project
python3 -B -u -m pipeline.storage.zip_bronze \
  --bronze-root /srv/dat535/data/bronze/entur
```

Apply the approved format migration:

```bash
python3 -B -u -m pipeline.storage.zip_bronze \
  --bronze-root /srv/dat535/data/bronze/entur --apply \
  --report /srv/dat535/data/metadata/zip-migration-1.json

df -h /srv/dat535/data
```

Use a new report filename on another invocation. The report records each source
manifest, date, logical XML bytes, retained ZIP bytes, skipped snapshots and
problems. Logical byte totals are not estimates of physical disk savings:
hardlinks may share storage. Use `df` to measure available space afterwards.

For each complete, validated snapshot, the tool:

1. Checks its passed validation report and every XML size and SHA-256 against
   the original lineage. Unexpected files, unsafe paths or changed XML block cleanup.
2. Builds a selected ZIP, or verifies a retained one. Checks the exact member
   set, sizes, CRCs and every decompressed XML SHA-256. It processes one snapshot
   at a time, rather than building all ZIPs before releasing any XML.
3. Saves a manifest backup under `storage_backups/`, then durably records
   `raw_storage="zip"`, `combined_zip`, its SHA-256 and compressed size.
4. Unlinks only that snapshot's known, verified XML files. Shared hardlinks in
   other snapshots remain intact. Checkpoints and derived outputs are untouched.

Failed/incomplete snapshots and their evidence remain. The failed February
source therefore still occupies storage; this tool does not silently discard it.
A problem with one manifest is reported and causes a nonzero exit status.
Already migrated snapshots are verified on a rerun. A ZIP published immediately
before an interruption can be adopted only after a full lineage match.

#### Processing and recovery

`run_snapshot`, standalone XML conversion, calendar support checks, manual-import
duplicate verification and independent journey tracing understand ZIP storage.
They verify the ZIP and restore XML to its original paths under an exclusive
snapshot lock. All source paths, dates, IDs, references and content fingerprints
stay the same. Nested processing uses the same extraction.

The extraction remains until that snapshot's operation finishes. On success or
a Python exception, cleanup rechecks the archive and XML before releasing the
expanded copy. The ZIP remains authoritative even if Spark fails. A hard kill
can leave expanded XML; a later run verifies it and recovers. Changed cache files
or a damaged archive are preserved and reported, not silently removed.

Allow space for one full snapshot plus at least 1 GiB headroom before extraction;
Spark and new outputs may require additional space. The existing processing
checkpoints still verify completed Parquet outputs before reuse. Updated reader
code can require a new staging implementation and downstream attempts once;
the storage migration itself keeps the source fingerprint unchanged. Source hashes
are validated even when every stage can be reused. GUI and route-history reads
of existing Gold do not need raw XML extraction.

The full-month rebuild uses a separate 2 GiB Java heap per sequential snapshot
worker. A Java `OutOfMemoryError` concerns RAM; deleting Bronze ZIPs cannot fix
it. See [batch runtime and resume](#full-month-daily-service).

Low-level `load_sources`/`load_snapshot_sources` callers that read XML directly
must hold `pipeline.storage.zip_bronze.snapshot_xml(manifest, date)` around their
whole operation. Public processing entry points do this automatically.

#### New ingestion

The scheduled workflow now supplies `--zip-only --remove-national-after-validation`.
Calendar-support acquisitions use the same retention policy. Successful national
downloads are removed only after selected data validation and ZIP verification;
failed sources remain. Both historical generation and polling expose `--zip-only`.
Manual upload also accepts `--zip-only` and continues to preserve the original upload.
The Python APIs retain their old expanded defaults for existing callers.

Merge the workflow change to the default branch to update scheduled jobs. Pulling
the branch in `~/dat535-project` updates manual commands, not the Actions checkout.

#### Previous XML-only cleanup

`pipeline.storage.maintenance` remains available for checksum-verified hardlink
consolidation of expanded datasets. Its `--discard-provider-zips` option only
applies to redundant ZIP copies: it explicitly refuses to discard an archive
marked `raw_storage="zip"`. Do not use an older version of that command after
migration; it lacks that protection. ZIP retention supersedes the earlier
XML-only policy for the school server.

The Group04 runner was stopped/uninstalled by the user and its binary/update
packages removed. The DAT535 runner under `/home/ubuntu/dat535-runner`, its service,
and active VS Code server installations remain required.

## Processing

### Checkpointed pipeline

Processing uses Spark 4.2's native XML reader, explicit schemas and Snappy
Parquet. Bronze archives and XML remain authoritative. Route history
([route history](#route-history)) and the GUI ([GUI guide](#gui))
read the outputs described here.

Each stage's checkpoint identity includes the SHA-256 of the source files that
implement it (listed in `pipeline/run_snapshot.py`). Any change to those files,
including comments, means an already processed snapshot is recomputed into a new
directory instead of reused when it is run again. New snapshots are unaffected.

#### Run on the school server

The helper uses `~/spark-env/bin/python` and the project-specific Java 17 runtime
recorded in `~/dat535-runtime/java17.json`. It does not change the server's
default Java 8. `DAT535_SPARK_PYTHON` and `DAT535_JAVA_METADATA` can override those
locations. Run inside the cloned project:

```sh
bash scripts/run.sh processing \
  --manifest /srv/dat535/data/bronze/entur/kol_akt_2026_01_09/manifest.json \
  --date 2026-01-15 \
  --data-root /srv/dat535/data
```

Use `--all-snapshots` instead of `--date` for the nine validated historical
snapshots. `--through staging` or `--through silver` stops after that layer.
A complete manual/API ingestion manifest is accepted without `--date`.
Repeated manual ingestions marked `duplicate` should use their `duplicate_of`
canonical manifest rather than the metadata-only observation.

Outputs live outside the checkout under:

```text
/srv/dat535/data/
├── bronze/entur/                    Existing immutable source data
├── staging/entur/DATE/SOURCE_KEY/    Parsed records and source context
├── silver/entur/DATE/SOURCE_KEY/     Validated entities and quarantine tables
├── gold/entur/DATE/SOURCE_KEY/       Reconciled planned-service metrics
├── pipeline_state.sqlite3          Processing state and attempt history
└── pipeline_state.sqlite3.locks/    Per-snapshot process locks
```

Every attempt gets a new directory. There is no automatic deletion or
overwriting of failed attempts. Processing refuses to start a new snapshot
with less than 800 MiB free; this is a minimum guard, not a capacity guarantee.
The development Actions workflow additionally requires 2 GiB free before a
full ingestion/processing job. Source availability checks skip that budget.

#### What a checkpoint means

```sh
python3 -m pipeline.checkpoints --state /srv/dat535/data/pipeline_state.sqlite3
```

The registry identifies a snapshot by its date, selected providers and the
SHA-256 inventory of provider/shared XML plus national stops. It registers only
after the ingestion status, validation report and original file hashes pass.
It tracks each staging, Silver and Gold attempt with its input identity,
implementation fingerprint, timestamps, output path, status and error.

A stage becomes `complete` only after its manifest reports completion. Saved
manifest and Parquet hashes are recorded. A rerun checks those hashes before
reusing an output. A failed stage gets a new attempt; completed upstream stages
are reused. Changed transformation code invalidates that stage's cache. Changed
upstream output also changes downstream identity. Altered/missing outputs are
recorded as `invalidated`, preserved, and rebuilt into a new directory.

Exclusive OS locks prevent simultaneous processing of the same source through
the same registry. Crashes release the lock; the next run records unfinished
attempts as `interrupted`. This resumes at stage boundaries, not midway through
an XML file or Spark partition. The SQLite registry is processing metadata; it
is not the analytical database. The development workflow now chains validated
ingestion into this processing command and copies checkpoint status into its
report artifact. That change needs to reach `main` before scheduled/push runs
use it. Metadata-only checks skip processing.

#### Tables and keys

Staging includes frames, lines, routes, journey patterns and their ordered
points, service journeys, passing times, scheduled stops, stop assignments,
day types, operating periods/days, day-type assignments, flexible stop places,
national stop places and quays. Counts must match Bronze reports and Parquet
read-back. Spark hides underscore-prefixed source filenames, so the job makes
read-only symbolic input aliases while retaining original paths and hashes.

Silver retains snapshot/provider/source context and versioned references.
Line, route, pattern and journey occurrences retain their source file context;
an ID/version pair across different files is not automatically a duplicate.
Passing-time grain is `(journey_key, passing_time_index)`, preserving repeated
source IDs. Pattern points additionally retain pattern context and source order.
Shared stop definitions are collapsed only when their keys and projected
payloads agree. Conflicting definitions are quarantined.

Reference joins count candidates and accept exactly one match. Missing or
ambiguous matches produce reason codes. Source and final journey/passing-time
counts must match, preventing joins from silently multiplying or dropping rows.
The `journeys` and `passing_times` tables retain both valid and invalid records
with `is_valid` and `reason_codes`; `quarantine_*` tables provide explicit views
of invalid records. Consumers must filter valid records or use Gold.

#### Calendars, time and flexible services

The default analysis window is the full calendar month containing the snapshot
date, from its first to its last day. For migration, source validity, calendar
support and the per-date coverage audit, see full-month processing.
`--service-from` and `--service-to` select an explicit window of up to 366 days.
These are service dates described by that snapshot, not a claim of complete
daily observation throughout the nine months. Snapshot date and service date
remain separate.

The small calendar definition tables are resolved on the driver; large journey
and passing-time transformations use Spark. Expansion supports weekday sets,
inclusive operating-period end dates, operating-day references and explicit
date additions/exclusions. Explicit dates override period rules; an exclusion
wins at equal specificity. Unsupported holiday/season/property rules and
unresolved references are reported rather than interpreted as everyday service.
The rules follow Entur's [calendar examples](https://entur.atlassian.net/wiki/spaces/PUBLIC/pages/728825959/Calendar)
and [Norwegian framework profile](https://entur.atlassian.net/wiki/spaces/PUBLIC/pages/728727624/framework).

Clock times retain raw strings. Normalized seconds include signed day offsets,
so `00:10:00` with offset 1 becomes 87,000 seconds. Invalid clocks, backwards
passing times and departures before arrivals receive quality reasons. These
are operating-day seconds, not UTC timestamps; timezone/DST conversion is not
implemented by this slice.

HentMeg services use `FlexibleLineRef` and earliest/latest operating windows.
Those fields and flexible-stop references are preserved. Flexible services are
classified separately and excluded from fixed-departure frequency metrics.

#### Gold and verification

Gold contains daily departures per line, a provider/date network spine with
source-coverage flags, a service-window summary, planned stop
coverage for eligible journeys and excluded service-date records. Summary
averages divide by all calendar days in the declared window, including days
without service. Daily departure totals must equal eligible fixed-service
occurrences in Silver. Metrics describe planned timetables, not observed
operations, ridership or delays. Historical change and SCD tables are in
[route history](#route-history).

Gold publishes only if its Silver input passes the quality gate
(`config/quality_thresholds.json`): any rejection warns, and more than 1%
rejected journeys, passing times or service occurrences, any ambiguous
reference, or no accepted service for a provider stops publication. The
profile and gate decision are saved with each Gold output. See
[data quality](#data-quality).

Tests cover source damage, underscore-prefixed reference files, repeated IDs in
different files, reference versions, leading-zero public codes, calendar
exceptions, overnight times, flexible windows, invalid-time quarantine, Gold
reconciliation, safe reruns, failed attempts, lock conflicts and altered-output
recovery. See the project journal for dated real-dataset/server results.

#### Historical server run — 28 October 2026

All nine snapshots completed staging, Silver and Gold on `group4-spark-vm`.
Each Gold total reconciled to eligible fixed-service occurrences in Silver.
The last column covers the seven days starting on that snapshot date, across
the selected providers' transport modes; it is not a whole-month total.

| Snapshot | Journey occurrences | Passing-time occurrences | Gold fixed departures |
| --- | ---: | ---: | ---: |
| 2026-01-15 | 25,289 | 768,701 | 53,322 |
| 2026-02-15 | 28,628 | 906,396 | 48,927 |
| 2026-03-15 | 32,702 | 1,034,292 | 53,461 |
| 2026-04-15 | 32,974 | 1,075,434 | 53,369 |
| 2026-05-15 | 34,328 | 1,109,078 | 53,580 |
| 2026-06-15 | 34,089 | 1,092,197 | 52,936 |
| 2026-07-15 | 25,581 | 765,424 | 40,527 |
| 2026-08-15 | 23,592 | 725,385 | 54,904 |
| 2026-09-15 | 26,574 | 820,047 | 55,467 |

Totals: 263,757 journey occurrences and 8,296,954 passing-time occurrences.
The declared windows contain 466,493 fixed-service occurrences and 192
flexible-service occurrences, reported separately. All nine runs had zero
quarantined journeys/passing times and zero calendar issues under the current
rules. These results do not replace full NeTEx profile/XSD validation.

A second full historical invocation reused all 27 completed stages. The registry
still contained exactly 27 attempts, with 27 reuse events and no new output
directories. The logs are under `/srv/dat535/data/metadata/processing/`.

With user approval, seven byte-identical national ZIP copies were consolidated
using hardlinks, retaining every original path and checksum. This recovered
1,957,911,441 bytes. All archive paths remain immutable. Around 1.8 GiB remained
free after the batch; additional persistent capacity is needed before relying
on continued full scheduled ingestion. The development workflow's 2 GiB
preflight budget prevents it from starting a full job below that threshold.

### Full-month daily service

The school-server rebuild completed on **10 November 2026**: all **22 jobs**
finished, published-plan coverage passed for **January–November 2026**, and route
history was generated. The [verified coverage](#verified-full-month-coverage) page records
that completion separately from the detailed January–September local totals.
Use the commands below to rerun the batch, resume interrupted work or extend
coverage when adding months.

The processing default is now the first through the last day of the snapshot's
calendar month, including February and leap years. The original seven-day
outputs remain historical evidence; the current GUI uses the rebuilt full-month
outputs. Source XML bytes and completed attempts are preserved. With
ZIP retention, verified XML is stored compressed and extracted temporarily.

#### Sources for early dates

Most 15 January XML frames start their declared validity on 12–13 January.
Simply expanding that export from 1 January would not supply the earlier plans.
The same rolling cut-off occurs in the other sampled months.

`prepare_monthly_support` validates a first-of-month export for each month. If
that source is broken, it tries an earlier export with validity covering the
whole requested month. The recorded observation date stays unchanged. The 1
and 2 February sources failed on the missing reference `NSR:Quay:43289`; the
validated 14 January export supplies the earlier February plan instead. The
failed archives and validation reports are retained.

These auxiliary outputs have `history_role=calendar_support`. Route history
continues to compare the original 15th-of-month observations and scheduled
primary observations. It does not reinterpret verified transitions because an
extra calendar source was added.

#### Rerunning or extending the Spark-server rebuild

Run from an up-to-date project checkout on the school server. The existing
registry and validated Bronze ZIPs (or expanded XML) must be available. Use the
paths from your server, not paths copied from a Mac manifest. This example
reuses the recorded January–October support range; registered November primary
exports receive support automatically. Adjust the month range when adding months.
The existing `calendar_support_2026_01_09` directory keeps its historical name.

```bash
python3 -B -u -m pipeline.ingestion.prepare_monthly_support \
  --providers KOL AKT --from-month 2026-01 --to-month 2026-10 \
  --output /srv/dat535/data/bronze/entur/calendar_support_2026_01_09 \
  --remove-national-after-validation

bash scripts/run.sh rebuild \
  --data-root /srv/dat535/data \
  --primary-manifest /srv/dat535/data/bronze/entur/kol_akt_2026_01_09/manifest.json \
  --support-plan /srv/dat535/data/bronze/entur/calendar_support_2026_01_09/calendar_support_plan.json \
  --remove-national-after-validation
```

The explicit removal option above applies only to newly downloaded national
ZIPs after successful extraction and validation. Omit it to retain those ZIPs.
Failed sources are kept. New calendar-support sources use ZIP-only retention:
their selected XML stays in a checksum-verified compact ZIP and is extracted
temporarily when checking coverage or processing. Existing expanded sources
can be migrated without downloading again. Allow room for one extracted snapshot,
Spark temporary files and new outputs. See [storage maintenance](#server-storage-maintenance).

The rebuild includes every existing registered primary snapshot, so older
scheduled seven-day outputs are migrated as well. `--primary-manifest` also
ensures all nine historical dates are included. Registered later months, such
as November, get their own validated support plan automatically. Auxiliary
sources use dates distinct from the primary observation sample. Compatible verified stages are
reused; changed implementations produce new attempts. A changed source selector
can require new staging as well as Silver and Gold.

The command records progress under `DATA_ROOT/rebuilds/monthly-*/manifest.json`.
It launches one sequential Python/Spark worker per snapshot, followed by a
separate route-history worker. Each JVM exits before the next worker starts,
releasing its heap, cached plans and shuffle state. The default Java driver
heap is 2 GiB; override it with `--driver-memory 3g` or `DAT535_DRIVER_MEMORY` if
the host has sufficient RAM. The heap is configured before JVM startup, as
required for [Spark client-mode driver memory](https://spark.apache.org/docs/4.2.0/configuration.html#application-properties).
This is RAM, separate from disk space used by raw ZIPs and temporary XML.

Each worker writes its result and actual heap limit under the rebuild's
`workers/` directory. The parent process records failure and completed progress
even if a worker's JVM crashes. The staging/Silver/Gold transformation versions
are unchanged by worker isolation, so completed compatible checkpoints are
reused. Failed or interrupted attempts remain as evidence and receive a new
attempt when retried. Re-run the same rebuild command to resume; use a new
terminal log filename to preserve the failed run's log.

It checks every requested date for both providers before marking completion,
then rebuilds route history. A rerun uses completed checkpoints. If the process
fails, its evidence is preserved. `--skip-history` is for isolated verification
runs; use the default for the complete application.

After completion, restart the GUI or use **Refresh data**. Scheduled ingestion
uses the new full-month default for subsequent snapshots. When adding a new
month, prepare its calendar support and rerun the rebuild with that support
plan to cover any early dates its first primary export no longer contains.

#### Meaning and outputs

The GUI reconstructs each service date from the latest export whose processed
window and shared-calendar validity cover it for both selected source providers.
The choice is independent of operator/mode filters. It selects whole exports;
missing lines are not backfilled. Overlapping exports are never added together.
The source export date is retained in daily downloads.

A later observation can describe an earlier service date when its validity
allows it. These are reconstructed published plans, not a claim about what was
known on each day or what vehicles actually ran. A valid day without service is
zero; a date without a valid source stays unknown and fails the full-month audit.

Gold's `daily_network_service` contains one row per provider and calendar date,
with departures, active lines, first/last departure and `is_source_covered`.
`daily_line_service` retains actual fixed line/day facts. The manifest includes
the service-window policy, source calendar coverage and history role. Journey
dates are also bounded by their own XML frame validity.

Overview, Daily service, Compare months and line service profiles use the same
dated reconstruction. Totals include all covered days; means divide by calendar
days, including zero-service days. Month lengths and weekday mixes differ, so
comparisons show coverage, daily means and weekday profiles. Route-history
changes and stop details remain measurements of each primary export, not the
stitched monthly network. Historical seven-day reports are labelled as such.

## Data quality

Zero quarantined rows only says that nothing broke the current rules. This
page describes what we measure about each Silver output, when Gold is allowed
to publish, and how we checked real journeys independently. The numbers are in
two generated files:

* [`quality_report.md`](evidence/quality_report.md): the before/after table for every
  snapshot and provider;
* [`quality_traces.md`](evidence/quality_traces.md): four journeys traced from the source
  XML through Silver to Gold.

These are dated seven-day baseline reports. They remain regression evidence;
their counts are not the rebuilt full-month totals. Current calendar coverage
is recorded in [monthly coverage](#verified-full-month-coverage). Regenerate quality
reports against the desired published outputs when preparing current results.

### What is measured

`pipeline/quality/silver_profile.py` profiles a completed Silver output per
provider. "Before" is the staging Parquet, which holds every source occurrence;
"after" is Silver.

| Area | Measures |
| --- | --- |
| Volumes | Lines, journeys and passing times in staging and Silver; accepted and quarantined |
| Duplicates | Identical stop definitions collapsed into one row; conflicting definitions withheld |
| Missing values | **Essential**: line transport mode, journey pattern and day types, a physical stop and a time for every fixed passing time. **Optional**: line name and number, stop-point name, quay coordinates, route direction, `ServiceJourney.DepartureTime` |
| References | Unresolved (no target) and ambiguous (several targets) references for route, pattern, line and day type |
| Times | Invalid clock values, arrival after departure, non-monotonic times, overnight times kept with a day offset |
| Calendar | Day types whose dates could not be derived, by reason |
| Calendar impact | Distinct journeys with an unresolved calendar, divided by journeys before service-date expansion |
| Service | Service occurrences in the window: accepted for Gold, excluded as flexible, rejected as invalid; valid fixed journeys that do not operate in the window |

`ServiceJourney.DepartureTime` is optional: in the Nordic profile the times are
on the passing times, and nearly every journey leaves it empty. Our first draft
counted it as essential, and the real data showed that was wrong.

### Publication gate

Before writing any Gold table, `build_gold` profiles its Silver input and
applies the rules in `config/quality_thresholds.json` to each provider:

| Rule | Warn when | Stop when |
| --- | --- | --- |
| Provider selection | – | No declared providers, an expected provider has no Silver rows, or an undeclared provider appears |
| Staging → Silver occurrences | – | Line, journey or passing-time counts differ for a provider |
| Journeys quarantined | any | > 1% |
| Passing times quarantined | any | > 1% |
| Service occurrences rejected as invalid | any | > 1% |
| Essential fields missing | any | > 1% of journeys, passing times and lines |
| Ambiguous references | – | any |
| Unresolved references | any | – |
| Conflicting definitions withheld | any | – |
| Calendar issues | any | – |
| Journeys affected by unresolved calendars | any | > 1% of journeys before date expansion |
| Accepted service occurrences | – | none for a provider |

* **pass**: Gold publishes.
* **warn**: Gold publishes, and the findings are in the Gold manifest
  (`quality_gate`).
* **fail**: Gold is not published. The stage fails with the rules that fired,
  the attempt is recorded as failed in the checkpoint registry, and the
  workflow's processing step fails visibly.

The full profile is saved with every Gold output (`metadata/quality_profile.json`).
Gate version 2 records both the manifest's `expected_providers` and the
`observed_providers` found in Silver. The provider-selection checks cannot be
disabled by changing a numerical threshold. A deliberate zero-service-window
override still requires the selected providers' data to be present. Empty or
missing provider profiles cannot pass.

Calendar issue counts remain warnings when they describe unused definitions.
The impact rate measures distinct affected journeys, so a broken calendar
cannot hide its impact by producing no expanded service-date rows. The same
journey is counted once even when several of its day types are unresolved.

The gate code and the thresholds are part of Gold's checkpoint identity, so
changing a rule rebuilds Gold (Silver is reused). Without a gate, Gold would
reconcile perfectly with whatever survived Silver, even after large exclusions.
The test `test_invalid_times_are_quarantined_and_the_quality_gate_stops_gold`
shows this: with half of AKT's journeys broken, the default rules stop Gold, and
publishing needs a separate, recorded threshold file.

The limits were set after profiling all ten 2026 snapshots. All of them have zero
rejections, so any rejection should be noticed (warn), and more than 1% means
something structural changed in the source or the code (stop).

### Previously recorded results on the ten 2026 snapshots

The committed report records all ten snapshots passing the original gate for
both providers. Gate version 2 and the expanded traces have now also passed on
a fresh full January rebuild; see [the validation record](#quality-validation-10-november-2026).
The other snapshots need their evidence regenerated with the updated checker.
Most of the evidence
shows the source is consistent, rather than that much was repaired:

* **Nothing rejected or collapsed.** In every snapshot, staging and Silver hold
  the same lines, journeys and passing times. No duplicates were collapsed, no
  conflicting definitions were withheld, and no references are unresolved or
  ambiguous. Silver's work here is resolving references, deriving dates and
  converting times, not cleaning.
* **Overnight service is substantial.** Between 5,902 and 12,881 AKT passing
  times and between 24,254 and 52,210 Kolumbus passing times per snapshot fall
  after midnight. They are kept on their operating day with a day offset (at
  most +1), not moved to the next date.
* **Many valid journeys don't run in the window.** For example, in January
  1,715 of 8,845 AKT and 6,063 of 16,432 Kolumbus fixed journeys have no date in
  15–21 January. This is why "stops served" is measured from journeys that
  operate in the window (see [route history](#route-history)).
* **Flexible service is excluded, not lost.** Kolumbus has 13–23 on-demand
  occurrences a week. They are kept in Silver and listed in Gold's
  `excluded_service_dates`, and they are not counted as departures.
* **Optional gaps.** AKT's scheduled stop points have no names at all (stop
  names come from the stop places), and Kolumbus has 20–24 unnamed stop points
  and 2 routes without a direction. None of these affect departures.

### Traced journeys

`pipeline/quality/trace.py` picks four real journeys from the February
snapshot (the holiday week), then reads the original XML with Python's
standard library only. It does not use Spark or any Silver code. From the XML
alone it recomputes the passing times in seconds and the operating dates from
`DayType`, `DayTypeAssignment` and `OperatingPeriod`, then compares them with
Silver and checks how Gold treated the journey:

| Case | Journey | What it shows |
| --- | --- | --- |
| Ordinary | AKT, 14 stops, Mon–Fri | Times and dates match; Gold's departures for the line and date equal a recount from Silver |
| Overnight | AKT, 69 stops, runs past midnight | Times after 24:00 keep their day offset and the journey stays on its operating day |
| Calendar exception | Kolumbus, weekday service | Friday 20 February is marked "not available" in the XML; both the independent reading and Silver drop it (4 dates) |
| Flexible | Kolumbus on-demand journey | Silver keeps it with its time window; Gold excludes it from departures and lists it as excluded |

The committed February report records the original four checks. The expanded
checker (10 November) additionally:

* Resolves pattern point → scheduled stop → assignment → quay → national stop
  place, or a flexible stop place, directly from XML and compares each identity
  and relevant resolved version with Silver. Missing or ambiguous targets stop
  the check instead of selecting a candidate.
* Compares `EarliestDepartureTime` and `LatestArrivalTime` separately with the
  preserved Silver flexible-window fields. Empty fixed arrival/departure values
  cannot by themselves make a flexible-window check pass.
* Recounts **every source journey occurrence for the selected line across its
  provider's XML files**, derives its dates independently, and compares daily
  fixed counts and flexible exclusions with Gold. Repeated IDs in different
  files remain separate occurrences. This supplements the Silver recount.

The source checker imports no Silver, Gold or calendar transformation code.
It supports the weekday/date/period rules exercised by these exports; unknown
restrictions and conflicting or multiversion calendar definitions are refused.
It is a bounded independent check, not a complete NeTEx schema validator.
The script exits with an error on a mismatch or unsupported source evidence.

Regression tests deliberately move another journey's date in both Silver and
Gold. Their counts still agree with each other, but the independent whole-line
XML recount detects the error. Other tests change a physical quay or swap the
flexible window while leaving fixed times unchanged.

Historical Markdown evidence is not rewritten automatically. Regenerate it
with the commands below to record the additional checks on the school dataset.

### Running it

```bash
bash scripts/run.sh quality-report --data-root /srv/dat535/data --markdown docs/evidence/quality_report.md
bash scripts/run.sh quality-trace --data-root /srv/dat535/data --snapshot 2026-02-15 --markdown docs/evidence/quality_traces.md
```

The report exits with an error if any snapshot fails the gate. The trace
accepts `--journey <journey_key>` to trace specific journeys.

## Route history

`pipeline/analysis/route_history.py` compares every completed snapshot with
the next, line by line: the nine historical monthly snapshots (15 January –
15 September 2026) and the scheduled snapshots that follow (first: 8 November
2026). It reads only completed, hash-verified Silver and Gold checkpoints, so it
runs after `scripts/run.sh processing`.

### What the dates and counts mean

* **Observation, not effect.** A snapshot shows what the operator had
  *published* on that date. An export can announce a future timetable or
  correct an earlier one, so a difference between two snapshots says only that
  the published data changed after the first and by the second. Versions carry
  `first_observed`, `last_observed`, `previous_snapshot` and
  `first_not_observed`, never `valid_from`. Effective dates come only from an
  operator source, quoted in the evidence files. (Silver expands calendars only
  inside each declared Gold window, so the pipeline has no effective date of
  its own.)
* **Disappeared is not retired.** A line missing from the next export is
  `disappeared` (no longer observed). `line_transitions.interpretation` calls it
  `renumbered_verified` or `discontinued_verified` only when a verified entry in
  `config/route_renumberings.json` or `config/route_discontinuations.json`
  covers that exact transition. Otherwise it stays `not_observed`.
* **Two stop measures.** `stop_ids` are the stops in a line's published journey
  patterns (route structure, used for versions). `window_stop_ids` are the stops
  served by fixed journeys that operate on a date inside the Gold window, the
  same eligibility as the departure totals. A pattern can be published without
  any journey in the window (school routes in a holiday week, for example).
* **Modes.** The data is multimodal: bus and water (boats). Totals include both;
  `bus_window_departures_*` and `network_snapshots.transport_mode` give the
  bus-only figures. Flexible (on-demand) services are not in the departure
  totals, as in Gold.
* Everything describes **planned** timetables, not vehicles that ran.
  Departures use each primary export's declared window (now a full calendar month).
  Calendar-support exports fill the GUI's monthly reconstruction without adding
  observations to the route-history sample. See [full-month processing](#full-month-daily-service).

### Running it

```bash
bash scripts/run.sh history --data-root /srv/dat535/data
bash scripts/run.sh inspect --data-root /srv/dat535/data summary
bash scripts/run.sh inspect --data-root /srv/dat535/data candidates
bash scripts/run.sh inspect --data-root /srv/dat535/data month 2026-07-15
bash scripts/run.sh inspect --data-root /srv/dat535/data line AKT 55
bash scripts/run.sh inspect --data-root /srv/dat535/data date AKT 55 2026-07-20
```

Both scripts use the school Spark environment and the project Java 17 runtime.
They `unset SPARK_HOME` so the Spark 4.2 bundled with `~/spark-env` is used,
not the older lab installation in `/opt/spark`.

A run with the same inputs, code, options and evidence files reuses the earlier
output. Pass `--force` to rebuild. Outputs go to
`DATA_ROOT/gold/entur/history/<identity>/history-<id>/`.

### Memory use and processing time

The 10 November run failed while writing `stop_coverage_changes`, after saving
72,323 coverage rows. Spark exhausted its Java heap while building a broadcast
hash table. This was a Java-memory failure, not a disk-full error. The other
messages about pandas compatibility and the native Hadoop library were warnings.

The history job now uses a bounded sequence:

1. Compute one output table, using a temporary `DISK_ONLY` cache so its count
   check and write do not evaluate the same joins twice.
2. Write and verify the Parquet output, then release that cache immediately
   with `unpersist(blocking=True)`, including when a write fails.
3. Read the saved `stop_coverage` table before comparing consecutive exports.
   The comparison no longer repeats its original stop-definition/name joins.

Both `spark.sql.autoBroadcastJoinThreshold` and
`spark.sql.adaptive.autoBroadcastJoinThreshold` are set to `-1` for the history
session. This prevents automatic broadcast joins from constructing large hash
tables in the driver's heap. Spark still needs working memory for tasks, and
shuffle joins can use temporary disk space; this is not a promise of zero RAM
use. Other pipeline Spark sessions keep their existing settings. See the
[Spark join documentation](https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html#automatically-broadcasting-joins).

The manifest records the actual JVM heap limit and SQL settings in `runtime`,
the total `elapsed_seconds`, and the elapsed time for each output table.
GitHub's route-history report includes those timings. Temporary caches are
released; the published tables, raw XML and failed-run evidence are preserved.

The [failed run](https://github.com/TOGUNOLUWATOBI/dat535-project/actions/runs/42814695326)
had this breakdown, before the memory fix:

| Step | Time |
| --- | --- |
| Basic tests | 2 s |
| Ingestion | 43 s |
| Full Spark tests | 3 min 36 s |
| Snapshot Silver/Gold processing | 28 s |
| History, ending in the heap failure | 1 min 47 s |

Most elapsed time was in tests, so memory cleanup alone will not remove that
cost. A successful full server rerun is needed to measure the production speed
change; no speedup is assumed from a local fixture.

Local regression testing with a **1 GiB Java heap** passed all 17 history tests,
including 79,200 coverage rows and 636,933 national stop-name rows across eleven
synthetic snapshots. The comparison produced exactly 3,000 gained and 3,000
lost window-service stops, with no definition changes or broadcast exchange in
its plan. Success and failed-write tests verified that no persisted RDD cache
was left behind. The synthetic case is larger in coverage rows than the failed
run, but it does not reproduce every property of the school dataset.

After updating the server checkout, history can be retried without ingestion:

```bash
bash scripts/run.sh history --data-root /srv/dat535/data
```

The retry uses completed Silver/Gold checkpoints and writes a new history
attempt. The failed output is not overwritten or deleted.

### Output tables

| Table | One row per | Task |
| --- | --- | --- |
| `line_snapshots` | snapshot, provider, line ID: bus number, name, mode, pattern stops, window stops, window departures | T38 |
| `line_patterns` | snapshot, line, journey pattern (ordered stop sequence) | T40 |
| `line_transitions` | consecutive snapshot pair, line: `appeared`, `disappeared`, `changed`, `service_changed`, `unchanged`, plus `interpretation` | T39 |
| `line_changes` | individual change event (see below) | T39–T40 |
| `renumbering_candidates` | possible renumbering, joined with verified mappings | T42 |
| `line_versions` | SCD Type 2 version of a line's structure, by observation date | T41 |
| `line_service_versions` | Gold service date linked to its line version | T43 |
| `stop_coverage` | snapshot, stop place: lines with it in their patterns, and lines serving it in the window | T47 |
| `stop_coverage_changes` | stop gained, lost or with changed lines, per `measure` (`definitions`, `window_service`) | T47 |
| `network_snapshots` | snapshot, provider, transport mode: network totals | T46 |
| `change_summary` | snapshot pair, provider: counts of each kind of change | T46 |

Change events: `line_appeared`, `line_disappeared`, `public_code_changed`,
`name_changed`, `transport_mode_changed`, `stops_added`, `stops_removed`,
`patterns_changed` (structure) and `service_started`, `service_stopped`,
`departures_increased`, `departures_decreased`, `first_departure_changed`,
`last_departure_changed` (service level).

### Rules

* **Line identity** is `(provider, NeTEx line ID)`. A new ID is a new line
  unless a renumbering is verified.
* **Stops** are national stop places (`NSR:StopPlace`). A platform change inside
  one stop place is not a route change.
* **Versions** (SCD Type 2) change only when structure changes: bus number,
  name, mode, line kind, stop set or stop sequences. Departure counts are
  service levels: they create change events but not new versions.
* **Departure changes** need at least 20% and at least 5 departures
  (`--frequency-threshold`, `--min-departure-change`). First/last bus changes
  need 30 minutes (`--span-threshold-minutes`).

#### Renumbering candidates

A candidate is produced when:

1. a line keeps its ID but its bus number changes (`same_line_id_new_code`), or
2. a line disappears and a line in the next snapshot shares its bus number
   (`same_code_new_line_id`, stop similarity ≥ 0.2), or has a different number
   but similar stops (`similar_stops`, stop similarity ≥ 0.6), or
3. the pair is listed as verified in `config/route_renumberings.json` but the
   rules above miss it (`verified_mapping`).

Stop similarity is the Jaccard index of the two lines' stop sets.

Two lessons from the real data shaped these rules:

* **Operators publish the new line ID one month early.** AKT's new `AKT:Line:55`
  appears in the 15 June snapshot with no departures, and the old
  `AKT:Line:1055` disappears only on 15 July. A disappeared line is therefore
  compared with every line in the next snapshot whose bus number first appeared
  up to one snapshot earlier (`--renumber-lookback`), not only with lines added
  in that same step. `new_line_since` records when the new line first appeared.
* **Bus numbers get reused.** In July 2026 Kolumbus' old line 1 continued as line 9
  (identical stops), while the former line A took number 1. Matching on the
  number alone paired unrelated routes with stop similarity around 0.01, so
  same-number pairs also need stop similarity ≥ 0.2 (`--same-code-similarity`).

Candidates are suggestions. A pair counts as a renumbering only after someone
checks it against an operator source and records it as evidence.

#### Evidence files

`config/route_renumberings.json` (old line → new line) and
`config/route_discontinuations.json` (line) are JSON lists. The reader is
strict, so a typo cannot turn a guess into a fact:

* `verified` must be JSON `true` or `false`. It is `false` when missing, and
  strings such as `"false"` are refused.
* A verified entry needs `verified_by`, `source_url` (a web address), a `note`
  quoting the source, and `to_snapshot`. `from_snapshot` is optional but
  enforced when given: the entry applies only to that exact transition.
* Unknown fields (for example `verifed`), IDs from another provider, bad
  dates and duplicate entries are refused.
* Every verified entry must match the data: an unmatched one fails the run.

Verified pairs link the new line's version to the old one through
`line_versions.renumbered_from_line_id` (comma-separated if several old lines
merged). A discontinued line goes in the discontinuations file, not the
renumberings file.

### Self-checks

The run fails (manifest `status: failed`) unless all of these hold:

* each snapshot's lines and departures equal its Silver line count and Gold
  departure total, and every Gold line exists in Silver;
* lines after = lines before + appeared − disappeared, for each provider and step;
* window-served stops are a subset of pattern stops, and a line has window
  stops exactly when it has window departures;
* every verified renumbering and discontinuation is found in the data;
* every line snapshot belongs to exactly one version;
* exploded change rows equal the change lists in `line_transitions`;
* every Gold service date links to a version and the linked departures equal
  the Gold total.

It also reports the number of each interpretation and of lines published
without any departure in the window.

### Results

Run of 9 November 2026 on ten snapshots (the nine historical months and the
first scheduled snapshot, 8 November): all checks passed. 521,185 Gold
departures (476,651 bus, 44,534 water) reconcile exactly with the history
tables and the version-linked service dates. For the nine historical months
the totals are 466,493 (426,395 bus, 40,098 water). 5,358 line snapshots form
1,336 line versions. 31 renumbering candidates were found; 16 renumberings and
4 discontinuations are verified, and all 20 match the data.

Departures in each snapshot's seven-day window (all modes; bus only in
brackets):

| Step | AKT | KOL |
| --- | --- | --- |
| Jan → Feb | 21,236 → 16,650 (−21.6%) [19,891 → 15,384] | 32,086 → 32,277 (+0.6%) |
| Feb → Mar | 16,650 → 21,193 | 32,277 → 32,268 |
| Mar → Apr | 21,193 → 21,115 | 32,268 → 32,254 |
| Apr → May | 21,115 → 21,335 | 32,254 → 32,245 |
| May → Jun | 21,335 → 21,066 | 32,245 → 31,870 |
| Jun → Jul | 21,066 → 15,245 (−27.6%) [19,737 → 13,960] | 31,870 → 25,282 (−20.7%) [28,711 → 22,168] |
| Jul → Aug | 15,245 → 20,614 | 25,282 → 34,290 (+35.6%) |
| Aug → Sep | 20,614 → 20,597 | 34,290 → 34,870 |
| Sep → 8 Nov | 20,597 → 20,753 | 34,870 → 33,939 |

The last step is eight weeks, not a month, and its window (8–14 November) is
not the 15th–21st. Read it as a separate observation, not a monthly change.

* **July:** summer timetables in both regions. Jun → Jul also has the most
  structural change: AKT 19 lines disappeared (11 explained by verified
  sources) and 6 appeared, and KOL 9 disappeared (8 explained).
* **February:** AKT alone drops by about a fifth. This is the Agder winter
  school holiday (week 8, 16–20 February 2026); see below.
* **August:** Kolumbus returns about 7.6% above its June level.

#### The two stop measures give very different answers

| Snapshot | AKT stops in patterns | AKT stops served in window | KOL stops in patterns | KOL stops served in window |
| --- | --- | --- | --- | --- |
| 15 Jan | 3,742 | 3,742 | 2,853 | 2,848 |
| 15 Feb | 3,744 | **2,269** | 2,852 | 2,847 |
| 15 Jun | 3,730 | 3,710 | 2,872 | 2,834 |
| 15 Jul | 3,676 | **2,175** | 2,863 | **1,986** |
| 15 Aug | 3,675 | 3,675 | 2,865 | 2,853 |

Route definitions barely change, but in the February holiday week about 1,470
AKT stops have no departure, and in July about 1,550 AKT and 870 Kolumbus
stops. Only 116 of AKT's 212 lines run in the February week. The stops that
drop out are mostly on AKT's 19xx and 23xx school routes. Reporting the
pattern measure as "stops served" would hide this, so the summaries use the
window measure for service and the pattern measure for route structure.

### Verified real change: Agder, 22 June and 1 July 2026

Source: AKT, *Route changes in Agder from 22 June and 1 July 2026*,
<https://www.akt.no/english/news/route-changes-in-agder-from-22-june-and-1-july-2026.9944.aspx>.
The data matches each announced change:

**Route 56 discontinued, Route 55 extended to Odderhei (Kristiansand).**

* `AKT:Line:1056` (bus 56, Rona – Odderhei, 178 departures a week) is no
  longer published from 15 July. The notice says it is discontinued, so it is
  in `config/route_discontinuations.json` (`discontinued_verified`).
* Bus 55 (Sørlandssenteret – Dvergsnes, 277 departures a week) moves from
  `AKT:Line:1055` to `AKT:Line:55`. The new ID is published on 15 June with
  0 departures and takes over all 277 on 15 July. Its route now also serves
  Odderhei, Odderhei nedre and Dvergsneskrysset (25 → 28 stops). This is the
  `same_code_new_line_id` candidate with stop similarity 0.89, published one
  month early.
* Eight stops were served only by line 56 and lost all service: Fuglevik,
  Fuglevikkleiva, Kjellevik, Knarrevik, Randesundheimen, Salbustad, Sømskleiva
  and Sømsveien/Sømslia (`stop_coverage_changes`, `stop_lost`, both measures).

**Route 178 replaced by Route 172, with AKT Svipp in Åseral.**

* `AKT:Line:178` (bus 178, Åseral–Evje, 37 stops, 131 departures a week) is
  no longer published from 15 July.
* `AKT:Line:172` (bus 172, Åseral–Evje, 23 stops) is published from 15 June. It
  has 0 departures in the 15–21 July window and 30 a week from August.
  Its stop similarity with 178 is only 0.46 (shorter route), so the automatic
  rules miss it. It is included through the verified mapping
  (`verified_mapping`).
* The supplementary line `AKT:Line:9178` (178S, published only on 15 June, no
  fixed departures) continues as `AKT:Line:9172` (SVIPP Åseral–Evje, 44
  departures a week from July), stop similarity 0.81.

**Kristiansand city lines, 22 June.** The same page also explains the other
Kristiansand IDs that changed in the June/July exports:

| Notice | Old line | New line |
| --- | --- | --- |
| Route 54 "Replaced by Route 32" | `AKT:Line:1054` (54) | `AKT:Line:32` |
| "Route 32 will be extended to/from Drangsvann" | `AKT:Line:1032` (32) | `AKT:Line:32` |
| Route 19: frequency every 35 instead of 30 minutes | `AKT:Line:1019` | `AKT:Line:19` |
| Routes 30 and 31 terminate at Grovane | `AKT:Line:1030`, `1031` | `AKT:Line:30`, `31` |
| Route 35 terminates at the Brattvollsheia turning loop | `AKT:Line:1035` | `AKT:Line:35` (published from 15 May) |

New line 32 is a merge, so its version lists both old lines. Night bus N110
(`AKT:Line:9110`) is a verified discontinuation: the notice cuts "the
Sunday-morning night bus departure at 02:45 from Arendal to Jovannslia", and
that was the line's only departure (26:45 on Saturday's service day).

Not marked, because the notice does not say the line ends: route 237 ("some
departures … transferred to Route 230"), route 36, and route 41 (a July trial
that is published from June and gone again in August).

### Verified real change: Kolumbus Bussveien, 1 July 2026

Source: Kolumbus, *Ruteendringer 1. juli 2026*,
<https://www.kolumbus.no/aktuelt/ruteendringer-1.-juli-2026/>. Bussveien
between Stavanger and Sandnes opened on 1 July 2026, and lines 1–4 were
renumbered around it. Every renumbering in the notice is in the data. In each
case the old line is last published on 15 June and the new line is published
by 15 July:

| Notice | Old line | New line | Stop similarity | How found |
| --- | --- | --- | --- | --- |
| "Rute 1 Stavanger – Hundvåg blir nå rute 9" | `8_1000` (1) | `8_1109` (9) | 1.00 | `similar_stops` |
| "Rute 4 Rosenli – Madlakrossen blir nå rute 12" | `8_1005` (4) | `8_1112` (12) | 1.00 | `similar_stops` |
| "Rute 2 Stavanger – Sandnes blir nå rute 1" | `8_1002` (2) | `8_1001` (1) | 0.26 | `verified_mapping` |
| "rute 2 Stavanger – Risavika blir rute 4" | `8_1002` (2) | `8_1104` (4) | 0.51 | `verified_mapping` |
| "Rute 3 Stavanger – Forus blir nå rute 2" | `8_1004` (3) | `8_1003` (2) | 0.46 | `verified_mapping` |

* Old line 2 (93 stops) was split in two. All of new line 4's stops and 75% of
  new line 1's stops were on old line 2; the rest of line 1 are new Bussveien
  stops. Old line 3 was split too: all stops of new line 2 and new line 3
  were on it.
* New lines 1 and 2 reuse the line IDs `8_1001` and `8_1003`, which had been
  published since January as lines **A** and **B** with no departures. These
  were placeholder entries for the Bussveien routes, given their final numbers
  in June. They are not renumberings of a running service, and their IDs did
  not change, so they need no mapping.
* The rule-based candidates `8_1002 → 8_1003` (shared number 2, similarity
  0.22) and `8_1004 → 8_1103` (shared number 3, 0.55) are **not** what the
  notice describes and stay unverified. They show why shared numbers are
  weak evidence. The notice's summary also says route 3 now runs Viste
  Hageby – Stavanger, so part of old line 3 does continue in the new line 3,
  but the route-by-route list calls line 3 a new route, so it is not marked.

The same notice explains four more July disappearances:

* "Rute X77 Blir lagt ned, og erstattes av rute 43": `8_1040` → `8_1024`.
  Line 43 was published from January with no departures, and from July it runs
  55 departures a week, the same as X77.
* Rute 46 "erstattes av rute nummer SK0203": `8_2014` → `8_2687`. SK0203 was
  published from April, three snapshots early, so only the verified mapping
  links it. All its stops were on line 46.
* Rute 67 "Blir lagt ned" (`8_1042`) and the boat route 704 to Borgøy
  (`8_5752`) are verified discontinuations.

### Resolved: AKT winter holiday, February 2026

The 15–21 February window covers the Agder winter school holiday in week 8
(16–20 February 2026). Kristiansand and Agder County Council use week 8, as their
current calendars for 2026/27 (22–26 February 2027) show; the 2025/26 pages
are no longer online, and week 8 of 2026 is 16–20 February
(<https://www.kristiansand.kommune.no/navigasjon/barnehage-og-skole/skole-og-sfo/skoleruta/>,
<https://agderfk.no/vare-tjenester/skole-og-opplaring/opplaring-i-skole/skoledager-ferier-og-fridager/>).
The daily data shows the drop exactly there:

| | AKT weekday (Mon–Fri) | AKT Saturday | AKT Sunday | KOL weekday |
| --- | --- | --- | --- | --- |
| 15–21 January | ≈ 3,730 | 1,668 | 897 | ≈ 5,520 |
| 15–21 February | ≈ 2,815 (−25%) | 1,667 | 896 | ≈ 5,560 |
| 15–21 March | ≈ 3,725 | 1,667 | 896 | ≈ 5,560 |

Only AKT's weekdays drop, and weekends are unchanged, so this is a school-day
reduction, not a route change. Kolumbus shows no reduction in that week (we
did not check Rogaland's holiday dates). The window-stop measure shows the same
thing: only 116 of AKT's 212 lines run that week, and the missing stops are on
school routes (19xx/23xx).

### Resolved in the data: AKT "T" bus numbers

`AKT:Line:583`, `585`, `586`, `587`, `588` and `590` drop the `T` from their
bus number on 15 September (e.g. `586T` → `586`), and `AKT:Line:3701` changes
`881T` → `881` on 15 July. The line IDs, stop sets (similarity 1.0) and
September departures are unchanged; only `881` goes from 2 to 4 departures a
week. These are label changes on continuing lines, already handled by the
version history (`same_line_id_new_code`), so no mapping is needed. We found no
AKT source that explains the `T` suffix (it may mark school-day services,
since these are low-frequency Kristiansand-area routes), so its meaning is
not documented here.

### Limitations and open points

* Snapshots are observations. A difference cannot be dated more precisely
  than the interval between two exports, and the pipeline holds no effective
  dates beyond what the evidence files quote.
* 24 disappearances across all transitions remain `not_observed`
  (unexplained), for example Kolumbus SK school lines and AKT 2235/2256. They
  are reported as no longer published, with no claim about why.
* Stop similarity uses stop sets, not sequences or distances; it is a
  screening tool for candidates, not evidence.

## GUI

A read-only interface to the published Gold and route-history outputs. It
reads only completed, registered checkpoints under the data root and never
writes to it. Pandas reads the small Gold Parquet tables directly, so no Spark
session is needed.

```bash
python3 -m venv ~/dat535-gui-env
~/dat535-gui-env/bin/pip install -r requirements.txt
bash scripts/run_gui.sh          # http://localhost:8501 (VS Code forwards the port)
```

`DAT535_DATA_ROOT` (default `/srv/dat535/data`), `DAT535_GUI_PORT` (default
8501) and `DAT535_GUI_PYTHON` can be overridden. The app listens on
`0.0.0.0` (all network interfaces, including localhost). SSH or VS Code port
forwarding to `http://localhost:8501` still works. For direct school-server
access, open `http://152.94.163.144:8501`; inbound TCP 8501 must be allowed by
the school/cloud and host firewalls. The app has no login.

The workflow calls `bash scripts/run_gui.sh --restart` after successful
processing and route history. The same command can be run manually: it stops
this app on its configured port, starts it in the background and writes
`DAT535_DATA_ROOT/metadata/gui.log`. After a VM reboot, run the command or a
successful workflow to bring the GUI back.

| Page | Shows | Source |
| --- | --- | --- |
| Overview | Select a month, operator and mode; departures, active lines, daily mean and coverage | Reconstructed Gold daily plans |
| Daily service | Select a month, operator and mode; departures and active lines by operating date, coverage and source export, daily CSV | Gold `daily_line_service`, published window metadata |
| Compare months | Monthly totals, percentage change, daily means, weekday profiles, line gains/losses and CSV | Reconstructed Gold daily plans, optional route-history names |
| One line | Search by number or place; service, observed versions, changes and stops in separate tabs; daily profile and CSV | Gold `daily_line_service`, route history |
| What changed | Consecutive exports: lines appeared/disappeared, verified explanations and sources, largest departure changes, stops no longer served | Route history, `config/route_*.json` |
| Replaced lines | A verified replacement: old/new departures and definition stops, with the operator source | Route history, `config/route_renumberings.json` |
| Data checks | Quality-gate results, reconciliation and retained exclusions split into valid on-demand / invalid / unclassified, CSV | Gold manifests, `excluded_service_dates`, optional route history and Silver report |

The comparison matches lines by operator and NeTEx ID. It does not infer a
replacement from similar numbers or a change in departures. Percentage change
is undefined when the earlier total is zero. Weekday profiles average over
calendar days, including days with no service. A new or missing service in a
service window does not establish that a route opened or closed; holidays
and seasonal plans also affect counts.

Daily/monthly service metrics default to operating dates through **today in
Europe/Oslo**. Completed months stay whole; the current month stops at today,
and wholly future months are hidden. **Include future timetable plans** restores
the full published windows. Future dates are explicitly labelled in daily tables
and CSV downloads and shown in amber in daily charts. They are published plans,
not proof that a journey has happened. Today's figures include that operating
date's whole timetable, not only departures before the current clock time.

When the later comparison month is still in progress, both months use matching
day ranges, capped to the shorter month's length. The displayed date ranges and
covered-date denominators show what was compared. Monthly CSVs retain the true
`calendar_days` plus `period_days` and `period_end` for the selected view.
Comparison CSVs record `earlier_period_end` and `later_period_end` as well.
History and data checks retain the full published exports and their audit totals.

Daily service includes every calendar date in the selected period. A date is
covered only if a published output's window and source-calendar validity include
it for the full provider selection. Overlapping windows use the latest such
export for the whole date, even if observed later; missing lines are not
backfilled from older plans. Selection is independent of UI filters.
An uncovered date stays blank, while covered dates without matching service
are zero. The mean uses covered dates only, and the coverage card shows the
denominator. These stitched plans may come from different exports; they are
not actual operations or a single month's timetable release.

Processing now defaults to full calendar months. Older seven-day outputs need
the [full-month rebuild](#full-month-daily-service), including validated
support exports for dates discarded from the 15th-of-month files. That rebuild
preserves old files and records a provider/date coverage audit. Supporting sources
do not change the original route-history observation sample.

The exclusion breakdown reads the retained rows of the selected Gold output.
One occurrence is one journey on one operating date, not one XML file. Valid
on-demand availability windows are separate from fixed departures. If older
outputs lack complete type/validity flags, the GUI reports their reasons as
unclassified instead of guessing. The historical seven-day January rerun recorded
23 valid on-demand occurrences and no invalid occurrences; the older local January
output has incomplete type flags and should not be used to infer their reasons.
Those seven-day counts are not the full-month totals.

Overview, daily service, comparisons and data checks work without route history. A single
published export can be viewed; comparisons need two months. The GUI reads Gold from
`pipeline_state.sqlite3` in read-only mode and does not require the Silver
files. Exports must share a provider selection and service-window definition.
The history pages use the latest complete history output and show its last
observation date. If history is older than Gold, the app shows a notice.

Results are cached for 60 seconds and checked again on interaction after the
cache expires. **Refresh data** clears the cache immediately. The GUI does not
trigger ingestion or processing.

Gold outputs built before the quality gate show the gate as *not recorded*.
The next processing run records it. Manual ZIP upload (T55) is not part of this
app; see [manual ingestion](#manual-zip-ingestion).

`app/data.py` holds data access, `app/analytics.py` holds the comparison and
calendar calculations, and `app/gold_app.py` holds the pages. Run their unit
tests in the GUI environment:

```bash
~/dat535-gui-env/bin/python -m unittest tests.test_data tests.test_analytics tests.test_app -v
```

## Evidence

Dated records keep their original scope; older seven-day results are not
evidence for the rebuilt full-month totals.

### Verified full-month coverage

#### School-server completion — 10 November 2026

The server rebuild `monthly-67b04d35cf56` completed all **22 jobs**. Its coverage
audit passed for every month from **January through November 2026**. Route history
was recorded at `gold/entur/history/d5d5ea417f73a691/history-53300d0ab697` under
`/srv/dat535/data`. The user supplied the completed report summary and a disk
check showing **20 GiB free**, with root disk use at 47 percent.

These checks establish completed jobs and calendar coverage, not actual vehicle
operations or 11 sets of independently recounted monthly totals. The detailed
departure totals below are the separately verified January–September local run.

Complete coverage means a valid published plan covers each date. The GUI defaults
to dates through today in Norway; later November dates appear only when future
plans are enabled. This display limit does not remove Gold data or change the audit.

#### Detailed local verification — January–September

Verified locally on 10 November 2026 from the rebuilt KOL + AKT dataset.
Nine primary exports and nine calendar-support exports cover all 273 operating
dates (546 provider/date pairs). All 18 Gold publication gates passed.
These are reconstructed published fixed-service plans, including bus and water,
not actual operations or 273 daily observations. On-demand service is retained separately.

| Month | Dates covered | AKT departures | KOL departures | Total |
| --- | ---: | ---: | ---: | ---: |
| 2026-01 | 31/31 | 90,368 | 131,583 | 221,951 |
| 2026-02 | 28/28 | 80,266 | 125,589 | 205,855 |
| 2026-03 | 31/31 | 90,803 | 138,292 | 229,095 |
| 2026-04 | 30/30 | 81,428 | 125,517 | 206,945 |
| 2026-05 | 31/31 | 82,645 | 127,846 | 210,491 |
| 2026-06 | 30/30 | 83,767 | 127,770 | 211,537 |
| 2026-07 | 31/31 | 68,712 | 113,513 | 182,225 |
| 2026-08 | 31/31 | 81,499 | 131,815 | 213,314 |
| 2026-09 | 30/30 | 87,204 | 150,090 | 237,294 |

**Total fixed departures: 1,918,707.**

Overview, Daily service, Compare months and line profiles were checked against
this real dataset. January shows 31/31 dates and February 28/28. January 1 has
2,722 fixed departures and January 31 has 4,337; both retain their source export.
The combined date selection takes one whole valid export per day and does not
sum overlapping snapshots. Monthly totals reconcile with those selected daily rows.

All 140 tests passed across the GUI and Spark environments. Saved file
inventories were rechecked for all 54 completed processing stages.

The 1 and 2 February sources failed reference validation on `NSR:Quay:43289`.
A validated 14 January source, whose calendars cover February, supplies the
earlier February plan. Its source date is kept; it is not renamed as February.
Other support sources are dated the first of their requested month.
All failed and successful raw files and the previous seven-day outputs remain.

Local data root: `data/monthly_2026_01_09` (generated, not in Git).
Rebuild audit: `data/monthly_2026_01_09/rebuilds/monthly-bb78e2091c9b/manifest.json`.
Route-history output: `data/monthly_2026_01_09/gold/entur/history/cf56da4ff430d375/history-6ca77aeab099`.
The saved CSV is `data/monthly_2026_01_09/monthly_service_verified.csv`.
This section records the local rebuild; the later school-server completion is
recorded above. See [server commands](#full-month-daily-service).

### Quality validation, 10 November 2026

This checks the provider-completeness gate and the expanded independent XML
traces. It is a local verification of January, not a claim that the school
server or all ten snapshots have been upgraded.

#### Full January snapshot

The preserved 15 January XML was reprocessed with the current staging schema,
then Silver and Gold were rebuilt in temporary storage. Existing archives,
manifests and Parquet outputs were left unchanged. Temporary processing outputs
were removed after the check; the small verification reports were retained.

| Measure | Result |
| --- | --- |
| Journey occurrences | 25,289 |
| Passing-time occurrences | 768,701 |
| Service-date occurrences, 15–21 January | 53,345 |
| Fixed departures published | 53,322 |
| Flexible occurrences excluded | 23 |
| Gold publication gate | pass, version 2, both AKT and KOL |
| Silver → Gold counts | reconciled |

The old local staging schema lacked `FlexibleLineRef`, and its Silver output
also lacked flexible-window fields. Those old outputs were not relabelled or
patched to pass. Rebuilding from XML produced all four trace cases, including
the flexible journey that was unavailable as a valid example in the old output.
The checker reports missing window fields as a failed check when the XML
contains a window.

#### Independent source checks

| Case | Line | Passing times checked | Whole-line source journey occurrences | Fixed departures in window | Result |
| --- | --- | --- | --- | --- | --- |
| Ordinary | AKT 411 | 14 | 20 | 52 | pass |
| Overnight | AKT 100 | 69 | 150 | 418 | pass |
| Calendar exception | KOL SK2104 | 9 | 11 | 14 | pass |
| Flexible | KOL HentMeg | 2 | 7 | 0; 11 flexible occurrences excluded | pass |

For each case the standard-library XML reader independently resolved the line,
pattern point, scheduled stop, assignment and physical stop. Quays were linked
to national stop places; the flexible journey was linked to a flexible stop
place. Resolved identities/versions, fixed times, flexible windows and operating
dates matched Silver. Counts derived from every source journey occurrence of
the selected line matched Gold by date, including flexible exclusions.

The line recount is independent of Silver's validity flags and calendar code.
Unsupported restrictions or ambiguous source definitions stop the check. These
are selected line checks, not independent certification of every source row.

#### Regression results

The final Spark-enabled suite ran 102 tests in 149 seconds: 99 passed and
3 GUI tests were skipped because pandas is intentionally absent from that
environment. Those 3 GUI tests passed separately in the isolated GUI environment.
`git diff --check` also passed.

#### Failure cases covered by regression tests

* An empty or missing provider selection fails. A missing expected provider
  still fails if a zero-service-window override is supplied.
* Unexpected providers and staging/Silver occurrence-count differences fail.
* Calendar impact is measured before date expansion. The fixture with broken
  AKT calendars has two affected journeys and no expanded dates; its impact
  rate is 100%, rather than zero.
* A wrong physical quay or swapped flexible window is detected even when
  fixed arrival/departure values match.
* Missing/ambiguous assignments and ambiguous journey definitions are refused.
* Repeated journey IDs in different XML files remain separate occurrences.
* Moving another journey's service date in both Silver and Gold leaves their
  recounts consistent. The independent whole-line XML recount detects it.

Full per-stop traces, JSON results and implementation hashes are saved locally
under `docs/audit/2026-11-10/` (ignored by Git). The older February trace report
is preserved as earlier evidence. See [data quality](#data-quality) for the
commands to regenerate evidence on the school dataset.

## Task plan

6-week plan · 2 developers + AI

This is the task breakdown I recommend for you and your partner, organized into epics, user stories, technical tasks, ownership, and deliverables.

The workload is divided so that both of you can work independently without constantly blocking each other, while still collaborating on the most important part of the course: Silver processing using PySpark.

### 1. Project scope

Project title: Kolumbus and Agder Historical Service Evolution and Analytics Platform

Problem we're solving: Kolumbus and Agder bus routes, numbers, stopping patterns, and timetables change over time. We want to build a data platform that preserves these changes and makes it possible to compare planned public-transport services historically.

| Item                 | Decision                                          |
| -------------------- | ------------------------------------------------- |
| Region               | Rogaland and Agder: Kolumbus (`KOL`) + Agder Kollektivtrafikk (`AKT`) |
| Historical period    | 15 January – 15 September 2026                   |
| Historical ingestion | Nine monthly NeTEx snapshots, on the 15th of each month |
| Other ingestion      | Manual ZIP ingestion, API polling and GitHub Actions scheduling |
| Processing           | PySpark + Spark SQL                               |
| Architecture         | Bronze → Silver → Gold                            |
| Interface            | Streamlit GUI                                     |
| Deployment           | Group PySpark server                              |
| Development duration | 6 weeks                                           |

### 2. Responsibility split

#### Developer A

Historical data & analytics lead

Owns historical data acquisition, NeTEx parsing, core Silver transformations, route history and Gold analytics.

Main focus: dataset correctness, modeling, Spark processing.

#### Developer B

Ingestion, automation & GUI lead

Owns manual uploads, API polling, cron jobs, pipeline orchestration, deployment and GUI.

Main focus: operational reliability, application and integration.

Both developers: Silver quality validation, architecture decisions, integration testing, PySpark performance analysis and the final report.

### 3. Epic and task backlog

The checkboxes below are your implementation checklist. Each user story explains the purpose of the work.

#### Phase 1 — Source data and ingestion

##### EPIC 01 — Project setup and planning

User story: As a data engineering team, we want a clearly defined project scope and development environment so that we can build and deploy a reproducible pipeline.

Week 1 · Both developers

| Task ID | Task                                                  | Owner |
| ------- | ----------------------------------------------------- | ----- |
| T01     | Create GitHub repository and issue board              | B     |
| T02     | Set up Python, PySpark and project dependencies       | B     |
| T03     | Finalize data sources and nine-month coverage          | A     |
| T04     | Define Bronze, Silver and Gold table specifications   | A     |
| T05     | Prepare architecture diagram and project definition   | Both  |
| T06     | Define directory structure and dev/prod configuration | B     |

Done when: Both developers can run a basic PySpark job, use the repository, and agree on the table schemas and scope.

##### EPIC 02 — Historical dataset acquisition

User story: As a transport analyst, I want access to nine months of historical Kolumbus and Agder timetables so that I can investigate how services changed over time.

Weeks 1–2 · Developer A

| Task ID | Task                                                                 | Owner |
| ------- | -------------------------------------------------------------------- | ----- |
| T07     | Connect to the Entur historical NeTEx archive listing                | A     |
| T08     | Select nine monthly snapshots from January through September 2026 | A     |
| T09     | Build historical ZIP downloader                                      | A     |
| T10     | Filter/extract KOL and AKT files and required shared reference data     | A     |
| T11     | Create snapshot manifest: source date, file size, checksum, status   | A     |
| T12     | Validate downloaded archives and missing dates                       | A     |

Done when: Selected historical archives are reproducibly available and can be re-downloaded or reprocessed.

##### EPIC 03 — Manual upload and API ingestion

User story: As a platform operator, I want to upload historical files and automatically collect API updates so that data can enter the system through multiple ingestion methods.

Weeks 2–3 · Developer B

| Task ID | Task                                                               | Owner |
| ------- | ------------------------------------------------------------------ | ----- |
| T13     | Implement manual ZIP ingestion via Python CLI/service              | B     |
| T14     | Validate ZIP integrity, format and duplicate uploads               | B     |
| T15     | Store original uploaded archive with ingestion metadata            | B     |
| T16     | Build Entur API polling client for KOL and AKT                                 | B     |
| T17     | Implement request interval, timeout, retry and rate-limit handling | B     |
| T18     | Store raw API responses with source and ingestion timestamps       | B     |
| T19     | Implement ingestion success/failure logs                           | B     |

Done when: Both manual ingestion and repeated API polling can write raw data to Bronze without creating duplicate logical ingestions.

Implemented on 28 October: manual KOL + AKT ZIP intake through
`pipeline.ingestion.ingest_local_zip`, including retained original uploads,
CRC/XML/reference checks and repeat-upload detection. See
[manual ingestion](#manual-zip-ingestion). The GUI upload interface and shared
duplicate detection across API polling and manual intake remain open, so this
epic is not complete yet.

##### EPIC 04 — Bronze and NeTEx parsing

User story: As a data engineer, I want to preserve source records and convert complex XML into structured staging data so that they can be processed reliably using PySpark.

Weeks 2–3 · Developer A, with B integrating the ingestion paths

| Task ID | Task                                                        | Owner |
| ------- | ----------------------------------------------------------- | ----- |
| T20     | Implement Bronze storage layout for ZIP, XML and API data   | A     |
| T21     | Extract lines, routes and service journeys from NeTEx       | A     |
| T22     | Extract journey patterns, stop references and passing times | A     |
| T23     | Extract calendars, validity periods and shared definitions  | A     |
| T24     | Convert parsed entities into structured staging tables      | A     |
| T25     | Preserve source IDs and file/snapshot lineage               | A     |
| T26     | Test record counts and parser failures                      | Both  |

Done when: At least one complete Kolumbus and Agder snapshot is transformed into structured records, with the original archive preserved and all output rows traceable to their source.

First projection implemented on 28 October: direct Spark 4 XML reads and Parquet
for provider CompositeFrames, lines, service journeys and ordered passing times.
Source hashes, parent frame IDs/versions and occurrence indices are retained.
That first standalone projection was later folded into the complete staging
step (its 28 October run is recorded in the journal). The complete
projection adds routes, journey patterns/points, shared calendar definitions,
scheduled stops, assignments, flexible stops, national stop places and quays.
The first January run matched all Bronze counts locally. See
[checkpointed processing](#processing) for schemas, commands and
validation limits; server verification is recorded separately in the journal.

#### Phase 2 — Silver and historical analytics

This is the most academically important phase. Both developers must contribute PySpark code here, even though A leads the data model.

##### EPIC 05 — Silver preprocessing and data quality

User story: As a transport analyst, I want accurate, standardized and connected timetable data so that my comparisons are not affected by inconsistent timestamps, duplicates, or unresolved references.

Weeks 3–4 · Both developers

| Task ID | Task                                                           | Owner |
| ------- | -------------------------------------------------------------- | ----- |
| T27     | Define Silver table grains, keys and schemas                   | A     |
| T28     | Profile missing values, duplicated keys and data types         | B     |
| T29     | Standardize columns, IDs, datatypes and timestamps             | A     |
| T30     | Handle overnight trips and departure-day offsets               | A     |
| T31     | Handle flexible services and special timetable fields          | B     |
| T32     | Implement appropriate deduplication rules                      | B     |
| T33     | Join journeys, routes, stops and service calendars             | A     |
| T34     | Validate referential integrity and prevent join multiplication | A     |
| T35     | Quarantine invalid records with reason codes                   | B     |
| T36     | Produce data-quality metrics before and after Silver           | B     |
| T37     | Write unit tests for transformations and Spark SQL checks      | Both  |

Done when: The cleaned Silver data is reusable and documented, with passing schema tests, validated joins, and quantitative quality results.

First implementation: file-scoped version-aware joins, calendar expansion,
signed overnight offsets, flexible-service classification, count reconciliation,
quality reasons and quarantine outputs. This is a first processing slice;
timezone/DST timestamps, broader profiling and all remaining special NeTEx
cases still need assessment against the historical snapshots.

##### EPIC 06 — Historical route change detection

User story: As a transport analyst, I want to identify bus-number, route, and timetable changes so that I can accurately compare Kolumbus and Agder services before and after major network reorganizations.

Week 4 · Developer A, with B testing

| Task ID | Task                                                           | Owner |
| ------- | -------------------------------------------------------------- | ----- |
| T38     | Compare line and route definitions across historical snapshots | A     |
| T39     | Detect added, changed and retired routes                       | A     |
| T40     | Compare stop sequences and journey patterns                    | A     |
| T41     | Implement SCD Type 2 / versioned route dimensions              | A     |
| T42     | Create mappings for verified route renumberings                | A     |
| T43     | Test effective dates and historical timetable joins            | B     |

Done when: We can demonstrate a documented route change in Kolumbus or Agder while retaining the earlier version and producing correct results for historical service dates.

##### EPIC 07 — Gold analytical tables

User story: As a transport analyst, I want aggregated information about planned bus services so that I can understand changes in service frequency and network coverage.

Weeks 4–5 · Developer A

| Task ID | Task                                                         | Owner |
| ------- | ------------------------------------------------------------ | ----- |
| T44     | Calculate scheduled departures per route and date            | A     |
| T45     | Create weekly/monthly planned service-frequency metrics      | A     |
| T46     | Create route addition, retirement and modification summaries | A     |
| T47     | Calculate stop coverage and changes over time                | A     |
| T48     | Validate Gold aggregates against Silver source counts        | Both  |

Done when: Gold datasets support comparisons across selected dates and are reconciled against the underlying timetable records.

First implementation: daily planned departures per line, bounded service-window
summaries, planned stop coverage and reconciliation with eligible Silver
service occurrences. Route-change/SCD tables and before/after comparisons are
also implemented in the history layer. Default processing now expands full calendar
months. Validated calendar-support exports fill early dates absent from the
15th-of-month files; the rebuilt dataset must pass a provider/date coverage
audit. These are reconstructed monthly plans, not daily observations or actual
operations. See [full-month processing](#full-month-daily-service).

#### Phase 3 — Automation, GUI and deployment

##### EPIC 08 — Scheduled pipeline orchestration

User story: As a platform operator, I want data ingestion and processing to happen automatically so that the data platform remains current without manual execution.

Weeks 3–5 · Developer B

| Task ID | Task                                                    | Owner |
| ------- | ------------------------------------------------------- | ----- |
| T49     | Configure push-triggered and scheduled GitHub Actions ingestion on the school Linux runner | B     |
| T50     | Configure periodic API polling                          | B     |
| T51     | Configure weekly snapshot comparison and reconciliation | B     |
| T52     | Orchestrate Bronze → Silver → Gold dependencies         | B     |
| T53     | Add job locking, safe reruns and failure recovery       | B     |

Done when: Scheduled jobs run without manual intervention, and failed upstream processing prevents invalid downstream publication.

The processing CLI now has persistent SQLite stage checkpoints, exclusive
snapshot locks, implementation/input fingerprints and verified output reuse.
It chains staging → Silver → Gold and retains failed attempts. The development
workflow now calls processing after validated ingestion and copies checkpoint
status into its report artifact. That change must reach `main` and pass an
automatic run before end-to-end scheduling is claimed complete.

##### EPIC 09 — Streamlit GUI

User story: As a researcher or transport analyst, I want an interactive application so that I can explore historical Kolumbus and Agder services without needing to understand the underlying XML or PySpark code.

Week 5 · Developer B

| Task ID | Task                                      | Owner |
| ------- | ----------------------------------------- | ----- |
| T54     | Create Streamlit app with navigation      | B     |
| T55     | Implement manual ZIP upload interface     | B     |
| T56     | Create historical date/route filters      | B     |
| T57     | Build before-and-after route comparison   | B     |
| T58     | Build service-frequency visualizations    | B     |
| T59     | Display data-quality and ingestion status | B     |
| T60     | Connect GUI to published Gold datasets    | B     |

Done when: A user can upload a ZIP and explore real Gold data, including changes in scheduled services around a known route reorganization.

##### EPIC 10 — Deployment and CI/CD

User story: As a data engineer, I want an automated and reproducible deployment so that another engineer can run and maintain the platform.

Weeks 1–6, implemented incrementally · Developer B

| Task ID | Task                                                  | Owner |
| ------- | ----------------------------------------------------- | ----- |
| T61     | Package Python/PySpark dependencies and configuration | B     |
| T62     | Configure GitHub Actions for linting and tests        | B     |
| T63     | Prepare deployment scripts for the Spark server       | B     |
| T64     | Set up environment-specific paths and credentials     | B     |
| T65     | Configure application/job logging and monitoring      | B     |
| T66     | Test recovery and redeployment                        | B     |

Done when: The platform can be configured and deployed consistently, with passing automated checks and visible operational status.

##### EPIC 11 — Testing, performance and final report

User story: As a course assessor or another engineer, I want evidence that the platform is correct, scalable and reproducible so that its engineering quality can be evaluated.

Weeks 5–6 · Both developers

| Task ID | Task                                                        | Owner |
| ------- | ----------------------------------------------------------- | ----- |
| T67     | Run end-to-end ingestion and transformation tests           | Both  |
| T68     | Benchmark Spark joins, partitioning and execution plans     | A     |
| T69     | Test duplicate ingestion, bad files and recovery behavior   | B     |
| T70     | Verify historical route changes against original records    | A     |
| T71     | Prepare architecture, modeling and quality documentation    | A     |
| T72     | Prepare operations, automation and deployment documentation | B     |
| T73     | Write results, limitations and engineering reflections      | Both  |
| T74     | Prepare demo and final presentation                         | Both  |

Done when: An end-to-end pipeline run can be reproduced, tested and explained, with measurable Spark performance evidence and a complete report.

### 4. Weekly task assignments

Here is the actual work allocation I would use.

| Week | Developer A                          | Developer B                   | Joint milestone                                 |
| ---- | ------------------------------------ | ----------------------------- | ----------------------------------------------- |
| 1    | T03–T05, T07–T12                     | T01–T02, T06, initial T61–T62 | Dataset scoped and sample parsing started       |
| 2    | T20–T26, finish historical ingestion | T13–T19                       | Bronze pipeline functional                      |
| 3    | T27, T29–T30, T33                    | T28, T31–T32, T35, T49–T51    | First trusted Silver tables                     |
| 4    | T34, T38–T42, T44–T45                | T36–T37, T43, T52–T53         | Silver validated and historical changes modeled |
| 5    | T46–T48, T68, T70                    | T54–T66                       | Gold and GUI deployed                           |
| 6    | T67, T71, T73–T74                    | T67, T69, T72–T74             | End-to-end tests, benchmarks and report         |

Some tasks are deliberately started before their main week—for example, CI/CD in Week 1—so that deployment doesn't become a last-minute problem.

### 5. Dependency order

EP-08 scheduling, EP-10 CI/CD and testing are cross-cutting workstreams. The GUI should start with mock data but be validated against published Gold tables.

The key coordination point is the ingestion-to-Silver contract. Developer B must write raw data and metadata in a format Developer A can consume without manual fixes. Agree on that interface in Week 1.

### 6. Recommended first sprint

I would not start with the GUI. Your first sprint should prove the source works.

#### Sprint 1 — First 7 days

Developer A

- Identify the selected NeTEx snapshots.
- Use January and September 2026 samples for both KOL and AKT.
- Parse lines, journeys and journey patterns.
- Inspect real join keys and source validity dates.

Developer B

- Set up GitHub and PySpark environment.
- Build a basic ZIP ingestion script.
- Establish Bronze folder conventions and metadata.
- Verify an Entur polling endpoint and capture a sample response.

Joint checkpoint: Demonstrate a sample timetable archive flowing into Bronze and at least one structured PySpark table, and document the agreed schema.

### 7. Scope boundaries

To keep this realistic for two people, I would make these explicit:

Must deliver: Nine-month selected historical dataset for KOL and AKT, manual upload, API polling, daily/weekly cron, Bronze, substantial Silver, versioned route history, Gold, Streamlit GUI, tests, and deployment evidence.

Only if time permits: Full daily historical backfill, WebSocket streaming, Kafka, live vehicle map, and advanced automatic detection of complex route splits or mergers.

Not part of the historical analysis: Actual bus punctuality before the date you began collecting realtime observations. Historical NeTEx supports planned timetables, not historical observed delays.

### Final division

**Developer A — data pipeline**

Historical acquisition, NeTEx parsing, Silver data model, route change tracking, Gold analytics.

**Developer B — platform pipeline**

Manual ingestion, API polling, cron, Silver quality controls, GUI, CI/CD and deployment.

The responsibilities are balanced by engineering complexity, not by ticket count. Developer A has fewer but more technically demanding NeTEx and historical-modeling tasks. Developer B has more integration and infrastructure tasks.

Treat these as the initial assignments, then rebalance after Week 2 based on the actual parsing complexity and available server resources.

The non-negotiable target is a functioning Bronze → Silver → Gold pipeline by the end of Week 4. That leaves the final two weeks for the GUI, automated deployment, performance testing, and reporting—without putting the course's primary Silver-layer requirements at risk.

## Project journal

A running note of what we did, what went wrong and why. Earlier entries keep the
file names and commands that existed at the time.

This is just a running note of what we have done, what went wrong and why we
made certain decisions. We can add to it as we go. It does not need to read
like the final report.


### 22 October 2026 - Getting the dataset sorted

We started with Kolumbus, mainly because it gave us a clear geographical
scope in Rogaland. The idea was to look at how planned routes, stopping
patterns and timetables change over time. We were trying to collect roughly
monthly NeTEx snapshots from January 2025 through September 2026.

The first downloader could fetch the national Entur ZIP files and make
Kolumbus-focused extracts. At first the collection looked fairly complete:
there were 21 ZIP files with the expected dates in their names. But we decided
to inspect the actual contents before starting preprocessing. That turned out
to be important.

All twelve files labelled as 2025 had exactly the same SHA-256 checksum. They
were copies of the same payload. Inside them, the ordinary Kolumbus timetable
XML had a publication date of 2 January 2026. So the names looked historical,
but they did not give us the twelve different 2025 observations we wanted.

We checked the public bucket listing as well. All 365 daily NeTEx objects
labelled as 2025 reported the same MD5 checksum and were created on 2 January
2026. We also checked the publicly visible generations and did not find an
older usable version there. We do not know what caused this. It looks like
the same export ended up under a lot of dated names, but that is only our
interpretation of the evidence.

The script was generating different URLs correctly. Its weakness was that
HTTP 200 and a readable ZIP were being treated as enough proof that we had
the right historical data. There was also a reuse problem: an existing
Kolumbus ZIP could make the script skip extraction even after a national
archive had been downloaded again. The old manifest was overwritten on each
run too, which explained why it was not a complete record of the local files.

We used AI assistance to inspect the archives and improve the acquisition
code. We added source identity and checksum checks, publication-date checks,
validation before reusing an extraction, and separate manifests for runs.
The important lesson here was that a ZIP passing its CRC checks still needs
to be checked for what it actually represents.


Deciding what data to use

We found a separate historical GTFS archive with different checksums and
matching creation dates for the 2025 exports. A January 2025 sample contained
Kolumbus routes, trip records and 2025 calendars. So there was a possible
alternative, although we only checked selected members of that sample and
did not validate every complete monthly GTFS archive.

We decided to continue with NeTEx and use January to September 2026. Switching
formats would have meant changing the parser and working with a reduced
representation of the original NeTEx data. We also did not want the project
to depend on waiting for a reply from Entur or Kolumbus about old backups.
No request was sent to either organisation.

Then we came back to the amount of data. The teacher had mentioned around
3.5 GB or more. The nine valid Kolumbus snapshots contained about 2.55 GB of
provider XML. Their extracts were larger when national stop data and other
providers' shared files were included, so we had to be careful about what
we were actually counting.

We considered taking more frequent Kolumbus snapshots or adding another
provider. We chose Kolumbus plus Agder Kollektivtrafikk, whose code in the
files is AKT. The main reason for choosing Agder was practical: it brought
the provider XML total to about 4.15 GB while adding just one provider.
Skyss would have made the dataset much bigger. Comparing the two providers
could also be useful later, but we have not done that analysis yet.

Our agreed scope is now KOL + AKT, with one snapshot on the 15th of each
month from January through September 2026. That is nine combined snapshots.
These are observations of published timetables. They do not automatically
give us every change that happened between those dates.


Building the combined dataset

We downloaded the nine national source archives, checked their source
checksums and ZIP integrity, and extracted the KOL and AKT line files,
their shared definitions and the matching national stop registry. The
selected XML was kept unchanged, including its original IDs and versions.

We kept both the XML files and compact ZIPs of the same selected content.
We also recorded source generations, checksums, file-level lineage and
validation results. The August download timed out near the end. We recovered
the partial transfer and checked it before using it, so we did not have to
restart the whole dataset build.

The final numbers are:

  Kolumbus provider XML:          about 2.55 GB
  Agder provider XML:             about 1.61 GB
  Both providers together:        about 4.15 GB
  XML including national stops:   about 7.96 GB
  Compact ZIPs:                   about 586 MB

There are 4,866 XML files and nine compact ZIP files. The ZIPs and XML folders
are two forms of the same data, so we should not add their sizes together
and present that as extra source data. The 4.15 GB figure is uncompressed
provider XML. We still need to be clear about that when discussing the
teacher's size expectation.


The Agder ID issue

The July validation found repeated ID/version pairs in Agder's XML. We
looked into an actual example instead of immediately removing duplicates.
The same ServiceLinkInJourneyPattern ID appeared at order 2 and order 30
inside the same journey pattern, and both pointed to the same service link.

A journey pattern may visit the same link more than once. The repeated ID
alone does not tell us which occurrence, if any, is incorrect. If we used
dropDuplicates on ID and version, we could lose one of the sequence positions.
That would quietly change the pattern we were trying to analyse.

There were 182 repeated ID/version occurrences across the Agder snapshots:
61 in July, 60 in August and 61 in September. We preserved them in the raw
data and recorded warnings. We have not cleaned or corrected them yet.
This is something to handle properly in the Silver work, with the parent
journey pattern, sequence order and snapshot context available.

The provider reference checks passed for all nine snapshots, including the
explicit same-file versions. There are also unresolved references found only
in the broader national stop registry, which were recorded separately.
We have not completed full NeTEx schema validation or calendar validation,
so we should describe the checks we actually performed rather than calling
the whole dataset perfectly clean.


Cleaning up the files and updating the plan

The national ZIPs downloaded for this build were deleted after the extraction
and validation checks passed. We kept the original selected XML, the combined
ZIPs and the provenance information. Later, after checking that the new data
contained the matching 2026 Kolumbus files and stops, the previous Kolumbus
folder and old netex_manifest.csv were removed. The audit evidence remains.

The dataset folder became a bit messy with reports and manifests everywhere,
so we organised only that folder. It now has four things at the top level:

```text
entur_netex_snapshots/kol_agder_2026/
  README.md
  xml/
  zip/
  metadata/
```

The detailed reports, manifests and lineage are under metadata. The XML and
ZIP contents were unchanged by that tidy-up. The completed delivery layout
is organised differently from an interrupted generator run, so a fresh
regeneration should use a new output directory.

We also updated task_readme.md to cover Kolumbus and Agder and the January
to September 2026 period, with nine monthly snapshots. We kept the original
74 tasks, responsibility split and six-week development plan. We have not
rewritten the whole plan or marked every task as completed.


Where we are now

We have the source dataset, acquisition tools, validation evidence and 16
passing regression tests for the acquisition/generation code. The current
dataset is marked complete_with_warnings because we preserved the source
issues described above.

We do not have the full data engineering pipeline yet. Scanning XML for
validation and entity counts is not the same as producing structured tables.
The PySpark transformations, database publishing, API polling, scheduled
jobs and GUI are still work to do.

The next sensible step is to take one complete snapshot for both providers
and extract the lines, routes, journeys, journey patterns, passing times,
stops and calendars into structured tables. We need to keep source IDs,
versions, sequence positions and file/snapshot lineage while doing that.
Once the joins and quality rules work for one snapshot, we can run them
over all nine.

Another thing to remember: seeing a difference between January 15 and
February 15 does not prove the change happened on either of those days.
Exact effective dates need calendar/validity evidence or more snapshots.
And these timetable files describe planned services; they do not tell us
which buses actually ran or how late they were.

We also noticed that database storage is not explicit in the task breakdown
yet. That still needs a decision. We have not chosen or set up the database.


Next entry:

Date:
What we worked on:
What worked / what did not:
Decisions and why:
What we need to do next:


### 22 October 2026 - Giving the project a clearer layout

We moved the acquisition scripts into pipeline/ingestion and the plan, journal
and audit notes into docs. There are now separate places for processing,
storage, analysis, the GUI, configuration and scheduled-job scripts. Most of
those are still just reserved areas for the work ahead. The existing dataset
stayed where it was so we did not break its source and file lineage paths.
The scripts are now run with python -m from the project root.


### 22 October 2026 - Keeping the initial ingestion setup simple

We have started with a small historical batch ingestion step: use Entur's
public archive API to get the national NeTEx files, select KOL and AKT and
their required shared files, and save the data with validation and metadata.
Periodic API polling is still something we need to implement later.


### 23 October 2026 - Adding a weekly collection job

The nine monthly snapshots are our starting dataset, but we also want new data
to arrive after that. We decided to use GitHub Actions for scheduling and chose
weekly collection. The workflow is set for Monday morning, using Sunday's
dated archive. That gives the source export time to finish and keeps the dates
clear. There is also a manual run button for checking the setup.

We reused our KOL and AKT extraction and reference checks instead of making a
second filtering approach. New runs get their own folders. Downloads have a
few retries for temporary network errors, and an interrupted attempt keeps its
bytes rather than overwriting them. Source repeats still stay as warnings;
missing provider references fail the run.

For now the workflow saves the selected raw ZIP and reports as downloadable
GitHub artifacts. This is enough to start running the job, but the artifacts
expire. We still need to decide on permanent storage before relying on this
as our growing history. Manual reruns can save the same source again; there is
no shared duplicate lookup between GitHub runs yet.

The new fixture tests passed, and a live metadata-only check found the archive
for 22 October. We have not run the complete job on GitHub yet. The workflow
needs to be pushed to the default branch and checked there. This step collects
raw data; it does not build Silver, Gold, a database or weekly comparisons yet.


### 23 October 2026 - Clarifying where the job will actually run

We clarified that the school will provide a Linux server and that we will run
the GitHub Actions runner there. GitHub will handle the weekly timer, while
the school machine does the download, filtering and validation.

This changes the storage arrangement from the earlier note. The workflow now
keeps raw archives, XML, ZIPs and metadata on persistent school storage. It
only uploads a copy of the report to GitHub. An expiring GitHub artifact is
therefore no longer the place we rely on for keeping the data.

We made the storage directory configurable as DAT535_DATA_ROOT. It needs to
sit outside the checkout because checkout cleanup can remove ignored files.
The workflow checks that setting before checking out the code. There is no
automatic removal of server data, including failed download attempts.

We still have to register the Linux runner, add the dat535 label, choose the
actual storage path and give the runner account access. None of that has
been done on the server yet. The setup steps are in scheduled_ingestion.md.


### 24 October 2026 - Setting up the runner on spark

We connected to the school Linux server through its SSH jump host. The jump
host needed an interactive login, so we opened a shared SSH connection from
the Mac terminal and used that to finish the setup.

There was already a runner for the group04 repository. We left that service
alone and installed this project's runner separately in
/home/ubuntu/dat535-runner. The new runner is called dat535-spark and has
the dat535 label, along with the normal Linux and self-hosted labels.

The server already had Python 3.11. We installed runner version 2.338.1 after
checking the download's SHA-256. The new service runs as ubuntu and is enabled
to start when the server boots. GitHub now shows it online.

We created /srv/dat535/data for persistent project storage and set the GitHub
repository variable DAT535_DATA_ROOT to that path. All 25 ingestion tests
passed on the server. We also checked Entur's metadata for 23 October and saved
the result in bronze/entur/setup-check-20261024 under that storage directory.
That was a small metadata check, so full archive extraction on the server is
still to test.

The runner is ready, but the ingestion workflow is still on our feature branch.
It needs to reach main before we can do the first run through GitHub Actions.
The server had about 26 GB free before installing the runner. Since we retain
the archives and extracted data, we will need to watch storage as snapshots
accumulate. We have not copied the historical dataset to the server yet.


### 24 October 2026 - Testing a real manual ingestion

We ran the full ingestion command manually on spark for the 23 October archive.
This downloaded the national ZIP, selected KOL, AKT and their shared documents,
kept the full national stop file, and checked the XML and provider references.
It took about 43 seconds and produced 532 XML files. The original national ZIP,
loose XML, compact selected ZIP and metadata all stayed on the server under
/srv/dat535/data/bronze/entur/manual-full-20261023-20261024.

There were no unresolved provider references or same-file version failures.
Agder had 65 repeated ID/version occurrences, which we preserved and reported
as warnings. The run therefore finished as complete_with_warnings.

We also checked the saved XML hashes and ZIP contents against the source, then
tried running the command into the same output directory again. It refused to
overwrite it, and the stored file inventory stayed unchanged. The small test
report is saved in the run metadata and copied into our local audit notes.

This tests the manual command on Linux. It does not yet give us evidence of
the Run workflow button working, since the workflow still needs to reach main.
The separate existing-ZIP upload requirement also remains to implement and
test for both KOL and AKT.

### 8 November 2026 - Clearing national archives and fixing scheduled retention

The server was down to 219 MiB free. The user chose to remove fetched national
ZIPs without a Google Drive backup if the exact originals could currently be
downloaded again. We checked each saved Entur URL and generation with a HEAD
request, matching its size and MD5. We also checked the local national SHA-256,
the selected ZIP checksum/CRCs and all extracted XML hashes against lineage.
Only completed, validated ingestions were eligible.

We removed 21 national ZIP paths, including the nine monthly source archives.
Some repeated runs already shared hardlinks, so those paths did not each free
a full archive's worth of space. Free storage rose from 228,794,368 bytes to
4,317,605,888 bytes. XML, selected ZIPs, metadata and processed data remain.
Manifests now record the removals and their cleanup audit. Failed/incomplete
downloads were not included in this cleanup, and no Drive upload is claimed.

Scheduled ingestion now passes --remove-national-after-validation and checks
for 2 GiB free before starting. The CLI keeps its explicit opt-in cleanup flag;
failed validation or download checks leave the fetched source intact. All 28
ingestion tests passed under Python 3.11, including successful cleanup, failed
validation/source preservation and low-space rejection. Workflow YAML and shell
syntax checks also passed. Manual access to a remote source today is not a
promise that Entur will retain that source forever.


26–27 October 2026 - The Actions job works, but the timer is not confirmed

We merged the ingestion setup into main and ran the complete workflow through
the Run workflow button. It passed on dat535-spark, including all 25 tests,
the real download, extraction, reference checks and report upload. That run
is 40388358483. Its manifest says workflow_dispatch, so we cannot use it as
proof that the weekly timer works.

We tried temporary cron checks and simpler workflows on both our school
runner and a GitHub-hosted runner. The simple workflows also passed manually.
By the afternoon of 27 October, the run history still contained no schedule
events. We did not establish why. The troubleshooting went on too long and
repeated probes stopped giving us new information.

We removed the temporary workflows and kept only the Monday 08:23 UTC timer.
Local copies of the probes and a short verification record are in the ignored
audit notes. The automatic ingestion requirement is still open. The raw data
was not changed or deleted during these checks.


### 27 October 2026 - Changing collection to hourly

We changed the GitHub Actions cron from Monday mornings to every hour at
minute 23, using 23 * * * *. The source selection is still yesterday in Oslo.
This changes how often we try ingestion, not how often Entur publishes a new
daily archive. Without cross-run duplicate detection, repeated runs can save
the same archive again. We still need a real schedule event to confirm that
the automatic trigger works.



We then adjusted the hourly trigger to minute 45, using 45 * * * *.

The :45 trigger also produced no scheduled run. We changed it to the top
of every hour, 0 * * * *. The missing scheduler events are still unresolved.

### 27 October 2026 - Checking whether an environment was missing

We checked the live workflow and repository settings. Actions was enabled,
the workflow was active on main, and dat535-spark was online and idle. The
workflow has no environment requirement. GitHub still reported zero schedule
runs. We disabled and re-enabled this workflow once as a diagnostic reset and
confirmed it was active again at 16:04 UTC. The hourly :00 schedule stays the
same. This reset is not proof that scheduling works.


### 27 October 2026 - An automatic push run finally gives us clearer evidence

We added a push trigger restricted to main. Publishing the workflow change
started run 40537981353 automatically, without pressing Run workflow. GitHub
records its event as push. The job ran on dat535-spark for about 52 seconds and
passed the tests, download, extraction, validation and report upload.

We downloaded the report and checked its manifest too. It records push and the
correct runner, with 532 XML files from the 26 October archive. Ingestion was
complete_with_warnings because the source contains 65 repeated AKT ID/version
occurrences. Those records were kept unchanged.

This confirms the automatic push path works. The missing cron runs remain a
separate unresolved trigger problem. We kept the hourly :00 cron expression
and documented the push trigger. No existing raw data was deleted.

We changed the hourly cron to minute 17, using 17 * * * *. The push-to-main
trigger stays enabled. This timing change still needs a real schedule run
to verify the cron trigger.


### 28 October 2026 - We finally have real scheduled runs

We left a five-minute cron on main overnight and checked the run history in
the morning. There were three scheduled runs, all successful: 21:51 on the
27th, then 01:05 and 06:28 on the 28th, using Oslo time. They were hours apart,
so this does not show that the requested five-minute interval is reliable.

The jobs started about two seconds after GitHub created each run and finished
in roughly 53 seconds on dat535-spark. We downloaded the latest report as well.
Its manifest says schedule, with 532 XML files from the 27 October archive.
Provider reference checks passed, and the 65 repeated AKT ID/version records
were kept as source warnings. The tests, ingestion and report upload all passed.

We can now demonstrate automatic scheduled ingestion for the project. We still
do not know why schedule delivery was so sparse. GitHub documents possible
delays and dropped runs, but that alone does not establish our particular cause.
The five-minute setting on main was not changed during this check. No raw data
was deleted. The verification summary is saved in the ignored audit notes.


### 28 October 2026 - Adding the missing manual ZIP intake

We moved on from the timer investigation and built a proper manual import
command for both KOL and AKT. The earlier tools could download and filter data,
but the future upload screen still needed a backend for an existing ZIP.

The new ingest_local_zip module accepts either a national archive or one of our
selected KOL + AKT ZIPs. We require the claimed source date rather than guessing
it from a filename. It keeps an exact uploaded copy and reuses our extraction,
XML and reference checks. It does not clean or rewrite the original XML.

We added a small SQLite catalog for successful manual imports. The same ZIP
bytes, date and providers now count as a repeat upload, even if the file has
been renamed. A repeat gets a metadata record pointing at the validated first
import rather than another XML extraction. Concurrent calls are serialized,
failed imports are not registered as successes, and damaged previous outputs
are not silently reused. This catalog is just ingestion bookkeeping; it is not
our future transport analytics database.

All 40 tests pass on local Python 3.11 and 3.14. We also imported the real January
ZIP: 541 XML files and 1,200,320 provider reference occurrences, with no missing
provider references or same-file version failures. Repeating that upload gave
duplicate and created only metadata. The original January ZIP hash and the
first import's manifest stayed unchanged. The local verification record is in
audit/2026-10-28/manual_zip_ingestion_verification.json.

The code and usage notes are local work for now, not a deployment to spark.
Manual duplicate checks do not yet stop repeated API downloads or recognize
repacked ZIPs that contain the same XML. The GUI, entity parser and Silver
transformations are still ahead. We did not change the live scheduler or delete
any raw data while building this piece.


### 28 October 2026 - Our first real Spark XML-to-Parquet processing test

We checked the school environment and it has Spark 4.2.0. XML reading is built
in there, so we can let Spark read NeTEx directly and write Parquet. We added a
processing requirements file and used a separate local virtual environment with
the same Spark version and Java 17 for testing.

We read CompositeFrame first instead of jumping directly to every child tag.
That gives us parent frame IDs and versions before expanding lines, service
journeys and passing times into separate datasets. We kept occurrence indices
and the original IDs, references, versions, times and day offsets. Nothing gets
deduplicated during this step. The raw XML remains in Bronze.

The small KOL + AKT test passed. The full January test then caught a filename
URI-encoding issue in our source lookup. We fixed that and checked paths with
spaces, plus signs, commas and Unicode too. The failed attempt was kept.

The successful full run processed 537 provider line XML files and wrote 537
frames, 537 lines, 25,289 service journeys and 768,701 passing times. Spark read
each Parquet dataset back, and the counts matched the Bronze reports. The XML
hashes before and after were unchanged. Conversion took about 28 seconds after
setup. All 46 tests pass with the actual Spark reader and writer.

This is our first staging projection, not Silver. The four shared documents,
national stop definitions, structured routes, journey patterns and calendars
still need their own projections. Some other child content is kept as Spark
wildcard fragments, but those are not exact original XML containers; the source
files remain the authority. The successful test output is under
data/staging/entur/january-2026-provider-files-verified, with details in
docs/xml_to_parquet.md. The code is still local work and has not been deployed
to spark or added to the ingestion workflow.


### 28 October 2026 - Preparing the tested work for the server

This commit brings together the manual ZIP intake, Spark XML-to-Parquet staging,
their tests and the updated usage notes. All 46 tests passed, and the January
provider conversion was checked locally. We kept main's current five-minute
cron setting when bringing the development branch up to date. The next server
test needs the new code pulled into the cloned dat535-project first.

### 28 October 2026 - Slowing down the ingestion schedule

We changed main from the five-minute test to 08:23 UTC on days 1, 4, 7 and so on
each month. That is how the cron expresses every third day, but it resets when
the month changes rather than counting exactly 72 hours. Push to main and
manual runs still work. Each run asks for yesterday's archive; it does not go
back and download the days between runs.

We also talked about checkpoints. The existing manifests tell us which imports
and Parquet outputs completed, and manual uploads have their own duplicate
catalog. We have not yet built a shared pipeline checkpoint registry. That
should be part of the next processing milestone, so a failure in Silver does
not require downloading Bronze again. The next useful target is one January
snapshot working on the school server, then the missing reference tables and
the first validated Silver joins before scaling to all nine months.

### 28 October 2026 - First complete processing slice

We read the whole task breakdown again. The main target is still substantial
Silver processing in PySpark, with historical route changes and a GUI later.
Today we added the shared definitions needed to connect journeys to their
patterns, routes, scheduled stops, quays and calendars. January's local full
staging run matched every expected count, including 25,289 journeys, 768,701
passing times, 3,148 routes and 3,246 journey patterns.

Spark quietly ignored the source filenames beginning with an underscore. The
count checks caught this before we could call the extraction complete. We now
read those files through symbolic aliases and keep the original source paths
and hashes. Another useful catch was HentMeg: its routes use FlexibleLineRef,
and its journeys describe operating windows. We preserved those fields and
separated flexible service from the fixed-departure metrics.

The processing registry now lives outside the checkout. It records attempts,
checks completed output hashes before reuse, and resumes at stage boundaries.
We tested failed attempts, changed files, conflicting locks and reruns. Failed
outputs remain available. Calendar tests cover weekday patterns, explicit date
exceptions and overnight offsets. The default metrics use a stated seven-day
window from each snapshot, not a claim that we observed every day of the month.

The school server had Java 8 active. We installed a checksum-verified Java 17
runtime under /home/ubuntu/dat535-runtime, keeping the system Java unchanged.
All 57 tests passed on the server. Its cloned repository had older history, so
we preserved that branch and checked out server/processing-20261028 for this
work. The historical batch was then started against the existing Bronze data,
with its log in /srv/dat535/data/metadata/processing/historical-20261028.log.
Batch completion is recorded separately after checking the actual manifests.

Disk space is tight, around 1.7 GiB before the historical run. We verified seven
duplicate national ZIP copies which could share storage through hardlinks,
but have not changed them because raw-storage approval is still pending. We
also reduced the new Silver output by dropping repeated join-helper fields
while preserving source lineage, reference versions and the row grain.

This is still a first pipeline slice. Route-history/SCD tables, renumbering
verification, GUI/database publication, automatic processing after ingestion,
and broader performance evidence remain on the task list.

### 28 October 2026 - January verified on the school server

The real January run completed staging, Silver and Gold on spark. Silver kept
all 25,289 journeys and 768,701 passing times. It reported zero quarantined
journeys, zero quarantined passing times, zero conflicting stop definitions
and zero calendar issues. The HentMeg records now resolve as flexible services
instead of being mistaken for missing line references.

For 15-21 January, Silver produced 53,345 service occurrences. Gold's 53,322
fixed-service departures reconciled exactly; 23 flexible-service occurrences
were kept in the excluded-service table. This is planned service from the
January snapshot. It is not a count of buses that actually ran.

The January checkpoint paths are:
staging/entur/2026-01-15/c1565aaadd3eb72c/staging-334aacd1c05b
silver/entur/2026-01-15/c1565aaadd3eb72c/silver-9e48ff98b18a
gold/entur/2026-01-15/c1565aaadd3eb72c/gold-997f99eb64a7
These are relative to /srv/dat535/data. The compact Silver outputs total about
56.6 MB of Parquet, and Gold about 0.37 MB. February was processing at the last
check; the remaining months are not yet claimed complete. The disk had about
920 MiB free during that run. We requested more access time before the original
90-minute window ended, and still have not consolidated raw archive copies.

### 28 October 2026 - Approved archive storage consolidation

The user approved the seven duplicate ZIP hardlinks and extended access by
30 minutes. We checked all source and target hashes again, replaced each
duplicate copy atomically, and checked the hashes and shared inodes afterwards.
All original paths and bytes remain available. The seven copies now share
their two matching original archive files, recovering 1,957,911,441 bytes of
physical storage. These archive paths must continue to be treated as immutable.
The server audit is metadata/storage-consolidation-20261028.json under the data
root. Free space was around 3 GiB afterwards, so the historical batch could
continue. January through April had completed at the next check, and May was
being staged. The main workflow still runs ingestion only; the development
workflow now includes processing and a saved checkpoint report for review.

### 28 October 2026 - Nine months complete, and the checkpoint rerun worked

All nine historical snapshots finished staging, Silver and Gold on the school
server. Across them we preserved 263,757 journey occurrences and 8,296,954
passing-time occurrences. Every month had zero quarantined journeys, zero
quarantined passing times and zero calendar issues under our current checks.
The Gold counts reconciled in every declared seven-day window. We still need
broader profile validation, historical route modeling and interpretation of
the changes; these successful checks do not settle those tasks.

We ran the whole historical command again. It reused all 27 completed stages,
and the registry stayed at 27 attempts. That gives us a real server check of
the checkpoint design, beyond the small tests. There was no new download or
replacement of the completed outputs. The exact monthly counts and limits
are recorded in docs/processing_pipeline.md.

Free space after the batch is around 1.8 GiB. The proposed Actions chain now
checks for 2 GiB before full ingestion and processing, rather than filling the
disk. We will need more persistent storage for continued new snapshots. The
new Actions processing step has passed workflow validation locally; a live
Actions run and publication to main remain to verify.

### 2 November 2026 - Keeping the original archives in Google Drive

The school disk filled up after more scheduled ingestions. We want to keep the
original national ZIPs and use Google Drive for the cloud archive. The upload
has not been done or verified yet. We should record the snapshot date, source
generation, original filename, size, checksum and Drive file ID once each
upload is checked. Different monthly snapshots have different national ZIPs;
one January ZIP would not back up the other months. The first upload's scope
is still being clarified. Existing local raw files stay intact for now.

### 8 November 2026 - User chose source re-downloads instead of a Drive backup

The user decided we could delete the fetched national ZIPs if their exact
originals were currently available to download again. For each completed run,
we checked the pinned Entur URL, generation, size and MD5, then verified the
local national SHA-256, extracted XML hashes and selected ZIP checksum/CRCs.
We removed 21 national ZIP paths. Metadata and its source identities remain,
with manifest backups and an audit under
/srv/dat535/data/metadata/cleanup-national-20261108T141654/.

Space rose from 219 MiB to about 4 GiB. A tested fix was published directly to
main as 0e9fd84: scheduled ingestion now removes the national ZIP only after
successful extraction and validation, and full ingestion requires 2 GiB free.
All 28 ingestion tests passed with Python 3.11. The resulting automatic push
run, 42489361135, ingested the 7 November snapshot and recorded
national_removed=true; its national ZIP was confirmed absent. After retaining
that run's selected data, around 3.2 GiB remained free. All 27 historical
processing stages remain complete, and the server's dirty route-history files
were not changed. Failed/interrupted source files were left for inspection.
No Google Drive upload was performed or claimed.

### 8 November 2026 - Duplicate runs and development caches cleared

The user approved the cleanup list. We verified the two canonical October
snapshots and each duplicate's XML hashes, selected ZIP and source identity.
Seven duplicate datasets were cleared, keeping their metadata and pointing it
to the canonical manifest. The three interrupted runs had downloadable pinned
sources; their incomplete files were removed and their failure records kept.
None of these runs was an input to the completed historical processing jobs.

We also cleared the pip download cache, three old VS Code server versions,
unused runner versions and the runner update cache. A process still had an old
runner Node runtime loaded, so the cleanup stopped before deleting it. Both
runners were confirmed idle, then restarted to use their installed current
versions. The remaining unused files were removed after checking process
references again. Both services are active and GitHub reports them online.

Free space rose from 3,419,664,384 to 15,986,823,168 bytes, recovering about
11.7 GiB. The disk now has roughly 15 GiB free. All nine historical snapshots,
all 27 complete processing stages, Spark 4.2.0 and the active VS Code version
remain. The server's uncommitted route-history work was unchanged. The audit
and original run manifests are under
/srv/dat535/data/metadata/server-cleanup-20261108T143646/.

### 9 November 2026 - Route history built and checked against a real change

We added route-change detection across the nine snapshots (EPIC 06, T38-T43,
T46-T47) in pipeline/analysis/route_history.py, with an inspect tool that
prints results with stop names. It compares each month with the next and
records added, retired and renumbered lines, stops added or removed, changed
stop sequences, departure changes and moved first/last buses. It also keeps
a versioned (SCD Type 2) history of each line, lists possible renumberings,
tracks stops that gain or lose service, and links every Gold service date to
the line version of its snapshot. The run checks itself against Silver and
Gold: on the real data all 466,493 departures reconciled, every line snapshot
belonged to exactly one version, and every service date found a version.

The clearest network-wide changes are the July summer timetables (AKT down
27.6%, Kolumbus down 20.7% in the seven-day window), an AKT-only drop in
February that probably reflects the winter school holiday (not yet checked),
and Kolumbus returning in August about 7.6% above June.

We checked the results against AKT's news page "Route changes in Agder from
22 June and 1 July 2026". Route 56 ends between the June and July snapshots,
and eight stops that only it served lost all service. Route 55 keeps its
number under a new line ID and now also serves Odderhei. Route 178 Åseral-Evje
ends and the new Route 172 plus a Svipp line take over. These three pairs
are recorded as verified in config/route_renumberings.json; line 56 is a
discontinuation and is not listed there.

The real data showed two weaknesses in the first version. AKT and Kolumbus
publish the new line ID one month before the old one disappears, so the
renumbering check missed bus 55; replacements may now have been published up
to one snapshot early. Kolumbus also reused bus numbers in July (old line 1
became 9 while former line A took number 1), which produced same-number pairs
with almost no shared stops; those now need a minimum stop similarity. The
178 to 172 replacement has a shorter route and only 46% shared stops, so
verified mappings are now kept even when the automatic rules miss them.
Ten new tests cover these cases and a full run on a small three-month network.
Details are in docs/route_history.md.

We then checked the remaining open cases against public sources. Kolumbus'
notice "Ruteendringer 1. juli 2026" (Bussveien opening) confirms five
renumberings in the July snapshot: 1 to 9, 4 to 12, old 2 split into new 1
(Sandnes) and new 4 (Risavika), and old 3 to new 2 (Forus). These are now
verified mappings, giving eight in total, and all eight matched on the real
run with every check still passing. Lines A and B had been published since
January with no departures; they were placeholders for the Bussveien lines
and became 1 and 2 under the same line IDs. The February AKT dip is the Agder
winter holiday in week 8: only weekdays 16-20 February drop (about 25%),
while weekends equal January and March. AKT's T-suffixed bus numbers lose
the T on continuing lines with unchanged stops; we found no source for what
the T means, so we only record that they are label changes.

### 9 November 2026 - Review follow-up: observation semantics, evidence and scope

Tobi's readiness review raised four problems in the route history. We changed
the code for each.

Observation, not effect. A snapshot shows what was published on that date,
not when a change took effect. Versions now carry first_observed,
last_observed, previous_snapshot and first_not_observed instead of
valid_from. A line missing from the next export is "disappeared". It is only
called renumbered or discontinued when a verified source covers that exact
transition (line_transitions.interpretation); otherwise it stays
not_observed.

Two stop measures. Route structure still uses the stops in published journey
patterns. Service now uses the stops served by fixed journeys that operate
inside the Gold window, the same scope as the departure totals. The two give
very different answers: in the February holiday week about 1,470 AKT stops
had no departure (only 116 of 212 AKT lines ran), and in July about 1,550 AKT
and 870 Kolumbus stops. The pattern measure had shown 1 and 70. The stops
that drop out are mostly on AKT's school routes (19xx/23xx).

Strict evidence. The mapping reader treated a missing or string "false"
verified field as verified. Evidence entries are now validated: verified must
be a JSON boolean (false when missing), a verified entry needs verified_by,
source_url, note and to_snapshot, from_snapshot is enforced when given, and
unknown fields, foreign IDs, bad dates and duplicates are refused. A verified
entry that does not match the data now fails the run. We also fixed a bug
where a mapping for the wrong transition removed the candidate row instead of
leaving it unverified. Discontinuations have their own evidence file.

Modes. The data includes boats: 44,534 of the 521,185 window departures
(about 6% of AKT's and 10% of Kolumbus' departures). Totals are now reported
as all modes, with bus-only columns and a per-mode network table, so they are
no longer called bus departures.

Re-reading the AKT and Kolumbus notices verified more July changes: AKT
54 to 32, the new IDs of Kristiansand lines 19, 30, 31, 32 and 35, the
discontinued N110 night trip (its only departure, 02:45 Sunday), Kolumbus X77
to 43, 46 to SK0203, and the end of route 67 and boat route 704. That gives
16 verified renumberings and 4 verified discontinuations, all matched. 24
disappearances across all transitions remain unexplained and are reported
only as no longer published.

The first scheduled snapshot (8 November) was processed by the main workflow
and is now included: ten snapshots, 521,185 departures, all checks passed.
The workflow change on this branch adds the Spark test suite before
processing and route history after it, and saves the history checks with the
run report. 76 tests pass on the server. The draft project definition
(docs/project_definition.md) records the scope and meanings above and the
decisions the team still has to agree.

### 9 November 2026 - Quality evidence and a publication gate (review point 4)

Zero quarantine said only that nothing broke our rules. We added a quality
profile for every Silver output (pipeline/quality/silver_profile.py): before
and after volumes, duplicates collapsed, essential and optional fields
missing, unresolved and ambiguous references, invalid and overnight times,
calendar issues, and accepted, excluded and rejected service. Gold now
profiles its Silver input first and applies config/quality_thresholds.json:
any rejection warns, and more than 1% rejected, any ambiguous reference, or no
accepted service for a provider stops Gold. Before this, Gold would reconcile
perfectly with whatever survived Silver. The updated pipeline test shows the
gate stopping Gold when half of AKT's journeys are broken.

The profile of all ten snapshots changed one rule before we used it.
ServiceJourney.DepartureTime is empty for almost every journey, because the
Nordic profile puts times on the passing times, so it is optional, not
essential. All ten snapshots pass. Nothing was rejected, collapsed or
unresolved, so Silver's role here is resolution and derivation, not cleaning.
The evidence shows substantial overnight service (up to 52,210 Kolumbus
passing times after midnight in one snapshot), many valid journeys that do not
run in the window, and flexible service that is excluded, not lost. The
table is in docs/quality_report.md.

We also traced four real February journeys from the source XML, read with the
Python standard library only, to Silver and Gold: an ordinary journey, an
overnight journey, a journey whose calendar marks Friday 20 February as not
available, and a flexible journey. Passing times, operating dates and Gold's
treatment matched in every case (docs/quality_traces.md, docs/data_quality.md).


### 10 November 2026 - Provider completeness and independent source checks

We worked on two gaps from the review. The quality gate used to discover its
providers from whatever rows were left, so an empty profile could pass. It now
compares the manifest selection with the providers in Silver and checks the
staging/Silver occurrence counts for each provider. Missing providers cannot be
allowed through by relaxing the zero-service rule. Calendar impact is counted
from journeys before dates are expanded, since broken calendars can otherwise
leave no rows to count.

The traces now follow the stop references in the XML all the way to a quay and
national stop place, or a flexible stop place. They also compare the earliest
and latest flexible times separately. We added a whole-line recount from every
original journey occurrence, including repeated IDs in different XML files.
The useful failure test moves another journey's date in both Silver and Gold:
they still agree with each other, but the XML recount catches the mistake.

The older local January Parquet did not have the current flexible fields. We
left it alone and regenerated January into temporary storage from the raw XML.
The full run passed gate version 2: 25,289 journeys, 768,701 passing times,
53,322 fixed departures and 23 flexible occurrences excluded. All four real
trace cases passed, including physical stops, calendar exceptions and flexible
windows. The temporary processing files were removed afterwards; only the
small evidence reports were kept. This is local verification, not a server
deployment or a new verification of the other nine snapshots.


### 10 November 2026 - History memory failure and releasing work between tables

The workflow got through ingestion and Silver/Gold, then Java ran out of heap
while building a broadcast join for stop coverage changes. This was not another
full-disk incident. We were saving stop_coverage but still deriving its changes
from the original lazy plan, which repeated the stop/name joins.

History now reads that saved table for the comparison. It disables automatic
broadcast joins (including the adaptive setting), uses a temporary disk-only
cache to share each table's count and write, then unpersists it straight away.
Failed writes also release the cache. Published outputs and failure evidence
are kept; this is cleanup of Spark working data, not deletion of our source.
The history manifest and Actions report now include heap settings and timings.

We checked the actual failed run: ingestion 43 seconds, Spark tests 3 minutes
36 seconds, snapshot processing 28 seconds, history 1 minute 47 seconds before
failure. Tests were the largest part of the roughly seven-minute run.

With a 1 GiB Java heap the larger synthetic case passed: 79,200 coverage rows
and 636,933 national stop names over eleven snapshots, with exactly 3,000 stops
gained and 3,000 lost in the service windows. All 104 tests passed across the
Spark and GUI environments (101 in the Spark environment, 3 GUI tests run
separately). We have not yet retried the full school-server dataset with this
fix, so we do not claim a production speedup. Details are in docs/route_history.md.


### 10 November 2026 - Making the GUI easier to use, and adding comparisons

The server retry worked, and we moved on to the GUI. We wanted both a clearer
layout and something more useful to analyse, rather than just changing colours.
The overview now has an export selector and operator/mode filters that change
the cards as well as the chart. Dates have real calendar spacing, so the gap
between September and November does not look like one ordinary monthly step.

We added Compare exports for any earlier/later pair. It shows departure totals,
absolute and percentage changes, a weekday profile and line-by-line changes.
The weekday comparison includes days with no service. A zero earlier total
does not get an invented percentage. Lines are matched by provider and NeTEx
ID; this page does not guess replacements. Those still need verified operator
evidence on Replaced lines. The comparison and daily line profile can be saved
as CSV files.

The line page now separates service, observed versions, changes and stops into
tabs. We also added a refresh button and show how far the history has reached.
Overview can work with one published Gold export and no history yet, and the
GUI no longer needs the Silver files just to find those exports. It still reads
the checkpoint registry without writing to it, and needs no Spark session.

We checked all six pages with synthetic published data, including empty mode
filters and missing history. Twenty GUI data, analysis and interaction tests
passed. The local
layout preview on port 8502 is labelled synthetic; it is not a new result from
the school server. This entry records local GUI changes, not a deployment.


### 10 November 2026 - What the 23 exclusions mean, and looking at individual days

The label about trips being left out was confusing. The verified January run
had 23 valid on-demand occurrences and no invalid ones. A HentMeg availability
window is not one fixed departure, so these records belong in a separate table.
We also checked the old local January output: it has 23 rows but incomplete
type flags. We kept it as it was. The GUI now reads the retained exclusion rows
for the selected export and separates valid on-demand, invalid and unclassified
occurrences. It does not copy the verified report's answer onto an older output.

We added Daily service with a month selector, operator and mode filters, daily
departure and active-line counts, first/last departures and a CSV. Monthly
sampling is the frequency of our exports, not the granularity of every metric.
However, our historical Gold currently only expands the 15th to the 21st.
Those seven days cannot stand for a full month. The new view shows uncovered
dates as blank, covered dates without service as zero, and the covered-day
denominator next to the mean.

Scheduled exports can overlap, so each operating date uses one whole export:
the latest covering plan observed on or before that date. We do not add both
plans together or fill missing lines from older plans. All 26 GUI tests passed,
including overlap, future-export, missing-coverage and exclusion cases. No raw
data or processing window was changed. Full-month coverage remains a separate
processing step, subject to the source calendars and observation dates.


### 10 November 2026 - Correcting the seven-day limit and actually rebuilding months

The seven-day window was our implementation choice. It was not what we needed:
we want every calendar date in each month. Changing the GUI alone was not enough.
We changed the default Silver expansion to the first through the last day of the
month, including February, and added a provider/date spine in Gold.

There was another catch in the actual XML. Most 15 January frames only declare
validity from 12 or 13 January. We could not just fill the earlier days with
zeroes. We downloaded additional early exports and checked their XML, references
and validity. The 1 and 2 February sources have an unresolved quay reference,
NSR:Quay:43289. Those failed copies were kept. The validated 14 January export
does cover February's calendars, so it supplies the early February plan, with
its original observation date shown in the daily results.

Calendar-support sources do not add observations to our original route-history
sample. That keeps the verified June-to-July mappings meaningful. They fill
daily plans where the main export has no valid coverage. One whole export is
selected per date, independently of operator and mode filters; overlapping
plans are never added together. Overview, comparisons and line profiles now
use that same monthly reconstruction, not only the primary export's partial
counts. Unknown dates remain different from valid zero-service dates.

We rebuilt all nine primary plans and nine support plans locally, into the
separate data/monthly_2026_01_09 root. All 18 Gold gates passed. Every one of the
273 January-to-September dates is covered for both providers: 546 provider/date
pairs and 1,918,707 planned fixed departures in the reconstruction. January is
31/31 dates, February 28/28. The seven GUI pages also passed against this real
data, and route history completed with the original nine primary observations.
All 140 tests passed across the GUI and Spark environments, including the 19
Spark cases skipped in the GUI environment. We also rechecked the saved file
inventories for all 54 completed staging, Silver and Gold attempts.
The counts and audit paths are in docs/monthly_coverage.md.

Old outputs and every raw source were preserved. scripts/rebuild_monthly.sh
records progress and can reuse compatible completed checkpoints; it also
refuses to call the rebuild complete if the date coverage audit has gaps.
This was a local rebuild. The forwarded school-server GUI still needs the
new code and its own rebuild; we have not used the expired SSH access window.


### 10 November 2026 - Storage again: the XML copies were much bigger than the ZIPs

The server had 4.9 GiB free on a 38 GiB disk. Bronze used 20 GiB, while the 23
ZIPs together used 1.37 GiB. The large repetition was extracted XML: several
runs for the same November dates each had about 0.8 GiB of it. Removing just
national ZIPs was not enough. Compact ZIP copies were also still being kept.

We added a dry-run storage command that checks completed inputs against their
lineage hashes, then can consolidate identical copies with hardlinks. Every
path and its raw bytes remain, so references and checkpoint identities still
work. It skips failed/running snapshots and corrupted files. An explicit option
can discard compact provider ZIPs after verifying every XML hash and ZIP CRC;
metadata records XML-only retention so resuming does not demand a removed ZIP.
Original manual uploads stay protected.

Scheduled and new monthly-support ingestion now keep XML without creating a
compact ZIP. Successful national downloads are still removed after validation;
failed downloads remain evidence. The scheduled workflow also consolidates
verified duplicate storage. This addresses repeated same-source runs, rather
than assuming each push contains a new dataset.

Both runner services were active: DAT535 and the old Group04 project. The user
chose Group04 for removal. We must stop/uninstall that exact service and keep
its _work until unique data is checked. Both observed VS Code server versions
were also active, so deleting their directories blindly would be wrong.
The cleanup tool is prepared locally; no server files have been deleted by us.


### 10 November 2026 - Switching Bronze to selected ZIPs

We had focused on deleting ZIP copies, but that was not the right long-term
choice for this small server. The full-month support downloads finished and
Bronze grew to 23 GiB. The server had only 3.5 GiB free even after verified
hardlink consolidation and removing the unused Group04 runner binaries and
update package. The raw XML, including national stops, was the main cost.
A larger VM disk was not available, and Drive was not configured.

We decided to keep the selected KOL + AKT XML inside ZIPs and extract one
snapshot at a time. The new migration builds archives from the files we already
have, checks member CRCs and SHA-256 against the original lineage, backs up the
manifest, then removes only the verified expanded copy. Failed sources and
original manual uploads remain. National re-downloads are not needed.

An isolated copy of the real 15 January snapshot contained 541 XML files,
820,984,157 uncompressed bytes. Its rebuilt ZIP was 62,842,321 bytes. Restoring
it preserved all XML hashes, source selection and checkpoint identity; the
original local dataset was not changed. A real Spark fixture also verified
ZIP-backed processing, checkpoint reuse, independent XML-to-Gold tracing and
cleanup after processing. The server format migration has not been run or
claimed here. Its free-space result must be checked after the user runs it.


### 10 November 2026 - Server ZIP migration and the next memory problem

The user ran the migration on Spark. Its report contains 34 migrated snapshots,
12 skipped entries and no problems. Free disk space rose from about 3.5 GiB to
23 GiB, with the root disk at 41 percent used. That confirms compression solved
the immediate disk pressure; those are the actual server figures, not a local
estimate. The raw XML bytes remain inside the verified selected ZIPs.

The full-month rebuild then completed 14 of its 22 jobs. The next input, the
1 April calendar-support export, failed in staging with Java heap exhaustion.
The later gateway errors were consequences of the JVM crash. The launcher had
not specified a heap budget and the batch kept every snapshot in one JVM.
We cannot prove how much was caused by the larger April input versus retained
state across that long run, so the fix addresses both.

The rebuild now sets a 2 GiB driver heap before JVM startup and runs one Spark
worker process at a time, with a fresh process for each snapshot and for history.
The parent saves progress and errors independently of the JVM. Exception text
and shutdown handling no longer prevent failure recording if the gateway dies.
The transformation fingerprint inputs are unchanged, so the completed server
checkpoints can be reused rather than rebuilding those 14 snapshots.

An isolated copy of the real April ZIP passed staging, Silver and Gold with an
actual 2,147,483,648-byte heap. It contained 34,309 journeys and 1,193,513 passing
times; Silver recorded no quarantined journeys or passing times. A second worker
reused all three stages. The parent rebuild also resumed it and passed April's
full-month coverage audit. Original local datasets were not changed. The resumed
22-job school-server run still needs to be completed and checked separately.


### 10 November 2026 - Repository sweep after the GUI restart change

We reviewed the tracked files and actual function callers rather than deleting
files just because there were a lot of them. No ZIPs, XML datasets, Parquet,
databases or logs were tracked. The large local data and audit folders were
already ignored, so we left those original files and environments alone.

Four old GUI helpers were no longer used by the application. We removed them
and kept the useful filter, weekday and quality-status checks against the active
monthly functions and visible GUI instead. Storage maintenance moved from
pipeline/bronze_storage.py to pipeline/storage/maintenance.py. Its workflow call,
imports and current instructions moved too. We grouped GUI, quality and storage
tests into matching packages; their package markers are needed for unittest
collection. The unused top-level quarantine placeholder was removed because
quarantine records are actually written beside each Silver run.

A storage review also found that choosing a new duplicate as canonical could
miss an older group of existing hardlinks. The helper now prefers the already
shared inode. Its regression test checks that the new copy joins that group,
keeps the original inode and bytes, and is idempotent on another pass.

The root README and directory map still described the GUI and Silver/Gold as
future work. They now describe the implemented pipeline, actual ZIP retention,
current commands and the successful January-November server coverage check.
Older seven-day reports remain dated evidence. Links to ignored local audits
are labelled as local paths instead of broken GitHub links. The task plan is
still a backlog, not a claim that every feature is finished. Serving-database
publication, a GUI upload interface and full XSD/profile validation remain open.

The sweep preserved all transformation and route-history fingerprint inputs:
we did not reformat those files just for style and force a replay of the completed
server data. There is a redundant date import in Silver to remove during a future
substantive edit there. Comments added in this sweep explain source-export date
preservation, partial test windows and the hardlink choice, rather than narrating
obvious assignments. The GUI environment passed 171 discovered tests with 21
Spark skips; the Spark environment passed the same suite with 28 GUI skips.
Together they executed every one of the 171 cases. No raw data was deleted.


### 10 November 2026 - Showing an unfinished month honestly

The user spotted November dates through the 30th in the GUI even though today
was the 10th. The raw timetable can legitimately contain those future plans,
but a full-month display was not the default the user wanted. We agreed to show
service dates through today by default, with an explicit option for the full
published plan. The current date comes from Europe/Oslo, not the server's UTC
calendar. The whole current operating day's plan is included; these still are
not actual completed vehicle journeys.

Overview, Daily service and line service totals now apply that date limit.
Past months stay whole. The future-plans checkbox restores the whole window;
future daily entries are labelled and coloured amber. Unknown dates remain
unknown, not fake zero service. An unfinished month comparison uses matching
day ranges in both months, capped to the shorter month's length. Monthly and
comparison CSVs record their period endpoints so a partial-month export is not
mistaken for a full-month total later.

Gold still retains and validates the full published month. No source files,
Gold tables or checkpoints were changed or rebuilt for this display correction.
History and Data checks retain their published-export scope, explained in the
sidebar and page text. Tests fixed today at 10 November: the daily table stopped
at the 10th by default, the full option showed all 30 dates with 20 future labels,
and the October/November comparison used matching first-ten-day totals. The
existing real Gold dataset also passed all seven GUI pages after the change.

### 10 November 2026 - Code clean-up: shared helpers and superseded entry points

A review for unused code found two entry points that only the original
experiments still used. The Kolumbus-only command line in
download_entur_netex.py (probe, KOL extraction and its CSV manifest) had been
replaced by generate_provider_dataset, the poller and manual ingestion; the
module now keeps only the shared metadata, download and checksum helpers, and
its tests call download() directly. The original four-table xml_to_parquet job
had been replaced by the complete staging step. Its two remaining helpers moved
into snapshot_to_parquet.py, the unused sampling option left staging_sources,
and its Spark tests now exercise convert_snapshot on the same punctuated and
Unicode paths. docs/xml_to_parquet.md was removed; the 28 October entries above
still record that run.

Four copies of utc_now, two JSON writers and three SHA-256 helpers became
pipeline/common.py, using the fsync-backed atomic writer everywhere. Seven
copies of the Spark session builder became spark_runtime.start_spark, and the
seven Spark scripts now source scripts/spark_env.sh instead of repeating the
Java 17 setup. The redundant Silver date import and an unused staging read were
removed, a test-only branch left renumbering_candidates, and a personal macOS
JAVA_HOME was removed from two tests. rebuild_monthly now closes its read-only
registry connection; sqlite3's context manager only commits.

This sweep did change files that feed the staging, Silver, Gold and
route-history fingerprints, unlike the previous one. No output values change.
New snapshots are unaffected, and route history is rebuilt for every new
snapshot anyway, but re-running an already processed month now recomputes it
into a new directory instead of reusing it. Check free space before a full
monthly rebuild. The suite has 169 tests (seven legacy download-CLI tests were
replaced by five direct ones) and all passed with Spark and the GUI libraries.

The repository layout was also simplified (106 files in 28 folders became 81
in 19). Tests now sit in one tests/ folder. The eight Spark shell wrappers became
scripts/run.sh <command> (processing, rebuild, history, inspect, quality-report,
quality-trace, tests); run_gui.sh is unchanged and the workflow uses the new
commands. Fifteen documentation pages became six topic pages (project,
ingestion, processing, quality, route_history, gui) plus docs/evidence/ for the
dated and generated reports. The directory map and the config/, scripts/, data/
and app/ READMEs were folded into the root README, and the empty data/
placeholders were removed; data/ remains ignored for local runs. Earlier
journal entries keep the file names that existed at the time.
