# Weekly ingestion on the school Linux server

The historical January–September 2026 dataset is our starting data. The workflow
in [ingest-netex.yml](../.github/workflows/ingest-netex.yml) collects new data
**every Monday at 08:23 UTC**, using Sunday's dated Entur archive. In Norway the
scheduled time is 09:23 in winter and 10:23 in summer. Using the previous day
avoids depending on the completion time of that morning's export.

GitHub handles the timer and sends the job to a **self-hosted Linux runner on
the school server**. The server executes the Python command and keeps the data
on its persistent disk. The workflow targets runner labels
`[self-hosted, linux, dat535]`. See [GitHub's runner label documentation](https://docs.github.com/en/actions/how-tos/manage-runners/self-hosted-runners/use-in-a-workflow).

This polls Entur's public archive metadata/download API for planned timetables.
It does not collect real-time vehicle positions or delays. The workflow runs
only ingestion; Silver, Gold, database publication and comparisons still need
implementation.

## Current installation — 24 October 2026

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

The ingestion code is pushed on `ft/initial-setup-plus-initial-data-finding`.
It still needs merging into `main` before the first GitHub Actions run. Full
manual command execution on the server has passed, as described below.

## Full manual CLI test — 24 October 2026

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
[`audit/2026-10-24/manual_ingestion_test.json`](audit/2026-10-24/manual_ingestion_test.json).
This evidence covers the manual CLI. A first GitHub Actions dispatch remains
pending the workflow reaching `main`; importing an existing ZIP through a new
KOL + AKT upload interface has not been tested here.

To check the runner from a shell on `spark`:

```sh
sudo systemctl status actions.runner.TOGUNOLUWATOBI-dat535-project.dat535-spark.service
sudo journalctl -u actions.runner.TOGUNOLUWATOBI-dat535-project.dat535-spark.service -n 50 --no-pager
```

## Set up the school server

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

## Enable the weekly workflow

1. Merge the ingestion workflow, Python module and tests from
   `ft/initial-setup-plus-initial-data-finding` into the default branch, `main`.
2. In **Actions**, open **Ingest Entur NeTEx**, then **Run workflow**. Leave the
   date blank to use yesterday in Oslo. Choose `check_only` for a small metadata
   check first, then run normally to collect and validate an actual snapshot.
3. Check the run's logs, server output directory and downloadable report.
   Subsequent weekly runs use the same command automatically. No Entur API key
   is needed. Runner registration and the storage variable must be configured
   before the first run.

GitHub schedules require the workflow on the default branch. They may start
late, and public repositories can have inactive schedules disabled after 60 days
without repository activity. See [GitHub's schedule documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).

## What each run does

1. Run the ingestion tests before accessing the source.
2. Request metadata for one date, recording its generation, source creation time,
   expected size and MD5. Download that exact object generation.
3. Verify source size, MD5, SHA-256 and the national ZIP's member CRCs.
4. Select `KOL_*`, `AKT_*`, their provider shared XML, and the full `_stops.xml`.
   Store original XML and a compact ZIP without rewriting XML.
5. Reuse our XML parsing, publication-date and provider reference checks. Record
   source repeats as warnings; missing provider references fail the run.
6. Keep national downloads, selected XML/ZIP and metadata on the school server,
   including failed or interrupted runs. Upload a copy of the report to GitHub.
   Failed validation marks the server manifest as failed and fails the job;
   downstream processing must accept only `complete` or `complete_with_warnings`
   manifests.

The checks are not full NeTEx XSD, profile or calendar validation. Registry-only
reference diagnostics are reported separately from provider reference failures.

Network timeouts, incomplete transfers, HTTP 408/429 and server errors get up to
three attempts with backoff. `Retry-After` is respected up to 60 seconds. A 404
fails clearly; we do not silently choose a different date. Each download attempt
uses its own path, keeping interrupted bytes. Checksum or validation errors are
fatal and retain the evidence. GitHub concurrency prevents overlapping runs.

## Persistent storage

With `DAT535_DATA_ROOT=/srv/dat535/data`, each weekly execution writes:

```text
/srv/dat535/data/
└── bronze/entur/run-RUN_ID-ATTEMPT/
    ├── metadata/       Manifest, source metadata, lineage and validation report
    ├── national/       Original national ZIP and any interrupted attempts
    ├── xml/DATE/       Unmodified KOL + AKT XML and national stops
    └── zip/            Compact copy of the selected XML
```

A metadata-only check creates just `metadata/`; it does not claim ZIP or XML
validation. These server directories are retained across runs and reboots,
subject to the school's disk and mount configuration. The workflow does not
delete data or apply a retention period to server files. Storage capacity,
backups and any later retention policy still need to be arranged with the school.

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
recognize identical national payloads for the same provider selection. A weekly
schedule captures one observation each week; it does not backfill every day or
identify the exact date of a change between observations.

## Run the same command locally

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
passed on `spark`, with the warnings recorded above. A complete run through
GitHub Actions still needs to be verified after merging the workflow into `main`.
