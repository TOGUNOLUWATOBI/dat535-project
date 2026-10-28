# Checkpointed Bronze → staging → Silver → Gold

The first complete processing slice uses Spark 4.2's native XML reader, explicit
schemas and Snappy Parquet. Bronze archives and XML remain authoritative. This
does not finish the whole task backlog: route-history modeling, verified route
renumberings, the GUI, shared ingestion deduplication and performance comparisons
remain separate work.

## Run on the school server

The helper uses `~/spark-env/bin/python` and the project-specific Java 17 runtime
recorded in `~/dat535-runtime/java17.json`. It does not change the server's
default Java 8. `DAT535_SPARK_PYTHON` and `DAT535_JAVA_METADATA` can override those
locations. Run inside the cloned project:

```sh
bash scripts/run_processing.sh \
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

## What a checkpoint means

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
is not the analytical database. The GitHub workflow currently runs ingestion
only; automatic processing still needs to be connected after server validation.

## Tables and keys

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

## Calendars, time and flexible services

The default analysis window is the snapshot date through six days later.
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

## Gold and verification

Gold contains daily departures per line, a service-window summary, planned stop
coverage for eligible journeys and excluded service-date records. Summary
averages divide by all calendar days in the declared window, including days
without service. Daily departure totals must equal eligible fixed-service
occurrences in Silver. Metrics describe planned timetables, not observed
operations, ridership or delays. Historical change/SCD tables remain to build.

Tests cover source damage, underscore-prefixed reference files, repeated IDs in
different files, reference versions, leading-zero public codes, calendar
exceptions, overnight times, flexible windows, invalid-time quarantine, Gold
reconciliation, safe reruns, failed attempts, lock conflicts and altered-output
recovery. See the project journal for dated real-dataset/server results.
