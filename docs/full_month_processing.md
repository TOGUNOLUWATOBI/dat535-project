# Full-month daily service

The school-server rebuild completed on **10 November 2026**: all **22 jobs**
finished, published-plan coverage passed for **January–November 2026**, and route
history was generated. The [verified coverage](monthly_coverage.md) page records
that completion separately from the detailed January–September local totals.
Use the commands below to rerun the batch, resume interrupted work or extend
coverage when adding months.

The processing default is now the first through the last day of the snapshot's
calendar month, including February and leap years. The original seven-day
outputs remain historical evidence; the current GUI uses the rebuilt full-month
outputs. Source XML bytes and completed attempts are preserved. With
ZIP retention, verified XML is stored compressed and extracted temporarily.

## Sources for early dates

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

## Rerunning or extending the Spark-server rebuild

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

bash scripts/rebuild_monthly.sh \
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
Spark temporary files and new outputs. See [storage maintenance](storage_maintenance.md).

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

## Meaning and outputs

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
dated reconstruction. The GUI defaults to service dates through today in Oslo;
an explicit future-plans option shows the full published month. The underlying
Gold windows and coverage audit always retain the full calendar month.
An unfinished month is compared with matching dates in the earlier month.
Totals include the selected covered days; means include covered zero-service
days. Month lengths and weekday mixes differ, so
comparisons show coverage, daily means and weekday profiles. Route-history
changes and stop details remain measurements of each primary export, not the
stitched monthly network. Historical seven-day reports are labelled as such.
