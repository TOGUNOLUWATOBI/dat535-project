# Project definition (draft for team agreement)

This page fixes what the project claims and what its words mean. The report,
Gold tables, route history and demo should all use these definitions. Items
marked **Decision** need agreement from both team members.

## Purpose

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
use the route-history CLI (`scripts/inspect_route_history.sh`) and an
analytical report or notebook with fixed queries, charts and verified case
studies. A Streamlit GUI is optional and comes after these.

## Scope

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

## What the dates mean

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

## What the counts mean

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

## Claims we make and do not make

We claim:

* The outputs reproduce exactly what each export published for the window,
  with reconciled counts from Silver to Gold to route history.
* Verified route changes match quoted operator notices.

We do not claim:

* That buses ran as planned (no real-time data).
* Effective dates of changes, except where an operator source states them.
* Actual daily observations between snapshots. Monthly plans are reconstructed from dated exports, with source provenance for each service date.
* Multi-node scalability. Processing runs on one server (`local[2]`).

## Why Spark

Each export is nested XML. The nine historical snapshots hold 263,757 journey
occurrences and 8,296,954 passing-time occurrences. Silver needs large joins
(passing times to patterns, stops and calendars) and window operations that
are repeated for every snapshot, and the whole history is reprocessed when
rules change. Spark gives the schema-aware XML parsing, distributed joins and
checkpointed reruns this needs. Pandas on one machine would need hand-written
chunking.

## Open decisions

1. Consumer and main deliverable (proposed above).
2. Multimodal versus bus-only scope (proposed: multimodal, bus reported separately).
3. Quality thresholds that stop or warn before Gold is published. Proposed and
   implemented: any rejection warns, more than 1% (or any ambiguous reference)
   stops. See [data quality](data_quality.md) and `config/quality_thresholds.json`.
