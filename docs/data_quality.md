# Data quality evidence and the Gold publication gate

Zero quarantined rows only says that nothing broke the current rules. This
page describes what we measure about each Silver output, when Gold is allowed
to publish, and how we checked real journeys independently. The numbers are in
two generated files:

* [`quality_report.md`](quality_report.md): the before/after table for every
  snapshot and provider;
* [`quality_traces.md`](quality_traces.md): four journeys traced from the source
  XML through Silver to Gold.

## What is measured

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
| Service | Service occurrences in the window: accepted for Gold, excluded as flexible, rejected as invalid; valid fixed journeys that do not operate in the window |

`ServiceJourney.DepartureTime` is optional: in the Nordic profile the times are
on the passing times, and nearly every journey leaves it empty. Our first draft
counted it as essential, and the real data showed that was wrong.

## Publication gate

Before writing any Gold table, `build_gold` profiles its Silver input and
applies the rules in `config/quality_thresholds.json` to each provider:

| Rule | Warn when | Stop when |
| --- | --- | --- |
| Journeys quarantined | any | > 1% |
| Passing times quarantined | any | > 1% |
| Service occurrences rejected as invalid | any | > 1% |
| Essential fields missing | any | > 1% of journeys, passing times and lines |
| Ambiguous references | – | any |
| Unresolved references | any | – |
| Conflicting definitions withheld | any | – |
| Calendar issues | any | – |
| Accepted service occurrences | – | none for a provider |

* **pass**: Gold publishes.
* **warn**: Gold publishes, and the findings are in the Gold manifest
  (`quality_gate`).
* **fail**: Gold is not published. The stage fails with the rules that fired,
  the attempt is recorded as failed in the checkpoint registry, and the
  workflow's processing step fails visibly.

The full profile is saved with every Gold output (`metadata/quality_profile.json`).
The gate code and the thresholds are part of Gold's checkpoint identity, so
changing a rule rebuilds Gold (Silver is reused). Without a gate, Gold would
reconcile perfectly with whatever survived Silver, even after large exclusions.
The test `test_invalid_times_are_quarantined_and_the_quality_gate_stops_gold`
shows this: with half of AKT's journeys broken, the default rules stop Gold, and
publishing needs a separate, recorded threshold file.

The limits were set after profiling all ten 2026 snapshots. All of them have zero
rejections, so any rejection should be noticed (warn), and more than 1% means
something structural changed in the source or the code (stop).

## Results on the ten 2026 snapshots

All ten snapshots pass the gate for both providers. Most of the evidence
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
  operate in the window (see [route history](route_history.md)).
* **Flexible service is excluded, not lost.** Kolumbus has 13–23 on-demand
  occurrences a week. They are kept in Silver and listed in Gold's
  `excluded_service_dates`, and they are not counted as departures.
* **Optional gaps.** AKT's scheduled stop points have no names at all (stop
  names come from the stop places), and Kolumbus has 20–24 unnamed stop points
  and 2 routes without a direction. None of these affect departures.

## Traced journeys

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

All four are consistent. The script exits with an error on any mismatch.

## Running it

```bash
bash scripts/run_quality_report.sh --data-root /srv/dat535/data --markdown docs/quality_report.md
bash scripts/run_quality_trace.sh --data-root /srv/dat535/data --snapshot 2026-02-15 --markdown docs/quality_traces.md
```

The report exits with an error if any snapshot fails the gate. The trace
accepts `--journey <journey_key>` to trace specific journeys.
