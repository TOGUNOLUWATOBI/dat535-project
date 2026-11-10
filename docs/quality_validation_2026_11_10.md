# Quality validation, 10 November 2026

This checks the provider-completeness gate and the expanded independent XML
traces. It is a local verification of January, not a claim that the school
server or all ten snapshots have been upgraded.

## Full January snapshot

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

## Independent source checks

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

## Regression results

The final Spark-enabled suite ran 102 tests in 149 seconds: 99 passed and
3 GUI tests were skipped because pandas is intentionally absent from that
environment. Those 3 GUI tests passed separately in the isolated GUI environment.
`git diff --check` also passed.

## Failure cases covered by regression tests

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
is preserved as earlier evidence. See [data quality](data_quality.md) for the
commands to regenerate evidence on the school dataset.
