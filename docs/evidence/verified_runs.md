# Verified runs and results

Dated records of what was run and what it showed, kept as evidence for the
[project report](../Project_Report.pdf). Each record keeps its original scope:
older seven-day results are not evidence for the full-month totals. All figures
describe **published timetable plans**, not what vehicles actually ran.

The generated [quality report](quality_report.md) and
[February journey traces](quality_traces.md) sit next to this file.

## Departure totals and what they count

| Total | Period | Record |
| ---: | --- | --- |
| 466,493 | Seven-day windows of the nine historical snapshots (15th–21st, Jan–Sep) | [Historical processing](#historical-processing--28-october-2026) |
| 521,185 | The same nine windows plus the 8 November snapshot window (54,692) | [Route history](#route-history--9-november-2026) |
| 1,918,707 | All 273 days of January–September 2026 | [Full-month coverage](#full-month-coverage--10-november-2026) |
| 83,637 | 1–11 November 2026 in the GUI (dates up to today) | Report, Section 3.4.7 |

These totals cover different periods and do not add up to each other.

## Ingestion and automation

### School-server runner (installed 24 October 2026)

| Setting | Value |
| --- | --- |
| Operating system | Ubuntu 24.04.4 LTS, x86_64, Python 3.11.15 |
| Runner | `dat535-spark`, labels `self-hosted`, `Linux`, `X64`, `dat535` |
| Runner directory | `/home/ubuntu/dat535-runner` |
| Systemd service | `actions.runner.TOGUNOLUWATOBI-dat535-project.dat535-spark.service` |
| Persistent storage | `/srv/dat535/data` (repository variable `DAT535_DATA_ROOT`) |

### Manual CLI ingestion (24 October 2026)

`poll_entur_netex --date 2026-10-23` on the school server finished in 43.5 s
with status `complete_with_warnings`.

| Result | Value |
| --- | ---: |
| National ZIP | 282,505,026 bytes |
| Selected XML documents (322 KOL, 209 AKT, national stops) | 532 |
| XML including national stops | 822,336,592 bytes |
| Selected ZIP | 63,454,934 bytes |
| Provider references checked | 1,296,889 |
| Unresolved references / same-file version failures | 0 / 0 |
| Repeated AKT ID/version occurrences (kept as warnings) | 65 |

Sizes, MD5, SHA-256, every XML hash and every ZIP CRC were checked again
afterwards. A second call with the same output path was refused with
`FileExistsError` and changed nothing.

### GitHub Actions runs

| Trigger | Run | Date | Result |
| --- | --- | --- | --- |
| Manual dispatch | [40388358483](https://github.com/TOGUNOLUWATOBI/dat535-project/actions/runs/40388358483) | 26 Oct | All steps passed in 52 s; 25 Oct snapshot, 532 XML files |
| Push to `main` | [40537981353](https://github.com/TOGUNOLUWATOBI/dat535-project/actions/runs/40537981353) | 27 Oct | All steps passed in 52 s; manifest records `github_event_name=push` |
| Schedule (3 runs) | latest [40638229520](https://github.com/TOGUNOLUWATOBI/dat535-project/actions/runs/40638229520) | 27–28 Oct | About 53 s each; manifest records `github_event_name=schedule` |
| Push to `main` | [42814695326](https://github.com/TOGUNOLUWATOBI/dat535-project/actions/runs/42814695326) | 10 Nov | Route history failed on Java heap; fixed afterwards (see [execution times](#execution-times)) |

Every ingestion run had zero unresolved provider references and kept the 65
repeated AKT IDs as warnings. GitHub started the scheduled runs hours apart
rather than at the requested times, so the manifest records the trigger type
and the schedule is treated as a request, not a guarantee.

## Historical processing (28 October 2026)

All nine snapshots completed staging, Silver and Gold on the school server.
Each Gold total reconciled exactly to eligible fixed-service occurrences in
Silver. The last column is the seven days starting on the snapshot date.

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
| **Total** | **263,757** | **8,296,954** | **466,493** |

The windows also held 192 flexible (on-demand) occurrences, reported
separately. All nine runs had zero quarantined journeys or passing times and
zero calendar issues.

**Checkpoint reuse.** A second full run reused all 27 completed stages. The
registry still had exactly 27 attempts and no new output folders were created.

## Storage

| Step | Result |
| --- | --- |
| Hardlinking 7 byte-identical national ZIPs (28 Oct) | 1,957,911,441 bytes recovered; every path and checksum kept |
| Selected ZIP instead of expanded XML, 15 Jan snapshot (local copy) | 541 XML files, 820,984,157 bytes → 62,842,321-byte ZIP; all XML hashes and the checkpoint identity unchanged after restore |
| Server migration to selected ZIPs (10 Nov) | 34 snapshots migrated, 12 skipped, no problems; free disk 3.5 GiB → 23 GiB on the 38 GiB VM |

## Full-month coverage (10 November 2026)

### School server

Rebuild `monthly-67b04d35cf56` completed all **22 jobs**, and the coverage
audit passed for every month from **January through November 2026**. Route
history was written to `gold/entur/history/d5d5ea417f73a691/history-53300d0ab697`
under `/srv/dat535/data`. Free disk afterwards: 20 GiB (root disk 47% used).
This confirms completed jobs and date coverage; the departure totals below come
from the separately verified local run.

### Local verification, January–September

Nine primary and nine calendar-support exports cover all 273 dates for both
providers (546 provider/date pairs). All 18 Gold gates passed. Bus and water,
fixed departures only:

| Month | Dates covered | AKT | KOL | Total |
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
| **Total** | **273/273** | **746,692** | **1,172,015** | **1,918,707** |

Each date uses the newest valid export that covers it for both providers;
overlapping exports are never added together. January 1 has 2,722 fixed
departures and January 31 has 4,337. The saved file inventories of all 54
completed staging, Silver and Gold attempts were rechecked.

The 1 and 2 February exports failed reference validation on
`NSR:Quay:43289` and were kept as evidence. The validated 14 January export,
whose calendars cover February, supplies the early February dates under its own
source date.

## Quality validation (10 November 2026)

### Full 15 January snapshot

The preserved 15 January XML was reprocessed with the current staging schema,
and Silver and Gold were rebuilt in temporary storage. Existing outputs were
not changed.

| Measure | Result |
| --- | --- |
| Journey occurrences | 25,289 |
| Passing-time occurrences | 768,701 |
| Service-date occurrences, 15–21 January | 53,345 |
| Fixed departures published | 53,322 |
| Flexible occurrences excluded | 23 |
| Gold publication gate | pass (version 2, both AKT and KOL) |
| Silver → Gold counts | reconciled |

### Independent XML-to-Gold traces

`pipeline/quality/trace.py` reads the XML with the Python standard library only
(no Spark, no Silver code). It resolves pattern point → scheduled stop →
assignment → quay → national stop place (or flexible stop place), recomputes
times and operating dates, and recounts every source journey of the selected
line against Gold.

| Case | Line | Passing times checked | Source journey occurrences (whole line) | Fixed departures in window | Result |
| --- | --- | ---: | ---: | --- | --- |
| Ordinary | AKT 411 | 14 | 20 | 52 | pass |
| Overnight | AKT 100 | 69 | 150 | 418 | pass |
| Calendar exception | KOL SK2104 | 9 | 11 | 14 | pass |
| Flexible | KOL HentMeg | 2 | 7 | 0 (11 flexible excluded) | pass |

These are checks of the hardest cases, not of every source row. The committed
[quality_traces.md](quality_traces.md) is the earlier February run of the same
four case types with the original checker. The detailed per-stop output of the
January run was saved locally under `docs/audit/2026-11-10/` (ignored by Git).

### Failure cases covered by regression tests

* An empty provider selection, a missing expected provider or an undeclared
  provider fails the gate, as does any staging/Silver count difference.
* Calendar impact is measured before date expansion: broken AKT calendars give
  a 100% impact rate, not zero.
* A wrong quay or a swapped flexible window is detected even when fixed times
  match.
* Missing or ambiguous assignments and ambiguous journey definitions are refused.
* Repeated journey IDs in different XML files stay separate occurrences.
* Moving a journey's date in both Silver and Gold keeps them consistent with
  each other, but the independent XML recount detects it.
* `test_invalid_times_are_quarantined_and_the_quality_gate_stops_gold` breaks
  half of the AKT journeys and checks that Gold is not published.

## Route history (9 November 2026)

Ten snapshots (the nine historical months and the 8 November scheduled
snapshot); every self-check passed.

| Measure | Value |
| --- | ---: |
| Gold departures reconciled with history tables | 521,185 (476,651 bus, 44,534 water) |
| Of which, nine historical windows | 466,493 (426,395 bus, 40,098 water) |
| Line snapshots → line versions (SCD Type 2) | 5,358 → 1,336 |
| Renumbering candidates | 31 |
| Verified renumberings / discontinuations (all match the data) | 16 / 4 |
| Disappearances without an operator source (`not_observed`) | 24 |

### Seven-day departures per step (all modes)

| Step | AKT | KOL |
| --- | --- | --- |
| Jan → Feb | 21,236 → 16,650 (−21.6%) | 32,086 → 32,277 |
| Feb → Mar | 16,650 → 21,193 | 32,277 → 32,268 |
| Mar → Apr | 21,193 → 21,115 | 32,268 → 32,254 |
| Apr → May | 21,115 → 21,335 | 32,254 → 32,245 |
| May → Jun | 21,335 → 21,066 | 32,245 → 31,870 |
| Jun → Jul | 21,066 → 15,245 (−27.6%) | 31,870 → 25,282 (−20.7%) |
| Jul → Aug | 15,245 → 20,614 | 25,282 → 34,290 (+35.6%) |
| Aug → Sep | 20,614 → 20,597 | 34,290 → 34,870 |
| Sep → 8 Nov | 20,597 → 20,753 | 34,870 → 33,939 |

The last step spans eight weeks and its window is 8–14 November, so it is a
separate observation rather than a monthly change.

### Stops in patterns versus stops actually served

| Snapshot | AKT in patterns | AKT served in window | KOL in patterns | KOL served in window |
| --- | ---: | ---: | ---: | ---: |
| 15 Jan | 3,742 | 3,742 | 2,853 | 2,848 |
| 15 Feb | 3,744 | **2,269** | 2,852 | 2,847 |
| 15 Jun | 3,730 | 3,710 | 2,872 | 2,834 |
| 15 Jul | 3,676 | **2,175** | 2,863 | **1,986** |
| 15 Aug | 3,675 | 3,675 | 2,865 | 2,853 |

### Verified real changes

**Kolumbus Bussveien, 1 July 2026**
([Kolumbus notice](https://www.kolumbus.no/aktuelt/ruteendringer-1.-juli-2026/)):

| Notice | Old line | New line | Stop similarity | How found |
| --- | --- | --- | ---: | --- |
| Rute 1 Stavanger – Hundvåg blir nå rute 9 | `8_1000` (1) | `8_1109` (9) | 1.00 | `similar_stops` |
| Rute 4 Rosenli – Madlakrossen blir nå rute 12 | `8_1005` (4) | `8_1112` (12) | 1.00 | `similar_stops` |
| Rute 2 Stavanger – Sandnes blir nå rute 1 | `8_1002` (2) | `8_1001` (1) | 0.26 | `verified_mapping` |
| rute 2 Stavanger – Risavika blir rute 4 | `8_1002` (2) | `8_1104` (4) | 0.51 | `verified_mapping` |
| Rute 3 Stavanger – Forus blir nå rute 2 | `8_1004` (3) | `8_1003` (2) | 0.46 | `verified_mapping` |

The rule-based candidates `8_1002 → 8_1003` (shared number 2, similarity 0.22)
and `8_1004 → 8_1103` (shared number 3, 0.55) contradict the notice and stay
unverified. The same notice explains X77 → 43 (`8_1040` → `8_1024`), 46 →
SK0203 (`8_2014` → `8_2687`) and the discontinued route 67 (`8_1042`) and boat
route 704 (`8_5752`).

**Agder, 22 June and 1 July 2026**
([AKT notice](https://www.akt.no/english/news/route-changes-in-agder-from-22-june-and-1-july-2026.9944.aspx)):

* Route 56 (`AKT:Line:1056`, 178 departures a week) is discontinued from 15 July.
  Eight stops served only by line 56 lost all service.
* Bus 55 moved from `AKT:Line:1055` to `AKT:Line:55`: the new ID was published
  on 15 June with no departures and had all 277 on 15 July. It was extended to
  Odderhei (25 → 28 stops, similarity 0.89).
* Route 178 was replaced by 172 (similarity 0.46), linked by the verified mapping.
* Kristiansand city lines: 54 → 32 and 32 → 32 (a merge), and new IDs for
  19, 30, 31 and 35. Night bus N110 (`AKT:Line:9110`) is a verified
  discontinuation.

**AKT February drop (resolved).** The 15–21 February window is the Agder winter
school holiday (week 8, 16–20 February 2026). Only AKT weekdays drop:

| Window | AKT weekday | AKT Saturday | AKT Sunday | KOL weekday |
| --- | ---: | ---: | ---: | ---: |
| 15–21 January | ≈ 3,730 | 1,668 | 897 | ≈ 5,520 |
| 15–21 February | ≈ 2,815 (−25%) | 1,667 | 896 | ≈ 5,560 |
| 15–21 March | ≈ 3,725 | 1,667 | 896 | ≈ 5,560 |

Only 116 of AKT's 212 lines run that week; the missing stops are on school
routes (19xx/23xx).

## Execution times

| Stage | Run | Time |
| --- | --- | ---: |
| Ingestion, manual CLI | 24 Oct, 532 XML files | 43.5 s |
| Ingestion job, Actions | dispatch 26 Oct; push 27 Oct | 52 s each |
| Scheduled ingestion jobs | 3 runs, 27–28 Oct | ≈ 53 s each |
| Ingestion step | Actions run 42814695326, 10 Nov | 43 s |
| Spark test suite step | same run | 3 min 36 s |
| Snapshot processing (staging → Gold) | same run | 28 s |
| Route history (failed on Java heap) | same run | 1 min 47 s |

**Memory fixes.** Route history ran out of Java heap while broadcasting during
`stop_coverage_changes`; automatic broadcast joins are now off for that job and
each table's cache is released after it is written. Locally, all 17 history
tests passed with a 1 GiB heap, including 79,200 coverage rows. The monthly
rebuild crashed after 14 of 22 jobs on the 1 April support export; it now runs
one Spark process per snapshot with a 2 GiB heap. An isolated rerun of that
April export (34,309 journeys, 1,193,513 passing times) passed with exactly
2,147,483,648 bytes of heap, and the server rebuild then completed all 22 jobs.

## Tests

The suite has 175 tests in 18 files: unit tests plus Spark tests that run the
real transformations on small NeTEx fixtures. The workflow runs the ingestion
tests before every download and the Spark tests before every processing run.
