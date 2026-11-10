# Verified full-month coverage

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
This verifies the local rebuild; deployment and regeneration on the school
server are separate. See [server commands](full_month_processing.md).
