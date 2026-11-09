# Route history (EPIC 06: T38–T43, T46–T47)

`pipeline/analysis/route_history.py` compares the nine monthly snapshots
(15 January – 15 September 2026) line by line and records how the Kolumbus
(`KOL`) and AKT networks changed. It reads only completed, hash-verified Silver
and Gold checkpoints, so it runs after `scripts/run_processing.sh`.

Everything here describes **published planned timetables**, not buses that ran.
Snapshots are monthly, so a change found between 15 June and 15 July happened
at some unknown time in that interval. Departure counts are for each snapshot's
seven-day Gold window, not whole months.

## Running it

```bash
bash scripts/run_route_history.sh --data-root /srv/dat535/data
bash scripts/inspect_route_history.sh --data-root /srv/dat535/data summary
bash scripts/inspect_route_history.sh --data-root /srv/dat535/data candidates
bash scripts/inspect_route_history.sh --data-root /srv/dat535/data month 2026-07-15
bash scripts/inspect_route_history.sh --data-root /srv/dat535/data line AKT 55
bash scripts/inspect_route_history.sh --data-root /srv/dat535/data date AKT 55 2026-07-20
```

Both scripts use the school Spark environment and the project Java 17 runtime.
They `unset SPARK_HOME` so the Spark 4.2 bundled with `~/spark-env` is used,
not the older lab installation in `/opt/spark`.

A run with the same inputs, code, options and mapping file reuses the earlier
output. Pass `--force` to rebuild. Outputs go to
`DATA_ROOT/gold/entur/history/<identity>/history-<id>/`.

## Output tables

| Table | One row per | Task |
| --- | --- | --- |
| `line_snapshots` | snapshot, provider, line ID: bus number, name, stops, patterns, window departures | T38 |
| `line_patterns` | snapshot, line, journey pattern (ordered stop sequence) | T40 |
| `line_transitions` | consecutive snapshot pair, line: `added`, `retired`, `changed`, `service_changed`, `unchanged` | T39 |
| `line_changes` | individual change event (see below) | T39–T40 |
| `renumbering_candidates` | possible renumbering, joined with verified mappings | T42 |
| `line_versions` | SCD Type 2 version of a line's structure | T41 |
| `line_service_versions` | Gold service date linked to its line version | T43 |
| `stop_coverage` | snapshot, stop place: lines serving it | T47 |
| `stop_coverage_changes` | stop that gained or lost service, or whose serving lines changed | T47 |
| `network_snapshots` | snapshot, provider: network totals | T46 |
| `change_summary` | snapshot pair, provider: counts of each kind of change | T46 |

Change events: `line_added`, `line_retired`, `public_code_changed`,
`name_changed`, `transport_mode_changed`, `stops_added`, `stops_removed`,
`patterns_changed` (structure) and `service_started`, `service_stopped`,
`departures_increased`, `departures_decreased`, `first_departure_changed`,
`last_departure_changed` (service level).

## Rules

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

### Renumbering candidates

A candidate is produced when:

1. a line keeps its ID but its bus number changes (`same_line_id_new_code`), or
2. a line is retired and a line in the next snapshot shares its bus number
   (`same_code_new_line_id`, stop similarity ≥ 0.2), or has a different number
   but similar stops (`similar_stops`, stop similarity ≥ 0.6), or
3. the pair is listed as verified in `config/route_renumberings.json` but the
   rules above miss it (`verified_mapping`).

Stop similarity is the Jaccard index of the two lines' stop sets.

Two lessons from the real data shaped these rules:

* **Operators publish the new line ID one month early.** AKT's new `AKT:Line:55`
  appears in the 15 June snapshot with no departures, and the old
  `AKT:Line:1055` disappears only on 15 July. A retired line is therefore
  compared with every line in the next snapshot whose bus number first appeared
  up to one snapshot earlier (`--renumber-lookback`), not only with lines added
  in that same step. `new_line_since` records when the new line first appeared.
* **Bus numbers get reused.** In July 2026 Kolumbus' old line 1 continued as line 9
  (identical stops), while the former line A took number 1. Matching on the
  number alone paired unrelated routes with stop similarity around 0.01, so
  same-number pairs also need stop similarity ≥ 0.2 (`--same-code-similarity`).

Candidates are suggestions. A pair counts as a renumbering only after someone
checks it against an operator source and adds it to
`config/route_renumberings.json` with `verified`, `verified_by`, `to_snapshot`
and a `note` that cites the source. Verified pairs link the new line's version
to the old line through `line_versions.renumbered_from_line_id`. A
discontinued line is not a renumbering and must not be listed there.

## Self-checks

The run fails (manifest `status: failed`) unless all of these hold:

* each snapshot's lines and departures equal its Silver line count and Gold
  departure total, and every Gold line exists in Silver;
* lines after = lines before + added − retired, for each provider and step;
* every line snapshot belongs to exactly one version;
* exploded change rows equal the change lists in `line_transitions`;
* every Gold service date links to a version and the linked departures equal
  the Gold total.

It also reports how many verified mappings were matched.

## Results on the nine 2026 snapshots

Run of 9 November 2026: all checks passed. 466,493 Gold departures reconcile
exactly with the history tables and the version-linked service dates. 4,830
line snapshots form 1,237 line versions. 27 renumbering candidates were found;
8 are verified (3 AKT, 5 Kolumbus; all 8 mappings matched).

Departures in each snapshot's seven-day window:

| Step | AKT | KOL |
| --- | --- | --- |
| Jan → Feb | 21,236 → 16,650 (−21.6%) | 32,086 → 32,277 (+0.6%) |
| Feb → Mar | 16,650 → 21,193 | 32,277 → 32,268 |
| Mar → Apr | 21,193 → 21,115 | 32,268 → 32,254 |
| Apr → May | 21,115 → 21,335 | 32,254 → 32,245 |
| May → Jun | 21,335 → 21,066 | 32,245 → 31,870 |
| Jun → Jul | 21,066 → 15,245 (−27.6%) | 31,870 → 25,282 (−20.7%) |
| Jul → Aug | 15,245 → 20,614 | 25,282 → 34,290 (+35.6%) |
| Aug → Sep | 20,614 → 20,597 | 34,290 → 34,870 |

* **July:** summer timetables in both regions. The Jun → Jul step also has the
  most structural change: AKT retired 19 lines and added 6, and KOL retired 9.
* **February:** AKT alone drops by about a fifth. This is the Agder winter
  school holiday (week 8, 16–20 February 2026); see below.
* **August:** Kolumbus returns about 7.6% above its June level.

## Verified real change: Agder, 22 June and 1 July 2026

Source: AKT, *Route changes in Agder from 22 June and 1 July 2026*,
<https://www.akt.no/english/news/route-changes-in-agder-from-22-june-and-1-july-2026.9944.aspx>.
The data matches each announced change:

**Route 56 discontinued, Route 55 extended to Odderhei (Kristiansand).**

* `AKT:Line:1056` (bus 56, Rona – Odderhei, 178 departures a week) is retired
  between 15 June and 15 July.
* Bus 55 (Sørlandssenteret – Dvergsnes, 277 departures a week) moves from
  `AKT:Line:1055` to `AKT:Line:55`. The new ID is published on 15 June with
  0 departures and takes over all 277 on 15 July. Its route now also serves
  Odderhei, Odderhei nedre and Dvergsneskrysset (25 → 28 stops). This is the
  `same_code_new_line_id` candidate with stop similarity 0.89, published one
  month early.
* Eight stops were served only by line 56 and lost all service: Fuglevik,
  Fuglevikkleiva, Kjellevik, Knarrevik, Randesundheimen, Salbustad, Sømskleiva
  and Sømsveien/Sømslia (`stop_coverage_changes`, `stop_lost_service`).

**Route 178 replaced by Route 172, with AKT Svipp in Åseral.**

* `AKT:Line:178` (bus 178, Åseral–Evje, 37 stops, 131 departures a week) is
  retired between 15 June and 15 July.
* `AKT:Line:172` (bus 172, Åseral–Evje, 23 stops) is published from 15 June. It
  has 0 departures in the 15–21 July window and 30 a week from August.
  Its stop similarity with 178 is only 0.46 (shorter route), so the automatic
  rules miss it. It is included through the verified mapping
  (`verified_mapping`).
* The supplementary line `AKT:Line:9178` (178S, published only on 15 June, no
  fixed departures) continues as `AKT:Line:9172` (SVIPP Åseral–Evje, 44
  departures a week from July), stop similarity 0.81.

All three pairs are in `config/route_renumberings.json` and linked in
`line_versions`. Line 56 is deliberately not listed, because it was
discontinued, not renumbered.

## Verified real change: Kolumbus Bussveien, 1 July 2026

Source: Kolumbus, *Ruteendringer 1. juli 2026*,
<https://www.kolumbus.no/aktuelt/ruteendringer-1.-juli-2026/>. Bussveien
between Stavanger and Sandnes opened on 1 July 2026, and lines 1–4 were
renumbered around it. Every renumbering in the notice is in the data, all with
old line retired and new line published between 15 June and 15 July:

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

## Resolved: AKT winter holiday, February 2026

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
did not check Rogaland's holiday dates).

## Resolved in the data: AKT "T" bus numbers

`AKT:Line:583`, `585`, `586`, `587`, `588` and `590` drop the `T` from their
bus number on 15 September (e.g. `586T` → `586`), and `AKT:Line:3701` changes
`881T` → `881` on 15 July. The line IDs, stop sets (similarity 1.0) and
September departures are unchanged; only `881` goes from 2 to 4 departures a
week. These are label changes on continuing lines, already handled by the
version history (`same_line_id_new_code`), so no mapping is needed. We found no
AKT source that explains the `T` suffix (it may mark school-day services,
since these are low-frequency Kristiansand-area routes), so its meaning is
not documented here.

## Limitation

Monthly snapshots cannot date a change more precisely than the interval
between two snapshots.
