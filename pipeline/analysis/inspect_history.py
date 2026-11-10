"""Print route-history results in readable form, for checking and documenting changes.

Reads the newest complete route-history output under DATA_ROOT. Examples:

    bash scripts/inspect_route_history.sh --data-root /srv/dat535/data summary
    bash scripts/inspect_route_history.sh --data-root /srv/dat535/data candidates
    bash scripts/inspect_route_history.sh --data-root /srv/dat535/data month 2026-06-15
    bash scripts/inspect_route_history.sh --data-root /srv/dat535/data line AKT 100
    bash scripts/inspect_route_history.sh --data-root /srv/dat535/data date AKT 100 2026-07-20

``line`` and ``date`` accept a bus number (public code) or a full NeTEx line ID.
Departure numbers are planned departures in each snapshot's Gold window
(a full calendar month by default). Observation and service dates remain separate.
"""

import argparse
from datetime import date
import json
import os
from pathlib import Path
import sys


def latest_history(data_root):
    root = Path(data_root) / "gold" / "entur" / "history"
    found = []
    for manifest in root.glob("*/history-*/metadata/manifest.json"):
        try:
            data = json.loads(manifest.read_text())
        except (OSError, ValueError):
            continue
        if data.get("status") == "complete":
            found.append((data.get("completed_at_utc", ""), manifest.parent.parent, data))
    if not found:
        raise SystemExit(f"No complete route history under {root}; run scripts/run_route_history.sh first")
    _, path, data = max(found, key=lambda item: item[0])
    return path, data


def clock(seconds):
    if seconds is None:
        return "-"
    seconds = int(seconds)
    day, rest = divmod(seconds, 86400)
    return f"{rest // 3600:02d}:{rest % 3600 // 60:02d}" + (f"+{day}d" if day else "")


class History:
    def __init__(self, spark, path):
        self.spark, self.path = spark, Path(path)
        self._names = None

    def table(self, name):
        return self.spark.read.parquet(str(self.path / name))

    def stop_names(self):
        if self._names is None:
            rows = self.table("stop_coverage").select("stop_id", "stop_name").where("stop_name IS NOT NULL").distinct().collect()
            self._names = {r.stop_id: r.stop_name for r in rows}
        return self._names

    def name_stops(self, ids, limit=12):
        names = self.stop_names()
        shown = [names.get(i, i) for i in (ids or [])[:limit]]
        more = len(ids or []) - limit
        return ", ".join(shown) + (f" … (+{more} more)" if more > 0 else "")

    def resolve(self, provider, line):
        from pyspark.sql import functions as F
        snapshots = self.table("line_snapshots").where(F.col("provider") == provider.upper())
        if ":" in line:
            ids = [line]
        else:
            ids = sorted({r.line_id for r in snapshots.where(F.array_contains("public_codes", line))
                          .select("line_id").collect()})
        if not ids:
            raise SystemExit(f"No {provider.upper()} line with bus number or ID {line!r}")
        return ids


def show_summary(h, report):
    print(f"Route history: {h.path}")
    print("Snapshots:", ", ".join(s["snapshot_date"] for s in report["snapshots"]))
    print("Checks:", json.dumps({k: v for k, v in report["checks"].items() if k != "snapshots"}))
    print("\nNetwork per published export (departures = planned departures in the declared Gold window;")
    print("stops = provider stops in published patterns / served by journeys operating in the window)")
    print(f"{'snapshot':11} {'prov':4} {'mode':6} {'lines':>5} {'w/ service':>10} {'departures':>10} {'stops':>13}")
    for r in h.table("network_snapshots").orderBy("snapshot_date", "provider", "transport_mode").collect():
        print(f"{r.snapshot_date:11} {r.provider:4} {r.transport_mode:6} {r.lines_published:5} "
              f"{r.lines_with_window_service:10} {r.window_departures:10} "
              f"{r.provider_stops_in_definitions:6}/{r.provider_stops_served_in_window:<6}")


def show_candidates(h):
    from pyspark.sql import functions as F
    lines = {(r.snapshot_date, r.provider, r.line_id): r for r in h.table("line_snapshots").select(
        "snapshot_date", "provider", "line_id", "name", "stop_ids", "window_departures").collect()}
    rows = h.table("renumbering_candidates").orderBy("from_snapshot", "provider", F.desc("stop_similarity")).collect()
    print(f"{len(rows)} renumbering candidates (verified = verified entry in config/route_renumberings.json)\n")
    for n, r in enumerate(rows, 1):
        old = lines.get((r.from_snapshot, r.provider, r.old_line_id))
        new = lines.get((r.to_snapshot, r.provider, r.new_line_id))
        early = f"  (new line published from {r.new_line_since})" if r.new_line_since and r.new_line_since < r.to_snapshot else ""
        print(f"[{n}] {r.provider}  {r.from_snapshot} → {r.to_snapshot}  {r.match_type}  "
              f"stop similarity {r.stop_similarity:.0%}  verified={r.verified}{early}")
        print(f"    before: bus {r.old_public_code:6} {r.old_line_id}  \"{old.name if old else '?'}\"  "
              f"{old.window_departures if old else '?'} departures")
        print(f"    after:  bus {r.new_public_code:6} {r.new_line_id}  \"{new.name if new else '?'}\"  "
              f"{new.window_departures if new else '?'} departures")
        if old and new:
            gone = sorted(set(old.stop_ids) - set(new.stop_ids))
            came = sorted(set(new.stop_ids) - set(old.stop_ids))
            if gone:
                print(f"    stops no longer served: {h.name_stops(gone, 8)}")
            if came:
                print(f"    new stops: {h.name_stops(came, 8)}")
        print()


def show_month(h, start, top):
    from pyspark.sql import functions as F
    transitions = h.table("line_transitions").where(F.col("from_snapshot") == start)
    if not transitions.limit(1).count():
        raise SystemExit(f"No transition starting at {start}; use a snapshot date such as 2026-06-15")
    end = transitions.first().to_snapshot
    print(f"Differences between the {start} and {end} exports\n")
    for r in h.table("change_summary").where(F.col("from_snapshot") == start).orderBy("provider").collect():
        print(f"{r.provider}: {r.lines_before}→{r.lines_after} lines (+{r.lines_appeared} appeared / "
              f"-{r.lines_disappeared} disappeared, of which {r.lines_disappeared_renumbered_verified} verified "
              f"renumbered and {r.lines_disappeared_discontinued_verified} verified discontinued), "
              f"{r.lines_structure_changed} changed, {r.lines_public_code_changed} new bus numbers, "
              f"departures {r.window_departures_before}→{r.window_departures_after} "
              f"(bus {r.bus_window_departures_before}→{r.bus_window_departures_after}), "
              f"window-served stops +{r.stops_gained_window_service} / -{r.stops_lost_window_service}")
    for status, title in (("appeared", "Lines that appeared"), ("disappeared", "Lines no longer observed")):
        rows = transitions.where(F.col("status") == status).orderBy("provider", "line_id").collect()
        if rows:
            print(f"\n{title} ({len(rows)}):")
            for r in rows:
                code = r.public_code_after if status == "appeared" else r.public_code_before
                name = r.name_after if status == "appeared" else r.name_before
                departures = r.departures_after if status == "appeared" else r.departures_before
                print(f"  {r.provider} {code:6} {name}  ({departures} departures)  {r.line_id}  [{r.interpretation}]")
    renamed = transitions.where(F.array_contains("change_types", "public_code_changed")).collect()
    if renamed:
        print(f"\nRenumbered ({len(renamed)}):")
        for r in renamed:
            print(f"  {r.provider} bus {r.public_code_before} → {r.public_code_after}  {r.name_after}  {r.line_id}")
    both = transitions.where("departures_before > 0 AND departures_after > 0")
    for title, order in (("Largest departure cuts", F.asc("departure_change")),
                         ("Largest departure increases", F.desc("departure_change"))):
        rows = both.orderBy(order).limit(top).collect()
        print(f"\n{title}:")
        for r in rows:
            if (r.departure_change < 0) == title.startswith("Largest departure cuts") and r.departure_change:
                print(f"  {r.provider} bus {r.public_code_after:6} {r.departures_before:5} → {r.departures_after:5} "
                      f"({r.departure_change_ratio:+.0%})  {r.name_after}")
    stops = h.table("stop_coverage_changes").where((F.col("from_snapshot") == start)
                                                   & (F.col("measure") == "window_service"))
    for kind, title in (("stop_lost", "Stops no longer served in the window"),
                        ("stop_gained", "Stops newly served in the window")):
        rows = stops.where(F.col("change_type") == kind).orderBy("provider", "stop_name").collect()
        if rows:
            print(f"\n{title} ({len(rows)}):")
            for r in rows[:top * 3]:
                codes = r.line_codes_before if kind == "stop_lost" else r.line_codes_after
                print(f"  {r.provider} {r.stop_name or r.stop_id}  (buses {', '.join(codes)})  {r.stop_id}")
            if len(rows) > top * 3:
                print(f"  … and {len(rows) - top * 3} more")


def show_line(h, provider, line):
    from pyspark.sql import functions as F
    for line_id in h.resolve(provider, line):
        print(f"=== {provider.upper()} {line_id}\n")
        print(f"{'snapshot':11} {'bus':6} {'stops':>5} {'patterns':>8} {'departures':>10} {'first':>6} {'last':>7}  name")
        for r in h.table("line_snapshots").where(F.col("line_id") == line_id).orderBy("snapshot_date").collect():
            print(f"{r.snapshot_date:11} {r.public_code:6} {r.stop_count:5} {r.pattern_count:8} {r.window_departures:10} "
                  f"{clock(r.first_departure_seconds):>6} {clock(r.last_departure_seconds):>7}  {r.name}")
        print("\nVersions (SCD Type 2, by observation date):")
        for v in h.table("line_versions").where(F.col("line_id") == line_id).orderBy("version_number").collect():
            print(f"  v{v.version_number}: observed {v.first_observed} … {v.last_observed}  bus {v.public_code}, "
                  f"{v.stop_count} stops, {v.pattern_count} patterns  reason={v.version_reason}"
                  f"{'  (in latest export)' if v.observed_in_latest else f'  not observed from {v.first_not_observed}'}"
                  f"{f'  renumbered from {v.renumbered_from_line_id}' if v.renumbered_from_line_id else ''}")
        changes = h.table("line_changes").where(F.col("line_id") == line_id).orderBy("from_snapshot", "change_type").collect()
        print("\nChanges:" if changes else "\nChanges: none")
        for c in changes:
            detail = ""
            if c.change_type in ("stops_added", "stops_removed", "line_appeared", "line_disappeared"):
                detail = f"  [{h.name_stops(c.items)}]"
            elif c.change_type.endswith("departure_changed"):
                detail = f"  {clock(float(c.old_value)) if c.old_value else '-'} → {clock(float(c.new_value)) if c.new_value else '-'}"
            elif c.old_value is not None or c.new_value is not None:
                detail = f"  {c.old_value} → {c.new_value}"
            print(f"  {c.from_snapshot} → {c.to_snapshot}  {c.change_type}{detail}")
        patterns = h.table("line_patterns").where(F.col("line_id") == line_id).orderBy("snapshot_date", F.desc("journeys")).collect()
        print("\nStop sequences per snapshot (most used first):")
        seen = {}
        for p in patterns:
            label = seen.setdefault(p.pattern_signature, chr(ord("A") + len(seen)) if len(seen) < 26 else p.pattern_signature)
            print(f"  {p.snapshot_date}  pattern {label}: {p.journeys} journeys, {p.stops} stops")
        for signature, label in seen.items():
            sequence = next(p.stop_sequence for p in patterns if p.pattern_signature == signature)
            print(f"  pattern {label}: {h.name_stops(sequence, 60)}")
        print()


def show_date(h, provider, line, day):
    """Which line version and departures apply on a service date (T43)."""
    from pyspark.sql import functions as F
    day = date.fromisoformat(day).isoformat()
    for line_id in h.resolve(provider, line):
        rows = h.table("line_service_versions").where((F.col("line_id") == line_id) & (F.col("service_date") == day)).collect()
        if rows:
            for r in rows:
                print(f"{provider.upper()} {line_id} on {day}: version v{r.version_number} (bus {r.version_public_code}, "
                      f"observed {r.first_observed} … {r.last_observed}), {r.scheduled_departures} planned departures "
                      f"{clock(r.first_departure_seconds)}–{clock(r.last_departure_seconds)}, published in snapshot {r.snapshot_date}")
            continue
        snapshots = sorted(r.snapshot_date for r in h.table("line_snapshots").where(F.col("line_id") == line_id)
                           .select("snapshot_date").collect())
        earlier = [s for s in snapshots if s <= day]
        print(f"{provider.upper()} {line_id} on {day}: not inside any snapshot's Gold window."
              + (f" The latest earlier observation is {earlier[-1]}; departures between snapshots were not computed."
                 if earlier else " The line was not observed before this date."))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--history", type=Path, help="A specific history output (default: newest complete)")
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--master", default="local[2]")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("summary")
    commands.add_parser("candidates")
    month = commands.add_parser("month"); month.add_argument("from_snapshot")
    line = commands.add_parser("line"); line.add_argument("provider"); line.add_argument("line")
    when = commands.add_parser("date"); when.add_argument("provider"); when.add_argument("line"); when.add_argument("service_date")
    args = parser.parse_args(argv)
    if args.history:
        path, report = args.history, json.loads((args.history / "metadata/manifest.json").read_text())
    else:
        path, report = latest_history(args.data_root)
    from pyspark.sql import SparkSession
    if args.master.startswith("local"):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder.appName("DAT535-inspect-history").master(args.master)
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        h = History(spark, path)
        if args.command == "summary":
            show_summary(h, report)
        elif args.command == "candidates":
            show_candidates(h)
        elif args.command == "month":
            show_month(h, args.from_snapshot, args.top)
        elif args.command == "line":
            show_line(h, args.provider, args.line)
        else:
            show_date(h, args.provider, args.line, args.service_date)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())