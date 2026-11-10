"""Trace real journeys from the source XML through Silver to Gold, checked independently.

For one snapshot this picks four kinds of journey (or uses ``--journey`` IDs):

* ordinary: fixed, valid, no overnight times, no calendar exception in the window;
* overnight: a passing time after midnight, kept on its operating day with a day offset;
* calendar_exception: its day type has an ``isAvailable=false`` date inside the window;
* flexible: an on-demand journey, which Gold must exclude.

The source side is read with Python's standard XML parser only (no Spark, no
Silver code): passing times are converted to seconds and operating dates are
derived from DayType / DayTypeAssignment / OperatingPeriod directly. These are
compared with the Silver passing times and service dates, and the journey's
treatment in Gold (counted per line and date, or listed as excluded) is checked.
Pattern/stop references and flexible windows are resolved independently too;
whole-line XML counts guard against mutually consistent Silver/Gold errors.

    bash scripts/run_quality_trace.sh --data-root /srv/dat535/data --snapshot 2026-02-15 \\
        --markdown docs/quality_traces.md
"""

import argparse
from collections import defaultdict
from datetime import date, timedelta
import json
import os
from pathlib import Path
import sys

from pipeline.quality.source_checks import SourceIndex, SourceReferenceError, entities, frame_validity

NS = "{http://www.netex.org.uk/netex}"
DAYS = {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6}
GROUPS = {"Everyday": set(range(7)), "Weekdays": set(range(5)), "Weekend": {5, 6}}


# --------------------------------------------------------------------------
# Independent source reading (standard library only).

def text(element, tag):
    found = element.find(NS + tag)
    return found.text.strip() if found is not None and found.text else None


def ref(element, tag):
    found = element.find(NS + tag)
    return found.get("ref") if found is not None else None


def clock_seconds(value, offset):
    """'HH:MM:SS' plus a day offset, as seconds from the operating day's midnight."""
    if value is None:
        return None
    hours, minutes, secs = value.split(":")
    return int(hours) * 3600 + int(minutes) * 60 + float(secs) + int(offset or 0) * 86400


def source_journey(xml_path, journey_id, version=None):
    """The ServiceJourney (or flexible journey) element with this id, parsed from the line file."""
    matches = []
    for tag, element in entities(xml_path):
        if (tag in ("ServiceJourney", "FlexibleServiceJourney") and element.get("id") == journey_id
                and (version is None or element.get("version") == version)):
            times = []
            for point in element.iter(NS + "TimetabledPassingTime"):
                arrival, departure = text(point, "ArrivalTime"), text(point, "DepartureTime")
                times.append({
                    "point": ref(point, "StopPointInJourneyPatternRef"),
                    "point_version": point.find(NS + "StopPointInJourneyPatternRef").get("version")
                    if point.find(NS + "StopPointInJourneyPatternRef") is not None else None,
                    "arrival_raw": arrival, "departure_raw": departure,
                    "earliest_raw": text(point, "EarliestDepartureTime"), "latest_raw": text(point, "LatestArrivalTime"),
                    "arrival_seconds": clock_seconds(arrival, text(point, "ArrivalDayOffset")),
                    "departure_seconds": clock_seconds(departure, text(point, "DepartureDayOffset"))})
            pattern = element.find(NS + "JourneyPatternRef")
            matches.append({"id": journey_id, "version": element.get("version"), "kind": tag,
                    "line": ref(element, "LineRef"), "pattern": ref(element, "JourneyPatternRef"),
                    "pattern_version": pattern.get("version") if pattern is not None else None,
                    "day_types": [d.get("ref") for d in element.iter(NS + "DayTypeRef")], "passing_times": times})
    if len(matches) != 1:
        raise SourceReferenceError(f"Journey {journey_id} version {version}: found {len(matches)} in {xml_path}")
    return matches[0]


def source_calendar(xml_paths):
    """DayTypes, OperatingPeriods, OperatingDays and DayTypeAssignments from the provider's files."""
    day_types, periods, days, assignments = {}, {}, {}, defaultdict(list)
    versions = {}
    validity = {}
    def define(collection, element, value):
        identifier, version = element.get("id"), element.get("version")
        key = (element.tag, identifier)
        if identifier in collection and (collection[identifier] != value or versions[key] != version):
            raise SourceReferenceError(f"Conflicting or multiversion calendar definition: {identifier}")
        collection[identifier], versions[key] = value, version
    for path in xml_paths:
        bounds = frame_validity(path)
        for tag, element in entities(path):
            if tag == "DayType":
                validity[element.get("id")] = bounds
                weekdays = set()
                tokens = [t for prop in element.iter(NS + "PropertyOfDay") for t in (text(prop, "DaysOfWeek") or "").split()]
                for token in tokens:
                    if token not in GROUPS and token not in DAYS:
                        raise SourceReferenceError(f"Unsupported weekday token: {token}")
                    weekdays |= GROUPS.get(token, {DAYS[token]} if token in DAYS else set())
                for prop in element.iter(NS + "PropertyOfDay"):
                    unsupported = {c.tag.removeprefix(NS) for c in prop} - {"DaysOfWeek", "HolidayTypes"}
                    if unsupported or text(prop, "HolidayTypes") not in (None, "AnyDay"):
                        raise SourceReferenceError(f"Unsupported calendar restriction on {element.get('id')}")
                define(day_types, element, weekdays if tokens else set(range(7)))
            elif tag == "OperatingPeriod":
                define(periods, element, {"from": text(element, "FromDate"), "to": text(element, "ToDate"),
                                              "from_day": ref(element, "FromOperatingDayRef"),
                                              "to_day": ref(element, "ToOperatingDayRef")})
            elif tag == "OperatingDay":
                define(days, element, text(element, "CalendarDate"))
            elif tag == "DayTypeAssignment":
                available = text(element, "isAvailable") or "true"
                if available not in ("true", "false", "1", "0"):
                    raise SourceReferenceError(f"Invalid isAvailable: {available}")
                assignments[ref(element, "DayTypeRef")].append({
                    "date": text(element, "Date"), "period": ref(element, "OperatingPeriodRef"),
                    "day": ref(element, "OperatingDayRef"),
                    "available": available in ("true", "1")})
    return day_types, periods, days, assignments, validity


def operating_dates(day_type_ids, calendar, first, last):
    """Dates in [first, last] on which any of the day types operates, and the exceptions applied."""
    day_types, periods, days, assignments = calendar[:4]
    validity = calendar[4] if len(calendar) > 4 else {}
    as_date = lambda value: date.fromisoformat(value[:10])
    operating, exceptions = set(), []
    for day_type in day_type_ids:
        if day_type not in day_types:
            raise SourceReferenceError(f"Missing DayType: {day_type}")
        weekdays = day_types[day_type]
        valid_from, valid_to = validity.get(day_type, (None, None))
        choices = defaultdict(list)
        for a in assignments.get(day_type, []):
            if sum(bool(a[k]) for k in ("date", "day", "period")) != 1:
                raise SourceReferenceError("Calendar assignment must select one date, day or period")
            specific = bool(a["date"] or a["day"])
            if specific:
                dates = {as_date(a["date"] or days[a["day"]])}
            else:
                p = periods[a["period"]]
                start = as_date(p["from"] or days[p["from_day"]])
                end = as_date(p["to"] or days[p["to_day"]])
                if end < start:
                    raise SourceReferenceError("Reversed operating period")
                dates = {first + timedelta(n) for n in range((last - first).days + 1)
                         if start <= first + timedelta(n) <= end}
            for day in dates:
                if (first <= day <= last and (valid_from is None or str(day) >= valid_from)
                        and (valid_to is None or str(day) <= valid_to)
                        and (specific or not a["available"] or day.weekday() in weekdays)):
                    choices[day].append((2 if specific else 1, a["available"]))
        for day, rules in choices.items():
            priority = max(rank for rank, _ in rules)
            if all(available for rank, available in rules if rank == priority):
                operating.add(day)
            else:
                exceptions.append(str(day))
    return sorted(str(d) for d in operating), sorted(set(exceptions))


class SourceContext:
    """Reuse independent XML indexes/calendars across the four trace cases."""

    def __init__(self, manifest):
        self.manifest, self.indexes, self.calendars = manifest, {}, {}

    def provider(self, provider):
        if provider not in self.indexes:
            files = [f["source_file"] for f in self.manifest["files"] if f["provider"] == provider]
            index, calendar = SourceIndex(files), source_calendar(files)
            self.indexes[provider], self.calendars[provider] = index, calendar
        return self.indexes[provider], self.calendars[provider]


def compare_passing_times(source, silver, physical):
    """Compare facts from independent XML resolution with the ordered Silver rows."""
    checks = {"passing_time_count": (len(source), len(silver)),
              "resolved_stop_count": (len(physical), len(silver))}
    checks["times_match"] = all(s["arrival_seconds"] == t["arrival_seconds"]
                                and s["departure_seconds"] == t["departure_seconds"]
                                for s, t in zip(source, silver))
    checks["pattern_points_match"] = all(s["point_ref"] == t["point_ref"]
                                         and s["point_version"] == t["point_entity_version"]
                                         for s, t in zip(physical, silver))
    checks["physical_stops_match"] = all(
        s["scheduled_stop_ref"] == t["point_scheduled_stop_ref"]
        and s["scheduled_stop_version"] == t["stop_entity_version"]
        and s["quay_ref"] == t["assignment_quay_ref"]
        and s.get("resolved_quay_version") == t["quay_entity_version"]
        and s["stop_place_id"] == t["quay_stop_place_id"]
        and s["flexible_stop_ref"] == t["assignment_flexible_stop_place_ref"]
        for s, t in zip(physical, silver))
    checks["flexible_windows_match"] = all(
        s["earliest_raw"] == t.get("earliest_departure_time_raw")
        and s["latest_raw"] == t.get("latest_arrival_time_raw") for s, t in zip(source, silver))
    checks["flexible_window_fields_present"] = all(
        (s["earliest_raw"] is None or "earliest_departure_time_raw" in t)
        and (s["latest_raw"] is None or "latest_arrival_time_raw" in t) for s, t in zip(source, silver))
    return checks


# --------------------------------------------------------------------------
# Silver/Gold side.

def pick_examples(spark, silver, staging, first, last):
    from pyspark.sql import functions as F
    read = lambda root, manifest, name: spark.read.parquet(str(root / manifest["tables"][name]["path"]))
    s_manifest = json.loads((silver / "metadata/manifest.json").read_text())
    t_manifest = json.loads((staging / "metadata/manifest.json").read_text())
    journeys = read(silver, s_manifest, "journeys")
    times = read(silver, s_manifest, "passing_times")
    dates = read(silver, s_manifest, "journey_service_dates").select("journey_key").distinct()
    late = times.groupBy("journey_key").agg(F.max(F.greatest("arrival_seconds", "departure_seconds")).alias("latest"))
    negative = (read(staging, t_manifest, "day_type_assignments")
                .where(F.lower(F.col("record.isAvailable")) == "false")
                .where(F.col("record.Date").substr(1, 10).between(str(first), str(last)))
                .select("provider", F.col("record.DayTypeRef._ref").alias("day_entity_id")).distinct()
                .withColumn("has_exception", F.lit(True)))
    flags = (read(silver, s_manifest, "journey_day_types").join(negative, ["provider", "day_entity_id"], "left")
             .groupBy("journey_key").agg(F.max(F.coalesce("has_exception", F.lit(False))).alias("has_exception")))
    base = (journeys.join(late, "journey_key", "left").join(flags, "journey_key", "left")
            .join(dates.withColumn("in_window", F.lit(True)), "journey_key", "left"))
    fixed = base.where("is_valid AND NOT is_flexible AND in_window")
    choices = {
        "ordinary": fixed.where("latest < 86400 AND NOT has_exception"),
        "overnight": fixed.where("latest >= 86400"),
        "calendar_exception": fixed.where("has_exception"),
        "flexible": base.where("is_flexible AND in_window"),
    }
    picked = {}
    for kind, frame in choices.items():
        row = frame.orderBy("provider", "entity_id").select("journey_key").first()
        if row:
            picked[kind] = row["journey_key"]
    return picked


def trace(spark, silver, gold, staging, journey_key, kind, context=None):
    from pipeline.storage.zip_bronze import snapshot_xml
    source = json.loads((Path(staging) / "metadata/manifest.json").read_text())
    with snapshot_xml(source["source_manifest"], source["snapshot_date"]):
        return _trace(spark, silver, gold, staging, journey_key, kind, context)


def _trace(spark, silver, gold, staging, journey_key, kind, context=None):
    from pyspark.sql import functions as F
    s_manifest = json.loads((silver / "metadata/manifest.json").read_text())
    g_manifest = json.loads((gold / "metadata/manifest.json").read_text())
    s_read = lambda name: spark.read.parquet(str(silver / s_manifest["tables"][name]["path"]))
    g_read = lambda name: spark.read.parquet(str(gold / g_manifest["tables"][name]["path"]))
    journey = s_read("journeys").where(F.col("journey_key") == journey_key).first()
    times = s_read("passing_times").where(F.col("journey_key") == journey_key).orderBy("passing_time_index").collect()
    dates = sorted(str(r.service_date) for r in s_read("journey_service_dates")
                   .where(F.col("journey_key") == journey_key).collect())
    first, last = date.fromisoformat(s_manifest["service_from"]), date.fromisoformat(s_manifest["service_to"])

    staging_manifest = json.loads((staging / "metadata/manifest.json").read_text())
    files = {f["source_member"]: f["source_file"] for f in staging_manifest["files"]}
    line_file = files[journey.source_member]
    context = context or SourceContext(staging_manifest)
    index, calendar = context.provider(journey.provider)
    src = source_journey(line_file, journey.entity_id, journey.entity_version)
    source_dates, exceptions = operating_dates(src["day_types"], calendar, first, last)
    source_dates = index.bounded_dates(line_file, source_dates)
    physical = index.physical_stops(line_file, (src["pattern"], src["pattern_version"]),
                                   src["passing_times"], [f["source_file"] for f in staging_manifest["stop_files"]])
    checks = compare_passing_times(src["passing_times"], [t.asDict() for t in times], physical)
    descriptor = next(j for j in index.journeys if j["file"] == str(Path(line_file).resolve())
                      and j["id"] == src["id"] and j["version"] == src["version"])
    source_line = index.line(descriptor)
    checks["line_reference_match"] = (source_line["id"] == journey.line_entity_id
                                      and source_line["version"] == journey.line_entity_version)
    checks["operating_dates_match"] = source_dates == dates
    source_counts = index.line_departures(journey.line_entity_id, calendar, operating_dates, first, last)
    line_daily = (g_read("daily_line_service").where(F.col("provider") == journey.provider)
                  .where(F.col("line_entity_id") == journey.line_entity_id).groupBy("service_date")
                  .agg(F.sum("scheduled_departures").alias("n")).collect())
    published_line = {str(r.service_date): r.n for r in line_daily}
    checks["source_line_departures_match_gold"] = source_counts["fixed_departures_by_date"] == published_line
    excluded_line = (g_read("excluded_service_dates").where(F.col("provider") == journey.provider)
                     .where((F.col("line_entity_id") == journey.line_entity_id) & F.col("is_flexible"))
                     .groupBy("service_date").count().collect())
    checks["source_line_flexible_occurrences_match_gold"] = (
        source_counts["flexible_occurrences_by_date"] == {str(r.service_date): r["count"] for r in excluded_line})
    gold_check = {}
    if journey.is_valid and not journey.is_flexible:
        # Gold counts eligible journeys per line and date: recount them from Silver for this line.
        eligible = (s_read("journey_service_dates").where("is_valid AND NOT is_flexible")
                    .where(F.col("line_entity_id") == journey.line_entity_id)
                    .where(F.col("service_date").isin(dates)).groupBy("service_date").count().collect())
        recount = {str(r.service_date): r["count"] for r in eligible}
        daily = (g_read("daily_line_service").where(F.col("line_entity_id") == journey.line_entity_id)
                 .where(F.col("service_date").isin(dates)).groupBy("service_date")
                 .agg(F.sum("scheduled_departures").alias("n")).collect())
        published = {str(r.service_date): r.n for r in daily}
        gold_check = {"line_departures_by_date": published, "silver_recount": recount}
        checks["gold_counts_journey"] = bool(dates) and published == recount and all(d in published for d in dates)
    else:
        excluded = g_read("excluded_service_dates").where(F.col("journey_key") == journey_key).count()
        gold_check = {"excluded_service_dates_rows": excluded}
        checks["gold_excludes_journey"] = excluded == len(dates) and excluded > 0
    ok = all(v if isinstance(v, bool) else v[0] == v[1] for v in checks.values())
    return {"kind": kind, "ok": ok, "journey_id": journey.entity_id, "provider": journey.provider,
            "line": f"{journey.line_entity_id} ({journey.line_public_code}, {journey.line_transport_mode})",
            "source_file": line_file, "source": src, "day_type_exceptions_in_window": exceptions,
            "source_operating_dates": source_dates, "silver_service_dates": dates,
            "silver_passing_times": [{"index": t.passing_time_index, "stop": t.quay_name or t.flex_name,
                                      "stop_place": t.quay_stop_place_id, "arrival_seconds": t.arrival_seconds,
                                      "departure_seconds": t.departure_seconds,
                                      "earliest_raw": t.asDict().get("earliest_departure_time_raw"),
                                      "latest_raw": t.asDict().get("latest_arrival_time_raw")} for t in times],
            "silver_valid": journey.is_valid, "silver_flexible": journey.is_flexible,
            "source_physical_stops": physical,
            "source_line_recount": {**source_counts, "gold_fixed_departures_by_date": published_line},
            "gold": gold_check, "checks": {k: (v if isinstance(v, bool) else list(v)) for k, v in checks.items()}}


def clock(seconds):
    if seconds is None:
        return "–"
    day, rest = divmod(int(seconds), 86400)
    return f"{rest // 3600:02d}:{rest % 3600 // 60:02d}" + (f" (+{day} day)" if day else "")


def markdown(snapshot, window, results):
    out = ["# Traced journeys: XML → Silver → Gold", "",
           f"Snapshot {snapshot}, Gold window {window}. Generated by `pipeline/quality/trace.py`. The source",
           "side is parsed from the original XML with Python's standard library (no Spark, no Silver code);",
           "operating dates are derived directly from DayType, DayTypeAssignment and OperatingPeriod.",
           "Physical stops are independently resolved through pattern points and assignments to quays/stop places",
           "or flexible stop places. All source journey occurrences of the selected line are recounted against Gold.", ""]
    for r in results:
        src = r["source"]
        out += [f"## {r['kind'].replace('_', ' ').capitalize()}: {'✅ consistent' if r['ok'] else '❌ MISMATCH'}", "",
                f"* Journey `{r['journey_id']}` ({src['kind']}), {r['provider']}, line {r['line']}",
                f"* Source file `{Path(r['source_file']).name}`; day types {', '.join(f'`{d}`' for d in src['day_types'])}",
                f"* Calendar exceptions in window (not available): {', '.join(r['day_type_exceptions_in_window']) or 'none'}",
                f"* Operating dates from XML: {', '.join(r['source_operating_dates']) or 'none'}",
                f"* Silver service dates: {', '.join(r['silver_service_dates']) or 'none'}",
                f"* Silver: valid={r['silver_valid']}, flexible={r['silver_flexible']}", "",
                "| # | Physical stop from XML | XML arrival | XML departure | XML flexible window | Silver arrival | Silver departure | Silver flexible window |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |"]
        for s, t, physical in zip(src["passing_times"], r["silver_passing_times"], r["source_physical_stops"]):
            stop = physical["stop_place_id"] or physical["flexible_stop_ref"] or "–"
            window = f"{s['earliest_raw'] or '–'} – {s['latest_raw'] or '–'}" if s["earliest_raw"] or s["latest_raw"] else "–"
            silver_window = f"{t['earliest_raw'] or '–'} – {t['latest_raw'] or '–'}" if t["earliest_raw"] or t["latest_raw"] else "–"
            out.append(f"| {t['index']} | {stop} | {s['arrival_raw'] or '–'} | {s['departure_raw'] or '–'} | {window} | "
                       f"{clock(t['arrival_seconds'])} | {clock(t['departure_seconds'])} | {silver_window} |")
        out += ["", f"Independent whole-line recount: `{json.dumps(r['source_line_recount'], default=str)}`", "",
                f"Gold: `{json.dumps(r['gold'], default=str)}`", "",
                "Checks: " + ", ".join(f"{k} = {v}" for k, v in r["checks"].items()), ""]
    return "\n".join(out)


def main(argv=None):
    from pipeline.analysis.route_history import find_inputs
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--snapshot", default="2026-02-15", help="Snapshot date (default: February, holiday week)")
    parser.add_argument("--journey", action="append", help="Silver journey_key to trace instead of auto-picked ones")
    parser.add_argument("--markdown", type=Path)
    parser.add_argument("--json", type=Path)
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args(argv)
    found = [i for i in find_inputs(args.data_root / "pipeline_state.sqlite3") if i["snapshot_date"] == args.snapshot]
    if not found:
        raise SystemExit(f"No complete verified Gold for snapshot {args.snapshot}")
    item = found[0]
    silver, gold = Path(item["silver"]), Path(item["gold"])
    staging = Path(json.loads((silver / "metadata/manifest.json").read_text())["source_manifest"]).parent.parent
    from pyspark.sql import SparkSession
    if args.master.startswith("local"):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder.appName("DAT535-quality-trace").master(args.master)
             .config("spark.sql.shuffle.partitions", "4").config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        first, last = date.fromisoformat(item["service_from"]), date.fromisoformat(item["service_to"])
        picked = ({f"journey_{n}": key for n, key in enumerate(args.journey, 1)} if args.journey
                  else pick_examples(spark, silver, staging, first, last))
        context = SourceContext(json.loads((staging / "metadata/manifest.json").read_text()))
        results = [trace(spark, silver, gold, staging, key, kind, context) for kind, key in picked.items()]
        for r in results:
            print(f"{r['kind']}: {'consistent' if r['ok'] else 'MISMATCH'} {r['journey_id']} {r['checks']}", flush=True)
        text = markdown(args.snapshot, f"{first} – {last}", results)
        if args.markdown:
            args.markdown.write_text(text + "\n", encoding="utf-8")
        if args.json:
            args.json.write_text(json.dumps(results, indent=2, default=str) + "\n", encoding="utf-8")
        return 0 if results and all(r["ok"] for r in results) else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
