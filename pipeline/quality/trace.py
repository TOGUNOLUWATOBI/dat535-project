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
import xml.etree.ElementTree as ET

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


def source_journey(xml_path, journey_id):
    """The ServiceJourney (or flexible journey) element with this id, parsed from the line file."""
    for _, element in ET.iterparse(xml_path, events=("end",)):
        if element.tag in (NS + "ServiceJourney", NS + "FlexibleServiceJourney") and element.get("id") == journey_id:
            times = []
            for point in element.iter(NS + "TimetabledPassingTime"):
                arrival, departure = text(point, "ArrivalTime"), text(point, "DepartureTime")
                times.append({
                    "point": ref(point, "StopPointInJourneyPatternRef"),
                    "arrival_raw": arrival, "departure_raw": departure,
                    "earliest_raw": text(point, "EarliestDepartureTime"), "latest_raw": text(point, "LatestArrivalTime"),
                    "arrival_seconds": clock_seconds(arrival, text(point, "ArrivalDayOffset")),
                    "departure_seconds": clock_seconds(departure, text(point, "DepartureDayOffset"))})
            return {"id": journey_id, "kind": element.tag.replace(NS, ""),
                    "line": ref(element, "LineRef"), "pattern": ref(element, "JourneyPatternRef"),
                    "day_types": [d.get("ref") for d in element.iter(NS + "DayTypeRef")], "passing_times": times}
    raise LookupError(f"{journey_id} not found in {xml_path}")


def source_calendar(xml_paths):
    """DayTypes, OperatingPeriods, OperatingDays and DayTypeAssignments from the provider's files."""
    day_types, periods, days, assignments = {}, {}, {}, defaultdict(list)
    for path in xml_paths:
        for _, element in ET.iterparse(path, events=("end",)):
            tag = element.tag.replace(NS, "")
            if tag == "DayType":
                weekdays = set()
                tokens = [t for prop in element.iter(NS + "PropertyOfDay") for t in (text(prop, "DaysOfWeek") or "").split()]
                for token in tokens:
                    weekdays |= GROUPS.get(token, {DAYS[token]} if token in DAYS else set())
                day_types[element.get("id")] = weekdays if tokens else set(range(7))
            elif tag == "OperatingPeriod":
                periods[element.get("id")] = {"from": text(element, "FromDate"), "to": text(element, "ToDate"),
                                              "from_day": ref(element, "FromOperatingDayRef"),
                                              "to_day": ref(element, "ToOperatingDayRef")}
            elif tag == "OperatingDay":
                days[element.get("id")] = text(element, "CalendarDate")
            elif tag == "DayTypeAssignment":
                assignments[ref(element, "DayTypeRef")].append({
                    "date": text(element, "Date"), "period": ref(element, "OperatingPeriodRef"),
                    "day": ref(element, "OperatingDayRef"),
                    "available": (text(element, "isAvailable") or "true").lower() != "false"})
            if tag in ("DayType", "OperatingPeriod", "OperatingDay", "DayTypeAssignment"):
                element.clear()
    return day_types, periods, days, assignments


def operating_dates(day_type_ids, calendar, first, last):
    """Dates in [first, last] on which any of the day types operates, and the exceptions applied."""
    day_types, periods, days, assignments = calendar
    as_date = lambda value: date.fromisoformat(value[:10])
    operating, exceptions = set(), []
    for day_type in day_type_ids:
        weekdays = day_types.get(day_type, set(range(7)))
        positive, negative = set(), set()
        for a in assignments.get(day_type, []):
            if a["date"] or a["day"]:
                dates = {as_date(a["date"] or days[a["day"]])}
            else:
                p = periods[a["period"]]
                start = as_date(p["from"] or days[p["from_day"]])
                end = as_date(p["to"] or days[p["to_day"]])
                dates = {start + timedelta(n) for n in range((end - start).days + 1)
                         if (start + timedelta(n)).weekday() in weekdays}
            (positive if a["available"] else negative).update(dates)
        window = {d for d in positive - negative if first <= d <= last}
        operating |= window
        exceptions += sorted(str(d) for d in negative if first <= d <= last)
    return sorted(str(d) for d in operating), sorted(set(exceptions))


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


def trace(spark, silver, gold, staging, journey_key, kind):
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
    shared = [path for member, path in files.items()
              if member.startswith(f"_{journey.provider}_") and "shared" in member]
    src = source_journey(line_file, journey.entity_id)
    source_dates, exceptions = operating_dates(src["day_types"], source_calendar(shared + [line_file]), first, last)

    checks = {}
    checks["passing_time_count"] = (len(src["passing_times"]), len(times))
    seconds_ok = all(
        (s["arrival_seconds"] == t.arrival_seconds or (s["arrival_seconds"] is None and t.arrival_seconds is None))
        and (s["departure_seconds"] == t.departure_seconds
             or (s["departure_seconds"] is None and t.departure_seconds is None))
        for s, t in zip(src["passing_times"], times))
    checks["times_match"] = seconds_ok
    checks["operating_dates_match"] = source_dates == dates
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
            "line": f"{journey.line_entity_id} (bus {journey.line_public_code}, {journey.line_transport_mode})",
            "source_file": line_file, "source": src, "day_type_exceptions_in_window": exceptions,
            "source_operating_dates": source_dates, "silver_service_dates": dates,
            "silver_passing_times": [{"index": t.passing_time_index, "stop": t.quay_name or t.flex_name,
                                      "stop_place": t.quay_stop_place_id, "arrival_seconds": t.arrival_seconds,
                                      "departure_seconds": t.departure_seconds} for t in times],
            "silver_valid": journey.is_valid, "silver_flexible": journey.is_flexible,
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
           "operating dates are derived directly from DayType, DayTypeAssignment and OperatingPeriod.", ""]
    for r in results:
        src = r["source"]
        out += [f"## {r['kind'].replace('_', ' ').capitalize()}: {'✅ consistent' if r['ok'] else '❌ MISMATCH'}", "",
                f"* Journey `{r['journey_id']}` ({src['kind']}), {r['provider']}, line {r['line']}",
                f"* Source file `{Path(r['source_file']).name}`; day types {', '.join(f'`{d}`' for d in src['day_types'])}",
                f"* Calendar exceptions in window (not available): {', '.join(r['day_type_exceptions_in_window']) or 'none'}",
                f"* Operating dates from XML: {', '.join(r['source_operating_dates']) or 'none'}",
                f"* Silver service dates: {', '.join(r['silver_service_dates']) or 'none'}",
                f"* Silver: valid={r['silver_valid']}, flexible={r['silver_flexible']}", "",
                "| # | Stop (Silver) | XML arrival | XML departure | Silver arrival | Silver departure |",
                "| --- | --- | --- | --- | --- | --- |"]
        for s, t in zip(src["passing_times"], r["silver_passing_times"]):
            arrival = s["arrival_raw"] or s["earliest_raw"] or "–"
            departure = s["departure_raw"] or s["latest_raw"] or "–"
            out.append(f"| {t['index']} | {t['stop'] or '–'} | {arrival} | {departure} | "
                       f"{clock(t['arrival_seconds'])} | {clock(t['departure_seconds'])} |")
        out += ["", f"Gold: `{json.dumps(r['gold'], default=str)}`", "",
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
        results = [trace(spark, silver, gold, staging, key, kind) for kind, key in picked.items()]
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
