"""Historical route-change detection across validated snapshots (EPIC 06, T38–T43; T46–T47).

Reads the completed Silver and Gold outputs of every snapshot recorded in the
checkpoint registry and compares consecutive snapshots line by line:

* which lines appeared in or disappeared from the published exports;
* bus-number (public code), name and transport-mode changes;
* stops added/removed and changed stop sequences (journey patterns);
* service-level changes in the snapshot's Gold window (departures, first/last bus);
* a Type 2 slowly changing dimension of line versions;
* renumbering candidates, joined with a manually verified mapping file;
* network stop coverage per snapshot and stops that gained or lost service;
* every Gold service date linked to the line version valid in that snapshot (T43).

Run on the school server from the project root (see scripts/run_route_history.sh):

    bash scripts/run_route_history.sh --data-root /srv/dat535/data

Interpretation rules
--------------------
* Snapshots are observations of what was *published* on a date, not of when a
  change took effect for passengers. An export can announce a future timetable
  or correct an earlier one. Versions therefore carry ``first_observed`` and
  ``last_observed`` (snapshot dates), and ``previous_snapshot`` (the change was
  published after it). They have no effective-date fields: the Silver calendar
  is expanded only inside each Gold window, so the source does not give one here.
* A line missing from the next export is ``disappeared`` ("no longer
  observed"). Calling it a renumbering or discontinuation is an interpretation
  that needs a verified source (config/route_renumberings.json,
  config/route_discontinuations.json); see ``line_transitions.interpretation``.
* Line identity is ``(provider, NeTEx line id)``. Line IDs that change while
  the service stays the same are reported as candidates, not merged silently.
* Stops are measured two ways: stops in the line's published journey patterns
  (``stop_ids``, route structure) and stops served by fixed journeys that
  operate inside the Gold window (``window_stop_ids``, the same scope as the
  departure totals). A pattern can be valid without any journey in the window.
* All transport modes are included (bus and water); bus-only departures are
  reported separately in ``change_summary`` and ``network_snapshots``.
* Route structure (bus number, name, mode, kind, stop set, stop sequences)
  drives SCD versions. Departure counts are service levels, so they produce
  change events but not new line versions.
* Stop identity is the national stop place (NSR:StopPlace); quay/platform
  swaps inside one stop place are not route changes.
* Everything describes published planned timetables, not buses that ran.
"""

import argparse
from contextlib import contextmanager
from datetime import date
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import sys
from time import perf_counter
from uuid import uuid4

from pipeline.checkpoints import digest, identity, output_inventory
from pipeline.ingestion.generate_provider_dataset import save_json
from pipeline.processing.bronze.xml_to_parquet import utc_now
from pipeline.processing.windows import window_signature

SCHEMA_VERSION = 2
TRACKED = ["public_codes", "names", "transport_modes", "line_kinds", "stop_set_hash", "pattern_set_hash"]
STRUCTURAL = ["line_appeared", "line_disappeared", "public_code_changed", "name_changed",
              "transport_mode_changed", "stops_added", "stops_removed", "patterns_changed"]
MAPPING_KEYS = {"provider", "old_line_id", "new_line_id", "from_snapshot", "to_snapshot",
                "verified", "verified_by", "note", "source_url"}
DISCONTINUATION_KEYS = {"provider", "line_id", "from_snapshot", "to_snapshot",
                        "verified", "verified_by", "note", "source_url"}
DEFAULTS = {"frequency_threshold": 0.2, "min_departure_change": 5,
            "span_threshold_minutes": 30, "renumber_similarity": 0.6, "renumber_lookback": 1,
            "same_code_similarity": 0.2}


class HistoryInputError(RuntimeError):
    """The registry does not hold a usable, consistent set of snapshots."""


# --------------------------------------------------------------------------
# Inputs: completed Gold (and its Silver) for each snapshot, verified by hash.

def find_inputs(state_path, dates=None, verify=True):
    """Return the newest complete, hash-verified Gold+Silver pair per snapshot date."""
    state_path = Path(state_path)
    if not state_path.is_file():
        raise HistoryInputError(f"Checkpoint registry not found: {state_path}")
    db = sqlite3.connect(f"file:{state_path.resolve()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        rows = db.execute("""SELECT s.snapshot_date, s.providers, a.output_path, a.inventory_json,
                a.manifest_sha256, a.finished_at FROM snapshots s JOIN stage_attempts a USING(snapshot_key)
                WHERE a.stage='gold' AND a.status='complete' ORDER BY s.snapshot_date, a.finished_at DESC""").fetchall()
        silver_rows = {r["output_path"]: r for r in db.execute(
            "SELECT output_path, inventory_json, manifest_sha256 FROM stage_attempts "
            "WHERE stage='silver' AND status='complete'").fetchall()}
    finally:
        db.close()
    wanted = {date.fromisoformat(str(d)).isoformat() for d in dates} if dates else None
    chosen, problems = {}, []
    for row in rows:
        day = row["snapshot_date"]
        if day in chosen or (wanted and day not in wanted):
            continue
        gold = Path(row["output_path"])
        try:
            if verify and output_inventory(gold) != json.loads(row["inventory_json"]):
                raise HistoryInputError("Gold files differ from their checkpoint hashes")
            gold_manifest = json.loads((gold / "metadata/manifest.json").read_text())
            if gold_manifest.get("history_role") == "calendar_support":
                continue
            silver = Path(gold_manifest["source_manifest"]).parent.parent
            silver_row = silver_rows.get(str(silver))
            if silver_row is None:
                raise HistoryInputError(f"Silver output is not a complete checkpoint: {silver}")
            if verify and output_inventory(silver) != json.loads(silver_row["inventory_json"]):
                raise HistoryInputError("Silver files differ from their checkpoint hashes")
            silver_manifest = json.loads((silver / "metadata/manifest.json").read_text())
        except (OSError, ValueError, KeyError, RuntimeError) as error:
            problems.append(f"{day}: {error}")
            continue
        chosen[day] = {"snapshot_date": day, "providers": sorted(gold_manifest["providers"]),
                       "gold": str(gold), "silver": str(silver),
                       "gold_manifest_sha256": row["manifest_sha256"],
                       "silver_manifest_sha256": silver_row["manifest_sha256"],
                       "service_from": silver_manifest["service_from"],
                       "service_to": silver_manifest["service_to"]}
    if wanted and wanted - set(chosen):
        problems.append(f"No complete verified Gold for: {sorted(wanted - set(chosen))}")
    if problems:
        raise HistoryInputError("; ".join(problems))
    inputs = [chosen[d] for d in sorted(chosen)]
    if len(inputs) < 2:
        raise HistoryInputError(f"Route history needs at least two completed snapshots; found {len(inputs)}")
    providers = {tuple(i["providers"]) for i in inputs}
    if len(providers) != 1:
        raise HistoryInputError(f"Snapshots use different provider selections: {sorted(providers)}")
    windows = {window_signature(i) for i in inputs}
    if len(windows) != 1:
        raise HistoryInputError("Snapshots use different Gold service windows; departures would not be comparable")
    return inputs


# --------------------------------------------------------------------------
# Spark transformations.

def _empty_strings():
    from pyspark.sql import functions as F
    return F.array().cast("array<string>")


def _arr(column):
    from pyspark.sql import functions as F
    return F.coalesce(F.col(column), _empty_strings())


def load_line_snapshots(spark, inputs):
    """One row per (snapshot, provider, line id) plus pattern and stop detail tables."""
    from functools import reduce
    from pyspark.sql import functions as F
    lines_all, patterns_all, names_all, daily_all, checks = [], [], [], [], []
    for item in inputs:
        silver, gold = Path(item["silver"]), Path(item["gold"])
        s_manifest = json.loads((silver / "metadata/manifest.json").read_text())
        g_manifest = json.loads((gold / "metadata/manifest.json").read_text())
        def s(name): return spark.read.parquet(str(silver / s_manifest["tables"][name]["path"]))
        def g(name): return spark.read.parquet(str(gold / g_manifest["tables"][name]["path"]))
        day = F.lit(item["snapshot_date"])

        lines = s("lines").select("provider", F.col("entity_id").alias("line_id"), "name", "public_code",
                                  "transport_mode", "line_kind")
        attrs = lines.groupBy("provider", "line_id").agg(
            F.sort_array(F.collect_set("public_code")).alias("public_codes"),
            F.sort_array(F.collect_set("name")).alias("names"),
            F.sort_array(F.collect_set("transport_mode")).alias("transport_modes"),
            F.sort_array(F.collect_set("line_kind")).alias("line_kinds"),
            F.count("*").alias("line_occurrences"))

        journeys = s("journeys").where("is_valid AND line_entity_id IS NOT NULL").select(
            "journey_key", "provider", F.col("line_entity_id").alias("line_id"))
        times = s("passing_times").where("is_valid").select(
            "journey_key", "passing_time_index",
            F.coalesce("quay_stop_place_id", "assignment_flexible_stop_place_ref", "assignment_quay_ref",
                       "point_scheduled_stop_ref", F.lit("?")).alias("stop_id"))
        sequences = times.groupBy("journey_key").agg(
            F.sort_array(F.collect_list(F.struct("passing_time_index", "stop_id"))).alias("points"))
        sequences = sequences.select("journey_key", F.transform("points", lambda p: p["stop_id"]).alias("stop_sequence"))
        sequences = sequences.withColumn("pattern_signature",
                                         F.substring(F.sha2(F.array_join("stop_sequence", ">"), 256), 1, 16))
        patterns = (journeys.join(sequences, "journey_key", "inner")
                    .groupBy("provider", "line_id", "pattern_signature")
                    .agg(F.first("stop_sequence").alias("stop_sequence"), F.count("*").alias("journeys")))
        structure = patterns.groupBy("provider", "line_id").agg(
            F.sort_array(F.collect_set("pattern_signature")).alias("pattern_signatures"),
            F.sort_array(F.array_distinct(F.flatten(F.collect_list("stop_sequence")))).alias("stop_ids"),
            F.sum("journeys").alias("journeys_published"))
        # Stops actually served in the Gold window: fixed, valid journeys with an operating
        # date inside it (the same eligibility as the Gold departure totals).
        operating = s("journey_service_dates").where("is_valid AND NOT is_flexible").select(
            "journey_key", "provider", F.col("line_entity_id").alias("line_id")).distinct()
        window_stops = operating.join(sequences.select("journey_key", "stop_sequence"), "journey_key").groupBy(
            "provider", "line_id").agg(
            F.sort_array(F.array_distinct(F.flatten(F.collect_list("stop_sequence")))).alias("window_stop_ids"))

        window = g("line_window_summary").groupBy("provider", F.col("line_entity_id").alias("line_id")).agg(
            F.sum("scheduled_departures").alias("window_departures"),
            F.max("days_with_service").alias("days_with_service"),
            F.min("earliest_departure_seconds").alias("first_departure_seconds"),
            F.max("latest_departure_seconds").alias("last_departure_seconds"))

        daily_all.append(g("daily_line_service").groupBy("provider", F.col("line_entity_id").alias("line_id"), "service_date")
            .agg(F.sum("scheduled_departures").alias("scheduled_departures"),
                 F.min("first_departure_seconds").alias("first_departure_seconds"),
                 F.max("last_departure_seconds").alias("last_departure_seconds"))
            .withColumn("snapshot_date", day))
        frame = (attrs.join(structure, ["provider", "line_id"], "left")
                 .join(window_stops, ["provider", "line_id"], "left")
                 .join(window, ["provider", "line_id"], "left")
                 .withColumn("snapshot_date", day))
        lines_all.append(frame)
        patterns_all.append(patterns.withColumn("snapshot_date", day))
        stop_names = s("stop_places").select(F.col("stop_place_id").alias("stop_id"), "name")
        if "flexible_stop_places" in s_manifest["tables"]:
            stop_names = stop_names.unionByName(s("flexible_stop_places").select(
                F.col("entity_id").alias("stop_id"), "name"))
        names_all.append(stop_names.where("name IS NOT NULL").withColumn("snapshot_date", day))

        gold_total = g("line_window_summary").agg(F.coalesce(F.sum("scheduled_departures"), F.lit(0))).first()[0]
        silver_lines = lines.select("provider", "line_id").distinct().count()
        orphans = window.join(attrs, ["provider", "line_id"], "left_anti").count()
        checks.append({"snapshot_date": item["snapshot_date"], "gold_departures": int(gold_total),
                       "silver_distinct_lines": silver_lines, "gold_lines_without_silver_line": orphans})

    union = lambda frames: reduce(lambda a, b: a.unionByName(b), frames)
    return (finish_lines(union(lines_all)), union(patterns_all),
            union(names_all).dropDuplicates(["snapshot_date", "stop_id"]), union(daily_all), checks)


def finish_lines(lines):
    """Fill missing arrays/counts and add the display columns and structure hashes."""
    from pyspark.sql import functions as F
    lines = (lines
        .withColumn("public_codes", _arr("public_codes")).withColumn("names", _arr("names"))
        .withColumn("transport_modes", _arr("transport_modes")).withColumn("line_kinds", _arr("line_kinds"))
        .withColumn("pattern_signatures", _arr("pattern_signatures")).withColumn("stop_ids", _arr("stop_ids"))
        .withColumn("window_stop_ids", _arr("window_stop_ids"))
        .withColumn("window_stop_count", F.size("window_stop_ids"))
        .withColumn("is_bus", F.array_contains("transport_modes", "bus"))
        .withColumn("journeys_published", F.coalesce("journeys_published", F.lit(0)).cast("long"))
        .withColumn("window_departures", F.coalesce("window_departures", F.lit(0)).cast("long"))
        .withColumn("days_with_service", F.coalesce("days_with_service", F.lit(0)).cast("long"))
        .withColumn("public_code", F.array_join("public_codes", "/"))
        .withColumn("name", F.array_join("names", " / "))
        .withColumn("transport_mode", F.array_join("transport_modes", "/"))
        .withColumn("stop_count", F.size("stop_ids"))
        .withColumn("pattern_count", F.size("pattern_signatures"))
        .withColumn("stop_set_hash", F.sha2(F.array_join("stop_ids", ","), 256))
        .withColumn("pattern_set_hash", F.sha2(F.array_join("pattern_signatures", ","), 256))
        .withColumn("has_conflicting_definitions",
                    (F.size("public_codes") > 1) | (F.size("names") > 1) | (F.size("transport_modes") > 1)))
    return lines.withColumn("attribute_hash", F.sha2(F.to_json(F.struct(*TRACKED)), 256))


def detect_changes(spark, lines, dates, options):
    """Compare consecutive snapshots; returns (transitions, changes)."""
    from pyspark.sql import functions as F
    pairs = spark.createDataFrame(list(zip(dates[:-1], dates[1:])), ["from_snapshot", "to_snapshot"])
    keep = ["provider", "line_id", "public_codes", "public_code", "name", "names", "transport_mode",
            "transport_modes", "stop_ids", "pattern_signatures", "pattern_set_hash", "window_departures",
            "first_departure_seconds", "last_departure_seconds", "line_kinds", "is_bus", "window_stop_ids"]
    before = lines.join(pairs, lines.snapshot_date == pairs.from_snapshot).select(
        "from_snapshot", "to_snapshot", *[F.col(c).alias(("b_" + c) if c not in ("provider", "line_id") else c) for c in keep])
    after = lines.join(pairs, lines.snapshot_date == pairs.to_snapshot).select(
        "from_snapshot", "to_snapshot", *[F.col(c).alias(("a_" + c) if c not in ("provider", "line_id") else c) for c in keep])
    before = before.withColumn("b_present", F.lit(True))
    after = after.withColumn("a_present", F.lit(True))
    joined = before.join(after, ["from_snapshot", "to_snapshot", "provider", "line_id"], "full_outer")
    joined = joined.withColumn("b_present", F.coalesce("b_present", F.lit(False))) \
                   .withColumn("a_present", F.coalesce("a_present", F.lit(False)))
    both = F.col("b_present") & F.col("a_present")
    b_dep, a_dep = F.coalesce("b_window_departures", F.lit(0)), F.coalesce("a_window_departures", F.lit(0))
    diff = a_dep - b_dep
    relative = F.when(b_dep > 0, F.abs(diff) / b_dep)
    frequency = (both & (b_dep > 0) & (a_dep > 0) & (F.abs(diff) >= options["min_departure_change"])
                 & (relative >= options["frequency_threshold"]))
    span = options["span_threshold_minutes"] * 60
    first_moved = both & (F.abs(F.col("a_first_departure_seconds") - F.col("b_first_departure_seconds")) >= span)
    last_moved = both & (F.abs(F.col("a_last_departure_seconds") - F.col("b_last_departure_seconds")) >= span)
    stops_added = F.array_except(_arr("a_stop_ids"), _arr("b_stop_ids"))
    stops_removed = F.array_except(_arr("b_stop_ids"), _arr("a_stop_ids"))
    patterns_added = F.array_except(_arr("a_pattern_signatures"), _arr("b_pattern_signatures"))
    patterns_removed = F.array_except(_arr("b_pattern_signatures"), _arr("a_pattern_signatures"))
    none = _empty_strings()
    s = lambda c: F.col(c).cast("string")

    def change(condition, kind, old, new, items=None):
        return F.when(condition, F.struct(F.lit(kind).alias("change_type"), old.alias("old_value"),
                                          new.alias("new_value"), (items if items is not None else none).alias("items")))
    events = F.array(
        change(F.col("a_present") & ~F.col("b_present"), "line_appeared", F.lit(None).cast("string"), s("a_public_code"), _arr("a_stop_ids")),
        change(F.col("b_present") & ~F.col("a_present"), "line_disappeared", s("b_public_code"), F.lit(None).cast("string"), _arr("b_stop_ids")),
        change(both & (F.col("b_public_code") != F.col("a_public_code")), "public_code_changed", s("b_public_code"), s("a_public_code")),
        change(both & (F.col("b_name") != F.col("a_name")), "name_changed", s("b_name"), s("a_name")),
        change(both & (F.col("b_transport_mode") != F.col("a_transport_mode")), "transport_mode_changed",
               s("b_transport_mode"), s("a_transport_mode")),
        change(both & (F.size(stops_added) > 0), "stops_added", F.size(_arr("b_stop_ids")).cast("string"),
               F.size(_arr("a_stop_ids")).cast("string"), stops_added),
        change(both & (F.size(stops_removed) > 0), "stops_removed", F.size(_arr("b_stop_ids")).cast("string"),
               F.size(_arr("a_stop_ids")).cast("string"), stops_removed),
        change(both & (F.col("b_pattern_set_hash") != F.col("a_pattern_set_hash")), "patterns_changed",
               F.size(_arr("b_pattern_signatures")).cast("string"), F.size(_arr("a_pattern_signatures")).cast("string"),
               F.concat(F.transform(patterns_added, lambda p: F.concat(F.lit("+"), p)),
                        F.transform(patterns_removed, lambda p: F.concat(F.lit("-"), p)))),
        change(both & (b_dep == 0) & (a_dep > 0), "service_started", b_dep.cast("string"), a_dep.cast("string")),
        change(both & (b_dep > 0) & (a_dep == 0), "service_stopped", b_dep.cast("string"), a_dep.cast("string")),
        change(frequency & (diff > 0), "departures_increased", b_dep.cast("string"), a_dep.cast("string")),
        change(frequency & (diff < 0), "departures_decreased", b_dep.cast("string"), a_dep.cast("string")),
        change(first_moved, "first_departure_changed", s("b_first_departure_seconds"), s("a_first_departure_seconds")),
        change(last_moved, "last_departure_changed", s("b_last_departure_seconds"), s("a_last_departure_seconds")))
    joined = joined.withColumn("events", F.filter(events, lambda e: e.isNotNull()))
    structural = F.array(*[F.lit(k) for k in STRUCTURAL])
    joined = joined.withColumn("change_types", F.transform("events", lambda e: e["change_type"]))
    joined = joined.withColumn("structural_change", F.exists("change_types", lambda t: F.array_contains(structural, t)))
    joined = joined.withColumn("status", F.when(~F.col("b_present"), "appeared").when(~F.col("a_present"), "disappeared")
                               .when(F.col("structural_change"), "changed")
                               .when(F.size("change_types") > 0, "service_changed").otherwise("unchanged"))
    transitions = joined.select(
        "from_snapshot", "to_snapshot", "provider", "line_id", "status", "change_types", "structural_change",
        F.col("b_public_code").alias("public_code_before"), F.col("a_public_code").alias("public_code_after"),
        F.col("b_name").alias("name_before"), F.col("a_name").alias("name_after"),
        F.size(_arr("b_stop_ids")).alias("stops_before"), F.size(_arr("a_stop_ids")).alias("stops_after"),
        F.size(stops_added).alias("stops_added"), F.size(stops_removed).alias("stops_removed"),
        F.size(_arr("b_pattern_signatures")).alias("patterns_before"),
        F.size(_arr("a_pattern_signatures")).alias("patterns_after"),
        F.col("b_window_departures").alias("departures_before"), F.col("a_window_departures").alias("departures_after"),
        F.when(F.col("b_present") & F.col("a_present"), diff).alias("departure_change"),
        F.round(F.when(F.col("b_present") & F.col("a_present"), relative * F.signum(diff)), 4).alias("departure_change_ratio"),
        F.col("b_stop_ids").alias("stop_ids_before"), F.col("a_stop_ids").alias("stop_ids_after"),
        F.col("b_line_kinds").alias("line_kinds_before"), F.col("a_line_kinds").alias("line_kinds_after"),
        F.col("b_transport_modes").alias("transport_modes_before"), F.col("a_transport_modes").alias("transport_modes_after"),
        F.coalesce("a_is_bus", "b_is_bus").alias("is_bus"),
        F.col("b_window_stop_ids").alias("window_stop_ids_before"), F.col("a_window_stop_ids").alias("window_stop_ids_after"))
    changes = joined.select("from_snapshot", "to_snapshot", "provider", "line_id",
                            F.col("b_public_code").alias("public_code_before"),
                            F.col("a_public_code").alias("public_code_after"),
                            F.explode("events").alias("event"))
    changes = changes.select("from_snapshot", "to_snapshot", "provider", "line_id", "public_code_before",
                             "public_code_after", "event.change_type",
                             F.when(F.array_contains(structural, F.col("event.change_type")), "structure")
                              .otherwise("service").alias("change_category"),
                             "event.old_value", "event.new_value", "event.items",
                             F.size("event.items").alias("item_count"))
    return transitions, changes


def build_versions(lines, dates, verified=None):
    """Type 2 slowly changing dimension of line structure across snapshots.

    Dates are observation dates: ``first_observed``/``last_observed`` are the first and
    last snapshots that published this version, ``previous_snapshot`` the snapshot before
    (the change was published after it; empty for the first snapshot of the series, when
    the version may be older) and ``first_not_observed`` the next snapshot without it.
    """
    from pyspark.sql import functions as F, Window
    index = F.array_position(F.array(*[F.lit(d) for d in dates]), F.col("snapshot_date")).cast("int") - 1
    at = lambda i: F.element_at(F.array(*[F.lit(d) for d in dates]), i + 1)
    order = Window.partitionBy("provider", "line_id").orderBy("snapshot_index")
    x = lines.withColumn("snapshot_index", index)
    x = x.withColumn("prev_hash", F.lag("attribute_hash").over(order)).withColumn("prev_index", F.lag("snapshot_index").over(order))
    reason = (F.when(F.col("prev_hash").isNull(), "first_observed")
              .when(F.col("prev_index") != F.col("snapshot_index") - 1, "reappeared")
              .when(F.col("prev_hash") != F.col("attribute_hash"), "structure_changed"))
    x = x.withColumn("version_reason", reason)
    x = x.withColumn("version_number", F.sum(F.col("version_reason").isNotNull().cast("int")).over(
        order.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    attrs = ["public_code", "name", "transport_mode", "public_codes", "names", "transport_modes", "line_kinds",
             "stop_ids", "pattern_signatures", "stop_count", "pattern_count", "is_bus", "attribute_hash"]
    versions = x.groupBy("provider", "line_id", "version_number").agg(
        F.min("snapshot_index").alias("first_index"), F.max("snapshot_index").alias("last_index"),
        F.count("*").alias("snapshots_observed"), F.max("version_reason").alias("version_reason"),
        F.round(F.avg("window_departures"), 2).alias("average_window_departures"),
        *[F.first(c).alias(c) for c in attrs])
    last = len(dates) - 1
    versions = (versions
        .withColumn("first_observed", at(F.col("first_index")))
        .withColumn("previous_snapshot", F.when(F.col("first_index") > 0, at(F.col("first_index") - 1)))
        .withColumn("last_observed", at(F.col("last_index")))
        .withColumn("first_not_observed", F.when(F.col("last_index") < last, at(F.col("last_index") + 1)))
        .withColumn("observed_in_latest", F.col("last_index") == last)
        .withColumn("version_key", F.substring(F.sha2(F.concat_ws("|", "provider", "line_id",
                                                                  F.col("version_number").cast("string")), 256), 1, 16)))
    if verified is not None:
        # Continuity across a verified renumbering that changed the line ID.
        # The new line may have been published before the old one disappeared, so the link
        # goes to the new line's version that is current in the snapshot where the old one is gone.
        # Several old lines merged into one new line are all listed (comma-separated).
        links = verified.where("verified AND old_line_id != new_line_id").groupBy(
            F.col("provider").alias("l_provider"), F.col("new_line_id").alias("l_line_id"),
            F.col("to_snapshot").alias("l_to")).agg(
            F.concat_ws(",", F.sort_array(F.collect_set("old_line_id"))).alias("renumbered_from_line_id"))
        joined = versions.join(links, (versions.provider == links.l_provider) & (versions.line_id == links.l_line_id)
                               & (versions.first_observed <= links.l_to) & (versions.last_observed >= links.l_to), "left")
        versions = joined.dropDuplicates(["provider", "line_id", "version_number"]).drop("l_provider", "l_line_id", "l_to")
    else:
        versions = versions.withColumn("renumbered_from_line_id", F.lit(None).cast("string"))
    covered = x.select("provider", "line_id", "snapshot_date", "version_number")
    return versions.drop("first_index", "last_index"), covered


def service_versions(daily, covered, versions):
    """T43: attach each Gold service date to the line version of its snapshot.

    A service date belongs to the snapshot that published it, so it is joined
    on (provider, line, snapshot). Inner joins would hide unmatched rows, so a
    left join is used and the caller checks that every row found a version.
    """
    from pyspark.sql import functions as F
    keys = versions.select("provider", "line_id", "version_number", "version_key", "public_code",
                           "first_observed", "last_observed")
    joined = daily.join(covered, ["provider", "line_id", "snapshot_date"], "left").join(
        keys, ["provider", "line_id", "version_number"], "left")
    return joined.select("snapshot_date", "service_date", "provider", "line_id", "version_number", "version_key",
                         F.col("public_code").alias("version_public_code"), "first_observed", "last_observed",
                         "scheduled_departures", "first_departure_seconds", "last_departure_seconds")


def renumbering_candidates(spark, transitions, options, mapping_rows, lines=None, dates=None):
    """Possible renumberings/re-identifications, with manual verification joined in.

    Operators often publish a replacement line (or a line's new bus number) one
    snapshot before the old line disappears. A disappeared line is therefore compared
    with every line in the next snapshot whose current bus number first appeared
    at most ``renumber_lookback`` snapshots earlier, not only with lines added in
    the same transition. ``new_line_since`` records when that number first appeared.

    A shared bus number alone is weak evidence, because operators reuse numbers
    for different routes (Kolumbus, July 2026: old line 1 became 9 while the
    former line A took number 1). Same-number pairs therefore also need a stop
    similarity of at least ``same_code_similarity``.
    """
    from pyspark.sql import functions as F
    def jaccard(a, b):
        union = F.size(F.array_union(_arr(a), _arr(b)))
        return F.when(union > 0, F.round(F.size(F.array_intersect(_arr(a), _arr(b))) / union, 4)).otherwise(F.lit(0.0))
    same_id = transitions.where(F.array_contains("change_types", "public_code_changed")).select(
        "from_snapshot", "to_snapshot", "provider", F.col("line_id").alias("old_line_id"),
        F.col("line_id").alias("new_line_id"), F.col("public_code_before").alias("old_public_code"),
        F.col("public_code_after").alias("new_public_code"), F.lit("same_line_id_new_code").alias("match_type"),
        jaccard("stop_ids_before", "stop_ids_after").alias("stop_similarity"),
        F.col("to_snapshot").alias("new_line_since"))
    retired = transitions.where("status='disappeared'").select(
        "from_snapshot", "to_snapshot", "provider", F.col("line_id").alias("old_line_id"),
        F.col("public_code_before").alias("old_public_code"), F.col("stop_ids_before").alias("old_stops"),
        F.col("transport_modes_before").alias("old_modes"))
    if lines is None or dates is None:
        added = transitions.where("status='appeared'").select(
            "from_snapshot", "to_snapshot", "provider", F.col("line_id").alias("new_line_id"),
            F.col("public_code_after").alias("new_public_code"), F.col("stop_ids_after").alias("new_stops"),
            F.col("transport_modes_after").alias("new_modes"), F.col("to_snapshot").alias("new_line_since"))
    else:
        lookback = max(1, int(options["renumber_lookback"]))
        windows = spark.createDataFrame(
            [(dates[i], dates[i + 1], dates[max(0, i + 1 - lookback)]) for i in range(len(dates) - 1)],
            ["from_snapshot", "to_snapshot", "earliest"])
        since = lines.groupBy("provider", "line_id", "public_code").agg(F.min("snapshot_date").alias("new_line_since"))
        current = lines.join(windows, lines.snapshot_date == windows.to_snapshot).join(
            since, ["provider", "line_id", "public_code"])
        added = current.where(F.col("new_line_since") >= F.col("earliest")).select(
            "from_snapshot", "to_snapshot", "provider", F.col("line_id").alias("new_line_id"),
            F.col("public_code").alias("new_public_code"), F.col("stop_ids").alias("new_stops"),
            F.col("transport_modes").alias("new_modes"), "new_line_since")
    pairs = retired.join(added, ["from_snapshot", "to_snapshot", "provider"]).where("old_line_id != new_line_id")
    pairs = pairs.where(F.arrays_overlap(_arr("old_modes"), _arr("new_modes")) | (F.size(_arr("old_modes")) == 0))
    pairs = pairs.withColumn("stop_similarity", jaccard("old_stops", "new_stops"))
    pairs = pairs.withColumn("match_type", F.when(F.col("old_public_code") == F.col("new_public_code"),
                                                  "same_code_new_line_id").otherwise("similar_stops"))
    threshold = F.when(F.col("match_type") == "same_code_new_line_id", F.lit(float(options["same_code_similarity"]))) \
                 .otherwise(F.lit(float(options["renumber_similarity"])))
    pairs = pairs.where(F.col("stop_similarity") >= threshold)
    columns = ["from_snapshot", "to_snapshot", "provider", "old_line_id", "new_line_id", "old_public_code",
               "new_public_code", "match_type", "stop_similarity", "new_line_since"]
    candidates = same_id.select(*columns).unionByName(pairs.select(*columns))
    mappings = spark.createDataFrame(mapping_rows, MAPPING_SCHEMA)
    # A mapping applies only to the transition it names: both endpoints must match when given.
    # Candidates without a matching mapping stay in the table as unverified.
    m = mappings.select(*[F.col(c).alias("m_" + c) for c in mappings.columns])
    applies = ((candidates.provider == m.m_provider) & (candidates.old_line_id == m.m_old_line_id)
               & (candidates.new_line_id == m.m_new_line_id)
               & (m.m_mapping_to_snapshot.isNull() | (m.m_mapping_to_snapshot == candidates.to_snapshot))
               & (m.m_mapping_from_snapshot.isNull() | (m.m_mapping_from_snapshot == candidates.from_snapshot)))
    candidates = candidates.join(m, applies, "left").select(
        *columns, F.coalesce("m_verified", F.lit(False)).alias("verified"),
        F.col("m_verified_by").alias("verified_by"), F.col("m_note").alias("note"))
    if lines is not None and dates is not None:
        candidates = candidates.unionByName(_mapped_only(spark, lines, dates, mappings, candidates, jaccard))
    return candidates


def _mapped_only(spark, lines, dates, mappings, candidates, jaccard):
    """Verified mappings the automatic rules did not find (e.g. a replacement with a new number
    and a shortened route). The operator's announcement is the evidence, so they are added with
    ``match_type='verified_mapping'`` when the old line exists just before ``to_snapshot`` and the
    new line exists in it. Mappings without ``to_snapshot`` cannot be placed and stay unmatched."""
    from pyspark.sql import functions as F
    pairs = spark.createDataFrame(list(zip(dates[:-1], dates[1:])), ["from_snapshot", "mapping_to_snapshot"])
    keys = ["provider", "old_line_id", "new_line_id"]
    matched = candidates.where("verified").select(*keys, F.col("to_snapshot").alias("mapping_to_snapshot"))
    missing = (mappings.where("verified").join(matched, keys + ["mapping_to_snapshot"], "left_anti")
               .join(pairs, "mapping_to_snapshot")
               .where(F.col("mapping_from_snapshot").isNull() | (F.col("mapping_from_snapshot") == F.col("from_snapshot"))))
    old = lines.select("provider", F.col("line_id").alias("old_line_id"), F.col("snapshot_date").alias("from_snapshot"),
                       F.col("public_code").alias("old_public_code"), F.col("stop_ids").alias("old_stops"))
    since = lines.groupBy("provider", "line_id", "public_code").agg(F.min("snapshot_date").alias("new_line_since"))
    new = lines.join(since, ["provider", "line_id", "public_code"]).select(
        "provider", F.col("line_id").alias("new_line_id"), F.col("snapshot_date").alias("mapping_to_snapshot"),
        F.col("public_code").alias("new_public_code"), F.col("stop_ids").alias("new_stops"), "new_line_since")
    found = missing.join(old, ["provider", "old_line_id", "from_snapshot"]).join(
        new, ["provider", "new_line_id", "mapping_to_snapshot"])
    return found.select("provider", "old_line_id", "new_line_id", "from_snapshot",
                        F.col("mapping_to_snapshot").alias("to_snapshot"), "old_public_code", "new_public_code",
                        F.lit("verified_mapping").alias("match_type"),
                        jaccard("old_stops", "new_stops").alias("stop_similarity"), "new_line_since",
                        "verified", "verified_by", "note").select(*candidates.columns)


MEASURES = {"definitions": "stop_ids", "window_service": "window_stop_ids"}


def stop_coverage(lines, stop_names):
    """Stop coverage per snapshot under two measures.

    ``definitions``: the stop is in a published journey pattern of the line.
    ``window_service``: a fixed journey of the line serves the stop on a date in the
    Gold window. Each change row names its ``measure``.
    """
    from functools import reduce
    from pyspark.sql import functions as F
    keys = ["snapshot_date", "provider", "stop_id"]
    per_measure = []
    for measure, column in MEASURES.items():
        per_measure.append(lines.select("snapshot_date", "provider", "line_id", "public_code",
                                        F.explode(column).alias("stop_id"))
                           .groupBy(*keys).agg(F.countDistinct("line_id").alias(f"lines_{measure}"),
                                               F.sort_array(F.collect_set("public_code")).alias(f"line_codes_{measure}")))
    coverage = reduce(lambda a, b: a.join(b, keys, "full_outer"), per_measure)
    coverage = coverage.fillna(0, [f"lines_{m}" for m in MEASURES])
    for measure in MEASURES:
        coverage = coverage.withColumn(f"line_codes_{measure}", _arr(f"line_codes_{measure}"))
    coverage = coverage.join(stop_names, ["snapshot_date", "stop_id"], "left").withColumnRenamed("name", "stop_name")
    return coverage


def stop_coverage_changes(coverage, dates, spark):
    """Compare the saved coverage table, without repeating its definition/name joins."""
    from functools import reduce
    from pyspark.sql import functions as F
    pairs = spark.createDataFrame(list(zip(dates[:-1], dates[1:])), ["from_snapshot", "to_snapshot"])
    changes = []
    for measure in MEASURES:
        present = coverage.where(F.col(f"lines_{measure}") > 0)
        b = present.join(pairs, present.snapshot_date == pairs.from_snapshot).select(
            "from_snapshot", "to_snapshot", "provider", "stop_id", F.col("stop_name").alias("b_name"),
            F.col(f"lines_{measure}").alias("lines_before"), F.col(f"line_codes_{measure}").alias("line_codes_before"))
        a = present.join(pairs, present.snapshot_date == pairs.to_snapshot).select(
            "from_snapshot", "to_snapshot", "provider", "stop_id", F.col("stop_name").alias("a_name"),
            F.col(f"lines_{measure}").alias("lines_after"), F.col(f"line_codes_{measure}").alias("line_codes_after"))
        diff = b.join(a, ["from_snapshot", "to_snapshot", "provider", "stop_id"], "full_outer")
        diff = diff.withColumn("change_type", F.when(F.col("lines_before").isNull(), "stop_gained")
                               .when(F.col("lines_after").isNull(), "stop_lost")
                               .when(F.col("line_codes_before") != F.col("line_codes_after"), "serving_lines_changed"))
        changes.append(diff.where("change_type IS NOT NULL").select(
            "from_snapshot", "to_snapshot", "provider", "stop_id", F.coalesce("a_name", "b_name").alias("stop_name"),
            F.lit(measure).alias("measure"), "change_type",
            F.coalesce("lines_before", F.lit(0)).alias("lines_before"),
            F.coalesce("lines_after", F.lit(0)).alias("lines_after"),
            _arr("line_codes_before").alias("line_codes_before"), _arr("line_codes_after").alias("line_codes_after")))
    return reduce(lambda x, y: x.unionByName(y), changes)


def configure_history_session(spark):
    """Keep automatic hash-table broadcasts out of this history session's driver heap.

    AQE has a separate threshold, so disabling only the static planner is not
    sufficient when the caller supplies an adaptive broadcast setting.
    """
    for key in ("spark.sql.autoBroadcastJoinThreshold", "spark.sql.adaptive.autoBroadcastJoinThreshold"):
        spark.conf.set(key, "-1")


def interpret_transitions(spark, transitions, candidates, discontinuation_rows):
    """Add ``interpretation``: what a line appearing or disappearing means, and on what evidence.

    disappeared: ``renumbered_verified`` / ``discontinued_verified`` when a verified source
    covers that transition, otherwise ``not_observed`` (no claim about why).
    appeared: ``replacement_verified`` when the line is the new side of a verified mapping
    and first appeared in this transition, otherwise ``first_observed``.
    """
    from pyspark.sql import functions as F
    verified = candidates.where("verified")
    renumbered = verified.select("provider", F.col("old_line_id").alias("line_id"), "to_snapshot").distinct() \
        .withColumn("renumbered", F.lit(True))
    replacement = verified.select("provider", F.col("new_line_id").alias("line_id"),
                                  F.col("new_line_since").alias("to_snapshot")).distinct() \
        .withColumn("replacement", F.lit(True))
    stops = spark.createDataFrame(discontinuation_rows, DISCONTINUATION_SCHEMA).where("verified").select(
        "provider", "line_id", "to_snapshot", "from_snapshot").withColumnRenamed("from_snapshot", "d_from")
    keys = ["provider", "line_id", "to_snapshot"]
    stops = stops.withColumn("listed", F.lit(True))
    out = (transitions.join(renumbered, keys, "left").join(replacement, keys, "left").join(stops, keys, "left")
           # The entry covers the transition ending at to_snapshot; from_snapshot must also match if given.
           .withColumn("discontinued", F.coalesce("listed", F.lit(False))
                       & (F.col("d_from").isNull() | (F.col("d_from") == F.col("from_snapshot")))))
    interpretation = (F.when((F.col("status") == "disappeared") & F.coalesce("renumbered", F.lit(False)),
                             "renumbered_verified")
                      .when((F.col("status") == "disappeared") & F.coalesce("discontinued", F.lit(False)),
                            "discontinued_verified")
                      .when(F.col("status") == "disappeared", "not_observed")
                      .when((F.col("status") == "appeared") & F.coalesce("replacement", F.lit(False)),
                            "replacement_verified")
                      .when(F.col("status") == "appeared", "first_observed"))
    return out.withColumn("interpretation", interpretation).select(*transitions.columns, "interpretation")


def summaries(lines, transitions, coverage, stop_changes):
    from pyspark.sql import functions as F
    network = lines.groupBy("snapshot_date", "provider", "transport_mode").agg(
        F.count("*").alias("lines_published"),
        F.sum((F.col("window_departures") > 0).cast("int")).alias("lines_with_window_service"),
        F.sum("window_departures").alias("window_departures"),
        F.sum("journeys_published").alias("journeys_published"))
    count_stops = lambda measure: F.sum((F.col(f"lines_{measure}") > 0).cast("int"))
    network = network.join(coverage.groupBy("snapshot_date", "provider").agg(
        count_stops("definitions").alias("provider_stops_in_definitions"),
        count_stops("window_service").alias("provider_stops_served_in_window")), ["snapshot_date", "provider"], "left")
    has = lambda kind: F.sum(F.array_contains("change_types", kind).cast("int"))
    status = lambda value: F.sum((F.col("status") == value).cast("int"))
    meaning = lambda value: F.sum(F.coalesce(F.col("interpretation") == value, F.lit(False)).cast("int"))
    bus = lambda column: F.coalesce(F.sum(F.when(F.col("is_bus"), F.col(column))), F.lit(0))
    summary = transitions.groupBy("from_snapshot", "to_snapshot", "provider").agg(
        F.sum((F.col("status") != "appeared").cast("int")).alias("lines_before"),
        F.sum((F.col("status") != "disappeared").cast("int")).alias("lines_after"),
        status("appeared").alias("lines_appeared"),
        status("disappeared").alias("lines_disappeared"),
        meaning("renumbered_verified").alias("lines_disappeared_renumbered_verified"),
        meaning("discontinued_verified").alias("lines_disappeared_discontinued_verified"),
        meaning("not_observed").alias("lines_disappeared_unexplained"),
        status("changed").alias("lines_structure_changed"),
        status("service_changed").alias("lines_service_changed_only"),
        status("unchanged").alias("lines_unchanged"),
        has("public_code_changed").alias("lines_public_code_changed"),
        has("stops_added").alias("lines_with_stops_added"), has("stops_removed").alias("lines_with_stops_removed"),
        has("patterns_changed").alias("lines_with_pattern_changes"),
        has("departures_increased").alias("lines_departures_increased"),
        has("departures_decreased").alias("lines_departures_decreased"),
        F.coalesce(F.sum("departures_before"), F.lit(0)).alias("window_departures_before"),
        F.coalesce(F.sum("departures_after"), F.lit(0)).alias("window_departures_after"),
        bus("departures_before").alias("bus_window_departures_before"),
        bus("departures_after").alias("bus_window_departures_after"))
    counts = {}
    for measure in MEASURES:
        for kind in ("stop_gained", "stop_lost"):
            counts[f"{kind}_{measure}"] = F.sum(((F.col("measure") == measure)
                                                 & (F.col("change_type") == kind)).cast("int"))
    stops = stop_changes.groupBy("from_snapshot", "to_snapshot", "provider").agg(
        *[expr.alias(name.replace("stop_gained", "stops_gained").replace("stop_lost", "stops_lost"))
          for name, expr in counts.items()])
    names = [n.replace("stop_gained", "stops_gained").replace("stop_lost", "stops_lost") for n in counts]
    summary = summary.join(stops, ["from_snapshot", "to_snapshot", "provider"], "left").fillna(0, names)
    return network, summary


# --------------------------------------------------------------------------
# Evidence files: verified renumberings and discontinuations.

MAPPING_SCHEMA = ("provider string, old_line_id string, new_line_id string, verified boolean, verified_by string, "
                  "note string, mapping_from_snapshot string, mapping_to_snapshot string")
DISCONTINUATION_SCHEMA = ("provider string, line_id string, verified boolean, verified_by string, note string, "
                          "from_snapshot string, to_snapshot string")


def _read_evidence(path, kind, required, allowed):
    """Validate a JSON list of evidence entries.

    ``verified`` must be a JSON boolean and defaults to false. A verified entry must say
    who verified it (``verified_by``), what the evidence is (``note``, ``source_url``) and
    which transition it covers (``to_snapshot``; ``from_snapshot`` optional). Unknown keys
    are refused so a misspelt ``verified`` cannot silently change the meaning.
    """
    if path is None or not Path(path).is_file():
        return [], None
    try:
        entries = json.loads(Path(path).read_text(encoding="utf-8"))
    except ValueError as error:
        raise HistoryInputError(f"{kind} file {path} is not valid JSON: {error}") from error
    if not isinstance(entries, list):
        raise HistoryInputError(f"{kind} file {path} must contain a JSON list")
    seen = set()
    for number, entry in enumerate(entries, 1):
        where = f"{kind} entry {number}"
        if not isinstance(entry, dict):
            raise HistoryInputError(f"{where} must be a JSON object")
        unknown = sorted(set(entry) - allowed)
        if unknown:
            raise HistoryInputError(f"{where} has unknown fields {unknown}")
        missing = [k for k in required if not isinstance(entry.get(k), str) or not entry[k].strip()]
        if missing:
            raise HistoryInputError(f"{where} is missing {missing}")
        verified = entry.get("verified", False)
        if not isinstance(verified, bool):
            raise HistoryInputError(f"{where}: 'verified' must be true or false, not {verified!r}")
        if verified:
            absent = [k for k in ("verified_by", "note", "source_url", "to_snapshot")
                      if not isinstance(entry.get(k), str) or not entry[k].strip()]
            if absent:
                raise HistoryInputError(f"{where} is marked verified but has no {absent}")
            if not entry["source_url"].startswith(("https://", "http://")):
                raise HistoryInputError(f"{where}: source_url must be a web address")
        for field in ("from_snapshot", "to_snapshot"):
            if entry.get(field) is not None:
                try:
                    date.fromisoformat(entry[field])
                except (TypeError, ValueError) as error:
                    raise HistoryInputError(f"{where}: {field} must be YYYY-MM-DD") from error
        if entry.get("from_snapshot") and entry.get("to_snapshot") and entry["from_snapshot"] >= entry["to_snapshot"]:
            raise HistoryInputError(f"{where}: from_snapshot must be before to_snapshot")
        for field in required[1:]:
            if not entry[field].startswith(entry["provider"] + ":"):
                raise HistoryInputError(f"{where}: {field} {entry[field]!r} does not belong to {entry['provider']}")
        key = tuple(entry.get(k) for k in (*required, "from_snapshot", "to_snapshot"))
        if key in seen:
            raise HistoryInputError(f"{where} duplicates an earlier entry")
        seen.add(key)
    return entries, digest(path)


def read_mappings(path):
    """Renumberings/replacements (config/route_renumberings.json); unverified unless ``verified: true``."""
    entries, sha = _read_evidence(path, "Renumbering", ("provider", "old_line_id", "new_line_id"), MAPPING_KEYS)
    return [(e["provider"], e["old_line_id"], e["new_line_id"], e.get("verified", False), e.get("verified_by"),
             e.get("note"), e.get("from_snapshot"), e.get("to_snapshot")) for e in entries], sha


def read_discontinuations(path):
    """Discontinued lines (config/route_discontinuations.json); unverified unless ``verified: true``."""
    entries, sha = _read_evidence(path, "Discontinuation", ("provider", "line_id"), DISCONTINUATION_KEYS)
    return [(e["provider"], e["line_id"], e.get("verified", False), e.get("verified_by"), e.get("note"),
             e.get("from_snapshot"), e.get("to_snapshot")) for e in entries], sha


def build_history(spark, inputs, output, options=None, mappings_path=None, identity_key=None,
                  discontinuations_path=None):
    from pyspark.sql import functions as F
    started = perf_counter()
    configure_history_session(spark)
    options = {**DEFAULTS, **(options or {})}
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / "metadata").mkdir()
    dates = [i["snapshot_date"] for i in inputs]
    mapping_rows, mapping_sha = read_mappings(mappings_path)
    discontinuation_rows, discontinuation_sha = read_discontinuations(discontinuations_path)
    report = {"schema_version": SCHEMA_VERSION, "layer": "gold_history", "status": "in_progress",
              "identity": identity_key,
              "started_at_utc": utc_now(), "snapshots": inputs, "options": options,
              "renumbering_mappings": {"path": str(mappings_path) if mappings_path else None,
                                       "sha256": mapping_sha, "entries": len(mapping_rows)},
              "discontinuations": {"path": str(discontinuations_path) if discontinuations_path else None,
                                   "sha256": discontinuation_sha, "entries": len(discontinuation_rows)},
              "meaning": ("Differences between consecutive published planned-timetable exports (observation "
                          "dates). A difference was published after the earlier snapshot and by the later one; "
                          "it is not an effective date. Disappeared lines are only called renumbered or "
                          "discontinued with a verified source. All transport modes are included; bus-only "
                          "departures are reported separately."),
              "runtime": {"spark_version": spark.version, "master": spark.sparkContext.master,
                          "driver_heap_max_bytes": spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory(),
                          "sql": {key: spark.conf.get(key) for key in (
                              "spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled",
                              "spark.sql.autoBroadcastJoinThreshold", "spark.sql.adaptive.autoBroadcastJoinThreshold")}},
              "tables": {}, "checks": {}}
    path = output / "metadata/manifest.json"
    save_json(path, report)

    def write(name, frame, partitions=1):
        from pyspark import StorageLevel
        target = output / name
        started = perf_counter()
        # Count and write the same materialization, rather than evaluating a large
        # lazy join plan twice. Cache blocks go to Spark's temporary disk storage
        # and are released after this table, including on a failed write.
        frame = frame.persist(StorageLevel.DISK_ONLY)
        try:
            expected = frame.count()
            frame.coalesce(partitions).write.mode("errorifexists").option("compression", "snappy").parquet(str(target))
            saved = spark.read.parquet(str(target))
            if saved.count() != expected:
                raise RuntimeError(f"{name}: read-back count mismatch")
        finally:
            frame.unpersist(blocking=True)
        elapsed = round(perf_counter() - started, 3)
        report["tables"][name] = {"path": name, "rows": expected, "schema": json.loads(saved.schema.json()),
                                  "parquet_bytes": sum(p.stat().st_size for p in target.glob("*.parquet")),
                                  "elapsed_seconds": elapsed}
        save_json(path, report)
        print(f"history/{name}: {expected:,} rows ({elapsed:.1f}s)", flush=True)
        return saved

    try:
        lines, patterns, stop_names, daily, input_checks = load_line_snapshots(spark, inputs)
        lines = write("line_snapshots", lines.select(
            "snapshot_date", "provider", "line_id", "public_code", "name", "transport_mode", "public_codes", "names",
            "transport_modes", "line_kinds", "line_occurrences", "has_conflicting_definitions", "journeys_published",
            "pattern_count", "pattern_signatures", "stop_count", "stop_ids", "window_stop_count", "window_stop_ids",
            "is_bus", "window_departures", "days_with_service", "first_departure_seconds", "last_departure_seconds",
            "stop_set_hash", "pattern_set_hash", "attribute_hash"), 2)
        write("line_patterns", patterns.select("snapshot_date", "provider", "line_id", "pattern_signature",
                                               "stop_sequence", F.size("stop_sequence").alias("stops"), "journeys"), 2)
        observed, changes = detect_changes(spark, lines, dates, options)
        changes = write("line_changes", changes)
        candidates = write("renumbering_candidates",
                           renumbering_candidates(spark, observed, options, mapping_rows, lines, dates))
        transitions = write("line_transitions",
                            interpret_transitions(spark, observed, candidates, discontinuation_rows), 2)
        versions, covered = build_versions(lines, dates, candidates)
        versions = write("line_versions", versions)
        dated = write("line_service_versions", service_versions(daily, covered, versions), 2)
        # Materialize first, then derive changes from the read-back table. Keeping
        # the original lazy plan here repeats all stop-definition/name joins in
        # each before/after branch and can exhaust the driver's broadcast heap.
        coverage = write("stop_coverage", stop_coverage(lines, stop_names), 2)
        stop_changes = write("stop_coverage_changes", stop_coverage_changes(coverage, dates, spark))
        network, summary = summaries(lines, transitions, coverage, stop_changes)
        write("network_snapshots", network)
        summary = write("change_summary", summary)

        # ---- Reconciliation checks: failures stop the run (status=failed).
        checks, failures = report["checks"], []
        per_day = {r["snapshot_date"]: r for r in lines.groupBy("snapshot_date").agg(
            F.count("*").alias("lines"), F.sum("window_departures").alias("departures")).collect()}
        for item in input_checks:
            day = per_day[item["snapshot_date"]]
            ok = (day["departures"] == item["gold_departures"] and day["lines"] == item["silver_distinct_lines"]
                  and item["gold_lines_without_silver_line"] == 0)
            checks.setdefault("snapshots", []).append({**item, "history_lines": day["lines"],
                                                       "history_departures": int(day["departures"]), "ok": ok})
            if not ok:
                failures.append(f"{item['snapshot_date']}: line/departure totals differ from Silver/Gold")
        unbalanced = summary.where("lines_after - lines_before != lines_appeared - lines_disappeared").count()
        checks["transition_balance_failures"] = unbalanced
        if unbalanced:
            failures.append("Appeared/disappeared counts do not explain line count changes")
        # Window stops come from journeys operating in the window; they must be a subset of the
        # published pattern stops, and present exactly when the line has window departures.
        outside = lines.where(F.size(F.array_except("window_stop_ids", "stop_ids")) > 0).count()
        scope = lines.where((F.col("window_departures") > 0) != (F.col("window_stop_count") > 0)).count()
        checks["window_stops_outside_definitions"] = outside
        checks["window_stop_scope_mismatches"] = scope
        if outside or scope:
            failures.append("Window-served stops do not agree with definitions or departures")
        checks["lines_published_without_window_service"] = lines.where("window_departures = 0").count()
        version_rows = versions.agg(F.coalesce(F.sum("snapshots_observed"), F.lit(0))).first()[0]
        multi = covered.groupBy("provider", "line_id", "snapshot_date").count().where("count > 1").count()
        checks["versions_cover_every_line_snapshot"] = version_rows == lines.count() and multi == 0
        if not checks["versions_cover_every_line_snapshot"]:
            failures.append("SCD versions do not cover each line snapshot exactly once")
        expected_events = transitions.agg(F.coalesce(F.sum(F.size("change_types")), F.lit(0))).first()[0]
        checks["change_rows_match_transitions"] = expected_events == changes.count()
        if not checks["change_rows_match_transitions"]:
            failures.append("Exploded change rows differ from transition change lists")
        unversioned = dated.where("version_key IS NULL").count()
        dated_total = dated.agg(F.coalesce(F.sum("scheduled_departures"), F.lit(0))).first()[0]
        gold_total = sum(item["gold_departures"] for item in input_checks)
        checks["service_dates_without_version"] = unversioned
        checks["service_date_departures"] = {"gold": gold_total, "versioned": int(dated_total)}
        if unversioned or dated_total != gold_total:
            failures.append("Gold service dates are not each linked to exactly one line version")
        checks["lines_with_conflicting_definitions"] = lines.where("has_conflicting_definitions").count()
        if mapping_rows:
            wanted = {(r[0], r[1], r[2]) for r in mapping_rows if r[3]}
            matched = {(r.provider, r.old_line_id, r.new_line_id) for r in candidates.where("verified").collect()}
            checks["verified_mappings_matched"] = len(wanted & matched)
            checks["verified_mappings_unmatched"] = sorted("|".join(k) for k in wanted - matched)
            if wanted - matched:
                failures.append(f"Verified renumberings not found in the data: {checks['verified_mappings_unmatched']}")
        if discontinuation_rows:
            wanted = {(r[0], r[1]) for r in discontinuation_rows if r[2]}
            matched = {(r.provider, r.line_id) for r in transitions.where("interpretation = 'discontinued_verified'")
                       .select("provider", "line_id").collect()}
            checks["verified_discontinuations_matched"] = len(wanted & matched)
            checks["verified_discontinuations_unmatched"] = sorted("|".join(k) for k in wanted - matched)
            if wanted - matched:
                failures.append("Verified discontinuations not found as disappearances: "
                                f"{checks['verified_discontinuations_unmatched']}")
        checks["interpretations"] = {r["interpretation"]: r["count"] for r in transitions.where(
            "interpretation IS NOT NULL").groupBy("interpretation").count().collect()}
        if failures:
            raise RuntimeError("; ".join(failures))
        report["summary"] = [r.asDict() for r in summary.orderBy("from_snapshot", "provider").collect()]
        report.update(status="complete", completed_at_utc=utc_now(), elapsed_seconds=round(perf_counter() - started, 3))
        save_json(path, report)
        inventory = output_inventory(output)
        save_json(output / "metadata/inventory.json", inventory)
        return report
    except Exception as error:
        report.update(status="failed", completed_at_utc=utc_now(), elapsed_seconds=round(perf_counter() - started, 3),
                      error=f"{type(error).__name__}: {error}")
        save_json(path, report)
        raise


# --------------------------------------------------------------------------
# Command line: input identity, reuse of identical completed runs, locking.

@contextmanager
def history_lock(root):
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("Another route-history run is using this data root") from error
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def run_identity(inputs, options, mapping_sha, discontinuation_sha=None):
    code = digest(Path(__file__).resolve())
    return identity({"schema": SCHEMA_VERSION, "code": code, "options": options, "mappings": mapping_sha,
                     "discontinuations": discontinuation_sha,
                     "inputs": [(i["snapshot_date"], i["gold_manifest_sha256"], i["silver_manifest_sha256"])
                                for i in inputs]})


def reusable(directory, key):
    for candidate in sorted(directory.glob("history-*"), reverse=True):
        try:
            manifest = json.loads((candidate / "metadata/manifest.json").read_text())
            saved = json.loads((candidate / "metadata/inventory.json").read_text())
            if manifest.get("status") == "complete" and manifest.get("identity") == key \
                    and output_inventory(candidate) == saved:
                return candidate
        except (OSError, ValueError, KeyError, RuntimeError):
            continue
    return None


def run(spark, data_root, state=None, dates=None, options=None, mappings_path=None, force=False,
        discontinuations_path=None):
    data_root = Path(data_root).resolve()
    options = {**DEFAULTS, **(options or {})}
    inputs = find_inputs(state or data_root / "pipeline_state.sqlite3", dates)
    _, mapping_sha = read_mappings(mappings_path)
    _, discontinuation_sha = read_discontinuations(discontinuations_path)
    key = run_identity(inputs, options, mapping_sha, discontinuation_sha)
    root = data_root / "gold" / "entur" / "history"
    with history_lock(root):
        directory = root / key[:16]
        previous = None if force else reusable(directory, key) if directory.exists() else None
        if previous:
            print(f"route history: reuse {previous}", flush=True)
            return previous, json.loads((previous / "metadata/manifest.json").read_text()), True
        output = directory / f"history-{uuid4().hex[:12]}"
        report = build_history(spark, inputs, output, options, mappings_path, key, discontinuations_path)
        return output, report, False


def print_summary(report):
    print("\nObserved differences between exports (+appeared / -disappeared; stops = served in the Gold window)")
    print("from        to          prov  lines  +app  -dis  explained  changed  +stops  -stops  departures (bus)")
    for r in report.get("summary", []):
        explained = r["lines_disappeared_renumbered_verified"] + r["lines_disappeared_discontinued_verified"]
        print(f"{r['from_snapshot']}  {r['to_snapshot']}  {r['provider']:4}  {r['lines_after']:5}  "
              f"{r['lines_appeared']:4}  {r['lines_disappeared']:4}  {explained:9}  {r['lines_structure_changed']:7}  "
              f"{r['stops_gained_window_service']:6}  {r['stops_lost_window_service']:6}  "
              f"{r['window_departures_before']}→{r['window_departures_after']} "
              f"({r['bus_window_departures_before']}→{r['bus_window_departures_after']})")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-root", type=Path, required=True, help="e.g. /srv/dat535/data")
    parser.add_argument("--state", type=Path, help="Checkpoint registry (default: DATA_ROOT/pipeline_state.sqlite3)")
    parser.add_argument("--dates", nargs="+", help="Only these snapshot dates (at least two)")
    parser.add_argument("--renumberings", type=Path, default=Path("config/route_renumberings.json"),
                        help="Renumbering/replacement evidence (optional file)")
    parser.add_argument("--discontinuations", type=Path, default=Path("config/route_discontinuations.json"),
                        help="Discontinuation evidence (optional file)")
    parser.add_argument("--frequency-threshold", type=float, default=DEFAULTS["frequency_threshold"])
    parser.add_argument("--min-departure-change", type=int, default=DEFAULTS["min_departure_change"])
    parser.add_argument("--span-threshold-minutes", type=int, default=DEFAULTS["span_threshold_minutes"])
    parser.add_argument("--renumber-similarity", type=float, default=DEFAULTS["renumber_similarity"])
    parser.add_argument("--renumber-lookback", type=int, default=DEFAULTS["renumber_lookback"],
                        help="Snapshots before the removal in which a replacement may already be published")
    parser.add_argument("--same-code-similarity", type=float, default=DEFAULTS["same_code_similarity"],
                        help="Minimum stop similarity for an old and new line sharing a bus number")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--force", action="store_true", help="Build a new output even if an identical one exists")
    args = parser.parse_args(argv)
    options = {"frequency_threshold": args.frequency_threshold, "min_departure_change": args.min_departure_change,
               "span_threshold_minutes": args.span_threshold_minutes, "renumber_similarity": args.renumber_similarity,
               "renumber_lookback": args.renumber_lookback, "same_code_similarity": args.same_code_similarity}
    spark = None
    try:
        from pyspark.sql import SparkSession
        if args.master.startswith("local"):
            os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        spark = (SparkSession.builder.appName("DAT535-route-history").master(args.master)
                 .config("spark.sql.session.timeZone", "Europe/Oslo").config("spark.sql.shuffle.partitions", "4")
                 .config("spark.ui.showConsoleProgress", "false").getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        output, report, reused = run(spark, args.data_root, args.state, args.dates, options, args.renumberings,
                                     args.force, args.discontinuations)
        print(f"route history {'reused' if reused else 'complete'}: {output}")
        print_summary(report)
        return 0
    except HistoryInputError as error:
        print(f"Route history refused: {error}", file=sys.stderr)
        return 2
    except Exception as error:
        print(f"Route history failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
