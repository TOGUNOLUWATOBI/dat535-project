"""Historical route-change detection across validated snapshots (EPIC 06, T38–T43; T46–T47).

Reads the completed Silver and Gold outputs of every snapshot recorded in the
checkpoint registry and compares consecutive snapshots line by line:

* which lines were added or retired;
* bus-number (public code), name and transport-mode changes;
* stops added/removed and changed stop sequences (journey patterns);
* service-level changes in the snapshot's Gold window (departures, first/last bus);
* a Type 2 slowly changing dimension of line versions;
* renumbering candidates, joined with a manually verified mapping file;
* network stop coverage per snapshot and stops that gained or lost service.

Run on the school server from the project root (see scripts/run_route_history.sh):

    bash scripts/run_route_history.sh --data-root /srv/dat535/data

Interpretation rules
--------------------
* Snapshots are monthly observations. A change detected between 15 January
  and 15 February happened somewhere in (15 Jan, 15 Feb]; it is not dated
  more precisely. Versions record ``changed_after`` and ``valid_from`` for this.
* Line identity is ``(provider, NeTEx line id)``. Line IDs that change while
  the service stays the same are reported as candidates, not merged silently.
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
from uuid import uuid4

from pipeline.checkpoints import digest, identity, output_inventory
from pipeline.ingestion.generate_provider_dataset import save_json
from pipeline.processing.bronze.xml_to_parquet import utc_now

SCHEMA_VERSION = 1
TRACKED = ["public_codes", "names", "transport_modes", "line_kinds", "stop_set_hash", "pattern_set_hash"]
STRUCTURAL = ["line_added", "line_retired", "public_code_changed", "name_changed",
              "transport_mode_changed", "stops_added", "stops_removed", "patterns_changed"]
DEFAULTS = {"frequency_threshold": 0.2, "min_departure_change": 5,
            "span_threshold_minutes": 30, "renumber_similarity": 0.6}


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
    windows = {(date.fromisoformat(i["service_to"]) - date.fromisoformat(i["service_from"])).days + 1
               for i in inputs}
    offsets = {(date.fromisoformat(i["service_from"]) - date.fromisoformat(i["snapshot_date"])).days
               for i in inputs}
    if len(windows) != 1 or len(offsets) != 1:
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
    lines_all, patterns_all, names_all, checks = [], [], [], []
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

        window = g("line_window_summary").groupBy("provider", F.col("line_entity_id").alias("line_id")).agg(
            F.sum("scheduled_departures").alias("window_departures"),
            F.max("days_with_service").alias("days_with_service"),
            F.min("earliest_departure_seconds").alias("first_departure_seconds"),
            F.max("latest_departure_seconds").alias("last_departure_seconds"))

        frame = (attrs.join(structure, ["provider", "line_id"], "left")
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
    lines = union(lines_all)
    lines = (lines
        .withColumn("public_codes", _arr("public_codes")).withColumn("names", _arr("names"))
        .withColumn("transport_modes", _arr("transport_modes")).withColumn("line_kinds", _arr("line_kinds"))
        .withColumn("pattern_signatures", _arr("pattern_signatures")).withColumn("stop_ids", _arr("stop_ids"))
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
    lines = lines.withColumn("attribute_hash", F.sha2(F.to_json(F.struct(*TRACKED)), 256))
    return lines, union(patterns_all), union(names_all).dropDuplicates(["snapshot_date", "stop_id"]), checks


def detect_changes(spark, lines, dates, options):
    """Compare consecutive snapshots; returns (transitions, changes)."""
    from pyspark.sql import functions as F
    pairs = spark.createDataFrame(list(zip(dates[:-1], dates[1:])), ["from_snapshot", "to_snapshot"])
    keep = ["provider", "line_id", "public_codes", "public_code", "name", "names", "transport_mode",
            "transport_modes", "stop_ids", "pattern_signatures", "pattern_set_hash", "window_departures",
            "first_departure_seconds", "last_departure_seconds", "line_kinds"]
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
        change(F.col("a_present") & ~F.col("b_present"), "line_added", F.lit(None).cast("string"), s("a_public_code"), _arr("a_stop_ids")),
        change(F.col("b_present") & ~F.col("a_present"), "line_retired", s("b_public_code"), F.lit(None).cast("string"), _arr("b_stop_ids")),
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
    joined = joined.withColumn("status", F.when(~F.col("b_present"), "added").when(~F.col("a_present"), "retired")
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
        F.col("b_transport_modes").alias("transport_modes_before"), F.col("a_transport_modes").alias("transport_modes_after"))
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
    """Type 2 slowly changing dimension of line structure across snapshots."""
    from pyspark.sql import functions as F, Window
    index = F.array_position(F.array(*[F.lit(d) for d in dates]), F.col("snapshot_date")).cast("int") - 1
    at = lambda i: F.element_at(F.array(*[F.lit(d) for d in dates]), i + 1)
    order = Window.partitionBy("provider", "line_id").orderBy("snapshot_index")
    x = lines.withColumn("snapshot_index", index)
    x = x.withColumn("prev_hash", F.lag("attribute_hash").over(order)).withColumn("prev_index", F.lag("snapshot_index").over(order))
    reason = (F.when(F.col("prev_hash").isNull(), "first_seen")
              .when(F.col("prev_index") != F.col("snapshot_index") - 1, "reappeared")
              .when(F.col("prev_hash") != F.col("attribute_hash"), "structure_changed"))
    x = x.withColumn("version_reason", reason)
    x = x.withColumn("version_number", F.sum(F.col("version_reason").isNotNull().cast("int")).over(
        order.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    attrs = ["public_code", "name", "transport_mode", "public_codes", "names", "transport_modes", "line_kinds",
             "stop_ids", "pattern_signatures", "stop_count", "pattern_count", "attribute_hash"]
    versions = x.groupBy("provider", "line_id", "version_number").agg(
        F.min("snapshot_index").alias("first_index"), F.max("snapshot_index").alias("last_index"),
        F.count("*").alias("snapshots_observed"), F.max("version_reason").alias("version_reason"),
        F.round(F.avg("window_departures"), 2).alias("average_window_departures"),
        *[F.first(c).alias(c) for c in attrs])
    last = len(dates) - 1
    versions = (versions
        .withColumn("valid_from", at(F.col("first_index")))
        .withColumn("changed_after", F.when(F.col("first_index") > 0, at(F.col("first_index") - 1)))
        .withColumn("last_observed", at(F.col("last_index")))
        .withColumn("superseded_in", F.when(F.col("last_index") < last, at(F.col("last_index") + 1)))
        .withColumn("is_current", F.col("last_index") == last)
        .withColumn("version_key", F.substring(F.sha2(F.concat_ws("|", "provider", "line_id",
                                                                  F.col("version_number").cast("string")), 256), 1, 16)))
    if verified is not None:
        # Continuity across a verified renumbering that changed the line ID.
        links = verified.where("verified AND old_line_id != new_line_id").select(
            "provider", F.col("new_line_id").alias("line_id"), F.col("old_line_id").alias("renumbered_from_line_id"),
            F.col("to_snapshot").alias("valid_from")).dropDuplicates(["provider", "line_id", "valid_from"])
        versions = versions.join(links, ["provider", "line_id", "valid_from"], "left")
    else:
        versions = versions.withColumn("renumbered_from_line_id", F.lit(None).cast("string"))
    covered = x.select("provider", "line_id", "snapshot_date", "version_number")
    return versions.drop("first_index", "last_index"), covered


def renumbering_candidates(spark, transitions, options, mapping_rows):
    """Possible renumberings/re-identifications, with manual verification joined in."""
    from pyspark.sql import functions as F
    def jaccard(a, b):
        union = F.size(F.array_union(_arr(a), _arr(b)))
        return F.when(union > 0, F.round(F.size(F.array_intersect(_arr(a), _arr(b))) / union, 4)).otherwise(F.lit(0.0))
    same_id = transitions.where(F.array_contains("change_types", "public_code_changed")).select(
        "from_snapshot", "to_snapshot", "provider", F.col("line_id").alias("old_line_id"),
        F.col("line_id").alias("new_line_id"), F.col("public_code_before").alias("old_public_code"),
        F.col("public_code_after").alias("new_public_code"), F.lit("same_line_id_new_code").alias("match_type"),
        jaccard("stop_ids_before", "stop_ids_after").alias("stop_similarity"))
    retired = transitions.where("status='retired'").select(
        "from_snapshot", "to_snapshot", "provider", F.col("line_id").alias("old_line_id"),
        F.col("public_code_before").alias("old_public_code"), F.col("stop_ids_before").alias("old_stops"),
        F.col("transport_modes_before").alias("old_modes"))
    added = transitions.where("status='added'").select(
        "from_snapshot", "to_snapshot", "provider", F.col("line_id").alias("new_line_id"),
        F.col("public_code_after").alias("new_public_code"), F.col("stop_ids_after").alias("new_stops"),
        F.col("transport_modes_after").alias("new_modes"))
    pairs = retired.join(added, ["from_snapshot", "to_snapshot", "provider"])
    pairs = pairs.where(F.arrays_overlap(_arr("old_modes"), _arr("new_modes")) | (F.size(_arr("old_modes")) == 0))
    pairs = pairs.withColumn("stop_similarity", jaccard("old_stops", "new_stops"))
    pairs = pairs.withColumn("match_type", F.when(F.col("old_public_code") == F.col("new_public_code"),
                                                  "same_code_new_line_id").otherwise("similar_stops"))
    pairs = pairs.where((F.col("match_type") == "same_code_new_line_id") | (F.col("stop_similarity") >= options["renumber_similarity"]))
    columns = ["from_snapshot", "to_snapshot", "provider", "old_line_id", "new_line_id", "old_public_code",
               "new_public_code", "match_type", "stop_similarity"]
    candidates = same_id.select(*columns).unionByName(pairs.select(*columns))
    schema = "provider string, old_line_id string, new_line_id string, verified boolean, verified_by string, note string, " \
             "mapping_from_snapshot string, mapping_to_snapshot string"
    mappings = spark.createDataFrame(mapping_rows, schema)
    candidates = candidates.join(mappings, ["provider", "old_line_id", "new_line_id"], "left")
    candidates = candidates.where(F.col("mapping_to_snapshot").isNull() | (F.col("mapping_to_snapshot") == F.col("to_snapshot")))
    candidates = candidates.withColumn("verified", F.coalesce("verified", F.lit(False))).drop("mapping_from_snapshot", "mapping_to_snapshot")
    return candidates


def stop_coverage(lines, stop_names, dates, spark):
    """Network stop coverage per snapshot and stops gaining/losing service between snapshots."""
    from pyspark.sql import functions as F
    served = lines.select("snapshot_date", "provider", "line_id", "public_code", F.explode("stop_ids").alias("stop_id"))
    coverage = served.groupBy("snapshot_date", "provider", "stop_id").agg(
        F.countDistinct("line_id").alias("lines_serving"),
        F.sort_array(F.collect_set("public_code")).alias("line_public_codes"))
    coverage = coverage.join(stop_names, ["snapshot_date", "stop_id"], "left").withColumnRenamed("name", "stop_name")
    pairs = spark.createDataFrame(list(zip(dates[:-1], dates[1:])), ["from_snapshot", "to_snapshot"])
    b = coverage.join(pairs, coverage.snapshot_date == pairs.from_snapshot).select(
        "from_snapshot", "to_snapshot", "provider", "stop_id", F.col("stop_name").alias("b_name"),
        F.col("lines_serving").alias("lines_before"), F.col("line_public_codes").alias("line_codes_before"))
    a = coverage.join(pairs, coverage.snapshot_date == pairs.to_snapshot).select(
        "from_snapshot", "to_snapshot", "provider", "stop_id", F.col("stop_name").alias("a_name"),
        F.col("lines_serving").alias("lines_after"), F.col("line_public_codes").alias("line_codes_after"))
    changes = b.join(a, ["from_snapshot", "to_snapshot", "provider", "stop_id"], "full_outer")
    changes = changes.withColumn("change_type", F.when(F.col("lines_before").isNull(), "stop_gained_service")
                                 .when(F.col("lines_after").isNull(), "stop_lost_service")
                                 .when(F.col("line_codes_before") != F.col("line_codes_after"), "serving_lines_changed"))
    changes = changes.where("change_type IS NOT NULL").select(
        "from_snapshot", "to_snapshot", "provider", "stop_id", F.coalesce("a_name", "b_name").alias("stop_name"),
        "change_type", F.coalesce("lines_before", F.lit(0)).alias("lines_before"),
        F.coalesce("lines_after", F.lit(0)).alias("lines_after"),
        _arr("line_codes_before").alias("line_codes_before"), _arr("line_codes_after").alias("line_codes_after"))
    return coverage, changes


def summaries(lines, transitions, coverage, stop_changes):
    from pyspark.sql import functions as F
    network = lines.groupBy("snapshot_date", "provider").agg(
        F.count("*").alias("lines_published"),
        F.sum((F.col("window_departures") > 0).cast("int")).alias("lines_with_window_service"),
        F.sum("window_departures").alias("window_departures"),
        F.sum("journeys_published").alias("journeys_published"))
    network = network.join(coverage.groupBy("snapshot_date", "provider").agg(F.count("*").alias("stops_served")),
                           ["snapshot_date", "provider"], "left")
    has = lambda kind: F.sum(F.array_contains("change_types", kind).cast("int"))
    summary = transitions.groupBy("from_snapshot", "to_snapshot", "provider").agg(
        F.sum((F.col("status") != "added").cast("int")).alias("lines_before"),
        F.sum((F.col("status") != "retired").cast("int")).alias("lines_after"),
        F.sum((F.col("status") == "added").cast("int")).alias("lines_added"),
        F.sum((F.col("status") == "retired").cast("int")).alias("lines_retired"),
        F.sum((F.col("status") == "changed").cast("int")).alias("lines_structure_changed"),
        F.sum((F.col("status") == "service_changed").cast("int")).alias("lines_service_changed_only"),
        F.sum((F.col("status") == "unchanged").cast("int")).alias("lines_unchanged"),
        has("public_code_changed").alias("lines_renumbered"),
        has("stops_added").alias("lines_with_stops_added"), has("stops_removed").alias("lines_with_stops_removed"),
        has("patterns_changed").alias("lines_with_pattern_changes"),
        has("departures_increased").alias("lines_departures_increased"),
        has("departures_decreased").alias("lines_departures_decreased"),
        F.coalesce(F.sum("departures_before"), F.lit(0)).alias("window_departures_before"),
        F.coalesce(F.sum("departures_after"), F.lit(0)).alias("window_departures_after"))
    stops = stop_changes.groupBy("from_snapshot", "to_snapshot", "provider").agg(
        F.sum((F.col("change_type") == "stop_gained_service").cast("int")).alias("stops_gained_service"),
        F.sum((F.col("change_type") == "stop_lost_service").cast("int")).alias("stops_lost_service"))
    summary = summary.join(stops, ["from_snapshot", "to_snapshot", "provider"], "left").fillna(
        0, ["stops_gained_service", "stops_lost_service"])
    return network, summary


def read_mappings(path):
    """Manually verified renumberings: a JSON list (see config/route_renumberings.example.json)."""
    if path is None or not Path(path).is_file():
        return [], None
    entries = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = []
    for number, entry in enumerate(entries, 1):
        missing = [k for k in ("provider", "old_line_id", "new_line_id") if not entry.get(k)]
        if missing:
            raise HistoryInputError(f"Renumbering entry {number} is missing {missing}")
        rows.append((entry["provider"], entry["old_line_id"], entry["new_line_id"], bool(entry.get("verified", True)),
                     entry.get("verified_by"), entry.get("note"), entry.get("from_snapshot"), entry.get("to_snapshot")))
    return rows, digest(path)


def build_history(spark, inputs, output, options=None, mappings_path=None, identity_key=None):
    from pyspark.sql import functions as F
    options = {**DEFAULTS, **(options or {})}
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / "metadata").mkdir()
    dates = [i["snapshot_date"] for i in inputs]
    mapping_rows, mapping_sha = read_mappings(mappings_path)
    report = {"schema_version": SCHEMA_VERSION, "layer": "gold_history", "status": "in_progress",
              "identity": identity_key,
              "started_at_utc": utc_now(), "snapshots": inputs, "options": options,
              "renumbering_mappings": {"path": str(mappings_path) if mappings_path else None,
                                       "sha256": mapping_sha, "entries": len(mapping_rows)},
              "meaning": ("Changes between consecutive published planned-timetable snapshots. A change "
                          "between two snapshots happened at an unknown time within that interval."),
              "tables": {}, "checks": {}}
    path = output / "metadata/manifest.json"
    save_json(path, report)

    def write(name, frame, partitions=1):
        target = output / name
        expected = frame.count()
        frame.coalesce(partitions).write.mode("errorifexists").option("compression", "snappy").parquet(str(target))
        saved = spark.read.parquet(str(target))
        if saved.count() != expected:
            raise RuntimeError(f"{name}: read-back count mismatch")
        report["tables"][name] = {"path": name, "rows": expected, "schema": json.loads(saved.schema.json()),
                                  "parquet_bytes": sum(p.stat().st_size for p in target.glob("*.parquet"))}
        save_json(path, report)
        print(f"history/{name}: {expected:,} rows", flush=True)
        return saved

    try:
        lines, patterns, stop_names, input_checks = load_line_snapshots(spark, inputs)
        lines = write("line_snapshots", lines.select(
            "snapshot_date", "provider", "line_id", "public_code", "name", "transport_mode", "public_codes", "names",
            "transport_modes", "line_kinds", "line_occurrences", "has_conflicting_definitions", "journeys_published",
            "pattern_count", "pattern_signatures", "stop_count", "stop_ids", "window_departures", "days_with_service",
            "first_departure_seconds", "last_departure_seconds", "stop_set_hash", "pattern_set_hash", "attribute_hash"), 2)
        write("line_patterns", patterns.select("snapshot_date", "provider", "line_id", "pattern_signature",
                                               "stop_sequence", F.size("stop_sequence").alias("stops"), "journeys"), 2)
        transitions, changes = detect_changes(spark, lines, dates, options)
        transitions = write("line_transitions", transitions, 2)
        changes = write("line_changes", changes)
        candidates = write("renumbering_candidates", renumbering_candidates(spark, transitions, options, mapping_rows))
        versions, covered = build_versions(lines, dates, candidates)
        versions = write("line_versions", versions)
        coverage, stop_changes = stop_coverage(lines, stop_names, dates, spark)
        coverage = write("stop_coverage", coverage, 2)
        stop_changes = write("stop_coverage_changes", stop_changes)
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
        unbalanced = summary.where("lines_after - lines_before != lines_added - lines_retired").count()
        checks["transition_balance_failures"] = unbalanced
        if unbalanced:
            failures.append("Added/retired counts do not explain line count changes")
        version_rows = versions.agg(F.coalesce(F.sum("snapshots_observed"), F.lit(0))).first()[0]
        multi = covered.groupBy("provider", "line_id", "snapshot_date").count().where("count > 1").count()
        checks["versions_cover_every_line_snapshot"] = version_rows == lines.count() and multi == 0
        if not checks["versions_cover_every_line_snapshot"]:
            failures.append("SCD versions do not cover each line snapshot exactly once")
        expected_events = transitions.agg(F.coalesce(F.sum(F.size("change_types")), F.lit(0))).first()[0]
        checks["change_rows_match_transitions"] = expected_events == changes.count()
        if not checks["change_rows_match_transitions"]:
            failures.append("Exploded change rows differ from transition change lists")
        checks["lines_with_conflicting_definitions"] = lines.where("has_conflicting_definitions").count()
        if mapping_rows:
            matched = candidates.where("verified").select("provider", "old_line_id", "new_line_id").distinct().count()
            checks["verified_mappings_matched"] = matched
            checks["verified_mappings_unmatched"] = len({(r[0], r[1], r[2]) for r in mapping_rows if r[3]}) - matched
        if failures:
            raise RuntimeError("; ".join(failures))
        report["summary"] = [r.asDict() for r in summary.orderBy("from_snapshot", "provider").collect()]
        report.update(status="complete", completed_at_utc=utc_now())
        save_json(path, report)
        inventory = output_inventory(output)
        save_json(output / "metadata/inventory.json", inventory)
        return report
    except Exception as error:
        report.update(status="failed", completed_at_utc=utc_now(), error=f"{type(error).__name__}: {error}")
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


def run_identity(inputs, options, mapping_sha):
    code = digest(Path(__file__).resolve())
    return identity({"schema": SCHEMA_VERSION, "code": code, "options": options, "mappings": mapping_sha,
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


def run(spark, data_root, state=None, dates=None, options=None, mappings_path=None, force=False):
    data_root = Path(data_root).resolve()
    options = {**DEFAULTS, **(options or {})}
    inputs = find_inputs(state or data_root / "pipeline_state.sqlite3", dates)
    _, mapping_sha = read_mappings(mappings_path)
    key = run_identity(inputs, options, mapping_sha)
    root = data_root / "gold" / "entur" / "history"
    with history_lock(root):
        directory = root / key[:16]
        previous = None if force else reusable(directory, key) if directory.exists() else None
        if previous:
            print(f"route history: reuse {previous}", flush=True)
            return previous, json.loads((previous / "metadata/manifest.json").read_text()), True
        output = directory / f"history-{uuid4().hex[:12]}"
        report = build_history(spark, inputs, output, options, mappings_path, key)
        return output, report, False


def print_summary(report):
    print("\nfrom        to          prov  lines  +add  -ret  changed  renumb  +stops  -stops  departures")
    for r in report.get("summary", []):
        print(f"{r['from_snapshot']}  {r['to_snapshot']}  {r['provider']:4}  {r['lines_after']:5}  {r['lines_added']:4}  "
              f"{r['lines_retired']:4}  {r['lines_structure_changed']:7}  {r['lines_renumbered']:6}  "
              f"{r['stops_gained_service']:6}  {r['stops_lost_service']:6}  "
              f"{r['window_departures_before']}→{r['window_departures_after']}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-root", type=Path, required=True, help="e.g. /srv/dat535/data")
    parser.add_argument("--state", type=Path, help="Checkpoint registry (default: DATA_ROOT/pipeline_state.sqlite3)")
    parser.add_argument("--dates", nargs="+", help="Only these snapshot dates (at least two)")
    parser.add_argument("--renumberings", type=Path, default=Path("config/route_renumberings.json"),
                        help="Manually verified renumbering mappings (optional file)")
    parser.add_argument("--frequency-threshold", type=float, default=DEFAULTS["frequency_threshold"])
    parser.add_argument("--min-departure-change", type=int, default=DEFAULTS["min_departure_change"])
    parser.add_argument("--span-threshold-minutes", type=int, default=DEFAULTS["span_threshold_minutes"])
    parser.add_argument("--renumber-similarity", type=float, default=DEFAULTS["renumber_similarity"])
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--force", action="store_true", help="Build a new output even if an identical one exists")
    args = parser.parse_args(argv)
    options = {"frequency_threshold": args.frequency_threshold, "min_departure_change": args.min_departure_change,
               "span_threshold_minutes": args.span_threshold_minutes, "renumber_similarity": args.renumber_similarity}
    spark = None
    try:
        from pyspark.sql import SparkSession
        if args.master.startswith("local"):
            os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        spark = (SparkSession.builder.appName("DAT535-route-history").master(args.master)
                 .config("spark.sql.session.timeZone", "Europe/Oslo").config("spark.sql.shuffle.partitions", "4")
                 .config("spark.ui.showConsoleProgress", "false").getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        output, report, reused = run(spark, args.data_root, args.state, args.dates, options, args.renumberings, args.force)
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