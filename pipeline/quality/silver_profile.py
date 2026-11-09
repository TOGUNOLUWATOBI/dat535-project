"""Quality evidence for one Silver output, and the rule that decides whether Gold may publish it.

Zero quarantined rows says only that nothing broke the current rules. This profile
records, per provider, what the cleaning actually did and what it found:

* ``volumes``: rows in staging (before) and Silver (after) for each entity;
* ``duplicates``: identical definitions collapsed and conflicting ones withheld;
* ``missing``: empty essential fields (needed to place a departure at a stop and
  time) and empty optional fields (names, coordinates, direction);
* ``references``: unresolved (no target) and ambiguous (several targets) references;
* ``times``: invalid, inverted and non-monotonic passing times, and overnight
  times kept with a day offset;
* ``calendar``: day types whose dates could not be derived, by reason;
* ``service``: service occurrences in the window that are accepted for Gold,
  excluded as flexible, or rejected as invalid, plus valid fixed journeys that do
  not operate in the window;
* ``reasons``: quarantine reason codes for journeys and passing times.

``evaluate`` applies the thresholds (config/quality_thresholds.json) to the profile
and returns ``pass``, ``warn`` or ``fail`` with the rules that fired. Gold refuses
to publish on ``fail``.
"""

import json
from pathlib import Path

GATE_VERSION = 1
# Default publication rules. A rule fires when the metric is above the limit.
# Rates are fractions of the relevant total; counts are absolute.
DEFAULT_THRESHOLDS = {
    "journey_quarantine_rate": {"warn": 0.0, "fail": 0.01},
    "passing_time_quarantine_rate": {"warn": 0.0, "fail": 0.01},
    "invalid_service_occurrence_rate": {"warn": 0.0, "fail": 0.01},
    "ambiguous_references": {"warn": None, "fail": 0},
    "unresolved_references": {"warn": 0, "fail": None},
    "essential_missing_rate": {"warn": 0.0, "fail": 0.01},
    "conflicting_definitions": {"warn": 0, "fail": None},
    "calendar_issues": {"warn": 0, "fail": None},
    "eligible_service_occurrences_min": {"fail_below": 1},
}
CANONICAL = ("scheduled_stop_points", "stop_assignments", "quays", "flexible_stop_places")


def _by_provider(frame, **aggregations):
    """{provider: {name: value}} for aggregations given as Spark column expressions."""
    rows = frame.groupBy("provider").agg(*[expr.alias(name) for name, expr in aggregations.items()]).collect()
    return {r["provider"]: {name: (r[name] if r[name] is not None else 0) for name in aggregations} for r in rows}


def _merge(*parts):
    merged = {}
    for part in parts:
        for provider, values in part.items():
            merged.setdefault(provider, {}).update(values)
    return merged


def profile_silver(spark, silver):
    """Profile a completed Silver output; returns a JSON-serialisable dict."""
    from pyspark.sql import functions as F
    silver = Path(silver).resolve()
    manifest = json.loads((silver / "metadata/manifest.json").read_text())
    if manifest.get("status") != "complete":
        raise ValueError(f"Quality profile needs a complete Silver output: {silver}")
    staging = json.loads(Path(manifest["source_manifest"]).read_text())
    staging_root = Path(manifest["source_manifest"]).parent.parent

    def table(name):
        return spark.read.parquet(str(silver / manifest["tables"][name]["path"]))

    def staging_table(name):
        return spark.read.parquet(str(staging_root / staging["tables"][name]["path"]))

    count = lambda condition: F.sum(F.when(condition, 1).otherwise(0))
    blank = lambda column: F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == "")
    journeys, times = table("journeys"), table("passing_times")
    fixed = ~F.col("is_flexible")

    # Before/after volumes per entity.
    volumes = _merge(
        {p: {"lines_staging": v["n"]} for p, v in _by_provider(staging_table("lines"), n=F.count("*")).items()},
        {p: {"lines_silver": v["n"]} for p, v in _by_provider(table("lines"), n=F.count("*")).items()},
        {p: {"journeys_staging": v["n"]} for p, v in
         _by_provider(staging_table("service_journeys"), n=F.count("*")).items()},
        _by_provider(journeys, journeys_silver=F.count("*"), journeys_accepted=count(F.col("is_valid")),
                     journeys_quarantined=count(~F.col("is_valid")), journeys_flexible=count(F.col("is_flexible"))),
        {p: {"passing_times_staging": v["n"]} for p, v in
         _by_provider(staging_table("passing_times"), n=F.count("*")).items()},
        _by_provider(times, passing_times_silver=F.count("*"), passing_times_accepted=count(F.col("is_valid")),
                     passing_times_quarantined=count(~F.col("is_valid"))))

    # Duplicates: identical definitions collapsed into one canonical row, conflicts withheld.
    duplicates = {}
    for name in CANONICAL:
        if name not in manifest["tables"]:
            continue
        before = _by_provider(staging_table(name), n=F.count("*")) if name in staging["tables"] else {}
        after = _by_provider(table(name), rows=F.count("*"), occurrences=F.sum("source_occurrences"))
        conflicts = manifest.get("quality", {}).get(f"{name}_conflicting_occurrences", 0)
        for provider, values in after.items():
            entry = duplicates.setdefault(provider, {})
            entry[f"{name}_staging"] = before.get(provider, {}).get("n", values["occurrences"])
            entry[f"{name}_silver"] = values["rows"]
            entry[f"{name}_collapsed"] = values["occurrences"] - values["rows"]
        duplicates.setdefault("_all", {})[f"{name}_conflicting_withheld"] = conflicts

    # Missing values: essential fields carry the departure/stop/time facts Gold uses.
    lines = table("lines")
    missing = _merge(
        _by_provider(lines, lines=F.count("*"), essential_line_transport_mode=count(blank("transport_mode")),
                     optional_line_public_code=count(blank("public_code")), optional_line_name=count(blank("name"))),
        # ServiceJourney.DepartureTime is optional in the Nordic profile (times are on the
        # passing times), so only passing-time times are essential.
        _by_provider(journeys, essential_journey_pattern_ref=count(blank("pattern_ref")),
                     essential_journey_day_types=count(F.col("day_type_refs").isNull()
                                                       | (F.size("day_type_refs") == 0)),
                     optional_journey_departure_time=count(fixed & blank("departure_time_raw"))),
        _by_provider(times, essential_physical_stop=count(F.col("quay_stop_place_id").isNull()
                                                          & F.col("assignment_flexible_stop_place_ref").isNull()),
                     essential_fixed_time=count(~F.col("is_flexible") & blank("arrival_time_raw")
                                                & blank("departure_time_raw")),
                     optional_stop_name=count(blank("stop_name")),
                     optional_quay_coordinates=count(F.col("quay_stop_place_id").isNotNull()
                                                     & (F.col("quay_latitude").isNull()
                                                        | F.col("quay_longitude").isNull()))),
        _by_provider(table("routes"), optional_route_direction=count(blank("direction"))))

    # References: 0 matches = unresolved, >1 = ambiguous (both are never guessed).
    ref = lambda frame, column, **extra: _by_provider(
        frame, **{f"unresolved_{column}": count(F.col(f"{column}_matches") == 0),
                  f"ambiguous_{column}": count(F.col(f"{column}_matches") > 1)}, **extra)
    references = _merge(ref(table("journey_patterns"), "route"), ref(journeys, "pattern"), ref(journeys, "line"),
                        ref(table("journey_day_types"), "day"))

    # Times: offsets keep overnight journeys on their operating day.
    reason = lambda code: count(F.array_contains("reason_codes", code))
    times_profile = _by_provider(
        times, invalid_time=reason("invalid_time"), arrival_after_departure=reason("arrival_after_departure"),
        non_monotonic_time=reason("non_monotonic_time"),
        overnight_passing_times=count(F.greatest("arrival_seconds", "departure_seconds") >= 86400),
        max_day_offset=F.max(F.floor(F.greatest("arrival_seconds", "departure_seconds") / 86400)))

    issues = table("calendar_issues").groupBy("provider", "reason").count().collect()
    calendar = {}
    for r in issues:
        calendar.setdefault(r["provider"], {})[r["reason"]] = r["count"]

    dates = table("journey_service_dates")
    in_window = dates.select("journey_key").distinct().withColumn("operates", F.lit(True))
    service = _merge(
        _by_provider(dates, service_occurrences=F.count("*"),
                     eligible_fixed=count(F.col("is_valid") & ~F.col("is_flexible")),
                     excluded_flexible=count(F.col("is_flexible") & F.col("is_valid")),
                     rejected_invalid=count(~F.col("is_valid"))),
        _by_provider(journeys.join(in_window, "journey_key", "left"),
                     valid_fixed_journeys=count(F.col("is_valid") & ~F.col("is_flexible")),
                     valid_fixed_journeys_not_in_window=count(F.col("is_valid") & ~F.col("is_flexible")
                                                              & F.col("operates").isNull())))

    explode = lambda frame: frame.select("provider", F.explode("reason_codes").alias("reason")) \
        .groupBy("provider", "reason").count().collect()
    reasons = {}
    for kind, frame in (("journeys", journeys), ("passing_times", times)):
        for r in explode(frame):
            reasons.setdefault(r["provider"], {}).setdefault(kind, {})[r["reason"]] = r["count"]

    providers = sorted(set(volumes) | set(service))
    return {"gate_version": GATE_VERSION, "snapshot_date": manifest["snapshot_date"],
            "service_from": manifest["service_from"], "service_to": manifest["service_to"],
            "silver": str(silver), "providers": providers,
            "volumes": volumes, "duplicates": duplicates, "missing": missing, "references": references,
            "times": times_profile, "calendar": calendar, "service": service, "reasons": reasons}


def gate_metrics(profile):
    """Reduce a profile to the per-provider numbers the publication rules use."""
    metrics = {}
    conflicts = sum(profile["duplicates"].get("_all", {}).values())
    for provider in profile["providers"]:
        v = profile["volumes"].get(provider, {})
        s = profile["service"].get(provider, {})
        m = profile["missing"].get(provider, {})
        r = profile["references"].get(provider, {})
        rate = lambda part, whole: (part / whole) if whole else 0.0
        essential = sum(value for key, value in m.items() if key.startswith("essential_"))
        essential_base = v.get("journeys_silver", 0) + v.get("passing_times_silver", 0) + m.get("lines", 0)
        metrics[provider] = {
            "journey_quarantine_rate": rate(v.get("journeys_quarantined", 0), v.get("journeys_silver", 0)),
            "passing_time_quarantine_rate": rate(v.get("passing_times_quarantined", 0),
                                                 v.get("passing_times_silver", 0)),
            "invalid_service_occurrence_rate": rate(s.get("rejected_invalid", 0), s.get("service_occurrences", 0)),
            "ambiguous_references": sum(value for key, value in r.items() if key.startswith("ambiguous_")),
            "unresolved_references": sum(value for key, value in r.items() if key.startswith("unresolved_")),
            "essential_missing_rate": rate(essential, essential_base),
            "conflicting_definitions": conflicts,
            "calendar_issues": sum(profile["calendar"].get(provider, {}).values()),
            "eligible_service_occurrences_min": s.get("eligible_fixed", 0),
        }
    return metrics


def evaluate(profile, thresholds=None):
    """Return {"status": pass|warn|fail, "findings": [...], "metrics": {...}}."""
    rules = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    metrics = gate_metrics(profile)
    findings = []
    for provider, values in metrics.items():
        for name, rule in rules.items():
            value = values.get(name)
            if value is None:
                continue
            if "fail_below" in rule:
                if value < rule["fail_below"]:
                    findings.append({"provider": provider, "rule": name, "level": "fail", "value": value,
                                     "limit": f"< {rule['fail_below']}"})
                continue
            for level in ("fail", "warn"):
                limit = rule.get(level)
                if limit is not None and value > limit:
                    findings.append({"provider": provider, "rule": name, "level": level, "value": value,
                                     "limit": f"> {limit}"})
                    break
    levels = {f["level"] for f in findings}
    status = "fail" if "fail" in levels else "warn" if "warn" in levels else "pass"
    return {"status": status, "gate_version": GATE_VERSION, "thresholds": rules, "findings": findings,
            "metrics": metrics}


def read_thresholds(path):
    """Thresholds file (optional); unknown rule names are refused so typos cannot disable a rule."""
    if path is None or not Path(path).is_file():
        return dict(DEFAULT_THRESHOLDS)
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    rules = data.get("rules", data)
    unknown = sorted(set(rules) - set(DEFAULT_THRESHOLDS))
    if unknown:
        raise ValueError(f"Unknown quality rules in {path}: {unknown}")
    return {**DEFAULT_THRESHOLDS, **rules}
