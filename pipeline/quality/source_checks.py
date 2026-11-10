"""Independent, bounded XML checks. No Spark or transformation code is imported.

Definitions are small dictionaries; passing-time arrays are never retained while
indexing a provider. Local references retain their file scope. Missing or ambiguous
references stop a check instead of choosing the first candidate.
"""

from collections import Counter, defaultdict
import json
from pathlib import Path
import xml.etree.ElementTree as ET

NS = "{http://www.netex.org.uk/netex}"
RECORDS = {"Line", "FlexibleLine", "Route", "JourneyPattern", "ScheduledStopPoint",
           "PassengerStopAssignment", "FlexibleStopAssignment", "FlexibleStopPlace",
           "ServiceJourney", "FlexibleServiceJourney", "DayType", "OperatingPeriod",
           "OperatingDay", "DayTypeAssignment", "StopPlace", "ServiceLink"}


class SourceReferenceError(ValueError):
    """The original XML cannot support an unambiguous comparison."""


def reference(element, tag):
    child = element.find(NS + tag)
    return (child.get("ref"), child.get("version")) if child is not None else (None, None)


def entities(path):
    """Stream complete entity subtrees, then clear them to bound parser memory."""
    with Path(path).open("rb") as stream:
        for _, element in ET.iterparse(stream, events=("end",)):
            tag = element.tag.removeprefix(NS)
            if tag in RECORDS:
                yield tag, element
                element.clear()


def resolve(rows, target, description, collapse=False):
    identifier, version = target
    matches = [r for r in rows if r["id"] == identifier
               and (version in (None, "any", "*") or r["version"] == version)] if identifier else []
    if collapse:
        matches = list({json.dumps(r, sort_keys=True): r for r in matches}.values())
    if len(matches) != 1:
        raise SourceReferenceError(f"{description} {target}: expected one definition, found {len(matches)}")
    return matches[0]


class SourceIndex:
    """Provider XML definitions and journey descriptors, preserving file/ID scope."""

    def __init__(self, paths):
        self.local = defaultdict(lambda: defaultdict(list))
        self.shared = defaultdict(list)
        self.journeys = []
        for path in dict.fromkeys(str(Path(p).resolve()) for p in paths):
            for tag, element in entities(path):
                row = {"id": element.get("id"), "version": element.get("version")}
                if tag in ("Line", "FlexibleLine"):
                    row["flexible"] = tag == "FlexibleLine"
                    self.local[path]["lines"].append(row)
                elif tag == "Route":
                    row["line"] = reference(element, "LineRef")
                    if row["line"][0] is None:
                        row["line"] = reference(element, "FlexibleLineRef")
                    self.local[path]["routes"].append(row)
                elif tag == "JourneyPattern":
                    row["route"] = reference(element, "RouteRef")
                    row["points"] = [{"id": p.get("id"), "version": p.get("version"),
                                      "stop": reference(p, "ScheduledStopPointRef")}
                                     for p in element.findall(f"{NS}pointsInSequence/{NS}StopPointInJourneyPattern")]
                    self.local[path]["patterns"].append(row)
                elif tag == "ScheduledStopPoint":
                    self.shared["stops"].append(row)
                elif tag in ("PassengerStopAssignment", "FlexibleStopAssignment"):
                    identifier, version = reference(element, "ScheduledStopPointRef")
                    self.shared["assignments"].append({"id": identifier, "version": version or "",
                        "quay": reference(element, "QuayRef"),
                        "flexible_stop": reference(element, "FlexibleStopPlaceRef")})
                elif tag == "FlexibleStopPlace":
                    self.shared["flexible_stops"].append(row)
                elif tag in ("ServiceJourney", "FlexibleServiceJourney"):
                    self.journeys.append({**row, "file": path, "kind": tag,
                        "line": reference(element, "LineRef"), "pattern": reference(element, "JourneyPatternRef"),
                        "day_types": [d.get("ref") for d in element.findall(f"{NS}dayTypes/{NS}DayTypeRef")]})

    def line(self, journey):
        local = self.local[str(Path(journey["file"]).resolve())]
        target = journey["line"]
        if target[0] is None:
            pattern = resolve(local["patterns"], journey["pattern"], "JourneyPattern")
            route = resolve(local["routes"], pattern["route"], "Route")
            target = route["line"]
        return resolve(local["lines"], target, "Line")

    def physical_stops(self, path, pattern_ref, passing_times, stop_files):
        local = self.local[str(Path(path).resolve())]
        pattern = resolve(local["patterns"], pattern_ref, "JourneyPattern")
        resolved = []
        for passing in passing_times:
            point = resolve(pattern["points"], (passing["point"], passing["point_version"]), "Pattern point")
            stop = resolve(self.shared["stops"], point["stop"], "ScheduledStopPoint", collapse=True)
            # Assignments are selected by the stop they assign, not their own entity ID.
            assignment = resolve(self.shared["assignments"], (stop["id"], None), "Stop assignment", collapse=True)
            item = {"point_ref": point["id"], "point_version": point["version"],
                    "scheduled_stop_ref": stop["id"], "scheduled_stop_version": stop["version"],
                    "quay_ref": assignment["quay"][0], "quay_version": assignment["quay"][1],
                    "flexible_stop_ref": assignment["flexible_stop"][0], "stop_place_id": None}
            if item["flexible_stop_ref"]:
                resolve(self.shared["flexible_stops"], assignment["flexible_stop"], "FlexibleStopPlace", collapse=True)
            if not item["quay_ref"] and not item["flexible_stop_ref"]:
                raise SourceReferenceError(f"Stop {stop['id']} has no physical target")
            resolved.append(item)
        wanted = {r["quay_ref"] for r in resolved if r["quay_ref"]}
        quays = []
        if wanted:
            for stop_file in stop_files:
                for tag, place in entities(stop_file):
                    if tag != "StopPlace":
                        continue
                    for quay in place.findall(f"{NS}quays/{NS}Quay"):
                        if quay.get("id") in wanted:
                            quays.append({"id": quay.get("id"), "version": quay.get("version"),
                                          "stop_place_id": place.get("id")})
        for item in resolved:
            if item["quay_ref"]:
                quay = resolve(quays, (item["quay_ref"], item["quay_version"]), "Quay", collapse=True)
                item["stop_place_id"] = quay["stop_place_id"]
                item["resolved_quay_version"] = quay["version"]
        return resolved

    def line_departures(self, line_id, calendar, operating_dates, first, last):
        """Recount every original journey occurrence for a line across provider files.

        Definitions with repeated IDs remain distinct journey occurrences. Flexible
        journeys are accounted for separately; no Silver eligibility flag is read.
        """
        fixed, flexible = Counter(), Counter()
        occurrences = 0
        for journey in self.journeys:
            line = self.line(journey)
            if line["id"] != line_id:
                continue
            occurrences += 1
            if not journey["day_types"]:
                raise SourceReferenceError(f"Journey {journey['id']} has no day types")
            dates, _ = operating_dates(journey["day_types"], calendar, first, last)
            target = flexible if line["flexible"] or journey["kind"] == "FlexibleServiceJourney" else fixed
            target.update(dates)
        if not occurrences:
            raise SourceReferenceError(f"No source journey occurrences found for line {line_id}")
        return {"source_journey_occurrences": occurrences,
                "fixed_departures_by_date": dict(sorted(fixed.items())),
                "flexible_occurrences_by_date": dict(sorted(flexible.items()))}
