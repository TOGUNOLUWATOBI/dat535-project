"""Select one validated snapshot without inferring dates from filenames."""

from datetime import date
import json
from pathlib import Path, PurePosixPath

from pipeline.ingestion.download_entur_netex import file_checksums
from pipeline.ingestion.generate_provider_dataset import provider_for


ENTITY_TAGS = {
    "lines": ("Line", "FlexibleLine"),
    "service_journeys": ("ServiceJourney", "FlexibleServiceJourney"),
    "passing_times": ("TimetabledPassingTime",),
}


def load_sources(manifest_path, snapshot_date=None, sample_per_provider=None):
    path = Path(manifest_path).resolve()
    manifest = json.loads(path.read_text())
    if manifest.get("status") not in ("complete", "complete_with_warnings"):
        raise ValueError("Processing requires a complete or complete_with_warnings ingestion")
    if sample_per_provider is not None and sample_per_provider < 1:
        raise ValueError("sample_per_provider must be positive")
    root = path.parent.parent if path.parent.name == "metadata" else path.parent
    if "snapshots" in manifest:
        if snapshot_date is None:
            raise ValueError("Specify --date when processing a historical dataset manifest")
        day = date.fromisoformat(str(snapshot_date))
        rows = [r for r in manifest["snapshots"] if r["snapshot_date"] == day.isoformat()]
        if len(rows) != 1 or rows[0]["status"] not in ("validated", "validated_with_warnings"):
            raise ValueError("Requested historical snapshot is missing or not validated")
        snapshot = rows[0]
        metadata = path.parent if path.parent.name == "metadata" else root
        lineage_path = metadata / "lineage" / f"{day}.json"
        validation_path = Path(snapshot["validation_report"])
        if not validation_path.is_absolute():
            validation_path = root / validation_path
    else:
        snapshot = manifest
        day = date.fromisoformat(snapshot["snapshot_date"])
        if snapshot_date is not None and date.fromisoformat(str(snapshot_date)) != day:
            raise ValueError("Requested date does not match the ingestion manifest")
        lineage_path = path.parent / "lineage.json"
        validation_path = path.parent / "validation.json"
    xml_root = Path(snapshot["xml_directory"])
    if not xml_root.is_absolute():
        xml_root = root / xml_root
    xml_root = xml_root.resolve()
    lineage = json.loads(lineage_path.read_text())
    validation = json.loads(validation_path.read_text())
    if validation.get("status") not in ("passed", "passed_with_warnings"):
        raise ValueError("Source reference validation has not passed")
    if validation.get("snapshot_date") != day.isoformat():
        raise ValueError("Source validation date does not match the requested snapshot")
    reports = {m["member"]: m for m in validation["members"]}
    providers = manifest["providers"]
    selected = []
    for item in lineage:
        owner = item["provider"]
        # Shared files and national stops need their own entity projections.
        if owner not in providers or not Path(item["member"]).name.upper().startswith(owner + "_"):
            continue
        member = PurePosixPath(item["member"])
        if member.is_absolute() or ".." in member.parts or "\\" in str(member):
            raise ValueError(f"Unsafe member path in source lineage: {member}")
        source = (xml_root / str(member)).resolve()
        if not source.is_relative_to(xml_root) or provider_for(str(member), providers) != owner:
            raise ValueError(f"Source member or provider is inconsistent: {member}")
        report = reports[item["member"]]
        if report["entity_counts"].get("CompositeFrame", 0) != 1:
            raise ValueError(f"First staging projection requires one CompositeFrame per provider file: {member}")
        selected.append({
            "source_file": str(source), "source_member": str(member), "provider": owner,
            "source_xml_sha256": item["sha256"], "source_bytes": item["bytes"],
            "source_profile": report["profile"],
            "publication_timestamp": report["publication_timestamp"],
            "entity_counts": report["entity_counts"],
        })
    chosen = []
    for provider in providers:
        candidates = sorted((s for s in selected if s["provider"] == provider),
                            key=lambda s: (s["source_bytes"], s["source_member"]))
        if sample_per_provider:
            candidates = [s for s in candidates if sum(s["entity_counts"].get(tag, 0)
                          for tag in ENTITY_TAGS["service_journeys"]) > 0]
        if not candidates:
            raise ValueError(f"No eligible validated provider XML selected for {provider}")
        chosen.extend(candidates[:sample_per_provider] if sample_per_provider else candidates)
    verify_sources(chosen)
    expected = {table: sum(s["entity_counts"].get(tag, 0) for s in chosen for tag in tags)
                for table, tags in ENTITY_TAGS.items()}
    expected["frames"] = len(chosen)
    return {
        "source_manifest": str(path), "source_root": str(root), "snapshot_date": day.isoformat(),
        "providers": providers, "scope": "sample" if sample_per_provider else "provider_line_files",
        "sample_per_provider": sample_per_provider, "files": chosen, "expected_rows": expected,
        "excluded_shared_files": sum(1 for i in lineage if Path(i["member"]).name.startswith("_")),
    }


def verify_sources(sources):
    for item in sources:
        path = Path(item["source_file"])
        if not path.is_file() or path.stat().st_size != item["source_bytes"] or file_checksums(path)[0] != item["source_xml_sha256"]:
            raise RuntimeError(f"Source XML changed since Bronze validation: {path}")


def load_snapshot_sources(manifest_path, snapshot_date=None):
    """Select all provider line/shared frames and the preserved national stop file."""
    selected = load_sources(manifest_path, snapshot_date)
    path = Path(selected["source_manifest"])
    root = Path(selected["source_root"])
    manifest = json.loads(path.read_text())
    day = selected["snapshot_date"]
    if "snapshots" in manifest:
        snapshot = next(s for s in manifest["snapshots"] if s["snapshot_date"] == day)
        metadata = path.parent if path.parent.name == "metadata" else root
        lineage_path = metadata / "lineage" / f"{day}.json"
        report_path = Path(snapshot["validation_report"])
        if not report_path.is_absolute(): report_path = root / report_path
    else:
        snapshot = manifest
        lineage_path = path.parent / "lineage.json"
        report_path = path.parent / "validation.json"
    xml = Path(snapshot["xml_directory"])
    if not xml.is_absolute(): xml = root / xml
    xml = xml.resolve()
    lineage = json.loads(lineage_path.read_text())
    reports = {r["member"]: r for r in json.loads(report_path.read_text())["members"]}
    stops, shared = [], []
    for item in lineage:
        member = PurePosixPath(item["member"])
        if not member.name.startswith("_"): continue
        if member.is_absolute() or ".." in member.parts or "\\" in str(member):
            raise ValueError(f"Unsafe shared source member: {member}")
        source = (xml / str(member)).resolve()
        if not source.is_relative_to(xml): raise ValueError("Shared source escapes XML root")
        report = reports[str(member)]
        entry = {"source_file": str(source), "source_member": str(member),
                 "provider": item["provider"] or "NSR", "source_xml_sha256": item["sha256"],
                 "source_bytes": item["bytes"], "source_profile": report["profile"],
                 "publication_timestamp": report["publication_timestamp"], "entity_counts": report["entity_counts"]}
        if member.name == "_stops.xml": stops.append(entry)
        elif item["provider"] in selected["providers"]:
            if entry["entity_counts"].get("CompositeFrame") != 1:
                raise ValueError(f"Expected one shared CompositeFrame: {member}")
            shared.append(entry)
    if len(stops) != 1: raise ValueError("Complete snapshot requires exactly one national _stops.xml")
    verify_sources(shared + stops)
    selected.update(files=selected["files"] + shared, stop_files=stops, scope="complete_snapshot",
                    source_sha256=snapshot.get("source_sha256"), source_generation=snapshot.get("source_generation"))
    tags = {**ENTITY_TAGS, "routes": ("Route",), "journey_patterns": ("JourneyPattern",),
            "journey_pattern_points": ("StopPointInJourneyPattern",),
            "scheduled_stop_points": ("ScheduledStopPoint",),
            "stop_assignments": ("PassengerStopAssignment", "FlexibleStopAssignment"),
            "day_types": ("DayType",), "operating_periods": ("OperatingPeriod",),
            "operating_days": ("OperatingDay",), "day_type_assignments": ("DayTypeAssignment",),
            "flexible_stop_places": ("FlexibleStopPlace",)}
    selected["expected_rows"] = {name: sum(f["entity_counts"].get(t, 0)
        for f in selected["files"] for t in entities) for name, entities in tags.items()}
    selected["expected_rows"].update(frames=len(selected["files"]),
        stop_places=sum(f["entity_counts"].get("StopPlace", 0) for f in stops),
        quays=sum(f["entity_counts"].get("Quay", 0) for f in stops))
    return selected
