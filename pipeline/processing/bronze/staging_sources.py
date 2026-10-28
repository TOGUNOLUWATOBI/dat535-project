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
