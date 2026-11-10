"""Deduplicate completed Bronze inputs without removing paths or changing bytes.

Default is a checksum-verified dry run. --apply atomically replaces an identical
copy with a hardlink. Raw inputs must remain immutable, as required by ingestion.
Running/failed snapshots, uploads/original.zip, metadata and Parquet are untouched.
"""

import argparse
from collections import defaultdict
import json
import os
from pathlib import Path
import stat
from uuid import uuid4

from pipeline.ingestion.generate_provider_dataset import save_json, verify_materialized_snapshot
from pipeline.storage.zip_bronze import checksum

GOOD = {"complete", "complete_with_warnings", "validated", "validated_with_warnings"}


def signature(path):
    s = path.lstat()
    if not stat.S_ISREG(s.st_mode):
        raise ValueError(f"Not a regular file: {path}")
    return (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns)


def candidates(root):
    """Only paths and expected hashes from completed ingestion lineage."""
    root = Path(root).resolve()
    found, problems = {}, []
    def add(path, expected, size=None):
        path = Path(path)
        if path.is_symlink() or not path.is_file():
            return
        path = path.resolve()
        if not path.is_relative_to(root) or not isinstance(expected, str) or len(expected) != 64:
            return
        if size is not None and path.stat().st_size != size:
            problems.append(f"Size differs from lineage: {path}")
            return
        if path in found and found[path] != expected:
            raise ValueError(f"Conflicting lineage for {path}")
        found[path] = expected
    for manifest_path in sorted(root.rglob("manifest.json")):
        try:
            manifest = json.loads(manifest_path.read_text())
            metadata = manifest_path.parent
            base = metadata.parent if metadata.name == "metadata" else metadata
            rows = manifest.get("snapshots", [manifest])
            for row in rows:
                if row.get("status") not in GOOD or not row.get("xml_directory"):
                    continue
                day = row["snapshot_date"]
                lineage_path = metadata / "lineage" / f"{day}.json" if "snapshots" in manifest else metadata / "lineage.json"
                lineage = json.loads(lineage_path.read_text())
                directory = Path(row["xml_directory"])
                directory = directory if directory.is_absolute() else base / directory
                for item in lineage:
                    add(directory / item["member"], item["sha256"], item["bytes"])
                for field, hash_field in (("combined_zip", "combined_zip_sha256"), ("national_zip", "source_sha256")):
                    if row.get(field):
                        path = Path(row[field])
                        add(path if path.is_absolute() else base / path, row.get(hash_field))
        except (OSError, ValueError, KeyError, TypeError) as error:
            problems.append(f"Skipped manifest {manifest_path}: {error}")
    return found, problems


def deduplicate(root, apply=False):
    files, problems = candidates(root)
    groups = defaultdict(list)
    for path, expected in files.items():
        s = path.stat()
        groups[(expected, s.st_size, s.st_dev, s.st_uid, s.st_gid, stat.S_IMODE(s.st_mode))].append(path)
    report = {"applied": apply, "indexed_files": len(files), "duplicate_copies": 0,
              "reclaimable_bytes": 0, "linked_files": 0, "problems": problems, "examples": []}
    for (expected, _, _, _, _, _), paths in groups.items():
        if len({(p.stat().st_dev, p.stat().st_ino) for p in paths}) < 2:
            continue
        canonical, canonical_signature = None, None
        # Keep an existing shared inode as canonical. Choosing a new copy first
        # would skip the already-linked originals and miss the new duplicate.
        for path in sorted(paths, key=lambda p: (-p.stat().st_nlink, str(p))):
            try:
                before = signature(path)
                if canonical is not None:
                    if path.samefile(canonical) or path.stat().st_nlink != 1:
                        continue
                if checksum(path) != expected or signature(path) != before:
                    raise ValueError("File changed or checksum differs from ingestion lineage")
                if canonical is None:
                    canonical, canonical_signature = path, before
                    continue
                if signature(canonical) != canonical_signature:
                    raise ValueError("Canonical file changed; group is no longer safe")
                size = path.stat().st_blocks * 512
                if apply:
                    temporary = path.with_name(f".{path.name}.dedup-{uuid4().hex}")
                    try:
                        os.link(canonical, temporary)
                        if signature(path) != before or signature(canonical) != canonical_signature:
                            raise ValueError("Input changed before atomic replacement")
                        os.replace(temporary, path)
                    finally:
                        temporary.unlink(missing_ok=True)
                    report["linked_files"] += 1
                report["duplicate_copies"] += 1
                report["reclaimable_bytes"] += size
                if len(report["examples"]) < 10:
                    report["examples"].append({"copy": str(path), "canonical": str(canonical), "bytes": size})
            except (OSError, ValueError) as error:
                report["problems"].append(f"Left unchanged {path}: {error}")
    return report


def discard_provider_zips(root, apply=False):
    """Remove only verified regenerable ZIP copies; retain raw XML and manual uploads."""
    root = Path(root).resolve()
    result = {"applied": apply, "zip_copies": 0, "reclaimable_bytes": 0, "problems": []}
    for path in sorted(root.rglob("manifest.json")):
        try:
            manifest = json.loads(path.read_text())
            if manifest.get("ingestion_method") == "manual_zip" or manifest.get("uploaded_archive"):
                continue
            metadata = path.parent
            base = metadata.parent if metadata.name == "metadata" else metadata
            rows = manifest.get("snapshots", [manifest])
            for row in rows:
                if row.get("raw_storage") == "zip":
                    continue  # This ZIP is the authoritative raw copy, not a redundant cache.
                name = row.get("combined_zip") or row.get("discarded_combined_zip")
                if row.get("status") not in GOOD or not name:
                    continue
                archive = Path(name)
                archive = archive if archive.is_absolute() else base / archive
                if archive.is_symlink() or not archive.is_file() or not archive.resolve().is_relative_to(root):
                    continue
                before = signature(archive)
                if checksum(archive) != row.get("combined_zip_sha256"):
                    raise ValueError(f"ZIP differs from its recorded hash: {archive}")
                day = row["snapshot_date"]
                lineage_path = metadata / "lineage" / f"{day}.json" if "snapshots" in manifest else metadata / "lineage.json"
                lineage = json.loads(lineage_path.read_text())
                directory = Path(row["xml_directory"])
                directory = directory if directory.is_absolute() else base / directory
                if not directory.resolve().is_relative_to(root):
                    raise ValueError("XML directory is outside Bronze root")
                verify_materialized_snapshot(directory, archive, lineage)
                if signature(archive) != before:
                    raise ValueError("ZIP changed during verification")
                size = archive.stat().st_blocks * 512 if archive.stat().st_nlink == 1 else 0
                print(f"{'Discarding' if apply else 'Verified ZIP copy'}: {archive}", flush=True)
                if apply:
                    row.update(xml_only=True, combined_zip=None, discarded_combined_zip=name,
                               discarded_combined_zip_bytes=archive.stat().st_size)
                    # Switch readers to verified XML before removing the compressed copy.
                    save_json(path, manifest)
                    archive.unlink()
                    row.update(combined_zip_removed=True, combined_zip_bytes=0)
                    if "snapshots" in manifest and "combined_zip_bytes" in manifest:
                        manifest["combined_zip_bytes"] = sum(r.get("combined_zip_bytes", 0) for r in rows)
                    save_json(path, manifest)
                result["zip_copies"] += 1
                result["reclaimable_bytes"] += size
        except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
            result["problems"].append(f"Left source data intact {path}: {error}")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-root", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--discard-provider-zips", action="store_true",
                        help="Also discard verified provider ZIP copies, recording XML-only retention in metadata")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if not args.bronze_root.is_dir():
        parser.error("Bronze root does not exist")
    if args.report and args.report.exists():
        parser.error("Report already exists; choose a new report filename")
    print("Checking completed inputs against their recorded SHA-256 hashes; this can take a few minutes.", flush=True)
    report = deduplicate(args.bronze_root, args.apply)
    if args.discard_provider_zips:
        report["provider_zip_cleanup"] = discard_provider_zips(args.bronze_root, args.apply)
    print(json.dumps(report, indent=2))
    label = "Storage consolidated" if args.apply else "Potential saving (dry run)"
    print(f"{label}: {report['reclaimable_bytes'] / 1024**3:.2f} GiB across {report['duplicate_copies']} copies")
    if args.discard_provider_zips:
        print(f"Provider ZIP copies: {report['provider_zip_cleanup']['reclaimable_bytes'] / 1024**3:.2f} GiB")
    if args.report:
        with args.report.open("x") as output:
            output.write(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
