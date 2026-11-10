"""Keep validated Bronze XML in ZIPs and materialize one snapshot while reading it.

Migration never downloads data. XML is removed only after every ZIP member has
matched the existing lineage. Original paths, source hashes and metadata remain.
"""

import argparse
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import date
import fcntl
from functools import wraps
import hashlib
import inspect
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tempfile
from uuid import uuid4
import zipfile

from pipeline.common import BLOCK_SIZE, sha256_file, write_json

# Manifest statuses that mean a snapshot passed ingestion and validation.
GOOD = {"complete", "complete_with_warnings", "validated", "validated_with_warnings"}
# XML directories restored by the current call stack; nested users reuse the restoration.
ACTIVE = ContextVar("materialized_bronze", default=frozenset())


def contained(base, value):
    """Resolve a manifest path against ``base`` and refuse symlinks or anything outside it."""
    path = Path(value)
    path = path if path.is_absolute() else base / path
    if path.is_symlink() or not path.resolve().is_relative_to(base) or path.resolve() == base:
        raise ValueError(f"Storage path must stay inside the ingestion directory: {path}")
    return path


def specification(manifest_path, snapshot_date=None, document=None):
    """Everything needed to verify, archive or restore one snapshot, read from its manifest.

    Accepts both layouts: a historical dataset manifest with a ``snapshots`` list
    (``snapshot_date`` required) and a single-run ``metadata/manifest.json``.
    Lineage member names are checked here so later steps can trust them as paths.
    """
    path = Path(manifest_path).resolve()
    document = document if document is not None else json.loads(path.read_text())
    base = path.parent.parent if path.parent.name == "metadata" else path.parent
    if "snapshots" in document:
        if snapshot_date is None:
            raise ValueError("Specify a date for a historical dataset")
        day = date.fromisoformat(str(snapshot_date)).isoformat()
        rows = [r for r in document["snapshots"] if r["snapshot_date"] == day]
        if len(rows) != 1:
            raise ValueError("Snapshot date is missing or not unique")
        row = rows[0]
        lineage_path = path.parent / "lineage" / f"{day}.json"
        validation = contained(base, row["validation_report"]) if row.get("validation_report") else None
    else:
        row = document
        day = date.fromisoformat(row["snapshot_date"]).isoformat()
        if snapshot_date is not None and str(snapshot_date) != day:
            raise ValueError("Snapshot date differs from the manifest")
        lineage_path = path.parent / "lineage.json"
        validation = path.parent / "validation.json"
    if row.get("status") not in GOOD:
        raise ValueError("Only complete, validated snapshots can use ZIP storage")
    xml = contained(base, row["xml_directory"])
    lineage = json.loads(lineage_path.read_text())
    members = set()
    for item in lineage:
        member = item["member"]
        relative = PurePosixPath(member)
        if (not member or member != relative.as_posix() or relative.is_absolute()
                or ".." in relative.parts or "\\" in member or re.match(r"^[A-Za-z]:", member)
                or member in members or not relative.name.lower().endswith(".xml")):
            raise ValueError(f"Unsafe or duplicate lineage member: {member}")
        if not re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) or type(item["bytes"]) is not int or item["bytes"] < 0:
            raise ValueError(f"Invalid source hash or size: {member}")
        members.add(member)
    if not members:
        raise ValueError("Cannot archive an empty lineage")
    archive = contained(base, row["combined_zip"]) if row.get("combined_zip") else None
    if archive and archive.resolve().is_relative_to(xml.resolve()):
        raise ValueError("ZIP must be outside the extracted XML directory")
    return {"path": path, "document": document, "row": row, "base": base, "day": day,
            "xml": xml, "lineage": lineage, "validation": validation, "archive": archive}


@contextmanager
def snapshot_lock(spec):
    """Exclusive, non-blocking lock per XML directory for archive, restore and cleanup."""
    # Coordinate clones using the same XML paths, without modifying source evidence.
    directory = Path(tempfile.gettempdir()) / f"dat535-bronze-locks-{os.getuid()}"
    directory.mkdir(mode=0o700, exist_ok=True)
    lock = directory / (hashlib.sha256(str(spec["xml"].resolve()).encode()).hexdigest() + ".lock")
    with lock.open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("Snapshot is already being read or migrated; retry after that job finishes") from error
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def verify_xml(spec, allow_missing=False):
    """Check extracted XML against lineage; return file signatures to detect later changes.

    Unexpected files are refused rather than removed. With ``allow_missing``,
    absent members are skipped (a partially restored or already released copy).
    """
    root = spec["xml"]
    expected = {i["member"] for i in spec["lineage"]}
    if root.exists():
        for path in root.rglob("*"):
            if path.is_symlink() or (not path.is_dir() and path.relative_to(root).as_posix() not in expected):
                raise RuntimeError(f"Unexpected file in XML directory; left intact: {path}")
    signatures = {}
    for item in spec["lineage"]:
        path = root / item["member"]
        if not path.exists() and allow_missing:
            continue
        if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
            raise RuntimeError(f"Unsafe XML path: {path}")
        before = path.stat()
        if not stat.S_ISREG(before.st_mode) or before.st_size != item["bytes"] or sha256_file(path) != item["sha256"]:
            raise RuntimeError(f"XML differs from source lineage: {path}")
        signature = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        after = path.stat()
        if signature != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise RuntimeError(f"XML changed during verification: {path}")
        signatures[path] = signature
    return signatures


def verify_zip(spec, extract_to=None):
    """Check the retained ZIP's hash, members, sizes, CRCs and XML hashes against lineage.

    With ``extract_to``, each member is also written there while it is hashed,
    so a restore never needs a second pass over the archive.
    """
    archive = spec["archive"]
    expected_sha = spec["row"].get("combined_zip_sha256")
    if archive is None or archive.is_symlink() or not archive.is_file():
        raise RuntimeError("Retained ZIP is missing")
    before = archive.stat()
    if not expected_sha or sha256_file(archive) != expected_sha:
        raise RuntimeError(f"ZIP checksum differs from its manifest: {archive}")
    expected = {i["member"]: i for i in spec["lineage"]}
    with zipfile.ZipFile(archive) as source:
        names = [i.filename for i in source.infolist()]
        if len(names) != len(set(names)) or set(names) != set(expected):
            raise RuntimeError("ZIP members differ from source lineage")
        for info in source.infolist():
            item = expected[info.filename]
            if (info.file_size != item["bytes"] or stat.S_ISLNK(info.external_attr >> 16)
                    or ("source_crc32" in item and info.CRC != item["source_crc32"])):
                raise RuntimeError(f"ZIP member size/CRC/type differs from lineage: {info.filename}")
            destination = extract_to / info.filename if extract_to is not None else None
            if destination:
                destination.parent.mkdir(parents=True, exist_ok=True)
            digest = hashlib.sha256()
            with source.open(info) as stream:
                target = destination.open("xb") if destination else None
                try:
                    for block in iter(lambda: stream.read(BLOCK_SIZE), b""):
                        digest.update(block)
                        if target:
                            target.write(block)
                finally:
                    if target:
                        target.close()
            if digest.hexdigest() != item["sha256"]:
                raise RuntimeError(f"ZIP XML hash differs from source lineage: {info.filename}")
    after = archive.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise RuntimeError("ZIP changed during verification")


def remove_xml(spec, signatures):
    # Only known, verified files are unlinked. Never recursively delete a source tree.
    for path, before in signatures.items():
        current = path.lstat()
        if before != (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns):
            raise RuntimeError(f"XML changed before cleanup; left intact: {path}")
    for path in signatures:
        path.unlink()
    root = spec["xml"]
    if root.exists():
        for directory in sorted((p for p in root.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
            directory.rmdir()
        root.rmdir()


def retain_snapshot(manifest_path, snapshot_date=None, apply=False, document=None):
    """Verify/build a compact ZIP, commit its metadata, then release the XML copy."""
    spec = specification(manifest_path, snapshot_date, document)
    with snapshot_lock(spec):
        # Re-read after locking: another migration may have finished just before us.
        spec = specification(manifest_path, snapshot_date, document)
        row = spec["row"]
        report = {"manifest": str(spec["path"]), "date": spec["day"], "applied": apply,
                  "xml_bytes": sum(i["bytes"] for i in spec["lineage"])}
        if row.get("raw_storage") == "zip":
            verify_zip(spec)
            signatures = verify_xml(spec, allow_missing=True)
            if apply:
                remove_xml(spec, signatures)
            return {**report, "already_zip": True, "zip_bytes": spec["archive"].stat().st_size}
        validation = json.loads(spec["validation"].read_text()) if spec["validation"] else {}
        if validation.get("status") not in ("passed", "passed_with_warnings") or validation.get("snapshot_date") != spec["day"]:
            raise RuntimeError("A matching passed validation report is required before migration")
        signatures = verify_xml(spec)
        existing = spec["archive"]
        if existing and existing.exists():
            verify_zip(spec)
        elif not apply:
            return {**report, "already_zip": False, "zip_bytes": None}
        else:
            if shutil.disk_usage(spec["base"]).free < 256 * 1024**2:
                raise RuntimeError("Less than 256 MiB free: cannot safely build another ZIP")
            archive = contained(spec["base"], existing or spec["base"] / "zip" / f"selected_netex_{spec['day'].replace('-', '')}.zip")
            if archive.exists():
                # Recover a crash after publishing the ZIP but before committing
                # metadata. Adopt only a full byte-for-byte lineage match.
                candidate = {**spec, "archive": archive, "row": {**row, "combined_zip_sha256": sha256_file(archive)}}
                verify_zip(candidate)
                spec["archive"] = archive
                return _commit_archive(spec, signatures, report)
            archive.parent.mkdir(parents=True, exist_ok=True)
            partial = archive.with_name(f".{archive.name}-{uuid4().hex}.part")
            try:
                with zipfile.ZipFile(partial, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=4) as target:
                    for item in spec["lineage"]:
                        target.write(spec["xml"] / item["member"], arcname=item["member"])
                # Check the new ZIP before publishing it or changing the manifest.
                candidate = {**spec, "archive": partial, "row": {**row, "combined_zip_sha256": sha256_file(partial)}}
                verify_zip(candidate)
                # Link instead of replace: an unrelated existing ZIP is never overwritten.
                os.link(partial, archive)
            finally:
                partial.unlink(missing_ok=True)
            spec["archive"] = archive
        if not apply:
            return {**report, "already_zip": False, "zip_bytes": spec["archive"].stat().st_size}
        return _commit_archive(spec, signatures, report)


def _commit_archive(spec, signatures, report):
    """Record a verified ZIP in the manifest (keeping a backup copy), then release the XML."""
    archive, row = spec["archive"], spec["row"]
    report.update(already_zip=False, zip_bytes=archive.stat().st_size)
    with archive.open("rb") as stream:
        os.fsync(stream.fileno())
    directory = os.open(archive.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    # Verify XML again after compression; preserve any file modified meanwhile.
    if signatures != verify_xml(spec):
        raise RuntimeError("XML changed while its archive was being created; left intact")
    backup = spec["path"].parent / "storage_backups"
    backup.mkdir(exist_ok=True)
    write_json(backup / f"{uuid4().hex}-manifest.json", spec["document"])
    new_sha = sha256_file(archive)
    if row.get("combined_zip_sha256") and row["combined_zip_sha256"] != new_sha:
        row["previous_combined_zip_sha256"] = row["combined_zip_sha256"]
    row.update(raw_storage="zip", xml_only=False, combined_zip=str(archive.relative_to(spec["base"])),
               combined_zip_sha256=new_sha, combined_zip_bytes=archive.stat().st_size,
               combined_zip_removed=False)
    if "snapshots" in spec["document"]:
        spec["document"]["combined_zip_bytes"] = sum(r.get("combined_zip_bytes", 0) for r in spec["document"]["snapshots"])
    # Metadata switches first. An interrupted cleanup leaves a recoverable XML cache.
    write_json(spec["path"], spec["document"])
    remove_xml(spec, signatures)
    return report


@contextmanager
def snapshot_xml(manifest_path, snapshot_date=None):
    """Restore to the original paths under a lock; remove only verified ZIP-backed XML."""
    spec = specification(manifest_path, snapshot_date)
    key = str(spec["xml"].resolve())
    if key in ACTIVE.get():
        yield
        return
    with snapshot_lock(spec):
        spec = specification(manifest_path, snapshot_date)
        if spec["row"].get("raw_storage") != "zip":
            token = ACTIVE.set(ACTIVE.get() | {key})
            try:
                yield
            finally:
                ACTIVE.reset(token)
            return
        signatures = verify_xml(spec, allow_missing=True)
        # Space for a complete extraction plus Spark/system headroom. A temporary
        # tree prevents corrupt ZIPs from leaving a half-restored source directory.
        needed = sum(i["bytes"] for i in spec["lineage"]) + 1024**3
        if shutil.disk_usage(spec["base"]).free < needed:
            raise RuntimeError(f"Not enough space to restore snapshot {spec['day']} with 1 GiB headroom")
        spec["xml"].parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".zip-restore-", dir=spec["xml"].parent) as temporary:
            restored = Path(temporary) / "snapshot"
            restored.mkdir()
            verify_zip(spec, restored)
            if signatures != verify_xml(spec, allow_missing=True):
                raise RuntimeError("XML cache changed during restoration; left intact")
            spec["xml"].mkdir(exist_ok=True)
            for item in spec["lineage"]:
                target = spec["xml"] / item["member"]
                if not target.exists():
                    target.parent.mkdir(parents=True, exist_ok=True)
                    os.link(restored / item["member"], target)
        token = ACTIVE.set(ACTIVE.get() | {key})
        try:
            yield
        finally:
            ACTIVE.reset(token)
            # Do not release the working copy if the retained archive was damaged
            # while Spark (or an independent source check) was using the XML.
            verify_zip(spec)
            remove_xml(spec, verify_xml(spec))


def with_snapshot_xml(function):
    """Keep public processing signatures while holding source files until Spark finishes."""
    signature = inspect.signature(function)
    @wraps(function)
    def run(*args, **kwargs):
        arguments = signature.bind(*args, **kwargs).arguments
        manifest = arguments.get("manifest_path", arguments.get("manifest"))
        day = arguments.get("snapshot_date", arguments.get("day"))
        with snapshot_xml(manifest, day):
            return function(*args, **kwargs)
    return run


def migrate(root, apply=False):
    """Move every validated snapshot under ``root`` to ZIP storage (dry run unless ``apply``)."""
    root = Path(root).resolve()
    result = {"applied": apply, "snapshots": [], "skipped": 0, "problems": []}
    for path in sorted(root.rglob("manifest.json")):
        try:
            document = json.loads(path.read_text())
            for row in document.get("snapshots", [document]):
                if row.get("status") not in GOOD or not row.get("xml_directory"):
                    result["skipped"] += 1
                    continue
                print(f"{'Migrating' if apply else 'Checking'} {row['snapshot_date']}: {path}", flush=True)
                result["snapshots"].append(retain_snapshot(path, row["snapshot_date"], apply, document))
        except (OSError, ValueError, KeyError, TypeError, RuntimeError, zipfile.BadZipFile) as error:
            result["problems"].append(f"Left source evidence intact {path}: {error}")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-root", type=Path, required=True)
    parser.add_argument("--apply", action="store_true", help="Create verified ZIPs and release their extracted XML")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if not args.bronze_root.is_dir():
        parser.error("Bronze root does not exist")
    if args.report and args.report.exists():
        parser.error("Report exists; choose a new name")
    report = migrate(args.bronze_root, args.apply)
    print(json.dumps(report, indent=2))
    if args.report:
        write_json(args.report, report)
    return 1 if report["problems"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
