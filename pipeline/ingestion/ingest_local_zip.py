"""Import an existing national or KOL + AKT ZIP into a new Bronze run.

The input is never modified. Its date must be supplied explicitly; a filename
does not prove historical coverage. Successful imports are indexed in a small
SQLite metadata catalog so repeat uploads can refer to the validated original.
"""

import argparse
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sqlite3
import stat
import sys
from uuid import uuid4
from zoneinfo import ZoneInfo
import zipfile

from .download_entur_netex import file_checksums
from .generate_provider_dataset import (
    extract_snapshot,
    save_json,
    validate_snapshot,
    verify_materialized_snapshot,
)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def check_archive(path):
    """Check every member's CRC and reject ambiguous or unsafe member paths."""
    with zipfile.ZipFile(path) as archive:
        seen = set()
        for member in archive.infolist():
            name = member.filename
            parts = name.rstrip("/").split("/")
            if (not name or "\\" in name or "\x00" in member.orig_filename
                    or PurePosixPath(name).is_absolute()
                    or any(part in ("", ".", "..") for part in parts)
                    or re.match(r"^[A-Za-z]:", name)):
                raise RuntimeError(f"Unsafe ZIP member: {name!r}")
            if name in seen:
                raise RuntimeError(f"Duplicate ZIP member name: {name}")
            seen.add(name)
            if stat.S_ISLNK(member.external_attr >> 16):
                raise RuntimeError(f"ZIP contains a symbolic link: {name}")
        bad = archive.testzip()
        if bad is not None:
            raise RuntimeError(f"ZIP CRC check failed: {bad}")


def verify_previous(manifest_path, identity):
    """A duplicate must point at complete data that is still intact."""
    path = Path(manifest_path)
    previous = json.loads(path.read_text())
    if (previous.get("status") not in ("complete", "complete_with_warnings")
            or (previous.get("source_sha256"), previous.get("snapshot_date"),
                ",".join(sorted(previous.get("providers", [])))) != identity):
        raise RuntimeError(f"Catalog entry does not identify a complete matching import: {path}")
    root = path.parent.parent
    raw = root / previous["uploaded_archive"]
    if not raw.is_file() or file_checksums(raw)[0] != identity[0]:
        raise RuntimeError(f"Cataloged uploaded archive is missing or changed: {raw}")
    compact = root / previous["combined_zip"]
    if not compact.is_file() or file_checksums(compact)[0] != previous["combined_zip_sha256"]:
        raise RuntimeError(f"Cataloged selected ZIP is missing or changed: {compact}")
    lineage = json.loads((root / "metadata/lineage.json").read_text())
    verify_materialized_snapshot(root / previous["xml_directory"],
                                 compact, lineage)
    return path


def ingest_zip(source, day, output, providers=("KOL", "AKT"), catalog=None):
    """Preserve, extract and validate one upload; return its saved manifest.

    Duplicate identity is (original ZIP SHA-256, requested date, providers).
    A duplicate creates only a metadata observation pointing at the complete
    original. Failed imports are never registered as successful duplicates.
    """
    source = Path(source).resolve()
    output = Path(output).resolve()
    providers = list(dict.fromkeys(p.upper() for p in providers))
    if not providers or any(not re.fullmatch(r"[A-Z]{3}", p) for p in providers):
        raise ValueError("Provider codes must have exactly three letters")
    if day > datetime.now(ZoneInfo("Europe/Oslo")).date():
        raise ValueError("Cannot ingest a future source date")
    if not source.is_file():
        raise FileNotFoundError(f"Upload is not an existing file: {source}")
    catalog = Path(catalog).resolve() if catalog is not None else output.parent / "_manual_ingestions.sqlite3"
    if catalog == source or (catalog.exists() and catalog.samefile(source)) or catalog.is_relative_to(output):
        raise ValueError("Catalog must be separate from the input archive and per-run output directory")
    output.mkdir(parents=True, exist_ok=False)
    metadata = output / "metadata"
    metadata.mkdir()
    manifest_path = metadata / "manifest.json"
    manifest = {
        "schema_version": 1, "ingestion_method": "manual_zip", "status": "running",
        "started_at_utc": utc_now(), "snapshot_date": day.isoformat(),
        "providers": providers, "source_name": source.name,
    }
    save_json(manifest_path, manifest)
    connection = None
    try:
        sha256, md5 = file_checksums(source)
        size = source.stat().st_size
        identity = (sha256, day.isoformat(), ",".join(sorted(providers)))
        manifest.update(source_sha256=sha256, source_md5_base64=md5,
                        source_bytes=size, ingestion_id=sha256 + ":" + identity[2])
        save_json(manifest_path, manifest)
        catalog.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(catalog, timeout=120)
        connection.execute("""CREATE TABLE IF NOT EXISTS manual_imports (
            source_sha256 TEXT NOT NULL, snapshot_date TEXT NOT NULL,
            providers TEXT NOT NULL, manifest_path TEXT NOT NULL,
            PRIMARY KEY (source_sha256, snapshot_date, providers))""")
        # Serialize the check/import/register sequence, including concurrent
        # CLI or future GUI calls. A crashed process releases SQLite's lock;
        # its uncommitted registration is rolled back, preserving its files.
        connection.execute("BEGIN IMMEDIATE")
        previous = connection.execute(
            "SELECT manifest_path FROM manual_imports WHERE source_sha256=? AND snapshot_date=? AND providers=?",
            identity,
        ).fetchone()
        if previous:
            original = verify_previous(previous[0], identity)
            manifest.update(status="duplicate", duplicate_of=os.path.relpath(original, output),
                            completed_at_utc=utc_now())
            save_json(manifest_path, manifest)
            connection.commit()
            print(f"duplicate: {manifest_path} (original: {original})", flush=True)
            return manifest

        raw = output / "uploads/original.zip"
        raw.parent.mkdir()
        manifest["uploaded_archive"] = str(raw.relative_to(output))
        save_json(manifest_path, manifest)
        with source.open("rb") as infile, raw.open("xb") as outfile:
            shutil.copyfileobj(infile, outfile, length=1024 * 1024)
        if raw.stat().st_size != size or file_checksums(raw) != (sha256, md5):
            raise RuntimeError("Upload changed while being copied; saved bytes retained for inspection")
        print("Checking original uploaded ZIP and all member CRCs", flush=True)
        check_archive(raw)
        directory = output / "xml" / day.isoformat()
        compact = output / "zip" / f"{'_'.join(providers)}_netex_{day:%Y%m%d}.zip"
        compact.parent.mkdir()
        lineage = extract_snapshot(raw, directory, compact, providers)
        for item in lineage:
            item["xml_path"] = str(Path(item["xml_path"]).relative_to(output))
        save_json(metadata / "lineage.json", lineage)
        print(f"Validating all {len(lineage)} XML files and provider references", flush=True)
        report = validate_snapshot(directory, lineage, providers, day)
        save_json(metadata / "validation.json", report)
        if report["status"] not in ("passed", "passed_with_warnings"):
            raise RuntimeError("Provider reference validation failed; see metadata/validation.json")
        manifest.update(
            status="complete_with_warnings" if report["status"] == "passed_with_warnings" else "complete",
            xml_directory=str(directory.relative_to(output)), combined_zip=str(compact.relative_to(output)),
            combined_zip_sha256=file_checksums(compact)[0],
            xml_files=report["xml_files"], provider_xml_bytes=report["provider_xml_bytes"],
            total_xml_bytes=report["total_xml_bytes"],
            duplicate_id_version_occurrences=sum(report["duplicate_provider_id_version_pairs"].values()),
            completed_at_utc=utc_now(),
        )
        save_json(manifest_path, manifest)
        connection.execute("INSERT INTO manual_imports VALUES (?, ?, ?, ?)", (*identity, str(manifest_path)))
        connection.commit()
        print(f"{manifest['status']}: {manifest_path}", flush=True)
        return manifest
    except Exception as error:
        if connection is not None:
            connection.rollback()
        manifest.update(status="failed", completed_at_utc=utc_now(),
                        error=f"{type(error).__name__}: {error}")
        save_json(manifest_path, manifest)
        raise
    finally:
        if connection is not None:
            connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True, help="Existing national or selected-provider ZIP")
    parser.add_argument("--date", type=date.fromisoformat, required=True, help="Claimed source snapshot day YYYY-MM-DD")
    parser.add_argument("--providers", nargs="+", default=["KOL", "AKT"])
    parser.add_argument("--output", type=Path, help="New run directory; existing directories are never overwritten")
    parser.add_argument("--catalog", type=Path, help="Shared SQLite metadata catalog; default: OUTPUT_PARENT/_manual_ingestions.sqlite3")
    args = parser.parse_args()
    name = "manual-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    output = args.output or Path("data/bronze/entur") / name
    try:
        ingest_zip(args.source, args.date, output, args.providers, args.catalog)
    except Exception as error:
        print(f"Manual ingestion failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
