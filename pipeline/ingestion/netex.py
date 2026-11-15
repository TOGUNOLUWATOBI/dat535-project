"""Shared NeTEx acquisition, provider selection and source validation.

All ingestion entry points use these functions. They preserve original XML
bytes, pin downloads to the inspected GCS generation and reject broken source
references before Spark staging. Independent quality traces deliberately use
their own XML reader in ``pipeline.quality.source_checks``.
"""

import base64
from collections import Counter
from contextlib import ExitStack
from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import shutil
import time
import xml.etree.ElementTree as ET
import urllib.request
import zipfile

from pipeline.common import BLOCK_SIZE, sha256_file

BASE_URL = (
    "https://storage.googleapis.com/download/storage/v1/b/sarpanit-production/o/"
    "netex%2Frb_norway-aggregated-netex-{date}.zip?alt=media"
)
METADATA_URL = (
    "https://storage.googleapis.com/storage/v1/b/sarpanit-production/o/"
    "netex%2Frb_norway-aggregated-netex-{date}.zip"
)
HEADERS = {"User-Agent": "dat535-kolumbus-research/1.0"}


def make_url(date_string: str) -> str:
    """Media URL for the export dated ``YYYYMMDD`` (callers append ``&generation=``)."""
    return BASE_URL.format(date=date_string)


def source_metadata(date_string: str):
    """Read immutable source identity before treating a filename as history."""
    request = urllib.request.Request(METADATA_URL.format(date=date_string), headers=HEADERS)
    with urllib.request.urlopen(request, timeout=35) as response:
        metadata = json.load(response)
    expected = f"netex/rb_norway-aggregated-netex-{date_string}.zip"
    if metadata.get("name") != expected or not metadata.get("generation"):
        raise RuntimeError(f"Unexpected source metadata for {expected}")
    if not metadata.get("md5Hash") or not str(metadata.get("size", "")).isdigit():
        raise RuntimeError(f"Missing source checksum or size for {expected}")
    return metadata


def file_checksums(path: Path):
    """(SHA-256 hex, base64 MD5) in one pass; GCS publishes MD5 in base64."""
    sha256 = hashlib.sha256()
    md5 = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(BLOCK_SIZE), b""):
            sha256.update(chunk)
            md5.update(chunk)
    return sha256.hexdigest(), base64.b64encode(md5.digest()).decode("ascii")


def validate_national_zip(path: Path, metadata=None):
    """Check size/MD5 against source metadata (when given) and every member CRC; return SHA-256."""
    sha256, md5 = file_checksums(path)
    if metadata and (path.stat().st_size != int(metadata["size"]) or md5 != metadata["md5Hash"]):
        raise RuntimeError(f"Source size/checksum mismatch: {path}; preserve it and use a new --output")
    with zipfile.ZipFile(path) as archive:
        bad = archive.testzip()
        if bad is not None:
            raise RuntimeError(f"CRC check failed on {bad} in {path}")
    return sha256


def download(url: str, path: Path, metadata=None):
    """Download to ``path`` (or verify an existing copy) and return its SHA-256.

    Bytes are streamed to a ``.part`` file and published only when complete.
    Failed or interrupted downloads are never deleted, so they stay available
    for investigation; an existing ``.part`` file therefore blocks a retry into
    the same location.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        checksum = validate_national_zip(path, metadata)
        print(f"  Verified existing national ZIP: {path}")
        return checksum
    partial = path.with_suffix(".part")
    if partial.exists():
        raise FileExistsError(f"Partial download already exists; preserve it and use a new --output: {partial}")
    request = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(request, timeout=100) as response, partial.open("wb") as f:
        if response.status != 200:
            raise RuntimeError(f"HTTP status: {response.status}")
        shutil.copyfileobj(response, f, length=BLOCK_SIZE)
    if not zipfile.is_zipfile(partial):
        raise RuntimeError("Downloaded content isn't a valid ZIP (possibly an error page).")
    # Publish complete downloaded bytes before validation so a failed size/CRC
    # check leaves the evidence available for investigation.
    partial.replace(path)
    checksum = validate_national_zip(path, metadata)
    print(f"  Saved {path} ({path.stat().st_size / (1024**2):.1f} MiB)")
    return checksum


def month_range(start: date, end: date, day: int):
    """Dates with the given day of month for every month from ``start`` to ``end`` inclusive."""
    first_index = start.year * 12 + start.month - 1
    last_index = end.year * 12 + end.month - 1
    if first_index > last_index:
        raise ValueError("--from-month must be on or before --to-month")
    if last_index - first_index > 59:
        raise ValueError("Selection exceeds 60 monthly snapshots; narrow the range")
    return [date(index // 12, index % 12 + 1, day)
            for index in range(first_index, last_index + 1)]


def parse_month(text: str) -> date:
    """``YYYY-MM`` as the first day of that month (also used as an argparse type)."""
    if len(text) != 7 or text[4] != "-":
        raise ValueError("Expected YYYY-MM")
    return date.fromisoformat(text + "-01")


NETEX = "http://www.netex.org.uk/netex"
ROOT = f"{{{NETEX}}}PublicationDelivery"


def provider_for(name, providers):
    """Provider code that owns an XML member (``KOL_*.xml`` or ``_KOL_*.xml``), or None."""
    base = Path(name).name.upper()
    if not base.endswith(".XML"):
        return None
    return next((p for p in providers if base.startswith((p + "_", "_" + p + "_"))), None)


def selected_member(name, providers):
    """True for the provider XML and the national ``_stops.xml`` kept in a snapshot."""
    base = Path(name).name.upper()
    return provider_for(name, providers) is not None or base == "_STOPS.XML"


def extract_snapshot(source, directory, zip_path, providers):
    """Copy the selected members of a national ZIP to XML (and optionally a compact ZIP).

    Every member is streamed once, written byte for byte and hashed on the way.
    The compact ZIP is built as ``.part`` and only published after its members
    match the national source exactly. Returns the lineage records.
    """
    if directory.exists() or (zip_path is not None and (zip_path.exists() or zip_path.with_suffix(".part").exists())):
        raise FileExistsError(f"Snapshot output already exists; use a new --output: {directory}")
    directory.mkdir(parents=True)
    partial = zip_path.with_suffix(".part") if zip_path is not None else None
    lineage = []
    with zipfile.ZipFile(source) as src, ExitStack() as stack:
        dst = stack.enter_context(zipfile.ZipFile(partial, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=4)) if partial else None
        selected = [i for i in src.infolist() if not i.is_dir() and selected_member(i.filename, providers)]
        if len({i.filename for i in selected}) != len(selected):
            raise RuntimeError("Source contains duplicate selected member names")
        for provider in providers:
            if not any(Path(i.filename).name.upper().startswith(provider + "_") for i in selected):
                raise RuntimeError(f"Source has no line XML for {provider}")
            if not any(Path(i.filename).name.upper() == f"_{provider}_SHARED_DATA.XML" for i in selected):
                raise RuntimeError(f"Source has no ordinary shared data for {provider}")
        if not any(Path(i.filename).name.upper() == "_STOPS.XML" for i in selected):
            raise RuntimeError("Source is missing the national stop document")
        for info in selected:
            relative = Path(info.filename)
            if relative.is_absolute() or ".." in relative.parts:
                raise RuntimeError(f"Unsafe ZIP member: {info.filename}")
            target = directory / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            digest = hashlib.sha256()
            written = 0
            with src.open(info) as infile, target.open("xb") as xml, ExitStack() as members:
                zipped = members.enter_context(dst.open(info.filename, "w")) if dst else None
                for chunk in iter(lambda: infile.read(BLOCK_SIZE), b""):
                    xml.write(chunk)
                    if zipped is not None:
                        zipped.write(chunk)
                    digest.update(chunk)
                    written += len(chunk)
            if written != info.file_size:
                raise RuntimeError(f"Uncompressed size mismatch: {info.filename}")
            lineage.append({"member": info.filename, "xml_path": str(target),
                            "provider": provider_for(info.filename, providers),
                            "bytes": written, "source_crc32": info.CRC, "sha256": digest.hexdigest()})
    if partial is None:
        return lineage
    partial.replace(zip_path)
    with zipfile.ZipFile(source) as src, zipfile.ZipFile(zip_path) as dst:
        expected = sorted((i.filename, i.CRC, i.file_size) for i in src.infolist()
                          if not i.is_dir() and selected_member(i.filename, providers))
        actual = sorted((i.filename, i.CRC, i.file_size) for i in dst.infolist())
        if actual != expected:
            raise RuntimeError("Combined ZIP members do not match the national source")
        bad = dst.testzip()
        if bad is not None:
            raise RuntimeError(f"Combined ZIP CRC failure: {bad}")
    return lineage


def verify_materialized_snapshot(directory, zip_path, lineage):
    """Resume only when both stored representations still match source lineage."""
    for item in lineage:
        path = directory / item["member"]
        if not path.is_file() or path.stat().st_size != item["bytes"] or sha256_file(path) != item["sha256"]:
            raise RuntimeError(f"Existing XML does not match recorded source lineage: {path}")
    if zip_path is None:
        return
    expected = sorted((i["member"], i["source_crc32"], i["bytes"]) for i in lineage)
    with zipfile.ZipFile(zip_path) as archive:
        actual = sorted((i.filename, i.CRC, i.file_size) for i in archive.infolist())
        if actual != expected or archive.testzip() is not None:
            raise RuntimeError(f"Existing combined ZIP does not match source lineage: {zip_path}")


def validate_snapshot(directory, lineage, providers, snapshot_date):
    """Check full XML and provider refs without mixing definitions across dates.

    Explicit versions must match within the same XML document. Unversioned
    provider references must resolve in the snapshot. Registry-only references
    are reported separately. This is not XSD or full calendar validation.
    """
    ids = set()
    references = Counter()
    registry_refs = Counter()
    version_failures = []
    duplicate_pairs = Counter()
    duplicate_details = []
    summaries = []
    provider_counts = {p: Counter() for p in providers}
    last_progress = time.monotonic()
    for number, item in enumerate(lineage, 1):
        name = item["member"]
        owner = item["provider"]
        meta = {"member": name, "provider": owner, "profile": None,
                "publication_timestamp": None, "entity_counts": Counter(),
                "line_modes": Counter(), "composite_validity": []}
        local_pairs = set()
        local_refs = Counter()
        stack = []
        with (directory / name).open("rb") as f:
            for event, element in ET.iterparse(f, events=("start", "end")):
                tag = element.tag.rsplit("}", 1)[-1]
                if event == "start":
                    stack.append(element)
                    if len(stack) == 1:
                        if element.tag != ROOT:
                            raise RuntimeError(f"Unexpected XML root in {name}: {element.tag}")
                        meta["profile"] = element.get("version")
                    continue
                meta["entity_counts"][tag] += 1
                if owner:
                    provider_counts[owner][tag] += 1
                ident, version, ref = element.get("id"), element.get("version"), element.get("ref")
                if ident:
                    ids.add(ident)
                    if owner:
                        pair = (ident, version)
                        if pair in local_pairs:
                            duplicate_pairs[owner] += 1
                            duplicate_details.append({"provider": owner, "member": name, "tag": tag,
                                                      "id": ident, "version": version,
                                                      "order": element.get("order")})
                        local_pairs.add(pair)
                if ref is not None:
                    key = (owner, tag, ref, version)
                    if owner:
                        references[key] += 1
                        if version is not None:
                            local_refs[(tag, ref, version)] += 1
                    else:
                        registry_refs[(tag, ref)] += 1
                if tag == "PublicationTimestamp":
                    meta["publication_timestamp"] = (element.text or "").strip()
                if tag == "TransportMode" and len(stack) > 1 and stack[-2].tag.rsplit("}", 1)[-1] in ("Line", "FlexibleLine"):
                    meta["line_modes"][(element.text or "").strip()] += 1
                if tag in ("FromDate", "ToDate") and len(stack) >= 4 and stack[-2].tag.rsplit("}", 1)[-1] == "AvailabilityCondition" and stack[-4].tag.rsplit("}", 1)[-1] == "CompositeFrame":
                    meta["composite_validity"].append({tag: (element.text or "").strip()})
                stack.pop()
                if stack:
                    stack[-1].remove(element)
                element.clear()
        for (tag, ref, version), count in local_refs.items():
            if (ref, version) not in local_pairs:
                version_failures.append({"member": name, "tag": tag, "ref": ref,
                                         "version": version, "occurrences": count})
        if not meta["profile"] or not meta["publication_timestamp"]:
            raise RuntimeError(f"Missing XML profile or publication timestamp: {name}")
        publication = datetime.fromisoformat(meta["publication_timestamp"]).date()
        if owner and meta["entity_counts"].get("Line", 0) and publication > snapshot_date + timedelta(days=1):
            raise RuntimeError(f"Historical mismatch in {name}: {publication} after {snapshot_date}")
        summaries.append(meta)
        if time.monotonic() - last_progress >= 15:
            print(f"    XML scan: {number}/{len(lineage)} files", flush=True)
            last_progress = time.monotonic()
    missing = [{"provider": owner, "tag": tag, "ref": ref, "version": version, "occurrences": count}
               for (owner, tag, ref, version), count in references.items() if ref not in ids]
    registry_missing = [{"tag": tag, "ref": ref, "occurrences": count}
                        for (tag, ref), count in registry_refs.items() if ref not in ids]
    provider_reports = {}
    for provider in providers:
        files = [m for m in summaries if m["provider"] == provider]
        provider_reports[provider] = {
            "xml_files": len(files), "xml_bytes": sum(i["bytes"] for i in lineage if i["provider"] == provider),
            "entity_counts": provider_counts[provider],
            "line_modes": sum((m["line_modes"] for m in files), Counter()),
            "publication_dates": dict(Counter(m["publication_timestamp"][:10] for m in files)),
            "reference_occurrences": sum(count for (owner, _, _, _), count in references.items() if owner == provider)}
    # Raw source repeats must remain intact. They are an explicit source-quality
    # warning, not an extraction failure or permission to deduplicate source XML.
    passed = not missing and not version_failures
    status = "failed" if not passed else "passed_with_warnings" if duplicate_pairs else "passed"
    return {"status": status, "snapshot_date": snapshot_date.isoformat(),
            "scope": "XML well-formedness, root/profile presence, future ordinary-line publication dates, provider reference existence, exact same-file versioned references, and duplicate provider ID/version pairs. Not full XSD/profile/calendar semantic validation.",
            "xml_files": len(lineage), "total_xml_bytes": sum(i["bytes"] for i in lineage),
            "provider_xml_bytes": sum(i["bytes"] for i in lineage if i["provider"]),
            "unique_ids": len(ids), "providers": provider_reports,
            "provider_reference_occurrences": sum(references.values()),
            "unresolved_provider_references": missing,
            "same_file_version_failures": version_failures,
            "duplicate_provider_id_version_pairs": duplicate_pairs,
            "duplicate_provider_id_version_details": duplicate_details,
            "unresolved_registry_only_references": registry_missing, "members": summaries}
