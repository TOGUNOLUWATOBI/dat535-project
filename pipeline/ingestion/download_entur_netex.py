"""HTTP and archive helpers for Entur's date-stamped national NeTEx exports.

Each daily export is a Google Cloud Storage object. Its metadata (generation,
MD5 and size) identifies the exact payload, so a download is always pinned to
the generation that was inspected and checked against that identity before it
is used. These helpers are shared by the historical generator, the scheduled
poller and monthly calendar support; they have no command line of their own.
"""

import base64
from datetime import date
import hashlib
import json
from pathlib import Path
import shutil
import urllib.request
import zipfile

from pipeline.common import BLOCK_SIZE

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
