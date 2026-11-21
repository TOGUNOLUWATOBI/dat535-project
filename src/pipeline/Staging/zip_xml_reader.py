"""Executor-side NeTEx ZIP archive and XML member reader.

Distributes selected Bronze NeTEx archive records across Spark executors,
opens ZIPs without collecting archives on the driver, reads and decodes one
uncompressed XML member at a time to bound retained memory, and yields tagged
member or failure records.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
import zipfile


def sha256_file(path: Path) -> str:
    """Compute SHA-256 hex digest in 1 MiB chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_zip_members_partition(records_iter):
    """Run on Spark executors; yield tagged tuples: (kind, payload_dict).

    ``kind`` is one of:
      - ``'member'``
      - ``'archive_failure'``
      - ``'member_failure'``
    """
    for rec in records_iter:
        zip_path_str = str(rec["file"])
        zip_path = Path(zip_path_str)
        base_ctx = {
            "snapshot_date": str(rec["target_date"]),
            "provider": str(rec.get("provider", "KOL")),
            "source_zip": zip_path_str,
            "source_zip_sha256": str(rec["sha256"]),
            "source_generation": str(rec.get("generation", "")),
            "publication_date": str(rec.get("publication_date", "")),
        }

        if not zip_path.is_file():
            yield (
                "archive_failure",
                {
                    **base_ctx,
                    "source_member": None,
                    "member_index": None,
                    "stage": "archive_open",
                    "error": f"FileNotFoundError: ZIP not found: {zip_path_str}",
                },
            )
            continue

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                xml_infos = [
                    info
                    for info in zf.infolist()
                    if not info.is_dir() and info.filename.lower().endswith(".xml")
                ]
                for idx, info in enumerate(xml_infos):
                    try:
                        with zf.open(info, "r") as member_stream:
                            raw_bytes = member_stream.read()
                        xml_text = raw_bytes.decode("utf-8")
                        member_sha256 = hashlib.sha256(raw_bytes).hexdigest()
                        is_stops = Path(info.filename).name == "_stops.xml"
                        yield (
                            "member",
                            {
                                **base_ctx,
                                "source_member": info.filename,
                                "member_index": idx,
                                "member_bytes": len(raw_bytes),
                                "source_xml_sha256": member_sha256,
                                "is_stops_file": is_stops,
                                "xml_content": xml_text,
                            },
                        )
                    except Exception as member_exc:
                        yield (
                            "member_failure",
                            {
                                **base_ctx,
                                "source_member": info.filename,
                                "member_index": idx,
                                "stage": "member_read",
                                "error": f"{type(member_exc).__name__}: {member_exc}",
                            },
                        )
        except Exception as zip_exc:
            yield (
                "archive_failure",
                {
                    **base_ctx,
                    "source_member": None,
                    "member_index": None,
                    "stage": "archive_open",
                    "error": f"{type(zip_exc).__name__}: {zip_exc}",
                },
            )