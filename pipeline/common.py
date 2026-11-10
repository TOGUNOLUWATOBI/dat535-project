"""Small helpers shared by every pipeline stage: timestamps, hashes and JSON metadata."""

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

BLOCK_SIZE = 1024 * 1024


def utc_now():
    """Current UTC time as an ISO 8601 string, used in every manifest."""
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    """SHA-256 of a file's bytes, read in 1 MiB blocks so large XML/ZIPs stay out of memory."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(BLOCK_SIZE), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path, data):
    """Atomically replace ``path`` with pretty-printed JSON.

    The document is written to a uniquely named temporary file in the same
    directory, flushed to disk and then renamed over the target, so readers see
    either the previous or the new complete manifest, never a partial one.
    """
    path = Path(path)
    temporary = path.with_name(f".{path.name}-{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(data, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        # Persist the rename itself, not only the file contents.
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)
