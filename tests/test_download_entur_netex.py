"""Regression tests for the shared Entur download helpers."""

import base64
import contextlib
from datetime import date
import hashlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from pipeline.ingestion import download_entur_netex as downloader


def archive_bytes(description="current"):
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("KOL_line.xml", f"<line>{description}</line>")
        archive.writestr("_stops.xml", "<stops/>")
    return data.getvalue()


def metadata(data):
    return {"generation": "12345", "size": str(len(data)),
            "md5Hash": base64.b64encode(hashlib.md5(data).digest()).decode()}


class Response(io.BytesIO):
    status = 200


class DownloadTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.raw = Path(temp.name) / "national/netex_20260115.zip"

    def fetch(self, data, identity):
        with patch.object(downloader.urllib.request, "urlopen", side_effect=lambda *a, **k: Response(data)) as media, \
                contextlib.redirect_stdout(io.StringIO()):
            checksum = downloader.download("https://example.invalid/x?generation=12345", self.raw, identity)
        return checksum, media

    def test_valid_download_is_saved_and_verified_copy_is_reused(self):
        data = archive_bytes()
        checksum, media = self.fetch(data, metadata(data))
        self.assertEqual(checksum, hashlib.sha256(data).hexdigest())
        self.assertEqual(self.raw.read_bytes(), data)
        self.assertIn("generation=12345", media.call_args.args[0].full_url)
        _, media = self.fetch(data, metadata(data))
        media.assert_not_called()

    def test_existing_source_mismatch_is_not_reused_or_overwritten(self):
        stale = archive_bytes("stale")
        self.raw.parent.mkdir(parents=True)
        self.raw.write_bytes(stale)
        updated = archive_bytes("updated")
        with self.assertRaisesRegex(RuntimeError, "Source size/checksum mismatch"):
            self.fetch(updated, metadata(updated))
        self.assertEqual(self.raw.read_bytes(), stale)

    def test_checksum_failure_after_download_keeps_the_downloaded_bytes(self):
        data = archive_bytes()
        wrong = {**metadata(data), "md5Hash": metadata(b"other")["md5Hash"]}
        with self.assertRaisesRegex(RuntimeError, "Source size/checksum mismatch"):
            self.fetch(data, wrong)
        self.assertEqual(self.raw.read_bytes(), data)

    def test_existing_partial_download_blocks_a_retry_into_the_same_place(self):
        partial = self.raw.with_suffix(".part")
        partial.parent.mkdir(parents=True)
        partial.write_bytes(b"interrupted")
        with self.assertRaises(FileExistsError):
            self.fetch(archive_bytes(), None)
        self.assertEqual(partial.read_bytes(), b"interrupted")

    def test_month_selection_crosses_years_and_rejects_reversed_ranges(self):
        months = downloader.month_range(date(2025, 11, 1), date(2026, 2, 1), 15)
        self.assertEqual(months, [date(2025, 11, 15), date(2025, 12, 15), date(2026, 1, 15), date(2026, 2, 15)])
        with self.assertRaises(ValueError):
            downloader.month_range(date(2026, 2, 1), date(2026, 1, 1), 15)
        with self.assertRaises(ValueError):
            downloader.parse_month("2026-1")


if __name__ == "__main__":
    unittest.main()
