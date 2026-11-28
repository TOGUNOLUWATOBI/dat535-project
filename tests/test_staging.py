"""The current executor ZIP reader preserves provenance and reports isolated failures."""

import hashlib
from pathlib import Path
import tempfile
import unittest
import zipfile

from pipeline.Staging.zip_xml_reader import extract_zip_members_partition, sha256_file


class ZipReaderTests(unittest.TestCase):
    def record(self, path):
        return {"file": str(path), "target_date": "2026-01-01", "provider": "KOL",
                "sha256": sha256_file(path) if path.is_file() else "missing", "generation": "123"}

    def test_xml_content_order_hashes_and_stops_identity_are_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "archive with spaces æ.zip"
            members = [("folder/_stops.xml", b"<StopPlaces/>"), ("KOL.xml", "<Name>Øst</Name>".encode())]
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("notes.txt", "ignored")
                for name, content in members:
                    archive.writestr(name, content)
            rows = list(extract_zip_members_partition(iter([self.record(path)])))
            self.assertEqual([kind for kind, _ in rows], ["member", "member"])
            for index, ((_, row), (name, content)) in enumerate(zip(rows, members)):
                self.assertEqual((row["source_member"], row["member_index"]), (name, index))
                self.assertEqual(row["xml_content"], content.decode())
                self.assertEqual(row["source_xml_sha256"], hashlib.sha256(content).hexdigest())
                self.assertEqual(row["source_zip_sha256"], sha256_file(path))
                self.assertEqual(row["snapshot_date"], "2026-01-01")
                self.assertEqual(row["is_stops_file"], index == 0)

    def test_invalid_member_does_not_discard_later_valid_xml(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "source.zip"
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("bad.xml", b"\xff")
                archive.writestr("good.xml", b"<OK/>")
            rows = list(extract_zip_members_partition(iter([self.record(path)])))
            self.assertEqual([kind for kind, _ in rows], ["member_failure", "member"])
            self.assertIn("UnicodeDecodeError", rows[0][1]["error"])
            self.assertEqual(rows[1][1]["xml_content"], "<OK/>")

    def test_missing_and_corrupt_archives_are_reported_without_stopping_partition(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing, corrupt = Path(tmp) / "missing.zip", Path(tmp) / "bad.zip"
            corrupt.write_bytes(b"invalid ZIP")
            rows = list(extract_zip_members_partition(iter([self.record(missing), self.record(corrupt)])))
            self.assertEqual([kind for kind, _ in rows], ["archive_failure", "archive_failure"])
            self.assertIn("FileNotFoundError", rows[0][1]["error"])
            self.assertIn("BadZipFile", rows[1][1]["error"])
