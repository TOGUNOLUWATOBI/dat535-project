"""Historical publication selection and verified Bronze bytes, with no network access."""

import base64
from datetime import date
import hashlib
from pathlib import Path
import tempfile
import unittest
import zipfile

from pipeline.Bronze.Netex import bronze_entur_netex as bronze


class NetexDownloadTests(unittest.TestCase):
    def test_monthly_sample_includes_leap_day_and_cutoff_once(self):
        dates = bronze.iter_monthly_step_dates(2024, [2], cutoff_date=date(2024, 2, 29))
        self.assertEqual(dates, [date(2024, 2, d) for d in (1, 8, 15, 22, 29)])
        self.assertEqual(bronze.iter_monthly_step_dates(2026, [1], cutoff_date=date(2026, 1, 10)),
                         [date(2026, 1, d) for d in (1, 8, 10)])

    def test_invalid_month_selection_and_step_are_rejected(self):
        for months in ("0", "13", "4-2"):
            with self.subTest(months=months), self.assertRaises(ValueError):
                bronze.parse_months(months)
        with self.assertRaises(ValueError):
            bronze.iter_monthly_step_dates(2026, [1], step_days=0)

    def test_selects_latest_publication_before_day_end_without_future_data(self):
        versions = [
            {"generation": "future", "timeCreated": "2026-01-02T00:00:00Z"},
            {"generation": "early", "timeCreated": "2026-01-01T08:00:00Z"},
            {"generation": "latest", "timeCreated": "2026-01-01T22:00:00Z", "size": "123"},
            {"generation": "broken", "timeCreated": "not a timestamp"},
        ]
        chosen = bronze.resolve_publication_for_date(date(2026, 1, 1), versions)
        self.assertEqual(chosen["generation"], "latest")
        self.assertEqual(chosen["expected_size"], 123)
        self.assertIsNone(bronze.resolve_publication_for_date(date(2025, 12, 31), versions))

    def test_valid_archive_is_verified_and_tampering_prevents_reuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "source.zip"
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("KOL/timetable.xml", "<PublicationDelivery/>")
            content = path.read_bytes()
            md5 = base64.b64encode(hashlib.md5(content).digest()).decode()
            valid, error, sha, _ = bronze.validate_downloaded_archive(path, len(content), md5)
            self.assertTrue(valid, error)
            self.assertEqual(sha, hashlib.sha256(content).hexdigest())
            record = {"target_date": "2026-01-01", "status": "downloaded", "file": str(path), "sha256": sha}
            self.assertEqual(bronze.find_existing_download([record], "2026-01-01"), record)
            path.write_bytes(b"changed")
            self.assertIsNone(bronze.find_existing_download([record], "2026-01-01"))

    def test_wrong_size_checksum_and_non_xml_archive_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "source.zip"
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("readme.txt", "not a timetable")
            for size, checksum, reason in ((1, None, "Size mismatch"),
                                           (None, "wrong", "MD5 mismatch"),
                                           (None, None, "no .xml")):
                with self.subTest(reason=reason):
                    valid, error, _, _ = bronze.validate_downloaded_archive(path, size, checksum)
                    self.assertFalse(valid)
                    self.assertIn(reason, error)
