import base64
from datetime import date, datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error
import zipfile

from pipeline.ingestion import poll_entur_netex as poller
from tests.ingestion.test_generate_provider_dataset import fixture


class ScheduledIngestionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source.zip"
        fixture(self.source)
        payload = self.source.read_bytes()
        self.identity = {
            "name": "netex/rb_norway-aggregated-netex-20260115.zip",
            "generation": "123", "size": str(len(payload)),
            "md5Hash": base64.b64encode(hashlib.md5(payload).digest()).decode("ascii"),
            "timeCreated": "2026-01-15T07:30:00Z",
        }
        self.output = self.root / "run"
        self.day = date(2026, 1, 15)
        self.addCleanup(patch.stopall)
        patch.object(poller, "source_metadata", return_value=self.identity).start()
        self.sleep = patch.object(poller.time, "sleep").start()
        patch("sys.stdout", new=io.StringIO()).start()

    def manifest(self):
        return json.loads((self.output / "metadata/manifest.json").read_text())

    def http_response(self):
        # Exercise the real HTTP downloader, source digest and national ZIP CRC
        # checks with fixture bytes instead of downloading hundreds of MB.
        response = io.BytesIO(self.source.read_bytes())
        response.status = 200
        return response

    def test_success_keeps_originals_pins_generation_and_writes_portable_lineage(self):
        with patch("urllib.request.urlopen", side_effect=lambda *a, **k: self.http_response()) as request:
            result = poller.ingest_snapshot(self.day, self.output)
        self.assertEqual(result["status"], "complete")
        self.assertIn("&generation=123", request.call_args.args[0].full_url)
        self.assertEqual((self.output / result["national_zip"]).read_bytes(), self.source.read_bytes())
        self.assertEqual(result["source_sha256"], hashlib.sha256(self.source.read_bytes()).hexdigest())
        self.assertEqual(result["snapshot_date"], "2026-01-15")
        lineage = json.loads((self.output / "metadata/lineage.json").read_text())
        with zipfile.ZipFile(self.source) as original, zipfile.ZipFile(self.output / result["combined_zip"]) as selected:
            self.assertNotIn("OTHER_line.xml", selected.namelist())
            self.assertIn("_stops.xml", selected.namelist())
            for item in lineage:
                self.assertFalse(Path(item["xml_path"]).is_absolute())
                self.assertEqual((self.output / item["xml_path"]).read_bytes(), original.read(item["member"]))
                self.assertEqual(selected.read(item["member"]), original.read(item["member"]))

    def test_failed_download_is_preserved_and_retry_uses_another_path(self):
        calls = []

        def interrupted_then_success(url, path, metadata):
            calls.append(path)
            if len(calls) == 1:
                path.parent.mkdir(parents=True)
                path.with_suffix(".part").write_bytes(b"interrupted source bytes")
                raise TimeoutError("connection timed out")
            return real_download(url, path, metadata)

        real_download = poller.download
        with patch.object(poller, "download", side_effect=interrupted_then_success), \
                patch("urllib.request.urlopen", side_effect=lambda *a, **k: self.http_response()):
            result = poller.ingest_snapshot(self.day, self.output)
        self.assertEqual(result["status"], "complete")
        self.assertNotEqual(calls[0], calls[1])
        self.assertEqual(calls[0].with_suffix(".part").read_bytes(), b"interrupted source bytes")
        self.assertTrue(calls[1].is_file())
        self.assertEqual(result["request_failures"][0]["phase"], "download")
        self.sleep.assert_called_once_with(2)

    def test_rate_limit_respects_retry_after_and_keeps_failure_log(self):
        error = urllib.error.HTTPError("url", 429, "rate limit", {"Retry-After": "7"}, None)
        with patch.object(poller, "source_metadata", side_effect=[error, self.identity]), \
                patch.object(poller, "download") as download:
            result = poller.ingest_snapshot(self.day, self.output, check_only=True)
        self.assertEqual(result["status"], "source_available")
        self.sleep.assert_called_once_with(7)
        download.assert_not_called()
        self.assertNotIn("combined_zip", result)
        self.assertFalse((self.output / "metadata/validation.json").exists())
        self.assertEqual(len(result["request_failures"]), 1)

    def test_missing_source_fails_without_fallback_or_download(self):
        error = urllib.error.HTTPError("url", 404, "not found", {}, None)
        with patch.object(poller, "source_metadata", side_effect=error) as metadata, \
                patch.object(poller, "download") as download, self.assertRaises(urllib.error.HTTPError):
            poller.ingest_snapshot(self.day, self.output)
        self.assertEqual(self.manifest()["status"], "failed")
        self.assertEqual(metadata.call_count, 1)
        download.assert_not_called()
        self.sleep.assert_not_called()

    def test_exhausted_server_retries_fail_and_preserve_attempt_errors(self):
        error = urllib.error.HTTPError("url", 503, "unavailable", {}, None)
        with patch.object(poller, "source_metadata", side_effect=error) as metadata, \
                self.assertRaises(urllib.error.HTTPError):
            poller.ingest_snapshot(self.day, self.output)
        self.assertEqual(metadata.call_count, 3)
        self.assertEqual(self.manifest()["status"], "failed")
        self.assertEqual(len(self.manifest()["request_failures"]), 3)

    def test_checksum_failure_is_fatal_and_preserves_downloaded_bytes(self):
        self.identity["md5Hash"] = "incorrect"
        with patch("urllib.request.urlopen", side_effect=lambda *a, **k: self.http_response()), \
                self.assertRaisesRegex(RuntimeError, "Source size/checksum mismatch"):
            poller.ingest_snapshot(self.day, self.output)
        result = self.manifest()
        self.assertEqual(result["status"], "failed")
        self.assertEqual((self.output / result["national_zip"]).read_bytes(), self.source.read_bytes())
        self.assertFalse((self.output / "zip").exists())
        self.sleep.assert_not_called()

    def test_reference_failure_preserves_raw_data_without_marking_it_complete(self):
        fixture(self.source, missing=True)
        payload = self.source.read_bytes()
        self.identity.update(size=str(len(payload)), md5Hash=base64.b64encode(hashlib.md5(payload).digest()).decode("ascii"))
        with patch("urllib.request.urlopen", side_effect=lambda *a, **k: self.http_response()), \
                self.assertRaisesRegex(RuntimeError, "Provider reference validation failed"):
            poller.ingest_snapshot(self.day, self.output)
        result = self.manifest()
        self.assertEqual(result["status"], "failed")
        self.assertTrue((self.output / result["national_zip"]).exists())
        report = json.loads((self.output / "metadata/validation.json").read_text())
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["unresolved_provider_references"][0]["ref"], "AKT:Operator:1")

    def test_existing_run_is_never_overwritten(self):
        self.output.mkdir()
        evidence = self.output / "raw.txt"
        evidence.write_bytes(b"keep")
        with patch.object(poller, "source_metadata") as metadata, self.assertRaises(FileExistsError):
            poller.ingest_snapshot(self.day, self.output)
        metadata.assert_not_called()
        self.assertEqual(evidence.read_bytes(), b"keep")

    def test_previous_day_uses_oslo_time_across_midnight_and_dst(self):
        self.assertEqual(poller.previous_source_day(datetime(2026, 10, 22, 23, 30, tzinfo=timezone.utc)), date(2026, 10, 22))
        self.assertEqual(poller.previous_source_day(datetime(2026, 3, 29, 1, 30, tzinfo=timezone.utc)), date(2026, 3, 28))
        self.assertEqual(poller.previous_source_day(datetime(2026, 1, 1, 0, 30, tzinfo=timezone.utc)), date(2025, 12, 31))


if __name__ == "__main__":
    unittest.main()
