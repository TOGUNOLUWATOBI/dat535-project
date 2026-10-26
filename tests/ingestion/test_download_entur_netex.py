import base64
import contextlib
import csv
from datetime import date
import hashlib
import io
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from pipeline.ingestion import download_entur_netex as downloader


def archive_bytes(publication="2026-01-15T00:00:00", description="current"):
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("KOL_line.xml", f'''<PublicationDelivery xmlns="http://www.netex.org.uk/netex">
<PublicationTimestamp>{publication}</PublicationTimestamp><Description>{description}</Description>
</PublicationDelivery>''')
        archive.writestr("_KOL_shared_data.xml", "<shared/>")
        archive.writestr("_stops.xml", "<stops/>")
        archive.writestr("OTHER_line.xml", "<other/>")
    return data.getvalue()


class Response(io.BytesIO):
    status = 200


class DownloaderRegressionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.out = Path(self.temp.name)

    def metadata(self, data):
        return {"generation": "12345", "size": str(len(data)),
                "md5Hash": base64.b64encode(hashlib.md5(data).digest()).decode(),
                "timeCreated": "2026-01-15T07:30:00Z"}

    def run_main(self, data, dates=("20260115",), check=False):
        argv = ["download_entur_netex.py", "--dates", *dates,
                "--output", str(self.out)]
        argv += ["--check"] if check else ["--download", "--extract-kolumbus"]
        with patch.object(sys, "argv", argv), \
                patch.object(downloader, "source_metadata", return_value=self.metadata(data)), \
                patch.object(downloader, "probe", return_value=(200, str(len(data)), "")), \
                patch.object(downloader.urllib.request, "urlopen", side_effect=lambda *a, **kw: Response(data)) as media, \
                contextlib.redirect_stdout(io.StringIO()):
            result = downloader.main()
        return result, media

    def manifests(self):
        result = []
        for path in self.out.glob("manifests/*.csv"):
            with path.open(newline="") as f:
                result.append(list(csv.DictReader(f)))
        return result

    def test_duplicate_source_dates_rejected_before_media_download(self):
        result, media = self.run_main(archive_bytes(), dates=("20250115", "20250215"))
        self.assertEqual(result, 1)
        media.assert_not_called()
        rows = self.manifests()[0]
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(r["validation_status"] == "rejected_duplicate_source" for r in rows))

    def test_future_content_rejected_and_national_bytes_preserved(self):
        data = archive_bytes("2026-01-02T15:24:59")
        result, _ = self.run_main(data, dates=("20250115",))
        self.assertEqual(result, 1)
        raw = self.out / "national/rb_norway-aggregated-netex-20250115.zip"
        self.assertEqual(raw.read_bytes(), data)
        self.assertFalse(list(self.out.glob("kolumbus/*.zip")))
        row = self.manifests()[0][0]
        self.assertIn("Historical date mismatch", row["note"])
        self.assertEqual(row["raw_zip"], str(raw))

    def test_valid_download_pins_generation_and_reruns_preserve_manifest(self):
        legacy = self.out / "netex_manifest.csv"
        legacy.write_text("existing history\n")
        data = archive_bytes()
        result, media = self.run_main(data)
        self.assertEqual(result, 0)
        self.assertIn("generation=12345", media.call_args.args[0].full_url)
        raw = self.out / "national/rb_norway-aggregated-netex-20260115.zip"
        self.assertEqual(raw.read_bytes(), data)
        result, media = self.run_main(data)
        self.assertEqual(result, 0)
        media.assert_not_called()
        self.assertEqual(legacy.read_text(), "existing history\n")
        self.assertEqual(len(self.manifests()), 2)
        with zipfile.ZipFile(next(self.out.glob("kolumbus/*.zip"))) as archive:
            self.assertEqual(set(archive.namelist()), {"KOL_line.xml", "_KOL_shared_data.xml", "_stops.xml"})

    def test_stale_extraction_is_rejected_without_overwriting_it(self):
        target = self.out / "kolumbus/KOL_rb_norway-aggregated-netex-20260115.zip"
        target.parent.mkdir()
        stale = archive_bytes(description="stale")
        target.write_bytes(stale)
        result, _ = self.run_main(archive_bytes(description="updated"))
        self.assertEqual(result, 1)
        self.assertEqual(target.read_bytes(), stale)
        self.assertIn("does not match source", self.manifests()[0][0]["note"])

    def test_existing_national_source_mismatch_is_not_reused(self):
        raw = self.out / "national/rb_norway-aggregated-netex-20260115.zip"
        raw.parent.mkdir()
        stale = archive_bytes(description="stale")
        raw.write_bytes(stale)
        result, media = self.run_main(archive_bytes(description="updated"))
        self.assertEqual(result, 1)
        media.assert_not_called()
        self.assertEqual(raw.read_bytes(), stale)
        self.assertIn("Source size/checksum mismatch", self.manifests()[0][0]["note"])

    def test_existing_partial_and_extracted_outputs_are_preserved(self):
        source = self.out / "source.zip"
        source.write_bytes(archive_bytes())
        target = self.out / "target.zip"
        partial = target.with_suffix(".part")
        partial.write_bytes(b"interrupted extraction")
        with self.assertRaises(FileExistsError):
            downloader.extract_kolumbus(source, target)
        self.assertEqual(partial.read_bytes(), b"interrupted extraction")
        target.write_bytes(b"existing output")
        with self.assertRaises(FileExistsError):
            downloader.extract_kolumbus(source, target)
        self.assertEqual(target.read_bytes(), b"existing output")

    def test_check_only_does_not_claim_xml_was_validated(self):
        result, media = self.run_main(archive_bytes(), check=True)
        self.assertEqual(result, 0)
        media.assert_not_called()
        self.assertEqual(self.manifests()[0][0]["validation_status"], "metadata_only")


if __name__ == "__main__":
    unittest.main()
