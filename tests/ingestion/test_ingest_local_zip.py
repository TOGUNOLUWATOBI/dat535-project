from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import date
import hashlib
import io
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch
import warnings
import xml.etree.ElementTree as ET
import zipfile

from pipeline.ingestion import ingest_local_zip as uploader
from pipeline.ingestion.generate_provider_dataset import selected_member
from tests.ingestion.test_generate_provider_dataset import document, fixture


class ManualZipIngestionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source.zip"
        fixture(self.source)
        self.output = self.root / "bronze/first"
        self.day = date(2026, 1, 15)
        self.stdout = patch("sys.stdout", new=io.StringIO())
        self.stdout.start()
        self.addCleanup(self.stdout.stop)

    def import_zip(self, source=None, output=None, day=None, providers=("KOL", "AKT")):
        return uploader.ingest_zip(source or self.source, day or self.day,
                                   output or self.output, providers)

    def manifest(self, output=None):
        return json.loads(((output or self.output) / "metadata/manifest.json").read_text())

    def rewrite(self, transform):
        with zipfile.ZipFile(self.source) as archive:
            members = [(i.filename, archive.read(i)) for i in archive.infolist()]
        with zipfile.ZipFile(self.source, "w", compression=zipfile.ZIP_STORED) as archive:
            for name, content in transform(members):
                archive.writestr(name, content)

    def test_national_upload_keeps_input_and_raw_copy_and_preserves_selected_xml(self):
        original = self.source.read_bytes()
        with patch("urllib.request.urlopen", side_effect=AssertionError("Manual imports must be offline")):
            manifest = self.import_zip()
        self.assertEqual(manifest["status"], "complete")
        self.assertEqual(manifest["ingestion_method"], "manual_zip")
        self.assertEqual(manifest["source_sha256"], hashlib.sha256(original).hexdigest())
        self.assertEqual(self.source.read_bytes(), original)
        self.assertEqual((self.output / manifest["uploaded_archive"]).read_bytes(), original)
        lineage = json.loads((self.output / "metadata/lineage.json").read_text())
        with zipfile.ZipFile(self.source) as source, zipfile.ZipFile(self.output / manifest["combined_zip"]) as compact:
            self.assertNotIn("OTHER_line.xml", compact.namelist())
            self.assertNotIn("_OTHER_shared_data.xml", compact.namelist())
            self.assertIn("_stops.xml", compact.namelist())
            self.assertIn("_KOL_flexible_shared_data.xml", compact.namelist())
            for item in lineage:
                self.assertFalse(Path(item["xml_path"]).is_absolute())
                self.assertEqual((self.output / item["xml_path"]).read_bytes(), source.read(item["member"]))
                self.assertEqual(compact.read(item["member"]), source.read(item["member"]))

    def test_selected_provider_zip_is_also_accepted(self):
        self.rewrite(lambda members: [(n, b) for n, b in members if selected_member(n, ["KOL", "AKT"])])
        result = self.import_zip()
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["xml_files"], 6)

    def test_renamed_repeat_creates_metadata_only_and_points_to_unchanged_original(self):
        first = self.import_zip()
        before = {str(p.relative_to(self.output)): p.read_bytes() for p in self.output.rglob("*") if p.is_file()}
        renamed = self.root / "renamed.zip"
        shutil.copyfile(self.source, renamed)
        second = self.root / "bronze/second"
        with patch.object(uploader, "extract_snapshot", side_effect=AssertionError("Duplicate must not extract")):
            duplicate = self.import_zip(renamed, second, providers=("AKT", "KOL", "AKT"))
        self.assertEqual(duplicate["status"], "duplicate")
        self.assertEqual(duplicate["ingestion_id"], first["ingestion_id"])
        self.assertEqual((second / duplicate["duplicate_of"]).resolve(), (self.output / "metadata/manifest.json").resolve())
        self.assertEqual([p.name for p in second.iterdir()], ["metadata"])
        after = {str(p.relative_to(self.output)): p.read_bytes() for p in self.output.rglob("*") if p.is_file()}
        self.assertEqual(before, after)

    def test_date_and_provider_selection_are_part_of_duplicate_identity(self):
        self.import_zip()
        different_date = self.import_zip(output=self.root / "bronze/next-date", day=date(2026, 1, 16))
        different_selection = self.import_zip(output=self.root / "bronze/kol-only", providers=("KOL",))
        self.assertEqual(different_date["status"], "complete")
        self.assertEqual(different_selection["status"], "complete")
        self.assertEqual(different_selection["providers"], ["KOL"])

    def test_duplicate_does_not_hide_a_damaged_original(self):
        manifest = self.import_zip()
        xml = self.output / manifest["xml_directory"] / "KOL_line.xml"
        xml.write_text("changed after import")
        second = self.root / "bronze/second"
        with self.assertRaisesRegex(RuntimeError, "does not match recorded source lineage"):
            self.import_zip(output=second)
        self.assertEqual(self.manifest(second)["status"], "failed")
        self.assertEqual(xml.read_text(), "changed after import")

    def test_failure_preserves_original_copy_and_can_be_retried_without_catalog_poisoning(self):
        original = self.source.read_bytes()
        with patch.object(uploader, "validate_snapshot", side_effect=RuntimeError("interrupted validation")), \
                self.assertRaisesRegex(RuntimeError, "interrupted validation"):
            self.import_zip()
        failed = self.manifest()
        self.assertEqual(failed["status"], "failed")
        self.assertEqual((self.output / failed["uploaded_archive"]).read_bytes(), original)
        second = self.import_zip(output=self.root / "bronze/retry")
        self.assertEqual(second["status"], "complete")
        self.assertEqual(self.source.read_bytes(), original)
        self.assertEqual(self.manifest()["status"], "failed")

    def test_missing_reference_is_failed_and_raw_bytes_are_retained(self):
        fixture(self.source, missing=True)
        with self.assertRaisesRegex(RuntimeError, "Provider reference validation failed"):
            self.import_zip()
        manifest = self.manifest()
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual((self.output / manifest["uploaded_archive"]).read_bytes(), self.source.read_bytes())
        report = json.loads((self.output / "metadata/validation.json").read_text())
        self.assertEqual(report["unresolved_provider_references"][0]["ref"], "AKT:Operator:1")

    def test_source_repeats_remain_unchanged_and_are_warnings(self):
        def repeat(members):
            for name, content in members:
                if name == "AKT_line.xml":
                    repeated = b'<StopPoint id="AKT:StopPoint:repeat" version="1"/>' * 2
                    content = content.replace(b"</Line>", repeated + b"</Line>")
                yield name, content
        self.rewrite(repeat)
        manifest = self.import_zip()
        self.assertEqual(manifest["status"], "complete_with_warnings")
        self.assertEqual(manifest["duplicate_id_version_occurrences"], 1)
        with zipfile.ZipFile(self.source) as original:
            self.assertEqual((self.output / manifest["xml_directory"] / "AKT_line.xml").read_bytes(), original.read("AKT_line.xml"))

    def test_future_publication_is_not_accepted_under_an_earlier_claimed_date(self):
        self.import_zip()
        second = self.root / "bronze/wrong-date"
        with self.assertRaisesRegex(RuntimeError, "Historical mismatch"):
            self.import_zip(output=second, day=date(2026, 1, 1))
        self.assertEqual(self.manifest(second)["status"], "failed")

    def test_missing_stops_is_rejected(self):
        self.rewrite(lambda members: [(n, b) for n, b in members if n != "_stops.xml"])
        with self.assertRaisesRegex(RuntimeError, "missing the national stop document"):
            self.import_zip()
        self.assertEqual(self.manifest()["status"], "failed")

    def test_invalid_archive_and_xml_both_fail_with_original_upload_preserved(self):
        for label, data in (("not-zip", b"not a ZIP"), ("bad-xml", None)):
            with self.subTest(label=label):
                source = self.root / (label + ".zip")
                if data is not None:
                    source.write_bytes(data)
                else:
                    with zipfile.ZipFile(source, "w") as archive, zipfile.ZipFile(self.source) as original:
                        for name in original.namelist():
                            archive.writestr(name, b"<broken" if name == "AKT_line.xml" else original.read(name))
                output = self.root / "bronze" / label
                with self.assertRaises(zipfile.BadZipFile if data is not None else ET.ParseError):
                    self.import_zip(source, output)
                manifest = self.manifest(output)
                self.assertEqual(manifest["status"], "failed")
                self.assertEqual((output / manifest["uploaded_archive"]).read_bytes(), source.read_bytes())

    def test_crc_is_checked_even_for_unselected_members(self):
        self.rewrite(lambda members: members)
        with zipfile.ZipFile(self.source) as archive:
            info = archive.getinfo("OTHER_line.xml")
            offset = info.header_offset + 30 + len(info.filename.encode()) + len(info.extra)
        with self.source.open("r+b") as data:
            data.seek(offset)
            value = data.read(1)
            data.seek(offset)
            data.write(bytes([value[0] ^ 1]))
        original = self.source.read_bytes()
        with self.assertRaisesRegex(RuntimeError, "CRC"):
            self.import_zip()
        manifest = self.manifest()
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual((self.output / manifest["uploaded_archive"]).read_bytes(), original)

    def test_unsafe_paths_and_duplicate_names_are_rejected_before_extraction(self):
        for index, member in enumerate(("../KOL_escape.xml", "/KOL_escape.xml", "./KOL_alias.xml", "nested/../KOL_escape.xml", "KOL_line.xml")):
            with self.subTest(member=member):
                source = self.root / f"unsafe-{index}.zip"
                shutil.copyfile(self.source, source)
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    with zipfile.ZipFile(source, "a") as archive:
                        archive.writestr(member, document(""))
                output = self.root / "bronze" / f"unsafe-{index}"
                with patch.object(uploader, "extract_snapshot") as extract, self.assertRaisesRegex(RuntimeError, "Unsafe ZIP member|Duplicate ZIP member"):
                    self.import_zip(source, output)
                extract.assert_not_called()
                self.assertFalse((self.root / "KOL_escape.xml").exists())
                self.assertEqual(self.manifest(output)["status"], "failed")

    def test_concurrent_uploads_register_only_one_successful_import(self):
        barrier = threading.Barrier(2)
        def upload(name):
            barrier.wait(timeout=5)
            return self.import_zip(output=self.root / "bronze" / name)["status"]
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(upload, ("left", "right")))
        self.assertCountEqual(results, ["complete", "duplicate"])
        with closing(sqlite3.connect(self.root / "bronze/_manual_ingestions.sqlite3")) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM manual_imports").fetchone()[0], 1)

    def test_existing_output_and_catalog_pointing_at_input_are_refused(self):
        self.output.mkdir(parents=True)
        marker = self.output / "keep.txt"
        marker.write_text("keep original")
        with self.assertRaises(FileExistsError):
            self.import_zip()
        self.assertEqual(marker.read_text(), "keep original")
        original = self.source.read_bytes()
        with self.assertRaisesRegex(ValueError, "Catalog must be separate"):
            uploader.ingest_zip(self.source, self.day, self.root / "new", catalog=self.source)
        self.assertEqual(self.source.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
