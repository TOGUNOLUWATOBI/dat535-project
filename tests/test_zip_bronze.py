import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile
import zlib

from pipeline.storage.maintenance import discard_provider_zips
from pipeline.storage.zip_bronze import migrate, retain_snapshot, snapshot_xml, specification, verify_zip


class ZipBronzeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def fixture(self, name="one", historical=False):
        base = self.root / name
        xml = base / "xml/2026-01-15"
        xml.mkdir(parents=True)
        raw = {
            "nested/KOL_line.xml": b"<raw>original reference KOL:Line:1</raw>" * 100,
            "AKT_line.xml": b"<AKT>unchanged</AKT>" * 100,
            "_stops.xml": b"<StopPlace id='NSR:StopPlace:1'/>" * 100,
        }
        lineage = []
        for member, data in raw.items():
            path = xml / member
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            lineage.append(
                {
                    "member": member,
                    "bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "source_crc32": zlib.crc32(data),
                    "provider": None if member.startswith("_") else member[:3],
                }
            )
        metadata = base if historical else base / "metadata"
        metadata.mkdir(exist_ok=True)
        if historical:
            (metadata / "lineage").mkdir()
        lineage_path = metadata / "lineage/2026-01-15.json" if historical else metadata / "lineage.json"
        lineage_path.write_text(json.dumps(lineage))
        validation = {"status": "passed", "snapshot_date": "2026-01-15"}
        if historical:
            (base / "validation").mkdir()
        validation_path = base / "validation/2026-01-15.json" if historical else metadata / "validation.json"
        validation_path.write_text(json.dumps(validation))
        row = {
            "status": "validated" if historical else "complete",
            "snapshot_date": "2026-01-15",
            "xml_directory": str(xml.relative_to(base)),
            "combined_zip": None,
            "xml_only": True,
            "validation_report": str(validation_path.relative_to(base)),
        }
        document = (
            {"status": "complete", "providers": ["KOL", "AKT"], "snapshots": [row]}
            if historical
            else {**row, "providers": ["KOL", "AKT"]}
        )
        manifest = metadata / "manifest.json"
        manifest.write_text(json.dumps(document))
        return manifest, xml, raw

    def test_migration_preserves_all_bytes_metadata_and_paths_in_both_layouts(self):
        for historical in (False, True):
            with self.subTest(historical=historical):
                manifest, xml, raw = self.fixture(str(historical), historical)
                original = manifest.read_bytes()
                lineage = next(manifest.parent.glob("lineage*.json"), None)
                before = lineage.read_bytes() if lineage else None
                preview = retain_snapshot(manifest, "2026-01-15")
                self.assertFalse(preview["applied"])
                self.assertEqual(manifest.read_bytes(), original)
                self.assertTrue(xml.exists())
                retain_snapshot(manifest, "2026-01-15", apply=True)
                spec = specification(manifest, "2026-01-15")
                self.assertFalse(xml.exists())
                with zipfile.ZipFile(spec["archive"]) as archive:
                    self.assertEqual(set(archive.namelist()), set(raw))
                    for member, data in raw.items():
                        self.assertEqual(archive.read(member), data)
                self.assertEqual(
                    next(manifest.parent.glob("storage_backups/*")).read_text(),
                    json.dumps(json.loads(original), indent=2) + "\n",
                )
                if lineage:
                    self.assertEqual(lineage.read_bytes(), before)
                with snapshot_xml(manifest, "2026-01-15"):
                    for member, data in raw.items():
                        self.assertEqual((xml / member).read_bytes(), data)
                    with snapshot_xml(manifest, "2026-01-15"):
                        self.assertTrue(xml.exists())
                    self.assertTrue(xml.exists())
                self.assertFalse(xml.exists())
                self.assertTrue(retain_snapshot(manifest, "2026-01-15", apply=True)["already_zip"])
                self.assertEqual(discard_provider_zips(manifest.parent.parent, apply=True)["zip_copies"], 0)
                self.assertTrue(spec["archive"].exists())

    def test_changed_or_untracked_xml_is_never_deleted(self):
        for corruption in ("changed", "extra", "symlink"):
            with self.subTest(corruption=corruption):
                manifest, xml, raw = self.fixture(corruption)
                if corruption == "changed":
                    (xml / "AKT_line.xml").write_bytes(b"changed")
                elif corruption == "extra":
                    (xml / "notes.txt").write_text("keep this")
                else:
                    (xml / "AKT_line.xml").unlink()
                    (xml / "AKT_line.xml").symlink_to(xml / "_stops.xml")
                before = manifest.read_bytes()
                with self.assertRaises(RuntimeError):
                    retain_snapshot(manifest, apply=True)
                self.assertEqual(before, manifest.read_bytes())
                self.assertTrue(xml.exists())
                self.assertFalse(list(manifest.parent.parent.rglob("*.zip")))

    def test_unsafe_and_duplicate_lineage_is_refused(self):
        for member in ("../escape.xml", "/escape.xml", "C:/escape.xml", "x\\escape.xml", "AKT_line.xml"):
            with self.subTest(member=member):
                manifest, xml, _ = self.fixture(str(len(list(self.root.iterdir()))))
                path = manifest.parent / "lineage.json"
                lineage = json.loads(path.read_text())
                lineage[0]["member"] = member
                path.write_text(json.dumps(lineage))
                with self.assertRaises(ValueError):
                    retain_snapshot(manifest, apply=True)
                self.assertTrue(xml.exists())

    def test_interruption_and_processing_failure_leave_a_reusable_zip(self):
        manifest, xml, raw = self.fixture()
        retain_snapshot(manifest, apply=True)
        # A process killed during extraction can leave a correct partial cache.
        xml.mkdir()
        (xml / "AKT_line.xml").write_bytes(raw["AKT_line.xml"])
        with self.assertRaisesRegex(RuntimeError, "processing failed"):
            with snapshot_xml(manifest):
                with self.assertRaisesRegex(RuntimeError, "already being read"):
                    retain_snapshot(manifest, apply=True)
                self.assertEqual((xml / "_stops.xml").read_bytes(), raw["_stops.xml"])
                raise RuntimeError("processing failed")
        self.assertFalse(xml.exists())
        verify_zip(specification(manifest))
        with snapshot_xml(manifest):
            self.assertTrue(xml.exists())
        self.assertFalse(xml.exists())

    def test_corrupt_zip_or_cache_never_hides_source_damage(self):
        manifest, xml, raw = self.fixture()
        retain_snapshot(manifest, apply=True)
        spec = specification(manifest)
        original = spec["archive"].read_bytes()
        spec["archive"].write_bytes(b"damaged")
        with self.assertRaisesRegex(RuntimeError, "checksum"):
            with snapshot_xml(manifest):
                self.fail("Corrupt archive was exposed")
        self.assertFalse(xml.exists())
        spec["archive"].write_bytes(original)
        xml.mkdir()
        (xml / "AKT_line.xml").write_bytes(b"unrecorded change")
        with self.assertRaisesRegex(RuntimeError, "differs from source lineage"):
            with snapshot_xml(manifest):
                self.fail("Corrupt cache was reused")
        self.assertEqual((xml / "AKT_line.xml").read_bytes(), b"unrecorded change")
        self.assertEqual(spec["archive"].read_bytes(), original)

    def test_archive_member_hashes_are_checked_even_if_archive_checksum_matches(self):
        manifest, xml, raw = self.fixture()
        retain_snapshot(manifest, apply=True)
        spec = specification(manifest)
        with zipfile.ZipFile(spec["archive"], "w") as archive:
            for member, data in raw.items():
                archive.writestr(member, data.replace(b"unchanged", b"corrupted"))
        document = json.loads(manifest.read_text())
        document["combined_zip_sha256"] = hashlib.sha256(spec["archive"].read_bytes()).hexdigest()
        manifest.write_text(json.dumps(document))
        with self.assertRaisesRegex(RuntimeError, "differs from lineage"):
            with snapshot_xml(manifest):
                self.fail("Changed member was accepted")
        self.assertFalse(xml.exists())

    def test_failed_snapshots_uploads_and_other_hardlinks_are_preserved(self):
        manifest, xml, raw = self.fixture()
        failed, failed_xml, _ = self.fixture("failed")
        data = json.loads(failed.read_text())
        data["status"] = "failed"
        failed.write_text(json.dumps(data))
        upload = manifest.parent.parent / "uploads/original.zip"
        upload.parent.mkdir()
        upload.write_bytes(b"original uploaded archive")
        linked = self.root / "other.xml"
        os.link(xml / "_stops.xml", linked)
        result = migrate(self.root, apply=True)
        self.assertEqual(result["problems"], [])
        self.assertEqual(len(result["snapshots"]), 1)
        self.assertTrue(failed_xml.exists())
        self.assertEqual(upload.read_bytes(), b"original uploaded archive")
        self.assertEqual(linked.read_bytes(), raw["_stops.xml"])
        self.assertFalse(xml.exists())

    def test_low_disk_refuses_archive_creation_without_changing_the_source(self):
        manifest, xml, _ = self.fixture()
        before = manifest.read_bytes()
        with patch("pipeline.storage.zip_bronze.shutil.disk_usage") as disk:
            disk.return_value.free = 0
            with self.assertRaisesRegex(RuntimeError, "256 MiB"):
                retain_snapshot(manifest, apply=True)
        self.assertEqual(manifest.read_bytes(), before)
        self.assertTrue(xml.exists())

    def test_zip_published_before_metadata_commit_is_verified_and_adopted_on_retry(self):
        manifest, xml, raw = self.fixture()
        original = manifest.read_bytes()
        with patch("pipeline.storage.zip_bronze._commit_archive", side_effect=RuntimeError("interrupted")):
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                retain_snapshot(manifest, apply=True)
        self.assertEqual(manifest.read_bytes(), original)
        self.assertTrue(xml.exists())
        archive = next(manifest.parent.parent.glob("zip/*.zip"))
        before = archive.read_bytes()
        retain_snapshot(manifest, apply=True)
        self.assertEqual(archive.read_bytes(), before)
        self.assertFalse(xml.exists())

    def test_archive_damage_during_processing_preserves_the_extracted_copy(self):
        manifest, xml, raw = self.fixture()
        retain_snapshot(manifest, apply=True)
        spec = specification(manifest)
        with self.assertRaisesRegex(RuntimeError, "checksum"):
            with snapshot_xml(manifest):
                spec["archive"].write_bytes(b"damaged during processing")
        self.assertTrue(xml.exists())
        for member, data in raw.items():
            self.assertEqual((xml / member).read_bytes(), data)


if __name__ == "__main__":
    unittest.main()
