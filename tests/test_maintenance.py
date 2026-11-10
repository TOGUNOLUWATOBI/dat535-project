import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
import zipfile

from pipeline.storage.maintenance import deduplicate, discard_provider_zips


class BronzeStorageTests(unittest.TestCase):
    def fixture(self, root, name, status="complete"):
        base = root / name
        (base / 'xml/2026-11-09').mkdir(parents=True)
        (base / 'metadata').mkdir()
        raw = base / 'xml/2026-11-09/KOL_line.xml'
        raw.write_bytes(b'<PublicationDelivery>preserved raw bytes</PublicationDelivery>' * 100)
        lineage = [
            {'member': raw.name, 'bytes': raw.stat().st_size, 'sha256': hashlib.sha256(raw.read_bytes()).hexdigest()}
        ]
        (base / 'metadata/lineage.json').write_text(json.dumps(lineage))
        (base / 'metadata/manifest.json').write_text(
            json.dumps({'status': status, 'snapshot_date': '2026-11-09', 'xml_directory': 'xml/2026-11-09'})
        )
        return raw

    def test_dry_run_does_not_change_files_then_apply_preserves_paths_bytes_and_metadata(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            a = self.fixture(root, 'a')
            b = self.fixture(root, 'b')
            before = {p: p.read_bytes() for p in root.rglob('*.json')}
            raw = a.read_bytes()
            report = deduplicate(root)
            self.assertEqual(report['duplicate_copies'], 1)
            self.assertFalse(a.samefile(b))
            result = deduplicate(root, apply=True)
            self.assertEqual(result['linked_files'], 1)
            self.assertTrue(a.samefile(b))
            self.assertEqual((a.read_bytes(), b.read_bytes()), (raw, raw))
            self.assertEqual({p: p.read_bytes() for p in root.rglob('*.json')}, before)
            self.assertEqual(deduplicate(root, apply=True)['duplicate_copies'], 0)

    def test_corrupt_copy_is_not_overwritten_or_used_as_canonical(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            a = self.fixture(root, 'a')
            b = self.fixture(root, 'b')
            a.write_bytes(b'x' * a.stat().st_size)
            original = (a.read_bytes(), b.read_bytes())
            report = deduplicate(root, apply=True)
            self.assertEqual(report['linked_files'], 0)
            self.assertTrue(report['problems'])
            self.assertEqual((a.read_bytes(), b.read_bytes()), original)

    def test_new_duplicate_joins_existing_hardlinks_even_when_its_path_sorts_first(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            existing = self.fixture(root, 'b')
            linked = self.fixture(root, 'c')
            linked.unlink()
            os.link(existing, linked)
            new = self.fixture(root, 'a')
            inode = existing.stat().st_ino
            result = deduplicate(root, apply=True)
            self.assertEqual(result['linked_files'], 1)
            self.assertTrue(new.samefile(existing))
            self.assertTrue(new.samefile(linked))
            self.assertEqual(existing.stat().st_ino, inode)
            self.assertEqual(deduplicate(root, apply=True)['linked_files'], 0)

    def test_failed_and_running_snapshots_and_original_uploads_are_untouched(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.fixture(root, 'a')
            failed = self.fixture(root, 'failed', 'failed')
            running = self.fixture(root, 'running', 'running')
            original = root / 'a/uploads/original.zip'
            original.parent.mkdir()
            original.write_bytes(failed.read_bytes())
            inodes = {p: p.stat().st_ino for p in (failed, running, original)}
            self.assertEqual(deduplicate(root, apply=True)['linked_files'], 0)
            self.assertEqual({p: p.stat().st_ino for p in inodes}, inodes)

    def test_different_permissions_and_symlink_targets_are_not_consolidated(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            a = self.fixture(root, 'a')
            b = self.fixture(root, 'b')
            c = self.fixture(root, 'c')
            os.chmod(a, 0o600)
            os.chmod(b, 0o644)
            c.unlink()
            c.symlink_to(a)
            self.assertEqual(deduplicate(root, apply=True)['linked_files'], 0)
            self.assertFalse(a.samefile(b))
            self.assertTrue(c.is_symlink())

    def test_provider_zip_removal_requires_exact_xml_and_records_the_retention_change(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = self.fixture(root, 'a')
            base = root / 'a'
            archive = base / 'selected.zip'
            with zipfile.ZipFile(archive, 'w') as zipped:
                zipped.writestr(raw.name, raw.read_bytes())
            lineage_path = base / 'metadata/lineage.json'
            lineage = json.loads(lineage_path.read_text())
            with zipfile.ZipFile(archive) as zipped:
                lineage[0]['source_crc32'] = zipped.infolist()[0].CRC
            lineage_path.write_text(json.dumps(lineage))
            manifest_path = base / 'metadata/manifest.json'
            manifest = json.loads(manifest_path.read_text())
            manifest.update(
                combined_zip='selected.zip', combined_zip_sha256=hashlib.sha256(archive.read_bytes()).hexdigest()
            )
            manifest_path.write_text(json.dumps(manifest))
            self.assertEqual(discard_provider_zips(root)['zip_copies'], 1)
            self.assertTrue(archive.exists())
            original = raw.read_bytes()
            raw.write_bytes(b'x' * len(original))
            self.assertEqual(discard_provider_zips(root, apply=True)['zip_copies'], 0)
            self.assertTrue(archive.exists())
            raw.write_bytes(original)
            self.assertEqual(discard_provider_zips(root, apply=True)['zip_copies'], 1)
            self.assertFalse(archive.exists())
            self.assertEqual(raw.read_bytes(), original)
            saved = json.loads(manifest_path.read_text())
            self.assertTrue(saved['xml_only'])
            self.assertIsNone(saved['combined_zip'])


if __name__ == '__main__':
    unittest.main()
