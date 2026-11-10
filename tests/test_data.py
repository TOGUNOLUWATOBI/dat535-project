from contextlib import closing
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch


@unittest.skipUnless(importlib.util.find_spec("pandas"), "pandas is needed for the GUI data helpers")
class GuiDataTests(unittest.TestCase):
    def test_clock_keeps_overnight_times_on_the_operating_day(self):
        from app.data import clock
        self.assertEqual(clock(6 * 3600 + 5 * 60), "06:05")
        self.assertEqual(clock(86400 + 15 * 60), "00:15 +1d")
        self.assertEqual(clock(None), "–")

    def test_evidence_files_load_with_sources(self):
        from app.data import evidence
        renumberings, discontinuations = evidence()
        self.assertTrue(renumberings.source_url.str.startswith("https://").all())
        self.assertIn("AKT:Line:1056", set(discontinuations.line_id))

    def test_unverified_candidates_are_not_presented_as_confirmed(self):
        from app import data
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "config").mkdir()
            entries = [{"old_line_id": str(i), "verified": flag} for i, flag in
                       enumerate([True, False, "true", None])]
            (root / "config/route_renumberings.json").write_text(json.dumps(entries))
            with patch.object(data, "REPO", root):
                renumberings, discontinuations = data.evidence()
            self.assertEqual(list(renumberings.old_line_id), ["0"])
            self.assertTrue(discontinuations.empty)

    def registry(self, root):
        """A minimal checkpoint registry, closed when the ``with`` block ends."""
        db = sqlite3.connect(root / "pipeline_state.sqlite3")
        db.executescript("""CREATE TABLE snapshots(snapshot_key TEXT, snapshot_date TEXT);
            CREATE TABLE stage_attempts(snapshot_key TEXT, stage TEXT, status TEXT,
                output_path TEXT, started_at TEXT, finished_at TEXT);""")
        return closing(db)

    def publish(self, root, db, day="2026-01-15", tag="first", status="complete", providers=None):
        output = root / "gold" / day / tag
        (output / "metadata").mkdir(parents=True)
        manifest = {"status": status, "snapshot_date": day, "providers": providers or ["AKT"],
                    "service_from": day, "service_to": day[:8] + "21",
                    "tables": {"daily_line_service": {"path": "daily_line_service"}}}
        (output / "metadata/manifest.json").write_text(json.dumps(manifest))
        db.execute("INSERT INTO snapshots VALUES (?,?)", (tag, day))
        db.execute("INSERT INTO stage_attempts VALUES (?,'gold',?,?,?,?)",
                   (tag, status, str(output), tag, tag))
        db.commit()
        return output

    def test_one_published_export_can_be_viewed_without_silver(self):
        from app.data import snapshots
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.registry(root) as db:
                output = self.publish(root, db)
            items = snapshots(root)
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["gold"], str(output))
            self.assertFalse((root / "silver").exists())

    def test_only_latest_complete_registered_output_is_selected(self):
        from app.data import snapshots
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.registry(root) as db:
                self.publish(root, db, tag="a")
                expected = self.publish(root, db, tag="b")
                self.publish(root, db, tag="c", status="failed")
            self.assertEqual(snapshots(root)[0]["gold"], str(expected))

    def test_incomparable_provider_selections_are_refused(self):
        from app.data import snapshots
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.registry(root) as db:
                self.publish(root, db, tag="a")
                self.publish(root, db, day="2026-02-15", tag="b", providers=["AKT", "KOL"])
            with self.assertRaisesRegex(ValueError, "different provider"):
                snapshots(root)

    def test_registry_status_does_not_hide_an_incomplete_manifest(self):
        from app.data import snapshots
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.registry(root) as db:
                output = self.publish(root, db)
            path = output / "metadata/manifest.json"
            manifest = json.loads(path.read_text())
            manifest["status"] = "in_progress"
            path.write_text(json.dumps(manifest))
            with self.assertRaises(ValueError):
                snapshots(root)

    def test_invalid_provider_and_window_metadata_are_refused(self):
        from app.data import snapshots
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.registry(root) as db:
                output = self.publish(root, db)
            path = output / "metadata/manifest.json"
            original = json.loads(path.read_text())
            for providers in [None, [], "AKT", ["AKT", "AKT"]]:
                with self.subTest(providers=providers):
                    path.write_text(json.dumps(dict(original, providers=providers)))
                    with self.assertRaisesRegex(ValueError, "invalid provider"):
                        snapshots(root)
            path.write_text(json.dumps(dict(original, service_to="2026-01-01")))
            with self.assertRaisesRegex(ValueError, "reversed service window"):
                snapshots(root)

    def test_incomparable_windows_are_refused(self):
        from app.data import snapshots
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.registry(root) as db:
                self.publish(root, db, tag="a")
                output = self.publish(root, db, day="2026-02-15", tag="b")
            path = output / "metadata/manifest.json"
            manifest = json.loads(path.read_text())
            manifest["service_to"] = "2026-02-22"
            path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "different provider selections or service-window"):
                snapshots(root)

    def test_missing_registry_is_not_created_by_the_gui(self):
        from app.data import snapshots
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.assertRaises(ValueError):
                snapshots(root)
            self.assertFalse((root / "pipeline_state.sqlite3").exists())


if __name__ == "__main__":
    unittest.main()
