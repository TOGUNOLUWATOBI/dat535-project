"""Daily history retention, duplicate authority checks and non-destructive publication."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("pipeline_history", ROOT / ".github/scripts/pipeline_history.py")
history = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(history)


def write(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def silver(root, name, days):
    run = root / name
    report = run / "quality/quality_report.json"
    write(report, {"gate_status": "passed", "daily_summaries": [{"operating_date": day} for day in days]})
    write(run / "silver_manifest.json", {"status": "complete", "quality_report_path": str(report)})
    return run


class PipelineHistoryTests(unittest.TestCase):
    def test_combines_history_and_new_dates_without_duplicate_reruns(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            old = silver(root / "old", "batch", ["2026-01-01", "2026-01-08"])
            silver(root / "new", "rerun", ["2026-01-01"])
            new = silver(root / "new", "daily-2026-11-26", ["2026-11-26"])
            with contextlib.redirect_stdout(io.StringIO()):
                history.prepare(root / "old", root / "new", root / "view", root / "dates.json")
            self.assertEqual(json.loads((root / "dates.json").read_text()),
                             ["2026-01-01", "2026-01-08", "2026-11-26"])
            self.assertEqual({p.resolve() for p in (root / "view").iterdir()}, {old, new})
            self.assertTrue((old / "silver_manifest.json").is_file())

    def test_duplicate_historical_authority_fails_before_creating_view(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            silver(root / "old", "one", ["2026-01-01"])
            silver(root / "old", "two", ["2026-01-01"])
            (root / "new").mkdir()
            with self.assertRaisesRegex(ValueError, "Duplicate historical"):
                history.prepare(root / "old", root / "new", root / "view", root / "dates.json")
            self.assertFalse((root / "view").exists())

    def candidate(self, root):
        write(root / "gold/kol/runs/prod-gold/gold_manifest.json", {
            "status": "complete", "gate_status": "passed", "published_operating_dates": ["2026-11-26"]})
        write(root / "metadata/run.json", {"status": "complete"})
        silver(root / "silver/kol/runs", "prod-silver-2026-11-26", ["2026-11-26"])
        path = root / "bronze/netex/kol/2026-11/kol_2026-11-26.zip"
        path.parent.mkdir(parents=True)
        path.write_bytes(b"verified new archive")
        write(root / "bronze/netex/kol/manifest.json", {"records": [{
            "target_date": "2026-11-26", "status": "downloaded", "file": str(path),
            "sha256": history.sha256(path)}]})

    def test_publication_appends_bronze_and_silver_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            root, dest = Path(tmp).resolve() / "candidate", Path(tmp).resolve() / "published"
            self.candidate(root)
            old_silver = silver(dest / "silver/kol/runs", "batch", ["2026-01-01"])
            old_file = dest / "bronze/netex/kol/2026-01/kol_2026-01-01.zip"
            old_file.parent.mkdir(parents=True)
            old_file.write_bytes(b"historical archive")
            write(dest / "bronze/netex/kol/manifest.json", {"records": [{
                "target_date": "2026-01-01", "status": "downloaded", "file": str(old_file),
                "sha256": history.sha256(old_file)}]})
            history.publish(root, dest)
            history.publish(root, dest)
            self.assertEqual(old_file.read_bytes(), b"historical archive")
            published = json.loads((dest / "bronze/netex/kol/manifest.json").read_text())
            self.assertIn("updated_at", published)
            records = published["records"]
            self.assertEqual({r["target_date"] for r in records}, {"2026-01-01", "2026-11-26"})
            self.assertTrue(all(Path(r["file"]).is_relative_to(dest) for r in records))
            self.assertTrue((old_silver / "silver_manifest.json").is_file())
            self.assertEqual((dest / "silver/kol/runs/prod-silver-2026-11-26").resolve(),
                             root / "silver/kol/runs/prod-silver-2026-11-26")

    def test_failed_candidate_cannot_publish(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, dest = Path(tmp).resolve() / "candidate", Path(tmp).resolve() / "published"
            self.candidate(root)
            write(root / "metadata/run.json", {"status": "failed"})
            with self.assertRaisesRegex(ValueError, "Only a complete"):
                history.publish(root, dest)
            self.assertFalse(dest.exists())

    def test_gold_only_rebuild_can_publish_without_new_silver_or_bronze(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            root, dest = Path(tmp).resolve() / "candidate", Path(tmp).resolve() / "published"
            write(root / "gold/kol/runs/prod-gold/gold_manifest.json", {
                "status": "complete", "gate_status": "passed", "published_operating_dates": ["2026-01-01"]})
            write(root / "metadata/run.json", {"status": "complete"})
            history.publish(root, dest)
            self.assertTrue((dest / "gold/kol/runs/prod-candidate").is_dir())


if __name__ == "__main__":
    unittest.main()
