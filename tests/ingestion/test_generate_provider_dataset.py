import contextlib
from datetime import date
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from pipeline.ingestion import generate_provider_dataset as builder


def document(body, publication="2026-01-15T00:00:00"):
    return (f'<PublicationDelivery xmlns="{builder.NETEX}" version="1.15:NO-NeTEx-networktimetable:1.5">'
            f'<PublicationTimestamp>{publication}</PublicationTimestamp>{body}</PublicationDelivery>')


def fixture(path, missing=False, akt_publication="2026-01-15T00:00:00"):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for provider in ("KOL", "AKT"):
            body = (f'<Line id="{provider}:Line:1" version="1"><TransportMode>bus</TransportMode>'
                    f'<LineRef ref="{provider}:Line:1" version="1"/>'
                    f'<OperatorRef ref="{provider}:Operator:1"/>'
                    '<StopPlaceRef ref="NSR:StopPlace:1"/></Line>')
            archive.writestr(f"{provider}_line.xml", document(body, akt_publication if provider == "AKT" else "2026-01-15T00:00:00"))
            shared = '' if missing and provider == "AKT" else f'<Operator id="{provider}:Operator:1" version="1"/>'
            archive.writestr(f"_{provider}_shared_data.xml", document(shared))
        archive.writestr("_KOL_flexible_shared_data.xml", document(""))
        archive.writestr("_stops.xml", document('<StopPlace id="NSR:StopPlace:1" version="3"/>'))
        archive.writestr("OTHER_line.xml", document(""))
        archive.writestr("_OTHER_shared_data.xml", document('<Operator id="AKT:Operator:1" version="1"/>'))


class CombinedDatasetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source.zip"

    def extract(self):
        directory = self.root / "xml"
        compact = self.root / "combined.zip"
        lineage = builder.extract_snapshot(self.source, directory, compact, ["KOL", "AKT"])
        return directory, compact, lineage

    def test_selection_retains_both_providers_and_stops_without_other_shared(self):
        fixture(self.source)
        directory, compact, lineage = self.extract()
        names = {i["member"] for i in lineage}
        self.assertEqual(names, {"KOL_line.xml", "AKT_line.xml", "_KOL_shared_data.xml",
                                 "_AKT_shared_data.xml", "_KOL_flexible_shared_data.xml", "_stops.xml"})
        with zipfile.ZipFile(self.source) as src, zipfile.ZipFile(compact) as dst:
            for name in names:
                self.assertEqual((directory / name).read_bytes(), src.read(name))
                self.assertEqual(dst.read(name), src.read(name))
        report = builder.validate_snapshot(directory, lineage, ["KOL", "AKT"], date(2026, 1, 15))
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["provider_reference_occurrences"], 6)
        self.assertEqual(set(report["providers"]), {"KOL", "AKT"})

    def test_missing_akt_dependency_is_detected(self):
        fixture(self.source, missing=True)
        directory, _, lineage = self.extract()
        report = builder.validate_snapshot(directory, lineage, ["KOL", "AKT"], date(2026, 1, 15))
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["unresolved_provider_references"][0]["ref"], "AKT:Operator:1")

    def test_future_publication_guard_applies_to_akt(self):
        fixture(self.source, akt_publication="2026-06-15T00:00:00")
        directory, _, lineage = self.extract()
        with self.assertRaisesRegex(RuntimeError, "Historical mismatch in AKT_line.xml"):
            builder.validate_snapshot(directory, lineage, ["KOL", "AKT"], date(2026, 1, 15))

    def run_generation(self, missing=False):
        fixture(self.source, missing=missing)
        output = self.root / "dataset"
        metadata = {"generation": "123", "md5Hash": "unique", "size": str(self.source.stat().st_size),
                    "timeCreated": "2026-01-15T07:30:00Z"}

        def fake_download(url, path, identity):
            path.write_bytes(self.source.read_bytes())
            return "source-sha"

        with patch.object(builder, "source_metadata", return_value=metadata), \
                patch.object(builder, "download", side_effect=fake_download), \
                contextlib.redirect_stdout(io.StringIO()):
            if missing:
                with self.assertRaisesRegex(RuntimeError, "Reference validation failed"):
                    builder.generate(["KOL", "AKT"], [date(2026, 1, 15)], output, remove_national=True)
            else:
                builder.generate(["KOL", "AKT"], [date(2026, 1, 15)], output, remove_national=True)
        return output, json.loads((output / "manifest.json").read_text())

    def test_cleanup_only_after_success_and_outputs_are_preserved(self):
        output, manifest = self.run_generation()
        self.assertEqual(manifest["status"], "complete")
        row = manifest["snapshots"][0]
        self.assertTrue(row["national_removed"])
        self.assertFalse(Path(row["national_zip"]).exists())
        self.assertTrue(Path(row["combined_zip"]).exists())
        self.assertTrue((Path(row["xml_directory"]) / "AKT_line.xml").exists())
        with self.assertRaises(FileExistsError):
            builder.generate(["KOL", "AKT"], [date(2026, 1, 15)], output)

    def test_validation_failure_preserves_national_source(self):
        _, manifest = self.run_generation(missing=True)
        self.assertEqual(manifest["status"], "failed")
        row = manifest["snapshots"][0]
        self.assertFalse(row["national_removed"])
        self.assertTrue(Path(row["national_zip"]).exists())

    def test_duplicate_source_elements_are_preserved_and_flagged(self):
        fixture(self.source)
        directory, _, lineage = self.extract()
        path = directory / "AKT_line.xml"
        original = path.read_text()
        repeated = original.replace('</Line>', '<StopPoint id="AKT:StopPoint:repeat" version="1"/>'
                                             '<StopPoint id="AKT:StopPoint:repeat" version="1"/></Line>')
        path.write_text(repeated)
        report = builder.validate_snapshot(directory, lineage, ["KOL", "AKT"], date(2026, 1, 15))
        self.assertEqual(report["status"], "passed_with_warnings")
        self.assertEqual(report["duplicate_provider_id_version_pairs"], {"AKT": 1})
        self.assertEqual(path.read_text(), repeated)

    def test_resume_verifies_completed_outputs_without_redownloading(self):
        output, _ = self.run_generation()
        metadata = json.loads((output / "source_metadata.json").read_text())["20260115"]
        with patch.object(builder, "source_metadata", return_value=metadata) as metadata_lookup, \
                patch.object(builder, "download") as download, contextlib.redirect_stdout(io.StringIO()):
            manifest = builder.generate(["KOL", "AKT"], [date(2026, 1, 15)], output,
                                        remove_national=True, resume=True)
        download.assert_not_called()
        metadata_lookup.assert_not_called()
        self.assertEqual(manifest["status"], "complete")
        self.assertEqual(len(manifest["snapshots"]), 1)

    def test_resume_rejects_changed_output(self):
        output, manifest = self.run_generation()
        path = Path(manifest["snapshots"][0]["xml_directory"]) / "AKT_line.xml"
        path.write_text("changed")
        metadata = json.loads((output / "source_metadata.json").read_text())["20260115"]
        with patch.object(builder, "source_metadata", return_value=metadata), \
                contextlib.redirect_stdout(io.StringIO()), \
                self.assertRaisesRegex(RuntimeError, "does not match recorded source lineage"):
            builder.generate(["KOL", "AKT"], [date(2026, 1, 15)], output, resume=True)

    def test_resume_requires_an_existing_dataset(self):
        output = self.root / "missing-dataset"
        with patch.object(builder, "source_metadata") as metadata, self.assertRaises(FileNotFoundError):
            builder.generate(["KOL", "AKT"], [date(2026, 1, 15)], output, resume=True)
        metadata.assert_not_called()
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
