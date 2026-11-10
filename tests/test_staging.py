"""Staging source selection and the real Spark XML-to-Parquet projection."""

from datetime import date
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from pipeline.common import sha256_file
from pipeline.ingestion.generate_provider_dataset import validate_snapshot
from pipeline.processing.bronze.staging_sources import load_snapshot_sources, load_sources
from tests.fixtures import complete_fixture, spark_session


def staged_fixture(root):
    root.mkdir()
    xml = root / "xml/2026-01-15"
    xml.mkdir(parents=True)
    metadata = root / "metadata"
    metadata.mkdir()
    lineage = []
    for provider in ("KOL", "AKT"):
        payload = f'''<PublicationDelivery xmlns="http://www.netex.org.uk/netex" version="1.15:NO-NeTEx-networktimetable:1.5">
<PublicationTimestamp>2026-01-15T00:00:00</PublicationTimestamp>
<dataObjects><CompositeFrame id="{provider}:CompositeFrame:1" version="01"><frames>
<ServiceFrame id="{provider}:ServiceFrame:1" version="01"><lines>
<Line id="{provider}:Line:1" version="0001"><Name>Route æ</Name><PublicCode>007</PublicCode>
<TransportMode>bus</TransportMode><OperatorRef ref="{provider}:Operator:1"/>
<TransportSubmode><BusSubmode>localBus</BusSubmode></TransportSubmode></Line></lines>
<journeyPatterns><JourneyPattern id="{provider}:JourneyPattern:1" version="1"/></journeyPatterns></ServiceFrame>
<TimetableFrame id="{provider}:TimetableFrame:1" version="01"><vehicleJourneys>
<ServiceJourney id="{provider}:ServiceJourney:1" version="0001"><Name>Night journey</Name>
<LineRef ref="{provider}:Line:1" version="0001"/><JourneyPatternRef ref="{provider}:JourneyPattern:1" version="1"/>
<dayTypes><DayTypeRef ref="{provider}:DayType:1"/></dayTypes><passingTimes>
<TimetabledPassingTime id="{provider}:Time:repeat" version="1"><StopPointInJourneyPatternRef ref="{provider}:Point:1"/>
<DepartureTime>23:55:00</DepartureTime></TimetabledPassingTime>
<TimetabledPassingTime id="{provider}:Time:repeat" version="1"><StopPointInJourneyPatternRef ref="{provider}:Point:2"/>
<ArrivalTime>00:10:00</ArrivalTime><ArrivalDayOffset>1</ArrivalDayOffset></TimetabledPassingTime>
</passingTimes></ServiceJourney></vehicleJourneys></TimetableFrame></frames></CompositeFrame></dataObjects></PublicationDelivery>'''
        path = xml / f"{provider}_line.xml"
        path.write_text(payload)
        lineage.append({"member": path.name, "xml_path": str(path.relative_to(root)), "provider": provider,
                        "bytes": path.stat().st_size, "sha256": sha256_file(path), "source_crc32": 0})
    # The fixture's referenced entities live in shared XML, as in the real source.
    shared = xml / "_stops.xml"
    body = "".join(f'<Operator id="{p}:Operator:1"/><DayType id="{p}:DayType:1"/>'
                   f'<StopPoint id="{p}:Point:1"/><StopPoint id="{p}:Point:2"/>' for p in ("KOL", "AKT"))
    shared.write_text('<PublicationDelivery xmlns="http://www.netex.org.uk/netex" version="1.15">'
                      '<PublicationTimestamp>2026-01-15T00:00:00</PublicationTimestamp>' + body + '</PublicationDelivery>')
    lineage.append({"member": shared.name, "xml_path": str(shared.relative_to(root)), "provider": None,
                    "bytes": shared.stat().st_size, "sha256": sha256_file(shared), "source_crc32": 0})
    report = validate_snapshot(xml, lineage, ["KOL", "AKT"], date(2026, 1, 15))
    if report["status"] != "passed_with_warnings":
        raise AssertionError(report)
    (metadata / "lineage.json").write_text(json.dumps(lineage))
    (metadata / "validation.json").write_text(json.dumps(report))
    manifest = metadata / "manifest.json"
    manifest.write_text(json.dumps({"status": "complete_with_warnings", "snapshot_date": "2026-01-15",
                                    "providers": ["KOL", "AKT"], "xml_directory": "xml/2026-01-15"}))
    return manifest


class SourceSelectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest = staged_fixture(self.root / "bronze")

    def test_selects_validated_provider_files_and_expected_counts(self):
        result = load_sources(self.manifest)
        self.assertEqual(result["scope"], "provider_line_files")
        self.assertEqual([f["source_member"] for f in result["files"]], ["KOL_line.xml", "AKT_line.xml"])
        self.assertEqual(result["expected_rows"], {"frames": 2, "lines": 2, "service_journeys": 2, "passing_times": 4})

    def test_changed_source_and_wrong_date_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "does not match"):
            load_sources(self.manifest, date(2026, 2, 15))
        (self.root / "bronze/xml/2026-01-15/KOL_line.xml").write_text("changed")
        with self.assertRaisesRegex(RuntimeError, "changed since Bronze validation"):
            load_sources(self.manifest)

    def test_failed_or_duplicate_ingestions_are_not_processed(self):
        value = json.loads(self.manifest.read_text())
        for status in ("failed", "duplicate", "running"):
            value["status"] = status
            self.manifest.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError, "Processing requires"):
                load_sources(self.manifest)

    def test_historical_manifest_supports_original_and_metadata_subdirectory_layouts(self):
        for pretty in (False, True):
            with self.subTest(pretty=pretty):
                root = self.root / f"historical-{pretty}"
                original_manifest = staged_fixture(root)
                metadata = root / "metadata" if pretty else root
                for name in ("lineage", "validation"):
                    directory = metadata / name
                    directory.mkdir()
                    (directory / "2026-01-15.json").write_text((original_manifest.parent / f"{name}.json").read_text())
                target = metadata / "historical-manifest.json"
                report_path = (metadata / "validation/2026-01-15.json").relative_to(root)
                target.write_text(json.dumps({"status": "complete_with_warnings", "providers": ["KOL", "AKT"],
                    "snapshots": [{"snapshot_date": "2026-01-15", "status": "validated_with_warnings",
                                   "xml_directory": "xml/2026-01-15", "validation_report": str(report_path)}]}))
                with self.assertRaisesRegex(ValueError, "Specify --date"):
                    load_sources(target)
                result = load_sources(target, date(2026, 1, 15))
                self.assertEqual(result["expected_rows"]["passing_times"], 4)
                partial = json.loads(target.read_text())
                partial["status"] = "failed"
                partial["snapshots"].append({"snapshot_date":"2026-02-01","status":"failed"})
                target.write_text(json.dumps(partial))
                self.assertEqual(load_sources(target,date(2026,1,15))["expected_rows"]["passing_times"],4)
                with self.assertRaisesRegex(ValueError,"missing or not validated"):
                    load_sources(target,date(2026,2,1))


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "Install requirements.txt to run real Spark integration tests")
class SparkStagingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        # Spaces, '+', Unicode and punctuation: Hadoop and pathlib encode these differently.
        self.bronze = self.root / "bronze with spaces + æ, (test)"
        self.manifest = complete_fixture(self.bronze)

    def test_real_spark_staging_preserves_context_references_order_and_raw_values(self):
        from pipeline.processing.bronze.snapshot_to_parquet import convert_snapshot
        output = self.root / "staging"
        result = convert_snapshot(self.spark, self.manifest, output)
        self.assertEqual(result["status"], "complete")
        self.assertEqual({name: result["tables"][name]["rows"] for name in
                          ("lines", "service_journeys", "passing_times", "stop_places", "quays")},
                         {"lines": 4, "service_journeys": 4, "passing_times": 8, "stop_places": 1, "quays": 2})
        lines = self.spark.read.parquet(str(output / "lines")).collect()
        self.assertEqual(sorted(row.record.PublicCode for row in lines), ["007", "007", "008", "HentMeg"])
        self.assertTrue(all(row.record._version == "0001" for row in lines))
        self.assertTrue(all(row.parent_frame_id.startswith(row.provider + ":ServiceFrame:") for row in lines))
        self.assertTrue(all(row.record._unmapped_xml and "BusSubmode" in row.record._unmapped_xml[0] for row in lines))
        journeys = self.spark.read.parquet(str(output / "service_journeys")).collect()
        self.assertTrue(all(row.record.JourneyPatternRef._ref == row.provider + ":JourneyPattern:1" for row in journeys))
        times = (self.spark.read.parquet(str(output / "passing_times"))
                 .where("record.DepartureTime IS NOT NULL OR record.ArrivalTime IS NOT NULL")
                 .orderBy("source_member", "passing_time_index").collect())
        for first, second in zip(times[::2], times[1::2], strict=True):
            self.assertEqual(first.record._id, second.record._id)
            self.assertEqual(first.record.DepartureTime, "23:55:00")
            self.assertEqual((second.record.ArrivalTime, second.record.ArrivalDayOffset), ("00:10:00", "1"))
            self.assertEqual((first.passing_time_index, second.passing_time_index), (0, 1))
            self.assertEqual(first.service_journey_id, second.service_journey_id)
        with self.assertRaises(FileExistsError):
            convert_snapshot(self.spark, self.manifest, output)

    def test_count_mismatch_fails_and_keeps_failure_manifest(self):
        from pipeline.processing.bronze.snapshot_to_parquet import convert_snapshot
        report_path = self.manifest.parent / "validation.json"
        report = json.loads(report_path.read_text())
        member = next(m for m in report["members"] if m["member"] == "KOL_007.xml")
        member["entity_counts"]["Line"] += 1
        report_path.write_text(json.dumps(report))
        self.assertEqual(load_snapshot_sources(self.manifest)["expected_rows"]["lines"], 5)
        output = self.root / "failed-staging"
        with self.assertRaisesRegex(RuntimeError, "lines: 4 rows; Bronze records 5"):
            convert_snapshot(self.spark, self.manifest, output)
        self.assertEqual(json.loads((output / "metadata/manifest.json").read_text())["status"], "failed")
        self.assertTrue((self.bronze / "xml/2026-01-15/KOL_007.xml").exists())


if __name__ == "__main__":
    unittest.main()
