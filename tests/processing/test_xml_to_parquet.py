from datetime import date
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from pipeline.ingestion.download_entur_netex import file_checksums
from pipeline.ingestion.generate_provider_dataset import validate_snapshot
from pipeline.processing.bronze.staging_sources import load_sources


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
                        "bytes": path.stat().st_size, "sha256": file_checksums(path)[0], "source_crc32": 0})
    # The fixture's referenced entities live in shared XML, as in the real source.
    shared = xml / "_stops.xml"
    body = "".join(f'<Operator id="{p}:Operator:1"/><DayType id="{p}:DayType:1"/>'
                   f'<StopPoint id="{p}:Point:1"/><StopPoint id="{p}:Point:2"/>' for p in ("KOL", "AKT"))
    shared.write_text('<PublicationDelivery xmlns="http://www.netex.org.uk/netex" version="1.15">'
                      '<PublicationTimestamp>2026-01-15T00:00:00</PublicationTimestamp>' + body + '</PublicationDelivery>')
    lineage.append({"member": shared.name, "xml_path": str(shared.relative_to(root)), "provider": None,
                    "bytes": shared.stat().st_size, "sha256": file_checksums(shared)[0], "source_crc32": 0})
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
        result = load_sources(self.manifest, sample_per_provider=1)
        self.assertEqual(result["scope"], "sample")
        self.assertEqual(result["expected_rows"], {"frames": 2, "lines": 2, "service_journeys": 2, "passing_times": 4})
        self.assertEqual(result["excluded_shared_files"], 1)

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


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "Install requirements-processing.txt to run real Spark integration tests")
class SparkConversionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[2]").appName("DAT535-processing-tests")
                     .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.session.timeZone", "Europe/Oslo").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.bronze = self.root / "bronze with spaces + æ, (test)"
        self.manifest = staged_fixture(self.bronze)

    def test_real_spark_conversion_preserves_context_references_order_and_raw_values(self):
        from pipeline.processing.bronze.xml_to_parquet import convert
        output = self.root / "staging"
        result = convert(self.spark, self.manifest, output)
        self.assertEqual(result["status"], "complete")
        self.assertEqual({name: value["rows"] for name, value in result["tables"].items()},
                         {"frames": 2, "lines": 2, "service_journeys": 2, "passing_times": 4})
        lines = self.spark.read.parquet(str(output / "lines")).collect()
        self.assertTrue(all(row.record.PublicCode == "007" and row.record._version == "0001" for row in lines))
        self.assertTrue(all(row.parent_frame_id.startswith(row.provider + ":ServiceFrame:") for row in lines))
        self.assertTrue(all(row.record._unmapped_xml and "BusSubmode" in row.record._unmapped_xml[0] for row in lines))
        frames = self.spark.read.parquet(str(output / "frames")).collect()
        self.assertTrue(all(any(row.provider + ":JourneyPattern:1" in fragment
                                for fragment in row.frames.ServiceFrame[0]._unmapped_xml)
                            for row in frames))
        journeys = self.spark.read.parquet(str(output / "service_journeys")).collect()
        self.assertTrue(all(row.record.LineRef._ref == row.provider + ":Line:1" for row in journeys))
        times = self.spark.read.parquet(str(output / "passing_times")).orderBy("provider", "passing_time_index").collect()
        for first, second in (times[:2], times[2:]):
            self.assertEqual(first.record._id, second.record._id)
            self.assertEqual(first.record.DepartureTime, "23:55:00")
            self.assertEqual(second.record.ArrivalTime, "00:10:00")
            self.assertEqual(second.record.ArrivalDayOffset, "1")
            self.assertEqual((first.passing_time_index, second.passing_time_index), (0, 1))
            self.assertEqual(first.service_journey_id, second.service_journey_id)
        with self.assertRaises(FileExistsError):
            convert(self.spark, self.manifest, output)

    def test_count_mismatch_fails_and_keeps_failure_manifest(self):
        from pipeline.processing.bronze.xml_to_parquet import convert
        report_path = self.manifest.parent / "validation.json"
        report = json.loads(report_path.read_text())
        report["members"][0]["entity_counts"]["Line"] += 1
        report_path.write_text(json.dumps(report))
        output = self.root / "failed-staging"
        with self.assertRaisesRegex(RuntimeError, "Bronze validation records"):
            convert(self.spark, self.manifest, output)
        self.assertEqual(json.loads((output / "metadata/manifest.json").read_text())["status"], "failed")
        self.assertTrue((self.bronze / "xml/2026-01-15/KOL_line.xml").exists())


if __name__ == "__main__":
    unittest.main()
