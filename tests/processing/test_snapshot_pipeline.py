from datetime import date
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

from pipeline.ingestion.download_entur_netex import file_checksums
from pipeline.ingestion.generate_provider_dataset import validate_snapshot


def complete_fixture(root,bad_time=False):
    xml=root/'xml/2026-01-15';xml.mkdir(parents=True);metadata=root/'metadata';metadata.mkdir()
    def publication(body):return '<PublicationDelivery xmlns="http://www.netex.org.uk/netex" version="1.15:NO-NeTEx-networktimetable:1.5"><PublicationTimestamp>2026-01-15T00:00:00</PublicationTimestamp><dataObjects>'+body+'</dataObjects></PublicationDelivery>'
    for provider,number,flexible in [('KOL','007',False),('AKT','007',False),('AKT','008',False),('KOL','HentMeg',True)]:
        line_kind='FlexibleLine' if flexible else 'Line';line_ref='FlexibleLineRef' if flexible else 'LineRef'
        timing1='<EarliestDepartureTime>09:00:00</EarliestDepartureTime><LatestArrivalTime>13:00:00</LatestArrivalTime>' if flexible else '<DepartureTime>23:55:00</DepartureTime>'
        timing2=timing1 if flexible else '<ArrivalTime>'+('25:10:00' if bad_time and provider=='AKT' else '00:10:00')+'</ArrivalTime><ArrivalDayOffset>1</ArrivalDayOffset>'
        body=f'''<CompositeFrame id="{provider}:CompositeFrame:1" version="1"><validityConditions><AvailabilityCondition id="{provider}:Availability:1"><FromDate>2026-01-15T00:00:00</FromDate><ToDate>2026-01-21T00:00:00</ToDate></AvailabilityCondition></validityConditions><frames>
<ServiceFrame id="{provider}:ServiceFrame:1" version="1"><lines><{line_kind} id="{provider}:{line_kind}:1" version="0001"><Name>Test æ</Name><PublicCode>{number}</PublicCode><TransportMode>bus</TransportMode></{line_kind}></lines>
<routes><Route id="{provider}:Route:1" version="1"><{line_ref} ref="{provider}:{line_kind}:1" version="0001"/></Route></routes>
<journeyPatterns><JourneyPattern id="{provider}:JourneyPattern:1" version="1"><RouteRef ref="{provider}:Route:1" version="1"/><pointsInSequence>
<StopPointInJourneyPattern id="{provider}:Point:1" version="1" order="1"><ScheduledStopPointRef ref="{provider}:Stop:1"/></StopPointInJourneyPattern>
<StopPointInJourneyPattern id="{provider}:Point:2" version="1" order="2"><ScheduledStopPointRef ref="{provider}:Stop:2"/></StopPointInJourneyPattern>
</pointsInSequence></JourneyPattern></journeyPatterns></ServiceFrame>
<TimetableFrame id="{provider}:TimetableFrame:1" version="1"><vehicleJourneys><ServiceJourney id="{provider}:Journey:repeat" version="1"><JourneyPatternRef ref="{provider}:JourneyPattern:1" version="1"/><dayTypes><DayTypeRef ref="{provider}:DayType:1"/></dayTypes><passingTimes>
<TimetabledPassingTime id="{provider}:Time:repeat" version="1"><StopPointInJourneyPatternRef ref="{provider}:Point:1"/>{timing1}</TimetabledPassingTime>
<TimetabledPassingTime id="{provider}:Time:repeat" version="1"><StopPointInJourneyPatternRef ref="{provider}:Point:2"/>{timing2}</TimetabledPassingTime>
</passingTimes></ServiceJourney></vehicleJourneys></TimetableFrame></frames></CompositeFrame>'''
        (xml/f'{provider}_{number}.xml').write_text(publication(body))
    for p in ('KOL','AKT'):
        body=f'''<CompositeFrame id="{p}:CompositeFrame:shared" version="1"><frames><ServiceFrame id="{p}:ServiceFrame:shared" version="1"><scheduledStopPoints><ScheduledStopPoint id="{p}:Stop:1" version="1"><Name>First</Name></ScheduledStopPoint><ScheduledStopPoint id="{p}:Stop:2" version="1"/></scheduledStopPoints><stopAssignments><PassengerStopAssignment id="{p}:Assignment:1" version="1"><ScheduledStopPointRef ref="{p}:Stop:1" version="1"/><QuayRef ref="NSR:Quay:1"/></PassengerStopAssignment><PassengerStopAssignment id="{p}:Assignment:2" version="1"><ScheduledStopPointRef ref="{p}:Stop:2" version="1"/><QuayRef ref="NSR:Quay:2"/></PassengerStopAssignment></stopAssignments></ServiceFrame>
<ServiceCalendarFrame id="{p}:ServiceCalendarFrame:1" version="1"><dayTypes><DayType id="{p}:DayType:1" version="1"><properties><PropertyOfDay><DaysOfWeek>Weekdays</DaysOfWeek></PropertyOfDay></properties></DayType></dayTypes><operatingPeriods><OperatingPeriod id="{p}:Period:1" version="1"><FromDate>2026-01-15T00:00:00</FromDate><ToDate>2026-01-21T00:00:00</ToDate></OperatingPeriod></operatingPeriods><dayTypeAssignments>
<DayTypeAssignment id="{p}:DayTypeAssignment:1" version="1"><OperatingPeriodRef ref="{p}:Period:1"/><DayTypeRef ref="{p}:DayType:1"/></DayTypeAssignment>
<DayTypeAssignment id="{p}:DayTypeAssignment:2" version="1"><Date>2026-01-16</Date><DayTypeRef ref="{p}:DayType:1"/><isAvailable>false</isAvailable></DayTypeAssignment>
<DayTypeAssignment id="{p}:DayTypeAssignment:3" version="1"><Date>2026-01-17</Date><DayTypeRef ref="{p}:DayType:1"/></DayTypeAssignment>
</dayTypeAssignments></ServiceCalendarFrame></frames></CompositeFrame>'''
        (xml/f'_{p}_shared_data.xml').write_text(publication(body))
    (xml/'_stops.xml').write_text(publication('<SiteFrame id="NSR:SiteFrame:1" version="1"><stopPlaces><StopPlace id="NSR:StopPlace:1" version="1"><Name>Test station</Name><quays><Quay id="NSR:Quay:1" version="1"><Name>First</Name></Quay><Quay id="NSR:Quay:2" version="1"/></quays></StopPlace></stopPlaces></SiteFrame>'))
    lineage=[]
    for p in sorted(xml.glob('*.xml')):
        provider=next((v for v in ('KOL','AKT') if p.name.startswith(v+'_') or p.name.startswith('_'+v+'_')),None)
        lineage.append({'member':p.name,'xml_path':str(p.relative_to(root)),'provider':provider,'bytes':p.stat().st_size,'sha256':file_checksums(p)[0],'source_crc32':0})
    report=validate_snapshot(xml,lineage,['KOL','AKT'],date(2026,1,15))
    if report['status'] not in ('passed','passed_with_warnings'):raise AssertionError(report)
    (metadata/'validation.json').write_text(json.dumps(report));(metadata/'lineage.json').write_text(json.dumps(lineage))
    path=metadata/'manifest.json';path.write_text(json.dumps({'status':'complete_with_warnings','snapshot_date':'2026-01-15','providers':['KOL','AKT'],'xml_directory':'xml/2026-01-15'}))
    return path


@unittest.skipUnless(importlib.util.find_spec('pyspark'),'Install processing dependencies for Spark integration')
class SnapshotPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault('PYSPARK_PYTHON',sys.executable)
        java=Path('/Users/bebs/Library/Java/JavaVirtualMachines/ms-17.0.15/Contents/Home')
        if java.exists():os.environ.setdefault('JAVA_HOME',str(java))
        from pyspark.sql import SparkSession
        cls.spark=SparkSession.builder.master('local[2]').appName('DAT535-full-pipeline-tests').config('spark.ui.enabled','false').config('spark.ui.showConsoleProgress','false').config('spark.sql.shuffle.partitions','2').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')
    @classmethod
    def tearDownClass(cls):cls.spark.stop()
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
    def test_real_pipeline_preserves_occurrences_handles_flexible_and_resumes(self):
        from pipeline.run_snapshot import run_snapshot
        manifest=complete_fixture(self.root/'bronze')
        result=run_snapshot(self.spark,manifest,None,self.root/'data')
        silver=Path(result['silver']['path']);gold=Path(result['gold']['path'])
        journeys=self.spark.read.parquet(str(silver/'journeys'))
        self.assertEqual(journeys.count(),4)
        self.assertEqual(journeys.where('is_flexible').count(),1)
        self.assertEqual(journeys.where('NOT is_valid').count(),0)
        self.assertEqual(journeys.where("provider='AKT'").select('entity_id').distinct().count(),1)
        self.assertEqual({r.line_public_code for r in journeys.where("provider='AKT'").collect()},{'007','008'})
        times=self.spark.read.parquet(str(silver/'passing_times'))
        self.assertEqual(times.count(),8)
        self.assertEqual(times.where('arrival_seconds=87000').count(),3)
        self.assertEqual(times.where("earliest_departure_time_raw='09:00:00'").count(),2)
        dates=self.spark.read.parquet(str(silver/'journey_service_dates'))
        self.assertEqual(dates.count(),20)
        self.assertEqual(dates.where("service_date='2026-01-16'").count(),0)
        reconciliation=json.loads((gold/'metadata/manifest.json').read_text())['reconciliation']
        self.assertEqual(reconciliation['gold_scheduled_departures'],15)
        self.assertEqual(reconciliation['excluded_service_occurrences'],5)
        repeat=run_snapshot(self.spark,manifest,None,self.root/'data')
        self.assertTrue(all(stage['reused'] for stage in repeat.values()))
        self.assertEqual(result['gold']['path'],repeat['gold']['path'])
    def test_invalid_times_are_quarantined_and_excluded_from_gold(self):
        from pipeline.run_snapshot import run_snapshot
        result=run_snapshot(self.spark,complete_fixture(self.root/'bronze',bad_time=True),None,self.root/'data')
        silver=Path(result['silver']['path']);gold=Path(result['gold']['path'])
        self.assertEqual(self.spark.read.parquet(str(silver/'quarantine_passing_times')).count(),2)
        self.assertEqual(self.spark.read.parquet(str(silver/'quarantine_journeys')).count(),2)
        reconciliation=json.loads((gold/'metadata/manifest.json').read_text())['reconciliation']
        self.assertEqual(reconciliation['gold_scheduled_departures'],5)
        self.assertEqual(reconciliation['excluded_service_occurrences'],15)

    def test_ambiguous_unversioned_references_do_not_multiply_source_rows(self):
        from pipeline.processing.silver.build import lookup
        source=self.spark.createDataFrame([
            ('unversioned','KOL','line.xml','same',None),
            ('versioned','KOL','line.xml','same','2'),
            ('missing','KOL','line.xml','absent',None)],
            ['occurrence_key','provider','source_member','ref_id','ref_version'])
        target=self.spark.createDataFrame([
            ('first','KOL','line.xml','same','1'),('second','KOL','line.xml','same','2')],
            ['occurrence_key','provider','source_member','entity_id','entity_version'])
        rows={r.occurrence_key:r for r in lookup(source,target,'ref_id','ref_version','match_').collect()}
        self.assertEqual(len(rows),3)
        self.assertFalse(rows['unversioned'].match_resolved)
        self.assertEqual(rows['unversioned'].match_matches,2)
        self.assertTrue(rows['versioned'].match_resolved)
        self.assertEqual(rows['versioned'].match_entity_version,'2')
        self.assertFalse(rows['missing'].match_resolved)
