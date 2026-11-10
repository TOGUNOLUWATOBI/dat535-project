"""Shared test data and the shared Spark session.

Test modules import from here instead of from each other, so each test file
stays self-contained.
"""

from datetime import date
import json
import os
import sys
import zipfile
import zlib

from pipeline.common import sha256_file
from pipeline.ingestion.generate_provider_dataset import NETEX, validate_snapshot


def spark_session():
    """One local Spark session for every Spark test class in a run (started on first use).

    Test classes must not stop it; it ends with the test process. A class that
    changes session settings restores them in its ``tearDownClass``.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.pop("SPARK_HOME", None)  # use the Spark bundled with PySpark
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.master("local[2]").appName("DAT535-tests")
             .config("spark.ui.enabled", "false").config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "Europe/Oslo")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# Ingestion: national-style ZIPs ------------------------------------------------

def document(body, publication="2026-01-15T00:00:00"):
    """A NeTEx PublicationDelivery wrapping ``body``."""
    return (
        f'<PublicationDelivery xmlns="{NETEX}" version="1.15:NO-NeTEx-networktimetable:1.5">'
        f'<PublicationTimestamp>{publication}</PublicationTimestamp>{body}</PublicationDelivery>'
    )


def provider_zip(path, missing=False, akt_publication="2026-01-15T00:00:00"):
    """A small national-style ZIP with KOL + AKT line/shared XML, stops and another provider."""
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for provider in ("KOL", "AKT"):
            body = (
                f'<Line id="{provider}:Line:1" version="1"><TransportMode>bus</TransportMode>'
                f'<LineRef ref="{provider}:Line:1" version="1"/>'
                f'<OperatorRef ref="{provider}:Operator:1"/>'
                '<StopPlaceRef ref="NSR:StopPlace:1"/></Line>'
            )
            archive.writestr(
                f"{provider}_line.xml", document(body, akt_publication if provider == "AKT" else "2026-01-15T00:00:00")
            )
            shared = '' if missing and provider == "AKT" else f'<Operator id="{provider}:Operator:1" version="1"/>'
            archive.writestr(f"_{provider}_shared_data.xml", document(shared))
        archive.writestr("_KOL_flexible_shared_data.xml", document(""))
        archive.writestr("_stops.xml", document('<StopPlace id="NSR:StopPlace:1" version="3"/>'))
        archive.writestr("OTHER_line.xml", document(""))
        archive.writestr("_OTHER_shared_data.xml", document('<Operator id="AKT:Operator:1" version="1"/>'))


# Processing: a complete validated snapshot -------------------------------------

def complete_fixture(root, bad_time=False, bad_calendar=False, full_month=False):
    """A validated KOL + AKT ingestion run (line, flexible, shared and stop XML); returns its manifest.

    ``bad_time`` and ``bad_calendar`` inject quarantinable AKT errors; ``full_month``
    widens validity from 15–21 January to the whole month.
    """
    xml = root / 'xml/2026-01-15'
    xml.mkdir(parents=True)
    metadata = root / 'metadata'
    metadata.mkdir()

    def publication(body):
        return (
            '<PublicationDelivery xmlns="http://www.netex.org.uk/netex" version="1.15:NO-NeTEx-networktimetable:1.5"><PublicationTimestamp>2026-01-15T00:00:00</PublicationTimestamp><dataObjects>'
            + body
            + '</dataObjects></PublicationDelivery>'
        )

    for provider, number, flexible in [
        ('KOL', '007', False),
        ('AKT', '007', False),
        ('AKT', '008', False),
        ('KOL', 'HentMeg', True),
    ]:
        line_kind = 'FlexibleLine' if flexible else 'Line'
        line_ref = 'FlexibleLineRef' if flexible else 'LineRef'
        timing1 = (
            '<EarliestDepartureTime>09:00:00</EarliestDepartureTime><LatestArrivalTime>13:00:00</LatestArrivalTime>'
            if flexible
            else '<DepartureTime>23:55:00</DepartureTime>'
        )
        timing2 = (
            timing1
            if flexible
            else '<ArrivalTime>'
            + ('25:10:00' if bad_time and provider == 'AKT' else '00:10:00')
            + '</ArrivalTime><ArrivalDayOffset>1</ArrivalDayOffset>'
        )
        body = f'''<CompositeFrame id="{provider}:CompositeFrame:1" version="1"><validityConditions><AvailabilityCondition id="{provider}:Availability:1"><FromDate>2026-01-15T00:00:00</FromDate><ToDate>2026-01-21T00:00:00</ToDate></AvailabilityCondition></validityConditions><frames>
<ServiceFrame id="{provider}:ServiceFrame:1" version="1"><lines><{line_kind} id="{provider}:{line_kind}:1" version="0001"><Name>Test æ</Name><PublicCode>{number}</PublicCode><TransportMode>bus</TransportMode><TransportSubmode><BusSubmode>localBus</BusSubmode></TransportSubmode></{line_kind}></lines>
<routes><Route id="{provider}:Route:1" version="1"><{line_ref} ref="{provider}:{line_kind}:1" version="0001"/></Route></routes>
<journeyPatterns><JourneyPattern id="{provider}:JourneyPattern:1" version="1"><RouteRef ref="{provider}:Route:1" version="1"/><pointsInSequence>
<StopPointInJourneyPattern id="{provider}:Point:1" version="1" order="1"><ScheduledStopPointRef ref="{provider}:Stop:1"/></StopPointInJourneyPattern>
<StopPointInJourneyPattern id="{provider}:Point:2" version="1" order="2"><ScheduledStopPointRef ref="{provider}:Stop:2"/></StopPointInJourneyPattern>
</pointsInSequence></JourneyPattern></journeyPatterns></ServiceFrame>
<TimetableFrame id="{provider}:TimetableFrame:1" version="1"><vehicleJourneys><ServiceJourney id="{provider}:Journey:repeat" version="1"><JourneyPatternRef ref="{provider}:JourneyPattern:1" version="1"/><dayTypes><DayTypeRef ref="{provider}:DayType:1"/></dayTypes><passingTimes>
<TimetabledPassingTime id="{provider}:Time:repeat" version="1"><StopPointInJourneyPatternRef ref="{provider}:Point:1"/>{timing1}</TimetabledPassingTime>
<TimetabledPassingTime id="{provider}:Time:repeat" version="1"><StopPointInJourneyPatternRef ref="{provider}:Point:2"/>{timing2}</TimetabledPassingTime>
</passingTimes></ServiceJourney></vehicleJourneys></TimetableFrame></frames></CompositeFrame>'''
        if full_month:
            body = body.replace('2026-01-15T00:00:00', '2026-01-01T00:00:00').replace(
                '2026-01-21T00:00:00', '2026-01-31T00:00:00'
            )
        (xml / f'{provider}_{number}.xml').write_text(publication(body))
    for p in ('KOL', 'AKT'):
        body = f'''<CompositeFrame id="{p}:CompositeFrame:shared" version="1"><frames><ServiceFrame id="{p}:ServiceFrame:shared" version="1"><scheduledStopPoints><ScheduledStopPoint id="{p}:Stop:1" version="1"><Name>First</Name></ScheduledStopPoint><ScheduledStopPoint id="{p}:Stop:2" version="1"/></scheduledStopPoints><stopAssignments><PassengerStopAssignment id="{p}:Assignment:1" version="1"><ScheduledStopPointRef ref="{p}:Stop:1" version="1"/><QuayRef ref="NSR:Quay:1"/></PassengerStopAssignment><PassengerStopAssignment id="{p}:Assignment:2" version="1"><ScheduledStopPointRef ref="{p}:Stop:2" version="1"/><QuayRef ref="NSR:Quay:2"/></PassengerStopAssignment></stopAssignments></ServiceFrame>
<ServiceCalendarFrame id="{p}:ServiceCalendarFrame:1" version="1"><dayTypes><DayType id="{p}:DayType:1" version="1"><properties><PropertyOfDay><DaysOfWeek>Weekdays</DaysOfWeek></PropertyOfDay></properties></DayType></dayTypes><operatingPeriods><OperatingPeriod id="{p}:Period:1" version="1"><FromDate>2026-01-15T00:00:00</FromDate><ToDate>2026-01-21T00:00:00</ToDate></OperatingPeriod></operatingPeriods><dayTypeAssignments>
<DayTypeAssignment id="{p}:DayTypeAssignment:1" version="1"><OperatingPeriodRef ref="{p}:Period:1"/><DayTypeRef ref="{p}:DayType:1"/></DayTypeAssignment>
<DayTypeAssignment id="{p}:DayTypeAssignment:2" version="1"><Date>2026-01-16</Date><DayTypeRef ref="{p}:DayType:1"/><isAvailable>false</isAvailable></DayTypeAssignment>
<DayTypeAssignment id="{p}:DayTypeAssignment:3" version="1"><Date>2026-01-17</Date><DayTypeRef ref="{p}:DayType:1"/></DayTypeAssignment>
</dayTypeAssignments></ServiceCalendarFrame></frames></CompositeFrame>'''
        if bad_calendar and p == 'AKT':
            body = body.replace('</DaysOfWeek>', '</DaysOfWeek><HolidayTypes>PublicHoliday</HolidayTypes>')
        if full_month:
            body = body.replace('2026-01-15T00:00:00', '2026-01-01T00:00:00').replace(
                '2026-01-21T00:00:00', '2026-01-31T00:00:00'
            )
        (xml / f'_{p}_shared_data.xml').write_text(publication(body))
    (xml / '_stops.xml').write_text(
        publication(
            '<SiteFrame id="NSR:SiteFrame:1" version="1"><stopPlaces><StopPlace id="NSR:StopPlace:1" version="1"><Name>Test station</Name><quays><Quay id="NSR:Quay:1" version="1"><Name>First</Name></Quay><Quay id="NSR:Quay:2" version="1"/></quays></StopPlace></stopPlaces></SiteFrame>'
        )
    )
    lineage = []
    for p in sorted(xml.glob('*.xml')):
        provider = next(
            (v for v in ('KOL', 'AKT') if p.name.startswith(v + '_') or p.name.startswith('_' + v + '_')), None
        )
        lineage.append(
            {
                'member': p.name,
                'xml_path': str(p.relative_to(root)),
                'provider': provider,
                'bytes': p.stat().st_size,
                'sha256': sha256_file(p),
                'source_crc32': zlib.crc32(p.read_bytes()),
            }
        )
    report = validate_snapshot(xml, lineage, ['KOL', 'AKT'], date(2026, 1, 15))
    if report['status'] not in ('passed', 'passed_with_warnings'):
        raise AssertionError(report)
    (metadata / 'validation.json').write_text(json.dumps(report))
    (metadata / 'lineage.json').write_text(json.dumps(lineage))
    path = metadata / 'manifest.json'
    path.write_text(
        json.dumps(
            {
                'status': 'complete_with_warnings',
                'snapshot_date': '2026-01-15',
                'providers': ['KOL', 'AKT'],
                'xml_directory': 'xml/2026-01-15',
            }
        )
    )
    return path


# GUI: daily_line_service rows --------------------------------------------------

def service_fixture():
    """Partial windows exercise unknown dates versus covered dates with zero service."""
    import pandas as pd
    rows = [
        ("2026-01-15", "AKT", "shared", "10", "bus", "2026-01-15", 10),
        ("2026-01-15", "AKT", "shared", "10", "bus", "2026-01-16", 10),
        ("2026-01-15", "AKT", "water", "B1", "water", "2026-01-17", 5),
        ("2026-01-15", "KOL", "shared", "10", "bus", "2026-01-15", 7),
        ("2026-01-15", "KOL", "water", "B2", "water", "2026-01-16", 2),
        ("2026-02-15", "AKT", "shared", "10", "bus", "2026-02-15", 30),
        ("2026-02-15", "AKT", "shared", "10", "bus", "2026-02-16", 5),
        ("2026-02-15", "AKT", "new", "20", "bus", "2026-02-16", 9),
        ("2026-02-15", "KOL", "shared", "10", "bus", "2026-02-15", 7),
        ("2026-02-15", "KOL", "water", "B2", "water", "2026-02-15", 4),
    ]
    frame = pd.DataFrame(rows, columns=["snapshot_date", "provider", "line_entity_id", "line_public_code",
                                        "line_transport_mode", "service_date", "scheduled_departures"])
    frame["first_departure_seconds"], frame["last_departure_seconds"] = 21600, 79200
    items = [{"snapshot_date": d, "service_from": d, "service_to": d[:8] + "21",
              "gold_manifest": {"providers": ["AKT", "KOL"]}} for d in ["2026-01-15", "2026-02-15"]]
    return frame, items


def current_month_fixture():
    """Future service is deliberately large so leaking it changes visible totals."""
    import pandas as pd
    rows = []
    for month, source, dates in [
        ("2026-10", "2026-10-15", [(1, 5), (10, 15), (11, 50), (31, 100)]),
        ("2026-11", "2026-11-09", [(1, 10), (10, 20), (11, 100), (30, 200)]),
    ]:
        rows.extend((source, "AKT", "line", "10", "bus", f"{month}-{day:02d}", count) for day, count in dates)
    daily = pd.DataFrame(rows, columns=["snapshot_date", "provider", "line_entity_id", "line_public_code",
                                       "line_transport_mode", "service_date", "scheduled_departures"])
    daily["first_departure_seconds"], daily["last_departure_seconds"] = 21600, 79200
    items = [{"snapshot_date": source, "service_from": month + "-01", "service_to": month + end,
              "gold_manifest": {"providers": ["AKT"]}}
             for month, source, end in [("2026-10", "2026-10-15", "-31"), ("2026-11", "2026-11-09", "-30")]]
    return daily, items
