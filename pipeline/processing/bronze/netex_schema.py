"""Explicit Spark schemas for the first NeTEx staging projection.

IDs, versions, public codes and timetable values remain strings. Unprojected
child elements are retained as XML fragments via Spark's wildcard field.
Original XML is authoritative for fields/attributes outside this projection.
"""


def composite_frame_schema():
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType

    def struct(*fields):
        return StructType([StructField(name, kind, True) for name, kind in fields]
                          + [StructField("_unmapped_xml", ArrayType(StringType()), True)])

    string = StringType()
    reference = struct(("_ref", string), ("_version", string), ("_order", string))
    line = struct(
        ("_id", string), ("_version", string), ("Name", string),
        ("PublicCode", string), ("PrivateCode", string), ("TransportMode", string),
        ("OperatorRef", reference), ("RepresentedByGroupRef", reference),
    )
    passing_time = struct(
        ("_id", string), ("_version", string), ("_order", string),
        ("StopPointInJourneyPatternRef", reference),
        ("ArrivalTime", string), ("DepartureTime", string),
        ("ArrivalDayOffset", string), ("DepartureDayOffset", string),
    )
    journey = struct(
        ("_id", string), ("_version", string), ("Name", string),
        ("PrivateCode", string), ("DepartureTime", string),
        ("JourneyPatternRef", reference), ("LineRef", reference),
        ("OperatorRef", reference),
        ("dayTypes", struct(("DayTypeRef", ArrayType(reference)))),
        ("passingTimes", struct(("TimetabledPassingTime", ArrayType(passing_time)))),
    )
    service_frame = struct(
        ("_id", string), ("_version", string),
        ("lines", struct(("Line", ArrayType(line)), ("FlexibleLine", ArrayType(line)))),
    )
    timetable_frame = struct(
        ("_id", string), ("_version", string),
        ("vehicleJourneys", struct(("ServiceJourney", ArrayType(journey)),
                                   ("FlexibleServiceJourney", ArrayType(journey)))),
    )
    return struct(
        ("_id", string), ("_version", string), ("_created", string),
        ("frames", struct(("ServiceFrame", ArrayType(service_frame)),
                           ("TimetableFrame", ArrayType(timetable_frame)))),
    )
