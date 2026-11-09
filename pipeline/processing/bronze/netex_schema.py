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


def complete_frame_schema():
    """Provider frames including the reference definitions used by Silver."""
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType

    s = StringType()

    def struct(*fields):
        return StructType([StructField(name, kind, True) for name, kind in fields]
                          + [StructField("_unmapped_xml", ArrayType(s), True)])

    def extend(schema, *fields):
        return StructType(schema.fields + [StructField(name, kind, True) for name, kind in fields])

    ref = struct(("_ref", s), ("_version", s), ("_order", s))
    validity = struct(("AvailabilityCondition", ArrayType(struct(
        ("_id", s), ("_version", s), ("FromDate", s), ("ToDate", s),
        ("IsAvailable", s)))))
    point = struct(("_id", s), ("_version", s), ("_order", s),
                   ("ScheduledStopPointRef", ref), ("ForBoarding", s), ("ForAlighting", s))
    pattern = struct(("_id", s), ("_version", s), ("Name", s), ("RouteRef", ref),
                     ("pointsInSequence", struct(("StopPointInJourneyPattern", ArrayType(point)))))
    route = struct(("_id", s), ("_version", s), ("Name", s), ("ShortName", s),
                   ("LineRef", ref), ("FlexibleLineRef", ref), ("DirectionType", s),
                   ("pointsInSequence", struct(("PointOnRoute", ArrayType(struct(
                       ("_id", s), ("_version", s), ("_order", s), ("RoutePointRef", ref)))))))
    stop = struct(("_id", s), ("_version", s), ("Name", s))
    assignment = struct(("_id", s), ("_version", s), ("_order", s),
                        ("ScheduledStopPointRef", ref), ("QuayRef", ref),
                        ("StopPlaceRef", ref), ("FlexibleStopPlaceRef", ref))
    day_type = struct(("_id", s), ("_version", s),
                      ("properties", struct(("PropertyOfDay", ArrayType(struct(
                          ("DaysOfWeek", s), ("WeeksOfMonth", s), ("MonthOfYear", s),
                          ("DayOfYear", s), ("HolidayTypes", s), ("Seasons", s)))))))
    period = struct(("_id", s), ("_version", s), ("FromDate", s), ("ToDate", s),
                    ("FromOperatingDayRef", ref), ("ToOperatingDayRef", ref))
    day = struct(("_id", s), ("_version", s), ("CalendarDate", s), ("EarliestTime", s), ("DayLength", s))
    day_assignment = struct(("_id", s), ("_version", s), ("_order", s), ("Date", s),
                            ("DayTypeRef", ref), ("OperatingPeriodRef", ref), ("OperatingDayRef", ref),
                            ("isAvailable", s))
    calendar = struct(("_id", s), ("_version", s),
                      ("dayTypes", struct(("DayType", ArrayType(day_type)))),
                      ("operatingPeriods", struct(("OperatingPeriod", ArrayType(period)))),
                      ("operatingDays", struct(("OperatingDay", ArrayType(day)))),
                      ("dayTypeAssignments", struct(("DayTypeAssignment", ArrayType(day_assignment)))))
    schema = composite_frame_schema()
    frames = schema["frames"].dataType
    service = extend(frames["ServiceFrame"].dataType.elementType,
                     ("routes", struct(("Route", ArrayType(route)))),
                     ("journeyPatterns", struct(("JourneyPattern", ArrayType(pattern)))),
                     ("scheduledStopPoints", struct(("ScheduledStopPoint", ArrayType(stop)))),
                     ("stopAssignments", struct(("PassengerStopAssignment", ArrayType(assignment)),
                                                ("FlexibleStopAssignment", ArrayType(assignment)))))
    # Preserve flexible operating windows as distinct fields; they are not fixed departures.
    timetable = frames["TimetableFrame"].dataType.elementType
    journeys = timetable["vehicleJourneys"].dataType
    def with_windows(kind):
        journey = journeys[kind].dataType.elementType
        passing = journey["passingTimes"].dataType["TimetabledPassingTime"].dataType.elementType
        passing = extend(passing, ("EarliestDepartureTime", s), ("LatestArrivalTime", s))
        journey = StructType([StructField(f.name, struct(("TimetabledPassingTime", ArrayType(passing))))
                              if f.name == "passingTimes" else f for f in journey.fields])
        return extend(journey, ("DepartureDayOffset", s),
                      ("EarliestDepartureTime", s), ("LatestArrivalTime", s),
                      ("validityConditions", validity))
    timetable = StructType([StructField(f.name, struct(
        ("ServiceJourney", ArrayType(with_windows("ServiceJourney"))),
        ("FlexibleServiceJourney", ArrayType(with_windows("FlexibleServiceJourney")))))
        if f.name == "vehicleJourneys" else f for f in timetable.fields])
    site = struct(("_id", s), ("_version", s),
                  ("flexibleStopPlaces", struct(("FlexibleStopPlace", ArrayType(struct(
                      ("_id", s), ("_version", s), ("Name", s), ("Description", s),
                      ("TransportMode", s)))))))
    new_frames = StructType([StructField(f.name, ArrayType(service)) if f.name == "ServiceFrame"
                            else StructField(f.name, ArrayType(timetable)) if f.name == "TimetableFrame"
                            else f for f in frames.fields]
                           + [StructField("ServiceCalendarFrame", ArrayType(calendar)),
                              StructField("SiteFrame", ArrayType(site))])
    return StructType([StructField(f.name, new_frames) if f.name == "frames" else f for f in schema.fields]
                      + [StructField("validityConditions", validity)])


def stop_place_schema():
    from pyspark.sql.types import ArrayType, StringType, StructField, StructType
    s = StringType()
    def struct(*fields):
        return StructType([StructField(name, kind, True) for name, kind in fields])
    location = struct(("Longitude", s), ("Latitude", s))
    centroid = struct(("Location", location))
    quay = struct(("_id", s), ("_version", s), ("Name", s), ("PublicCode", s), ("Centroid", centroid))
    return struct(("_id", s), ("_version", s), ("Name", s), ("StopPlaceType", s),
                  ("TransportMode", s), ("Centroid", centroid),
                  ("ParentSiteRef", struct(("_ref", s), ("_version", s))),
                  ("quays", struct(("Quay", ArrayType(quay)))))
