import unittest
from pipeline.processing.silver.calendars import expand_calendars


def row(identifier,**fields):return {'provider':'KOL','record':{'_id':identifier,'_version':'1',**fields}}
def ref(identifier):return {'_ref':identifier,'_version':'1'}


class CalendarTests(unittest.TestCase):
    def test_weekday_period_inclusive_end_and_date_exceptions(self):
        day=row('day',properties={'PropertyOfDay':[{'DaysOfWeek':'Weekdays'}]})
        period=row('period',FromDate='2026-01-15T00:00:00',ToDate='2026-01-21T00:00:00')
        assignments=[row('base',DayTypeRef=ref('day'),OperatingPeriodRef=ref('period')),
                     row('remove',DayTypeRef=ref('day'),Date='2026-01-16',isAvailable='false'),
                     row('add',DayTypeRef=ref('day'),Date='2026-01-17',isAvailable='true')]
        rows,issues=expand_calendars([day],[period],[],assignments,'2026-01-15','2026-01-21')
        self.assertEqual([str(r[3]) for r in rows],['2026-01-15','2026-01-17','2026-01-19','2026-01-20','2026-01-21'])
        self.assertEqual(issues,[])
    def test_operating_day_references_and_negative_offset_are_not_calendar_dates(self):
        days=[row('first',CalendarDate='2026-01-15'),row('last',CalendarDate='2026-01-16')]
        periods=[row('period',FromOperatingDayRef=ref('first'),ToOperatingDayRef=ref('last'))]
        rows,issues=expand_calendars([row('day')],periods,days,
            [row('assignment',DayTypeRef=ref('day'),OperatingPeriodRef=ref('period'))],'2026-01-15','2026-01-21')
        self.assertEqual(len(rows),2);self.assertFalse(issues)
    def test_unknown_rules_or_ambiguous_references_produce_issues(self):
        rows,issues=expand_calendars([row('day',properties={'PropertyOfDay':[{'HolidayTypes':'SchoolDay'}]})],[],[],[],
                                   '2026-01-15','2026-01-21')
        self.assertFalse(rows);self.assertIn('Unsupported holiday',issues[0][3])
        rows,issues=expand_calendars([row('day')],[],[],[row('a',DayTypeRef=ref('day'),OperatingPeriodRef=ref('missing'))],
                                   '2026-01-15','2026-01-21')
        self.assertFalse(rows);self.assertIn('Unresolved',issues[0][3])
