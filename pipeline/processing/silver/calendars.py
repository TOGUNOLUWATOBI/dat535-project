"""Bounded Norwegian-profile calendar expansion with explicit exceptions.

These reference tables are small and are resolved on the driver. Journey and
passing-time joins remain Spark transformations. Unsupported calendar rules
are reported rather than interpreted as everyday service.
"""

from collections import defaultdict
from datetime import date, timedelta

WEEKDAYS = {name: {i} for i, name in enumerate(
    ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))}
WEEKDAYS.update(Everyday=set(range(7)), Weekdays=set(range(5)), Weekend={5, 6})


def expand_calendars(day_types, periods, operating_days, assignments, start, end):
    start, end = date.fromisoformat(str(start)), date.fromisoformat(str(end))
    if end < start or (end-start).days > 365:
        raise ValueError("Service-date window must contain 1–366 days")
    def value(row):
        return row.asDict(recursive=True) if hasattr(row, "asDict") else row
    def index(rows):
        result = defaultdict(list)
        for raw in rows:
            row = value(raw)
            result[(row["provider"], row["record"]["_id"])].append(row)
        return result
    day_index, period_index = index(operating_days), index(periods)
    def resolve(collection, provider, ref):
        if not ref or not ref.get("_ref"): raise ValueError("Missing calendar reference")
        rows = collection.get((provider, ref["_ref"]), [])
        version = ref.get("_version")
        if version and version not in ("any", "*"):
            rows = [r for r in rows if r["record"].get("_version") == version]
        if len(rows) != 1: raise ValueError(f"Unresolved or ambiguous calendar reference: {ref['_ref']}")
        return rows[0]
    def day_of(ref, provider):
        return date.fromisoformat(resolve(day_index, provider, ref)["record"]["CalendarDate"][:10])
    def bounds(row):
        record = row["record"]; provider = row["provider"]
        first = (date.fromisoformat(record["FromDate"][:10]) if record.get("FromDate")
                 else day_of(record.get("FromOperatingDayRef"), provider))
        last = (date.fromisoformat(record["ToDate"][:10]) if record.get("ToDate")
                else day_of(record.get("ToOperatingDayRef"), provider))
        if last < first: raise ValueError("Reversed operating period")
        return first, last
    assignment_index = defaultdict(list)
    for raw in assignments:
        row = value(raw); ref = row["record"].get("DayTypeRef") or {}
        assignment_index[(row["provider"], ref.get("_ref"))].append(row)
    result, issues = [], []
    for raw in day_types:
        row = value(raw); record = row["record"]; provider = row["provider"]; identifier = record["_id"]
        try:
            props = (record.get("properties") or {}).get("PropertyOfDay") or []
            allowed = set() if props else set(range(7))
            for prop in props:
                if any(prop.get(k) for k in ("WeeksOfMonth", "MonthOfYear", "DayOfYear", "Seasons", "_unmapped_xml")):
                    raise ValueError("Unsupported PropertyOfDay restriction")
                if prop.get("HolidayTypes") not in (None, "", "AnyDay"):
                    raise ValueError("Unsupported holiday restriction")
                tokens = (prop.get("DaysOfWeek") or "Everyday").split()
                if any(t not in WEEKDAYS for t in tokens): raise ValueError("Unknown weekday token")
                for token in tokens: allowed.update(WEEKDAYS[token])
            candidates = defaultdict(list)
            for assignment in assignment_index.get((provider, identifier), []):
                a = assignment["record"]; ref_version = (a.get("DayTypeRef") or {}).get("_version")
                if ref_version and ref_version not in ("any", "*", record.get("_version")): continue
                selector_count = sum(bool(a.get(k)) for k in ("Date", "OperatingDayRef", "OperatingPeriodRef"))
                if selector_count != 1: raise ValueError("Calendar assignment must select one date/day/period")
                available = a.get("isAvailable")
                if available not in (None, "true", "false", "1", "0"):
                    raise ValueError("Invalid isAvailable boolean")
                available = available not in ("false", "0")
                specific = not bool(a.get("OperatingPeriodRef"))
                if a.get("Date"): first = last = date.fromisoformat(a["Date"][:10])
                elif a.get("OperatingDayRef"): first = last = day_of(a["OperatingDayRef"], provider)
                else: first, last = bounds(resolve(period_index, provider, a["OperatingPeriodRef"]))
                first, last = max(first, start), min(last, end)
                if row.get("valid_from"): first = max(first, date.fromisoformat(row["valid_from"][:10]))
                if row.get("valid_to"): last = min(last, date.fromisoformat(row["valid_to"][:10]))
                for offset in range(max(0, (last-first).days+1)):
                    day = first + timedelta(days=offset)
                    if specific or not available or day.weekday() in allowed:
                        candidates[day].append((2 if specific else 1, available))
            for day, choices in sorted(candidates.items()):
                priority = max(c[0] for c in choices)
                # Explicit date assignments override periods. At equal specificity,
                # an exclusion wins, independent of XML ordering.
                if all(available for rank, available in choices if rank == priority):
                    result.append((provider, identifier, record.get("_version"), day))
        except (ValueError, TypeError, KeyError) as error:
            issues.append((provider, identifier, record.get("_version"), str(error)))
    return result, issues
