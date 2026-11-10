"""Calendar-month service windows, independent of the export observation date."""

from calendar import monthrange
from datetime import date


def month_bounds(snapshot):
    day = date.fromisoformat(str(snapshot))
    return day.replace(day=1), day.replace(day=monthrange(day.year, day.month)[1])


def service_window(snapshot, first=None, last=None):
    start, end = month_bounds(snapshot)
    start = date.fromisoformat(str(first)) if first else start
    end = date.fromisoformat(str(last)) if last else end
    if end < start or (end - start).days > 365:
        raise ValueError("Service window must contain 1–366 days")
    return start, end


def window_signature(item):
    """All calendar months share a policy even when they have 28/29/30/31 days."""
    first, last = date.fromisoformat(item["service_from"]), date.fromisoformat(item["service_to"])
    if (first, last) == month_bounds(first):
        return ("calendar_month",)
    return ("relative", (last - first).days + 1,
            (first - date.fromisoformat(item["snapshot_date"])).days)
