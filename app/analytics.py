"""Read-only analysis of the published Gold tables; no Spark session is needed."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def today_in_oslo():
    """Use the transport system's calendar date, including on a UTC-hosted server."""
    return datetime.now(ZoneInfo("Europe/Oslo")).date()


def select_service(daily, providers=None, mode="all"):
    """Copy of the daily rows for the chosen providers (None = all) and transport mode."""
    selected = daily
    if providers is not None:
        selected = selected[selected.provider.isin(providers)]
    if mode != "all":
        selected = selected[selected.line_transport_mode == mode]
    return selected.copy()


def percentage_change(before, after):
    """A zero baseline has no defined percentage change."""
    return (after - before) / before * 100 if before else None


def compare_exports(daily, before, after, providers=None, mode="all", line_snapshots=None):
    """Compare fixed departures by provider and NeTEx line ID, without guessing renumberings."""
    if before >= after:
        raise ValueError("Choose an earlier export before the later export")
    selected = select_service(daily, providers, mode)
    keys = ["provider", "line_entity_id"]
    codes = lambda values: " / ".join(sorted({str(v) for v in values.dropna() if str(v)}))

    def totals(snapshot, side):
        part = selected[selected.snapshot_date == snapshot]
        return part.groupby(keys).agg(**{
            side: ("scheduled_departures", "sum"), f"code_{side}": ("line_public_code", codes)})

    result = totals(before, "before").join(totals(after, "after"), how="outer").reset_index()
    for side in ("before", "after"):
        result[side] = result[side].fillna(0).astype("int64")
        result[f"code_{side}"] = result[f"code_{side}"].fillna("")
    result["change"] = result.after - result.before
    result["change_pct"] = [percentage_change(a, b) for a, b in zip(result.before, result.after)]
    result["service_status"] = ["New service in window" if a == 0 and b > 0 else
                                 "No service in later window" if a > 0 and b == 0 else
                                 "Continuing service" for a, b in zip(result.before, result.after)]
    result["number"] = result.code_after.where(result.code_after != "", result.code_before)
    result["route"] = ""
    if line_snapshots is not None and not line_snapshots.empty:
        attributes = line_snapshots[line_snapshots.snapshot_date.isin([before, after])]
        attributes = attributes.sort_values("snapshot_date").drop_duplicates(["provider", "line_id"], keep="last")
        names = attributes.set_index(["provider", "line_id"])["name"].fillna("").to_dict()
        result["route"] = [names.get((p, line), "") or "" for p, line in zip(result.provider, result.line_entity_id)]
    result["label"] = (result.provider + " · " + result.number + " " + result.route).str.strip()
    result["earlier_export"], result["later_export"] = before, after
    return result.sort_values(["change", "provider", "number"]).reset_index(drop=True)


def daily_profile(selected, item, through=None):
    """One row per calendar day in a published window; days without service stay visible."""
    end = min(pd.Timestamp(item["service_to"]), pd.Timestamp(through)) if through is not None else item["service_to"]
    dates = pd.date_range(item["service_from"], end)
    frame = selected[selected.snapshot_date == item["snapshot_date"]].copy()
    frame["service_date"] = pd.to_datetime(frame.service_date)
    if not frame.empty and not frame.service_date.between(pd.Timestamp(item["service_from"]), pd.Timestamp(item["service_to"])).all():
        raise ValueError("Published service dates fall outside the declared window")
    frame = frame[frame.service_date.isin(dates)]
    result = frame.groupby("service_date").agg(
        departures=("scheduled_departures", "sum"), first=("first_departure_seconds", "min"),
        last=("last_departure_seconds", "max")).reindex(dates)
    result["departures"] = result.departures.fillna(0).astype("int64")
    result.index.name = "service_date"
    result = result.reset_index()
    result["weekday"] = result.service_date.dt.day_name()
    return result


def daily_month(daily, items, month, providers=None, mode="all", through=None):
    """Calendar dates with one whole valid published plan per day.

    Overlapping windows use the latest covering export. This is a reconstructed
    month of published plans, not what was known on each operating date.
    Missing lines in that export are not backfilled from older plans.
    Unprocessed dates remain null; covered dates without service are zero.
    A display cutoff excludes later operating dates without changing source data.
    """
    period = pd.Period(month, freq="M")
    end = min(period.end_time, pd.Timestamp(through)) if through is not None else period.end_time
    calendar = pd.DataFrame({"service_date": pd.date_range(period.start_time, end)})
    sources = []
    for operating_day in calendar.service_date.dt.strftime("%Y-%m-%d"):
        def covers(item):
            # Select a complete combined export independently of the UI filter,
            # so an operator's count does not change when another is selected.
            operators = item["gold_manifest"]["providers"]
            coverage = item["gold_manifest"].get("source_calendar_coverage")
            return (item["service_from"] <= operating_day <= item["service_to"] and
                    (coverage is None or all((coverage.get(p) or {}).get("from", "9999") <= operating_day
                                             <= (coverage.get(p) or {}).get("to", "0000") for p in operators)))
        candidates = [i["snapshot_date"] for i in items if covers(i)]
        sources.append(max(candidates) if candidates else None)
    calendar["source_export"] = pd.Series(sources, dtype="object")
    calendar["covered"] = calendar.source_export.notna()
    selected = select_service(daily, providers, mode)
    selected["service_date"] = pd.to_datetime(selected.service_date)
    # Join the chosen export first: adding all snapshots would count overlap twice.
    chosen = selected.merge(calendar, left_on=["service_date", "snapshot_date"],
                            right_on=["service_date", "source_export"], how="inner")
    chosen["_line"] = list(zip(chosen.provider, chosen.line_entity_id))
    totals = chosen.groupby("service_date").agg(
        departures=("scheduled_departures", "sum"), active_lines=("_line", "nunique"),
        first=("first_departure_seconds", "min"), last=("last_departure_seconds", "max"))
    result = calendar.join(totals, on="service_date")
    for name in ("departures", "active_lines"):
        result[name] = result[name].fillna(0).where(result.covered).astype("Int64")
    result["weekday"] = result.service_date.dt.day_name()
    result["coverage"] = result.covered.map({True: "Published window", False: "Not covered"})
    return result


def monthly_series(daily, items, providers=None, mode="all", through=None):
    """Monthly totals reconstructed from the same dated plans as Daily service."""
    operators = sorted(providers if providers is not None else
                       {p for item in items for p in item["gold_manifest"]["providers"]})
    end = max(i["service_to"] for i in items)
    end = min(end, str(through)) if through is not None else end
    months = pd.period_range(min(i["service_from"] for i in items), end, freq="M")
    rows = []
    for month in months:
        for provider in operators:
            chosen, profile = monthly_plan_rows(daily, items, str(month), [provider], mode, through)
            known = profile[profile.covered]
            rows.append({"month": str(month), "month_start": month.start_time, "provider": provider,
                         "departures": int(known.departures.sum()) if len(known) else None,
                         "active_lines": chosen.line_entity_id.nunique(), "covered_days": len(known),
                         "calendar_days": month.days_in_month, "period_days": len(profile),
                         "period_end": str(profile.service_date.max().date()) if len(profile) else None,
                         "mean_per_day": float(known.departures.mean()) if len(known) else None})
    return pd.DataFrame(rows, columns=["month", "month_start", "provider", "departures", "active_lines",
                                       "covered_days", "calendar_days", "period_days", "period_end", "mean_per_day"])


def monthly_plan_rows(daily, items, month, providers=None, mode="all", through=None):
    """Chosen line/day rows for a month, retaining each source export date."""
    profile = daily_month(daily, items, month, providers, mode, through)
    selected = select_service(daily, providers, mode)
    selected["service_date"] = pd.to_datetime(selected.service_date)
    result = selected.merge(profile[profile.covered][["service_date", "source_export"]],
                            left_on=["service_date", "snapshot_date"],
                            right_on=["service_date", "source_export"], how="inner")
    # The comparison groups by month; source_export keeps the actual observation
    # date for each selected daily plan after replacing this grouping key.
    result["snapshot_date"] = month + "-01"
    return result, profile


def comparison_cutoffs(before, after, through=None):
    """Compare matching day ranges when the later month has not finished yet."""
    cutoffs = {before: through, after: through}
    if through is not None:
        cutoff = pd.Timestamp(through)
        later = pd.Period(after, freq="M")
        if cutoff.to_period("M") == later and cutoff.day < later.days_in_month:
            last_day = min(cutoff.day, pd.Period(before, freq="M").days_in_month)
            cutoffs = {month: pd.Period(month, freq="M").start_time + pd.Timedelta(days=last_day - 1)
                       for month in (before, after)}
    return cutoffs


def compare_months(daily, items, before, after, providers=None, mode="all", line_snapshots=None, through=None):
    """``compare_exports`` for two reconstructed months (``YYYY-MM``) instead of two exports."""
    cutoffs = comparison_cutoffs(before, after, through)
    frames = [monthly_plan_rows(daily, items, m, providers, mode, cutoffs[m])[0] for m in (before, after)]
    attributes = None
    if line_snapshots is not None and not line_snapshots.empty:
        attributes = line_snapshots.sort_values("snapshot_date").copy()
        attributes["snapshot_date"] = attributes.snapshot_date.str[:7] + "-01"
        attributes = attributes.drop_duplicates(["provider", "line_id", "snapshot_date"], keep="last")
    result = compare_exports(pd.concat(frames, ignore_index=True), before + "-01", after + "-01",
                             providers, mode, attributes)
    for label, month in (("earlier", before), ("later", after)):
        end = pd.Period(month, freq="M").end_time.normalize()
        end = min(end, pd.Timestamp(cutoffs[month])) if cutoffs[month] is not None else end
        result[label + "_period_end"] = str(end.date())
    return result


def exclusion_reasons(frame):
    """Label each retained occurrence; unknown flags never become valid/invalid guesses."""
    result = frame.copy()
    result["reason"] = "Not classified"
    if {"is_valid", "is_flexible"}.issubset(result.columns):
        known_type = result.is_flexible.isin([True, False])
        result.loc[result.is_valid.eq(False) & known_type, "reason"] = "Invalid service occurrence"
        result.loc[result.is_valid.eq(True) & result.is_flexible.eq(True), "reason"] = "Valid on-demand service"
    return result
