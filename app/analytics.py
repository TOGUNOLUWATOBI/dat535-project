"""Read-only analysis of the published Gold tables; no Spark session is needed."""

import pandas as pd

WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def select_service(daily, providers=None, mode="all"):
    selected = daily
    if providers is not None:
        selected = selected[selected.provider.isin(providers)]
    if mode != "all":
        selected = selected[selected.line_transport_mode == mode]
    return selected.copy()


def percentage_change(before, after):
    """A zero baseline has no defined percentage change."""
    return (after - before) / before * 100 if before else None


def network_series(daily, items, providers=None, mode="all"):
    """Counts for every selected provider/export, including a mode with zero service."""
    selected = select_service(daily, providers, mode)
    operators = sorted(providers if providers is not None else
                       {p for item in items for p in item["gold_manifest"]["providers"]})
    dates = [item["snapshot_date"] for item in items]
    index = pd.MultiIndex.from_product([dates, operators], names=["snapshot_date", "provider"])
    grouped = selected.groupby(["snapshot_date", "provider"]).agg(
        departures=("scheduled_departures", "sum"), active_lines=("line_entity_id", "nunique"))
    result = grouped.reindex(index, fill_value=0).reset_index()
    windows = {item["snapshot_date"]: f"{item['service_from']} – {item['service_to']}" for item in items}
    result["window"] = result.snapshot_date.map(windows)
    return result


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


def daily_profile(selected, item):
    """One row per calendar day in a published window; days without service stay visible."""
    dates = pd.date_range(item["service_from"], item["service_to"])
    frame = selected[selected.snapshot_date == item["snapshot_date"]].copy()
    frame["service_date"] = pd.to_datetime(frame.service_date)
    if not frame.empty and not frame.service_date.isin(dates).all():
        raise ValueError("Published service dates fall outside the declared window")
    result = frame.groupby("service_date").agg(
        departures=("scheduled_departures", "sum"), first=("first_departure_seconds", "min"),
        last=("last_departure_seconds", "max")).reindex(dates)
    result["departures"] = result.departures.fillna(0).astype("int64")
    result.index.name = "service_date"
    result = result.reset_index()
    result["weekday"] = result.service_date.dt.day_name()
    return result


def weekday_comparison(daily, items, before, after, providers=None, mode="all"):
    """Compare weekdays using mean departures per calendar day, including zero-service days."""
    selected = select_service(daily, providers, mode)
    windows = {item["snapshot_date"]: item for item in items}
    frames = []
    for snapshot in (before, after):
        profile = daily_profile(selected, windows[snapshot])
        grouped = profile.groupby("weekday").departures.mean().reindex(WEEKDAYS)
        frames.append(grouped.rename("departures").reset_index().assign(snapshot_date=snapshot))
    return pd.concat(frames, ignore_index=True)
