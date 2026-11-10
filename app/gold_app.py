"""Bus and boat timetable history for Kolumbus (Rogaland) and AKT (Agder).

Start with:  bash scripts/run_gui.sh
"""

import os
from pathlib import Path
import sys

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app import analytics, data  # noqa: E402
from pipeline.processing.windows import window_signature  # noqa: E402

DATA_ROOT = os.environ.get("DAT535_DATA_ROOT", "/srv/dat535/data")
PROVIDERS = {"AKT": "AKT (Agder)", "KOL": "Kolumbus (Rogaland)"}

WHAT_HAPPENED = {
    "renumbered_verified": "Replaced by a new line (confirmed)",
    "discontinued_verified": "Stopped (confirmed)",
    "not_observed": "Gone from the timetable (reason unknown)",
    "replacement_verified": "Replaces an old line (confirmed)",
    "first_observed": "Appeared in this export",
}
CHANGE = {
    "line_appeared": "Appeared in export", "line_disappeared": "Missing from export",
    "public_code_changed": "Line number changed", "name_changed": "Name changed",
    "transport_mode_changed": "Bus/boat changed", "stops_added": "Stops added", "stops_removed": "Stops removed",
    "patterns_changed": "Stop order changed", "service_started": "Trips appear in the window",
    "service_stopped": "No trips in this window", "departures_increased": "More trips",
    "departures_decreased": "Fewer trips", "first_departure_changed": "First trip moved",
    "last_departure_changed": "Last trip moved",
}
WHY_NEW_VERSION = {"first_observed": "First time seen", "structure_changed": "Route or stops changed",
                   "reappeared": "Came back"}
GATE = {"pass": "✅ Passed", "warn": "⚠️ Passed with warnings", "fail": "❌ Failed"}

st.set_page_config(page_title="Timetable history", page_icon="🚌", layout="wide")
st.markdown("""<style>
    [data-testid="stMainBlockContainer"] {padding-top: 2rem; max-width: 1400px;}
    [data-testid="stMetricValue"] {font-size: 1.9rem;}
    [data-testid="stMetricLabel"] {font-size: .9rem;}
    h1, h2, h3 {letter-spacing: -.025em;}
</style>""", unsafe_allow_html=True)


# --------------------------------------------------------------------------
# Loading (read-only, cached)

@st.cache_data(ttl=60, show_spinner="Loading timetables …")
def load(root):
    items = data.snapshots(root)
    daily = data.gold_table(items, "daily_line_service")
    return items, daily


@st.cache_data(ttl=60, show_spinner="Loading route history …")
def load_history(root):
    h = data.History(root)
    tables = {name: h.table(name) for name in ("line_snapshots", "line_transitions", "line_changes",
                                               "line_versions", "stop_coverage_changes", "change_summary")}
    tables["catalog"] = data.line_catalog(tables["line_snapshots"])
    return h.manifest, tables, h.stop_names()


@st.cache_data(ttl=60, show_spinner="Reading exclusion details …")
def load_exclusions(item):
    return analytics.exclusion_reasons(data.gold_table([item], "excluded_service_dates"))


# --------------------------------------------------------------------------
# Small helpers

def day(value):
    return pd.Timestamp(value).strftime("%d %b %Y").lstrip("0") if pd.notna(value) else "–"


MODE = {"All modes": "all", "Bus": "bus", "Boat": "water"}
COLORS = alt.Scale(domain=list(PROVIDERS.values()), range=["#14b8a6", "#60a5fa"])


def stop_list(ids, lookup, limit=None):
    ids = list(ids)
    shown = [lookup.get(i, i) for i in ids[:limit]]
    more = len(ids) - limit if limit and len(ids) > limit else 0
    return ", ".join(shown) + (f" … and {more} more" if more else "")


def line_labels(catalog):
    return dict(zip(catalog.line_id, catalog.label))


def label_list(value, lookup):
    """Comma-separated line IDs -> readable line names."""
    if value is None or (isinstance(value, float) and pd.isna(value)) or value == "":
        return ""
    return "; ".join(lookup.get(part, part) for part in str(value).split(","))


def intro(text):
    st.caption(text)


def table(frame, **kwargs):
    st.dataframe(frame, hide_index=True, use_container_width=True, **kwargs)


def filters(key):
    left, right = st.columns([2, 1])
    provider = left.selectbox("Operator", ["all", *PROVIDERS], key=f"{key}_operator",
                              format_func=lambda p: "Both operators" if p == "all" else PROVIDERS[p])
    mode = right.selectbox("Transport mode", list(MODE), key=f"{key}_mode")
    return None if provider == "all" else [provider], MODE[mode]


def metric(column, label, value, change=None, help=None):
    with column.container(border=True):
        st.metric(label, value, delta=change if change else None, delta_color="off", help=help)


def download(frame, filename, label="Download CSV"):
    st.download_button(label, frame.to_csv(index=False).encode("utf-8-sig"), file_name=filename,
                       mime="text/csv", key=filename)


def history_required(history):
    if history is None:
        st.info("Route history is not available yet. Overview and Compare months use the published departures directly.")
        return False
    return True


# --------------------------------------------------------------------------
# Pages

def page_overview(items, daily):
    st.header("Network overview")
    intro("Daily and monthly planned service, reconstructed from validated primary and calendar-support exports.")
    providers, mode = filters("overview")
    series = analytics.monthly_series(daily, items, providers, mode)
    months = sorted(series.month.unique())
    selected = st.selectbox("Month to inspect", months, index=len(months) - 1,
                            format_func=lambda m: pd.Period(m, freq="M").strftime("%B %Y"), key="overview_month")
    current = series[series.month == selected]
    profile = analytics.daily_month(daily, items, selected, providers, mode)
    covered = profile[profile.covered]
    days = len(covered)
    total = int(covered.departures.sum()) if days else None
    cols = st.columns(4)
    metric(cols[0], "Planned departures", f"{total:,}" if total is not None else "–")
    metric(cols[1], "Lines with service", f"{int(current.active_lines.sum()):,}" if days else "–")
    metric(cols[2], "Departures per calendar day", f"{total / days:,.1f}" if days else "–")
    metric(cols[3], "Dates covered", f"{days} / {len(profile)}")
    if days < len(profile):
        st.info("This month has incomplete published coverage. Counts and averages describe covered dates only.")
    st.caption("Full-month totals use every calendar day, including covered dates with no service. "
               "Months have different lengths; use the daily mean and Compare exports weekday profiles when comparing them.")
    st.subheader("Planned service by month")
    chart_data = series.assign(company=series.provider.map(PROVIDERS))
    st.altair_chart(alt.Chart(chart_data).mark_line(point=True, strokeWidth=2.5).encode(
        x=alt.X("month_start:T", title="Service month", axis=alt.Axis(format="%b %Y")),
        y=alt.Y("departures:Q", title="Departures on covered dates", axis=alt.Axis(format=",")),
        color=alt.Color("company:N", title=None, scale=COLORS, legend=alt.Legend(orient="bottom")),
        tooltip=["month:N", "company:N", alt.Tooltip("departures:Q", title="Departures", format=","),
                 "covered_days:Q", "calendar_days:Q", "mean_per_day:Q", "active_lines:Q"])
        .properties(height=310), use_container_width=True)
    st.caption("This combines dated published plans, not actual operations or a claim of one observation on every day. "
               "Daily service shows which export supplied each date.")
    with st.expander("Inspect and download the numbers"):
        table(series)
        download(series, "monthly_service.csv")


def page_compare(items, daily, history):
    st.header("Compare months")
    intro("Compare reconstructed monthly plans, including valid calendar-support sources for early dates.")
    providers, mode = filters("compare")
    months = sorted({i["service_from"][:7] for i in items})
    if len(months) < 2:
        st.info("A comparison needs at least two published months.")
        return
    left, right = st.columns(2)
    describe = lambda m: pd.Period(m, freq="M").strftime("%B %Y")
    before = left.selectbox("Earlier month", months[:-1], format_func=describe, key="compare_before")
    later = [m for m in months if m > before]
    after = right.selectbox("Later month", later, index=len(later) - 1, format_func=describe, key="compare_after")
    profiles = {m: analytics.daily_month(daily, items, m, providers, mode) for m in (before, after)}
    counts = {m: int(frame.covered.sum()) for m, frame in profiles.items()}
    for month, frame in profiles.items():
        st.caption(f"{describe(month)}: {counts[month]} / {len(frame)} dates covered")
    if not all(counts.values()):
        st.info("Both months need published date coverage before they can be compared.")
        return
    if any(counts[m] < len(profiles[m]) for m in profiles):
        st.warning("Coverage is incomplete. These totals and averages describe covered dates only.")
    line_snapshots = history[1]["line_snapshots"] if history else None
    result = analytics.compare_months(daily, items, before, after, providers, mode, line_snapshots)
    first, last = int(result.before.sum()), int(result.after.sum())
    pct = analytics.percentage_change(first, last)
    cols = st.columns(4)
    metric(cols[0], "Earlier departures", f"{first:,}")
    metric(cols[1], "Later departures", f"{last:,}", last - first)
    metric(cols[2], "Change", f"{pct:+.1f}%" if pct is not None else "Not defined",
           help="Percentage change is undefined when the earlier total is zero.")
    metric(cols[3], "Lines with changed counts", f"{int((result.change != 0).sum()):,}")
    st.caption(f"Mean departures per covered calendar day: {first / counts[before]:,.1f} → {last / counts[after]:,.1f}. "
               "Months have different lengths and weekday mixes.")
    weekdays = pd.concat([frame[frame.covered].groupby("weekday").departures.mean().reindex(analytics.WEEKDAYS)
                          .rename("departures").reset_index().assign(month=describe(m))
                          for m, frame in profiles.items()], ignore_index=True)
    st.subheader("Weekday service profile")
    st.altair_chart(alt.Chart(weekdays).mark_bar().encode(
        x=alt.X("weekday:N", sort=analytics.WEEKDAYS, title=None), xOffset="month:N",
        y=alt.Y("departures:Q", title="Mean departures per covered calendar day"),
        color=alt.Color("month:N", title="Month", scale=alt.Scale(range=["#94a3b8", "#14b8a6"])),
        tooltip=["weekday:N", "month:N", alt.Tooltip("departures:Q", format=",.1f")]).properties(height=240), use_container_width=True)
    st.caption("Covered dates with no service count as zero. Uncovered dates are excluded from the mean.")
    st.subheader("Line-by-line changes")
    view = st.radio("Show lines", ["All", "Largest changes", "New service in window", "No service in later window"],
                    horizontal=True, key="compare_lines")
    shown = result
    if view == "Largest changes":
        shown = result.loc[result.change.abs().sort_values(ascending=False).head(20).index]
    elif view != "All":
        shown = result[result.service_status == view]
    if shown.empty:
        st.info("No lines match this view for the selected operator, mode and months.")
    else:
        table(shown[["provider", "number", "route", "before", "after", "change", "change_pct", "service_status"]]
              .rename(columns={"before": "Earlier", "after": "Later", "change": "Difference", "change_pct": "Change (%)"}))
    download(result.rename(columns={"earlier_export": "earlier_month_start", "later_export": "later_month_start"}),
             f"comparison_{before}_{after}_{mode}.csv", "Download full comparison")
    st.caption("Lines are matched by operator and NeTEx ID. Gaining or losing service in a month does not prove opening or closure. "
               "Daily service shows source dates; verified replacements are under Replaced lines.")


def page_daily(items, daily):
    st.header("Daily service")
    intro("Explore planned departures by operating date within a month. These are timetable plans, not actual vehicle movements.")
    providers, mode = filters("daily")
    months = pd.period_range(min(i["service_from"] for i in items), max(i["service_to"] for i in items), freq="M")
    month = st.selectbox("Month", [str(m) for m in months], index=len(months) - 1,
                         format_func=lambda m: pd.Period(m, freq="M").strftime("%B %Y"), key="daily_month")
    profile = analytics.daily_month(daily, items, month, providers, mode)
    known = profile[profile.covered]
    count = len(known)
    cols = st.columns(3)
    metric(cols[0], "Dates with published coverage", f"{count} / {len(profile)}")
    metric(cols[1], "Departures on covered dates", f"{int(known.departures.sum()):,}" if count else "–")
    metric(cols[2], "Mean per covered date", f"{known.departures.mean():,.1f}" if count else "–")
    if count < len(profile):
        st.info(f"{len(profile) - count} dates have no published coverage. Blank dates are unknown, not zero service. "
                "Full-month totals are unavailable until the remaining dates are covered.")
    st.caption("Each date uses the latest export whose calendar validity covers it, including dates before the export was downloaded. "
               "This reconstructs published plans for the month, not what was known on each day. "
               "The source export is shown for every date; missing lines are not filled from older plans.")
    st.subheader("Departures each day")
    chart_data = profile.assign(date_label=profile.service_date.map(day),
                                operating_date=profile.service_date.dt.strftime("%Y-%m-%d"))
    tooltip = [alt.Tooltip("date_label:N", title="Date"), "weekday:N", "coverage:N",
               alt.Tooltip("departures:Q", title="Departures"),
               alt.Tooltip("active_lines:Q", title="Lines with service"),
               alt.Tooltip("source_export:N", title="Source export")]
    base = alt.Chart(chart_data).encode(
        x=alt.X("operating_date:O", sort=None, title="Operating date",
                axis=alt.Axis(labelExpr="timeFormat(toDate(datum.label), '%d %b')", labelAngle=-45)), tooltip=tooltip)
    gaps = base.transform_filter("!datum.covered").mark_rect(color="#94a3b8", opacity=.12)
    bars = base.mark_bar(color="#14b8a6").encode(y=alt.Y("departures:Q", title="Planned departures"))
    zero = base.transform_filter("datum.covered && datum.departures === 0").mark_tick(
        color="#14b8a6", thickness=2).encode(y=alt.datum(0))
    st.altair_chart((gaps + bars + zero).properties(height=280), use_container_width=True)
    st.caption("Grey bands have no published coverage. Teal ticks mark covered dates with zero departures. "
               "Uncovered dates have no count.")
    shown = profile.assign(**{"first departure": profile["first"].map(data.clock),
                              "last departure": profile["last"].map(data.clock)})
    shown = shown.drop(columns=["covered", "first", "last"])
    table(shown, column_config={"service_date": st.column_config.DateColumn("Operating date", format="DD MMM YYYY"),
                               "departures": "Planned departures", "active_lines": "Lines with service",
                               "coverage": "Coverage", "source_export": "Source export"})
    download(shown, f"daily_service_{month}_{mode}.csv")


def page_line(items, daily, history):
    st.header("Line explorer")
    intro("Follow one published line: its planned service, observed versions, changes, and stops.")
    _, tables, stop_names = history
    catalog = tables["catalog"]
    names = line_labels(catalog)

    c1, c2, c3 = st.columns([1, 1, 3])
    provider = c1.selectbox("Operator", list(PROVIDERS), format_func=PROVIDERS.get)
    lines = catalog[catalog.provider == provider]
    mode = c2.selectbox("Transport mode", list(MODE), key="line_mode")
    if MODE[mode] != "all":
        lines = lines[lines["mode"] == ("boat" if MODE[mode] == "water" else MODE[mode])]
    options = list(lines.line_id)
    if not options:
        st.info("No published lines match this operator and mode.")
        return
    # Start on the current bus 55 in Kristiansand (a confirmed change) when it exists.
    latest = catalog.last_observed.max()
    start = next((i for i, r in enumerate(lines.itertuples())
                  if r.number == "55" and r.last_observed == latest), 0)
    line_id = c3.selectbox("Line — search by number or place", options, index=start, format_func=names.get)
    info = lines[lines.line_id == line_id].iloc[0]
    st.caption(f"{info['mode'].capitalize()} · in the timetables from {data.month(info.first_observed)} "
               f"to {data.month(info.last_observed)}")

    snaps = tables["line_snapshots"]
    mine = snaps[(snaps.line_id == line_id) & (snaps.provider == provider)].sort_values("snapshot_date")
    chosen = st.selectbox("Export to inspect", sorted(mine.snapshot_date.unique(), reverse=True), format_func=day)
    observed = mine[mine.snapshot_date == chosen].iloc[0]
    day_rows = daily[(daily.line_entity_id == line_id) & (daily.provider == provider)]
    month = chosen[:7]
    monthly_rows, coverage = analytics.monthly_plan_rows(day_rows, items, month, [provider])
    cols = st.columns(3)
    metric(cols[0], "Planned departures in month", f"{int(monthly_rows.scheduled_departures.sum()):,}")
    metric(cols[1], "Stops served by selected export", f"{int(observed.window_stop_count):,}")
    metric(cols[2], "Stops in route definitions", f"{int(observed.stop_count):,}")
    st.caption("Monthly departures include calendar-support plans. Stop and version details describe the selected primary export.")
    service, versions, changes, stops = st.tabs(["Service", "Observed versions", "Changes", "Stops"])
    with service:
        trend = analytics.monthly_series(day_rows, items, [provider])
        st.altair_chart(alt.Chart(trend).mark_line(point=True, color="#14b8a6").encode(
            x=alt.X("month_start:T", title="Service month", axis=alt.Axis(format="%b %Y")),
            y=alt.Y("departures:Q", title="Departures on covered dates"),
            tooltip=["month:N", "departures:Q", "covered_days:Q", "calendar_days:Q"])
            .properties(height=250), use_container_width=True)
        if not coverage.empty:
            period = pd.Period(month, freq="M")
            window = {"snapshot_date": month + "-01", "service_from": str(period.start_time.date()),
                      "service_to": str(period.end_time.date())}
            per_day = analytics.daily_profile(monthly_rows, window)
            per_day["departures"] = per_day.departures.where(coverage.covered).astype("Int64")
            per_day["source_export"] = coverage.source_export
            per_day["first departure"] = per_day["first"].map(data.clock)
            per_day["last departure"] = per_day["last"].map(data.clock)
            st.subheader("Each day in the selected month")
            st.altair_chart(alt.Chart(per_day).mark_bar(color="#14b8a6", cornerRadiusTopLeft=3, cornerRadiusTopRight=3).encode(
                x=alt.X("service_date:T", title=None, axis=alt.Axis(format="%a %d %b")),
                y=alt.Y("departures:Q", title="Departures"), tooltip=["weekday:N", "departures:Q", "first departure:N", "last departure:N"])
                .properties(height=200), use_container_width=True)
            download(per_day.drop(columns=["first", "last"]), f"line_{provider}_{chosen}_{info.number}.csv")
    with versions:
        st.caption("First/last seen are observation dates in published exports. Operating dates may differ.")
        v = tables["line_versions"]
        v = v[v.line_id == line_id].sort_values("version_number")
        table(pd.DataFrame({"Version": v.version_number, "Number": v.public_code, "Route": v["name"],
                            "First seen": v.first_observed.map(day), "Last seen": v.last_observed.map(day),
                            "Why": v.version_reason.map(WHY_NEW_VERSION), "Stops": v.stop_count,
                            "Replaces": v.renumbered_from_line_id.map(lambda x: label_list(x, names))}))
    with changes:
        ch = tables["line_changes"]
        ch = ch[ch.line_id == line_id].sort_values(["from_snapshot", "change_type"])
        def detail(row):
            if row.change_type in ("stops_added", "stops_removed", "line_appeared", "line_disappeared"):
                return stop_list(row["items"] if row["items"] is not None else [], stop_names, 8)
            if row.change_type.endswith("departure_changed"):
                old = data.clock(float(row.old_value)) if row.old_value else "–"
                new = data.clock(float(row.new_value)) if row.new_value else "–"
                return f"{old} → {new}"
            return f"{row.old_value or '–'} → {row.new_value or '–'}"
        if len(ch):
            table(pd.DataFrame({"Between": ch.from_snapshot.map(day) + " → " + ch.to_snapshot.map(day),
                                "What changed": ch.change_type.map(CHANGE), "Details": [detail(r) for _, r in ch.iterrows()]}))
        else:
            st.info("No changes found for this line.")
    with stops:
        st.caption(f"Export observed {day(chosen)}. Stops served in the window and stops in route definitions are different measures.")
        served, defined = st.columns(2)
        with served:
            st.subheader("Served in the window")
            st.write(stop_list(observed.window_stop_ids, stop_names) or "No fixed service in this window")
        with defined:
            st.subheader("In route definitions")
            st.write(stop_list(observed.stop_ids, stop_names) or "None")


def page_changes(history):
    st.header("What changed")
    intro("Compare two exports: which lines appeared or are no longer published, "
          "and which have more or fewer planned trips in their declared service windows.")
    st.caption("These service counts belong to the primary exports and their available validity. "
               "Use Compare months for complete monthly totals that include calendar-support plans.")
    _, tables, _ = history
    names = line_labels(tables["catalog"])
    renumberings, discontinuations = data.evidence()
    summary = tables["change_summary"]

    steps = summary[["from_snapshot", "to_snapshot"]].drop_duplicates().sort_values("from_snapshot")
    pairs = [(r.from_snapshot, r.to_snapshot) for r in steps.itertuples()]
    summer = ("2026-06-15", "2026-07-15")
    c1, c2 = st.columns([2, 1])
    start, end = c1.selectbox("Compare", pairs, index=pairs.index(summer) if summer in pairs else len(pairs) - 1,
                              format_func=lambda p: f"{day(p[0])}  →  {day(p[1])}")
    provider = c2.selectbox("Operator", list(PROVIDERS), format_func=PROVIDERS.get)
    row = summary[(summary.from_snapshot == start) & (summary.provider == provider)].iloc[0]

    cols = st.columns(4)
    cols[0].metric("Lines", int(row.lines_after), int(row.lines_after - row.lines_before))
    cols[1].metric("Lines appeared / disappeared", f"+{row.lines_appeared} / −{row.lines_disappeared}")
    cols[2].metric("Trips in service window", f"{row.window_departures_after:,}",
                   int(row.window_departures_after - row.window_departures_before))
    cols[3].metric("Stops gained / lost", f"+{row.stops_gained_window_service} / −{row.stops_lost_window_service}",
                   help="Stops with planned trips in only one of the two selected service windows.")

    t = tables["line_transitions"]
    t = t[(t.from_snapshot == start) & (t.provider == provider)]

    gone = t[t.status == "disappeared"]
    if len(gone):
        st.subheader("Lines no longer published")
        confirmed = gone.interpretation.isin(["renumbered_verified", "discontinued_verified"]).sum()
        st.caption(f"{confirmed} of {len(gone)} confirmed by the operator. Click a link to read the notice.")
        replaced = renumberings[renumberings.to_snapshot == end].groupby("old_line_id").new_line_id \
            .agg(lambda ids: "; ".join(names.get(i, i) for i in ids)) if len(renumberings) else pd.Series(dtype=str)
        notices = [f for f in (renumberings.rename(columns={"old_line_id": "line_id"}), discontinuations) if len(f)]
        notices = pd.concat(notices) if notices else pd.DataFrame(columns=["line_id", "to_snapshot", "source_url"])
        notices = notices[notices.to_snapshot == end].groupby("line_id").source_url.first()
        table(pd.DataFrame({
            "Number": gone.public_code_before, "Route": gone.name_before,
            "Trips in earlier window": gone.departures_before.astype("Int64"),
            "What happened": gone.interpretation.map(WHAT_HAPPENED),
            "Replaced by": gone.line_id.map(replaced).fillna(""), "Notice": gone.line_id.map(notices)}),
            column_config={"Notice": st.column_config.LinkColumn("Notice", display_text="read")})

    new = t[t.status == "appeared"]
    if len(new):
        st.subheader("Lines newly present in the export")
        st.caption("A line can be published before its planned service starts, so it can show 0 trips in this window.")
        table(pd.DataFrame({
            "Number": new.public_code_after, "Route": new.name_after,
            "Trips in later window": new.departures_after.astype("Int64"),
            "What it is": new.interpretation.map(WHAT_HAPPENED)}))

    both = t[(t.departures_before > 0) & (t.departures_after > 0)].copy()
    if len(both):
        st.subheader("Biggest changes in trips")
        st.caption("Lines with planned trips in both selected windows. Red = fewer trips, blue = more trips.")
        both["change"] = both.departure_change
        both["line"] = (both.public_code_after + " " + both.name_after.fillna("")).str.strip()
        top = pd.concat([both.nsmallest(8, "change"), both.nlargest(8, "change")]).drop_duplicates("line_id")
        top = top[top.change != 0].sort_values("change")
        st.altair_chart(alt.Chart(top).mark_bar().encode(
            x=alt.X("change:Q", title="Change in trips per service window"), y=alt.Y("line:N", sort=None, title=None),
            color=alt.condition("datum.change < 0", alt.value("#c0392b"), alt.value("#2e86c1")),
            tooltip=[alt.Tooltip("line:N", title="Line"), alt.Tooltip("departures_before:Q", title="Before"),
                     alt.Tooltip("departures_after:Q", title="After")]), use_container_width=True)

    stops = tables["stop_coverage_changes"]
    lost = stops[(stops.from_snapshot == start) & (stops.provider == provider) & (stops.measure == "window_service")
                 & (stops.change_type == "stop_lost")]
    with st.expander(f"Stops with no planned trips in the later window ({len(lost)})"):
        table(pd.DataFrame({"Stop": lost.stop_name,
                            "Lines serving the earlier window": lost.line_codes_before.map(lambda c: ", ".join(c))}))


def page_replaced(history):
    st.header("Replaced lines")
    intro("Lines the operator replaced with a new line or number. "
          "Pick one to compare the old and new line.")
    st.caption("Departure figures describe each primary export's available service window. "
               "Monthly reconstruction is shown in Compare months and One line.")
    _, tables, stop_names = history
    names = line_labels(tables["catalog"])
    renumberings, discontinuations = data.evidence()
    if not len(renumberings):
        st.write("No confirmed replacements recorded.")
        return

    short = lambda line_id: names.get(line_id, line_id).split("  (")[0]
    options = list(range(len(renumberings)))
    describe = lambda i: (f"{PROVIDERS[renumberings.provider.iloc[i]].split(' ')[0]}: "
                          f"{short(renumberings.old_line_id.iloc[i])}  →  {short(renumberings.new_line_id.iloc[i])}"
                          f"   ({data.month(renumberings.to_snapshot.iloc[i])})")
    start = next((i for i in options if renumberings.old_line_id.iloc[i] == "AKT:Line:1055"), 0)
    pick = renumberings.iloc[st.selectbox("Replacement", options, index=start, format_func=describe)]
    st.markdown(f"{pick.note}  \n[Read the operator's notice]({pick.source_url})")

    snaps = tables["line_snapshots"]
    pair = snaps[snaps.line_id.isin([pick.old_line_id, pick.new_line_id])].copy()
    pair["line"] = pair.line_id.map({pick.old_line_id: f"Old: {short(pick.old_line_id)}",
                                     pick.new_line_id: f"New: {short(pick.new_line_id)}"})
    st.altair_chart(alt.Chart(pair.sort_values("snapshot_date")).mark_line(point=True).encode(
        x=alt.X("snapshot_date:T", title="Export date", axis=alt.Axis(format="%d %b %Y")),
        y=alt.Y("window_departures:Q", title="Planned trips in service window"),
        color=alt.Color("line:N", title=None, legend=alt.Legend(orient="bottom", labelLimit=400)),
        tooltip=[alt.Tooltip("snapshot_date:T", title="Export", format="%d %b %Y"), "line:N",
                 alt.Tooltip("window_departures:Q", title="Trips")]).properties(height=280),
        use_container_width=True)

    before = snaps[(snaps.line_id == pick.old_line_id) & (snaps.snapshot_date < pick.to_snapshot)]
    after = snaps[(snaps.line_id == pick.new_line_id) & (snaps.snapshot_date >= pick.to_snapshot)]
    if len(before) and len(after):
        old, new = before.sort_values("snapshot_date").iloc[-1], after.sort_values("snapshot_date").iloc[0]
        old_stops, new_stops = set(old.stop_ids), set(new.stop_ids)
        c1, c2 = st.columns(2)
        c1.metric(f"Old: {old.public_code} {old['name']}", f"{old.window_departures} trips/window")
        c2.metric(f"New: {new.public_code} {new['name']}", f"{new.window_departures} trips/window",
                  int(new.window_departures - old.window_departures))
        st.markdown(f"**Stops on both lines:** {len(old_stops & new_stops)}")
        st.markdown(f"**Stops only in the old route definition ({len(old_stops - new_stops)}):** "
                    f"{stop_list(sorted(old_stops - new_stops), stop_names) or 'none'}")
        st.markdown(f"**Stops only in the new route definition ({len(new_stops - old_stops)}):** "
                    f"{stop_list(sorted(new_stops - old_stops), stop_names) or 'none'}")

    if len(discontinuations):
        with st.expander("Confirmed discontinued lines"):
            table(pd.DataFrame({
                "Operator": discontinuations.provider.map(PROVIDERS), "Line": discontinuations.line_id.map(short),
                "Last seen": discontinuations.from_snapshot.map(day), "Note": discontinuations.note,
                "Notice": discontinuations.source_url}),
                column_config={"Notice": st.column_config.LinkColumn("Notice", display_text="read")})


def page_checks(items, history):
    st.header("Data checks")
    intro("The gate checks each export before publication. Daily service separately reports calendar-date coverage. "
          "Older results may predate the quality gate.")
    gates = [(i["gold_manifest"].get("quality_gate") or {}).get("status", "not recorded") for i in items]
    cols = st.columns(3)
    metric(cols[0], "Published exports", len(items))
    metric(cols[1], "Passed quality gate", gates.count("pass"))
    metric(cols[2], "Warnings / older unchecked", gates.count("warn") + gates.count("not recorded"))
    rows = []
    for item in items:
        m = item["gold_manifest"]
        recon = m.get("reconciliation", {})
        gate = m.get("quality_gate") or {}
        rows.append({"Source export": day(item["snapshot_date"]),
                     "Service window": f"{day(item['service_from'])} – {day(item['service_to'])}",
                     "Role": "Calendar support" if m.get("history_role") == "calendar_support" else "Primary observation",
                     "Result": GATE.get(gate.get("status"), "Not checked (older run)"),
                     "Fixed departures counted": recon.get("gold_scheduled_departures"),
                     "Outside fixed-service metrics": recon.get("excluded_service_occurrences"),
                     "Totals add up": {True: "✅", False: "❌"}.get(recon.get("counts_match"), "Not recorded")})
    table(pd.DataFrame(rows))
    download(pd.DataFrame(rows), "published_quality.csv")
    st.caption("Outside fixed-service metrics includes valid on-demand services and invalid service occurrences. "
               "An occurrence means one journey on one operating date, not a whole line or an XML file. "
               "These records remain in excluded_service_dates.")

    st.subheader("Why service was excluded")
    dates = [i["snapshot_date"] for i in items]
    selected = st.selectbox("Export details", dates, index=len(dates) - 1, format_func=day, key="checks_export")
    item = next(i for i in items if i["snapshot_date"] == selected)
    if "excluded_service_dates" not in item["gold_manifest"].get("tables", {}):
        st.info("The reason breakdown was not recorded in this older output.")
    else:
        try:
            excluded = load_exclusions(item)
        except Exception as error:
            st.warning(f"Could not read the retained exclusion details: {error}")
        else:
            expected = item["gold_manifest"].get("reconciliation", {}).get("excluded_service_occurrences")
            if expected is not None and len(excluded) != expected:
                st.warning("The retained exclusion rows do not match the published total. Check this output before using its breakdown.")
            counts = excluded.reason.value_counts()
            cols = st.columns(3)
            metric(cols[0], "Valid on-demand occurrences", int(counts.get("Valid on-demand service", 0)))
            metric(cols[1], "Invalid occurrences", int(counts.get("Invalid service occurrence", 0)))
            metric(cols[2], "Unclassified occurrences", int(counts.get("Not classified", 0)))
            if counts.get("Not classified", 0):
                st.warning("Some saved occurrences lack a complete classification. This breakdown is incomplete; "
                           "their reasons need verification from a regenerated output.")
            st.caption("On-demand service can describe an availability window, such as 10:00–21:00. "
                       "It is valid service, but cannot be counted as a fixed departure. "
                       "Invalid occurrences are withheld by the validation rules.")
            if len(excluded):
                columns = [c for c in ("provider", "service_date", "line_public_code", "line_entity_id", "reason")
                           if c in excluded.columns]
                table(excluded[columns])
                download(excluded, f"excluded_occurrences_{selected}.csv")
            else:
                st.success("No service occurrences were excluded from this export.")

    manifest = history[0] if history else {}
    with st.expander("Technical details"):
        checks = {k: v for k, v in manifest.get("checks", {}).items() if k != "snapshots"}
        table(pd.DataFrame({"Check": [k.replace("_", " ").capitalize() for k in checks],
                            "Value": [str(v) for v in checks.values()]}))
        report = data.REPO / "docs/quality_report.md"
        if report.is_file():
            st.markdown(report.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------

PAGES = {"Overview": page_overview, "Daily service": page_daily, "Compare months": page_compare, "One line": page_line, "What changed": page_changes,
         "Replaced lines": page_replaced, "Data checks": page_checks}


def main():
    st.sidebar.title("🚌 Timetable history")
    st.sidebar.caption("Kolumbus · Rogaland\n\nAKT · Agder")
    page = st.sidebar.radio("Go to", list(PAGES), label_visibility="collapsed")
    with st.sidebar.expander("How to read this"):
        st.markdown(
            "- **Timetable**: an export of published plans. Historical exports are sampled on the 15th, "
            "January–September 2026; additional calendar-support exports fill early dates. Later exports come from scheduled ingestion.\n"
            "- **Trips**: planned departures in the full calendar month. These are plans, "
            "not buses that actually ran.\n"
            "- **Confirmed**: the operator announced the change on its website.\n"
            "- A difference between exports shows a change in the published plans. "
            "When it took effect for passengers needs confirmation from an operator source.")
    if st.sidebar.button("Refresh data", use_container_width=True):
        st.cache_data.clear()

    try:
        items, daily = load(DATA_ROOT)
    except Exception as error:  # a readable message instead of a traceback
        st.error(f"Could not load the data from {DATA_ROOT}. Has the pipeline been run?\n\n"
                 f"{type(error).__name__}: {error}")
        st.stop()

    if not items:
        st.info("No published exports are available yet.")
        st.stop()
    primary = [i for i in items if i["gold_manifest"].get("history_role") != "calendar_support"]
    st.sidebar.divider()
    st.sidebar.caption(f"{len(items)} published exports\n\n{day(items[0]['snapshot_date'])} – {day(items[-1]['snapshot_date'])}")
    history = None
    try:
        history = load_history(DATA_ROOT)
    except (Exception, SystemExit) as error:
        st.sidebar.caption("Route history unavailable")
        with st.sidebar.expander("History status"):
            st.write(str(error))
    if history:
        current = {i["snapshot_date"]: i for i in primary}
        if any(i["snapshot_date"] in current and
               (window_signature(i) != window_signature(current[i["snapshot_date"]]) or
                (i.get("gold_manifest_sha256") and current[i["snapshot_date"]].get("gold_manifest_sha256") and
                 i["gold_manifest_sha256"] != current[i["snapshot_date"]]["gold_manifest_sha256"]))
               for i in history[0].get("snapshots", [])):
            st.warning("Route history uses older Gold outputs or service windows. Rebuild history before comparing its results with the current monthly outputs.")
            history = None
    if history:
        last_history = history[1]["line_snapshots"].snapshot_date.max()
        st.sidebar.caption(f"Route history through {day(last_history)}")
        if last_history < items[-1]["snapshot_date"]:
            st.info(f"Departures are available through {day(items[-1]['snapshot_date'])}. "
                    f"Route history currently ends at {day(last_history)}.")

    if page == "Overview":
        page_overview(items, daily)
    elif page == "Daily service":
        page_daily(items, daily)
    elif page == "Compare months":
        page_compare(items, daily, history)
    elif page == "One line":
        if history_required(history):
            page_line(items, daily, history)
    elif page == "Data checks":
        page_checks(items, history)
    else:
        if history_required(history):
            PAGES[page](history)


main()
