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
from app import data  # noqa: E402

DATA_ROOT = os.environ.get("DAT535_DATA_ROOT", "/srv/dat535/data")
PROVIDERS = {"AKT": "AKT (Agder)", "KOL": "Kolumbus (Rogaland)"}

WHAT_HAPPENED = {
    "renumbered_verified": "Replaced by a new line (confirmed)",
    "discontinued_verified": "Stopped (confirmed)",
    "not_observed": "Gone from the timetable (reason unknown)",
    "replacement_verified": "Replaces an old line (confirmed)",
    "first_observed": "New line",
}
CHANGE = {
    "line_appeared": "Line added", "line_disappeared": "Line removed",
    "public_code_changed": "Line number changed", "name_changed": "Name changed",
    "transport_mode_changed": "Bus/boat changed", "stops_added": "Stops added", "stops_removed": "Stops removed",
    "patterns_changed": "Stop order changed", "service_started": "Trips started",
    "service_stopped": "No trips this week", "departures_increased": "More trips",
    "departures_decreased": "Fewer trips", "first_departure_changed": "First trip moved",
    "last_departure_changed": "Last trip moved",
}
WHY_NEW_VERSION = {"first_observed": "First time seen", "structure_changed": "Route or stops changed",
                   "reappeared": "Came back"}
GATE = {"pass": "✅ Passed", "warn": "⚠️ Passed with warnings", "fail": "❌ Failed"}

st.set_page_config(page_title="Timetable history", page_icon="🚌", layout="wide")


# --------------------------------------------------------------------------
# Loading (read-only, cached)

@st.cache_data(show_spinner="Loading timetables …")
def load(root):
    items = data.snapshots(root)
    daily = data.gold_table(items, "daily_line_service")
    return items, data.overview(items), daily


@st.cache_data(show_spinner="Loading route history …")
def load_history(root):
    h = data.History(root)
    tables = {name: h.table(name) for name in ("line_snapshots", "line_transitions", "line_changes",
                                               "line_versions", "stop_coverage_changes", "change_summary")}
    tables["catalog"] = data.line_catalog(tables["line_snapshots"])
    return h.manifest, tables, h.stop_names()


# --------------------------------------------------------------------------
# Small helpers

day = lambda d: pd.Timestamp(d).strftime("%-d %b %Y")  # '2026-07-15' -> '15 Jul 2026'
short_day = lambda d: pd.Timestamp(d).strftime("%-d %b")  # chart axis labels
DATE_AXIS = alt.Axis(labelAngle=0)


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
    st.info(text, icon="💡")


def table(frame, **kwargs):
    st.dataframe(frame, hide_index=True, use_container_width=True, **kwargs)


# --------------------------------------------------------------------------
# Pages

def page_overview(items, overview):
    st.header("Overview")
    intro("How many trips the operators planned, month by month. Each point is one monthly timetable, "
          "counting the planned trips in the week after it was published.")

    latest = overview[overview.snapshot == overview.snapshot.max()]
    cols = st.columns(len(latest) + 1)
    cols[0].metric("Timetables loaded", len(items))
    for col, row in zip(cols[1:], latest.itertuples()):
        previous = overview[(overview.provider == row.provider) & (overview.snapshot < row.snapshot)]
        delta = int(row.departures - previous.iloc[-1].departures) if len(previous) else None
        col.metric(f"{PROVIDERS[row.provider]}: trips per week", f"{row.departures:,}", delta,
                   help=f"Week of {day(row.snapshot)}. The arrow shows the change from the previous timetable.")

    choice = st.radio("Show", ["All", "Bus only", "Boat only"], horizontal=True)
    measure = {"All": "departures", "Bus only": "bus", "Boat only": "water"}[choice]
    chart_data = overview.assign(company=overview.provider.map(PROVIDERS), date=overview.snapshot.map(short_day))
    st.altair_chart(alt.Chart(chart_data).mark_line(point=True).encode(
        x=alt.X("date:N", sort=None, title="Timetable date (2026)", axis=DATE_AXIS),
        y=alt.Y(f"{measure}:Q", title="Planned trips per week"),
        color=alt.Color("company:N", title=None, legend=alt.Legend(orient="bottom")),
        tooltip=[alt.Tooltip("company:N", title="Operator"), alt.Tooltip("window:N", title="Week"),
                 alt.Tooltip(f"{measure}:Q", title="Trips", format=",")]).properties(height=380, padding={"left": 45}),
        use_container_width=True)
    st.caption("Why the dips? February is the winter school holiday in Agder (AKT only). "
               "July is the summer timetable in both regions.")

    with st.expander("See the numbers"):
        table(pd.DataFrame({
            "Timetable date": overview.snapshot.map(day), "Week counted": overview.window,
            "Operator": overview.provider.map(PROVIDERS), "Lines with trips": overview["lines with service"],
            "All trips": overview.departures, "Bus trips": overview.bus, "Boat trips": overview.water}))


def page_line(daily, history):
    st.header("One line")
    intro("Pick a bus or boat line to see how many trips it had each month, "
          "and what changed in its route.")
    _, tables, stop_names = history
    catalog = tables["catalog"]
    names = line_labels(catalog)

    c1, c2 = st.columns([1, 3])
    provider = c1.selectbox("Operator", list(PROVIDERS), format_func=PROVIDERS.get)
    lines = catalog[catalog.provider == provider]
    options = list(lines.line_id)
    # Start on the current bus 55 in Kristiansand (a confirmed change) when it exists.
    start = next((i for i, r in enumerate(lines.itertuples()) if r.number == "55" and not r.period.startswith("Jan")), 0)
    line_id = c2.selectbox("Line (type a number or place to search)", options, index=start, format_func=names.get)
    info = lines[lines.line_id == line_id].iloc[0]
    st.caption(f"{info['mode'].capitalize()} · in the timetables from {data.month(info.first_observed)} "
               f"to {data.month(info.last_observed)}")

    snaps = tables["line_snapshots"]
    mine = snaps[snaps.line_id == line_id].sort_values("snapshot_date")
    left, right = st.columns(2)
    with left:
        st.subheader("Trips per week, each month")
        st.altair_chart(alt.Chart(mine.assign(date=mine.snapshot_date.map(short_day))).mark_bar().encode(
            x=alt.X("date:N", sort=None, title="Timetable date (2026)", axis=DATE_AXIS),
            y=alt.Y("window_departures:Q", title="Planned trips per week"),
            tooltip=[alt.Tooltip("date:N", title="Timetable"), alt.Tooltip("public_code:N", title="Line number"),
                     alt.Tooltip("window_departures:Q", title="Trips"),
                     alt.Tooltip("window_stop_count:Q", title="Stops served")]), use_container_width=True)
    with right:
        st.subheader("Trips per day")
        day_rows = daily[daily.line_entity_id == line_id]
        dates = sorted(day_rows.snapshot_date.unique(), reverse=True)
        if dates:
            chosen = st.selectbox("Week starting", dates, format_func=day)
            per_day = day_rows[day_rows.snapshot_date == chosen].groupby("service_date", as_index=False).agg(
                trips=("scheduled_departures", "sum"), first=("first_departure_seconds", "min"),
                last=("last_departure_seconds", "max"))
            per_day["date"] = pd.to_datetime(per_day.service_date).dt.strftime("%a %-d")
            per_day["first trip"], per_day["last trip"] = per_day["first"].map(data.clock), per_day["last"].map(data.clock)
            st.altair_chart(alt.Chart(per_day).mark_bar().encode(
                x=alt.X("date:N", sort=None, title=None, axis=DATE_AXIS), y=alt.Y("trips:Q", title="Trips"),
                tooltip=["date", "trips", "first trip", "last trip"]), use_container_width=True)
        else:
            st.write("This line has no planned trips in any of the timetables.")

    st.subheader("Route versions")
    st.caption("A new version starts each time the line's route or stops change.")
    v = tables["line_versions"]
    v = v[v.line_id == line_id].sort_values("version_number")
    table(pd.DataFrame({
        "Version": v.version_number, "Number": v.public_code, "Route": v["name"],
        "From": v.first_observed.map(day), "Until": v.last_observed.map(day),
        "Why": v.version_reason.map(WHY_NEW_VERSION), "Stops": v.stop_count,
        "Replaces": v.renumbered_from_line_id.map(lambda x: label_list(x, names))}))

    st.subheader("What changed, month by month")
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
        table(pd.DataFrame({
            "Between": ch.from_snapshot.map(day) + " → " + ch.to_snapshot.map(day),
            "What changed": ch.change_type.map(CHANGE), "Details": [detail(r) for _, r in ch.iterrows()]}))
    else:
        st.write("No changes found for this line.")

    if len(mine):
        last = mine.iloc[-1]
        with st.expander(f"Stops served in the week of {day(last.snapshot_date)} "
                         f"({last.window_stop_count} of {last.stop_count} stops)"):
            st.write(stop_list(last.window_stop_ids, stop_names) or "None")


def page_changes(history):
    st.header("What changed")
    intro("Compare two monthly timetables side by side: which lines were added or removed, "
          "and which lines got more or fewer trips.")
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
    cols[1].metric("Lines added / removed", f"+{row.lines_appeared} / −{row.lines_disappeared}")
    cols[2].metric("Trips per week", f"{row.window_departures_after:,}",
                   int(row.window_departures_after - row.window_departures_before))
    cols[3].metric("Stops gained / lost", f"+{row.stops_gained_window_service} / −{row.stops_lost_window_service}",
                   help="Stops that got or lost all their trips in the week counted.")

    t = tables["line_transitions"]
    t = t[(t.from_snapshot == start) & (t.provider == provider)]

    gone = t[t.status == "disappeared"]
    if len(gone):
        st.subheader("Lines removed")
        confirmed = gone.interpretation.isin(["renumbered_verified", "discontinued_verified"]).sum()
        st.caption(f"{confirmed} of {len(gone)} confirmed by the operator. Click a link to read the notice.")
        replaced = renumberings[renumberings.to_snapshot == end].groupby("old_line_id").new_line_id \
            .agg(lambda ids: "; ".join(names.get(i, i) for i in ids)) if len(renumberings) else pd.Series(dtype=str)
        notices = [f for f in (renumberings.rename(columns={"old_line_id": "line_id"}), discontinuations) if len(f)]
        notices = pd.concat(notices) if notices else pd.DataFrame(columns=["line_id", "to_snapshot", "source_url"])
        notices = notices[notices.to_snapshot == end].groupby("line_id").source_url.first()
        table(pd.DataFrame({
            "Number": gone.public_code_before, "Route": gone.name_before,
            "Trips per week before": gone.departures_before.astype("Int64"),
            "What happened": gone.interpretation.map(WHAT_HAPPENED),
            "Replaced by": gone.line_id.map(replaced).fillna(""), "Notice": gone.line_id.map(notices)}),
            column_config={"Notice": st.column_config.LinkColumn("Notice", display_text="read")})

    new = t[t.status == "appeared"]
    if len(new):
        st.subheader("Lines added")
        st.caption("A new line often appears one month before it starts running, so it can show 0 trips.")
        table(pd.DataFrame({
            "Number": new.public_code_after, "Route": new.name_after,
            "Trips per week": new.departures_after.astype("Int64"),
            "What it is": new.interpretation.map(WHAT_HAPPENED)}))

    both = t[(t.departures_before > 0) & (t.departures_after > 0)].copy()
    if len(both):
        st.subheader("Biggest changes in trips")
        st.caption("Lines that ran in both timetables. Red = fewer trips, blue = more trips.")
        both["change"] = both.departure_change
        both["line"] = (both.public_code_after + " " + both.name_after.fillna("")).str.strip()
        top = pd.concat([both.nsmallest(8, "change"), both.nlargest(8, "change")]).drop_duplicates("line_id")
        top = top[top.change != 0].sort_values("change")
        st.altair_chart(alt.Chart(top).mark_bar().encode(
            x=alt.X("change:Q", title="Change in trips per week"), y=alt.Y("line:N", sort=None, title=None),
            color=alt.condition("datum.change < 0", alt.value("#c0392b"), alt.value("#2e86c1")),
            tooltip=[alt.Tooltip("line:N", title="Line"), alt.Tooltip("departures_before:Q", title="Before"),
                     alt.Tooltip("departures_after:Q", title="After")]), use_container_width=True)

    stops = tables["stop_coverage_changes"]
    lost = stops[(stops.from_snapshot == start) & (stops.provider == provider) & (stops.measure == "window_service")
                 & (stops.change_type == "stop_lost")]
    with st.expander(f"Stops that lost all their trips ({len(lost)})"):
        table(pd.DataFrame({"Stop": lost.stop_name,
                            "Lines that used to stop here": lost.line_codes_before.map(lambda c: ", ".join(c))}))


def page_replaced(history):
    st.header("Replaced lines")
    intro("Lines the operator replaced with a new line or number. "
          "Pick one to compare the old and new line.")
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
    pair["date"] = pair.snapshot_date.map(short_day)
    st.altair_chart(alt.Chart(pair.sort_values("snapshot_date")).mark_line(point=True).encode(
        x=alt.X("date:N", sort=None, title="Timetable date (2026)", axis=DATE_AXIS),
        y=alt.Y("window_departures:Q", title="Planned trips per week"),
        color=alt.Color("line:N", title=None, legend=alt.Legend(orient="bottom", labelLimit=400)),
        tooltip=["date", "line", alt.Tooltip("window_departures:Q", title="Trips")]).properties(padding={"left": 45}),
        use_container_width=True)

    before = snaps[(snaps.line_id == pick.old_line_id) & (snaps.snapshot_date < pick.to_snapshot)]
    after = snaps[(snaps.line_id == pick.new_line_id) & (snaps.snapshot_date >= pick.to_snapshot)]
    if len(before) and len(after):
        old, new = before.sort_values("snapshot_date").iloc[-1], after.sort_values("snapshot_date").iloc[0]
        old_stops, new_stops = set(old.stop_ids), set(new.stop_ids)
        c1, c2 = st.columns(2)
        c1.metric(f"Old: {old.public_code} {old['name']}", f"{old.window_departures} trips/week")
        c2.metric(f"New: {new.public_code} {new['name']}", f"{new.window_departures} trips/week",
                  int(new.window_departures - old.window_departures))
        st.markdown(f"**Stops on both lines:** {len(old_stops & new_stops)}")
        st.markdown(f"**Stops dropped ({len(old_stops - new_stops)}):** "
                    f"{stop_list(sorted(old_stops - new_stops), stop_names) or 'none'}")
        st.markdown(f"**New stops ({len(new_stops - old_stops)}):** "
                    f"{stop_list(sorted(new_stops - old_stops), stop_names) or 'none'}")

    if len(discontinuations):
        with st.expander("Lines that were stopped (no replacement)"):
            table(pd.DataFrame({
                "Operator": discontinuations.provider.map(PROVIDERS), "Line": discontinuations.line_id.map(short),
                "Last seen": discontinuations.from_snapshot.map(day), "Note": discontinuations.note,
                "Notice": discontinuations.source_url}),
                column_config={"Notice": st.column_config.LinkColumn("Notice", display_text="read")})


def page_checks(items, history):
    st.header("Data checks")
    intro("Before a timetable is shown here, it is checked automatically. "
          "If too many records are broken, it is not published.")
    rows = []
    for item in items:
        m = item["gold_manifest"]
        recon = m.get("reconciliation", {})
        gate = m.get("quality_gate") or {}
        rows.append({"Timetable date": day(item["snapshot_date"]),
                     "Result": GATE.get(gate.get("status"), "Not checked (older run)"),
                     "Trips counted": recon.get("gold_scheduled_departures"),
                     "Trips left out": recon.get("excluded_service_occurrences"),
                     "Totals add up": "✅" if recon.get("counts_match") else "❌"})
    table(pd.DataFrame(rows))
    st.caption("Trips are left out only when their dates or times could not be read. "
               "Rules are in config/quality_thresholds.json.")

    manifest, _, _ = history
    with st.expander("Technical details"):
        checks = {k: v for k, v in manifest.get("checks", {}).items() if k != "snapshots"}
        table(pd.DataFrame({"Check": [k.replace("_", " ").capitalize() for k in checks],
                            "Value": [str(v) for v in checks.values()]}))
        report = data.REPO / "docs/quality_report.md"
        if report.is_file():
            st.markdown(report.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------

PAGES = {"Overview": page_overview, "One line": page_line, "What changed": page_changes,
         "Replaced lines": page_replaced, "Data checks": page_checks}


def main():
    st.sidebar.title("🚌 Timetable history")
    st.sidebar.caption("Planned bus and boat trips for Kolumbus (Rogaland) and AKT (Agder), 2026")
    page = st.sidebar.radio("Go to", list(PAGES), label_visibility="collapsed")
    with st.sidebar.expander("How to read this"):
        st.markdown(
            "- **Timetable**: the published timetable, downloaded on the 15th of each month.\n"
            "- **Trips**: planned departures in the week after that date. These are plans, "
            "not buses that actually ran.\n"
            "- **Confirmed**: the operator announced the change on its website.\n"
            "- A change between two months happened somewhere in between; the exact day is not known.")
    if st.sidebar.button("Reload data"):
        st.cache_data.clear()

    try:
        items, overview, daily = load(DATA_ROOT)
        history = load_history(DATA_ROOT)
    except Exception as error:  # a readable message instead of a traceback
        st.error(f"Could not load the data from {DATA_ROOT}. Has the pipeline been run?\n\n"
                 f"{type(error).__name__}: {error}")
        st.stop()

    if page == "Overview":
        page_overview(items, overview)
    elif page == "One line":
        page_line(daily, history)
    elif page == "Data checks":
        page_checks(items, history)
    else:
        PAGES[page](history)


main()