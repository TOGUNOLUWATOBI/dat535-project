"""DAT535 Gold explorer: planned service and route changes for Kolumbus and AKT.

Run with:  bash scripts/run_gui.sh   (reads DAT535_DATA_ROOT, default /srv/dat535/data)
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
MEANING = ("Planned timetables as published in each export, not buses that ran. Departures are counted "
           "in each snapshot's seven-day window. A change between two snapshots was published somewhere "
           "in between; it is not an effective date unless an operator source says so.")
INTERPRETATION = {
    "renumbered_verified": "Replaced or renumbered (confirmed by operator)",
    "discontinued_verified": "Discontinued (confirmed by operator)",
    "not_observed": "No longer published (reason not confirmed)",
    "replacement_verified": "Replaces an earlier line (confirmed by operator)",
    "first_observed": "New in this export",
}
CHANGE = {
    "line_appeared": "Line first published", "line_disappeared": "Line no longer published",
    "public_code_changed": "Number changed", "name_changed": "Name changed",
    "transport_mode_changed": "Mode changed", "stops_added": "Stops added", "stops_removed": "Stops removed",
    "patterns_changed": "Stop sequence changed", "service_started": "Service started",
    "service_stopped": "No departures in window", "departures_increased": "More departures",
    "departures_decreased": "Fewer departures", "first_departure_changed": "First departure moved",
    "last_departure_changed": "Last departure moved",
}
REASON = {"first_observed": "First seen", "structure_changed": "Route changed", "reappeared": "Published again"}

st.set_page_config(page_title="DAT535 Gold explorer", page_icon="🚌", layout="wide")


# --------------------------------------------------------------------------
# Cached data access (read-only).

@st.cache_data(show_spinner="Reading Gold outputs …")
def load(root):
    items = data.snapshots(root)
    daily = data.gold_table(items, "daily_line_service")
    return items, data.overview(items), daily


@st.cache_data(show_spinner="Reading route history …")
def load_history(root):
    h = data.History(root)
    tables = {name: h.table(name) for name in ("line_snapshots", "line_transitions", "line_changes",
                                               "line_versions", "stop_coverage_changes", "change_summary")}
    tables["catalog"] = data.line_catalog(tables["line_snapshots"])
    return str(h.path), h.manifest, tables, h.stop_names()


def names(ids, lookup, limit=None):
    shown = [lookup.get(i, i) for i in list(ids)[:limit]]
    more = len(ids) - limit if limit and len(ids) > limit else 0
    return ", ".join(shown) + (f" … (+{more})" if more else "")


def labels_for(catalog):
    """line_id -> readable label (number, route, period)."""
    return dict(zip(catalog.line_id, catalog.label))


def label_list(value, lookup):
    """Comma-separated line IDs (e.g. a merge) -> readable labels."""
    if value is None or (isinstance(value, float) and pd.isna(value)) or value == "":
        return ""
    return "; ".join(lookup.get(part, part) for part in str(value).split(","))


# --------------------------------------------------------------------------
# Pages.

def page_overview(items, overview, history):
    st.header("Network overview")
    st.caption(MEANING)
    latest = overview[overview.snapshot == overview.snapshot.max()]
    cols = st.columns(len(latest) + 1)
    cols[0].metric("Snapshots", len(items))
    for col, row in zip(cols[1:], latest.itertuples()):
        previous = overview[(overview.provider == row.provider) & (overview.snapshot < row.snapshot)]
        delta = int(row.departures - previous.iloc[-1].departures) if len(previous) else None
        col.metric(f"{PROVIDERS[row.provider]} departures, {row.snapshot}", f"{row.departures:,}", delta)

    mode = st.radio("Transport mode", ["all", "bus", "boat"], horizontal=True)
    measure = {"all": "departures", "bus": "bus", "boat": "water"}[mode]
    chart = alt.Chart(overview).mark_line(point=True).encode(
        x=alt.X("snapshot:N", title="Snapshot"),
        y=alt.Y(f"{measure}:Q", title=f"Planned departures in window ({mode})"),
        color=alt.Color("provider:N", title="Provider"),
        tooltip=["snapshot", "window", "provider", "departures", "bus", "water", "lines with service"])
    st.altair_chart(chart, use_container_width=True)
    st.markdown("**Dips:** February is the Agder winter school holiday (AKT only, weekdays); July is the "
                "summer timetable in both regions. The last snapshot (8 Nov) is the first scheduled run "
                "and has an eight-week gap.")
    st.dataframe(overview.rename(columns={"water": "boat"}), hide_index=True, use_container_width=True)
    _, manifest, _, _ = history
    st.caption(f"Route history status: {manifest.get('status')}, checks: "
               f"{manifest['checks'].get('service_date_departures')}")


def page_line(daily, history):
    st.header("Line explorer")
    st.caption(MEANING)
    _, _, tables, stop_lookup = history
    catalog, lookup = tables["catalog"], labels_for(tables["catalog"])
    c1, c2 = st.columns([1, 3])
    provider = c1.selectbox("Provider", list(PROVIDERS), format_func=PROVIDERS.get)
    lines = catalog[catalog.provider == provider]
    options = list(lines.line_id)
    # Default: the current bus 55 in Kristiansand (a verified change), else the first line.
    default = next((i for i, r in enumerate(lines.itertuples()) if r.number == "55" and not r.period.startswith("Jan")), 0)
    line_id = c2.selectbox("Line (number and route; type to search)", options, index=default, format_func=lookup.get)
    info = lines[lines.line_id == line_id].iloc[0]
    st.caption(f"{info['mode'].capitalize()} · published {data.month(info.first_observed)} – "
               f"{data.month(info.last_observed)} · NeTEx line ID `{line_id}`")

    snaps = tables["line_snapshots"]
    mine = snaps[snaps.line_id == line_id].sort_values("snapshot_date")
    left, right = st.columns(2)
    with left:
        st.subheader("Departures per snapshot")
        chart = alt.Chart(mine.rename(columns={"window_departures": "departures", "public_code": "number"})).mark_bar() \
            .encode(x=alt.X("snapshot_date:N", title="Snapshot"), y=alt.Y("departures:Q", title="Departures in window"),
                    tooltip=["snapshot_date", "number", "name", "departures", "window_stop_count"])
        st.altair_chart(chart, use_container_width=True)
    with right:
        st.subheader("Departures per day")
        day_rows = daily[daily.line_entity_id == line_id]
        snapshot = st.selectbox("Snapshot", sorted(day_rows.snapshot_date.unique(), reverse=True)) \
            if len(day_rows) else None
        if snapshot:
            day = day_rows[day_rows.snapshot_date == snapshot].groupby("service_date", as_index=False).agg(
                departures=("scheduled_departures", "sum"), first=("first_departure_seconds", "min"),
                last=("last_departure_seconds", "max"))
            day["date"] = pd.to_datetime(day.service_date).dt.strftime("%a %d %b")
            day["first bus"], day["last bus"] = day["first"].map(data.clock), day["last"].map(data.clock)
            st.altair_chart(alt.Chart(day).mark_bar().encode(
                x=alt.X("date:N", sort=None, title="Service date"), y=alt.Y("departures:Q", title="Departures"),
                tooltip=["date", "departures", "first bus", "last bus"]), use_container_width=True)
        else:
            st.info("No departures for this line in any snapshot window.")

    st.subheader("Versions of the route")
    v = tables["line_versions"]
    v = v[v.line_id == line_id].sort_values("version_number")
    st.dataframe(pd.DataFrame({
        "Version": v.version_number, "Number": v.public_code, "Route": v["name"],
        "First seen": v.first_observed, "Last seen": v.last_observed, "Not seen from": v.first_not_observed,
        "Why a new version": v.version_reason.map(REASON), "Stops": v.stop_count, "Stop sequences": v.pattern_count,
        "Replaces": v.renumbered_from_line_id.map(lambda x: label_list(x, lookup))}),
        hide_index=True, use_container_width=True)
    st.subheader("Changes between exports")
    ch = tables["line_changes"]
    ch = ch[ch.line_id == line_id].sort_values(["from_snapshot", "change_type"])

    def detail(row):
        if row.change_type in ("stops_added", "stops_removed", "line_appeared", "line_disappeared"):
            return names(list(row["items"]) if row["items"] is not None else [], stop_lookup, 8)
        if row.change_type.endswith("departure_changed"):
            return f"{data.clock(float(row.old_value)) if row.old_value else '–'} → " \
                   f"{data.clock(float(row.new_value)) if row.new_value else '–'}"
        if row.change_type in ("stops_added", "stops_removed", "patterns_changed"):
            return f"{row.old_value} → {row.new_value}"
        return f"{row.old_value or '–'} → {row.new_value or '–'}"
    st.dataframe(pd.DataFrame({
        "Between": ch.from_snapshot + " → " + ch.to_snapshot, "Change": ch.change_type.map(CHANGE),
        "Kind": ch.change_category.map({"structure": "Route", "service": "Service"}),
        "Details": [detail(r) for _, r in ch.iterrows()]}), hide_index=True, use_container_width=True)
    if len(mine):
        last = mine.iloc[-1]
        st.subheader(f"Stops served in the {last.snapshot_date} window")
        st.caption(f"{last.window_stop_count} of the {last.stop_count} stops in the published route have a "
                   "departure in this window.")
        st.write(names(list(last.window_stop_ids), stop_lookup) or "none")


def page_changes(history):
    st.header("Network changes between exports")
    st.caption(MEANING)
    _, _, tables, stop_lookup = history
    lookup = labels_for(tables["catalog"])
    renumberings, discontinuations = data.evidence()
    summary = tables["change_summary"]
    steps = summary[["from_snapshot", "to_snapshot"]].drop_duplicates().sort_values("from_snapshot")
    labels = [f"{r.from_snapshot} → {r.to_snapshot}" for r in steps.itertuples()]
    c1, c2 = st.columns([2, 1])
    step = c1.selectbox("Between exports", labels, index=labels.index("2026-06-15 → 2026-07-15") if
                        "2026-06-15 → 2026-07-15" in labels else len(labels) - 1)
    provider = c2.selectbox("Provider", list(PROVIDERS), format_func=PROVIDERS.get)
    start, end = step.split(" → ")
    row = summary[(summary.from_snapshot == start) & (summary.provider == provider)].iloc[0]
    cols = st.columns(5)
    cols[0].metric("Lines published", int(row.lines_after), int(row.lines_after - row.lines_before))
    cols[1].metric("New / no longer published", f"+{row.lines_appeared} / −{row.lines_disappeared}")
    explained = int(row.lines_disappeared_renumbered_verified + row.lines_disappeared_discontinued_verified)
    cols[2].metric("Removals confirmed by operator", f"{explained} of {row.lines_disappeared}")
    cols[3].metric("Departures in window", f"{row.window_departures_after:,}",
                   int(row.window_departures_after - row.window_departures_before))
    cols[4].metric("Stops with departures", f"+{row.stops_gained_window_service} / −{row.stops_lost_window_service}")

    t = tables["line_transitions"]
    t = t[(t.from_snapshot == start) & (t.provider == provider)]
    gone = t[t.status == "disappeared"].copy()
    if len(gone):
        st.subheader("Lines no longer published")
        replaced = renumberings[renumberings.to_snapshot == end].groupby("old_line_id").new_line_id \
            .agg(lambda ids: "; ".join(lookup.get(i, i) for i in ids)) if len(renumberings) else pd.Series(dtype=str)
        sources = pd.concat([renumberings.rename(columns={"old_line_id": "line_id"}) if len(renumberings) else None,
                             discontinuations if len(discontinuations) else None])
        sources = sources[sources.to_snapshot == end].groupby("line_id").source_url.first() \
            if sources is not None and len(sources) else pd.Series(dtype=str)
        st.dataframe(pd.DataFrame({
            "Number": gone.public_code_before, "Route": gone.name_before,
            "Departures before": gone.departures_before.astype("Int64"),
            "What happened": gone.interpretation.map(INTERPRETATION),
            "Replaced by": gone.line_id.map(replaced).fillna(""),
            "Source": gone.line_id.map(sources), "Line ID": gone.line_id}),
            hide_index=True, use_container_width=True,
            column_config={"Source": st.column_config.LinkColumn("Source", display_text="operator notice")})
    new = t[t.status == "appeared"]
    if len(new):
        st.subheader("Lines published for the first time")
        st.caption("Operators often publish a new line one export before it starts running (0 departures).")
        st.dataframe(pd.DataFrame({
            "Number": new.public_code_after, "Route": new.name_after,
            "Departures after": new.departures_after.astype("Int64"),
            "What it is": new.interpretation.map(INTERPRETATION), "Line ID": new.line_id}),
            hide_index=True, use_container_width=True)
    both = t[(t.departures_before > 0) & (t.departures_after > 0)].copy()
    if len(both):
        st.subheader("Largest departure changes")
        both["change"] = both.departure_change
        both["line"] = (both.public_code_after + " " + both.name_after.fillna("")).str.strip()
        top = pd.concat([both.nsmallest(8, "change"), both.nlargest(8, "change")]).drop_duplicates("line_id")
        top = top[top.change != 0].sort_values("change")
        st.altair_chart(alt.Chart(top).mark_bar().encode(
            x=alt.X("change:Q", title="Change in departures"), y=alt.Y("line:N", sort=None, title=None),
            color=alt.condition("datum.change < 0", alt.value("#c0392b"), alt.value("#2e86c1")),
            tooltip=[alt.Tooltip("line:N", title="Line"), alt.Tooltip("departures_before:Q", title="Before"),
                     alt.Tooltip("departures_after:Q", title="After")]), use_container_width=True)
    stops = tables["stop_coverage_changes"]
    lost = stops[(stops.from_snapshot == start) & (stops.provider == provider) & (stops.measure == "window_service")
                 & (stops.change_type == "stop_lost")]
    with st.expander(f"Stops with no departure in the {end} window ({len(lost)})"):
        st.dataframe(pd.DataFrame({"Stop": lost.stop_name,
                                   "Served before by": lost.line_codes_before.map(lambda c: ", ".join(c)),
                                   "Stop ID": lost.stop_id}), hide_index=True, use_container_width=True)


def page_before_after(history):
    st.header("Before and after a confirmed route change")
    st.caption("Changes confirmed by an operator notice (config/route_renumberings.json). " + MEANING)
    _, _, tables, stop_lookup = history
    lookup = labels_for(tables["catalog"])
    renumberings, discontinuations = data.evidence()
    if not len(renumberings):
        st.info("No confirmed changes recorded.")
        return
    short = lambda line_id: lookup.get(line_id, line_id).split("  (")[0]
    options = list(range(len(renumberings)))
    describe = lambda i: (f"{PROVIDERS[renumberings.provider.iloc[i]].split(' ')[0]}: "
                          f"{short(renumberings.old_line_id.iloc[i])}  →  {short(renumberings.new_line_id.iloc[i])}"
                          f"   ({data.month(renumberings.to_snapshot.iloc[i])})")
    default = next((i for i in options if renumberings.old_line_id.iloc[i] == "AKT:Line:1055"), 0)
    pick = renumberings.iloc[st.selectbox("Confirmed change", options, index=default, format_func=describe)]
    st.markdown(f"**Source:** [operator notice]({pick.source_url})")
    st.write(pick.note)
    snaps = tables["line_snapshots"]
    pair = snaps[snaps.line_id.isin([pick.old_line_id, pick.new_line_id])].copy()
    pair["line"] = pair.line_id.map({pick.old_line_id: f"Before: {short(pick.old_line_id)}",
                                     pick.new_line_id: f"After: {short(pick.new_line_id)}"})
    pair = pair.rename(columns={"window_departures": "departures"})
    st.altair_chart(alt.Chart(pair).mark_line(point=True).encode(
        x=alt.X("snapshot_date:N", title="Snapshot"), y=alt.Y("departures:Q", title="Departures in window"),
        color=alt.Color("line:N", title=None, legend=alt.Legend(orient="bottom", labelLimit=400)),
        tooltip=["snapshot_date", "line", "departures"]), use_container_width=True)
    before = snaps[(snaps.line_id == pick.old_line_id) & (snaps.snapshot_date < pick.to_snapshot)]
    after = snaps[(snaps.line_id == pick.new_line_id) & (snaps.snapshot_date >= pick.to_snapshot)]
    if len(before) and len(after):
        old, new = before.iloc[-1], after.iloc[0]
        old_stops, new_stops = set(old.stop_ids), set(new.stop_ids)
        c1, c2 = st.columns(2)
        c1.metric(f"Before ({old.snapshot_date}): {old.public_code} {old['name']}", f"{old.window_departures} departures")
        c2.metric(f"After ({new.snapshot_date}): {new.public_code} {new['name']}", f"{new.window_departures} departures",
                  int(new.window_departures - old.window_departures))
        st.markdown(f"**Stops only before ({len(old_stops - new_stops)}):** "
                    f"{names(sorted(old_stops - new_stops), stop_lookup) or 'none'}")
        st.markdown(f"**Stops only after ({len(new_stops - old_stops)}):** "
                    f"{names(sorted(new_stops - old_stops), stop_lookup) or 'none'}")
        st.markdown(f"**Stops on both:** {len(old_stops & new_stops)}")
    st.caption(f"Line IDs: {pick.old_line_id} → {pick.new_line_id}")
    if len(discontinuations):
        with st.expander("Lines confirmed as discontinued"):
            st.dataframe(pd.DataFrame({
                "Provider": discontinuations.provider, "Line": discontinuations.line_id.map(short),
                "Last published": discontinuations.from_snapshot, "Source": discontinuations.source_url,
                "Note": discontinuations.note}), hide_index=True, use_container_width=True,
                column_config={"Source": st.column_config.LinkColumn("Source", display_text="operator notice")})


def page_quality(items, history):
    st.header("Data quality and run status")
    st.markdown("Gold is published only when its Silver input passes the quality gate "
                "(`config/quality_thresholds.json`): any rejection warns; more than 1% rejected, any ambiguous "
                "reference or no accepted service stops Gold. Outputs built before the gate show *not recorded*.")
    rows = []
    for item in items:
        m = item["gold_manifest"]
        recon = m.get("reconciliation", {})
        rows.append({"Snapshot": item["snapshot_date"], "Gold status": m.get("status"),
                     "Quality gate": data.gate_status(m),
                     "Service occurrences in Silver": recon.get("silver_service_occurrences"),
                     "Accepted (fixed, valid)": recon.get("eligible_fixed_service_occurrences"),
                     "Excluded": recon.get("excluded_service_occurrences"),
                     "Gold departures": recon.get("gold_scheduled_departures"),
                     "Counts match": recon.get("counts_match"),
                     "Completed (UTC)": m.get("completed_at_utc", "")[:19]})
    st.subheader("Gold outputs")
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
    path, manifest, _, _ = history
    st.subheader("Route-history checks")
    checks = {k: v for k, v in manifest.get("checks", {}).items() if k != "snapshots"}
    st.json(checks, expanded=False)
    st.caption(f"{path} · completed {manifest.get('completed_at_utc', '')[:19]} UTC")
    report = data.REPO / "docs/quality_report.md"
    if report.is_file():
        with st.expander("Silver before/after quality table (docs/quality_report.md)"):
            st.markdown(report.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------

def main():
    st.sidebar.title("🚌 DAT535 Gold explorer")
    st.sidebar.caption("Kolumbus and AKT planned timetables from Entur NeTEx exports")
    page = st.sidebar.radio("Page", ["Overview", "Line explorer", "Network changes", "Before / after",
                                     "Data quality"])
    st.sidebar.caption(f"Data root: `{DATA_ROOT}`")
    if st.sidebar.button("Reload data"):
        st.cache_data.clear()
    try:
        items, overview, daily = load(DATA_ROOT)
        history = load_history(DATA_ROOT)
    except Exception as error:  # show a readable message instead of a traceback
        st.error(f"Could not read published outputs under {DATA_ROOT}: {type(error).__name__}: {error}")
        st.stop()
    if page == "Overview":
        page_overview(items, overview, history)
    elif page == "Line explorer":
        page_line(daily, history)
    elif page == "Network changes":
        page_changes(history)
    elif page == "Before / after":
        page_before_after(history)
    else:
        page_quality(items, history)


main()
