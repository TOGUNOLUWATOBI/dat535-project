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

st.set_page_config(page_title="DAT535 Gold explorer", page_icon="🚌", layout="wide")


# --------------------------------------------------------------------------
# Cached data access (read-only).

@st.cache_data(show_spinner="Reading Gold outputs …")
def load(root):
    items = data.snapshots(root)
    daily = data.gold_table(items, "daily_line_service")
    summary = data.gold_table(items, "line_window_summary")
    return items, data.overview(items), daily, summary


@st.cache_data(show_spinner="Reading route history …")
def load_history(root):
    h = data.History(root)
    tables = {name: h.table(name) for name in ("line_snapshots", "line_transitions", "line_changes",
                                               "line_versions", "stop_coverage_changes", "change_summary")}
    return str(h.path), h.manifest, tables, h.stop_names()


def names(ids, lookup, limit=None):
    shown = [lookup.get(i, i) for i in list(ids)[:limit]]
    more = len(ids) - limit if limit and len(ids) > limit else 0
    return ", ".join(shown) + (f" … (+{more})" if more else "")


def line_label(row):
    return f"{row.line_public_code} · {row.line_entity_id}"


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

    mode = st.radio("Transport mode", ["all", "bus", "water"], horizontal=True)
    measure = {"all": "departures", "bus": "bus", "water": "water"}[mode]
    chart = alt.Chart(overview).mark_line(point=True).encode(
        x=alt.X("snapshot:N", title="Snapshot"),
        y=alt.Y(f"{measure}:Q", title=f"Planned departures in window ({mode})"),
        color=alt.Color("provider:N", title="Provider"),
        tooltip=["snapshot", "window", "provider", "departures", "bus", "water", "lines with service"])
    st.altair_chart(chart, use_container_width=True)
    st.markdown("**Dips:** February is the Agder winter school holiday (AKT only, weekdays); July is the "
                "summer timetable in both regions. The last snapshot (8 Nov) is the first scheduled run "
                "and has an eight-week gap.")
    st.dataframe(overview, hide_index=True, use_container_width=True)
    _, manifest, _, _ = history
    st.caption(f"Route history status: {manifest.get('status')}, checks: "
               f"{manifest['checks'].get('service_date_departures')}")


def page_line(items, daily, summary, history):
    st.header("Line explorer")
    st.caption(MEANING)
    _, _, tables, stop_lookup = history
    c1, c2 = st.columns([1, 3])
    provider = c1.selectbox("Provider", list(PROVIDERS), format_func=PROVIDERS.get)
    lines = (summary[summary.provider == provider][["line_public_code", "line_entity_id"]].drop_duplicates()
             .sort_values(["line_public_code", "line_entity_id"]))
    options = [line_label(r) for r in lines.itertuples()]
    default = next((i for i, o in enumerate(options) if o.startswith("55 ·")), 0)
    choice = c2.selectbox("Line (bus number · NeTEx line ID)", options, index=default)
    line_id = choice.split(" · ")[1]

    snaps = tables["line_snapshots"]
    mine = snaps[snaps.line_id == line_id].sort_values("snapshot_date")
    left, right = st.columns(2)
    with left:
        st.subheader("Departures per snapshot")
        chart = alt.Chart(mine).mark_bar().encode(
            x=alt.X("snapshot_date:N", title="Snapshot"), y=alt.Y("window_departures:Q", title="Departures in window"),
            tooltip=["snapshot_date", "public_code", "name", "window_departures", "stop_count", "window_stop_count"])
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
            day["service_date"] = day.service_date.astype(str)
            day["first bus"], day["last bus"] = day["first"].map(data.clock), day["last"].map(data.clock)
            st.altair_chart(alt.Chart(day).mark_bar().encode(
                x=alt.X("service_date:N", title="Service date"), y=alt.Y("departures:Q", title="Departures"),
                tooltip=["service_date", "departures", "first bus", "last bus"]), use_container_width=True)
        else:
            st.info("No departures for this line in any Gold window.")

    st.subheader("Versions (by observation date)")
    versions = tables["line_versions"]
    v = versions[versions.line_id == line_id].sort_values("version_number")
    st.dataframe(v[["version_number", "public_code", "name", "first_observed", "last_observed", "first_not_observed",
                    "version_reason", "stop_count", "pattern_count", "renumbered_from_line_id"]],
                 hide_index=True, use_container_width=True)
    st.subheader("Changes between exports")
    changes = tables["line_changes"]
    ch = changes[changes.line_id == line_id].sort_values(["from_snapshot", "change_type"]).copy()
    ch["details"] = ch["items"].map(lambda ids: names(list(ids), stop_lookup, 8) if ids is not None and len(ids) else "")
    st.dataframe(ch[["from_snapshot", "to_snapshot", "change_type", "change_category", "old_value", "new_value",
                     "details"]], hide_index=True, use_container_width=True)
    if len(mine):
        last = mine.iloc[-1]
        st.subheader(f"Stops served in the {last.snapshot_date} window ({last.window_stop_count} of {last.stop_count} "
                     "in published patterns)")
        st.write(names(list(last.window_stop_ids), stop_lookup) or "none")


def page_changes(history):
    st.header("Network changes between exports")
    st.caption(MEANING)
    _, _, tables, stop_lookup = history
    renumberings, discontinuations = data.evidence()
    summary = tables["change_summary"]
    steps = summary[["from_snapshot", "to_snapshot"]].drop_duplicates().sort_values("from_snapshot")
    labels = [f"{r.from_snapshot} → {r.to_snapshot}" for r in steps.itertuples()]
    c1, c2 = st.columns([2, 1])
    step = c1.selectbox("Step", labels, index=labels.index("2026-06-15 → 2026-07-15") if
                        "2026-06-15 → 2026-07-15" in labels else len(labels) - 1)
    provider = c2.selectbox("Provider", list(PROVIDERS), format_func=PROVIDERS.get)
    start, end = step.split(" → ")
    row = summary[(summary.from_snapshot == start) & (summary.provider == provider)].iloc[0]
    cols = st.columns(5)
    cols[0].metric("Lines", int(row.lines_after), int(row.lines_after - row.lines_before))
    cols[1].metric("Appeared / disappeared", f"+{row.lines_appeared} / −{row.lines_disappeared}")
    explained = int(row.lines_disappeared_renumbered_verified + row.lines_disappeared_discontinued_verified)
    cols[2].metric("Disappearances explained by a source", f"{explained} of {row.lines_disappeared}")
    cols[3].metric("Departures", f"{row.window_departures_after:,}",
                   int(row.window_departures_after - row.window_departures_before))
    cols[4].metric("Stops served in window", f"+{row.stops_gained_window_service} / −{row.stops_lost_window_service}")

    t = tables["line_transitions"]
    t = t[(t.from_snapshot == start) & (t.provider == provider)]
    gone = t[t.status == "disappeared"].copy()
    if len(gone):
        st.subheader("Lines no longer published")
        evidence = pd.concat([
            renumberings.assign(line_id=renumberings.get("old_line_id"),
                                evidence=lambda r: "→ " + r.new_line_id) if len(renumberings) else pd.DataFrame(),
            discontinuations.assign(evidence="discontinued") if len(discontinuations) else pd.DataFrame()])
        if len(evidence):
            evidence = evidence[evidence.to_snapshot == end].groupby("line_id").agg(
                evidence=("evidence", ", ".join), source=("source_url", "first")).reset_index()
            gone = gone.merge(evidence, on="line_id", how="left")
        st.dataframe(gone[["line_id", "public_code_before", "name_before", "departures_before", "interpretation"]
                          + [c for c in ("evidence", "source") if c in gone]],
                     hide_index=True, use_container_width=True,
                     column_config={"source": st.column_config.LinkColumn("source")})
    new = t[t.status == "appeared"]
    if len(new):
        st.subheader("Lines published for the first time")
        st.dataframe(new[["line_id", "public_code_after", "name_after", "departures_after", "interpretation"]],
                     hide_index=True, use_container_width=True)
    both = t[(t.departures_before > 0) & (t.departures_after > 0)].copy()
    if len(both):
        st.subheader("Largest departure changes")
        both["change"] = both.departure_change
        top = pd.concat([both.nsmallest(8, "change"), both.nlargest(8, "change")]).drop_duplicates("line_id")
        top = top[top.change != 0].sort_values("change")
        st.altair_chart(alt.Chart(top).mark_bar().encode(
            x=alt.X("change:Q", title="Change in departures"),
            y=alt.Y("public_code_after:N", sort=None, title="Bus number"),
            color=alt.condition("datum.change < 0", alt.value("#c0392b"), alt.value("#2e86c1")),
            tooltip=["line_id", "name_after", "departures_before", "departures_after"]), use_container_width=True)
    stops = tables["stop_coverage_changes"]
    lost = stops[(stops.from_snapshot == start) & (stops.provider == provider) & (stops.measure == "window_service")
                 & (stops.change_type == "stop_lost")]
    with st.expander(f"Stops with no departure in the {end} window ({len(lost)})"):
        st.dataframe(lost.assign(lines_before=lost.line_codes_before.map(lambda c: ", ".join(c)))[
            ["stop_name", "stop_id", "lines_before"]], hide_index=True, use_container_width=True)


def page_before_after(history):
    st.header("Before and after a verified route change")
    st.caption("Pairs confirmed by an operator source (config/route_renumberings.json). " + MEANING)
    _, _, tables, stop_lookup = history
    renumberings, discontinuations = data.evidence()
    if not len(renumberings):
        st.info("No verified renumberings recorded.")
        return
    labels = [f"{r.provider}: {r.old_line_id} → {r.new_line_id} ({r.to_snapshot})" for r in renumberings.itertuples()]
    default = next((i for i, l in enumerate(labels) if "AKT:Line:1055" in l), 0)
    pick = renumberings.iloc[labels.index(st.selectbox("Verified change", labels, index=default))]
    st.markdown(f"**Source:** [{pick.source_url}]({pick.source_url})")
    st.write(pick.note)
    snaps = tables["line_snapshots"]
    pair = snaps[snaps.line_id.isin([pick.old_line_id, pick.new_line_id])].copy()
    pair["line"] = pair.line_id.map({pick.old_line_id: f"before: {pick.old_line_id}",
                                     pick.new_line_id: f"after: {pick.new_line_id}"})
    st.altair_chart(alt.Chart(pair).mark_line(point=True).encode(
        x=alt.X("snapshot_date:N", title="Snapshot"), y=alt.Y("window_departures:Q", title="Departures in window"),
        color=alt.Color("line:N", title=None),
        tooltip=["snapshot_date", "line_id", "public_code", "name", "window_departures", "window_stop_count"]),
        use_container_width=True)
    before = snaps[(snaps.line_id == pick.old_line_id) & (snaps.snapshot_date < pick.to_snapshot)]
    after = snaps[(snaps.line_id == pick.new_line_id) & (snaps.snapshot_date >= pick.to_snapshot)]
    if len(before) and len(after):
        old, new = before.iloc[-1], after.iloc[0]
        old_stops, new_stops = set(old.stop_ids), set(new.stop_ids)
        c1, c2 = st.columns(2)
        c1.metric(f"Before ({old.snapshot_date}): bus {old.public_code}", f"{old.window_departures} departures",
                  help=old["name"])
        c2.metric(f"After ({new.snapshot_date}): bus {new.public_code}", f"{new.window_departures} departures",
                  int(new.window_departures - old.window_departures), help=new["name"])
        st.markdown(f"**Stops only before ({len(old_stops - new_stops)}):** "
                    f"{names(sorted(old_stops - new_stops), stop_lookup) or 'none'}")
        st.markdown(f"**Stops only after ({len(new_stops - old_stops)}):** "
                    f"{names(sorted(new_stops - old_stops), stop_lookup) or 'none'}")
        st.markdown(f"**Shared stops:** {len(old_stops & new_stops)}")
    if len(discontinuations):
        with st.expander("Verified discontinuations"):
            st.dataframe(discontinuations[["provider", "line_id", "to_snapshot", "source_url", "note"]],
                         hide_index=True, use_container_width=True,
                         column_config={"source_url": st.column_config.LinkColumn("source")})


def page_quality(items, overview, history):
    st.header("Data quality and run status")
    st.markdown("Gold is published only when its Silver input passes the quality gate "
                "(`config/quality_thresholds.json`): any rejection warns; more than 1% rejected, any ambiguous "
                "reference or no accepted service stops Gold. Outputs built before the gate show *not recorded*.")
    rows = []
    for item in items:
        m = item["gold_manifest"]
        recon = m.get("reconciliation", {})
        rows.append({"snapshot": item["snapshot_date"], "Gold status": m.get("status"),
                     "quality gate": data.gate_status(m),
                     "Silver service occurrences": recon.get("silver_service_occurrences"),
                     "accepted (fixed, valid)": recon.get("eligible_fixed_service_occurrences"),
                     "excluded": recon.get("excluded_service_occurrences"),
                     "Gold departures": recon.get("gold_scheduled_departures"),
                     "counts match": recon.get("counts_match"),
                     "completed (UTC)": m.get("completed_at_utc", "")[:19]})
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
        items, overview, daily, summary = load(DATA_ROOT)
        history = load_history(DATA_ROOT)
    except Exception as error:  # show a readable message instead of a traceback
        st.error(f"Could not read published outputs under {DATA_ROOT}: {type(error).__name__}: {error}")
        st.stop()
    if page == "Overview":
        page_overview(items, overview, history)
    elif page == "Line explorer":
        page_line(items, daily, summary, history)
    elif page == "Network changes":
        page_changes(history)
    elif page == "Before / after":
        page_before_after(history)
    else:
        page_quality(items, overview, history)


main()
