"""Kolumbus punctuality explorer over the Gold run (read-only, no Spark).

Pages: Overview, Lines, Stops, Changes, Data quality. Small Gold tables are read whole and cached;
the large stop table is read with PyArrow filter pushdown.
Stop names, the stops each line serves and the timetable changes come from Silver, since Gold stores only quay IDs.

Start with:  bash src/app/run_gui.sh --restart
"""

import json
import os
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

GOLD = Path(os.environ.get("DAT535_GOLD_RUN", "/srv/dat535/data/gold/kol/runs/gold-kol-53d-2026-v2"))
SILVER = Path(os.environ.get("DAT535_SILVER_RUNS", "/srv/dat535/data/silver/kol/runs"))

MODES = {"bus": "🚌 Bus", "water": "⛴️ Boat & ferry", "unknown": "❔ Not identified"}
EVENTS = {"departure": "Departures", "arrival": "Arrivals"}
BASELINES = {"et_aimed": "Live schedule (SIRI)", "netex_planned": "Printed timetable (NeTEx)"}
MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September",
          "October", "November", "December"]
COUNTS = ["eligible_event_count", "early_count", "within_window_count", "late_count", "severe_delay_count",
          "delay_sum_sec"]
SERVICE = ["scheduled_journey_count", "scheduled_with_et_journey_count", "scheduled_without_et_journey_count",
           "journey_cancelled_count", "journey_not_cancelled_count", "stop_cancelled_count", "stop_not_cancelled_count"]

# Plain-language column names; the format (%, min/s, count) is picked from the code's suffix in `show`.
LABELS = {
    "transport_mode": "Transport", "event_type": "Measured at", "baseline": "Compared against",
    "operating_date": "Date", "line_public_code": "Line", "line_name": "Route", "line_id": "Line ID",
    "stop_name": "Stop", "quay_id": "Quay ID", "eligible_event_count": "Stops measured",
    "within_window_rate": "On time", "early_rate": "Early", "late_rate": "Late",
    "severe_delay_rate": "Very late (15+ min)", "median_delay_sec": "Typical delay", "p90_delay_sec": "Worst 10%",
    "p95_delay_sec": "Worst 5%", "mean_delay_sec": "Average delay", "worst_day_p95_delay_sec": "Worst 5% (worst day)",
    "plan_agreement_rate": "Live = timetable", "mean_plan_disagreement_sec": "Avg. live − timetable",
    "median_abs_plan_disagreement_sec": "Typical gap", "p95_abs_plan_disagreement_sec": "Gap, worst 5%",
    "scheduled_journey_count": "Trips scheduled", "scheduled_with_et_journey_count": "Trips tracked live",
    "scheduled_without_et_journey_count": "Trips not tracked", "journey_cancellation_rate": "Trips cancelled",
    "stop_cancellation_rate": "Stops cancelled", "selected_date_count": "Days sampled",
    "weekday_date_counts": "Weekdays", "period_name": "Period", "reason": "Reason", "event_count": "Stops excluded",
    "planned_call_matching_coverage": "Timetable stops matched", "reported_call_matching_coverage": "Live reports matched",
    "siri_file_available": "Live feed present", "silver_gate_status": "Quality check",
}
REASONS = {  # Gold exclusion codes -> why a stop event could not be measured
    "missing_recorded_departure": "Vehicle never reported a departure time",
    "missing_recorded_arrival": "Vehicle never reported an arrival time",
    "missing_aimed_departure": "No live-schedule departure time to compare with",
    "missing_aimed_arrival": "No live-schedule arrival time to compare with",
    "missing_planned_departure": "No timetable departure time to compare with",
    "missing_planned_arrival": "No timetable arrival time to compare with",
    "estimated_time": "Only an estimate was reported, not an actual time",
    "cancelled_journey": "The whole trip was cancelled",
    "cancelled_stop": "The stop was cancelled",
    "unknown_stop_cancellation": "Unclear whether the stop was cancelled",
    "flexible_service_window": "On-demand trip with no fixed time",
}

st.set_page_config(page_title="Kolumbus punctuality", page_icon="🚌", layout="wide")


# Reading (cached)

@st.cache_data(show_spinner=False, max_entries=64)
def table(name, columns=None, filters=None):
    """One Gold table; `filters` (PyArrow) keeps the big tables from loading whole."""
    frame = pd.read_parquet(GOLD / name, columns=columns, filters=filters)
    if "weekday_date_counts" in frame:  # map column -> "Thu ×5"
        frame["weekday_date_counts"] = frame.weekday_date_counts.map(
            lambda pairs: ", ".join(f"{day} ×{n}" for day, n in (pairs or [])) or "—")
    return frame


@st.cache_data(show_spinner=False)
def read_json(relative):
    return json.loads((GOLD / relative).read_text())


@st.cache_data(show_spinner="Loading stop names …")
def stop_lookup():
    """(quay -> stop name, line -> quays it serves) from every Silver batch behind this Gold run."""
    names, served = [], []
    for run in sorted(SILVER.glob("*/")):
        names.append(pd.read_parquet(run / "journey_pattern_stops", columns=["quay_id", "stop_point_name"]))
        served.append(pd.read_parquet(run / "netex_stop_calls", columns=["resolved_line_id", "quay_id"])
                      .drop_duplicates())
    if not names:
        return {}, pd.DataFrame(columns=["resolved_line_id", "quay_id"])
    names = pd.concat(names).dropna().drop_duplicates("quay_id")
    return dict(zip(names.quay_id, names.stop_point_name)), pd.concat(served).dropna().drop_duplicates()


REGULAR_START = pd.Timestamp("2026-01-08")  # 1 January is a public holiday with reduced service
MIN_DAYS = 3  # a line or stop must show up on this many sampled days to count (filters one-off diversions)


@st.cache_data(show_spinner="Finding timetable changes …")
def timetable_changes():
    """New/ended lines and stops added to or dropped from a line, from the planned stop calls in Silver.

    A line is its public number (e.g. '3') plus transport mode, so a new internal line ID with the same
    number is not counted as a new line. A change is dated to the first sampled day it shows up on; it
    happened somewhere between the line's previous sampled run (`since`) and that day.
    """
    served, trips = [], []
    columns = ["operating_date", "resolved_public_code", "resolved_transport_mode", "quay_id", "service_journey_id"]
    for run in sorted(SILVER.glob("*/")):
        calls = pd.read_parquet(run / "netex_stop_calls", columns=columns).dropna(
            subset=["resolved_public_code", "quay_id"])
        # Hive partition columns may be categorical, especially for one-day runs.
        calls["operating_date"] = pd.to_datetime(calls.operating_date.astype("string"))
        served.append(calls.drop(columns="service_journey_id").drop_duplicates())
        trips.append(calls.groupby(["operating_date", "resolved_public_code"], observed=True).service_journey_id.nunique())
    served = pd.concat(served).rename(columns={"resolved_public_code": "line", "resolved_transport_mode": "mode"})
    trips = pd.concat(trips).groupby(level=[0, 1]).sum()
    end_cutoff = served.operating_date.max() - pd.Timedelta(days=14)  # weekend-only lines skip the last days

    runs = served[["line", "mode", "operating_date"]].drop_duplicates().sort_values(["line", "operating_date"])
    runs["prev_run"] = runs.groupby("line").operating_date.shift()
    runs["next_run"] = runs.groupby("line").operating_date.shift(-1)
    all_dates = sorted(served.operating_date.unique())

    lines_ = runs.groupby("line").agg(mode=("mode", "first"), first=("operating_date", "min"),
                                      last=("operating_date", "max"), days=("operating_date", "nunique"))
    lines_["trips_per_day"] = trips.groupby(level=1).mean().reindex(lines_.index)
    started = (lines_["first"] > REGULAR_START) & (lines_.days >= MIN_DAYS)
    ended = (lines_["last"] < end_cutoff) & (lines_.days >= MIN_DAYS)
    lines_["status"] = None
    lines_.loc[started, "status"] = "🆕 Started"
    lines_.loc[ended, "status"] = "⛔ Ended"
    lines_.loc[started & ended, "status"] = "🔁 Ran for a while"
    before = lambda d: max((x for x in all_dates if x < d), default=pd.NaT)
    after = lambda d: min((x for x in all_dates if x > d), default=pd.NaT)
    lines_["not_yet_on"] = lines_["first"].map(before)
    lines_["gone_by"] = lines_["last"].map(after)
    lines_ = lines_[lines_.status.notna()].reset_index()

    stops_ = served.groupby(["line", "quay_id"]).operating_date.agg(["min", "max", "nunique"]).reset_index()
    stops_ = stops_.join(runs.groupby("line").operating_date.agg(line_first="min", line_last="max"), on="line")
    added = stops_[(stops_["min"] > stops_.line_first) & (stops_["min"] > REGULAR_START)
                   & (stops_["nunique"] >= MIN_DAYS)]
    dropped = stops_[(stops_["max"] < stops_.line_last) & (stops_["max"] < end_cutoff)
                     & (stops_["nunique"] >= MIN_DAYS)]
    keyed = runs.set_index(["line", "operating_date"])
    added = added.assign(change="➕ Added", operating_date=added["min"]).join(
        keyed["prev_run"].rename("since"), on=["line", "operating_date"])
    dropped = dropped.join(keyed["next_run"], on=["line", "max"]).assign(change="➖ Dropped")
    dropped = dropped.assign(operating_date=dropped.next_run, since=dropped["max"])
    stops_ = pd.concat([added, dropped])[["line", "operating_date", "since", "change", "quay_id"]]
    stops_ = stops_.merge(runs[["line", "mode"]].drop_duplicates("line"), on="line")
    return lines_, stops_


def by_rates(frame, keys):
    """Add up counts per group and recompute rates; a rate is empty (—) when nothing was measured."""
    g = frame.groupby(keys, as_index=False, dropna=False)[COUNTS].sum()
    measured = g.eligible_event_count.where(g.eligible_event_count > 0)
    for rate in ["within_window", "early", "late", "severe_delay"]:
        g[f"{rate}_rate"] = g[f"{rate}_count"] / measured
    g["mean_delay_sec"] = g.delay_sum_sec / measured
    return g


def ratio(part, whole):
    return part / whole if whole else None  # zero denominator -> no rate (—), never 0 %


# Display helpers

def fmt_pct(v):
    return "—" if v is None or pd.isna(v) else f"{v:.1%}"


def fmt_sec(v):
    """Delay in words people read: '+45 s', '+2 min 05 s', '−1 min 10 s' (minus = early)."""
    if v is None or pd.isna(v):
        return "—"
    sign, v = ("+" if v >= 0 else "−"), abs(round(v))
    return f"{sign}{v} s" if v < 60 else f"{sign}{v // 60} min {v % 60:02d} s"


def show(frame, columns, height=None):
    """Sortable table with readable headers and values; missing values show as —."""
    formats = {}
    for c in columns:
        if c.endswith(("_rate", "_coverage")):
            formats[LABELS.get(c, c)] = fmt_pct
        elif c.endswith("_sec"):
            formats[LABELS.get(c, c)] = fmt_sec
        elif c.endswith("_count"):
            formats[LABELS.get(c, c)] = lambda v: "—" if pd.isna(v) else f"{v:,.0f}"
        elif frame[c].dtype == bool:
            formats[LABELS.get(c, c)] = lambda v: "✅ Yes" if v else "❌ No"
    styled = frame[columns].rename(columns=LABELS).style.format(formats, na_rep="—")
    st.dataframe(styled, hide_index=True, use_container_width=True, height=height)


def period_name(label):
    """'winter-2026' -> 'Winter 2026 (Jan–Feb)', '2026-03' -> 'March 2026'."""
    if label in SEASON_WINDOWS:
        first, last = (pd.Timestamp(d) for d in SEASON_WINDOWS[label])
        return f"{label.split('-')[0].title()} {first.year} ({first:%b}–{last:%b})"
    year, month = label.split("-")
    return f"{MONTHS[int(month) - 1]} {year}"


def sample_note(row):
    """Every period is a sample of days, not every day: always say which."""
    st.caption(f"📅 **Based on {row.selected_date_count} sampled days** ({row.first_selected_date} → "
               f"{row.last_selected_date}; {row.weekday_date_counts}), not every day of the period.")


def line_chart(frame, x, y, color, title, pct=True, order=None, tooltip=(), height=260):
    return alt.Chart(frame, title=title).mark_line(point=True).encode(
        x=alt.X(x, title=None, sort=order),
        y=alt.Y(y, title=None, axis=alt.Axis(format="%" if pct else "d")),
        color=alt.Color(color, title=None, legend=alt.Legend(orient="bottom")),
        tooltip=[x, color, alt.Tooltip(y, format=".1%" if pct else ".1f"), *tooltip]).properties(height=height)


def chart(c):
    st.altair_chart(c, use_container_width=True)


# Sidebar: one set of filters for every page

meta = read_json("gold_manifest.json")
SEASON_WINDOWS = meta["season_windows_2026"]
policy = meta["metric_policy"]
network = table("period_network_punctuality")

st.sidebar.title("🚌 Kolumbus punctuality")
st.sidebar.caption(f"{len(meta['published_operating_dates'])} sampled days in 2026")
page = st.sidebar.radio("Page", ["📊 Overview", "🚏 Lines", "📍 Stops", "🔄 Changes", "🩺 Data quality"],
                        label_visibility="collapsed")
st.sidebar.divider()
months = sorted(network.loc[network.period_type == "month", "period_label"].unique())
period = st.sidebar.selectbox("Time period", [*SEASON_WINDOWS, *months], format_func=period_name,
                              help="Seasons (9–15 sampled days) give steadier numbers than months (4–5 days).")
period_type = "season" if period in SEASON_WINDOWS else "month"
labels = list(SEASON_WINDOWS) if period_type == "season" else months
mode = st.sidebar.radio("Transport", ["bus", "water"], format_func=MODES.get, horizontal=True)
event = st.sidebar.radio("Measure at", list(EVENTS), format_func=EVENTS.get, horizontal=True,
                         help="Departures: leaving early counts against punctuality. "
                              "Arrivals: what passengers experience at their destination.")
baseline, other = "et_aimed", "netex_planned"  # rank and chart against the live schedule; compare in tables
with st.sidebar.expander("ℹ️ What do the numbers mean?"):
    st.markdown(
        f"- **On time**: departs 0–{policy['departure_within_window_max_sec'] / 60:.0f} min late "
        f"(earlier counts as **early**); arrives at most {policy['arrival_within_limit_max_sec'] / 60:.0f} min late.\n"
        f"- **Very late**: more than {policy['severe_delay_min_exclusive_sec'] / 60:.0f} min late.\n"
        "- **Typical delay**: the median: half of stops are better, half worse.\n"
        "- **Worst 5%**: 95% of stops are better than this.\n"
        "- **—** means there was nothing to measure, not zero.\n"
        "- Actual times are compared with the **live schedule** (SIRI: the time aimed for that day, "
        "including same-day changes). The Overview and a line's detail also compare with the **printed "
        "timetable** (NeTEx).\n"
        "- Bus and boat are always shown separately.")

in_period = (network.period_type == period_type) & (network.period_label == period)
period_rows = network[in_period & (network.transport_mode == mode)]
pick = lambda f, b=baseline: f[(f.event_type == event) & (f.baseline == b)]
if pick(period_rows).empty:
    st.warning(f"No data for {MODES[mode]} in {period_name(period)}.")
    st.stop()
start, end = period_rows.period_start_date.min(), period_rows.period_end_date.max()
dates_in_period = [d for d in meta["published_operating_dates"] if str(start) <= d <= str(end)]
scope = f"{MODES[mode]} · {EVENTS[event].lower()} · {period_name(period)}"


# Pages

def overview():
    st.title("How punctual is Kolumbus?")
    st.markdown(f"**{scope}**")
    row, alt_row = pick(period_rows).iloc[0], pick(period_rows, other).iloc[0]
    service = table("period_line_service")
    total = service[(service.period_type == period_type) & (service.period_label == period)
                    & (service.transport_mode == mode)][SERVICE].sum()

    def vs(a, b):
        return None if pd.isna(a) or pd.isna(b) else f"{(a - b) * 100:+.1f} points vs {BASELINES[other].split(' (')[0].lower()}"

    k = st.columns(5)
    k[0].metric("On time", fmt_pct(row.within_window_rate), vs(row.within_window_rate, alt_row.within_window_rate),
                help="Share of measured stops served on time.")
    k[1].metric("Late", fmt_pct(row.late_rate), vs(row.late_rate, alt_row.late_rate), delta_color="inverse")
    k[2].metric("Very late (15+ min)", fmt_pct(row.severe_delay_rate),
                vs(row.severe_delay_rate, alt_row.severe_delay_rate), delta_color="inverse")
    k[3].metric("Typical delay", fmt_sec(row.median_delay_sec), help="Median: half of stops are better than this.")
    k[4].metric("Worst 5% delay", fmt_sec(row.p95_delay_sec), help="95% of stops are better than this.")
    k = st.columns(5)
    k[0].metric("Trips scheduled", f"{total.scheduled_journey_count:,.0f}")
    k[1].metric("Tracked live", fmt_pct(ratio(total.scheduled_with_et_journey_count, total.scheduled_journey_count)),
                help="Scheduled trips that also sent real-time (SIRI ET) reports.")
    k[2].metric("Not tracked", fmt_pct(ratio(total.scheduled_without_et_journey_count, total.scheduled_journey_count)))
    k[3].metric("Trips cancelled", fmt_pct(ratio(total.journey_cancelled_count,
                                                 total.journey_cancelled_count + total.journey_not_cancelled_count)),
                help="— when no trip had a known cancellation status.")
    k[4].metric("Stops cancelled", fmt_pct(ratio(total.stop_cancelled_count,
                                                 total.stop_cancelled_count + total.stop_not_cancelled_count)))
    sample_note(row)

    st.subheader("Live schedule vs printed timetable")
    st.caption("If trips look much worse against the printed timetable than against the live schedule, the delay "
               "comes from the plan being changed on the day, not from vehicles missing the time they aimed for.")
    grid = period_rows.sort_values(["event_type", "baseline"], ascending=[False, True]).assign(
        event_type=lambda f: f.event_type.map(EVENTS), baseline=lambda f: f.baseline.map(BASELINES))
    show(grid, ["event_type", "baseline", "within_window_rate", "early_rate", "late_rate", "severe_delay_rate",
                "median_delay_sec", "p90_delay_sec", "p95_delay_sec", "mean_delay_sec", "eligible_event_count"])
    st.markdown("**How often did the live schedule match the printed timetable?**")
    show(grid[grid.baseline == BASELINES["netex_planned"]],
         ["event_type", "plan_agreement_rate", "mean_plan_disagreement_sec", "median_abs_plan_disagreement_sec",
          "p95_abs_plan_disagreement_sec"])
    st.caption("⚠️ Trust the **typical** values here. In this Gold run some stops differ from the timetable by "
               "hours (a date/time matching issue), which drags the *average* columns far below zero.")

    st.subheader("Through the year")
    left, right = st.columns([3, 2])
    trend = network[(network.period_type == period_type) & (network.transport_mode == mode)
                    & (network.event_type == event)].assign(
        baseline=lambda f: f.baseline.map(BASELINES), period_name=lambda f: f.period_label.map(period_name))
    with left:
        chart(line_chart(trend, "period_name", "within_window_rate", "baseline", f"On time by {period_type}",
                         order=[period_name(p) for p in labels],
                         tooltip=["selected_date_count", "weekday_date_counts"]))
        st.caption("Hover a point to see how many days it is based on.")
    with right:
        st.markdown(f"**Bus vs boat — {period_name(period)}**")
        show(pick(network[in_period & network.transport_mode.isin(["bus", "water"])]).assign(
            transport_mode=lambda f: f.transport_mode.map(MODES)),
            ["transport_mode", "within_window_rate", "late_rate", "median_delay_sec", "p95_delay_sec",
             "selected_date_count"])

    st.subheader("Day by day (all 53 sampled days)")
    on_time, delay, cancel = st.tabs(["On time", "Delay (all transport)", "Trips cancelled"])
    daily = table("daily_line_punctuality", columns=["operating_date", "transport_mode", "event_type", "baseline",
                                                     *COUNTS])
    daily = by_rates(daily[(daily.transport_mode == mode) & (daily.event_type == event)],
                     ["operating_date", "baseline"]).assign(
        baseline=lambda f: f.baseline.map(BASELINES), operating_date=lambda f: pd.to_datetime(f.operating_date))
    band = alt.Chart(pd.DataFrame({"a": [pd.Timestamp(start)], "b": [pd.Timestamp(end)]})).mark_rect(
        opacity=0.08).encode(x="a:T", x2="b:T")
    with on_time:
        chart(band + line_chart(daily, "operating_date:T", "within_window_rate", "baseline",
                                f"On time, {MODES[mode]} (shaded = selected period)"))
    with delay:
        net = pick(table("daily_network_punctuality")).melt(
            "operating_date", ["median_delay_sec", "p95_delay_sec"], var_name="measure").assign(
            measure=lambda f: f.measure.map(LABELS), minutes=lambda f: f.value / 60,
            operating_date=lambda f: pd.to_datetime(f.operating_date))
        chart(line_chart(net, "operating_date:T", "minutes", "measure",
                         "Delay in minutes, bus and boat together (this table has no transport split)", pct=False))
    with cancel:
        s = table("daily_line_service")
        s = s[s.transport_mode == mode].groupby("operating_date")[SERVICE].sum()
        known = s.journey_cancelled_count + s.journey_not_cancelled_count
        rate = (s.journey_cancelled_count / known.where(known > 0)).dropna()
        if rate.empty:
            st.info(f"No {MODES[mode].split(' ', 1)[1].lower()} trip has a known cancellation status in this data, "
                    "so the rate is shown as — (unknown), not 0 %.")
        else:
            st.bar_chart(rate, height=220)


def lines():
    st.title("Which lines perform worst (or best)?")
    st.caption(f"{scope}. Pick a line below the table to see its details.")
    service = table("period_line_service")
    service = service[(service.period_type == period_type) & (service.period_label == period)
                      & (service.transport_mode == mode)]
    punct = table("period_line_punctuality")
    punct = punct[(punct.period_type == period_type) & (punct.period_label == period)
                  & (punct.transport_mode == mode) & (punct.event_type == event) & punct.line_id.notna()]
    agreement = punct[punct.baseline == "netex_planned"][["line_id", "plan_agreement_rate"]]
    ranked = (punct[punct.baseline == baseline].drop(columns="plan_agreement_rate")
              .merge(agreement, on="line_id", how="left")
              .merge(service[["line_id", "scheduled_journey_count", "scheduled_with_et_journey_count",
                              "journey_cancellation_rate", "stop_cancellation_rate"]], on="line_id", how="left"))

    c = st.columns([3, 2, 2, 1])
    search = c[0].text_input("Search", placeholder="Line number or place, e.g. 3 or Sandnes")
    sort = c[1].selectbox("Rank by", ["late_rate", "p95_delay_sec", "journey_cancellation_rate",
                                      "plan_agreement_rate"], format_func=LABELS.get)
    best = c[2].radio("Show", ["Worst first", "Best first"], horizontal=True) == "Best first"
    min_events = c[3].number_input("Min. stops measured", 0, value=100, step=50,
                                   help="Hide lines with too few measurements to be reliable.")
    ranked = ranked[ranked.eligible_event_count >= min_events]
    if search:
        text = ranked.line_public_code.fillna("") + " " + ranked.line_name.fillna("")
        ranked = ranked[text.str.contains(search, case=False, regex=False)]
    high_is_bad = sort != "plan_agreement_rate"
    ranked = ranked.sort_values(sort, ascending=best == high_is_bad, na_position="last")
    st.caption(f"{len(ranked)} lines. Click a column header to sort by it.")
    show(ranked, ["line_public_code", "line_name", "within_window_rate", "late_rate", "severe_delay_rate",
                  "median_delay_sec", "p95_delay_sec", "plan_agreement_rate", "scheduled_journey_count",
                  "scheduled_with_et_journey_count", "journey_cancellation_rate", "stop_cancellation_rate",
                  "eligible_event_count", "line_id"], height=380)
    if ranked.empty:
        return
    sample_note(ranked.iloc[0])

    st.subheader("One line in detail")
    names = dict(zip(ranked.line_id, ranked.line_public_code.fillna("?") + " — " + ranked.line_name.fillna("")))
    line = st.selectbox("Line", list(names), format_func=names.get)
    detail = table("period_line_punctuality")
    detail = detail[(detail.line_id == line) & (detail.period_type == period_type) & (detail.period_label == period)]
    st.markdown("**Departures vs arrivals, against both the live schedule and the printed timetable**")
    show(detail.sort_values(["event_type", "baseline"], ascending=[False, True]).assign(
        event_type=lambda f: f.event_type.map(EVENTS), baseline=lambda f: f.baseline.map(BASELINES)),
        ["event_type", "baseline", "within_window_rate", "early_rate", "late_rate", "severe_delay_rate",
         "median_delay_sec", "p90_delay_sec", "p95_delay_sec", "mean_delay_sec", "plan_agreement_rate",
         "median_abs_plan_disagreement_sec", "eligible_event_count"])
    daily = table("daily_line_punctuality")
    daily = daily[(daily.line_id == line) & (daily.event_type == event)].assign(
        baseline=lambda f: f.baseline.map(BASELINES), operating_date=lambda f: pd.to_datetime(f.operating_date))
    chart(line_chart(daily, "operating_date:T", "within_window_rate", "baseline",
                     f"On time per sampled day — {EVENTS[event].lower()}"))


def stops():
    st.title("Where do delays build up?")
    st.caption(f"{scope}. Stops with the highest share of late {EVENTS[event].lower()}. A stop's numbers include "
               "every line that serves it.")
    names, served = stop_lookup()
    lines_ = table("period_line_service")
    lines_ = lines_[(lines_.transport_mode == mode) & lines_.line_id.notna()].drop_duplicates("line_id")
    line_names = dict(zip(lines_.line_id, lines_.line_public_code.fillna("?") + " — " + lines_.line_name.fillna("")))
    c = st.columns([2, 2, 1])
    line = c[0].selectbox("Only stops on line", ["all"] + sorted(line_names, key=line_names.get),
                          format_func=lambda v: "All lines" if v == "all" else line_names[v])
    day = c[1].selectbox("Day", ["all"] + dates_in_period,
                         format_func=lambda v: f"All {len(dates_in_period)} sampled days" if v == "all" else v)
    min_events = c[2].number_input("Min. stops measured", 0, value=200, step=50,
                                   help="Hide quays with too few measurements to be reliable.")
    when = ([("operating_date", ">=", start), ("operating_date", "<=", end)] if day == "all"
            else [("operating_date", "==", pd.Timestamp(day).date())])
    with st.spinner("Reading stop data …"):
        frame = table("daily_stop_punctuality", columns=["quay_id", "p95_delay_sec", *COUNTS],
                      filters=[*when, ("transport_mode", "==", mode), ("event_type", "==", event),
                               ("baseline", "==", baseline)])
    if line != "all":
        frame = frame[frame.quay_id.isin(served.loc[served.resolved_line_id == line, "quay_id"])]
    quays = by_rates(frame, ["quay_id"]).join(
        frame.groupby("quay_id").p95_delay_sec.max().rename("worst_day_p95_delay_sec"), on="quay_id")
    quays["stop_name"] = quays.quay_id.map(names).fillna("(unnamed)")
    quays = quays[quays.eligible_event_count >= min_events].sort_values("late_rate", ascending=False)
    st.caption(f"{len(quays):,} quays shown, most often late first. Counts are added up over the sampled days; "
               "'Worst 5% (worst day)' is the worst single day, since percentiles cannot be added up.")
    if quays.empty:
        st.info("No quay has enough measurements. Lower 'Min. stops measured'.")
        return
    top = quays.head(15).assign(label=lambda f: f.stop_name + " (" + f.quay_id.str.split(":").str[-1] + ")")
    chart(alt.Chart(top, title="15 quays most often late").mark_bar().encode(
        x=alt.X("late_rate", title=None, axis=alt.Axis(format="%")), y=alt.Y("label", sort="-x", title=None),
        tooltip=["stop_name", "quay_id", alt.Tooltip("late_rate", format=".1%", title="Late"),
                 alt.Tooltip("eligible_event_count", title="Stops measured")]))
    show(quays.head(200), ["stop_name", "within_window_rate", "late_rate", "severe_delay_rate", "mean_delay_sec",
                           "worst_day_p95_delay_sec", "eligible_event_count", "quay_id"], height=380)


def changes():
    st.title("When did lines start, stop or change route?")
    whole_year = st.toggle("Whole year", value=True, help="Off: only changes first seen in the selected period.")
    st.caption(f"{MODES[mode]} · {'all of 2026' if whole_year else period_name(period)}. The timetable is "
               f"checked on {len(meta['published_operating_dates'])} sampled days, so a change is dated to the first "
               "sampled day it shows up on; it happened between the previous sampled day and that day. A line is "
               "its public number, so a line that keeps its number but gets a new internal ID is not 'new'.")
    lines_, stops_ = timetable_changes()
    names, _ = stop_lookup()
    lines_, stops_ = lines_[lines_["mode"] == mode], stops_[stops_["mode"] == mode]
    if not whole_year:
        in_range = lambda d: d.between(pd.Timestamp(start), pd.Timestamp(end))
        lines_ = lines_[in_range(lines_["first"]) | in_range(lines_["gone_by"])]
        stops_ = stops_[in_range(stops_.operating_date)]

    routes = (stops_.groupby(["line", "operating_date", "since"])
              .agg(added=("change", lambda c: (c == "➕ Added").sum()),
                   dropped=("change", lambda c: (c == "➖ Dropped").sum())).reset_index())
    min_stops = st.number_input("Count a route change when at least this many stops are added or dropped", 1,
                                value=3, help="Small changes are often a single stop moved or a school trip.")
    routes = routes[routes.added + routes.dropped >= min_stops]

    k = st.columns(3)
    k[0].metric("Lines started", int(lines_.status.isin(["🆕 Started", "🔁 Ran for a while"]).sum()))
    k[1].metric("Lines ended", int(lines_.status.isin(["⛔ Ended", "🔁 Ran for a while"]).sum()))
    k[2].metric("Route changes", len(routes), help=f"{routes.line.nunique()} lines")

    timeline = pd.concat([
        pd.DataFrame({"date": lines_.loc[lines_.status != "⛔ Ended", "first"], "what": "Line started"}),
        pd.DataFrame({"date": lines_.loc[lines_.status != "🆕 Started", "gone_by"], "what": "Line ended"}),
        pd.DataFrame({"date": routes.operating_date, "what": "Route changed"})]).dropna()
    if timeline.empty:
        st.info("No changes found for this selection.")
        return
    chart(alt.Chart(timeline, title="Changes per sampled day").mark_bar().encode(
        x=alt.X("yearmonthdate(date):O", title=None, axis=alt.Axis(format="%d %b", labelAngle=-45)),
        y=alt.Y("count()", title=None), color=alt.Color("what", title=None, legend=alt.Legend(orient="bottom")),
        tooltip=[alt.Tooltip("yearmonthdate(date):T", title="Seen on"), "what", "count()"]).properties(height=240))

    st.subheader("Lines that started or ended")
    fmt = lambda d: "—" if pd.isna(d) else f"{d:%d %b}"
    lines_ = lines_.sort_values("first").assign(
        started=lambda f: f.apply(lambda r: f"{fmt(r.not_yet_on)} → {fmt(r['first'])}"
                                  if r.status != "⛔ Ended" else "—", axis=1),
        ended=lambda f: f.apply(lambda r: f"{fmt(r['last'])} → {fmt(r.gone_by)}"
                                if r.status != "🆕 Started" else "—", axis=1))
    st.dataframe(lines_[["line", "status", "started", "ended", "days", "trips_per_day"]].rename(columns={
        "line": "Line", "status": "What happened", "started": "Started (between)", "ended": "Ended (between)",
        "days": "Sampled days running", "trips_per_day": "Trips per day"}).style.format({"Trips per day": "{:.0f}"}),
        hide_index=True, use_container_width=True)
    st.caption("'Started between 22 Apr → 1 May' means it did not run on 22 April and did on 1 May. "
               "Lines that run only on holidays or a few days can look new; check 'Sampled days running'.")

    st.subheader("Route changes: stops added or dropped")
    if routes.empty:
        st.info("No route changes this size. Lower the number above.")
        return
    routes = routes.sort_values(["operating_date", "line"]).assign(
        label=lambda f: "Line " + f.line + " · " + f.since.map(fmt) + " → " + f.operating_date.map(fmt))
    st.dataframe(routes[["label", "added", "dropped"]].rename(columns={
        "label": "Line · changed between", "added": "Stops added", "dropped": "Stops dropped"}),
        hide_index=True, use_container_width=True, height=300)
    pick_ = st.selectbox("Show the stops for", routes.label.tolist())
    row = routes[routes.label == pick_].iloc[0]
    detail = stops_[(stops_.line == row.line) & (stops_.operating_date == row.operating_date)].assign(
        stop=lambda f: f.quay_id.map(names).fillna("(unnamed)")).sort_values(["change", "stop"])
    st.dataframe(detail[["change", "stop", "quay_id"]].rename(columns={
        "change": "Change", "stop": "Stop", "quay_id": "Quay ID"}), hide_index=True, use_container_width=True)


def quality():
    st.title("Can the numbers be trusted?")
    st.caption("Check here when a number looks odd: was the live feed missing, or were many stops not measurable?")
    report = read_json("quality/quality_report.json")
    k = st.columns(3)
    k[0].metric("Quality check", "✅ Passed" if report["gate_status"] == "passed" else f"❌ {report['gate_status']}")
    k[1].metric("Days published", f"{len(meta['published_operating_dates'])} of {len(meta['expected_operating_dates'])}")
    k[2].metric("Data version", meta["gold_run_id"])
    for note in report["quality_notes"]:
        st.caption(f"ℹ️ {note}")
    for failure in report["hard_failures"]:
        st.error(failure)

    dq = table("daily_data_quality").sort_values("operating_date")
    missing = dq[~dq.siri_file_available.astype(bool)]
    if len(missing):
        st.warning(f"Live feed missing on {len(missing)} of {len(dq)} days: "
                   + ", ".join(map(str, missing.operating_date)))
    else:
        st.success(f"Live (SIRI) feed present on all {len(dq)} days.")
    coverage = dq.melt("operating_date", ["planned_call_matching_coverage", "reported_call_matching_coverage"],
                       var_name="measure").assign(measure=lambda f: f.measure.map(LABELS),
                                                  operating_date=lambda f: pd.to_datetime(f.operating_date))
    chart(line_chart(coverage, "operating_date:T", "value", "measure",
                     "How much of the timetable and the live reports could be matched, per day"))

    st.subheader(f"Why some {EVENTS[event].lower()} could not be measured — {period_name(period)}")
    rows = [{"operating_date": d, "reason": REASONS.get(r["reason_code"], r["reason_code"].replace("_", " ")),
             "event_count": r["event_count"]}
            for d, populations in zip(dq.operating_date, dq.metric_populations) for p in populations
            if p["event_type"] == event and p["baseline"] == baseline for r in p["exclusion_reason_counts"]]
    reasons = pd.DataFrame(rows, columns=["operating_date", "reason", "event_count"])
    reasons = reasons[reasons.operating_date.between(start, end)].groupby("reason", as_index=False).event_count.sum()
    chart(alt.Chart(reasons).mark_bar().encode(
        x=alt.X("event_count", title="Stops excluded"), y=alt.Y("reason", sort="-x", title=None),
        tooltip=["reason", alt.Tooltip("event_count", format=",")]))
    st.markdown("**Per day**")
    show(dq, ["operating_date", "siri_file_available", "silver_gate_status", "scheduled_journey_count",
              "planned_call_matching_coverage", "reported_call_matching_coverage"], height=320)


{"📊 Overview": overview, "🚏 Lines": lines, "📍 Stops": stops, "🔄 Changes": changes,
 "🩺 Data quality": quality}[page]()
st.divider()
st.caption(f"Read-only view of Gold run `{GOLD.name}` · Kolumbus (KOL) · {len(meta['published_operating_dates'])} "
           f"sampled days, {meta['published_operating_dates'][0]} → {meta['published_operating_dates'][-1]}.")
