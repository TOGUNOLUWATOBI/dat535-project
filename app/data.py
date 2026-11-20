"""Read-only access to published Gold and route-history outputs for the GUI.

Only completed, registered checkpoints are read: the newest complete Gold per
snapshot from the checkpoint registry, and the newest complete route history.
Nothing here writes to the data root. Pandas reads the small Gold Parquet tables
directly, so the GUI needs no Spark session.
"""

import json
from pathlib import Path

import pandas as pd

from pipeline.checkpoints import latest_history
from pipeline.processing.monthly import gold_table, read_monthly, snapshots

REPO = Path(__file__).resolve().parent.parent


class History:
    """The newest complete route-history output."""

    def __init__(self, data_root):
        self.path, self.manifest = latest_history(data_root)

    def table(self, name):
        return pd.read_parquet(self.path / name)

    def stop_names(self):
        coverage = self.table("stop_coverage")[["stop_id", "stop_name"]].dropna()
        return dict(zip(coverage.stop_id, coverage.stop_name))


def month(snapshot):
    """'2026-07-15' -> 'Jul 2026'."""
    return pd.Timestamp(snapshot).strftime("%b %Y")


def line_catalog(line_snapshots):
    """One row per line ID with a readable label: number, route name and, when needed, period.

    The period is added when the line was not published in every snapshot, so that
    two IDs with the same number (e.g. bus 55 before and after June 2026) can be told apart.
    """
    snaps = line_snapshots.sort_values("snapshot_date")
    all_dates = sorted(snaps.snapshot_date.unique())
    rows = []
    for (provider, line_id), group in snaps.groupby(["provider", "line_id"], sort=False):
        last = group.iloc[-1]
        first_seen, last_seen = group.snapshot_date.iloc[0], group.snapshot_date.iloc[-1]
        code = last.public_code or "?"
        mode = "boat" if last.transport_mode == "water" else last.transport_mode or "bus"
        name = (last["name"] or "").replace(" - ", " – ")
        period = ""
        if first_seen != all_dates[0] or last_seen != all_dates[-1]:
            period = f"{month(first_seen)} – {month(last_seen)}" if first_seen != last_seen else month(first_seen)
        label = f"{code} {name}".strip() + (f"  ({period})" if period else "")
        rows.append({"provider": provider, "line_id": line_id, "number": code, "route": name, "mode": mode,
                     "first_observed": first_seen, "last_observed": last_seen, "period": period, "label": label})
    catalog = pd.DataFrame(rows)
    # Natural sort by number: 2 before 10, letters and codes after digits.
    key = catalog.number.str.extract(r"^(\d+)")[0].astype(float)
    return catalog.assign(_n=key.fillna(1e9)).sort_values(["provider", "_n", "number", "first_observed"]) \
        .drop(columns="_n").reset_index(drop=True)


def evidence():
    """Verified renumberings and discontinuations, keyed for joining with transitions."""
    def load(name):
        path = REPO / "config" / name
        entries = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else []
        return [entry for entry in entries if entry.get("verified") is True]
    renumberings = pd.DataFrame(load("route_renumberings.json"))
    discontinuations = pd.DataFrame(load("route_discontinuations.json"))
    return renumberings, discontinuations


def clock(seconds):
    """Seconds after the operating day's midnight as HH:MM (with +1d after midnight)."""
    if seconds is None or pd.isna(seconds):
        return "–"
    day, rest = divmod(int(seconds), 86400)
    return f"{rest // 3600:02d}:{rest % 3600 // 60:02d}" + (f" +{day}d" if day else "")
