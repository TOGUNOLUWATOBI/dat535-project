"""Read-only access to published Gold and route-history outputs for the GUI.

Only completed, registered checkpoints are read: the newest complete Gold per
snapshot from the checkpoint registry, and the newest complete route history.
Nothing here writes to the data root. Pandas reads the small Gold Parquet tables
directly, so the GUI needs no Spark session.
"""

import json
import hashlib
from pathlib import Path
import sqlite3
from datetime import date

import pandas as pd

from pipeline.analysis.inspect_history import latest_history
from pipeline.processing.windows import window_signature

REPO = Path(__file__).resolve().parent.parent


def snapshots(data_root):
    """Newest completed Gold per date. Viewing results does not require Silver files or two exports."""
    root = Path(data_root).resolve()
    state = root / "pipeline_state.sqlite3"
    if not state.is_file():
        raise ValueError(f"Published checkpoint registry not found: {state}")
    db = sqlite3.connect(state.as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        rows = db.execute("""SELECT s.snapshot_date, a.output_path
            FROM snapshots s JOIN stage_attempts a USING(snapshot_key)
            WHERE a.stage='gold' AND a.status='complete'
            ORDER BY s.snapshot_date, a.finished_at DESC, a.started_at DESC""").fetchall()
    finally:
        db.close()
    chosen = {}
    for row in rows:
        day = row["snapshot_date"]
        if day in chosen:
            continue
        gold = Path(row["output_path"])
        if not gold.is_absolute():
            gold = root / gold
        raw = (gold / "metadata/manifest.json").read_bytes()
        manifest = json.loads(raw)
        if manifest.get("status") != "complete" or manifest.get("snapshot_date") != day:
            raise ValueError(f"Registered Gold output has inconsistent publication metadata: {gold}")
        if "daily_line_service" not in manifest.get("tables", {}):
            raise ValueError(f"Published Gold output has no daily service table: {gold}")
        providers = manifest.get("providers")
        if (not isinstance(providers, list) or not providers or
                any(not isinstance(p, str) or not p for p in providers) or len(set(providers)) != len(providers)):
            raise ValueError(f"Published Gold output has an invalid provider selection: {gold}")
        if date.fromisoformat(manifest["service_to"]) < date.fromisoformat(manifest["service_from"]):
            raise ValueError(f"Published Gold output has a reversed service window: {gold}")
        chosen[day] = {"snapshot_date": day, "gold": str(gold), "gold_manifest": manifest,
                       "gold_manifest_sha256": hashlib.sha256(raw).hexdigest(),
                       "service_from": manifest["service_from"], "service_to": manifest["service_to"]}
    items = [chosen[day] for day in sorted(chosen)]
    selections = {tuple(sorted(item["gold_manifest"]["providers"])) for item in items}
    windows = {window_signature(i) for i in items}
    if len(selections) > 1 or len(windows) > 1:
        raise ValueError("Published exports use different provider selections or service-window definitions")
    return items


def gold_table(items, name):
    """A Gold table for every snapshot, with ``snapshot_date`` set."""
    frames = []
    for item in items:
        table = item["gold_manifest"]["tables"].get(name)
        if table:
            frame = pd.read_parquet(Path(item["gold"]) / table["path"])
            frame["snapshot_date"] = item["snapshot_date"]
            frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


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
