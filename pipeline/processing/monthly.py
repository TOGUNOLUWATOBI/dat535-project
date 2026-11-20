"""Publish reusable monthly plans from completed per-publication Gold aggregates.

One whole publication supplies each covered service date for all providers.
Later publications can revise earlier plans; these are not actual operations.
Pandas handles the small daily aggregates; XML and Silver are never reread.
"""

import argparse
from datetime import date
import fcntl
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
from uuid import uuid4

from pipeline.checkpoints import identity, output_inventory
from pipeline.common import sha256_file, utc_now, write_json
from pipeline.processing.windows import window_signature


def snapshots(data_root, verify=False):
    """Newest completed Gold per date. Viewing results does not require Silver files or two exports."""
    root = Path(data_root).resolve()
    state = root / "pipeline_state.sqlite3"
    if not state.is_file():
        raise ValueError(f"Published checkpoint registry not found: {state}")
    db = sqlite3.connect(state.as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        extra = ", a.inventory_json" if verify else ""
        rows = db.execute(f"""SELECT s.snapshot_date, a.output_path{extra}
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
        if verify and output_inventory(gold) != json.loads(row["inventory_json"]):
            raise ValueError(f"Registered Gold files no longer match their checkpoint: {gold}")
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
    import pandas as pd

    frames = []
    for item in items:
        table = item["gold_manifest"]["tables"].get(name)
        if table:
            frame = pd.read_parquet(Path(item["gold"]) / table["path"])
            frame["snapshot_date"] = item["snapshot_date"]
            frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def select_plans(daily, items):
    """Return canonical line/day rows and a calendar spine, before any UI filters.

    Coverage requires one publication valid for every selected provider. Never
    fill an absent line from an older publication: absence can be a real change.
    Missing coverage stays unknown; a covered date can legitimately have no rows.
    """
    import pandas as pd

    if not items or len({i["snapshot_date"] for i in items}) != len(items):
        raise ValueError("Monthly plans require nonempty, distinct publication dates")
    providers = {tuple(sorted(i["gold_manifest"]["providers"])) for i in items}
    if len(providers) != 1 or not next(iter(providers)):
        raise ValueError("Monthly plans require one consistent provider selection")
    first = pd.Timestamp(min(i["service_from"] for i in items)).to_period("M").start_time
    last = pd.Timestamp(max(i["service_to"] for i in items)).to_period("M").end_time
    calendar = pd.DataFrame({"service_date": pd.date_range(first, last)})
    calendar["source_export"] = pd.Series(None, index=calendar.index, dtype="object")
    for item in sorted(items, key=lambda i: i["snapshot_date"]):
        # Older manifests without calendar bounds retain their declared window.
        start, end = item["service_from"], item["service_to"]
        coverage = item["gold_manifest"].get("source_calendar_coverage")
        if coverage is not None:
            start = max([start] + [(coverage.get(p) or {}).get("from") or "9999-12-31"
                                  for p in next(iter(providers))])
            end = min([end] + [(coverage.get(p) or {}).get("to") or "0001-01-01"
                              for p in next(iter(providers))])
        # Compare ISO dates so missing calendar bounds do not overflow Timestamp.
        valid = calendar.service_date.dt.strftime("%Y-%m-%d").between(start, end)
        calendar.loc[valid, "source_export"] = item["snapshot_date"]
    calendar["covered"] = calendar.source_export.notna()
    calendar["month"] = calendar.service_date.dt.strftime("%Y-%m")
    calendar["providers"] = ",".join(next(iter(providers)))
    source = daily.copy()
    source["service_date"] = pd.to_datetime(source.service_date)
    source["snapshot_date"] = pd.to_datetime(source.snapshot_date).dt.strftime("%Y-%m-%d")
    if not source.provider.isin(next(iter(providers))).all():
        raise ValueError("Daily Gold contains an unexpected provider")
    if not source.snapshot_date.isin([i["snapshot_date"] for i in items]).all():
        raise ValueError("Daily Gold contains an unregistered publication")
    for item in items:
        dates = source.loc[source.snapshot_date == item["snapshot_date"], "service_date"]
        if not dates.between(pd.Timestamp(item["service_from"]), pd.Timestamp(item["service_to"])).all():
            raise ValueError("Daily Gold contains service dates outside its declared window")
    chosen = source.merge(calendar[["service_date", "source_export", "month"]],
                          left_on=["service_date", "snapshot_date"],
                          right_on=["service_date", "source_export"], how="inner", validate="many_to_one")
    return chosen, calendar


def publication_inputs(items):
    """Stable provenance used to refuse monthly results built from older Gold."""
    return [{"snapshot_date": i["snapshot_date"], "gold": i["gold"],
             "gold_manifest_sha256": i["gold_manifest_sha256"]} for i in items]


def read_monthly(data_root, items=None):
    """Read the published monthly tables, refusing stale or damaged outputs."""
    import pandas as pd

    base = Path(data_root).resolve() / "gold/entur/monthly"
    try:
        pointer = json.loads((base / "current.json").read_text())
        output = (base / pointer["path"]).resolve()
        if not output.is_relative_to(base) or output == base:
            raise ValueError("Monthly output path is outside its storage directory")
        manifest = json.loads((output / "metadata/manifest.json").read_text())
        if (manifest["status"] != "complete" or manifest["identity"] != pointer["identity"]
                or manifest["implementation"] != sha256_file(Path(__file__))
                or manifest["inputs"] != publication_inputs(items if items is not None else snapshots(data_root))):
            raise ValueError("Monthly publication is outdated")
        if output_inventory(output) != json.loads((output / "metadata/inventory.json").read_text()):
            raise ValueError("Monthly output files no longer match their saved hashes")
        tables = manifest["tables"]
        return (pd.read_parquet(output / tables["daily_line_service"]["path"]),
                pd.read_parquet(output / tables["daily_coverage"]["path"]))
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        raise ValueError(f"Monthly Gold is unavailable or outdated. Run bash scripts/run.sh monthly "
                         f"--data-root {Path(data_root)}. Detail: {error}") from error


def publish(data_root):
    """Build or reuse an immutable monthly Gold output and atomically publish it.

    Checkpoint hashes verify the inputs; saved inventories verify reuse. A failed
    build cannot replace current.json, and every previous attempt remains intact.
    """
    base = Path(data_root).resolve() / "gold/entur/monthly"
    base.mkdir(parents=True, exist_ok=True)
    with (base / ".publish.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        items = snapshots(data_root, verify=True)
        if not items:
            raise ValueError("No completed Gold publications to combine")
        inputs = publication_inputs(items)
        implementation = sha256_file(Path(__file__))
        key = identity([inputs, implementation])
        try:
            pointer = json.loads((base / "current.json").read_text())
            if pointer.get("identity") == key:
                read_monthly(data_root, items)
                output = base / pointer["path"]
                return output, json.loads((output / "metadata/manifest.json").read_text()), True
        except (OSError, ValueError, KeyError):
            pass  # Preserve damaged/old attempts and write a new one.
        output = base / f"monthly-{key[:12]}-{uuid4().hex[:8]}"
        (output / "metadata").mkdir(parents=True)
        path = output / "metadata/manifest.json"
        report = {"status": "running", "identity": key, "implementation": implementation,
                  "started_at_utc": utc_now(), "inputs": inputs, "tables": {},
                  "providers": sorted(items[0]["gold_manifest"]["providers"]),
                  "policy": "latest whole covering publication for all providers; no old-line backfill",
                  "meaning": "Reconstructed published plans, including later revisions; not actual operations"}
        write_json(path, report)
        try:
            daily = gold_table(items, "daily_line_service")
            chosen, calendar = select_plans(daily, items)
            # Reconcile against source rows independently for every selected date.
            totals = daily.assign(service_date=daily.service_date.astype(str)).groupby(
                ["snapshot_date", "service_date"]).scheduled_departures.sum()
            expected = sum(int(totals.get((row.source_export, str(row.service_date.date())), 0))
                           for row in calendar.itertuples() if row.covered)
            actual = int(chosen.scheduled_departures.sum())
            if actual != expected:
                raise RuntimeError("Monthly departures do not reconcile to the selected source plans")
            for name, table in (("daily_line_service", chosen), ("daily_coverage", calendar)):
                directory = output / name
                directory.mkdir()
                # Write a Parquet DATE, readable by Spark as well as pandas.
                table.assign(service_date=table.service_date.dt.date).to_parquet(
                    directory / "part-00000.parquet", index=False)
                report["tables"][name] = {"path": name, "rows": len(table)}
            # Refuse to publish if a concurrent snapshot run changed our inputs.
            if publication_inputs(snapshots(data_root, verify=True)) != inputs:
                raise RuntimeError("Gold publications changed during monthly processing; retry")
            report.update(status="complete", completed_at_utc=utc_now(),
                          reconciliation={"selected_source_departures": expected,
                                          "gold_scheduled_departures": actual, "counts_match": True},
                          coverage={month: {"days": len(part), "covered_days": int(part.covered.sum()),
                                            "complete": bool(part.covered.all())}
                                    for month, part in calendar.groupby("month")})
            write_json(path, report)
            write_json(output / "metadata/inventory.json", output_inventory(output))
            write_json(base / "current.json", {"path": output.name, "identity": key})
            return output, report, False
        except Exception as error:
            report.update(status="failed", completed_at_utc=utc_now(), error=f"{type(error).__name__}: {error}")
            write_json(path, report)
            raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        output, _, reused = publish(args.data_root)
        print(f"Monthly Gold {'reused' if reused else 'published'}: {output}")
        return 0
    except Exception as error:
        print(f"Monthly Gold failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
