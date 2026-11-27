"""Combine validated Silver history and append successful daily inputs without overwriting history."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil


def read_json(path: Path) -> dict:
    return json.loads(path.read_text())


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(payload, indent=2))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def silver_runs(root: Path) -> list[tuple[Path, set[str]]]:
    runs = []
    if not root.is_dir():
        raise FileNotFoundError(root)
    for run in sorted(root.iterdir()):
        if not run.is_dir() or run.name.startswith((".", "diagnostic-", "failed-", "superseded-")):
            continue
        manifest = read_json(run / "silver_manifest.json")
        report = read_json(Path(manifest.get("quality_report_path") or run / "quality/quality_report.json"))
        if manifest.get("status") != "complete" or report.get("gate_status") != "passed":
            raise ValueError(f"Silver history is not complete and gate-passed: {run}")
        days = {str(row.get("operating_date") or row.get("snapshot_date"))
                for row in report["daily_summaries"]}
        if not days or "None" in days:
            raise ValueError(f"Missing Silver operating dates: {run}")
        runs.append((run, days))
    return runs


def history_selection(history: Path, new: Path) -> tuple[list[Path], set[str]]:
    selected, dates = [], set()
    for run, days in silver_runs(history):
        if dates & days:
            raise ValueError(f"Duplicate historical Silver authority: {sorted(dates & days)}")
        selected.append(run)
        dates.update(days)
    for run, days in silver_runs(new):
        if days <= dates:
            continue  # Reruns keep the existing authoritative day.
        if dates & days:
            raise ValueError(f"New Silver batch partially overlaps history: {run}")
        selected.append(run)
        dates.update(days)
    return selected, dates


def prepare(history: Path, new: Path, view: Path, dates_file: Path) -> None:
    selected, dates = history_selection(history, new)
    if len({run.name for run in selected}) != len(selected):
        raise ValueError("Silver run names must be unique across history and new inputs.")
    view.mkdir(parents=True, exist_ok=False)
    for run in selected:
        (view / run.name).symlink_to(run.resolve(), target_is_directory=True)
    write_json(dates_file, sorted(dates))
    print(f"Gold will include {len(dates)} historical and new operating dates.", flush=True)


def sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def publish(root: Path, destination: Path) -> None:
    gold = root / "gold/kol/runs/prod-gold"
    manifest = read_json(gold / "gold_manifest.json")
    report = read_json(root / "metadata/run.json")
    if manifest.get("status") != "complete" or manifest.get("gate_status") != "passed" or report["status"] != "complete":
        raise ValueError("Only a complete, gate-passed and GUI-tested production run can extend history.")
    # Validate every Bronze addition before writing; retain already published dates.
    additions, manifests = [], []
    for layer in ("netex", "siri_et"):
        relative = Path("bronze") / layer / "kol"
        incoming_path = root / relative / "manifest.json"
        if not incoming_path.is_file() or (root / "bronze").is_symlink():
            continue  # A manual downstream-only build has no new Bronze inputs.
        incoming = read_json(incoming_path)
        target_manifest = destination / relative / "manifest.json"
        existing = read_json(target_manifest) if target_manifest.exists() else {**incoming, "records": []}
        records = {row["target_date"]: row for row in existing["records"]}
        for record in incoming["records"]:
            if record.get("status") != "downloaded":
                raise ValueError(f"Cannot publish unsuccessful Bronze date: {record['target_date']}")
            source = Path(record["file"]).resolve()
            target = destination / source.relative_to(root.resolve())
            digest = sha256(source)
            if digest != record["sha256"]:
                raise ValueError(f"Bronze checksum mismatch: {source}")
            previous = records.get(record["target_date"])
            if previous and previous.get("status") == "downloaded":
                if sha256(Path(previous["file"])) != previous["sha256"]:
                    raise ValueError(f"Historical Bronze checksum mismatch: {previous['file']}")
                continue
            if target.exists() and sha256(target) != digest:
                raise ValueError(f"Existing Bronze file differs: {target}")
            additions.append((source, target))
            records[record["target_date"]] = {**record, "file": str(target)}
        manifests.append((target_manifest, {**existing, "records": sorted(records.values(), key=lambda row: row["target_date"])}))

    silver_destination = destination / "silver/kol/runs"
    new_silver = root / "silver/kol/runs"
    new_links = []
    if new_silver.is_dir():
        selected, _ = history_selection(silver_destination, new_silver)
        for run in selected:
            if run.parent == new_silver:
                target = silver_destination / run.name
                if target.exists() and target.resolve() != run.resolve():
                    raise ValueError(f"Silver history path already exists: {target}")
                new_links.append((run, target))

    for source, target in additions:
        if target.exists():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        try:
            shutil.copyfile(source, temporary)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
    for path, payload in manifests:
        write_json(path, payload)
    for source, target in new_links:
        if not target.exists():
            target.symlink_to(source.resolve(), target_is_directory=True)
    gold_link = destination / "gold/kol/runs" / f"prod-{root.name}"
    gold_link.parent.mkdir(parents=True, exist_ok=True)
    if not gold_link.exists():
        gold_link.symlink_to(gold.resolve(), target_is_directory=True)
    write_json(destination / "metadata/production_runs" / f"{root.name}.json", {
        "run_root": str(root), "gold": str(gold_link),
        "published_operating_dates": manifest["published_operating_dates"],
        "staging_runs": str(root / "staging/netex/kol/runs"),
    })
    print(f"Saved daily inputs alongside existing history under {destination}.", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    prepare_parser = sub.add_parser("prepare")
    for name in ("history", "new", "view", "dates-file"):
        prepare_parser.add_argument(f"--{name}", type=Path, required=True)
    publish_parser = sub.add_parser("publish")
    publish_parser.add_argument("--root", type=Path, required=True)
    publish_parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(args.history, args.new, args.view, args.dates_file)
    else:
        publish(args.root.resolve(), args.destination.resolve())


if __name__ == "__main__":
    main()
