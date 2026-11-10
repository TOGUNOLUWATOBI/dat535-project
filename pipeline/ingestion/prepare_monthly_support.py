"""Find validated earlier plans that cover every date of each requested month.

Try the first-of-month export, then an earlier export if that source is broken.
Failed raw attempts are preserved. Sources are never renamed to another date.
"""

import argparse
from datetime import timedelta
import json
from pathlib import Path

from .download_entur_netex import parse_month
from pipeline.common import write_json
from .generate_provider_dataset import generate
from pipeline.processing.bronze.staging_sources import load_snapshot_sources
from pipeline.processing.windows import month_bounds
from pipeline.quality.source_checks import frame_validity
from pipeline.storage.zip_bronze import with_snapshot_xml


def existing_source(root, day):
    """Manifest of an already validated export for ``day`` under ``root``, or None.

    A previously failed attempt for the same day is preserved and refused, so
    the caller moves on to another candidate date instead of retrying it.
    """
    failures = []
    for path in sorted(root.rglob("manifest.json")):
        try:
            manifest = json.loads(path.read_text())
            rows = manifest.get("snapshots", [manifest])
            if any(r.get("snapshot_date") == str(day) and r.get("status") in
                   ("validated", "validated_with_warnings", "complete", "complete_with_warnings") for r in rows):
                return path
            if any(r.get("snapshot_date") == str(day) and r.get("status") == "failed" for r in rows):
                failures.append(str(path))
        except (OSError, ValueError):
            continue
    if failures:
        raise RuntimeError(f"Previously failed source is preserved: {failures[0]}")
    return None


@with_snapshot_xml
def covers_month(manifest, day, first, last, providers):
    """True if every provider's shared calendar validity spans the whole month."""
    selection = load_snapshot_sources(manifest, day)
    if set(selection["providers"]) != set(providers):
        raise ValueError("Support source has a different provider selection")
    for provider in providers:
        shared = [f for f in selection["files"]
                  if Path(f["source_member"]).name.upper() == f"_{provider}_SHARED_DATA.XML"]
        if len(shared) != 1:
            raise ValueError(f"Missing unique shared calendar file for {provider}")
        start, end = frame_validity(shared[0]["source_file"])
        if (start and start > str(first)) or (end and end < str(last)):
            return False
    return True


def prepare(root, first_month, last_month, providers, remove_national=False, excluded_dates=()):
    """Select (and if needed download) one full-month support export per month; return the plan path."""
    if first_month > last_month:
        raise ValueError("The first month must not follow the last month")
    if not providers or len(set(providers)) != len(providers):
        raise ValueError("Select a nonempty, unique provider list")
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    plan = {"schema_version": 1, "status": "in_progress", "providers": providers, "jobs": [], "attempts": []}
    plan_path = root / "calendar_support_plan.json"
    month = first_month
    missing = []
    while month <= last_month:
        first, last = month_bounds(month)
        previous = first - timedelta(days=1)
        # The 14th keeps auxiliary observations distinct from the original 15th sample.
        candidates = [first, previous.replace(day=14), first + timedelta(days=1), first + timedelta(days=2)]
        found = False
        for day in candidates:
            entry = {"month": str(first)[:7], "source_date": str(day)}
            try:
                if str(day) in {str(d) for d in excluded_dates}:
                    raise ValueError("This date belongs to the primary observation sample")
                manifest = existing_source(root, day)
                if manifest is None:
                    output = root / "additional" / str(day)
                    if output.exists():
                        raise RuntimeError("Earlier failed attempt retained; choosing another source")
                    generate(providers, [day], output, remove_national=remove_national, zip_only=True)
                    manifest = output / "manifest.json"
                if not covers_month(manifest, day, first, last, providers):
                    raise ValueError("Source validity does not cover the entire requested month")
                plan["jobs"].append({"manifest": str(manifest.relative_to(root)), "snapshot_date": str(day),
                                     "service_from": str(first), "service_to": str(last)})
                entry["status"] = "selected"
                found = True
                print(f"FULL-MONTH SOURCE: {first:%Y-%m} ← {day}", flush=True)
            except Exception as error:
                entry.update(status="rejected", error=f"{type(error).__name__}: {error}")
                print(f"Source refused: {first:%Y-%m} ← {day}: {entry['error']}", flush=True)
            plan["attempts"].append(entry)
            write_json(plan_path, plan)
            if found:
                break
        if not found:
            missing.append(str(first)[:7])
        month = last + timedelta(days=1)
    plan.update(status="failed" if missing else "complete", missing_months=missing)
    write_json(plan_path, plan)
    if missing:
        raise RuntimeError(f"No validated full-month support for {missing}; see {plan_path}")
    print(f"COMPLETE: {plan_path}", flush=True)
    return plan_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--from-month", type=parse_month, default=parse_month("2026-01"))
    parser.add_argument("--to-month", type=parse_month, default=parse_month("2026-09"))
    parser.add_argument("--providers", nargs="+", default=["KOL", "AKT"])
    parser.add_argument("--remove-national-after-validation", action="store_true")
    args = parser.parse_args()
    prepare(args.output, args.from_month, args.to_month, args.providers, args.remove_national_after_validation)


if __name__ == "__main__":
    main()
