"""Collect one new KOL + AKT archive snapshot, locally or from GitHub Actions.

The default source date is yesterday in Europe/Oslo. Each execution gets a new
output directory; original archives, XML and failed downloads are preserved.
"""

import argparse
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from http.client import IncompleteRead
import os
from pathlib import Path
import re
import sys
import time
import urllib.error
from uuid import uuid4
from zoneinfo import ZoneInfo

from .download_entur_netex import download, file_checksums, make_url, source_metadata
from .generate_provider_dataset import extract_snapshot, save_json, validate_snapshot


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def previous_source_day(now=None):
    now = now or datetime.now(timezone.utc)
    return now.astimezone(ZoneInfo("Europe/Oslo")).date() - timedelta(days=1)


def retry_delay(error, attempt):
    """Bound backoff, including a server's Retry-After, to at most 60 seconds."""
    value = error.headers.get("Retry-After") if isinstance(error, urllib.error.HTTPError) and error.headers else None
    if value:
        try:
            seconds = float(value)
        except ValueError:
            try:
                seconds = (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                seconds = 0
        return min(60, max(0, seconds))
    return min(60, 2 ** attempt)


def request_with_retries(operation, attempts, record_failure):
    for attempt in range(1, attempts + 1):
        try:
            return operation(attempt)
        except (urllib.error.URLError, TimeoutError, ConnectionError, IncompleteRead) as error:
            transient = not isinstance(error, urllib.error.HTTPError) or error.code in (408, 429) or 500 <= error.code < 600
            record_failure(attempt, error)
            if isinstance(error, urllib.error.HTTPError):
                error.close()
            if not transient or attempt == attempts:
                raise
            delay = retry_delay(error, attempt)
            print(f"Request failed ({error}); retrying in {delay:g}s", flush=True)
            time.sleep(delay)


def ingest_snapshot(day, output, providers=("KOL", "AKT"), attempts=3, check_only=False):
    """Acquire, filter and validate a single source without changing older data."""
    providers = list(dict.fromkeys(p.upper() for p in providers))
    if not providers or any(not re.fullmatch(r"[A-Z]{3}", p) for p in providers):
        raise ValueError("Provider codes must have exactly three letters")
    if not 1 <= attempts <= 5:
        raise ValueError("Request attempts must be between 1 and 5")
    if day > datetime.now(ZoneInfo("Europe/Oslo")).date():
        raise ValueError("Cannot ingest a future source date")
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    metadata_dir = output / "metadata"
    metadata_dir.mkdir()
    manifest_path = metadata_dir / "manifest.json"
    manifest = {
        "status": "running", "started_at_utc": utc_now(),
        "snapshot_date": day.isoformat(), "providers": providers,
        "check_only": check_only, "request_failures": [],
        "github_run_id": os.environ.get("GITHUB_RUN_ID"),
        "github_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "github_event_name": os.environ.get("GITHUB_EVENT_NAME"),
        "runner_name": os.environ.get("RUNNER_NAME"),
        "git_commit": os.environ.get("GITHUB_SHA"),
    }
    save_json(manifest_path, manifest)

    def record_failure(phase):
        def record(attempt, error):
            manifest["request_failures"].append({
                "phase": phase, "attempt": attempt, "at_utc": utc_now(),
                "error": f"{type(error).__name__}: {error}",
            })
            save_json(manifest_path, manifest)
        return record

    try:
        key = day.strftime("%Y%m%d")
        print(f"Reading Entur source metadata for {day}", flush=True)
        identity = request_with_retries(lambda _: source_metadata(key), attempts, record_failure("metadata"))
        save_json(metadata_dir / "source_metadata.json", identity)
        url = make_url(key) + "&generation=" + str(int(identity["generation"]))
        manifest.update(source_url=url, source_generation=identity["generation"],
                        source_md5_base64=identity["md5Hash"], source_bytes=int(identity["size"]),
                        source_created_at=identity.get("timeCreated"))
        save_json(manifest_path, manifest)
        if check_only:
            manifest["status"] = "source_available"
        else:
            def acquire(attempt):
                # Separate attempts retain partial bytes and never block a retry
                # because an earlier download left a .part file behind.
                raw = output / "national" / f"attempt-{attempt:02d}" / f"netex_{key}.zip"
                manifest["national_zip"] = str(raw.relative_to(output))
                save_json(manifest_path, manifest)
                return download(url, raw, identity)

            manifest["source_sha256"] = request_with_retries(acquire, attempts, record_failure("download"))
            raw = output / manifest["national_zip"]
            directory = output / "xml" / day.isoformat()
            compact = output / "zip" / f"{'_'.join(providers)}_netex_{key}.zip"
            compact.parent.mkdir()
            print("Extracting original provider XML and national stops", flush=True)
            lineage = extract_snapshot(raw, directory, compact, providers)
            for item in lineage:
                item["xml_path"] = str(Path(item["xml_path"]).relative_to(output))
            save_json(metadata_dir / "lineage.json", lineage)
            print(f"Validating {len(lineage)} XML files and provider references", flush=True)
            report = validate_snapshot(directory, lineage, providers, day)
            save_json(metadata_dir / "validation.json", report)
            if report["status"] not in ("passed", "passed_with_warnings"):
                raise RuntimeError("Provider reference validation failed; see metadata/validation.json")
            manifest.update(
                status="complete_with_warnings" if report["status"] == "passed_with_warnings" else "complete",
                combined_zip=str(compact.relative_to(output)),
                combined_zip_sha256=file_checksums(compact)[0],
                xml_directory=str(directory.relative_to(output)),
                xml_files=report["xml_files"], provider_xml_bytes=report["provider_xml_bytes"],
                total_xml_bytes=report["total_xml_bytes"],
                duplicate_id_version_occurrences=sum(report["duplicate_provider_id_version_pairs"].values()),
            )
            # Identity is content-based, so downstream storage can recognize the
            # same payload even if another run or archive date observes it again.
            manifest["ingestion_id"] = manifest["source_sha256"] + ":" + ",".join(sorted(providers))
        manifest["completed_at_utc"] = utc_now()
        save_json(manifest_path, manifest)
        print(f"{manifest['status']}: {manifest_path}", flush=True)
        return manifest
    except Exception as error:
        manifest.update(status="failed", completed_at_utc=utc_now(), error=f"{type(error).__name__}: {error}")
        save_json(manifest_path, manifest)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, default=None, help="Source day YYYY-MM-DD; default: yesterday in Oslo")
    parser.add_argument("--output", type=Path, help="New output directory; existing directories are never overwritten")
    parser.add_argument("--providers", nargs="+", default=["KOL", "AKT"])
    parser.add_argument("--attempts", type=int, choices=range(1, 6), default=3)
    parser.add_argument("--check-only", action="store_true", help="Check source metadata without downloading the national ZIP")
    args = parser.parse_args()
    day = args.date or previous_source_day()
    run_name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    output = args.output or Path("data/bronze/entur") / run_name
    try:
        ingest_snapshot(day, output, args.providers, args.attempts, args.check_only)
    except Exception as error:
        print(f"Ingestion failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
