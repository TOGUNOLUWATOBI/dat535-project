"""Input selection and provenance binding for the DAT535 Silver layer."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from . import (
    ALLOWED_QUARTERLY_RUN_NAMES,
    DEFAULT_NETEX_STAGING_RUNS_DIR,
    DEFAULT_PROVIDER,
    DEFAULT_SIRI_ET_BRONZE_DIR,
    EXCLUDED_RUN_PREFIXES,
)

DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class SelectedSnapshotInput:
    """Provenance record binding a target operating/snapshot date to inputs."""

    operating_date: str
    snapshot_date: str
    provider: str
    publication_id: str
    source_generation: str | None
    source_zip: str | None
    source_zip_sha256: str | None
    publication_date: str | None
    xml_publication_timestamp: str | None
    staging_run: str
    staging_run_dir: str
    siri_source_file: str | None
    siri_source_sha256: str | None
    siri_file_available: bool
    selection_status: str
    issue_codes: tuple[str, ...]


@dataclass(frozen=True)
class InputSelectionResult:
    """Full selection summary across NeTEx staging runs and SIRI ET files."""

    provider: str
    requested_dates: tuple[str, ...]
    selected_staging_runs: tuple[str, ...]
    ignored_run_dirs: tuple[str, ...]
    siri_parquet_files: tuple[str, ...]
    records: tuple[SelectedSnapshotInput, ...]
    hard_failures: tuple[str, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "requested_dates": list(self.requested_dates),
            "selected_staging_runs": list(self.selected_staging_runs),
            "ignored_run_dirs": list(self.ignored_run_dirs),
            "siri_parquet_files": list(self.siri_parquet_files),
            "records": [
                {**asdict(r), "issue_codes": list(r.issue_codes)}
                for r in self.records
            ],
            "hard_failures": list(self.hard_failures),
            "warnings": list(self.warnings),
        }


def sha256_file(path: Path) -> str:
    """Compute hex SHA-256 digest of a file using bounded chunks."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_iso_date_str(value: str) -> str:
    """Validate YYYY-MM-DD date string format."""
    text = value.strip()
    if not DATE_PATTERN.match(text):
        raise ValueError(f"Invalid date format (expected YYYY-MM-DD): {value!r}")
    date.fromisoformat(text)
    return text


def discover_staging_runs(
    staging_runs_dir: Path = DEFAULT_NETEX_STAGING_RUNS_DIR,
    allowed_runs: Iterable[str] = ALLOWED_QUARTERLY_RUN_NAMES,
    excluded_prefixes: Iterable[str] = EXCLUDED_RUN_PREFIXES,
) -> tuple[list[tuple[str, Path, dict[str, Any]]], list[str]]:
    """Discover allowed quarterly NeTEx staging runs and ignore excluded/other dirs."""
    if not staging_runs_dir.exists() or not staging_runs_dir.is_dir():
        raise FileNotFoundError(
            f"NeTEx staging runs directory not found: {staging_runs_dir}"
        )

    allowed_set = set(allowed_runs)
    excluded_tuple = tuple(excluded_prefixes)

    selected: list[tuple[str, Path, dict[str, Any]]] = []
    ignored: list[str] = []

    for child in sorted(staging_runs_dir.iterdir(), key=lambda p: p.name):
        if not child.is_dir() or child.name.startswith("."):
            continue
        if child.name.startswith(excluded_tuple) or child.name not in allowed_set:
            ignored.append(child.name)
            continue

        manifest_path = child / "staging_manifest.json"
        if not manifest_path.is_file():
            raise FileNotFoundError(
                f"Missing staging_manifest.json in run {child.name}: {manifest_path}"
            )
        with manifest_path.open("r", encoding="utf-8") as fh:
            manifest = json.load(fh)

        if manifest.get("status") != "complete":
            raise RuntimeError(
                f"Staging run {child.name} status is {manifest.get('status')!r}, expected 'complete'"
            )
        total_failures = int(manifest.get("total_failures", 0) or 0)
        if total_failures != 0:
            raise RuntimeError(
                f"Staging run {child.name} has non-zero total_failures={total_failures}"
            )

        selected.append((child.name, child, manifest))

    return selected, ignored


def _extract_snapshot_rows_from_run(
    run_name: str,
    run_dir: Path,
    manifest: dict[str, Any],
    provider: str,
) -> list[dict[str, Any]]:
    """Extract per-snapshot provenance rows from a staging run's Parquet or manifest."""
    rows_by_date: dict[str, dict[str, Any]] = {}

    cf_dir = run_dir / "composite_frames"
    parquet_files = sorted(cf_dir.glob("*.parquet")) if cf_dir.is_dir() else []

    if parquet_files:
        # Prefer reading lightweight metadata directly via pyarrow if installed,
        # falling back to manifest/row scan if pyarrow is absent.
        try:
            import pyarrow.parquet as pq  # type: ignore

            cols = [
                "snapshot_date",
                "provider",
                "source_zip",
                "source_zip_sha256",
                "source_generation",
                "publication_date",
                "xml_publication_timestamp",
            ]
            for pf in parquet_files:
                table = pq.read_table(pf, columns=[c for c in cols if c in pq.read_schema(pf).names])
                py_dict = table.to_pydict()
                snap_vals = py_dict.get("snapshot_date", [])
                n = len(snap_vals)
                for i in range(n):
                    s_date = snap_vals[i]
                    if not s_date:
                        continue
                    row_prov = py_dict.get("provider", [provider] * n)[i] or provider
                    if row_prov.upper() != provider.upper():
                        continue
                    existing = rows_by_date.get(s_date)
                    candidate = {
                        "snapshot_date": str(s_date),
                        "provider": str(row_prov),
                        "source_zip": py_dict.get("source_zip", [None] * n)[i],
                        "source_zip_sha256": py_dict.get("source_zip_sha256", [None] * n)[i],
                        "source_generation": (
                            str(py_dict["source_generation"][i])
                            if py_dict.get("source_generation", [None] * n)[i] is not None
                            else None
                        ),
                        "publication_date": (
                            str(py_dict["publication_date"][i])
                            if py_dict.get("publication_date", [None] * n)[i] is not None
                            else None
                        ),
                        "xml_publication_timestamp": (
                            str(py_dict["xml_publication_timestamp"][i])
                            if py_dict.get("xml_publication_timestamp", [None] * n)[i] is not None
                            else None
                        ),
                        "staging_run": run_name,
                        "staging_run_dir": str(run_dir),
                    }
                    if existing is None:
                        rows_by_date[s_date] = candidate
                    else:
                        # Keep earliest non-null xml_publication_timestamp if multiple frames exist
                        if not existing.get("xml_publication_timestamp") and candidate.get(
                            "xml_publication_timestamp"
                        ):
                            existing["xml_publication_timestamp"] = candidate[
                                "xml_publication_timestamp"
                            ]
        except ImportError:
            pass

    if not rows_by_date:
        for d in manifest.get("requested_dates", []) or []:
            d_str = str(d)
            rows_by_date[d_str] = {
                "snapshot_date": d_str,
                "provider": provider,
                "source_zip": None,
                "source_zip_sha256": None,
                "source_generation": None,
                "publication_date": None,
                "xml_publication_timestamp": None,
                "staging_run": run_name,
                "staging_run_dir": str(run_dir),
            }

    return list(rows_by_date.values())


def _build_publication_id(
    provider: str,
    snapshot_date: str,
    source_generation: str | None,
    source_zip_sha256: str | None,
) -> str:
    """Build a deterministic publication identifier without mutating source IDs."""
    gen_part = source_generation or "gen-unknown"
    hash_part = (source_zip_sha256 or "hash-unknown")[:12]
    return f"{provider}:{snapshot_date}:g{gen_part}:{hash_part}"


def discover_siri_et_files(
    siri_et_dir: Path = DEFAULT_SIRI_ET_BRONZE_DIR,
    provider: str = DEFAULT_PROVIDER,
) -> dict[str, tuple[Path, str]]:
    """Map operating dates (YYYY-MM-DD) to SIRI ET Parquet file path and SHA-256."""
    if not siri_et_dir.exists() or not siri_et_dir.is_dir():
        raise FileNotFoundError(
            f"SIRI ET Bronze directory not found: {siri_et_dir}"
        )

    date_re = re.compile(r"(?:^|[_-])(\d{4}-\d{2}-\d{2})\.parquet$", re.IGNORECASE)
    mapping: dict[str, tuple[Path, str]] = {}

    for pf in sorted(siri_et_dir.rglob("*.parquet"), key=lambda p: str(p)):
        if not pf.is_file() or pf.name == "manifest.json":
            continue
        m = date_re.search(pf.name)
        if not m:
            continue
        op_date = m.group(1)
        if op_date in mapping:
            raise RuntimeError(
                f"Duplicate SIRI ET Parquet file found for {provider} on {op_date}: "
                f"{mapping[op_date][0]} vs {pf}"
            )
        mapping[op_date] = (pf, sha256_file(pf))

    return mapping


def select_inputs(
    dates: Iterable[str] | None = None,
    provider: str = DEFAULT_PROVIDER,
    staging_runs_dir: Path = DEFAULT_NETEX_STAGING_RUNS_DIR,
    siri_et_dir: Path = DEFAULT_SIRI_ET_BRONZE_DIR,
) -> InputSelectionResult:
    """Resolve and verify NeTEx Staging snapshots and SIRI ET files for target dates."""
    selected_runs, ignored_runs = discover_staging_runs(staging_runs_dir=staging_runs_dir)
    siri_map = discover_siri_et_files(siri_et_dir=siri_et_dir, provider=provider)

    snapshots_by_date: dict[str, list[dict[str, Any]]] = {}
    for run_name, run_dir, manifest in selected_runs:
        for item in _extract_snapshot_rows_from_run(run_name, run_dir, manifest, provider):
            snapshots_by_date.setdefault(item["snapshot_date"], []).append(item)

    if dates is not None:
        requested_dates = tuple(sorted({parse_iso_date_str(d) for d in dates if str(d).strip()}))
    else:
        requested_dates = tuple(sorted(snapshots_by_date.keys()))

    hard_failures: list[str] = []
    warnings: list[str] = []
    records: list[SelectedSnapshotInput] = []

    for op_date in requested_dates:
        candidates = snapshots_by_date.get(op_date, [])
        issues: list[str] = []

        if not candidates:
            issues.append("missing_netex_snapshot")
            hard_failures.append(f"{op_date}: missing_netex_snapshot")
            siri_entry = siri_map.get(op_date)
            records.append(
                SelectedSnapshotInput(
                    operating_date=op_date,
                    snapshot_date=op_date,
                    provider=provider,
                    publication_id="",
                    source_generation=None,
                    source_zip=None,
                    source_zip_sha256=None,
                    publication_date=None,
                    xml_publication_timestamp=None,
                    staging_run="",
                    staging_run_dir="",
                    siri_source_file=str(siri_entry[0]) if siri_entry else None,
                    siri_source_sha256=siri_entry[1] if siri_entry else None,
                    siri_file_available=siri_entry is not None,
                    selection_status="invalid_missing_netex",
                    issue_codes=tuple(issues),
                )
            )
            continue

        if len(candidates) > 1:
            issues.append("duplicate_snapshot_across_runs")
            hard_failures.append(
                f"{op_date}: duplicate_snapshot_across_runs "
                f"({[c['staging_run'] for c in candidates]})"
            )

        chosen = candidates[0]
        pub_id = _build_publication_id(
            provider=provider,
            snapshot_date=chosen["snapshot_date"],
            source_generation=chosen.get("source_generation"),
            source_zip_sha256=chosen.get("source_zip_sha256"),
        )

        siri_entry = siri_map.get(op_date)
        siri_available = siri_entry is not None
        if not siri_available:
            issues.append("missing_siri_et_file")
            warnings.append(f"{op_date}: missing_siri_et_file")

        if any(code == "duplicate_snapshot_across_runs" for code in issues):
            status = "duplicate_snapshot_error"
        elif not siri_available:
            status = "selected_missing_siri_et"
        else:
            status = "selected_verified"

        records.append(
            SelectedSnapshotInput(
                operating_date=op_date,
                snapshot_date=chosen["snapshot_date"],
                provider=provider,
                publication_id=pub_id,
                source_generation=chosen.get("source_generation"),
                source_zip=chosen.get("source_zip"),
                source_zip_sha256=chosen.get("source_zip_sha256"),
                publication_date=chosen.get("publication_date"),
                xml_publication_timestamp=chosen.get("xml_publication_timestamp"),
                staging_run=chosen["staging_run"],
                staging_run_dir=chosen["staging_run_dir"],
                siri_source_file=str(siri_entry[0]) if siri_entry else None,
                siri_source_sha256=siri_entry[1] if siri_entry else None,
                siri_file_available=siri_available,
                selection_status=status,
                issue_codes=tuple(issues),
            )
        )

    return InputSelectionResult(
        provider=provider,
        requested_dates=requested_dates,
        selected_staging_runs=tuple(name for name, _, _ in selected_runs),
        ignored_run_dirs=tuple(ignored_runs),
        siri_parquet_files=tuple(str(p) for p, _ in sorted(siri_map.values(), key=lambda x: str(x[0]))),
        records=tuple(records),
        hard_failures=tuple(hard_failures),
        warnings=tuple(warnings),
    )


def write_selection_report(result: InputSelectionResult, output_path: Path) -> Path:
    """Write input selection audit report as formatted JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        **result.to_dict(),
    }
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    return output_path