"""Silver layer package for DAT535 NeTEx and SIRI ET reconciliation."""

from __future__ import annotations

from pathlib import Path
from typing import Final

SILVER_VERSION: Final[str] = "0.1.0-alpha1"
DEFAULT_PROVIDER: Final[str] = "KOL"
DEFAULT_TIMEZONE: Final[str] = "Europe/Oslo"

DEFAULT_DATA_ROOT: Final[Path] = Path("/srv/dat535/data")
DEFAULT_NETEX_STAGING_RUNS_DIR: Final[Path] = (
    DEFAULT_DATA_ROOT / "staging" / "netex" / "kol" / "runs"
)
DEFAULT_SIRI_ET_BRONZE_DIR: Final[Path] = (
    DEFAULT_DATA_ROOT / "bronze" / "siri_et" / "kol"
)
DEFAULT_SILVER_RUNS_DIR: Final[Path] = (
    DEFAULT_DATA_ROOT / "silver" / "kol" / "runs"
)

ALLOWED_QUARTERLY_RUN_NAMES: Final[tuple[str, ...]] = (
    "batch-2026-q1",
    "batch-2026-q2",
    "batch-2026-q3",
    "batch-2026-q4",
)

EXCLUDED_RUN_PREFIXES: Final[tuple[str, ...]] = (
    "diagnostic-",
    "bench-",
)

SILVER_TABLES: Final[tuple[str, ...]] = (
    "lines",
    "routes",
    "journey_patterns",
    "journey_pattern_stops",
    "journeys",
    "service_calendar",
    "journey_service_dates",
    "netex_stop_calls",
    "siri_et_stop_calls",
    "reconciled_stop_calls",
)

RECONCILIATION_STATUSES: Final[tuple[str, ...]] = (
    "matched",
    "planned_only",
    "reported_only",
    "ambiguous",
    "plan_unresolved",
)

CALENDAR_STATES: Final[tuple[str, ...]] = (
    "active",
    "inactive",
    "unresolved",
)

JOURNEY_SERVICE_STATUSES: Final[tuple[str, ...]] = (
    "scheduled",
    "not_scheduled",
    "unresolved",
)

TIME_KINDS: Final[tuple[str, ...]] = (
    "recorded",
    "estimated",
    "missing",
    "unresolved",
)

__all__ = [
    "ALLOWED_QUARTERLY_RUN_NAMES",
    "CALENDAR_STATES",
    "DEFAULT_DATA_ROOT",
    "DEFAULT_NETEX_STAGING_RUNS_DIR",
    "DEFAULT_PROVIDER",
    "DEFAULT_SILVER_RUNS_DIR",
    "DEFAULT_SIRI_ET_BRONZE_DIR",
    "DEFAULT_TIMEZONE",
    "EXCLUDED_RUN_PREFIXES",
    "JOURNEY_SERVICE_STATUSES",
    "RECONCILIATION_STATUSES",
    "SILVER_TABLES",
    "SILVER_VERSION",
    "TIME_KINDS",
]