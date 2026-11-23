"""Gold analytical layer contract and constants for Kolumbus (KOL)."""

from __future__ import annotations

GOLD_SCHEMA_VERSION = "0.1.0-alpha1"
METRIC_POLICY_VERSION = "0.1.0-alpha1"

DEFAULT_PROVIDER = "KOL"
DEFAULT_TIMEZONE = "Europe/Oslo"
DEFAULT_SILVER_RUNS_DIR = "/srv/dat535/data/silver/kol/runs"
DEFAULT_GOLD_RUNS_DIR = "/srv/dat535/data/gold/kol/runs"

DEFAULT_SHUFFLE_PARTITIONS = 4
DEFAULT_PERCENTILE_ACCURACY = 10000

# Punctuality and delay thresholds (seconds)
DEPARTURE_EARLY_MAX_EXCLUSIVE_SEC = 0.0
DEPARTURE_WITHIN_WINDOW_MAX_SEC = 300.0
ARRIVAL_WITHIN_LIMIT_MAX_SEC = 300.0
SEVERE_DELAY_MIN_EXCLUSIVE_SEC = 900.0

# Timetable agreement and timestamp identity tolerances (seconds)
TIMESTAMP_IDENTITY_TOLERANCE_SEC = 0.001
BUSINESS_AGREEMENT_TOLERANCE_SEC = 0.0

# Initial 9 Gold output tables
GOLD_TABLES = (
    "journey_performance",
    "daily_line_service",
    "daily_line_punctuality",
    "daily_stop_punctuality",
    "daily_network_punctuality",
    "daily_data_quality",
    "period_line_punctuality",
    "period_network_punctuality",
    "period_line_service",
)

# Study windows for 2026 (winter-2026 is explicitly Jan-Feb 2026 within this study)
SEASON_WINDOWS_2026 = {
    "winter-2026": ("2026-01-01", "2026-02-28"),
    "spring-2026": ("2026-03-01", "2026-05-31"),
    "summer-2026": ("2026-06-01", "2026-08-31"),
    "autumn-2026": ("2026-09-01", "2026-11-30"),
}

PERIOD_TYPES = ("month", "season")
EVENT_TYPES = ("arrival", "departure")
BASELINES = ("et_aimed", "netex_planned")

JOURNEY_CANCELLATION_STATES = (
    "cancelled",
    "not_cancelled",
    "unknown",
    "conflicting",
    "partially_unknown_true",
    "partially_unknown_false",
)

__all__ = [
    "ARRIVAL_WITHIN_LIMIT_MAX_SEC",
    "BASELINES",
    "BUSINESS_AGREEMENT_TOLERANCE_SEC",
    "DEFAULT_GOLD_RUNS_DIR",
    "DEFAULT_PERCENTILE_ACCURACY",
    "DEFAULT_PROVIDER",
    "DEFAULT_SHUFFLE_PARTITIONS",
    "DEFAULT_SILVER_RUNS_DIR",
    "DEFAULT_TIMEZONE",
    "DEPARTURE_EARLY_MAX_EXCLUSIVE_SEC",
    "DEPARTURE_WITHIN_WINDOW_MAX_SEC",
    "EVENT_TYPES",
    "GOLD_SCHEMA_VERSION",
    "GOLD_TABLES",
    "JOURNEY_CANCELLATION_STATES",
    "METRIC_POLICY_VERSION",
    "PERIOD_TYPES",
    "SEASON_WINDOWS_2026",
    "SEVERE_DELAY_MIN_EXCLUSIVE_SEC",
    "TIMESTAMP_IDENTITY_TOLERANCE_SEC",
]
