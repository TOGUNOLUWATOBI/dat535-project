"""Gold layer for Kolumbus (KOL): constants, metric policy and small shared Spark helpers."""

from __future__ import annotations

GOLD_SCHEMA_VERSION = "0.1.0-alpha1"
METRIC_POLICY_VERSION = "0.1.0-alpha1"

DEFAULT_PROVIDER = "KOL"
DEFAULT_TIMEZONE = "Europe/Oslo"
DEFAULT_SILVER_RUNS_DIR = "/srv/dat535/data/silver/kol/runs"
DEFAULT_GOLD_RUNS_DIR = "/srv/dat535/data/gold/kol/runs"
DEFAULT_SHUFFLE_PARTITIONS = 4
DEFAULT_PERCENTILE_ACCURACY = 10000

# Punctuality policy (seconds): departures 0..300 s late are on time (earlier = early),
# arrivals up to 300 s late are on time, more than 900 s late is severe.
DEPARTURE_EARLY_MAX_EXCLUSIVE_SEC = 0.0
DEPARTURE_WITHIN_WINDOW_MAX_SEC = 300.0
ARRIVAL_WITHIN_LIMIT_MAX_SEC = 300.0
SEVERE_DELAY_MIN_EXCLUSIVE_SEC = 900.0
TIMESTAMP_IDENTITY_TOLERANCE_SEC = 0.001  # check: netex delay == et delay + (aimed - planned)
BUSINESS_AGREEMENT_TOLERANCE_SEC = 0.0    # live schedule "agrees" with the timetable

GOLD_TABLES = (
    "journey_performance", "daily_line_service", "daily_line_punctuality", "daily_stop_punctuality",
    "daily_network_punctuality", "daily_data_quality", "period_line_punctuality",
    "period_network_punctuality", "period_line_service",
)
# Study seasons (winter-2026 is January-February 2026 in this study).
SEASON_WINDOWS_2026 = {
    "winter-2026": ("2026-01-01", "2026-02-28"),
    "spring-2026": ("2026-03-01", "2026-05-31"),
    "summer-2026": ("2026-06-01", "2026-08-31"),
    "autumn-2026": ("2026-09-01", "2026-11-30"),
}
EVENT_TYPES = ("arrival", "departure")
BASELINES = ("et_aimed", "netex_planned")


# ---------------------------------------------------------------- shared Spark helpers

def count_if(cond, name: str):
    """Number of rows where `cond` is true, as a long column."""
    from pyspark.sql import functions as F
    return F.sum(F.when(cond, F.lit(1)).otherwise(F.lit(0))).cast("long").alias(name)


def ratio(num, den):
    """num / den as double, NULL when den is not positive (a rate with nothing to measure)."""
    from pyspark.sql import functions as F
    num, den = (F.col(c) if isinstance(c, str) else c for c in (num, den))
    return F.when(den > 0, num.cast("double") / den.cast("double"))


def lower_text(col: str):
    """Lower-cased string value, '' when null."""
    from pyspark.sql import functions as F
    return F.lower(F.coalesce(F.col(col).cast("string"), F.lit("")))


def add_version_columns(df, gold_run_id: str, order=("gold_schema_version", "metric_policy_version", "gold_run_id")):
    """Stamp the run ID and schema/policy versions on a table."""
    from pyspark.sql import functions as F
    values = {"gold_run_id": gold_run_id, "gold_schema_version": GOLD_SCHEMA_VERSION,
              "metric_policy_version": METRIC_POLICY_VERSION}
    for name in order:
        df = df.withColumn(name, F.lit(values[name]).cast("string"))
    return df
