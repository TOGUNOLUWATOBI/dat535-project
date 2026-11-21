"""Calendar evaluation and dated NeTEx stop-call generation for the DAT535 Silver layer."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from . import DEFAULT_PROVIDER, DEFAULT_TIMEZONE
from .netex_definitions import _load_staged_table, _match_ref_expr, _occurrence_struct_col

WEEKDAY_NAMES: tuple[str, ...] = (
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
)

TIME_RE = re.compile(r"^(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d+))?$")


def _extract_iso_date(raw: str | None) -> date | None:
    if not raw:
        return None
    text = str(raw).strip()
    if len(text) >= 10:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None
    return None


def _parse_bool_flag(raw: str | None, default: bool | None = None) -> bool | None:
    if raw is None:
        return default
    val = str(raw).strip().lower()
    if val in ("true", "1", "yes"):
        return True
    if val in ("false", "0", "no"):
        return False
    return None


def _local_clock_to_utc(
    operating_date_str: str | None,
    raw_time_str: str | None,
    raw_day_offset: str | None,
    tz_name: str = DEFAULT_TIMEZONE,
) -> tuple[str | None, str | None, int | None, str | None]:
    """Convert NeTEx local clock + day offset on operating_date into UTC ISO timestamp.

    Detects both nonexistent spring DST wall times and repeated autumn DST wall times.
    """
    if raw_time_str is None or str(raw_time_str).strip() == "":
        return None, None, None, None

    op_d = _extract_iso_date(operating_date_str)
    if op_d is None:
        return None, None, None, "invalid_operating_date"

    day_offset = 0
    if raw_day_offset is not None and str(raw_day_offset).strip() != "":
        try:
            day_offset = int(str(raw_day_offset).strip())
        except ValueError:
            return None, None, None, "invalid_day_offset"

    m = TIME_RE.match(str(raw_time_str).strip())
    if not m:
        return None, None, day_offset, "invalid_time_format"

    hours = int(m.group(1))
    minutes = int(m.group(2))
    seconds = int(m.group(3))
    frac_str = m.group(4) or ""
    micros = int(frac_str.ljust(6, "0")[:6]) if frac_str else 0

    if minutes > 59 or seconds > 59 or hours < 0:
        return None, None, day_offset, "invalid_time_components"

    extra_days, norm_hour = divmod(hours, 24)
    total_day_offset = day_offset + extra_days
    civil_date = op_d + timedelta(days=total_day_offset)

    try:
        tz = ZoneInfo(tz_name)
        naive_dt = datetime(
            civil_date.year,
            civil_date.month,
            civil_date.day,
            norm_hour,
            minutes,
            seconds,
            micros,
        )
        dt_fold0 = naive_dt.replace(tzinfo=tz, fold=0)
        dt_fold1 = naive_dt.replace(tzinfo=tz, fold=1)

        utc_fold0 = dt_fold0.astimezone(ZoneInfo("UTC"))
        utc_fold1 = dt_fold1.astimezone(ZoneInfo("UTC"))

        rt0 = utc_fold0.astimezone(tz)
        if (
            rt0.hour != norm_hour
            or rt0.minute != minutes
            or rt0.second != seconds
            or rt0.date() != civil_date
        ):
            return None, dt_fold0.isoformat(), total_day_offset, "nonexistent_dst_local_time"

        if utc_fold0 != utc_fold1:
            return None, naive_dt.isoformat(), total_day_offset, "ambiguous_dst_local_time"

        return utc_fold0.isoformat(), dt_fold0.isoformat(), total_day_offset, None
    except Exception:
        return None, None, total_day_offset, "timezone_conversion_error"


_TIME_CONV_SCHEMA = T.StructType(
    [
        T.StructField("utc_iso", T.StringType(), True),
        T.StructField("local_iso", T.StringType(), True),
        T.StructField("resolved_day_offset", T.IntegerType(), True),
        T.StructField("error_code", T.StringType(), True),
    ]
)

_local_clock_to_utc_udf = F.udf(_local_clock_to_utc, _TIME_CONV_SCHEMA)


def _eval_day_type_for_date(
    operating_date_str: str | None,
    props_list: list | None,
    assignments_list: list | None,
    frame_conditions_list: list | None,
) -> tuple[str, list[str], int, int]:
    """Evaluate a single day_type on operating_date within its publication."""
    reasons: list[str] = []
    op_d = _extract_iso_date(operating_date_str)
    if op_d is None:
        return "unresolved", ["invalid_operating_date"], 0, 0

    if frame_conditions_list:
        any_frame_known = False
        any_frame_active = False
        for cond in frame_conditions_list:
            if cond is None:
                continue
            f_from = _extract_iso_date(cond["FromDate"] if "FromDate" in cond else None)
            f_to = _extract_iso_date(cond["ToDate"] if "ToDate" in cond else None)
            f_avail = _parse_bool_flag(
                cond["IsAvailable"] if "IsAvailable" in cond else None, default=True
            )
            if f_avail is None:
                reasons.append("invalid_frame_availability_flag")
                return "unresolved", reasons, 0, 0
            in_bounds = (f_from is None or op_d >= f_from) and (f_to is None or op_d <= f_to)
            if in_bounds:
                any_frame_known = True
                if f_avail:
                    any_frame_active = True
                else:
                    reasons.append("frame_availability_excluded")
                    return "inactive", reasons, 0, 0
            else:
                any_frame_known = True
        if any_frame_known and not any_frame_active:
            reasons.append("outside_composite_frame_validity")
            return "inactive", reasons, 0, 0

    weekday_name = WEEKDAY_NAMES[op_d.weekday()]
    props_match = True
    if props_list:
        any_prop_matched = False
        for p in props_list:
            if p is None:
                continue
            unsupported_fields = [
                fld
                for fld in ("WeeksOfMonth", "MonthOfYear", "DayOfYear", "HolidayTypes", "Seasons")
                if p[fld] is not None and str(p[fld]).strip() != ""
            ]
            if unsupported_fields:
                reasons.append(f"unsupported_property_of_day:{','.join(unsupported_fields)}")
                return "unresolved", reasons, 0, 0

            dow_raw = p["DaysOfWeek"] if "DaysOfWeek" in p else None
            if dow_raw is None or str(dow_raw).strip() == "":
                any_prop_matched = True
            else:
                tokens = {t.strip() for t in str(dow_raw).split() if t.strip()}
                unknown_tokens = tokens - set(WEEKDAY_NAMES)
                if unknown_tokens:
                    reasons.append(
                        f"unknown_days_of_week_token:{','.join(sorted(unknown_tokens))}"
                    )
                    return "unresolved", reasons, 0, 0
                if weekday_name in tokens:
                    any_prop_matched = True
        props_match = any_prop_matched

    if not assignments_list:
        reasons.append("missing_day_type_assignments")
        return "unresolved", reasons, 0, 0

    date_specific_states: list[bool] = []
    period_states: list[bool] = []

    for asg in assignments_list:
        if asg is None:
            continue
        is_avail = _parse_bool_flag(
            asg["is_available_raw"] if "is_available_raw" in asg else None, default=True
        )
        if is_avail is None:
            reasons.append("invalid_assignment_is_available")
            return "unresolved", reasons, 0, 0

        kind = asg["assignment_kind"] if "assignment_kind" in asg else None
        if kind == "date":
            target_d = _extract_iso_date(asg["target_date"] if "target_date" in asg else None)
            if target_d is None:
                reasons.append("unresolved_assignment_date")
                return "unresolved", reasons, 0, 0
            if target_d == op_d:
                date_specific_states.append(is_avail)
        elif kind == "operating_day":
            target_d = _extract_iso_date(asg["target_date"] if "target_date" in asg else None)
            if target_d is None:
                reasons.append("unresolved_operating_day_ref")
                return "unresolved", reasons, 0, 0
            if target_d == op_d:
                date_specific_states.append(is_avail)
        elif kind == "operating_period":
            p_from = _extract_iso_date(asg["from_date"] if "from_date" in asg else None)
            p_to = _extract_iso_date(asg["to_date"] if "to_date" in asg else None)
            if p_from is None and p_to is None:
                reasons.append("unresolved_operating_period_ref")
                return "unresolved", reasons, 0, 0
            in_period = (p_from is None or op_d >= p_from) and (p_to is None or op_d <= p_to)
            if in_period:
                period_states.append(is_avail)
        else:
            reasons.append("unsupported_assignment_target")
            return "unresolved", reasons, 0, 0

    active_cnt = sum(1 for s in (date_specific_states + period_states) if s)
    inactive_cnt = sum(1 for s in (date_specific_states + period_states) if not s)

    if date_specific_states:
        if True in date_specific_states and False in date_specific_states:
            reasons.append("conflicting_date_specific_assignments")
            return "unresolved", reasons, active_cnt, inactive_cnt
        if False in date_specific_states:
            reasons.append("explicit_date_unavailable_override")
            return "inactive", reasons, active_cnt, inactive_cnt
        if props_list and not props_match:
            reasons.append("explicit_date_available_override_weekday_mismatch")
        else:
            reasons.append("explicit_date_available")
        return "active", reasons, active_cnt, inactive_cnt

    if not period_states:
        reasons.append("outside_all_assigned_periods")
        return "inactive", reasons, active_cnt, inactive_cnt

    if True in period_states and False in period_states:
        reasons.append("conflicting_period_assignments")
        return "unresolved", reasons, active_cnt, inactive_cnt

    if False in period_states:
        reasons.append("period_unavailable")
        return "inactive", reasons, active_cnt, inactive_cnt

    if not props_match:
        reasons.append(f"weekday_mismatch:{weekday_name}")
        return "inactive", reasons, active_cnt, inactive_cnt

    reasons.append(f"active_period_and_weekday:{weekday_name}")
    return "active", reasons, active_cnt, inactive_cnt


_CAL_EVAL_SCHEMA = T.StructType(
    [
        T.StructField("calendar_state", T.StringType(), False),
        T.StructField("reason_codes", T.ArrayType(T.StringType()), False),
        T.StructField("active_assignment_count", T.IntegerType(), False),
        T.StructField("inactive_assignment_count", T.IntegerType(), False),
    ]
)

_eval_day_type_udf = F.udf(_eval_day_type_for_date, _CAL_EVAL_SCHEMA)


def _eval_journey_availability(
    operating_date_str: str | None,
    day_type_states: list[str] | None,
    journey_conditions: list | None,
) -> tuple[str, list[str]]:
    """Combine journey-level AvailabilityConditions with evaluated day-type states."""
    reasons: list[str] = []
    op_d = _extract_iso_date(operating_date_str)
    if op_d is None:
        return "unresolved", ["invalid_operating_date"]

    if journey_conditions:
        any_valid = False
        for cond in journey_conditions:
            if cond is None:
                continue
            c_from = _extract_iso_date(cond["FromDate"] if "FromDate" in cond else None)
            c_to = _extract_iso_date(cond["ToDate"] if "ToDate" in cond else None)
            c_avail = _parse_bool_flag(
                cond["IsAvailable"] if "IsAvailable" in cond else None, default=True
            )
            if c_avail is None:
                return "unresolved", ["invalid_journey_availability_flag"]
            in_bounds = (c_from is None or op_d >= c_from) and (c_to is None or op_d <= c_to)
            if in_bounds:
                if not c_avail:
                    return "not_scheduled", ["journey_validity_excluded"]
                any_valid = True
        if not any_valid:
            return "not_scheduled", ["outside_journey_validity_conditions"]

    if not day_type_states:
        return "unresolved", ["missing_journey_day_type_refs"]

    states = [s for s in day_type_states if s is not None]
    if not states:
        return "unresolved", ["unresolved_day_type_refs"]

    if "active" in states:
        reasons.append("at_least_one_active_day_type")
        if "unresolved" in states:
            reasons.append("coexisting_unresolved_day_type")
        return "scheduled", reasons

    if "unresolved" in states:
        return "unresolved", ["unresolved_day_type_no_active_match"]

    return "not_scheduled", ["all_day_types_inactive"]


_JOURNEY_EVAL_SCHEMA = T.StructType(
    [
        T.StructField("service_status", T.StringType(), False),
        T.StructField("reason_codes", T.ArrayType(T.StringType()), False),
    ]
)

_eval_journey_udf = F.udf(_eval_journey_availability, _JOURNEY_EVAL_SCHEMA)


def build_service_calendar(
    spark: SparkSession,
    run_dirs: list[str],
    publication_lookup: DataFrame,
    provider: str = DEFAULT_PROVIDER,
) -> DataFrame:
    """Evaluate every day_type version against its publication's target operating_date."""
    target_dates_df = publication_lookup.select(
        "staging_run",
        "snapshot_date",
        "publication_id",
        "operating_date",
    ).dropDuplicates()

    raw_dts = _load_staged_table(spark, run_dirs, "day_types", publication_lookup, provider)
    raw_dta = _load_staged_table(
        spark, run_dirs, "day_type_assignments", publication_lookup, provider
    )
    raw_op = _load_staged_table(
        spark, run_dirs, "operating_periods", publication_lookup, provider
    )
    raw_od = _load_staged_table(spark, run_dirs, "operating_days", publication_lookup, provider)
    raw_cf = _load_staged_table(
        spark, run_dirs, "composite_frames", publication_lookup, provider
    )

    od_lookup = (
        raw_od.withColumn("od_id", F.col("record._id"))
        .withColumn("od_ver", F.col("record._version"))
        .withColumn("od_calendar_date", F.col("record.CalendarDate"))
        .filter(F.col("od_id").isNotNull())
        .select("publication_id", "od_id", "od_ver", "od_calendar_date")
        .dropDuplicates()
    )

    op_base = (
        raw_op.withColumn("op_id", F.col("record._id"))
        .withColumn("op_ver", F.col("record._version"))
        .withColumn("raw_from_date", F.col("record.FromDate"))
        .withColumn("raw_to_date", F.col("record.ToDate"))
        .withColumn("from_od_ref", F.col("record.FromOperatingDayRef._ref"))
        .withColumn("from_od_ver", F.col("record.FromOperatingDayRef._version"))
        .withColumn("to_od_ref", F.col("record.ToOperatingDayRef._ref"))
        .withColumn("to_od_ver", F.col("record.ToOperatingDayRef._version"))
        .filter(F.col("op_id").isNotNull())
    )

    od_from = od_lookup.select(
        F.col("publication_id").alias("_f_pub"),
        F.col("od_id").alias("_f_id"),
        F.col("od_ver").alias("_f_ver"),
        F.col("od_calendar_date").alias("_f_cal_date"),
    )
    od_to = od_lookup.select(
        F.col("publication_id").alias("_t_pub"),
        F.col("od_id").alias("_t_id"),
        F.col("od_ver").alias("_t_ver"),
        F.col("od_calendar_date").alias("_t_cal_date"),
    )

    op_resolved = (
        op_base.join(
            od_from,
            on=(
                (op_base["publication_id"] == od_from["_f_pub"])
                & _match_ref_expr(
                    op_base["from_od_ref"],
                    op_base["from_od_ver"],
                    od_from["_f_id"],
                    od_from["_f_ver"],
                )
            ),
            how="left",
        )
        .join(
            od_to,
            on=(
                (F.col("publication_id") == F.col("_t_pub"))
                & _match_ref_expr(
                    F.col("to_od_ref"),
                    F.col("to_od_ver"),
                    F.col("_t_id"),
                    F.col("_t_ver"),
                )
            ),
            how="left",
        )
        .select(
            "publication_id",
            "op_id",
            "op_ver",
            F.coalesce(F.col("raw_from_date"), F.col("_f_cal_date")).alias("from_date"),
            F.coalesce(F.col("raw_to_date"), F.col("_t_cal_date")).alias("to_date"),
        )
        .dropDuplicates()
    )

    dta_base = (
        raw_dta.withColumn("day_type_ref", F.col("record.DayTypeRef._ref"))
        .withColumn("day_type_ref_ver", F.col("record.DayTypeRef._version"))
        .withColumn("asg_date", F.col("record.Date"))
        .withColumn("op_period_ref", F.col("record.OperatingPeriodRef._ref"))
        .withColumn("op_period_ref_ver", F.col("record.OperatingPeriodRef._version"))
        .withColumn("op_day_ref", F.col("record.OperatingDayRef._ref"))
        .withColumn("op_day_ref_ver", F.col("record.OperatingDayRef._version"))
        .withColumn("is_available_raw", F.col("record.isAvailable"))
        .filter(F.col("day_type_ref").isNotNull())
    )

    dta_enriched = (
        dta_base.join(
            op_resolved,
            on=(
                (dta_base["publication_id"] == op_resolved["publication_id"])
                & _match_ref_expr(
                    dta_base["op_period_ref"],
                    dta_base["op_period_ref_ver"],
                    op_resolved["op_id"],
                    op_resolved["op_ver"],
                )
            ),
            how="left",
        )
        .join(
            od_lookup,
            on=(
                (dta_base["publication_id"] == od_lookup["publication_id"])
                & _match_ref_expr(
                    dta_base["op_day_ref"],
                    dta_base["op_day_ref_ver"],
                    od_lookup["od_id"],
                    od_lookup["od_ver"],
                )
            ),
            how="left",
        )
        .select(
            dta_base["publication_id"].alias("publication_id"),
            dta_base["day_type_ref"].alias("day_type_ref"),
            dta_base["day_type_ref_ver"].alias("day_type_ref_ver"),
            F.when(dta_base["asg_date"].isNotNull(), F.lit("date"))
            .when(dta_base["op_day_ref"].isNotNull(), F.lit("operating_day"))
            .when(dta_base["op_period_ref"].isNotNull(), F.lit("operating_period"))
            .otherwise(F.lit("unknown"))
            .alias("assignment_kind"),
            F.coalesce(dta_base["asg_date"], od_lookup["od_calendar_date"]).alias("target_date"),
            op_resolved["from_date"].alias("from_date"),
            op_resolved["to_date"].alias("to_date"),
            dta_base["is_available_raw"].alias("is_available_raw"),
        )
        .groupBy("publication_id", "day_type_ref", "day_type_ref_ver")
        .agg(
            F.collect_list(
                F.struct(
                    "assignment_kind",
                    "target_date",
                    "from_date",
                    "to_date",
                    "is_available_raw",
                )
            ).alias("assignments_list")
        )
    )

    cf_conds = raw_cf.select(
        "publication_id",
        "source_member",
        "composite_frame_index",
        F.col("validity_conditions.AvailabilityCondition").alias("frame_conditions"),
    ).dropDuplicates()

    dt_dedup_win = Window.partitionBy(
        "publication_id",
        "source_member",
        "member_index",
        "composite_frame_index",
        "parent_frame_index",
        "entity_type",
        "entity_index",
    ).orderBy(F.col("snapshot_date").desc())

    dt_clean = (
        raw_dts.withColumn("_rn", F.row_number().over(dt_dedup_win))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("day_type_id", F.col("record._id"))
        .withColumn("day_type_version", F.col("record._version"))
        .withColumn("props_list", F.col("record.properties.PropertyOfDay"))
        .withColumn("source_occurrence", _occurrence_struct_col())
    )

    dt_with_context = (
        dt_clean.join(
            target_dates_df.select("publication_id", "operating_date"),
            on="publication_id",
            how="inner",
        )
        .join(
            dta_enriched,
            on=(
                (dt_clean["publication_id"] == dta_enriched["publication_id"])
                & (dt_clean["day_type_id"] == dta_enriched["day_type_ref"])
                & (
                    dta_enriched["day_type_ref_ver"].isNull()
                    | (F.trim(dta_enriched["day_type_ref_ver"]) == "")
                    | dt_clean["day_type_version"].isNull()
                    | (dta_enriched["day_type_ref_ver"] == dt_clean["day_type_version"])
                )
            ),
            how="left",
        )
        .join(
            cf_conds,
            on=["publication_id", "source_member", "composite_frame_index"],
            how="left",
        )
        .groupBy(
            dt_clean["publication_id"],
            dt_clean["provider"],
            dt_clean["snapshot_date"],
            F.col("operating_date").cast("date").alias("operating_date"),
            F.col("operating_date").alias("_op_date_str"),
            dt_clean["day_type_id"],
            dt_clean["day_type_version"],
            dt_clean["props_list"],
            cf_conds["frame_conditions"],
            dt_clean["source_occurrence"],
        )
        .agg(F.flatten(F.collect_list(dta_enriched["assignments_list"])).alias("assignments_list"))
    )

    evaluated = dt_with_context.withColumn(
        "_eval",
        _eval_day_type_udf(
            F.col("_op_date_str"),
            F.col("props_list"),
            F.col("assignments_list"),
            F.col("frame_conditions"),
        ),
    )

    return evaluated.select(
        "publication_id",
        "provider",
        "snapshot_date",
        "operating_date",
        "day_type_id",
        "day_type_version",
        F.col("_eval.calendar_state").alias("calendar_state"),
        F.col("_eval.reason_codes").alias("reason_codes"),
        F.col("_eval.active_assignment_count").alias("active_assignment_count"),
        F.col("_eval.inactive_assignment_count").alias("inactive_assignment_count"),
        "source_occurrence",
    )


def build_journey_service_dates(
    journeys_df: DataFrame,
    service_calendar_df: DataFrame,
    publication_lookup: DataFrame,
) -> DataFrame:
    """Evaluate every journey definition for its publication's target operating_date."""
    target_dates = publication_lookup.select("publication_id", "operating_date").dropDuplicates()

    cal_by_dt_ver = (
        service_calendar_df.groupBy(
            "publication_id", "operating_date", "day_type_id", "day_type_version"
        )
        .agg(F.collect_set("calendar_state").alias("_states"))
        .withColumn(
            "resolved_calendar_state",
            F.when(F.size("_states") > 1, F.lit("unresolved")).otherwise(
                F.element_at("_states", 1)
            ),
        )
        .select(
            "publication_id",
            "operating_date",
            "day_type_id",
            "day_type_version",
            "resolved_calendar_state",
        )
    )

    j_base = (
        journeys_df.join(target_dates, on="publication_id", how="inner")
        .withColumn("_j_row_id", F.monotonically_increasing_id())
        .withColumn("operating_date_dt", F.col("operating_date").cast("date"))
    )

    j_exploded = j_base.select(
        "_j_row_id",
        "publication_id",
        "operating_date_dt",
        F.explode_outer("day_type_refs").alias("_dt_ref"),
    ).select(
        "_j_row_id",
        "publication_id",
        "operating_date_dt",
        F.col("_dt_ref.day_type_id").alias("_ref_dt_id"),
        F.col("_dt_ref.day_type_version").alias("_ref_dt_ver"),
    )

    j_joined_cal = j_exploded.join(
        cal_by_dt_ver,
        on=(
            (j_exploded["publication_id"] == cal_by_dt_ver["publication_id"])
            & (j_exploded["operating_date_dt"] == cal_by_dt_ver["operating_date"])
            & _match_ref_expr(
                j_exploded["_ref_dt_id"],
                j_exploded["_ref_dt_ver"],
                cal_by_dt_ver["day_type_id"],
                cal_by_dt_ver["day_type_version"],
            )
        ),
        how="left",
    ).withColumn(
        "_effective_dt_state",
        F.when(F.col("_ref_dt_id").isNull(), F.lit(None).cast("string")).otherwise(
            F.coalesce(F.col("resolved_calendar_state"), F.lit("unresolved"))
        ),
    )

    dt_states_agg = j_joined_cal.groupBy("_j_row_id").agg(
        F.collect_list("_effective_dt_state").alias("day_type_states"),
        F.collect_list("_ref_dt_id").alias("evaluated_day_type_ids"),
    )

    j_eval = (
        j_base.join(dt_states_agg, on="_j_row_id", how="left")
        .withColumn(
            "_j_eval",
            _eval_journey_udf(
                F.col("operating_date"),
                F.col("day_type_states"),
                F.col("validity_conditions"),
            ),
        )
        .drop("_j_row_id")
    )

    return j_eval.select(
        "publication_id",
        "provider",
        "snapshot_date",
        F.col("operating_date_dt").alias("operating_date"),
        "service_journey_id",
        "service_journey_version",
        "resolved_line_id",
        "resolved_public_code",
        "resolved_transport_mode",
        "resolved_route_id",
        "resolved_journey_pattern_id",
        "resolved_operator_ref",
        "journey_resolution_status",
        "evaluated_day_type_ids",
        "day_type_states",
        F.col("_j_eval.service_status").alias("service_status"),
        F.col("_j_eval.reason_codes").alias("reason_codes"),
        "source_occurrence",
    )


def build_netex_stop_calls(
    spark: SparkSession,
    run_dirs: list[str],
    publication_lookup: DataFrame,
    journey_service_dates_df: DataFrame,
    journey_pattern_stops_df: DataFrame,
    provider: str = DEFAULT_PROVIDER,
    timezone_name: str = DEFAULT_TIMEZONE,
) -> DataFrame:
    """Expand scheduled dated journeys into planned stop visits with Oslo->UTC timestamps."""
    scheduled_journeys = journey_service_dates_df.filter(
        F.col("service_status") == "scheduled"
    ).select(
        "publication_id",
        "provider",
        "snapshot_date",
        "operating_date",
        "service_journey_id",
        "service_journey_version",
        "resolved_line_id",
        "resolved_public_code",
        "resolved_transport_mode",
        "resolved_route_id",
        "resolved_journey_pattern_id",
        "resolved_operator_ref",
        "journey_resolution_status",
        "source_occurrence",
    )

    raw_pt = _load_staged_table(spark, run_dirs, "passing_times", publication_lookup, provider)

    pt_base = (
        raw_pt.withColumn("passing_time_id", F.col("record._id"))
        .withColumn("passing_time_version", F.col("record._version"))
        .withColumn("passing_time_order_raw", F.col("record._order"))
        .withColumn(
            "stop_point_in_pattern_ref",
            F.col("record.StopPointInJourneyPatternRef._ref"),
        )
        .withColumn(
            "stop_point_in_pattern_ref_ver",
            F.col("record.StopPointInJourneyPatternRef._version"),
        )
        .withColumn("planned_arrival_time_raw", F.col("record.ArrivalTime"))
        .withColumn("planned_departure_time_raw", F.col("record.DepartureTime"))
        .withColumn("planned_arrival_day_offset_raw", F.col("record.ArrivalDayOffset"))
        .withColumn("planned_departure_day_offset_raw", F.col("record.DepartureDayOffset"))
        .withColumn("earliest_departure_time_raw", F.col("record.EarliestDepartureTime"))
        .withColumn("latest_arrival_time_raw", F.col("record.LatestArrivalTime"))
        .withColumn(
            "is_flexible_window",
            F.col("earliest_departure_time_raw").isNotNull()
            | F.col("latest_arrival_time_raw").isNotNull(),
        )
        .select(
            "publication_id",
            "service_journey_id",
            "service_journey_version",
            "passing_time_index",
            "passing_time_id",
            "passing_time_version",
            "passing_time_order_raw",
            "stop_point_in_pattern_ref",
            "stop_point_in_pattern_ref_ver",
            "planned_arrival_time_raw",
            "planned_departure_time_raw",
            "planned_arrival_day_offset_raw",
            "planned_departure_day_offset_raw",
            "earliest_departure_time_raw",
            "latest_arrival_time_raw",
            "is_flexible_window",
        )
    )

    dated_pt = (
        scheduled_journeys.join(
            pt_base,
            on=(
                (scheduled_journeys["publication_id"] == pt_base["publication_id"])
                & (scheduled_journeys["service_journey_id"] == pt_base["service_journey_id"])
                & (
                    scheduled_journeys["service_journey_version"].isNull()
                    | pt_base["service_journey_version"].isNull()
                    | (
                        scheduled_journeys["service_journey_version"]
                        == pt_base["service_journey_version"]
                    )
                )
            ),
            how="inner",
        )
        .select(
            scheduled_journeys["*"],
            pt_base["passing_time_index"],
            pt_base["passing_time_id"],
            pt_base["passing_time_version"],
            pt_base["passing_time_order_raw"],
            pt_base["stop_point_in_pattern_ref"],
            pt_base["stop_point_in_pattern_ref_ver"],
            pt_base["planned_arrival_time_raw"],
            pt_base["planned_departure_time_raw"],
            pt_base["planned_arrival_day_offset_raw"],
            pt_base["planned_departure_day_offset_raw"],
            pt_base["earliest_departure_time_raw"],
            pt_base["latest_arrival_time_raw"],
            pt_base["is_flexible_window"],
        )
        .withColumn("_call_occ_id", F.monotonically_increasing_id())
    )

    jps_lookup = journey_pattern_stops_df.select(
        F.col("publication_id").alias("_jps_pub_id"),
        F.col("journey_pattern_id").alias("_jps_jp_id"),
        F.col("point_index").alias("pattern_point_index"),
        F.col("pattern_stop_point_id").alias("_jps_spip_id"),
        F.col("pattern_stop_point_version").alias("_jps_spip_ver"),
        F.col("scheduled_stop_point_ref"),
        F.col("stop_point_name"),
        F.col("quay_id"),
        F.col("quay_version"),
        F.col("stop_assignment_match_count"),
        F.col("stop_assignment_status"),
        F.col("for_boarding_raw"),
        F.col("for_alighting_raw"),
    ).dropDuplicates()

    with_stop = dated_pt.join(
        jps_lookup,
        on=(
            (dated_pt["publication_id"] == jps_lookup["_jps_pub_id"])
            & (dated_pt["resolved_journey_pattern_id"] == jps_lookup["_jps_jp_id"])
            & _match_ref_expr(
                dated_pt["stop_point_in_pattern_ref"],
                dated_pt["stop_point_in_pattern_ref_ver"],
                jps_lookup["_jps_spip_id"],
                jps_lookup["_jps_spip_ver"],
            )
        ),
        how="left",
    )

    spip_cnt_win = Window.partitionBy("_call_occ_id")
    spip_pick_win = Window.partitionBy("_call_occ_id").orderBy(
        F.col("pattern_point_index").asc_nulls_last(),
        F.col("_jps_spip_id").asc_nulls_last(),
    )

    deduped_stop = (
        with_stop.withColumn(
            "pattern_stop_match_count",
            F.sum(F.when(F.col("_jps_spip_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))).over(
                spip_cnt_win
            ),
        )
        .withColumn("_rn", F.row_number().over(spip_pick_win))
        .filter(F.col("_rn") == 1)
        .drop(
            "_rn",
            "_call_occ_id",
            "_jps_pub_id",
            "_jps_jp_id",
            "_jps_spip_id",
            "_jps_spip_ver",
        )
    )

    op_date_str = F.date_format(F.col("operating_date"), "yyyy-MM-dd")
    tz_col = F.lit(timezone_name)

    with_times = (
        deduped_stop.withColumn(
            "_arr_conv",
            _local_clock_to_utc_udf(
                op_date_str,
                F.col("planned_arrival_time_raw"),
                F.col("planned_arrival_day_offset_raw"),
                tz_col,
            ),
        )
        .withColumn(
            "_dep_conv",
            _local_clock_to_utc_udf(
                op_date_str,
                F.col("planned_departure_time_raw"),
                F.col("planned_departure_day_offset_raw"),
                tz_col,
            ),
        )
        .withColumn("planned_arrival_utc", F.to_timestamp(F.col("_arr_conv.utc_iso")))
        .withColumn("planned_arrival_local_oslo", F.col("_arr_conv.local_iso"))
        .withColumn("planned_arrival_day_offset", F.col("_arr_conv.resolved_day_offset"))
        .withColumn("planned_arrival_time_error", F.col("_arr_conv.error_code"))
        .withColumn(
            "arrival_time_valid",
            F.col("planned_arrival_utc").isNotNull() & F.col("planned_arrival_time_error").isNull(),
        )
        .withColumn("planned_departure_utc", F.to_timestamp(F.col("_dep_conv.utc_iso")))
        .withColumn("planned_departure_local_oslo", F.col("_dep_conv.local_iso"))
        .withColumn("planned_departure_day_offset", F.col("_dep_conv.resolved_day_offset"))
        .withColumn("planned_departure_time_error", F.col("_dep_conv.error_code"))
        .withColumn(
            "departure_time_valid",
            F.col("planned_departure_utc").isNotNull()
            & F.col("planned_departure_time_error").isNull(),
        )
        .withColumn(
            "has_any_planned_time_error",
            F.col("planned_arrival_time_error").isNotNull()
            | F.col("planned_departure_time_error").isNotNull(),
        )
        .withColumn("source_timezone", tz_col)
        .withColumn(
            "planned_call_status",
            F.when(F.col("is_flexible_window"), F.lit("flexible_service_window"))
            .when(
                F.col("journey_resolution_status") != "resolved",
                F.lit("unresolved_journey_definition"),
            )
            .when(F.col("pattern_stop_match_count") == 0, F.lit("unresolved_pattern_stop"))
            .when(F.col("pattern_stop_match_count") > 1, F.lit("ambiguous_pattern_stop"))
            .when(F.col("stop_assignment_status") != "resolved", F.lit("unresolved_quay"))
            .when(
                F.col("planned_arrival_time_error").isNotNull()
                & F.col("planned_departure_time_error").isNotNull(),
                F.lit("invalid_planned_timestamps"),
            )
            .when(
                F.col("planned_arrival_time_error").isNotNull(),
                F.lit("resolved_with_arrival_time_error"),
            )
            .when(
                F.col("planned_departure_time_error").isNotNull(),
                F.lit("resolved_with_departure_time_error"),
            )
            .otherwise(F.lit("resolved")),
        )
    )

    return with_times.select(
        "publication_id",
        "provider",
        "snapshot_date",
        "operating_date",
        "service_journey_id",
        "service_journey_version",
        "resolved_line_id",
        "resolved_public_code",
        "resolved_transport_mode",
        "resolved_route_id",
        "resolved_journey_pattern_id",
        "resolved_operator_ref",
        "passing_time_index",
        "pattern_point_index",
        "passing_time_id",
        "passing_time_order_raw",
        "stop_point_in_pattern_ref",
        "scheduled_stop_point_ref",
        "stop_point_name",
        "quay_id",
        "quay_version",
        "stop_assignment_match_count",
        "stop_assignment_status",
        "pattern_stop_match_count",
        "for_boarding_raw",
        "for_alighting_raw",
        "source_timezone",
        "planned_arrival_time_raw",
        "planned_arrival_day_offset_raw",
        "planned_arrival_day_offset",
        "planned_arrival_local_oslo",
        "planned_arrival_utc",
        "planned_arrival_time_error",
        "arrival_time_valid",
        "planned_departure_time_raw",
        "planned_departure_day_offset_raw",
        "planned_departure_day_offset",
        "planned_departure_local_oslo",
        "planned_departure_utc",
        "planned_departure_time_error",
        "departure_time_valid",
        "has_any_planned_time_error",
        "earliest_departure_time_raw",
        "latest_arrival_time_raw",
        "is_flexible_window",
        "planned_call_status",
        "source_occurrence",
    )