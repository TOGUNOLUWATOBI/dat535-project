"""Punctuality policy boundaries, exclusion masking and known identity regressions."""

from datetime import date, datetime
import unittest
from unittest.mock import patch

from tests.fixtures import SparkTestCase, gold_inventory


EVENT_SCHEMA = """provider string, operating_date date, journey_key string, transport_mode string,
event_type string, baseline string, delay_sec double, eligible boolean, has_plan_comparison boolean,
plan_agreed boolean, plan_disagreement_sec double, abs_plan_disagreement_sec double,
ts_identity_checked boolean, ts_identity_failed boolean"""


def event(delay, kind="departure", eligible=True, day=date(2026, 1, 1), journey="sj::1"):
    return ("KOL", day, journey, "bus", kind, "et_aimed", delay, eligible, False, None, None, None, False, None)


class PunctualityTests(SparkTestCase):
    def aggregate(self, rows):
        from pipeline.Gold.event_metrics import aggregate_punctuality
        frame = self.spark.createDataFrame(rows, EVENT_SCHEMA)
        return aggregate_punctuality(frame, ["baseline", "event_type"], gold_run_id="unit-test").first()

    def test_departure_boundaries_and_exclusions_conserve_population(self):
        row = self.aggregate([event(d) for d in (-1.0, 0.0, 300.0, 300.5, 900.0, 900.5, None)]
                             + [event(9999.0, eligible=False)])
        self.assertEqual((row.population_event_count, row.eligible_event_count, row.excluded_event_count), (8, 6, 2))
        self.assertEqual((row.early_count, row.within_window_count, row.late_count, row.severe_delay_count), (1, 2, 3, 1))
        self.assertEqual(row.delay_sum_sec, 2400.0)
        self.assertEqual(row.positive_delay_sum_sec, 2401.0)
        self.assertEqual((row.median_delay_sec, row.p90_delay_sec, row.p95_delay_sec), (300.0, 900.5, 900.5))
        self.assertAlmostEqual(row.within_window_rate, 2 / 6)

    def test_arrivals_up_to_five_minutes_late_are_within_window(self):
        row = self.aggregate([event(d, kind="arrival") for d in (-90.0, 0.0, 300.0, 301.0)])
        self.assertEqual((row.within_window_count, row.late_count), (3, 1))
        self.assertEqual(row.within_window_rate, .75)

    def test_empty_eligible_population_has_null_rates_and_percentiles(self):
        row = self.aggregate([event(999.0, eligible=False), event(None)])
        self.assertEqual(row.eligible_event_count, 0)
        self.assertEqual(row.delay_sum_sec, 0.0)
        for field in ("mean_delay_sec", "median_delay_sec", "within_window_rate", "late_rate"):
            self.assertIsNone(row[field], field)

    def test_null_journey_ids_do_not_count_as_a_journey(self):
        self.assertEqual(self.aggregate([event(1.0, journey=None)]).contributing_journey_count, 0)

    @unittest.expectedFailure
    def test_period_journey_count_distinguishes_operating_dates(self):
        """Known defect: a service journey repeated on two dates currently counts once."""
        row = self.aggregate([event(1.0), event(1.0, day=date(2026, 1, 2))])
        self.assertEqual(row.contributing_journey_count, 2)

    def test_mode_normalization_preserves_unrecognized_modes(self):
        from pipeline.Gold.event_metrics import normalize_transport_mode_expr
        frame = self.spark.createDataFrame([(" coach ",), ("ferry",), (None,), ("rail",)], "mode string")
        self.assertEqual([r.mode for r in frame.select(normalize_transport_mode_expr("mode").alias("mode")).collect()],
                         ["bus", "water", "unknown", "rail"])

    def test_blank_journey_ids_remain_null(self):
        from pipeline.Gold.event_metrics import normalize_journey_key_expr
        frame = self.spark.createDataFrame([(None, "  ")], "dated_service_journey_id string, service_journey_id string")
        self.assertIsNone(frame.select(normalize_journey_key_expr()).first()[0])

    @unittest.expectedFailure
    def test_reported_and_planned_records_for_same_service_share_journey_key(self):
        """Known defect: supplying a dated journey ID changes the common service key."""
        from pipeline.Gold.event_metrics import normalize_journey_key_expr
        frame = self.spark.createDataFrame([(None, "journey-1"), ("dated-1", "journey-1")],
                                          "dated_service_journey_id string, service_journey_id string")
        keys = [r[0] for r in frame.select(normalize_journey_key_expr()).collect()]
        self.assertEqual(keys[0], keys[1])

    @unittest.expectedFailure
    def test_unknown_time_kind_is_not_a_recorded_observation(self):
        """Known defect: Gold accepts any non-estimated kind, including unknown/missing."""
        from pipeline.Gold.journey_metrics import _not_estimated
        frame = self.spark.createDataFrame([(datetime(2026, 1, 1), kind) for kind in
                    ("recorded", "estimated", "unknown", None)], "time timestamp, kind string")
        self.assertEqual([r.kind for r in frame.filter(_not_estimated("time", "kind")).collect()], ["recorded"])

    def test_event_projection_preserves_lineage_masks_delay_and_checks_timestamp_identity(self):
        from pyspark.sql import types as T
        from pipeline.Gold.event_metrics import build_logical_event_projection
        fields = {key: T.StringType() for key in (
            "provider", "publication_id", "source_silver_run_id", "et_canonical_id", "service_journey_id",
            "dated_service_journey_id", "resolved_line_id", "line_ref", "resolved_transport_mode", "quay_id")}
        fields["operating_date"] = T.DateType()
        values = {"provider": "KOL", "publication_id": "pub-1", "source_silver_run_id": "silver-test",
                  "et_canonical_id": "et-1", "service_journey_id": "sj-1", "resolved_line_id": "line-1",
                  "line_ref": "  ", "resolved_transport_mode": "coach", "quay_id": " ",
                  "operating_date": date(2026, 1, 1)}
        for kind in ("arrival", "departure"):
            for source, minute in (("planned", 0), ("aimed", 1), ("reported", 3)):
                column = f"{source}_{kind}_utc"
                fields[column] = T.TimestampType()
                values[column] = datetime(2026, 1, 1, 0, minute)
            for prefix, delay in (("reported", 120.0), ("netex", 180.0), ("planned", 60.0)):
                suffix = {"reported": "delay", "netex": "deviation", "planned": "disagreement"}[prefix]
                column = f"{prefix}_{kind}_{suffix}_sec"
                fields[column], values[column] = T.DoubleType(), delay
            for family in (f"reported_{kind}_delay", f"netex_{kind}_comparison"):
                fields[f"eligible_{family}"] = T.BooleanType()
                fields[f"exclusion_{family}"] = T.ArrayType(T.StringType())
                values[f"eligible_{family}"] = family.startswith("netex")
        frame = self.spark.createDataFrame([values], T.StructType([T.StructField(k, t) for k, t in fields.items()]))
        with patch("pipeline.Gold.event_metrics.read_silver_table_for_inventory", return_value=frame):
            rows = build_logical_event_projection(self.spark, gold_inventory()).collect()
        self.assertEqual(len(rows), 4)
        for row in rows:
            self.assertEqual(row.source_silver_run_id, "silver-test")
            self.assertEqual(row.transport_mode, "bus")
            self.assertIsNone(row.quay_id)
            self.assertEqual(row.exclusion_reason_codes, [])
            if row.baseline == "et_aimed":
                self.assertIsNone(row.delay_sec)
                self.assertIsNone(row.line_id)
            else:
                self.assertEqual(row.delay_sec, 180.0)
                self.assertEqual(row.plan_disagreement_sec, 60.0)
                self.assertTrue(row.ts_identity_checked)
                self.assertFalse(row.ts_identity_failed)

    def test_empty_network_population_is_dense_with_null_rates(self):
        from pyspark.sql import functions as F
        from pipeline.Gold.event_metrics import build_daily_network_punctuality
        empty = (self.spark.createDataFrame([], EVENT_SCHEMA)
                 .withColumn("publication_id", F.lit("pub-1"))
                 .withColumn("source_silver_run_id", F.lit("silver-test")))
        rows = build_daily_network_punctuality(self.spark, gold_inventory(), empty, gold_run_id="test").collect()
        self.assertEqual({(r.event_type, r.baseline) for r in rows},
                         {(e, b) for e in ("arrival", "departure") for b in ("et_aimed", "netex_planned")})
        for row in rows:
            self.assertEqual(row.population_event_count, 0)
            self.assertEqual(row.eligible_event_count, 0)
            self.assertIsNone(row.within_window_rate)
            if row.baseline == "netex_planned":
                self.assertEqual(row.timestamp_identity_failed_count, 0)
