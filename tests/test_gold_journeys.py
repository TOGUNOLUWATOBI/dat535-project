"""A two-stop trip using real Spark joins and distinct NeTEx/reconciliation ID namespaces."""

from datetime import date, datetime
import hashlib
import unittest
from unittest.mock import patch

from tests.fixtures import SparkTestCase, gold_inventory


class JourneyEndpointTests(SparkTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        from pyspark.sql import types as T
        from pipeline.Gold.journey_metrics import build_journey_performance
        common = {"provider": "KOL", "operating_date": date(2026, 1, 1),
                  "publication_id": "pub-1", "service_journey_id": "journey-1"}
        planned = [{**common, "resolved_line_id": "line-1", "resolved_transport_mode": "bus",
                    "service_status": "scheduled"}]
        calls, siri, reconciled = [], [], []
        for i in range(2):
            aimed, reported = datetime(2026, 1, 1, 10 + i), datetime(2026, 1, 1, 10 + i, 1)
            calls.append({**common, "passing_time_id": f"passing-time-{i}", "quay_id": f"quay-{i}",
                          "passing_time_index": i, "planned_departure_utc": aimed, "planned_arrival_utc": aimed})
            siri.append({**common, "et_canonical_id": f"et-{i}", "line_ref": "line-1", "stop_point_ref": f"quay-{i}",
                         "aimed_departure_utc": aimed, "aimed_arrival_utc": aimed,
                         "reported_departure_utc": reported, "reported_arrival_utc": reported,
                         "departure_time_kind": "recorded", "arrival_time_kind": "recorded",
                         "journey_cancellation_state": "not_cancelled", "duplicate_status": "unique"})
            reconciled.append({**common, "et_canonical_id": f"et-{i}",
                "planned_call_id": hashlib.sha256(f"passing-time-{i}".encode()).hexdigest(),
                "quay_id": f"quay-{i}", "passing_time_index": i, "reconciliation_status": "matched",
                "reported_departure_delay_sec": 60.0, "reported_arrival_delay_sec": 60.0,
                "netex_departure_deviation_sec": 60.0, "netex_arrival_deviation_sec": 60.0,
                **{f"eligible_{family}_{kind}_{suffix}": True for kind in ("arrival", "departure")
                   for family, suffix in (("reported", "delay"), ("netex", "comparison"))}})
        tables = {"journey_service_dates": planned, "netex_stop_calls": calls,
                  "siri_et_stop_calls": siri, "reconciled_stop_calls": reconciled}

        def read(spark, inventory, table, columns):
            def type_for(name):
                if name == "operating_date":
                    return T.DateType()
                if name.endswith("_utc"):
                    return T.TimestampType()
                if name.endswith("_sec"):
                    return T.DoubleType()
                if name.startswith("eligible_"):
                    return T.BooleanType()
                if name == "passing_time_index":
                    return T.LongType()
                return T.StringType()
            schema = T.StructType([T.StructField(c, type_for(c)) for c in columns])
            return spark.createDataFrame([{c: row.get(c) for c in columns} for row in tables[table]], schema)

        with patch("pipeline.Gold.journey_metrics.read_silver_table_for_inventory", side_effect=read):
            cls.rows = build_journey_performance(cls.spark, gold_inventory(), gold_run_id="test").collect()

    def test_trip_grain_counts_and_endpoint_observations_are_preserved(self):
        self.assertEqual(len(self.rows), 1)
        row, = self.rows
        self.assertEqual(row.planned_call_count, 2)
        self.assertEqual(row.matched_call_count, 2)
        self.assertEqual((row.origin_quay_id, row.terminal_quay_id), ("quay-0", "quay-1"))
        self.assertEqual(row.endpoint_observation_status, "both")

    @unittest.expectedFailure
    def test_reconciled_endpoint_delays_use_occurrence_identity_across_id_namespaces(self):
        """Known defect: NeTEx passing_time_id is currently compared with SHA-256 planned_call_id."""
        row, = self.rows
        self.assertTrue(row.netex_endpoint_pair_eligible)
        self.assertEqual(row.netex_origin_departure_delay_sec, 60.0)
        self.assertEqual(row.netex_terminal_arrival_delay_sec, 60.0)
