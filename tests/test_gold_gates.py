"""Reject duplicate grains and inconsistent published metric counts on small real DataFrames."""

from datetime import date

from tests.fixtures import SparkTestCase, gold_inventory


class GoldGateTests(SparkTestCase):
    def tables(self):
        network = self.spark.createDataFrame([
            (event, baseline, 0, 1, 0, 1, 0, 0)
            for event in ("arrival", "departure") for baseline in ("et_aimed", "netex_planned")
        ], """event_type string, baseline string, early_count long, within_window_count long,
        late_count long, eligible_event_count long, severe_delay_count long,
        timestamp_identity_failed_count long""")
        return {
            "daily_data_quality": self.spark.createDataFrame([(date(2026, 1, 1),)], "operating_date date"),
            "journey_performance": self.spark.createDataFrame([("KOL", date(2026, 1, 1), "sj::1")],
                                                               "provider string, operating_date date, journey_key string"),
            "daily_network_punctuality": network,
            "daily_line_punctuality": self.spark.createDataFrame([(4,)], "eligible_event_count long"),
            "daily_stop_punctuality": self.spark.createDataFrame([(4,)], "eligible_event_count long"),
        }

    def test_consistent_tables_pass(self):
        from pipeline.Gold.quality_and_publish import run_gold_gates
        failures, checks = run_gold_gates(gold_inventory(), self.tables())
        self.assertEqual(failures, [])
        self.assertEqual(checks["reconciliation_eligible_totals"], {"network": 4, "line": 4, "stop": 4})

    def test_corrupt_categories_timestamp_identity_and_duplicate_grains_are_rejected(self):
        from pyspark.sql import functions as F
        from pipeline.Gold.quality_and_publish import run_gold_gates
        tables = self.tables()
        tables["journey_performance"] = tables["journey_performance"].unionByName(tables["journey_performance"])
        tables["daily_network_punctuality"] = (tables["daily_network_punctuality"]
            .withColumn("within_window_count", F.lit(0))
            .withColumn("timestamp_identity_failed_count", F.lit(1)))
        tables["daily_stop_punctuality"] = self.spark.createDataFrame([(3,)], "eligible_event_count long")
        failures, _ = run_gold_gates(gold_inventory(), tables)
        for expected in ("grain duplicate", "Departure category conservation", "Arrival category conservation",
                         "Timestamp identity", "Eligible event count mismatch"):
            self.assertTrue(any(expected in message for message in failures), (expected, failures))
