"""BigQuery is mocked; real temporary Parquet files exercise daily Bronze publication."""

import contextlib
from datetime import date
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from tests.fixtures import available


@unittest.skipUnless(available("google.cloud.bigquery") and available("pyarrow"),
                     "Install requirements.txt for SIRI ET Parquet tests")
class SiriBronzeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pipeline.Bronze.SIRI_ET import bronze_entur_siri_et
        import pyarrow
        cls.bronze, cls.arrow = bronze_entur_siri_et, pyarrow

    def client(self, values):
        client = Mock()
        client.query.return_value.to_arrow.return_value = self.arrow.table({"value": values})
        client.query.return_value.total_bytes_billed = 100
        return client

    def test_query_parameters_bind_provider_and_operating_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            client = self.client([1, 2])
            target = Path(tmp) / "day.parquet"
            self.assertEqual(self.bronze.fetch_siri_et_day_to_parquet(
                client, "project.dataset.table", "kol", date(2026, 1, 1), target), (2, 100))
            query, = client.query.call_args.args
            config = client.query.call_args.kwargs["job_config"]
            self.assertIn("@operating_date", query)
            self.assertEqual({p.name: p.value for p in config.query_parameters},
                             {"data_source": "KOL", "operating_date": date(2026, 1, 1)})
            self.assertTrue(target.is_file())

    def test_successful_day_is_reused_without_querying_again(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            client, root = self.client([1]), Path(tmp)
            args = (client, "p.d.t", [date(2026, 1, 1)], root)
            manifest, failed, count = self.bronze.run_ingestion(*args)
            self.assertEqual((failed, count), (False, 1))
            record = self.bronze.load_manifest(manifest)[0]
            self.assertEqual(record["sha256"], self.bronze.sha256_file(Path(record["file"])))
            self.bronze.run_ingestion(*args)
            self.assertEqual(client.query.call_count, 1)
            self.assertEqual(len(self.bronze.load_manifest(manifest)), 1)

    def test_empty_day_is_unavailable_and_publishes_no_data_file(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            manifest, _, count = self.bronze.run_ingestion(
                self.client([]), "p.d.t", [date(2026, 1, 1)], Path(tmp))
            self.assertEqual(count, 0)
            self.assertEqual(self.bronze.load_manifest(manifest)[0]["status"], "unavailable")
            self.assertEqual(list(Path(tmp).rglob("*.parquet")), [])

    def test_query_failure_is_recorded_without_leaving_partial_files(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            client = Mock()
            client.query.side_effect = RuntimeError("query failed")
            manifest, failed, count = self.bronze.run_ingestion(
                client, "p.d.t", [date(2026, 1, 1)], Path(tmp))
            self.assertEqual((failed, count), (True, 0))
            self.assertEqual(self.bronze.load_manifest(manifest)[0]["status"], "failed")
            self.assertEqual(list(Path(tmp).rglob("*.parquet")), [])
