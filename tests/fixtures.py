"""Small local fixtures; no server files, credentials or downloads are needed."""

import atexit
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest


def available(module):
    try:
        return importlib.util.find_spec(module) is not None
    except ModuleNotFoundError:
        return False


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def silver_run(root, name="silver-test", days=("2026-01-01",), publication="pub-1", gate="passed"):
    run = root / name
    report = write_json(run / "quality/quality_report.json", {
        "gate_status": gate, "daily_summaries": [
            {"provider": "KOL", "operating_date": day, "publication_id": publication,
             "siri_file_available": True} for day in days]})
    write_json(run / "silver_manifest.json", {
        "status": "complete", "provider": "KOL", "quality_report_path": str(report)})
    return run


_spark = None


def gold_inventory(days=("2026-01-01",)):
    from pipeline.Gold.inputs import DateAuthority, SilverInventory
    authorities = tuple(DateAuthority("KOL", day, "silver-test", "/unused/silver_manifest.json",
                                      "test-hash", "pub-1", "passed", True) for day in days)
    return SilverInventory("KOL", tuple(days), authorities, ())


def spark_session():
    global _spark
    if _spark is None:
        from pyspark.sql import SparkSession
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("PYSPARK_PYTHON", os.sys.executable)
        warehouse = tempfile.TemporaryDirectory(prefix="dat535-test-warehouse-")
        atexit.register(warehouse.cleanup)
        _spark = (SparkSession.builder.master("local[1]").appName("dat535-tests")
                  .config("spark.driver.memory", "1g")
                  .config("spark.sql.shuffle.partitions", "2")
                  .config("spark.sql.adaptive.enabled", "false")
                  .config("spark.sql.session.timeZone", "UTC")
                  .config("spark.ui.enabled", "false")
                  .config("spark.sql.warehouse.dir", warehouse.name).getOrCreate())
        _spark.sparkContext.setLogLevel("ERROR")
        atexit.register(_spark.stop)
    return _spark


class SparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if not available("pyspark"):
            raise unittest.SkipTest("Install requirements.txt and Java 17 for Spark tests")
        cls.spark = spark_session()
