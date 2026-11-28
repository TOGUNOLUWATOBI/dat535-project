"""Measurement parsing and row comparison must detect real differences."""

import importlib.util
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests.fixtures import available


def load(name):
    path = Path(__file__).resolve().parents[1] / 'scripts/performance' / f'{name}.py'
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SparkLogMetricsTests(unittest.TestCase):
    def test_summary_uses_completed_equal_pairs_and_ignores_completion_marker(self):
        module = load('summarize')
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            root = Path(tmp)
            for name, seconds in [('baseline-01', 10), ('candidate-01', 7), ('candidate-02', 1)]:
                path = root / name
                path.mkdir()
                (path / 'measurement.json').write_text(json.dumps({'status': 'complete', 'elapsed_sec': seconds}))
            (root / 'comparison-01.json').write_text(json.dumps({'equal': True}))
            (root / 'comparison-complete.json').write_text(json.dumps({'status': 'complete'}))
            with patch('sys.argv', ['summarize', str(root)]):
                module.main()
            result = json.loads((root / 'summary.json').read_text())
            self.assertEqual(result['paired_repetitions'], 1)
            self.assertEqual(result['median_seconds'], {'baseline': 10, 'candidate': 7})

    def test_task_metrics_include_local_and_remote_shuffle_reads(self):
        module = load('summarize')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            events = [{'Event': 'SparkListenerJobEnd'}, {'Event': 'SparkListenerTaskEnd', 'Task Metrics': {
                'Executor Run Time': 10, 'JVM GC Time': 2, 'Input Metrics': {'Bytes Read': 30},
                'Shuffle Read Metrics': {'Local Bytes Read': 40, 'Remote Bytes Read': 50},
                'Shuffle Write Metrics': {'Shuffle Bytes Written': 60}, 'Disk Bytes Spilled': 70}}]
            (root / 'events').write_text('\n'.join(json.dumps(e) for e in events))
            result = module.spark_metrics(root)
            self.assertEqual((result['jobs'], result['tasks']), (1, 1))
            self.assertEqual(result['shuffle_read_bytes'], 90)
            self.assertEqual(result['executor_run_ms'], 10)
            self.assertEqual(result['disk_spill_bytes'], 70)


@unittest.skipUnless(available('pyarrow'), 'Install requirements.txt for Parquet comparison tests')
class OutputComparisonTests(unittest.TestCase):
    def test_row_order_and_run_id_are_ignored_but_duplicates_and_values_are_preserved(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        module = load('compare_gold')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            before, after = root / 'before', root / 'after'
            before.mkdir()
            after.mkdir()
            pq.write_table(pa.table({'gold_run_id': ['old', 'old'], 'count': [1, 2]}), before / 'part.parquet')
            pq.write_table(pa.table({'gold_run_id': ['new', 'new'], 'count': [2, 1]}), after / 'part.parquet')
            self.assertEqual(module.rows(before, {'gold_run_id'}), module.rows(after, {'gold_run_id'}))
            pq.write_table(pa.table({'gold_run_id': ['new', 'new'], 'count': [1, 1]}), after / 'part.parquet')
            self.assertNotEqual(module.rows(before, {'gold_run_id'}), module.rows(after, {'gold_run_id'}))
