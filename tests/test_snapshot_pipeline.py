from datetime import date
import importlib.util
import json
import shutil
from pathlib import Path
import tempfile
import unittest

from tests.fixtures import complete_fixture, spark_session


@unittest.skipUnless(importlib.util.find_spec('pyspark'), 'Install processing dependencies for Spark integration')
class SnapshotPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_real_pipeline_preserves_occurrences_handles_flexible_and_resumes(self):
        from pipeline.run_snapshot import run_snapshot

        manifest = complete_fixture(self.root / 'bronze')
        result = run_snapshot(self.spark, manifest, None, self.root / 'data')
        silver = Path(result['silver']['path'])
        gold = Path(result['gold']['path'])
        journeys = self.spark.read.parquet(str(silver / 'journeys'))
        self.assertEqual(journeys.count(), 4)
        self.assertEqual(journeys.where('is_flexible').count(), 1)
        self.assertEqual(journeys.where('NOT is_valid').count(), 0)
        self.assertEqual(journeys.where("provider='AKT'").select('entity_id').distinct().count(), 1)
        self.assertEqual({r.line_public_code for r in journeys.where("provider='AKT'").collect()}, {'007', '008'})
        times = self.spark.read.parquet(str(silver / 'passing_times'))
        self.assertEqual(times.count(), 8)
        self.assertEqual(times.where('arrival_seconds=87000').count(), 3)
        self.assertEqual(times.where("earliest_departure_time_raw='09:00:00'").count(), 2)
        dates = self.spark.read.parquet(str(silver / 'journey_service_dates'))
        self.assertEqual(dates.count(), 20)
        self.assertEqual(dates.where("service_date='2026-01-16'").count(), 0)
        gold_manifest = json.loads((gold / 'metadata/manifest.json').read_text())
        self.assertEqual((gold_manifest['service_from'], gold_manifest['service_to']), ('2026-01-01', '2026-01-31'))
        self.assertEqual(self.spark.read.parquet(str(gold / 'daily_network_service')).count(), 62)
        reconciliation = gold_manifest['reconciliation']
        self.assertEqual(reconciliation['gold_scheduled_departures'], 15)
        self.assertEqual(reconciliation['excluded_service_occurrences'], 5)
        self.assertEqual(gold_manifest['quality_gate']['status'], 'pass')
        profile = json.loads((gold / 'metadata/quality_profile.json').read_text())
        self.assertEqual(profile['service']['KOL']['excluded_flexible'], 5)
        self.assertEqual(profile['times']['AKT']['overnight_passing_times'], 2)
        self.assertEqual(profile['expected_providers'], ['AKT', 'KOL'])
        self.assertEqual(profile['observed_providers'], ['AKT', 'KOL'])
        # Source checks use XML for references, flexible windows and whole-line counts.
        from pipeline.quality.trace import SourceContext, trace

        staging = Path(result['staging']['path'])
        context = SourceContext(json.loads((staging / 'metadata/manifest.json').read_text()))
        selected = {r.source_member: r.journey_key for r in journeys.collect()}
        for member in ('AKT_007.xml', 'KOL_HentMeg.xml'):
            checked = trace(self.spark, silver, gold, staging, selected[member], member, context)
            self.assertTrue(checked['ok'], checked['checks'])
            self.assertTrue(checked['checks']['physical_stops_match'])
            self.assertTrue(checked['checks']['flexible_windows_match'])
            self.assertTrue(checked['checks']['source_line_departures_match_gold'])

        # Corrupt another journey's date consistently in both Silver and Gold.
        # The selected journey still matches XML, and Silver/Gold still agree.
        # Only the independent whole-line XML recount exposes the error.
        from pyspark.sql import functions as F

        wrong_silver = self.root / 'wrong-silver'
        wrong_gold = self.root / 'wrong-gold'
        shutil.copytree(silver, wrong_silver)
        shutil.copytree(gold, wrong_gold)
        changed = dates.withColumn(
            'service_date',
            F.when(
                (F.col('journey_key') == selected['AKT_008.xml']) & (F.col('service_date') == F.lit(date(2026, 1, 15))),
                F.lit(date(2026, 1, 16)),
            ).otherwise(F.col('service_date')),
        )
        changed.write.mode('overwrite').parquet(str(wrong_silver / 'journey_service_dates'))
        daily = self.spark.read.parquet(str(gold / 'daily_line_service'))
        changed_daily = daily.withColumn(
            'service_date',
            F.when(
                (F.col('provider') == 'AKT')
                & (F.col('line_public_code') == '008')
                & (F.col('service_date') == F.lit(date(2026, 1, 15))),
                F.lit(date(2026, 1, 16)),
            ).otherwise(F.col('service_date')),
        )
        changed_daily.write.mode('overwrite').parquet(str(wrong_gold / 'daily_line_service'))
        checked = trace(self.spark, wrong_silver, wrong_gold, staging, selected['AKT_007.xml'], 'corruption', context)
        self.assertTrue(checked['checks']['operating_dates_match'])
        self.assertTrue(checked['checks']['gold_counts_journey'])
        self.assertFalse(checked['checks']['source_line_departures_match_gold'])
        self.assertFalse(checked['ok'])
        # A provider promised by the manifest must not disappear merely because
        # every observed provider has clean, internally reconciled data.
        from pipeline.processing.gold.build import build_gold

        missing_manifest = wrong_silver / 'metadata/manifest.json'
        missing_data = json.loads(missing_manifest.read_text())
        missing_data['providers'].append('MISSING')
        missing_manifest.write_text(json.dumps(missing_data))
        blocked = self.root / 'missing-provider-gold'
        with self.assertRaisesRegex(RuntimeError, 'missing_expected_provider'):
            build_gold(self.spark, wrong_silver, blocked)
        blocked_report = json.loads((blocked / 'metadata/manifest.json').read_text())
        self.assertEqual(blocked_report['tables'], {})
        self.assertEqual(blocked_report['status'], 'failed')
        repeat = run_snapshot(self.spark, manifest, None, self.root / 'data')
        self.assertTrue(all(stage['reused'] for stage in repeat.values()))
        self.assertEqual(result['gold']['path'], repeat['gold']['path'])

    def test_zip_migration_reuses_checkpoints_and_raw_trace_without_retaining_xml(self):
        from pipeline.run_snapshot import run_snapshot
        from pipeline.storage.zip_bronze import retain_snapshot, snapshot_xml
        from pipeline.quality.trace import trace
        from pipeline.ingestion.prepare_monthly_support import covers_month

        manifest = complete_fixture(self.root / 'bronze', full_month=True)
        outputs = run_snapshot(self.spark, manifest, None, self.root / 'data')
        xml = manifest.parent.parent / 'xml/2026-01-15'
        original = {p.name: p.read_bytes() for p in xml.iterdir()}
        retain_snapshot(manifest, apply=True)
        self.assertFalse(xml.exists())
        self.assertTrue(covers_month(manifest, date(2026, 1, 15), date(2026, 1, 1), date(2026, 1, 31), ['KOL', 'AKT']))
        self.assertFalse(xml.exists())
        repeated = run_snapshot(self.spark, manifest, None, self.root / 'data')
        self.assertTrue(all(stage['reused'] for stage in repeated.values()))
        self.assertEqual(outputs['gold']['path'], repeated['gold']['path'])
        self.assertFalse(xml.exists())
        gold, silver, staging = (Path(outputs[name]['path']) for name in ('gold', 'silver', 'staging'))
        journey = self.spark.read.parquet(str(silver / 'journeys')).where("source_member='AKT_007.xml'").first()
        checked = trace(self.spark, silver, gold, staging, journey.journey_key, 'zip-backed')
        self.assertTrue(checked['ok'], checked['checks'])
        self.assertFalse(xml.exists())
        # A new output root forces real Spark XML reads from a restored ZIP snapshot.
        rebuilt = run_snapshot(self.spark, manifest, None, self.root / 'new-data')
        self.assertFalse(any(stage['reused'] for stage in rebuilt.values()))
        totals = self.spark.read.parquet(str(Path(rebuilt['gold']['path']) / 'daily_network_service'))
        self.assertEqual(totals.agg({'scheduled_departures': 'sum'}).first()[0], 66)
        with snapshot_xml(manifest):
            self.assertEqual({p.name: p.read_bytes() for p in xml.iterdir()}, original)
        self.assertFalse(xml.exists())

    def test_full_month_retains_service_before_the_fifteenth_and_after_the_twenty_first(self):
        from pipeline.run_snapshot import run_snapshot

        manifest = complete_fixture(self.root / 'bronze', full_month=True)
        outputs = run_snapshot(self.spark, manifest, None, self.root / 'data')
        gold = Path(outputs['gold']['path'])
        network = self.spark.read.parquet(str(gold / 'daily_network_service'))
        self.assertEqual(network.count(), 62)
        self.assertEqual(network.where("service_date='2026-01-01'").agg({'scheduled_departures': 'sum'}).first()[0], 3)
        self.assertEqual(network.where("service_date='2026-01-30'").agg({'scheduled_departures': 'sum'}).first()[0], 3)
        self.assertEqual(network.where("service_date='2026-01-31'").agg({'scheduled_departures': 'sum'}).first()[0], 0)
        self.assertEqual(network.where('NOT is_source_covered').count(), 0)
        saved = json.loads((gold / 'metadata/manifest.json').read_text())
        self.assertEqual(saved['reconciliation']['gold_scheduled_departures'], 66)
        self.assertEqual(saved['reconciliation']['excluded_service_occurrences'], 22)

    def test_invalid_times_are_quarantined_and_the_quality_gate_stops_gold(self):
        from pipeline.run_snapshot import run_snapshot

        manifest = complete_fixture(self.root / 'bronze', bad_time=True)
        # Half of AKT's journeys are quarantined: Gold must not publish internally consistent
        # totals over what is left, so the default rules stop it.
        with self.assertRaisesRegex(RuntimeError, 'Quality gate failed.*AKT:journey_quarantine_rate'):
            run_snapshot(self.spark, manifest, None, self.root / 'data')
        failed = next((self.root / 'data' / 'gold').rglob('metadata/manifest.json'))
        gate = json.loads(failed.read_text())
        self.assertEqual((gate['status'], gate['quality_gate']['status']), ('failed', 'fail'))
        self.assertNotIn('daily_line_service', gate['tables'])
        # An explicit, recorded decision to accept the losses publishes Gold with a warning.
        loose = self.root / 'loose.json'
        loose.write_text(
            json.dumps(
                {
                    'rules': {
                        'journey_quarantine_rate': {'warn': 0.0, 'fail': 1.0},
                        'passing_time_quarantine_rate': {'warn': 0.0, 'fail': 1.0},
                        'invalid_service_occurrence_rate': {'warn': 0.0, 'fail': 1.0},
                        'eligible_service_occurrences_min': {'fail_below': 0},
                    }
                }
            )
        )
        result = run_snapshot(self.spark, manifest, None, self.root / 'data', thresholds_path=loose)
        self.assertTrue(result['silver']['reused'])
        silver = Path(result['silver']['path'])
        gold = Path(result['gold']['path'])
        self.assertEqual(self.spark.read.parquet(str(silver / 'quarantine_passing_times')).count(), 2)
        self.assertEqual(self.spark.read.parquet(str(silver / 'quarantine_journeys')).count(), 2)
        gold_manifest = json.loads((gold / 'metadata/manifest.json').read_text())
        self.assertEqual(gold_manifest['quality_gate']['status'], 'warn')
        self.assertEqual(gold_manifest['reconciliation']['gold_scheduled_departures'], 5)
        self.assertEqual(gold_manifest['reconciliation']['excluded_service_occurrences'], 15)

    def test_calendar_loss_is_measured_before_dates_disappear(self):
        from pipeline.run_snapshot import run_snapshot
        from pipeline.quality.silver_profile import evaluate, profile_silver

        manifest = complete_fixture(self.root / 'bronze', bad_calendar=True)
        result = run_snapshot(self.spark, manifest, None, self.root / 'data', through='silver')
        profile = profile_silver(self.spark, Path(result['silver']['path']))
        self.assertEqual(profile['calendar_impact']['AKT']['affected_journeys'], 2)
        self.assertEqual(profile['service']['AKT']['service_occurrences'], 0)
        # Even a deliberate override of the broad quarantine/zero-service rules
        # cannot conceal the affected journeys behind an empty expanded table.
        gate = evaluate(
            profile,
            {
                'journey_quarantine_rate': {'warn': 0, 'fail': 1.0},
                'eligible_service_occurrences_min': {'fail_below': 0},
            },
        )
        self.assertEqual(gate['metrics']['AKT']['calendar_affected_journey_rate'], 1.0)
        self.assertEqual(gate['status'], 'fail')
        self.assertIn('calendar_affected_journey_rate', {f['rule'] for f in gate['findings'] if f['level'] == 'fail'})

    def test_ambiguous_unversioned_references_do_not_multiply_source_rows(self):
        from pipeline.processing.silver.build import lookup

        source = self.spark.createDataFrame(
            [
                ('unversioned', 'KOL', 'line.xml', 'same', None),
                ('versioned', 'KOL', 'line.xml', 'same', '2'),
                ('missing', 'KOL', 'line.xml', 'absent', None),
            ],
            ['occurrence_key', 'provider', 'source_member', 'ref_id', 'ref_version'],
        )
        target = self.spark.createDataFrame(
            [('first', 'KOL', 'line.xml', 'same', '1'), ('second', 'KOL', 'line.xml', 'same', '2')],
            ['occurrence_key', 'provider', 'source_member', 'entity_id', 'entity_version'],
        )
        rows = {r.occurrence_key: r for r in lookup(source, target, 'ref_id', 'ref_version', 'match_').collect()}
        self.assertEqual(len(rows), 3)
        self.assertFalse(rows['unversioned'].match_resolved)
        self.assertEqual(rows['unversioned'].match_matches, 2)
        self.assertTrue(rows['versioned'].match_resolved)
        self.assertEqual(rows['versioned'].match_entity_version, '2')
        self.assertFalse(rows['missing'].match_resolved)
