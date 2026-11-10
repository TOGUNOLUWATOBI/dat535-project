from datetime import date
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from pipeline.processing.windows import service_window, window_signature
from pipeline.rebuild_monthly import audit_coverage, ensure_support_months
from pipeline.analysis.route_history import find_inputs, HistoryInputError
from pipeline.ingestion.prepare_monthly_support import existing_source, prepare


class ServiceWindowTests(unittest.TestCase):
    def test_default_includes_first_and_last_calendar_day_including_leap_years(self):
        for snapshot, first, last in [("2026-01-15","2026-01-01","2026-01-31"),
                                      ("2026-02-15","2026-02-01","2026-02-28"),
                                      ("2028-02-15","2028-02-01","2028-02-29"),
                                      ("2026-12-31","2026-12-01","2026-12-31")]:
            with self.subTest(snapshot=snapshot):
                self.assertEqual(tuple(map(str,service_window(snapshot))),(first,last))

    def test_custom_window_is_explicit_and_bounded(self):
        self.assertEqual(service_window("2026-01-15","2026-01-15","2026-01-21"),
                         (date(2026,1,15),date(2026,1,21)))
        with self.assertRaises(ValueError):
            service_window("2026-01-15","2026-01-31","2026-01-01")

    def test_month_lengths_and_observation_day_do_not_change_month_policy(self):
        for day, end in [("2026-01-15","31"),("2026-02-01","28"),("2028-02-15","29")]:
            item={"snapshot_date":day,"service_from":day[:8]+"01","service_to":day[:8]+end}
            self.assertEqual(window_signature(item),("calendar_month",))
        self.assertEqual(window_signature({"snapshot_date":"2026-01-14","service_from":"2026-02-01",
                                           "service_to":"2026-02-28"}),("calendar_month",))

    def test_coverage_audit_does_not_hide_missing_early_dates(self):
        base={"snapshot_date":"2026-01-15","service_from":"2026-01-01","service_to":"2026-01-31",
              "providers":["AKT","KOL"],"source_calendar_coverage":{
                  p:{"from":"2026-01-13","to":"2026-01-31"} for p in ("AKT","KOL")}}
        result=audit_coverage([base],["2026-01"])["2026-01"]
        self.assertFalse(result["complete"])
        self.assertEqual(len(result["missing_dates"]["KOL"]),12)
        support={**base,"snapshot_date":"2026-01-01","source_calendar_coverage":{
            p:{"from":"2026-01-01","to":"2026-01-31"} for p in ("AKT","KOL")}}
        self.assertTrue(audit_coverage([base,support],["2026-01"])["2026-01"]["complete"])

    def test_existing_failed_download_is_not_redownloaded_or_overwritten(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            failed={"status":"failed","snapshots":[{"snapshot_date":"2026-02-01","status":"failed"}]}
            path=root/'manifest.json';path.write_text(json.dumps(failed))
            with self.assertRaisesRegex(RuntimeError,'Previously failed source'):
                existing_source(root,date(2026,2,1))
            self.assertEqual(json.loads(path.read_text()),failed)

    def test_reversed_support_range_does_not_create_an_empty_completed_plan(self):
        with tempfile.TemporaryDirectory() as temp:
            output=Path(temp)/'support'
            with self.assertRaises(ValueError):
                prepare(output,date(2026,9,1),date(2026,1,1),['KOL','AKT'])
            self.assertFalse(output.exists())

    def test_support_source_does_not_reuse_a_primary_observation_date(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp).resolve()
            with patch('pipeline.ingestion.prepare_monthly_support.existing_source',return_value=root/'source.json'), \
                 patch('pipeline.ingestion.prepare_monthly_support.covers_month',return_value=True):
                path=prepare(root,date(2026,2,1),date(2026,2,1),['KOL','AKT'],excluded_dates=['2026-02-01'])
            plan=json.loads(path.read_text())
            self.assertEqual(plan['status'],'complete')
            self.assertEqual(plan['jobs'][0]['snapshot_date'],'2026-01-14')

    def test_rebuild_extends_historical_support_for_registered_scheduled_months(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            def save(name,month,day):
                first,last=map(str,service_window(month+'-15'))
                path=root/name
                path.write_text(json.dumps({'status':'complete','providers':['KOL','AKT'],'jobs':[
                    {'snapshot_date':day,'manifest':'source.json','service_from':first,'service_to':last}]}))
                return path
            historical=save('historical.json','2026-01','2026-01-01')
            november=save('november.json','2026-11','2026-11-01')
            original=historical.read_bytes()
            with patch('pipeline.ingestion.prepare_monthly_support.prepare',return_value=november) as prepare_mock:
                jobs=ensure_support_months([{'snapshot_date':'2026-01-15'},{'snapshot_date':'2026-11-08'}],historical)
            self.assertEqual({j['service_from'][:7] for j in jobs},{'2026-01','2026-11'})
            self.assertIn('2026-11-08',prepare_mock.call_args.kwargs['excluded_dates'])
            self.assertEqual(historical.read_bytes(),original)


class HistoryWindowSelectionTests(unittest.TestCase):
    def fixture(self, root, weekly_february=False):
        db=sqlite3.connect(root/'pipeline_state.sqlite3')
        db.executescript('''CREATE TABLE snapshots(snapshot_key TEXT,snapshot_date TEXT,providers TEXT);
            CREATE TABLE stage_attempts(snapshot_key TEXT,stage TEXT,status TEXT,output_path TEXT,
                inventory_json TEXT,manifest_sha256 TEXT,finished_at TEXT);''')
        for day,role in [('2026-01-15','primary'),('2026-02-15','primary'),('2026-01-01','calendar_support')]:
            first,last=map(str,service_window(day))
            if weekly_february and day=='2026-02-15': first,last='2026-02-15','2026-02-21'
            silver=root/day/'silver';gold=root/day/'gold'
            for folder in (silver,gold): (folder/'metadata').mkdir(parents=True)
            (silver/'metadata/manifest.json').write_text(json.dumps({'service_from':first,'service_to':last}))
            (gold/'metadata/manifest.json').write_text(json.dumps({'providers':['AKT','KOL'],'history_role':role,
                'source_manifest':str(silver/'metadata/manifest.json')}))
            db.execute('INSERT INTO snapshots VALUES (?,?,?)',(day,day,'AKT,KOL'))
            for stage,folder in [('gold',gold),('silver',silver)]:
                db.execute('INSERT INTO stage_attempts VALUES (?,?,\'complete\',?,\'[]\',\'hash\',?)',
                           (day,stage,str(folder),day))
        db.commit();db.close()
        return root/'pipeline_state.sqlite3'

    def test_28_and_31_day_months_work_and_support_does_not_change_observation_sample(self):
        with tempfile.TemporaryDirectory() as temp:
            inputs=find_inputs(self.fixture(Path(temp)),verify=False)
            self.assertEqual([i['snapshot_date'] for i in inputs],['2026-01-15','2026-02-15'])

    def test_old_weekly_and_new_monthly_results_cannot_be_silently_mixed(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(HistoryInputError):
                find_inputs(self.fixture(Path(temp),weekly_february=True),verify=False)


if __name__ == "__main__":
    unittest.main()
