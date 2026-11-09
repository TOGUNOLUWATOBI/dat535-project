import json
from pathlib import Path
import tempfile
import unittest

from pipeline.checkpoints import Checkpoints


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name); self.state=Checkpoints(self.root/'state.sqlite3');self.addCleanup(self.state.close)
        self.key=self.state.register({'snapshot_date':'2026-01-15','providers':['KOL','AKT'],
            'source_manifest':'bronze/manifest.json','files':[{'source_member':'KOL.xml','source_xml_sha256':'abc'}],'stop_files':[]})
    def operation(self,path):
        (path/'metadata').mkdir(parents=True);(path/'table').mkdir()
        (path/'table/part.parquet').write_bytes(b'fixture-output')
        (path/'metadata/manifest.json').write_text(json.dumps({'status':'complete','tables':{'table':{'path':'table','rows':1}}}))
    def run_stage(self,version='v1',operation=None):
        return self.state.execute(self.key,'staging','input',version,self.root/'outputs',operation or self.operation)
    def test_complete_stage_is_reused_only_when_files_match(self):
        with self.state.lock(self.key):
            first,sha,reused=self.run_stage();self.assertFalse(reused)
            second,sha2,reused=self.run_stage(operation=lambda p:self.fail('complete stage should be reused'))
        self.assertEqual((first,sha),(second,sha2));self.assertTrue(reused)
    def test_failed_attempt_is_preserved_and_next_attempt_can_finish(self):
        def fail(path):path.mkdir(parents=True);(path/'evidence').write_text('kept');raise RuntimeError('injected failure')
        with self.state.lock(self.key):
            with self.assertRaisesRegex(RuntimeError,'injected'):self.run_stage(operation=fail)
            original=Path(self.state.status()[0]['output_path']);self.assertTrue((original/'evidence').exists())
            completed,_,_=self.run_stage()
        self.assertNotEqual(original,completed);self.assertEqual([s['status'] for s in self.state.status()],['failed','complete'])
    def test_tampered_outputs_are_preserved_and_rebuilt(self):
        with self.state.lock(self.key):
            first,_,_=self.run_stage();(first/'table/part.parquet').write_bytes(b'damaged')
            second,_,reused=self.run_stage()
        self.assertFalse(reused);self.assertNotEqual(first,second);self.assertTrue(first.exists())
        self.assertEqual(self.state.status()[0]['status'],'invalidated')
    def test_code_version_changes_require_new_output(self):
        with self.state.lock(self.key):
            first,_,_=self.run_stage();second,_,reused=self.run_stage('v2')
        self.assertNotEqual(first,second);self.assertFalse(reused)
    def test_second_snapshot_lock_is_rejected(self):
        with self.state.lock(self.key):
            with self.assertRaisesRegex(RuntimeError,'Another process'):
                with self.state.lock(self.key):pass
    def test_process_interruption_is_recorded_before_retry(self):
        self.state.db.execute("INSERT INTO stage_attempts(attempt_id,snapshot_key,stage,input_key,implementation,status,output_path,started_at,host,pid) VALUES ('crashed',?,'staging','input','v1','running','old','old','host',1)",(self.key,))
        self.state.db.commit()
        with self.state.lock(self.key):self.run_stage()
        self.assertEqual(next(s for s in self.state.status() if s['attempt_id']=='crashed')['status'],'interrupted')
