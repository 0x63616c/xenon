import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from omes_mixed import validate_result
from omes_workloads import ROOT, sha
from validate_acceptance import validate

spec=importlib.util.spec_from_file_location('mixed_runtime',ROOT/'scripts/ministack-runtime.py')
runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime)

class MixedTests(unittest.TestCase):
 def fixture(self,evidence):
  log=evidence/'command-00.log'
  log.write_text('[Scenario completion summary] Run ID: xenon-full-mixed, Total iterations completed: 40, Total child workflows: 160 (4 per iteration), Total continue-as-new workflows: 40 (1 per iteration), Total workflows completed: 240\n')
  command={'argv':['/actual/omes',*validate(ROOT)['workloads']['mixed'][1:]],'timeout_seconds':900,'exit_code':0,'timed_out':False,'log':log.name,'log_sha256':sha(log)}
  report={'stage':'mixed','profile':'without-faults','workload_completed':True,'full_acceptance':False,'commands':[command]}
  (evidence/'result.json').write_text(json.dumps(report));return report,log
 def test_exact_mixed_summary_and_invocation(self):
  with tempfile.TemporaryDirectory() as tmp:
   evidence=Path(tmp);self.fixture(evidence)
   r=validate_result(ROOT,evidence);self.assertEqual(r['upstream_reported_iterations'],40);self.assertEqual(r['full_history_action_oracle'],'NOT_EXECUTED')
   for change in ('deadline','count','failed','modified-log','summary'):
    report,log=self.fixture(evidence)
    if change=='deadline':report['commands'][0]['timeout_seconds']=901
    if change=='count':report['commands'][0]['argv'][report['commands'][0]['argv'].index('--iterations')+1]='39'
    if change=='failed':report['workload_completed']=False
    if change=='modified-log':log.write_text('modified')
    if change=='summary':
     log.write_text(log.read_text().replace('iterations completed: 40','iterations completed: 39'));report['commands'][0]['log_sha256']=sha(log)
    (evidence/'result.json').write_text(json.dumps(report))
    with self.assertRaises(ValueError):validate_result(ROOT,evidence)
 def test_modes_are_exclusive_before_setup(self):
  self.assertTrue(runtime.arguments(['--omes-mixed']).omes_mixed)
  self.assertFalse(runtime.arguments([]).omes_mixed)
  for flags in (['--omes-mixed','--fuzz-soak'],['--omes-mixed','--smoke'],['--smoke','--fuzz-soak']):
   with self.assertRaises(SystemExit):runtime.arguments(flags)

if __name__=='__main__':unittest.main()
