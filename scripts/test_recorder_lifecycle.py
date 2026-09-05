import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import recorder_lifecycle

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('recorder_runtime_control', ROOT / 'scripts/ministack-runtime.py')
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)

PRODUCER = r'''
import json,os,time,urllib.request
phase=os.environ['XENON_RPC_RECORDER_PHASE'];producer=os.environ['XENON_RPC_RECORDER_PRODUCER']
def send(e):
 req=urllib.request.Request(os.environ['XENON_RPC_RECORDER_URL']+'/event',json.dumps(e).encode(),{'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=3) as r:
  if r.status!=204:raise RuntimeError('not acknowledged')
send({'kind':'register','phase':phase,'producer':producer,'id':'one','sequence':1,'family':'shard','measurement':'rpc_invocation'})
print('BEGIN_ACK',flush=True)
if os.environ['CONTROL']=='killed':time.sleep(300)
send({'kind':'terminal','phase':phase,'producer':producer,'id':'one','sequence':1,'status':'completed','result_status':'OK','duration_ns':100})
if os.environ['CONTROL']!='missing_marker':print('TEMPORAL_TRACE_CLOSED',flush=True)
'''

class RecorderLifecycleControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.build = tempfile.TemporaryDirectory()
        cls.binary = Path(cls.build.name) / 'recorder'
        env = {**os.environ, 'GOTOOLCHAIN': 'go1.27.1', 'GOENV': 'off', 'GOWORK': 'off', 'GOFLAGS': '-mod=readonly'}
        subprocess.run(['go','build','-o',str(cls.binary),'./cmd/xenon-trace-recorder'],cwd=ROOT,env=env,check=True,timeout=120)
    @classmethod
    def tearDownClass(cls):
        cls.build.cleanup()
    def scenario(self, mode):
        with tempfile.TemporaryDirectory() as directory:
            evidence = Path(directory)
            processes = []
            def launch(name, argv, extra=None):
                p = runtime.Process(argv,ROOT,{**os.environ,**(extra or {})},evidence/(name+'.log'))
                processes.append(p)
                return p
            lifecycle = recorder_lifecycle.RecorderLifecycle(evidence,self.binary,launch,dict(recorder_lifecycle.EXPECTED))
            try:
                extra = lifecycle.environment('temporal-a')
                first = launch('temporal-a',[sys.executable,'-u','-c',PRODUCER],{**extra,'CONTROL':mode})
                lifecycle.bind('temporal-a',first)
                first.line('BEGIN_ACK',timeout=5)
                if mode == 'killed':
                    lifecycle.declare_kill(first)
                    first.stop(kill=True)
                    self.assertIsNone(lifecycle.process.process.poll(), 'recorder shared killed group')
                    # Fresh incarnation after simulated cold replacement, same phase.
                    second = launch('temporal-b',[sys.executable,'-u','-c',PRODUCER],{**lifecycle.environment('temporal-b'),'CONTROL':'graceful'})
                    lifecycle.bind('temporal-b',second)
                    second.process.wait(timeout=5);second.stop()
                else:
                    first.process.wait(timeout=5);first.stop()
                if mode=="recorder_loss":lifecycle.process.stop(kill=True)
                report = lifecycle.finalize()
                self.assertEqual(report,json.loads((evidence/'recorder-result.json').read_text()))
                if mode=='recorder_loss':
                    self.assertFalse(report['registration_census_complete'])
                    self.assertTrue(report['errors'])
                elif mode=='missing_marker':
                    self.assertFalse(report['registration_census_complete'])
                    self.assertIn('missing_graceful_close_or_kill',report['errors'])
                else:
                    self.assertTrue(report['registration_census_complete'],report)
                    self.assertEqual(report['census']['completed'],1)
                    self.assertEqual(len(report['census']['terminal_unobserved']),1 if mode=='killed' else 0)
                    self.assertFalse(report['steady_gate_executed'])
                    self.assertFalse(report['fault_latency_distribution_complete'])
            finally:
                for process in reversed(processes):process.stop()
    def test_declared_kill_and_fresh_incarnation(self):self.scenario('killed')
    def test_recorder_loss_fails(self):self.scenario('recorder_loss')
    def test_graceful_producer(self):self.scenario('graceful')
    def test_missing_close_marker_fails(self):self.scenario('missing_marker')
    def test_contract_changes_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'config.json'
            value=dict(recorder_lifecycle.EXPECTED);value['phase_type']='steady';path.write_text(json.dumps(value))
            with self.assertRaises(ValueError):recorder_lifecycle.config(path)

if __name__=='__main__':unittest.main()
