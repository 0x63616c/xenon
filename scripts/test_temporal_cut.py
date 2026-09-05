from datetime import datetime, timedelta, timezone
import copy
import json
from pathlib import Path
import signal
import subprocess
import sys
import unittest
from unittest.mock import patch
import uuid
import temporal_cut as cut

class TemporalCutTests(unittest.TestCase):
 def fixture(self):
  identity={'namespace_id':str(uuid.uuid4()),'workflow_id':'known-workflow','run_id':str(uuid.uuid4())}
  control={'session':str(uuid.uuid4()),'incarnation':str(uuid.uuid4()),'stage':'after_await','partition':'history-0','url':'http://127.0.0.1:17551'}
  state={'schema':1,**{k:control[k] for k in ['session','incarnation','stage']},'pid':123,'state':'paused','watch':identity,'selector':{'partition':'history-0','operation_id':str(uuid.uuid4()),'family':'execution','mutation_kind':'UPDATE','command_sha256':'ab'*32},'hit_utc':datetime.now(timezone.utc).isoformat()}
  return control,identity,state
 def test_exact_live_hit_required(self):
  c,w,s=self.fixture();cut.validate_state(s,c,123,'paused',w,s['selector'])
  for field,value in [('pid',124),('session',str(uuid.uuid4())),('incarnation',str(uuid.uuid4())),('hit_utc',(datetime.now(timezone.utc)-timedelta(seconds=6)).isoformat()),('watch',{})]:
   bad=copy.deepcopy(s);bad[field]=value
   with self.assertRaises(ValueError):cut.validate_state(bad,c,123,'paused',w,s['selector'])
  bad=copy.deepcopy(s);bad['selector']['command_sha256']='cd'*32
  with self.assertRaises(ValueError):cut.validate_state(bad,c,123,'paused',w,s['selector'])
  cut.validate_signal(-signal.SIGKILL)
  for value in [0,-signal.SIGTERM,None]:
   with self.assertRaises(ValueError):cut.validate_signal(value)
 def test_expired_candidate_cannot_be_reinterpreted(self):
  c,w,s=self.fixture();s['state']='timed_out'
  with patch.object(cut,'read',return_value=s):
   with self.assertRaises(ValueError):cut.await_stage(c,123,w,'candidate')
 def test_mode_exclusion_precedes_setup(self):
  script=Path(__file__).resolve().parent/'ministack-runtime.py'
  for mode in ['--fuzz-soak','--smoke']:
   result=subprocess.run([sys.executable,str(script),mode,'--process-cut-stage','after_await'],capture_output=True,text=True,timeout=10)
   self.assertEqual(result.returncode,2)
   self.assertIn('not allowed',result.stderr)
 def test_declaration_is_fixed(self):
  path=Path(__file__).resolve().parents[1]/'proof/ministack/process-cut.json';config=json.loads(path.read_text());cut.validate_config(config)
  config['pause_timeout_ms']=6000
  with self.assertRaises(ValueError):cut.validate_config(config)

if __name__=='__main__':unittest.main()
