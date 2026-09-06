import importlib.util,json,shutil,tempfile,unittest,io
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch
import corrected_fuzz as corrected
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('corrected_runtime',ROOT/'scripts/ministack-runtime.py');runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime)
class CorrectedFuzzControls(unittest.TestCase):
 def test_modes_are_exclusive_and_default_unchanged(self):
  self.assertFalse(runtime.arguments([]).corrected_fuzz_soak)
  self.assertTrue(runtime.arguments(['--corrected-fuzz-soak']).corrected_fuzz_soak)
  for mode in ['--fuzz-soak','--smoke','--omes-mixed']:
   with redirect_stderr(io.StringIO()),self.assertRaises(SystemExit):runtime.arguments(['--corrected-fuzz-soak',mode])
 def test_profile_rejects_reduced_limits_and_changed_actions(self):
  config,replay,_=corrected.configuration();self.assertEqual(len(replay['commands']),20)
  with tempfile.TemporaryDirectory() as d:
   root=Path(d)
   for name in ['proof/omes-corpus','test/scenarios/omes-signals']:shutil.copytree(ROOT/name,root/name)
   case=root/'test/scenarios/omes-signals/soak.json';value=json.loads(case.read_text());value['minimum_seconds']=3599;case.write_text(json.dumps(value))
   with self.assertRaises(ValueError):corrected.configuration(root)
   case.write_text(json.dumps(config));replay_path=root/'test/scenarios/omes-signals/replay.json';value=json.loads(replay_path.read_text());value['commands'][0][value['commands'][0].index('900s')]='901s';replay_path.write_text(json.dumps(value))
   with self.assertRaises(ValueError):corrected.configuration(root)
 def test_overlay_artifact_tamper_rejected(self):
  with tempfile.TemporaryDirectory() as d:
   bundle=Path(d);source=bundle/'source';prepared=source/'workers/go/prepared';prepared.mkdir(parents=True);(prepared/'program').write_text('worker');pb=source/'loadgen/kitchensink/kitchen_sink.pb.go';pb.parent.mkdir(parents=True);pb.write_text('schema');binary=bundle/'omes';binary.write_text('cli')
   manifest={'prepared':True,'compatibility_overlay':True,'upstream_commit':corrected.OMES,'protoc_version':'36.0','protoc_gen_go_version':'v1.31.0','patch_sha256':corrected.sha(corrected.CASE/'overlay.patch'),'contract_sha256':corrected.sha(corrected.CASE/'contract.json'),'source':str(source),'binary':str(binary),'source_sha256':corrected.source_tree(source),'prepared_sha256':corrected.tree(prepared),'binary_sha256':corrected.sha(binary),'generated_pb_sha256':corrected.sha(pb)}
   corrected.check_build(manifest,bundle/'build.json')
   for path in [pb,prepared/'program',binary]:
    before=path.read_bytes();path.write_bytes(before+b'changed')
    with self.assertRaises(ValueError):corrected.check_build(manifest,bundle/'build.json')
    path.write_bytes(before)

class PreparationInterruptionControl(unittest.TestCase):
 def test_preparation_interruption_retains_failed_manifest_and_kills_child(self):
  import os,signal,subprocess,sys,time
  with tempfile.TemporaryDirectory() as d:
   directory=Path(d);ready=directory/'child.pid'
   code=r'''
import importlib.util,json,sys,subprocess,tarfile
from pathlib import Path
from unittest.mock import patch
from contextlib import nullcontext
sys.path.insert(0,sys.argv[1]+"/scripts")
spec=importlib.util.spec_from_file_location("preparation",sys.argv[1]+"/scripts/prepare-corrected-omes.py");m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);m.install_signal_handlers()
from omes_workloads import command
root=Path(sys.argv[2]);m.CASE=root
(root/"overlay.patch").write_text("fixture")
(root/"contract.json").write_text(json.dumps({"overlay_sha256":m.sha(root/"overlay.patch")}))
class Archive:
 def extractall(self,*args,**kwargs):pass
child="import os,time;from pathlib import Path;Path("+repr(str(root/"child.pid"))+").write_text(str(os.getpid()));time.sleep(300)"
def launch(argv,path,timeout,cwd,env):return command([sys.executable,"-c",child],path,timeout,cwd,env)
with patch.object(m,"output",side_effect=lambda argv,cwd:m.OMES if "rev-parse" in argv else ""),patch.object(m.subprocess,"run"),patch.object(m.tarfile,"open",return_value=nullcontext(Archive())),patch.object(m,"command",side_effect=launch):
 result=m.prepare(root,root/"bundle")
sys.exit(0 if result["prepared"] else 1)
'''
   process=subprocess.Popen([sys.executable,'-c',code,str(ROOT),str(directory)],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
   try:
    deadline=time.monotonic()+5
    while not ready.exists() and time.monotonic()<deadline:time.sleep(.01)
    self.assertTrue(ready.exists(),'fixture did not reach active build child')
    pid=int(ready.read_text());process.terminate();stdout,stderr=process.communicate(timeout=5)
    self.assertEqual(process.returncode,1,(stdout,stderr))
    manifest=json.loads((directory/'bundle/build.json').read_text());self.assertFalse(manifest['prepared']);self.assertTrue(manifest['commands'][0]['interrupted']);self.assertEqual(manifest['commands'][0]['exit_code'],-signal.SIGKILL)
    with self.assertRaises(ProcessLookupError):os.kill(pid,0)
   finally:
    if process.poll() is None:process.kill();process.wait(timeout=5)

if __name__=='__main__':unittest.main()
