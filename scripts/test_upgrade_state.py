import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('upgrade_state',Path(__file__).with_name('upgrade_state.py'))
u=importlib.util.module_from_spec(spec);spec.loader.exec_module(u)

class FakeRuntime:
    instances=[]
    def __init__(self,pair,evidence,report):
        self.events=[];self.phase=None;self.cleaned=False;self.instances.append(self)
    def setup(self):self.events.append('new-store')
    def start(self,phase):
        if phase=='target':assert self.events[-1]=='old-stopped'
        self.phase=phase;self.events.append(phase)
    def worker(self,phase):pass
    def wait(self,check,seconds=120):assert check()
    def probe(self,mode,id='upgrade-active',extra=()):
        if mode=='start':return {'run_id':id+'-old-run','workflow_id':id}
        if mode=='phase':return {'phase':'await-control'}
        return {'okay':True}
    def verify(self,id,run,label):
        assert run==id+'-old-run'
        return {'history_sha256':{'first':'immutable','second':'immutable'},'result':{'completed':True}}
    def stop(self):self.events.append('old-stopped')
    def objects(self,label):return 'same-object-manifest'
    def cleanup(self):self.cleaned=True

class UpgradeControls(unittest.TestCase):
    def test_phase_stop_accepts_only_expected_owner_replacement(self):
        class Process:
            def __init__(self,code):self.process=type('P',(),{'returncode':code})()
            def stop(self):pass
        runtime=u.Runtime.__new__(u.Runtime);runtime.report={}
        runtime.phase_processes=[Process(-15),Process(0),Process(0)]
        runtime.stop()
        self.assertEqual(runtime.report['phase_stops'],[{'role':'worker','returncode':0},{'role':'server','returncode':0},{'role':'owner','returncode':-15}])

    def test_real_state_flow_and_same_run_identity(self):
        fake=FakeRuntime({},None,{})
        result={};u.exercise(fake,result)
        self.assertEqual(result['stage'],'complete')
        self.assertEqual(fake.events,['new-store','old','old-stopped','target'])
        self.assertEqual(result['completed_before'],result['completed_after'])
    def test_changed_objects_prevent_target(self):
        fake=FakeRuntime({},None,{})
        fake.objects=lambda label:label
        with self.assertRaisesRegex(ValueError,'object store changed'):u.exercise(fake,{})
        self.assertNotIn('target',fake.events)
    def test_failed_continuation_receipt_and_cleanup(self):
        class Broken(FakeRuntime):
            def verify(self,id,run,label):
                if self.phase=='target':raise ValueError('old state cannot decode')
                return super().verify(id,run,label)
        with tempfile.TemporaryDirectory() as directory,patch.object(u,'source',return_value=(u.ROOT,'test-commit')),patch.object(u,'validate_pair',return_value={}):
            result=u.execute({'rehearsal':False},Path(directory)/'evidence',Broken)
            self.assertFalse(result['component_pass']);self.assertEqual(result['existing_state_upgrade'],'NOT_TESTED')
            self.assertTrue(Broken.instances[-1].cleaned)
            self.assertEqual(json.loads((Path(directory)/'evidence/result.json').read_text()),result)
    def test_missing_target_is_failure_not_default(self):
        with tempfile.TemporaryDirectory() as directory,patch.object(u,'source',return_value=(u.ROOT,'test-commit')):
            result=u.execute({'schema':1,'old':'/absent-old-bundle','target':'/absent-target-bundle','rehearsal':False},Path(directory)/'evidence')
            self.assertEqual(result['result'],'failed');self.assertNotIn('project',result)
    def test_identical_pair_requires_explicit_rehearsal(self):
        bundle={'temporal_commit':'same','temporal_version':'v1','artifacts':{'xenon-temporal':{'sha256':'same'}}}
        with patch.object(u,'validate_bundle',return_value=bundle):
            plan={'schema':1,'old':'/old.json','target':'/target.json','rehearsal':False}
            with self.assertRaisesRegex(ValueError,'only a rehearsal'):u.validate_pair(plan)
            plan['rehearsal']=True;self.assertEqual(u.validate_pair(plan)['old'],bundle)
    def test_clean_git_required(self):
        with tempfile.TemporaryDirectory() as directory:
            subprocess.run(['git','init','-q',directory],check=True)
            subprocess.run(['git','-C',directory,'-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','--allow-empty','-qm','fixture'],check=True)
            u.source(directory)
            (Path(directory)/'unexpected').write_text('dirty')
            with self.assertRaisesRegex(ValueError,'dirty source'):u.source(directory)
    def test_bundle_is_recomputed_not_trusted(self):
        value={'xenon_source':'x','temporal_source':'t','temporal_commit':'old','temporal_version':'v1','native_library_sha256':'forged'}
        with tempfile.TemporaryDirectory() as directory,patch.object(u,'inspect_bundle',return_value={**value,'native_library_sha256':'actual'}):
            path=Path(directory)/'bundle.json';path.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError,'does not match'):u.validate_bundle(path)
    def test_inspected_bundle_artifact_and_version_bindings(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);repo=root/'xenon';temporal=root/'temporal'
            def init(path):
                path.mkdir(parents=True)
                subprocess.run(['git','init','-q',str(path)],check=True)
            def commit(path):
                subprocess.run(['git','-C',str(path),'add','.'],check=True)
                subprocess.run(['git','-C',str(path),'-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','--allow-empty','-qm','fixture'],check=True)
                return subprocess.check_output(['git','-C',str(path),'rev-parse','HEAD'],text=True).strip()
            init(repo);init(temporal);tcommit=commit(temporal)
            subprocess.run(['git','-C',str(temporal),'tag','v1.0.0'],check=True)
            native=repo/'.local/slatedb-native-source';init(native);(native/'Cargo.lock').write_text('lock');ncommit=commit(native)
            (repo/'.gitignore').write_text('.local/\n');(repo/'tools').mkdir()
            (repo/'tools/slatedb-native.json').write_text(json.dumps({'source_commit':ncommit}));xcommit=commit(repo)
            binaries=repo/'.local/bin';binaries.mkdir();library=repo/'.local/native.so';library.write_bytes(b'native')
            for name in u.NAMES:(binaries/name).write_text(name)
            manifest={'source_commit':ncommit,'source_clean':True,'source_cargo_lock_sha256':u.sha(native/'Cargo.lock'),'shared_library':'.local/native.so','shared_library_sha256':u.sha(library),'node_binary_sha256':u.sha(binaries/'xenon-go-node')}
            (repo/'.local/go-node-build.json').write_text(json.dumps(manifest))
            actual_output=u.output
            def output(argv,cwd=None):
                if argv[:3]==['go','version','-m']:
                    name=Path(argv[3]).name
                    return 'path\tgithub.com/0x63616c/xenon/cmd/'+name+'\nbuild\tvcs.revision='+xcommit+'\nbuild\tvcs.modified=false\ndep\tgo.temporal.io/server\tv1.0.0\th1:fixture'
                return actual_output(argv,cwd)
            with patch.object(u,'output',side_effect=output):
                value=u.inspect_bundle(repo,temporal,tcommit,'v1.0.0')
                bundle=root/'bundle.json';bundle.write_text(json.dumps(value));self.assertEqual(u.validate_bundle(bundle),value)
                library.write_bytes(b'changed')
                with self.assertRaisesRegex(ValueError,'native library changed'):u.validate_bundle(bundle)
    def test_interrupt_is_not_a_retry(self):
        fake=u.Runtime.__new__(u.Runtime)
        with self.assertRaises(u.Interrupted):fake.wait(lambda:(_ for _ in ()).throw(u.Interrupted('stop')),1)
    def test_command_failure_retains_log_and_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            fake=u.Runtime.__new__(u.Runtime);fake.sequence=0;fake.evidence=Path(directory);fake.processes=[];fake.report={};fake.env={}
            with self.assertRaisesRegex(RuntimeError,'command failed'):
                fake.run(['/bin/sh','-c','echo failure-evidence; exit 7'])
            receipt=fake.report['commands'][0]
            self.assertEqual(receipt['returncode'],7)
            self.assertIn('failure-evidence',(Path(directory)/receipt['log']).read_text())

if __name__=='__main__':unittest.main()
