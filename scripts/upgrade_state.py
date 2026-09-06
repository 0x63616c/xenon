#!/usr/bin/env python3
"""Explicit local old/target bundle rehearsal of durable workflow continuation."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import platform
import subprocess
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
SCENARIO=ROOT/'test/scenarios/ministack'
spec=importlib.util.spec_from_file_location('upgrade_runtime',ROOT/'scripts/ministack-runtime.py')
runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime)
class Interrupted(RuntimeError):pass

NAMES=('xenon-go-node','xenon-temporal','xenon-topology','xenon-sdk-probe')

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def output(argv,cwd=None):
    env={**os.environ,'GOTOOLCHAIN':'local','GOENV':'off','GOWORK':'off'}
    return subprocess.check_output(argv,cwd=cwd,env=env,text=True,stderr=subprocess.PIPE,timeout=30).strip()
def source(path,commit=None):
    path=Path(path).resolve()
    head=output(['git','rev-parse','HEAD'],path)
    if output(['git','status','--porcelain=v1','--untracked-files=all'],path):raise ValueError('dirty source checkout: '+str(path))
    if commit and head!=commit:raise ValueError('source revision changed')
    return path,head

def inspect_bundle(xenon_source,temporal_source,temporal_commit,temporal_version):
    repo,commit=source(xenon_source)
    temporal,actual=source(temporal_source,temporal_commit)
    if output(['git','rev-parse','--verify','--end-of-options',temporal_version+'^{commit}'],temporal)!=actual:raise ValueError('Temporal version tag does not identify selected source commit')
    native=repo/'.local/go-node-build.json';manifest=json.loads(native.read_text())
    pins=json.loads((repo/'tools/slatedb-native.json').read_text())
    native_source,native_commit=source(repo/'.local/slatedb-native-source',pins['source_commit'])
    if manifest['source_commit']!=native_commit or manifest['source_clean'] is not True or manifest['source_cargo_lock_sha256']!=sha(native_source/'Cargo.lock'):raise ValueError('native build provenance mismatch')
    library=(repo/manifest['shared_library']).resolve()
    if repo not in library.parents or sha(library)!=manifest['shared_library_sha256']:raise ValueError('native library changed or escaped source')
    artifacts={}
    for name in NAMES:
        path=repo/'.local/bin'/name
        info=output(['go','version','-m',str(path)])
        lines=[line.strip().split() for line in info.splitlines()]
        if ['build','vcs.revision='+commit] not in lines or ['build','vcs.modified=false'] not in lines:raise ValueError('binary/source VCS provenance mismatch: '+name)
        expected_path='github.com/0x63616c/xenon/cmd/'+name
        if ['path',expected_path] not in lines:raise ValueError('wrong binary entrypoint')
        if name=='xenon-temporal':
            lines=[line.strip().split() for line in info.splitlines()]
            matches=[i for i,line in enumerate(lines) if len(line)>=3 and line[:2]==['dep','go.temporal.io/server']]
            if len(matches)!=1 or lines[matches[0]][2]!=temporal_version or (matches[0]+1<len(lines) and lines[matches[0]+1][0]=='=>'):raise ValueError('embedded Temporal dependency mismatch or unsupported replacement')
        artifacts[name]={'path':str(path),'sha256':sha(path),'go_build_info':info}
    if artifacts['xenon-go-node']['sha256']!=manifest['node_binary_sha256']:raise ValueError('node/native manifest binding mismatch')
    source(repo,commit);source(temporal,actual);source(native_source,native_commit)
    return {'schema':1,'xenon_source':str(repo),'xenon_commit':commit,'temporal_source':str(temporal),'temporal_commit':actual,'temporal_version':temporal_version,'artifacts':artifacts,'native_manifest':str(native),'native_manifest_sha256':sha(native),'native_library':str(library),'native_library_sha256':sha(library)}

def validate_bundle(path):
    value=json.loads(Path(path).read_text())
    actual=inspect_bundle(value['xenon_source'],value['temporal_source'],value['temporal_commit'],value['temporal_version'])
    if actual!=value:raise ValueError('bundle metadata does not match inspected source/build artifacts')
    return value

def validate_pair(plan):
    if set(plan)!={'schema','old','target','rehearsal'} or plan['schema']!=1 or type(plan['rehearsal']) is not bool:raise ValueError('invalid explicit upgrade plan')
    if any(not isinstance(plan[name],str) or not Path(plan[name]).is_absolute() for name in ('old','target')):raise ValueError('bundle paths must be explicit absolute paths')
    pair={name:validate_bundle(plan[name]) for name in ('old','target')}
    same=(pair['old']['temporal_commit']==pair['target']['temporal_commit'] or pair['old']['temporal_version']==pair['target']['temporal_version'] or pair['old']['artifacts']['xenon-temporal']['sha256']==pair['target']['artifacts']['xenon-temporal']['sha256'])
    if same and not plan['rehearsal']:raise ValueError('identical Temporal version/binary pair is only a rehearsal, not an upgrade')
    return pair

class Runtime:
    def __init__(self,pair,evidence,report):
        self.pair=pair;self.evidence=evidence;self.report=report;self.processes=[];self.phase_processes=[];self.sequence=0;self.probe_timeout=120
        self.project='xenon-upgrade-'+uuid.uuid4().hex[:12]
        self.compose=['docker','compose','--project-name',self.project,'-f',str(SCENARIO/'config/compose.json')]
        self.env={k:os.environ[k] for k in ('PATH','HOME','USER','TMPDIR') if k in os.environ}
        self.env.update(AWS_ACCESS_KEY_ID='xenon-local',AWS_SECRET_ACCESS_KEY='xenon-local-test-only',AWS_DEFAULT_REGION='us-east-1',AWS_ENDPOINT='http://127.0.0.1:19006',AWS_ALLOW_HTTP='true',AWS_VIRTUAL_HOSTED_STYLE_REQUEST='false',XENON_BUCKET='xenon-upgrade-proof',XENON_TOPOLOGY_PREFIX='metadata',SLATEDB_UNIFFI_RUNTIME_THREADS='2')
        self.sdk=pair['old']['artifacts']['xenon-sdk-probe']['path']
        library=str(Path(pair['old']['native_library']).parent)
        self.env.update(LD_LIBRARY_PATH=library,DYLD_LIBRARY_PATH=library)
        self.namespace='xenon-upgrade';self.queue='xenon-upgrade-worker'
        self.report.update(project=self.project,store={'endpoint':self.env['AWS_ENDPOINT'],'bucket':self.env['XENON_BUCKET'],'directory_prefix':'metadata','data_prefix':'data/'})
    def launch(self,name,argv,env=None):
        folder=self.evidence/name;folder.mkdir()
        p=runtime.Process(argv,folder,env or self.env,self.evidence/(name+'.log'),stop_timeout=30);self.processes.append(p);return p
    def run(self,argv,timeout=60,env=None):
        self.sequence+=1;p=self.launch('command-'+str(self.sequence),argv,env)
        try:
            p.process.wait(timeout=timeout)
            if p.process.returncode:raise RuntimeError('command failed: '+str(argv))
        finally:
            p.stop()
            self.report.setdefault('commands',[]).append({'argv':argv,'log':Path(p.log.name).name,'sha256':sha(p.log.name),'returncode':p.process.returncode})
        return Path(p.log.name).read_text()
    def probe(self,mode,id='upgrade-active',extra=()):
        text=self.run([self.sdk,'--mode',mode,'--address','127.0.0.1:17233','--namespace',self.namespace,'--workflow-id',id,'--task-queue',self.queue,*extra],getattr(self,'probe_timeout',120))
        values=[json.loads(line) for line in text.splitlines() if line.startswith('{')]
        if not values:raise ValueError('missing probe result')
        return values[-1]
    def wait(self,check,seconds=120):
        deadline=time.monotonic()+seconds;last=None
        while time.monotonic()<deadline:
            try:
                self.probe_timeout=max(.1,deadline-time.monotonic())
                value=check()
                if value:return value
            except Interrupted:raise
            except Exception as error:last=str(error)
            finally:self.probe_timeout=120
            time.sleep(.25)
        raise TimeoutError('upgrade progress deadline: '+str(last))
    def setup(self):
        # Fail before starting containers if another scoped runtime owns these ports.
        for port in (19006,17935,17233,17243,17351,17352,17353,18233,19233,18243,19243,18234,18235,18236,18300,18301,18302,18303):
            with socket.socket() as listener:listener.bind(('0.0.0.0',port))
        self.report['tools']={name:output(argv) for name,argv in {'docker':['docker','--version'],'compose':['docker','compose','version'],'aws':['aws','--version'],'go':['go','version']}.items()}
        self.run([*self.compose,'up','-d','--pull','never','s3','ingress'],120)
        import urllib.request
        def ready():
            with urllib.request.urlopen(self.env['AWS_ENDPOINT']+'/minio/health/live',timeout=2) as r:return r.status==200
        self.wait(ready,30)
        self.run(['aws','--endpoint-url',self.env['AWS_ENDPOINT'],'s3api','create-bucket','--bucket',self.env['XENON_BUCKET']])
    def start(self,phase):
        bundle=self.pair[phase];library=str(Path(bundle['native_library']).parent)
        env={**self.env,'LD_LIBRARY_PATH':library,'DYLD_LIBRARY_PATH':library,'XENON_NODE':phase,'XENON_LISTEN':'0.0.0.0:17351','XENON_ADVERTISE':'127.0.0.1:17351','XENON_MAX_OUTCOMES':'1000000'}
        node=self.launch(phase+'-node',[bundle['artifacts']['xenon-go-node']['path']],env)
        _,name,incarnation,address=node.line('INGRESS ',timeout=30).split()
        case=json.loads((SCENARIO/'case.json').read_text())
        topology={'members':{name:{'address':address,'incarnation':incarnation}},'partitions':{p:{'node':name,'data_prefix':'data/'+p} for p in case['partitions']}}
        path=self.evidence/(phase+'-topology.json');path.write_text(json.dumps(topology))
        self.run([bundle['artifacts']['xenon-topology']['path'],str(path)],env=env)
        self.run([self.sdk,'--mode','partition-ready','--storage-address','127.0.0.1:17935','--storage-partition','global','--readiness-timeout','60s'],90,env)
        server=self.launch(phase+'-temporal',[bundle['artifacts']['xenon-temporal']['path'],'--config',str(SCENARIO/'config/temporal-a.json')],env)
        self.phase_processes=[node,server]
        server.line('TEMPORAL_STARTED',timeout=120);self.wait(lambda:self.probe('health'))
    def worker(self,phase):
        worker=self.launch(phase+'-worker',[self.sdk,'--mode','worker','--address','127.0.0.1:17233','--namespace',self.namespace,'--task-queue',self.queue])
        worker.line('{',timeout=30);self.phase_processes.append(worker)
    def stop(self):
        roles=['owner','server','worker'][:len(self.phase_processes)]
        for role,p in reversed(list(zip(roles,self.phase_processes))):
            p.stop()
            self.report.setdefault('phase_stops',[]).append({'role':role,'returncode':p.process.returncode})
            expected=-signal.SIGTERM if role=='owner' else 0
            if p.process.returncode!=expected:raise RuntimeError(role+' did not stop with the declared exit policy')
        self.phase_processes=[]
    def objects(self,label):
        raw=self.run(['aws','--endpoint-url',self.env['AWS_ENDPOINT'],'s3api','list-objects-v2','--bucket',self.env['XENON_BUCKET']])
        value=json.loads(raw)
        if not value.get('Contents'):raise ValueError('old state object inventory is empty')
        canonical=json.dumps(sorted(value['Contents'],key=lambda item:item['Key']),sort_keys=True)
        path=self.evidence/(label+'-objects.json');path.write_text(canonical)
        return sha(path)
    def verify(self,id,run,label):
        folder=self.evidence/label;folder.mkdir()
        result=self.probe('verify',id,('--run-id',run,'--output',str(folder)))
        paths=list(folder.glob('history-*.json'))
        if len(paths)!=2:raise ValueError('incomplete two-run history')
        self.report.setdefault('history_files',{})[label]={p.name:sha(p) for p in paths}
        hashes={p.name:hashlib.sha256(json.dumps(json.loads(p.read_text()),sort_keys=True,separators=(',',':')).encode()).hexdigest() for p in paths}
        return {'result':result,'canonical_history_sha256':hashes}
    def cleanup(self):
        errors=[]
        for p in reversed(self.processes):
            try:p.stop()
            except Exception as e:errors.append(str(e))
        try:
            self.run([*self.compose,'down','--volumes'],60)
            for argv in (['docker','ps','-aq','--filter','label=com.docker.compose.project='+self.project],['docker','volume','ls','-q','--filter','label=com.docker.compose.project='+self.project]):
                if self.run(argv).strip():raise ValueError('scoped upgrade resources survived cleanup')
        except Exception as e:errors.append(str(e))
        if errors:raise RuntimeError('; '.join(errors))

def exercise(ops,report):
    report['stage']='setup';ops.setup();ops.start('old');ops.wait(lambda:ops.probe('bootstrap'));ops.worker('old')
    report['stage']='old-state'
    completed=ops.probe('start','upgrade-completed');ops.wait(lambda:ops.probe('phase','upgrade-completed').get('phase')=='await-control')
    ops.probe('control','upgrade-completed');before=ops.verify('upgrade-completed',completed['run_id'],'completed-before')
    active=ops.probe('start');ops.wait(lambda:ops.probe('phase').get('phase')=='await-control')
    report['old_executions']={'completed':completed,'active':active};report['completed_before']=before
    ops.stop();saved=ops.objects('old-stopped');report['old_objects_sha256']=saved
    report['stage']='target-start'
    if ops.objects('before-target')!=saved:raise ValueError('object store changed between stopped phases')
    ops.start('target');ops.worker('target');ops.wait(lambda:ops.probe('phase').get('phase')=='await-control')
    after=ops.verify('upgrade-completed',completed['run_id'],'completed-after')
    if after!=before:raise ValueError('old completed history/result changed')
    report['completed_after']=after;report['stage']='target-continuation'
    ops.probe('control');report['continued']=ops.verify('upgrade-active',active['run_id'],'active-after')
    for id in ('upgrade-active','upgrade-completed'):
        ops.wait(lambda:ops.probe('visibility',id,('--query',"WorkflowId = '"+id+"' AND ExecutionStatus = 'Completed' AND XenonProof = 'durable'")))
    report['target_objects_sha256']=ops.objects('target-completed');report['stage']='complete'

def execute(plan,evidence,backend=Runtime):
    evidence=Path(evidence).resolve();evidence.mkdir(parents=True,exist_ok=False)
    report={'schema':1,'result':'failed','full_acceptance':False,'fresh_install':'NOT_TESTED','existing_state_upgrade':'NOT_TESTED','component_pass':False,'stage':'preflight','plan':plan,'plan_sha256':hashlib.sha256(json.dumps(plan,sort_keys=True).encode()).hexdigest(),'python':sys.version,'host':platform.platform()}
    ops=None
    try:
        repo,commit=source(ROOT);report['source_commit']=commit
        report['scenario_sha256']={str(p.relative_to(ROOT)):sha(p) for p in SCENARIO.rglob('*') if p.is_file()}
        pair=validate_pair(plan);report['bundles']=pair
        ops=backend(pair,evidence,report);exercise(ops,report)
        validate_pair(plan);source(repo,commit)
        if any(sha(ROOT/p)!=h for p,h in report['scenario_sha256'].items()):raise ValueError('scenario inputs changed')
        report.update(result='rehearsal-passed' if plan['rehearsal'] else 'component-passed',component_pass=True)
        if not plan['rehearsal']:report['existing_state_upgrade']='WORKFLOW_COMPONENT_PASSED; full existing-state coverage NOT_TESTED'
    except Exception as error:report['error']=type(error).__name__+': '+str(error)
    finally:
        if ops:
            try:ops.cleanup();report['cleanup']='complete'
            except Exception as error:report.update(result='failed',component_pass=False,existing_state_upgrade='NOT_TESTED',cleanup_error=str(error))
        (evidence/'result.json').write_text(json.dumps(report,indent=2)+'\n')
    return report

def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='mode',required=True)
    bundle=sub.add_parser('bundle');bundle.add_argument('--xenon-source',required=True);bundle.add_argument('--temporal-source',required=True);bundle.add_argument('--temporal-commit',required=True);bundle.add_argument('--temporal-version',required=True);bundle.add_argument('--output',type=Path,required=True)
    run=sub.add_parser('run');run.add_argument('--plan',type=Path,required=True);run.add_argument('--evidence',type=Path,required=True)
    args=parser.parse_args()
    if args.mode=='bundle':
        value=inspect_bundle(args.xenon_source,args.temporal_source,args.temporal_commit,args.temporal_version)
        with args.output.open('x') as stream:json.dump(value,stream,indent=2)
        return 0
    plan=json.loads(args.plan.read_text());report=execute(plan,args.evidence);print(json.dumps({'result':report['result'],'component_pass':report['component_pass']}));return 0 if report['component_pass'] else 1

if __name__=='__main__':
    def interrupted(signum,frame):raise Interrupted('upgrade controller interrupted: '+str(signum))
    for sig in (signal.SIGINT,signal.SIGTERM,signal.SIGALRM):signal.signal(sig,interrupted)
    signal.alarm(1800)
    raise SystemExit(main())
