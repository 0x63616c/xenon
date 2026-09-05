#!/usr/bin/env python3
"""Actual local runtime controller. Missing factories/builds are failures, not skips."""
import hashlib
import json
import os
from pathlib import Path
import queue
import platform
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

ROOT=Path(__file__).resolve().parents[1]
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
class Process:
    def __init__(self,argv,cwd,env,log):
        self.stopped=False;self.lines=queue.Queue(maxsize=1024);self.log=open(log,'w');self.process=subprocess.Popen(argv,cwd=cwd,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,start_new_session=True)
        def drain():
            for line in self.process.stdout:
                self.log.write(line);self.log.flush()
                try:self.lines.put_nowait(line.rstrip())
                except queue.Full:
                    try:self.lines.get_nowait()
                    except queue.Empty:pass
                    self.lines.put_nowait(line.rstrip())
        self.thread=threading.Thread(target=drain,daemon=True);self.thread.start()
    def line(self,prefix,timeout=30):
        end=time.monotonic()+timeout
        while time.monotonic()<end:
            if self.process.poll() is not None and self.lines.empty():raise RuntimeError('process exited before '+prefix)
            try:
                line=self.lines.get(timeout=min(1,max(0.01,end-time.monotonic())))
                if line.startswith(prefix):return line
            except queue.Empty:pass
        raise TimeoutError('waiting for '+prefix)
    def stop(self,kill=False):
        if self.stopped:return
        self.stopped=True
        try:os.killpg(self.process.pid,signal.SIGKILL if kill else signal.SIGTERM)
        except ProcessLookupError:pass
        if self.process.poll() is None:
            try:self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:os.killpg(self.process.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                self.process.wait(timeout=10)
        self.thread.join(timeout=5)
        if self.thread.is_alive():raise RuntimeError("process output did not drain after shutdown")
        self.process.stdout.close();self.log.close()

def main():
    case=json.loads((ROOT/'proof/ministack/case.json').read_text());pins=json.loads((ROOT/'tools/ministack.json').read_text())
    project='xenon-ministack-'+uuid.uuid4().hex[:12]
    evidence=ROOT/'.local/evidence'/(time.strftime('%Y%m%dT%H%M%SZ',time.gmtime())+'-'+project)
    evidence.mkdir(parents=True);runtime=evidence/'runtime';runtime.mkdir()
    env={k:os.environ[k] for k in ('PATH','HOME','USER','TMPDIR','RUSTUP_HOME','CARGO_HOME') if k in os.environ}
    library=ROOT/'.local/slatedb-native-target/debug'
    env.update(GOENV='off',GOWORK='off',GOFLAGS='-mod=readonly',GOTOOLCHAIN='go1.27.1',CGO_ENABLED='1',CGO_LDFLAGS='-L'+str(library),LD_LIBRARY_PATH=str(library),DYLD_LIBRARY_PATH=str(library),SLATEDB_UNIFFI_RUNTIME_THREADS='2',AWS_ACCESS_KEY_ID='xenon-local',AWS_SECRET_ACCESS_KEY='xenon-local-test-only',AWS_DEFAULT_REGION='us-east-1',AWS_ENDPOINT='http://127.0.0.1:19006',AWS_ALLOW_HTTP='true',AWS_VIRTUAL_HOSTED_STYLE_REQUEST='false',XENON_BUCKET='xenon-ministack-proof',XENON_TOPOLOGY_PREFIX='metadata')
    report={'kind':'ministack-runtime','scope':case['scope'],'full_acceptance':False,'proof_pass':False,'result':'failed','project':project,'commands':[],'events':[]};processes=[];sequence=0
    compose=['docker','compose','--project-name',project,'-f','deploy/ministack/compose.json']
    def run(argv,timeout=120,cwd=ROOT,command_env=None):
        nonlocal sequence
        sequence+=1;path=evidence/f'command-{sequence}.log'
        p=Process(argv,cwd,command_env or env,path)
        expired=False
        try:
            code=p.process.wait(timeout=timeout)
            p.thread.join(timeout=5)
        except subprocess.TimeoutExpired:
            expired=True;p.stop();code=p.process.returncode
        finally:
            p.stop()
        report['commands'].append({'argv':list(map(str,argv)),'exit_code':code,'timed_out':expired,'log':path.name,'sha256':sha(path)})
        if expired:raise TimeoutError('command deadline: '+repr(argv))
        if code:raise RuntimeError('command failed: '+repr(argv)+'; '+path.name)
        return path.read_text()
    def json_result(text):
        values=[json.loads(line) for line in text.splitlines() if line.startswith('{')]
        if not values:raise RuntimeError('missing structured probe result')
        return values[-1]
    def wait(check,timeout=90):
        end=time.monotonic()+timeout;last=None
        while time.monotonic()<end:
            try:
                value=check()
                if value:return value
            except (RuntimeError,TimeoutError,OSError) as error:last=str(error)
            time.sleep(.25)
        raise TimeoutError('progress deadline: '+str(last))
    def launch(name,argv,extra=None):
        directory=runtime/name;directory.mkdir()
        process=Process(argv,directory,{**env,**(extra or {})},evidence/(name+'.log'));processes.append(process);return process
    sdk=str(ROOT/'.local/bin/xenon-sdk-probe')
    probe_flags=['--address',f"127.0.0.1:{case['ports']['temporal_ingress']}",'--namespace',case['namespace'],'--workflow-id',case['workflow_id'],'--task-queue',case['task_queue']]
    def probe(mode,*args,timeout=20):return json_result(run([sdk,'--mode',mode,*probe_flags,*args],timeout))
    def event(name,**fields):report['events'].append({'name':name,'elapsed_seconds':round(time.monotonic()-started,3),**fields})
    started=time.monotonic()
    try:
        report['host']={'system':platform.system(),'release':platform.release(),'machine':platform.machine(),'python_version':sys.version,'python_executable':sys.executable}
        report['git_sha']=run(['git','rev-parse','HEAD']).strip()
        if run(['git','status','--porcelain=v1','--untracked-files=all']).strip():raise RuntimeError('runtime proof requires clean checkout')
        tracked=run(['git','ls-files']).splitlines();report['input_sha256']={p:sha(ROOT/p) for p in tracked}
        report['tools']={tool:run(argv).strip() for tool,argv in {'go':['go','version'],'node':['node','--version'],'npm':['npm','--version'],'docker':['docker','version','--format','{{.Server.Version}}'],'aws':['aws','--version']}.items()}
        if report['tools']['node']!=pins['runtime_tools']['node'] or report['tools']['npm']!=pins['runtime_tools']['npm']:raise RuntimeError('Node/npm tool pin mismatch')
        if not report['tools']['aws'].startswith('aws-cli/'+pins['runtime_tools']['aws_cli']+' '):raise RuntimeError('AWS CLI tool pin mismatch')
        run([sys.executable,'scripts/build-go-node.py'],900)
        native_manifest=ROOT/'.local/go-node-build.json'
        report['native_build_manifest']=json.loads(native_manifest.read_text())
        native_library=ROOT/report['native_build_manifest']['shared_library']
        report['native_artifacts_sha256']={str(path.relative_to(ROOT)):sha(path) for path in [native_manifest,native_library]}
        if sha(native_library)!=report['native_build_manifest']['shared_library_sha256']:raise RuntimeError('native build manifest/library mismatch')
        for name,path,tags in [('xenon-topology','./cmd/xenon-topology',[]),('xenon-sdk-probe','./cmd/xenon-sdk-probe',[]),('xenon-temporal','./cmd/xenon-temporal',['-tags','ministack'])]:
            run(['go','build',*tags,'-o',str(ROOT/'.local/bin'/name),path],900)
        report['binaries']={name:sha(ROOT/'.local/bin'/name) for name in ['xenon-go-node','xenon-topology','xenon-sdk-probe','xenon-temporal']}
        run([*compose,'up','-d'],120)
        def s3_ready():
            with urllib.request.urlopen(env['AWS_ENDPOINT']+'/minio/health/live',timeout=2) as response:return response.status==200
        wait(s3_ready,30)
        run(['aws','--endpoint-url',env['AWS_ENDPOINT'],'s3api','create-bucket','--bucket',env['XENON_BUCKET']])
        nodes={};members={};assignments={};metrics_ports={} 
        def node(name,port):
            process=launch(name,[str(ROOT/'.local/bin/xenon-go-node')],{'XENON_NODE':name,'XENON_LISTEN':f'0.0.0.0:{port}','XENON_ADVERTISE':f'127.0.0.1:{port}','XENON_MAX_OUTCOMES':str(case['max_outcomes']),'XENON_METRICS_LISTEN':f'127.0.0.1:{port+100}'})
            metrics_ports[name]=port+100
            _,id,incarnation,address=process.line('INGRESS ').split();nodes[name]=process;members[id]={'address':address,'incarnation':incarnation};event('node-ingress',node=id,incarnation=incarnation,pid=process.process.pid)
        def metrics(name):
            with urllib.request.urlopen(f'http://127.0.0.1:{metrics_ports[name]}/outcomes',timeout=12) as response:
                snapshot=json.load(response)
            if snapshot['schema_version']!=1 or snapshot['incarnation']!=members[name]['incarnation']:
                raise RuntimeError('metrics identity/schema mismatch')
            for partition in snapshot['partitions'].values():
                if partition.get('error'):raise RuntimeError('partition metrics error')
                usage=partition.get('usage')
                if usage and (not usage['accounting_complete'] or usage['unaccounted_legacy_entries'] or usage['remaining']<=0 or usage['limit']!=case['max_outcomes']):
                    raise RuntimeError('outcome capacity/accounting gate failed')
            return snapshot
        def checkpoint(label):
            snapshots=wait(lambda:{name:metrics(name) for name,process in nodes.items() if process.process.poll() is None},60)
            path=evidence/('outcomes-'+label+'.json');path.write_text(json.dumps(snapshots,indent=2))
            event('outcome-accounting-checkpoint',label=label,file=path.name,sha256=sha(path))
            return snapshots
        def successful(snapshot,partition=None):
            return sum(value['local_dispatch']['successful_operations'] for name,value in snapshot['partitions'].items() if partition is None or name==partition)
        def publish():
            path=evidence/'desired-topology.json';path.write_text(json.dumps({'members':members,'partitions':assignments},indent=2))
            run([str(ROOT/'.local/bin/xenon-topology'),str(path)])
            event('topology-published',members=dict(members),assignments=dict(assignments))
        node('a',17351);node('b',17352)
        for index,partition in enumerate(case['partitions']):assignments[partition]={'node':'a' if index%2==0 else 'b','data_prefix':'data/'+partition}
        publish()
        temporals=[]
        def temporal(name,config):
            process=launch(name,[str(ROOT/'.local/bin/xenon-temporal'),'--config',str(ROOT/config)]);temporals.append(process);return process
        def healthy_temporal(name,config,address):
            process=temporal(name,config);process.line('TEMPORAL_STARTED',timeout=120)
            wait(lambda:probe('health','--address',address),60)
            return process
        # Temporal's pinned first-cluster metadata initializer is a one-shot CAS.
        # Initialize it once before adding the second concurrently serving instance.
        ta=healthy_temporal('temporal-a','deploy/ministack/temporal-a.json','127.0.0.1:18233')
        tb=healthy_temporal('temporal-b','deploy/ministack/temporal-b.json','127.0.0.1:19233')
        event('both-temporal-instances-healthy')
        bootstrap=wait(lambda:probe('bootstrap'),120);event('namespace-and-search-schema-ready',**bootstrap)
        worker=launch('worker',[sdk,'--mode','worker',*probe_flags]);worker.line('{')
        execution=probe('start');event('workflow-started',**execution)
        wait(lambda:probe('phase').get('phase')=='await-control');event('workflow-durable-pause')
        initial=checkpoint('before-owner-kill')
        if any(successful(initial[name])<=0 for name in ['a','b']):raise RuntimeError('both initial nodes must perform local persistence work')
        owner=assignments[bootstrap['history_partition']]['node'];nodes[owner].stop(kill=True);event('active-history-owner-killed',node=owner)
        port=17351 if owner=='a' else 17352
        # Name is intentionally new: empty local directory and a new process incarnation.
        replacement=owner+'-replacement';node(replacement,port)
        for assignment in assignments.values():
            if assignment['node']==owner:assignment['node']=replacement
        publish()
        wait(lambda:probe('phase').get('phase')=='await-control');event('workflow-recovered');checkpoint('owner-recovered')
        live_history=evidence/'history-before-cold';live_history.mkdir()
        verify=launch('verify-live',[sdk,'--mode','verify',*probe_flags,'--run-id',execution['run_id'],'--output',str(live_history)])
        verify.line('VERIFY_CLIENT_CONNECTED',timeout=30)
        event('live-sdk-client-connected-before-temporal-kill')
        ta.stop(kill=True);event('temporal-instance-killed',pid=ta.process.pid)
        probe('control');verify.process.wait(timeout=120)
        if verify.process.returncode:raise RuntimeError('live SDK verifier failed')
        event('sdk-sequence-completed')
        wait(lambda:probe('visibility','--query',"WorkflowId = 'xenon-durable-workflow-1' AND ExecutionStatus = 'Completed' AND XenonProof = 'durable'"))
        # Omes uses its own pinned source/module, never the embedded server.
        omes=ROOT/'.local/omes-source'
        if not omes.exists():run(['git','clone','--no-checkout',pins['omes']['repository'],str(omes)],120)
        run(['git','checkout','--detach',pins['omes']['commit']],cwd=omes)
        if run(['git','status','--porcelain=v1','--untracked-files=all'],cwd=omes).strip():raise RuntimeError('dirty Omes source')
        run(['go','build','-o',str(ROOT/'.local/bin/omes'),'./cmd/omes'],900,cwd=omes)
        run([str(ROOT/'.local/bin/omes'),'prepare-worker','--language','go','--version',pins['omes']['worker_go_sdk'],'--dir-name','prepared'],900,cwd=omes)
        report['omes_binary_sha256']=sha(ROOT/'.local/bin/omes')
        def omes_inputs():return {str(path.relative_to(omes)):sha(path) for path in (omes/'workers/go/prepared').rglob('*') if path.is_file()}
        report['omes_generated_sha256']=omes_inputs()
        if not report['omes_generated_sha256']:raise RuntimeError('missing prepared Omes worker artifacts')
        omes_process=launch('omes',[str(ROOT/'.local/bin/omes'),*case['omes_command']])
        # Pinned getRepoDir uses runtime.Caller's build-source path. Build without
        # -trimpath above and retain the verified checkout for its worker builder.
        wait(lambda:probe('visibility-count','--query',"TaskQueue = 'omes-xenon-ministack-omes'").get('count',0)>0,60)
        if omes_process.process.poll() is not None:raise RuntimeError('Omes stopped before node addition')
        event('omes-work-observed-before-addition')
        node('c',17353);before_c=wait(lambda:metrics('c'));assignments['matching']['node']='c';publish();event('node-added-during-omes')
        wait(lambda:successful(metrics('c'),'matching')>successful(before_c,'matching'),60)
        checkpoint('node-c-served-matching')
        omes_process.process.wait(timeout=360)
        if omes_process.process.returncode:raise RuntimeError('Omes workload failed')
        wait(lambda:probe('visibility','--query',"TaskQueue = 'omes-xenon-ministack-omes' AND ExecutionStatus = 'Completed'",'--expected-count','20'))
        ui=ROOT/'.local/ministack-ui';ui.mkdir(exist_ok=True)
        for file in ['package.json','package-lock.json','probe.mjs']:shutil.copyfile(ROOT/'proof/ministack/ui'/file,ui/file)
        run(['npm','ci','--ignore-scripts'],120,cwd=ui)
        run(['npx','playwright','install','chromium'],300,cwd=ui)
        run(['node','probe.mjs',str(evidence/'browser'),str(ROOT/'proof/ministack/case.json')],120,cwd=ui)
        if omes_inputs()!=report['omes_generated_sha256']:raise RuntimeError('prepared Omes worker inputs changed during workload')
        if sha(ROOT/'.local/bin/omes')!=report['omes_binary_sha256']:raise RuntimeError('Omes binary changed during workload')
        if run(['git','status','--porcelain=v1','--untracked-files=all'],cwd=omes).strip():raise RuntimeError('Omes source changed')
        event('ui-assertions-passed');checkpoint('before-cold-restart')
        # Explicit final cold-local restart retains only the S3 service volume.
        for process in processes:process.stop()
        nodes.clear();members.clear()
        node('cold-a',17351);node('cold-b',17352)
        for index,assignment in enumerate(assignments.values()):assignment['node']='cold-a' if index%2==0 else 'cold-b'
        publish()
        healthy_temporal('cold-temporal-a','deploy/ministack/temporal-a.json','127.0.0.1:18233')
        healthy_temporal('cold-temporal-b','deploy/ministack/temporal-b.json','127.0.0.1:19233')
        cold_history=evidence/'history-after-cold';cold_history.mkdir()
        wait(lambda:probe('verify','--run-id',execution['run_id'],'--output',str(cold_history)),120)
        for original in sorted(live_history.glob('history-*.json')):
            recovered=cold_history/original.name
            if json.loads(original.read_text())!=json.loads(recovered.read_text()):raise RuntimeError('acknowledged history changed after cold recovery')
        report['history_sha256']={str(path.relative_to(evidence)):sha(path) for folder in [live_history,cold_history] for path in folder.glob('history-*.json')}
        wait(lambda:probe('visibility','--query',"WorkflowId = 'xenon-durable-workflow-1' AND ExecutionStatus = 'Completed' AND XenonProof = 'durable'"))
        wait(lambda:probe('visibility','--query',"TaskQueue = 'omes-xenon-ministack-omes' AND ExecutionStatus = 'Completed'",'--expected-count','20'))
        run(['node','probe.mjs',str(evidence/'browser-after-cold'),str(ROOT/'proof/ministack/case.json')],120,cwd=ui)
        event('cold-local-recovery-passed');checkpoint('cold-recovered')
        if report['git_sha']!=run(['git','rev-parse','HEAD']).strip() or run(['git','status','--porcelain=v1','--untracked-files=all']).strip():raise RuntimeError('checkout changed during runtime')
        if any(sha(ROOT/p)!=value for p,value in report['input_sha256'].items()):raise RuntimeError('input changed during runtime')
        if any(sha(ROOT/'.local/bin'/name)!=value for name,value in report['binaries'].items()):raise RuntimeError('runtime binary changed during proof')
        if any(sha(ROOT/path)!=value for path,value in report['native_artifacts_sha256'].items()):raise RuntimeError('native build artifacts changed during proof')
        report.update(result='passed',proof_pass=True)
    except Exception as error:report['error']=str(error)
    finally:
        for process in reversed(processes):
            try:process.stop()
            except Exception as error:report.setdefault('cleanup_errors',[]).append(str(error))
        try:
            run([*compose,'down','--volumes'],60)
            for argv in (['docker','ps','-aq','--filter','label=com.docker.compose.project='+project],['docker','volume','ls','-q','--filter','label=com.docker.compose.project='+project]):
                if run(argv).strip():raise RuntimeError('scoped Compose resources survived cleanup')
        except Exception as error:report.setdefault('cleanup_errors',[]).append(str(error))
        if report.get('cleanup_errors'):report.update(result='failed',proof_pass=False)
        (evidence/'result.json').write_text(json.dumps(report,indent=2)+'\n');print(report['result'].upper()+': '+str(evidence/'result.json'))
    return 0 if report['proof_pass'] else 1
if __name__=='__main__':
    def interrupted(signum,frame):raise RuntimeError('controller interrupted: '+str(signum))
    signal.signal(signal.SIGTERM,interrupted)
    signal.signal(signal.SIGINT,interrupted)
    signal.signal(signal.SIGALRM,interrupted)
    signal.alarm(1800)
    sys.exit(main())
