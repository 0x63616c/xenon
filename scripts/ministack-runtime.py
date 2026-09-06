#!/usr/bin/env python3
"""Actual local runtime controller. Missing factories/builds are failures, not skips."""
import argparse
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
from resource_samples import ProcessSampler
import runtime_measurements
import recorder_lifecycle
from omes_workloads import effective_sdk
from omes_mixed import validate_result as validate_mixed_result
import temporal_cut

ROOT=Path(__file__).resolve().parents[1]
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
class Process:
    def __init__(self,argv,cwd,env,log,stop_timeout=10):
        self.stop_timeout=stop_timeout
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
            try:self.process.wait(timeout=self.stop_timeout)
            except subprocess.TimeoutExpired:
                try:os.killpg(self.process.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                self.process.wait(timeout=10)
        self.thread.join(timeout=5)
        if self.thread.is_alive():raise RuntimeError("process output did not drain after shutdown")
        self.process.stdout.close();self.log.close()

def event_record(name,elapsed,fields):
    return {'name':name,'elapsed_seconds':round(elapsed,3),**json.loads(json.dumps(fields))}

def arguments(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--measurements',action='store_true',help='opt-in strict measurement evidence; intentional SIGKILL leaves fault traces incomplete')
    parser.add_argument('--external-recorder',action='store_true',help='opt-in static fault-phase census plus resources/S3; no steady gate')
    modes=parser.add_mutually_exclusive_group()
    modes.add_argument('--fuzz-soak',action='store_true',help='run the committed real Omes fuzz soak instead of smoke')
    modes.add_argument('--process-cut-stage',choices=temporal_cut.STAGES,help='bind a real SDK update to a native process cut within the smoke')
    modes.add_argument('--visibility-movement',action='store_true',help='run frozen public visibility traversal across two real ownership moves')
    modes.add_argument('--omes-mixed',action='store_true',help='run the frozen 40-iteration Omes mixed component instead of smoke')
    modes.add_argument('--smoke',action='store_true',help='run the default smoke explicitly')
    return parser.parse_args(argv)

def main():
    args=arguments()
    if args.visibility_movement:
        movement=json.loads((ROOT/"test/scenarios/ministack/visibility-movement.json").read_text())
        expected={"schema":1,"status":"NOT_EXECUTED","command":["python3","scripts/ministack-runtime.py","--visibility-movement"],"records":2000,"page_sizes":[1,7,100],"move_partitions":["history-0","vis-v1-0"],"destination":"c","cursor_pause_seconds":120,"minimum_completed_operations_before_scale":20,"minimum_local_operations_per_node":10,"require_active_workflow":True,"both_frontends_public_persistence_calls":True,"separate_mutation_records":20,"full_acceptance":False}
        if movement!=expected:raise ValueError("changed visibility movement contract")
    if args.measurements and args.external_recorder:raise ValueError("measurement modes are mutually exclusive")
    cut_config=temporal_cut.validate_config(json.loads((ROOT/'test/scenarios/ministack/process-cut.json').read_text())) if args.process_cut_stage else None
    measurement_config=json.loads((ROOT/'test/scenarios/ministack/measurements.json').read_text()) if args.measurements or args.external_recorder else None
    if measurement_config and (measurement_config['schema']!=1 or measurement_config['incomplete_policy']!='preserve_functional_result_but_fail_measurement_and_overall_proof' or measurement_config['resources']!='proof/acceptance/resources.json' or measurement_config['s3_meter']!='proof/s3-meter/local.json'):
        raise ValueError('unsupported measurement configuration')
    sampler=None;meter=None;traces={};recorder=None
    case=json.loads((ROOT/'test/scenarios/ministack/case.json').read_text());pins=json.loads((ROOT/'test/scenarios/ministack/pins.json').read_text())
    project='xenon-ministack-'+uuid.uuid4().hex[:12]
    evidence=ROOT/'.local/evidence'/(time.strftime('%Y%m%dT%H%M%SZ',time.gmtime())+'-'+project)
    evidence.mkdir(parents=True);runtime=evidence/'runtime';runtime.mkdir()
    env={k:os.environ[k] for k in ('PATH','HOME','USER','TMPDIR','RUSTUP_HOME','CARGO_HOME') if k in os.environ}
    library=ROOT/'.local/slatedb-native-target/debug'
    env.update(GOENV='off',GOWORK='off',GOFLAGS='-mod=readonly',GOTOOLCHAIN='go1.27.1',CGO_ENABLED='1',CGO_LDFLAGS='-L'+str(library),LD_LIBRARY_PATH=str(library),DYLD_LIBRARY_PATH=str(library),SLATEDB_UNIFFI_RUNTIME_THREADS='2',AWS_ACCESS_KEY_ID='xenon-local',AWS_SECRET_ACCESS_KEY='xenon-local-test-only',AWS_DEFAULT_REGION='us-east-1',AWS_ENDPOINT='http://127.0.0.1:19006',AWS_ALLOW_HTTP='true',AWS_VIRTUAL_HOSTED_STYLE_REQUEST='false',XENON_BUCKET='xenon-ministack-proof',XENON_TOPOLOGY_PREFIX='metadata')
    report={'kind':'ministack-runtime','scope':case['scope'],'full_acceptance':False,'proof_pass':False,'result':'failed','project':project,'commands':[],'events':[]};processes=[];sequence=0
    if args.process_cut_stage:report['process_cut_stage']=args.process_cut_stage
    compose=['docker','compose','--project-name',project,'-f','test/scenarios/ministack/config/compose.json']
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
        process=Process(argv,directory,{**env,**(extra or {})},evidence/(name+'.log'),stop_timeout=measurement_config['producer_shutdown_seconds'] if measurement_config and ('XENON_RPC_TRACE_PATH' in (extra or {}) or 'XENON_RPC_RECORDER_URL' in (extra or {})) else 10);processes.append(process)
        if sampler:sampler.register(name,process.process.pid)
        return process
    sdk=str(ROOT/'.local/bin/xenon-sdk-probe')
    probe_flags=['--address',f"127.0.0.1:{case['ports']['temporal_ingress']}",'--namespace',case['namespace'],'--workflow-id',case['workflow_id'],'--task-queue',case['task_queue']]
    def probe(mode,*args,timeout=20):return json_result(run([sdk,'--mode',mode,*probe_flags,*args],timeout))
    def event(name,**fields):report['events'].append(event_record(name,time.monotonic()-started,fields))
    started=time.monotonic()
    try:
        report['host']={'system':platform.system(),'release':platform.release(),'machine':platform.machine(),'python_version':sys.version,'python_executable':sys.executable}
        report['git_sha']=run(['git','rev-parse','HEAD']).strip()
        if run(['git','status','--porcelain=v1','--untracked-files=all']).strip():raise RuntimeError('runtime proof requires clean checkout')
        tracked=run(['git','ls-files']).splitlines();report['input_sha256']={p:sha(ROOT/p) for p in tracked}
        report['preflight_free_disk_bytes']=shutil.disk_usage(ROOT).free
        if report['preflight_free_disk_bytes']<case['minimum_free_disk_bytes']:raise RuntimeError('insufficient free disk for declared build preflight')
        node_bin=run([sys.executable,'scripts/ministack-node.py'],120).strip()
        env['PATH']=node_bin+os.pathsep+env['PATH']
        env['PLAYWRIGHT_BROWSERS_PATH']=str(ROOT/'.local/playwright-browsers')
        report['ui_node_sha256']=sha(Path(node_bin)/'node')
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
        if args.visibility_movement:
            run(['go','build','-o',str(ROOT/'.local/bin/xenon-visibility-probe'),'./cmd/xenon-visibility-probe'],900)
            report['binaries']['xenon-visibility-probe']=sha(ROOT/'.local/bin/xenon-visibility-probe')
        if args.omes_mixed:
            run(['go','build','-o',str(ROOT/'.local/bin/xenon-omes-oracle'),'./cmd/xenon-omes-oracle'],900)
            report['binaries']['xenon-omes-oracle']=sha(ROOT/'.local/bin/xenon-omes-oracle')
        if measurement_config:
            run(['go','build','-o',str(ROOT/'.local/bin/xenon-s3-meter'),'./cmd/xenon-s3-meter'],120)
            report['binaries']['xenon-s3-meter']=sha(ROOT/'.local/bin/xenon-s3-meter')
            resource_config=json.loads((ROOT/measurement_config['resources']).read_text())
            sampler=ProcessSampler(evidence/'resources.jsonl',resource_config['interval_seconds'],resource_config['max_samples'],resource_config['max_processes'])
        if args.external_recorder:
            run(['go','build','-o',str(ROOT/'.local/bin/xenon-trace-recorder'),'./cmd/xenon-trace-recorder'],120)
            report['binaries']['xenon-trace-recorder']=sha(ROOT/'.local/bin/xenon-trace-recorder')
            recorder=recorder_lifecycle.RecorderLifecycle(evidence,ROOT/'.local/bin/xenon-trace-recorder',launch,recorder_lifecycle.config(ROOT/'test/scenarios/ministack/recorder.json'))
        ui=ROOT/'.local/ministack-ui';ui.mkdir(exist_ok=True)
        for file in ['package.json','package-lock.json','probe.mjs']:shutil.copyfile(ROOT/'test/scenarios/ministack/ui'/file,ui/file)
        run(['npm','ci','--ignore-scripts'],120,cwd=ui)
        run(['npx','playwright','install','chromium'],300,cwd=ui)
        run([*compose,'up','-d'],120)
        def s3_ready():
            with urllib.request.urlopen(env['AWS_ENDPOINT']+'/minio/health/live',timeout=2) as response:return response.status==200
        wait(s3_ready,30)
        if measurement_config:
            meter_config=json.loads((ROOT/measurement_config['s3_meter']).read_text())
            if meter_config['target']!=env['AWS_ENDPOINT']:raise RuntimeError('meter target differs from declared local MinIO')
            meter=launch('s3-meter',[str(ROOT/'.local/bin/xenon-s3-meter'),'--config',str(ROOT/measurement_config['s3_meter'])])
            ready=json.loads(meter.line('{'))
            if ready.get('event')!='ready' or ready.get('listen')!=meter_config['listen'] or ready.get('report_listen')!=meter_config['report_listen']:raise RuntimeError('meter readiness identity mismatch')
            env['AWS_ENDPOINT']='http://'+meter_config['listen']
        run(['aws','--endpoint-url',env['AWS_ENDPOINT'],'s3api','create-bucket','--bucket',env['XENON_BUCKET']])
        nodes={};members={};assignments={};metrics_ports={};cut_controls={}
        def node(name,port,ready_timeout=30):
            cut_env={}
            if args.process_cut_stage:
                plan={'schema':1,'session':str(uuid.uuid4()),'listen':f'127.0.0.1:{port+200}','discovery':True,'selector':{},'stage':args.process_cut_stage,'timeout_ms':5000}
                path=evidence/(name+'-cut-plan.json');path.write_text(json.dumps(plan))
                cut_env={'XENON_PROCESS_CUT_PLAN':str(path)}
                cut_controls[name]={'session':plan['session'],'stage':plan['stage'],'url':'http://'+plan['listen']}
            process=launch(name,[str(ROOT/'.local/bin/xenon-go-node')],{'XENON_NODE':name,'XENON_LISTEN':f'0.0.0.0:{port}','XENON_ADVERTISE':f'127.0.0.1:{port}','XENON_MAX_OUTCOMES':str(case['max_outcomes']),'XENON_METRICS_LISTEN':f'127.0.0.1:{port+100}',**cut_env})
            metrics_ports[name]=port+100
            _,id,incarnation,address=process.line('INGRESS ',timeout=ready_timeout).split();nodes[name]=process;members[id]={'address':address,'incarnation':incarnation};event('node-ingress',node=id,incarnation=incarnation,pid=process.process.pid)
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
        def publish(timeout=120):
            path=evidence/'desired-topology.json';path.write_text(json.dumps({'members':members,'partitions':assignments},indent=2))
            run([str(ROOT/'.local/bin/xenon-topology'),str(path)],timeout)
            event('topology-published',members=dict(members),assignments=dict(assignments))
        node('a',17351);node('b',17352)
        for index,partition in enumerate(case['partitions']):assignments[partition]={'node':'a' if index%2==0 else 'b','data_prefix':'data/'+partition}
        publish()
        temporals=[]
        def temporal(name,config):
            extra=recorder.environment(name) if recorder else ({'XENON_RPC_TRACE_PATH':str(evidence/(name+'.rpc.jsonl'))} if measurement_config else None)
            process=launch(name,[str(ROOT/'.local/bin/xenon-temporal'),'--config',str(ROOT/config)],extra);temporals.append(process)
            if measurement_config:traces[name]=process
            if recorder:recorder.bind(name,process)
            return process
        def healthy_temporal(name,config,address):
            process=temporal(name,config);process.line('TEMPORAL_STARTED',timeout=120)
            wait(lambda:probe('health','--address',address),60)
            return process
        # Temporal's pinned first-cluster metadata initializer is a one-shot CAS.
        # Initialize it once before adding the second concurrently serving instance.
        ta=healthy_temporal('temporal-a','test/scenarios/ministack/config/temporal-a.json','127.0.0.1:18233')
        tb=healthy_temporal('temporal-b','test/scenarios/ministack/config/temporal-b.json','127.0.0.1:19233')
        event('both-temporal-instances-healthy')
        bootstrap=wait(lambda:probe('bootstrap'),120);event('namespace-and-search-schema-ready',**bootstrap)
        alias_query="OmesExecutionID = '__bootstrap__' AND KS_Keyword = '__bootstrap__' AND KS_Int = 0 AND XenonProof = '__bootstrap__'"
        for address in ['127.0.0.1:18233','127.0.0.1:19233']:
            wait(lambda:probe('visibility-count','--address',address,'--query',alias_query),60)
        event('both-frontends-search-aliases-ready')
        def prepare_omes():
            omes=ROOT/'.local/omes-source'
            if not omes.exists():run(['git','clone','--no-checkout',pins['omes']['repository'],str(omes)],120)
            run(['git','checkout','--detach',pins['omes']['commit']],cwd=omes)
            if run(['git','status','--porcelain=v1','--untracked-files=all'],cwd=omes).strip():raise RuntimeError('dirty Omes source')
            run(['go','build','-o',str(ROOT/'.local/bin/omes'),'./cmd/omes'],900,cwd=omes)
            run([str(ROOT/'.local/bin/omes'),'prepare-worker','--language','go','--version',pins['omes']['worker_go_sdk'],'--dir-name','prepared'],900,cwd=omes)
            worker_program=omes/'workers/go/prepared/program'
            report['omes_worker_build_info']=run(['go','version','-m',str(worker_program)])
            report['effective_omes_worker_sdk']=effective_sdk(report['omes_worker_build_info'])
            if report['effective_omes_worker_sdk']['version']!=pins['omes']['worker_go_sdk']:raise RuntimeError('prepared Omes binary SDK pin mismatch')
            report['omes_binary_sha256']=sha(ROOT/'.local/bin/omes')
            def omes_inputs():return {str(path.relative_to(omes)):sha(path) for path in (omes/'workers/go/prepared').rglob('*') if path.is_file()}
            report['omes_generated_sha256']=omes_inputs()
            if not report['omes_generated_sha256']:raise RuntimeError('missing prepared Omes worker artifacts')
            return omes,omes_inputs
        if args.visibility_movement:
            visibility_binary=str(ROOT/'.local/bin/xenon-visibility-probe')
            visibility_flags=['--address',probe_flags[1],'--namespace',case['namespace'],'--storage-address',f"127.0.0.1:{case['ports']['storage_ingress']}"]
            def visibility(mode,label,extra=None):
                output=evidence/('visibility-'+label+'.json')
                run([visibility_binary,'--mode',mode,*visibility_flags,'--checkpoint',label,'--output',str(output),*(extra or [])],910)
                value=json.loads(output.read_text())
                if value.get('full_acceptance') is not False or value.get('mode')!=mode:raise RuntimeError('invalid visibility receipt')
                return value
            report['visibility_before']=visibility('seed','before-movement')
            for address in ['127.0.0.1:18233','127.0.0.1:19233']:
                if probe('visibility-count','--address',address,'--query',"TaskQueue = 'xenon-frozen-visibility'").get('count')!=2000:raise RuntimeError('frontend frozen count mismatch')
            identity=probe('movement-identity')
            if identity.get('history_partition')!='history-0' or identity.get('history_shard')!=4:raise RuntimeError('wrong movement workflow domain')
            probe_flags[probe_flags.index('--workflow-id')+1]=identity['workflow_id']
            worker=launch('movement-worker',[sdk,'--mode','worker',*probe_flags]);worker.line('{')
            execution=probe('start');event('movement-workflow-started',**execution)
            wait(lambda:probe('phase').get('phase')=='await-control')
            before=checkpoint('visibility-before-movement')
            if sum(successful(value) for value in before.values())<20:raise RuntimeError('scale barrier lacks20 completed operations')
            release=evidence/'visibility-release';token=uuid.uuid4().hex;output=evidence/'visibility-through-movement.json'
            traversal=launch('visibility-traversal',[visibility_binary,'--mode','check',*visibility_flags,'--checkpoint','through-movement','--output',str(output),'--release-file',str(release),'--release-token',token])
            hit=json.loads(traversal.line('{',timeout=60))
            if hit!={'event':'VISIBILITY_FIRST_PAGE','checkpoint':'through-movement','token':token}:raise RuntimeError('wrong traversal barrier')
            event('visibility-page-barrier',token=token)
            movement_deadline=time.monotonic()+120
            def movement_remaining():
                left=movement_deadline-time.monotonic()
                if left<=0:raise TimeoutError('visibility movement120s budget exceeded')
                return left
            node('c',17353,min(30,movement_remaining()))
            c_before=metrics('c')
            assignments['history-0']['node']='c';assignments['vis-v1-0']['node']='c'
            publish(movement_remaining())
            probe('storage-ready','--readiness-timeout',str(max(1,int(movement_remaining()-1)))+'s',timeout=movement_remaining())
            staged_release=release.with_suffix('.tmp');staged_release.write_text(token);os.replace(staged_release,release);event('visibility-cursor-released-after-movement',incarnation=members['c']['incarnation'])
            # Both actual frontends serve persistence-backed calls after movement.
            for address in ['127.0.0.1:18233','127.0.0.1:19233']:
                probe('phase','--address',address,timeout=min(20,movement_remaining()))
                if probe('visibility-count','--address',address,'--query',"TaskQueue = 'xenon-frozen-visibility'",timeout=min(20,movement_remaining())).get('count')!=2000:raise RuntimeError('frontend frozen count mismatch')
            probe('control',timeout=movement_remaining())
            history=evidence/'movement-history';history.mkdir()
            probe('verify','--run-id',execution['run_id'],'--output',str(history),timeout=300)
            traversal.process.wait(timeout=910)
            if traversal.process.returncode:raise RuntimeError('movement traversal failed')
            report['visibility_through']=json.loads(output.read_text())
            report['visibility_mutation']=visibility('mutate','separate-mutation')
            after=checkpoint('visibility-after-movement')
            if any(successful(after[name])<10 for name in ['a','b','c']):raise RuntimeError('node lacks10 actual local operations')
            for partition in ['history-0','vis-v1-0']:
                if successful(after['c'],partition)<=successful(c_before,partition):raise RuntimeError('C did not actually serve '+partition)
            if any(sha(ROOT/p)!=value for p,value in report['input_sha256'].items()) or run(['git','status','--porcelain=v1','--untracked-files=all']).strip():raise RuntimeError('runtime source changed')
            if any(sha(ROOT/'.local/bin'/name)!=value for name,value in report['binaries'].items()):raise RuntimeError('runtime binary changed')
            if any(sha(ROOT/path)!=value for path,value in report['native_artifacts_sha256'].items()):raise RuntimeError('native artifacts changed')
            report.update(result='passed',proof_pass=True,scope='frozen2000 public visibility cursor across actual history/visibility ownership moves; separate mutation; no full acceptance',visibility_movement_executed=True)
        elif args.omes_mixed:
            omes,omes_inputs=prepare_omes()
            probe('fuzz-endpoint',timeout=60)
            readiness=probe('fuzz-endpoint-ready',timeout=75)
            event('mixed-nexus-functionally-ready',**readiness)
            signal.alarm(1200)
            run([sys.executable,'scripts/omes_workloads.py','--stage','mixed','--profile','without-faults','--evidence-dir',str(evidence/'mixed'),'--omes-binary',str(ROOT/'.local/bin/omes'),'--omes-source',str(omes)],950)
            report['mixed']=validate_mixed_result(ROOT,evidence/'mixed')
            inventory=wait(lambda:probe('mixed-inventory',timeout=15),60)
            report['mixed']['visibility_inventory']=inventory
            # The audit's existing 15-minute bound is separate from the frozen
            # 900-second workload deadline, which has already been enforced.
            signal.alarm(930)
            run([str(ROOT/'.local/bin/xenon-omes-oracle'),'--address',probe_flags[1],'--namespace',case['namespace'],'--omes-run-id','xenon-full-mixed','--mixed-profile','--exact-runs','240','--output',str(evidence/'mixed-histories')],910)
            history_report=evidence/'mixed-histories/result.json'
            report['mixed']['history_oracle']={'result':json.loads(history_report.read_text()),'sha256':sha(history_report)}
            checkpoint('mixed-finished')
            if report['git_sha']!=run(['git','rev-parse','HEAD']).strip() or run(['git','status','--porcelain=v1','--untracked-files=all']).strip():raise RuntimeError('checkout changed during mixed workload')
            if any(sha(ROOT/p)!=value for p,value in report['input_sha256'].items()):raise RuntimeError('input changed during mixed workload')
            if any(sha(ROOT/'.local/bin'/name)!=value for name,value in report['binaries'].items()):raise RuntimeError('runtime binary changed during mixed workload')
            if any(sha(ROOT/path)!=value for path,value in report['native_artifacts_sha256'].items()):raise RuntimeError('native artifacts changed during mixed workload')
            if omes_inputs()!=report['omes_generated_sha256']:raise RuntimeError('prepared Omes worker changed during mixed workload')
            report.update(result='passed',proof_pass=True,scope='frozen Omes mixed40 component and source-derived history checks against real MinIO stack; no injected faults or independent attempted-start census')
        elif args.fuzz_soak:
            omes,omes_inputs=prepare_omes()
            probe('fuzz-endpoint',timeout=60)
            event('fuzz-endpoint-created')
            readiness=probe('fuzz-endpoint-ready',timeout=75)
            event('fuzz-endpoint-functionally-ready',**readiness)
            config=json.loads((ROOT/'proof/omes-corpus/soak.json').read_text())
            signal.alarm(config['controller_timeout_seconds'])
            run([sys.executable,'scripts/fuzz-soak.py','--evidence-dir',str(evidence/'fuzz'),'--omes-binary',str(ROOT/'.local/bin/omes'),'--omes-source',str(omes)],config['controller_timeout_seconds']-60)
            report['fuzz']=json.loads((evidence/'fuzz/result.json').read_text())
            if not report['fuzz']['soak_pass']:raise RuntimeError('fuzz soak failed')
            checkpoint('fuzz-finished')
            if report['git_sha']!=run(['git','rev-parse','HEAD']).strip() or run(['git','status','--porcelain=v1','--untracked-files=all']).strip():raise RuntimeError('checkout changed during fuzz soak')
            if any(sha(ROOT/p)!=value for p,value in report['input_sha256'].items()):raise RuntimeError('input changed during fuzz soak')
            if any(sha(ROOT/'.local/bin'/name)!=value for name,value in report['binaries'].items()):raise RuntimeError('runtime binary changed during fuzz soak')
            if any(sha(ROOT/path)!=value for path,value in report['native_artifacts_sha256'].items()):raise RuntimeError('native artifacts changed during fuzz soak')
            if omes_inputs()!=report['omes_generated_sha256']:raise RuntimeError('prepared Omes worker changed during fuzz soak')
            report.update(result='passed',proof_pass=True,scope='saved Omes fuzz soak against real MinIO stack; no injected faults')
        else:
            worker=launch('worker',[sdk,'--mode','worker',*probe_flags]);worker.line('{')
            execution=probe('start');event('workflow-started',**execution)
            wait(lambda:probe('phase').get('phase')=='await-control');event('workflow-durable-pause')
            initial=checkpoint('before-owner-kill')
            if any(successful(initial[name])<=0 for name in ['a','b']):raise RuntimeError('both initial nodes must perform local persistence work')
            owner=assignments[bootstrap['history_partition']]['node']
            update_process=None;cut_deadline=None
            if args.process_cut_stage:
                control=cut_controls[owner];control['partition']=bootstrap['history_partition']
                identity={'namespace_id':bootstrap['namespace_id'],'workflow_id':execution['workflow_id'],'run_id':execution['run_id']}
                temporal_cut.watch(control,nodes[owner].process.pid,identity)
                event('execution-cut-watching',node=owner,watch=identity,session=control['session'],incarnation=control['incarnation'])
                update_process=launch('update-only',[sdk,'--mode','update-only',*probe_flags])
                candidate=temporal_cut.await_stage(control,nodes[owner].process.pid,identity,'candidate')
                event('execution-cut-candidate',candidate=candidate)
                temporal_cut.arm(control,candidate)
                hit=temporal_cut.await_stage(control,nodes[owner].process.pid,identity,'paused',candidate['selector'],timeout=5)
                if nodes[owner].process.poll() is not None:raise RuntimeError('cut process exited before SIGKILL')
                temporal_cut.validate_state(hit,control,nodes[owner].process.pid,'paused',identity,candidate['selector'])
                cut_deadline=time.monotonic()+case['recovery_seconds']
                nodes[owner].stop(kill=True)
                temporal_cut.validate_signal(nodes[owner].process.returncode)
                event('actual-execution-process-cut',node=owner,hit=hit,signal='SIGKILL',recovery_seconds=case['recovery_seconds'])
            else:
                nodes[owner].stop(kill=True);event('active-history-owner-killed',node=owner)
            def owner_recovery_remaining(default):
                if cut_deadline is None:return default
                remaining=cut_deadline-time.monotonic()
                if remaining<=0:raise TimeoutError('locked process-cut recovery deadline exceeded')
                return min(default,remaining)
            port=17351 if owner=='a' else 17352
            # Name is intentionally new: empty local directory and a new process incarnation.
            replacement=owner+'-replacement';node(replacement,port,owner_recovery_remaining(30))
            for assignment in assignments.values():
                if assignment['node']==owner:assignment['node']=replacement
            publish(owner_recovery_remaining(120))
            wait(lambda:probe('phase',timeout=owner_recovery_remaining(20)).get('phase')=='await-control',owner_recovery_remaining(90))
            if update_process is not None:
                update_process.process.wait(timeout=owner_recovery_remaining(case['recovery_seconds']))
                if update_process.process.returncode!=0:raise RuntimeError('SDK update did not recover')
                owner_recovery_remaining(1)
                event('cut-update-acknowledged-after-recovery',update_id=case['workflow_id']+'-update')
            event('workflow-recovered');checkpoint('owner-recovered')
            live_history=evidence/'history-before-cold';live_history.mkdir()
            verify=launch('verify-live',[sdk,'--mode','verify',*probe_flags,'--run-id',execution['run_id'],'--output',str(live_history)])
            verify.line('VERIFY_CLIENT_CONNECTED',timeout=30)
            event('live-sdk-client-connected-before-temporal-kill')
            recovery_deadline=time.monotonic()+case['recovery_seconds']
            def recovery_remaining():
                remaining=recovery_deadline-time.monotonic()
                if remaining<=0:raise TimeoutError('locked Temporal recovery deadline exceeded')
                return remaining
            if recorder:recorder.declare_kill(ta)
            ta.stop(kill=True);event('temporal-instance-killed',pid=ta.process.pid,recovery_seconds=case['recovery_seconds'])
            probe('control',timeout=recovery_remaining());verify.process.wait(timeout=recovery_remaining())
            if verify.process.returncode:raise RuntimeError('live SDK verifier failed')
            event('sdk-sequence-completed')
            wait(lambda:probe('visibility','--query',"WorkflowId = 'xenon-durable-workflow-1' AND ExecutionStatus = 'Completed' AND XenonProof = 'durable'"))
            # Omes uses its own pinned source/module, never the embedded server.
            omes,omes_inputs=prepare_omes()
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
            run(['node','probe.mjs',str(evidence/'browser'),str(ROOT/'test/scenarios/ministack/case.json')],120,cwd=ui)
            if omes_inputs()!=report['omes_generated_sha256']:raise RuntimeError('prepared Omes worker inputs changed during workload')
            if sha(ROOT/'.local/bin/omes')!=report['omes_binary_sha256']:raise RuntimeError('Omes binary changed during workload')
            if run(['git','status','--porcelain=v1','--untracked-files=all'],cwd=omes).strip():raise RuntimeError('Omes source changed')
            event('ui-assertions-passed');checkpoint('before-cold-restart')
            # Explicit final cold-local restart retains only the S3 service volume.
            cold_order=runtime_measurements.producer_first(processes,traces,meter) if measurement_config else processes
            for process in cold_order:
                if not recorder or process is not recorder.process:process.stop()
            nodes.clear();members.clear()
            node('cold-a',17351);node('cold-b',17352)
            for index,assignment in enumerate(assignments.values()):assignment['node']='cold-a' if index%2==0 else 'cold-b'
            publish()
            readiness=case['cold_storage_readiness_seconds']
            probe('storage-ready','--storage-address',f"127.0.0.1:{case['ports']['storage_ingress']}",'--readiness-timeout',str(readiness)+'s',timeout=readiness+5)
            event('cold-storage-ingress-ready')
            healthy_temporal('cold-temporal-a','test/scenarios/ministack/config/temporal-a.json','127.0.0.1:18233')
            healthy_temporal('cold-temporal-b','test/scenarios/ministack/config/temporal-b.json','127.0.0.1:19233')
            cold_history=evidence/'history-after-cold';cold_history.mkdir()
            wait(lambda:probe('verify','--run-id',execution['run_id'],'--output',str(cold_history)),120)
            for original in sorted(live_history.glob('history-*.json')):
                recovered=cold_history/original.name
                if json.loads(original.read_text())!=json.loads(recovered.read_text()):raise RuntimeError('acknowledged history changed after cold recovery')
            report['history_sha256']={str(path.relative_to(evidence)):sha(path) for folder in [live_history,cold_history] for path in folder.glob('history-*.json')}
            wait(lambda:probe('visibility','--query',"WorkflowId = 'xenon-durable-workflow-1' AND ExecutionStatus = 'Completed' AND XenonProof = 'durable'"))
            wait(lambda:probe('visibility','--query',"TaskQueue = 'omes-xenon-ministack-omes' AND ExecutionStatus = 'Completed'",'--expected-count','20'))
            run(['node','probe.mjs',str(evidence/'browser-after-cold'),str(ROOT/'test/scenarios/ministack/case.json')],120,cwd=ui)
            event('cold-local-recovery-passed');checkpoint('cold-recovered')
            if report['git_sha']!=run(['git','rev-parse','HEAD']).strip() or run(['git','status','--porcelain=v1','--untracked-files=all']).strip():raise RuntimeError('checkout changed during runtime')
            if any(sha(ROOT/p)!=value for p,value in report['input_sha256'].items()):raise RuntimeError('input changed during runtime')
            if sha(Path(node_bin)/'node')!=report['ui_node_sha256']:raise RuntimeError('UI Node binary changed during proof')
            if any(sha(ROOT/'.local/bin'/name)!=value for name,value in report['binaries'].items()):raise RuntimeError('runtime binary changed during proof')
            if any(sha(ROOT/path)!=value for path,value in report['native_artifacts_sha256'].items()):raise RuntimeError('native build artifacts changed during proof')
            report.update(result='passed',proof_pass=True)
    except Exception as error:report['error']=str(error)
    finally:
        try:run([*compose,'logs','--no-color'],30)
        except Exception as error:report.setdefault('diagnostic_errors',[]).append(str(error))
        shutdown_order=runtime_measurements.producer_first(list(reversed(processes)),traces) if measurement_config else reversed(processes)
        for process in shutdown_order:
            if recorder and process is recorder.process:continue
            try:process.stop()
            except Exception as error:report.setdefault('cleanup_errors',[]).append(str(error))
        if measurement_config:
            report['functional_result']=report['result'];report['functional_proof_pass']=report['proof_pass']
            try:
                if recorder:
                    census=recorder.finalize()
                    resources=sampler.stop() if sampler else {'complete':False}
                    if sampler:resources['sha256']=sha(evidence/'resources.jsonl')
                    s3=runtime_measurements.read_meter(evidence/'s3-meter.log',meter.process.returncode) if meter else {'complete':False}
                    report['measurements']={'enabled':True,'measurement_complete':census['registration_census_complete'] and resources['complete'] and s3['complete'],'scope':'declared static fault-phase registration census, sampled resources and S3 HTTP aggregates','steady_gate_executed':False,'full_acceptance':False,'recorder':census,'resources':resources,'s3':s3}
                else:
                    report['measurements']=runtime_measurements.finalize(evidence,traces,meter,sampler,measurement_config) if sampler else {'enabled':True,'measurement_complete':False,'error':'measurement setup never completed'}
            except Exception as error:
                if sampler:
                    try:sampler.stop()
                    except Exception:pass
                report['measurements']={'enabled':True,'measurement_complete':False,'error':str(error)}
            if not report['measurements']['measurement_complete']:
                report.update(result='failed',proof_pass=False)
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
