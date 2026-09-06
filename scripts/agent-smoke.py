#!/usr/bin/env python3
"""Reproducible combined-agent component proof. No real AWS credentials accepted."""
import argparse, hashlib, json, os, re, signal, socket, subprocess, sys, time, urllib.request, uuid
from urllib.parse import quote_plus
from pathlib import Path
from omes_workloads import effective_sdk
from agent_control import decode_control, ready_assignments, absent_owner, same_authority

ROOT=Path(__file__).resolve().parents[1]
SCENARIO=ROOT/'test/scenarios/agent'

class ScenarioInvariant(RuntimeError):
    pass


def check_children(children, expected_stops, names):
    for child in children:
        code=child.poll()
        name=names[child.pid]
        if code is not None and child.pid not in expected_stops and ((name != 'omes' and not name.startswith('workload-')) or code != 0):
            raise ScenarioInvariant('background process exited unexpectedly: '+name+' ('+str(code)+')')

class OmesLog:
    # Pinned generic_executor.go logs these terminal iteration errors before
    # worker cleanup. max-iteration-attempts=1 makes them fatal for this profile.
    failure=re.compile(rb'iteration [0-9]+ (?:encountered error|failed):')
    def __init__(self,path):self.path=path;self.offset=0;self.tail=b''
    def check(self):
        with self.path.open('rb') as stream:
            stream.seek(self.offset);data=stream.read((1<<20)+1);self.offset=stream.tell()
        if len(data)>1<<20:raise ScenarioInvariant('Omes log burst exceeds 1 MiB')
        text=self.tail+data
        if self.failure.search(text):raise ScenarioInvariant('terminal Omes iteration failure; see '+str(self.path))
        self.tail=text[-4096:]


def kill_and_collect(process):
    if process.poll() is None:
        try:os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError:pass
    # Reap and retain output even if the process exited between poll and kill.
    return process.communicate(timeout=10)


def agent_ready(config, probe):
    # Frontend health may precede completion of embedded Temporal startup.
    # Require the app's storage + Temporal lifecycle readiness in the same wait.
    with urllib.request.urlopen('http://'+config['diagnostics_address']+'/readyz',timeout=2) as response:
        if response.status != 200 or response.read(64) != b'ready\n':
            return False
    return bool(probe('health','--address','127.0.0.1:'+str(config['base_port'])))


def schema_ready(config, probe, namespace, end):
    remaining=end-time.monotonic()
    if remaining<=0:raise TimeoutError('schema setup deadline')
    observed=probe('schema-ready','--address','127.0.0.1:'+str(config['base_port']),
                   '--namespace',namespace,'--readiness-timeout',str(remaining)+'s')
    if time.monotonic()>=end:raise TimeoutError('schema setup deadline')
    if observed.get('ready') is not True:raise RuntimeError('schema readiness not confirmed')
    return observed


def capture_failure_diagnostics(run, configs, control_command):
    results={}
    commands={name: [sys.executable,'-c',
        'import sys,urllib.request; print(urllib.request.urlopen(sys.argv[1],timeout=2).read(1048576).decode())',
        'http://'+config['diagnostics_address']+'/version'] for name,config in configs.items()}
    commands['control']=control_command
    for name,command in commands.items():
        try:
            run(command,timeout=3 if name != 'control' else 5)
            results[name]='captured'
        except Exception as error:
            results[name]=type(error).__name__+': '+str(error)
    return results


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--development',action='store_true');parser.add_argument('--profile',choices=['smoke','ten-minute'],default='smoke');parser.add_argument('--evidence',type=Path);args=parser.parse_args()
    receipt=args.evidence.resolve() if args.evidence else ROOT/'.local/evidence'/('agent-'+time.strftime('%Y%m%dT%H%M%S')+'-'+uuid.uuid4().hex[:6]);receipt.mkdir(parents=not bool(args.evidence),exist_ok=False)
    project='xenon-agent-'+uuid.uuid4().hex[:10]
    env=os.environ.copy()
    for key in list(env):
        if key.startswith(('AWS_','XENON_','OMES_')):env.pop(key)
    lib=ROOT/'.local/slatedb-native-target/debug'
    env.update(GOENV='off',GOWORK='off',GOFLAGS='-mod=readonly',GOTOOLCHAIN='go1.27.1',CGO_ENABLED='1',CGO_LDFLAGS='-L'+str(lib),DYLD_LIBRARY_PATH=str(lib),LD_LIBRARY_PATH=str(lib),SLATEDB_UNIFFI_RUNTIME_THREADS='2',AWS_ACCESS_KEY_ID='xenon-local',AWS_SECRET_ACCESS_KEY='xenon-local-test-only',AWS_DEFAULT_REGION='us-east-1',AWS_ENDPOINT='http://127.0.0.1:19006',AWS_ALLOW_HTTP='true',AWS_VIRTUAL_HOSTED_STYLE_REQUEST='false')
    compose=['docker','compose','--project-name',project,'-f',str(SCENARIO/'compose.json')]
    agent_case=json.loads((SCENARIO/'case.json').read_text())
    expanded=None
    if args.profile=='ten-minute':
        from agent_ten_minute import expand
        expanded=expand(ROOT)
    ministack_pins=json.loads((ROOT/'test/scenarios/ministack/pins.json').read_text())
    omes_run_id=agent_case['omes_command'][agent_case['omes_command'].index('--run-id')+1]
    omes_queue='omes-'+omes_run_id
    report={'schema':1,'scope':agent_case['scope'],'status':'failed','full_acceptance':False,'development':args.development,'commands':[],'events':[]}
    monitors=[]
    children=[];expected_stops=set();child_names={};logs=[];deadline=time.monotonic()+(expanded['profile']['setup_seconds'] if expanded else 900);started=False
    def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
    def tree(path):return {str(p.relative_to(path)):sha(p) for p in sorted(path.rglob('*')) if p.is_file()}
    def save(name,value):
        with (receipt/name).open('w') as stream:
            json.dump(value,stream,indent=2);stream.write('\n');stream.flush();os.fsync(stream.fileno())
    def record(event):
        report['events'].append(event)
        save('progress.json',report)
    def set_budget(seconds):
        nonlocal deadline
        deadline=time.monotonic()+seconds
    def background_health():
        if report.get('error'):return  # bounded diagnostics retain the already-latched primary
        check_children(children,expected_stops,child_names)
        for monitor in monitors:monitor.check()
        if sum((receipt/name).stat().st_size for name in [p.name for p in receipt.glob('*.log')])>256<<20:
            raise ScenarioInvariant('scenario logs exceed 256 MiB')
    def run(command,timeout=60,cwd=ROOT):
        remaining=deadline-time.monotonic()
        if remaining<=0:raise TimeoutError('scenario deadline')
        path=receipt/('command-'+str(len(report['commands']))+'.log')
        item={'argv':list(map(str,command)),'log':path.name,'timed_out':False}
        report['commands'].append(item)
        save('progress.json',report)
        primary=None
        with path.open('xb') as log:
            monitor=OmesLog(path) if Path(str(command[0])).name=='omes' else None
            p=subprocess.Popen(command,cwd=cwd,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            end=time.monotonic()+min(timeout,remaining)
            try:
                while p.poll() is None:
                    background_health()
                    if monitor:monitor.check()
                    if path.stat().st_size>16<<20:raise ScenarioInvariant('command log exceeds 16 MiB')
                    left=end-time.monotonic()
                    if left<=0:
                        item['timed_out']=True
                        raise TimeoutError('command deadline: '+str(command))
                    try:p.wait(timeout=min(.2,left))
                    except subprocess.TimeoutExpired:pass
                p.wait()
            except BaseException as error:
                primary=error
                try:kill_and_collect(p)
                except Exception as cleanup_error:report.setdefault('command_cleanup_errors',[]).append(str(cleanup_error))
            finally:
                # The scoped command may spawn workers: retire its group even
                # after the command leader exits, without losing its exit status.
                try:os.killpg(p.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                log.flush();os.fsync(log.fileno())
        item.update(returncode=p.returncode,output_sha256=sha(path))
        if primary is not None:raise primary
        if monitor:monitor.check()
        if path.stat().st_size>16<<20:raise ScenarioInvariant('command log exceeds 16 MiB')
        if p.returncode:raise RuntimeError('command failed: '+str(command)+'; see '+str(path))
        return path.read_text()
    def launch(name,command,cwd=receipt):
        log=(receipt/(name+'.log')).open('w');logs.append(log)
        item={'name':name,'argv':list(map(str,command)),'cwd':str(cwd),'log':name+'.log'}
        report.setdefault('background_commands',[]).append(item)
        save('progress.json',report)
        p=subprocess.Popen(command,cwd=cwd,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        item['pid']=p.pid
        if name=='omes' or name.startswith('workload-'):monitors.append(OmesLog(receipt/(name+'.log')))
        children.append(p);child_names[p.pid]=name;return p
    def stop(p,kill=False):
        expected_stops.add(p.pid)
        if p.poll() is not None:
            try:os.killpg(p.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            p.wait();return
        try:os.killpg(p.pid,signal.SIGKILL if kill else signal.SIGTERM)
        except ProcessLookupError:pass
        try:p.wait(timeout=35)
        except subprocess.TimeoutExpired:
            try:os.killpg(p.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            p.wait(timeout=10);raise
    def wait(fn,timeout=120):
        end=min(deadline,time.monotonic()+timeout);last=None
        while time.monotonic()<end:
            background_health()
            try:
                v=fn()
                if v:return v
            except ScenarioInvariant:raise
            except Exception as e:last=e
            time.sleep(.5)
        raise TimeoutError(str(last))
    sdk=ROOT/'.local/bin/xenon-sdk-probe';agent=ROOT/'.local/bin/xenon'
    def topology(name):
        path=receipt/name
        run(['aws','--endpoint-url',env['AWS_ENDPOINT'],'s3api','get-object','--bucket','xenon-agent-proof','--key',agent_case['control_key'],str(path)])
        try:return decode_control(path.read_bytes(),json.loads((SCENARIO/'a.json').read_text()))
        except (ValueError,KeyError,TypeError) as e:raise ScenarioInvariant('invalid control: '+str(e)) from e
    def probe(mode,*flags):
        out=run([str(sdk),'--mode',mode,'--cluster','xenon','--storage-address','127.0.0.1:17935',*flags],timeout=125)
        return json.loads(next(line for line in reversed(out.splitlines()) if line.startswith('{')))
    def omes_running_ids():
        query="TaskQueue = '"+omes_queue+"' AND ExecutionStatus = 'Running'"
        url='http://127.0.0.1:17243/api/v1/namespaces/{}/workflows?pageSize=100&query={}'.format(quote_plus(agent_case['namespace']),quote_plus(query))
        executions=http_json(url).get('executions',[])
        return sorted({item.get('execution',{}).get('workflowId') for item in executions if item.get('execution',{}).get('workflowId')})
    def http_json(url,timeout=10):
        with urllib.request.urlopen(url,timeout=timeout) as response:
            if response.status!=200:raise RuntimeError('unexpected HTTP status '+str(response.status)+' for '+url)
            return json.loads(response.read())
    def probe_temporal_http():
        namespaces=http_json('http://127.0.0.1:17243/api/v1/namespaces')
        listed=[item.get('namespaceInfo',{}).get('name') for item in namespaces.get('namespaces',[])]
        if not listed:raise RuntimeError('temporal HTTP namespaces payload was empty')
        if agent_case['namespace'] not in listed:raise RuntimeError('expected namespace missing from temporal HTTP namespaces')
        workflows=http_json('http://127.0.0.1:17243/api/v1/namespaces/{}/workflows?pageSize=5'.format(quote_plus(agent_case['namespace'])))
        if not isinstance(workflows,dict):raise RuntimeError('temporal HTTP workflow list payload is not an object')
        # Proto JSON omits an empty repeated field before the first workflow exists.
        return {'namespaces_count':len(listed),'workflow_page_size':len(workflows.get('executions',[]))}
    def probe_ui_api():
        base='http://127.0.0.1:'+str(agent_case['ui_port'])
        namespaces=http_json(base+'/api/v1/namespaces')
        listed=[item.get('namespaceInfo',{}).get('name') for item in namespaces.get('namespaces',[])]
        if agent_case['namespace'] not in listed:raise RuntimeError('UI API is not connected to the unified Temporal endpoint')
        query="TaskQueue = '"+omes_queue+"' AND ExecutionStatus = 'Completed'"
        workflows=http_json(base+'/api/v1/namespaces/{}/workflows?pageSize=100&query={}'.format(quote_plus(agent_case['namespace']),quote_plus(query)))
        if len(workflows.get('executions',[]))!=20:raise RuntimeError('UI API did not return the 20 completed Omes workflows')
        return {'namespaces_count':len(listed),'omes_completed_workflows':len(workflows['executions'])}
    def ui_surface():
        ui_port=agent_case['ui_port']
        with urllib.request.urlopen('http://127.0.0.1:'+str(ui_port),timeout=10) as response:
            if response.status!=200:raise RuntimeError('UI root returned '+str(response.status))
            if b'<html' not in response.read(2048).lower():raise RuntimeError('UI root did not return HTML')
        workflow='http://127.0.0.1:{}/namespaces/{}/workflows'.format(ui_port,quote_plus(agent_case['namespace']))
        with urllib.request.urlopen(workflow,timeout=10) as response:
            if response.status!=200:raise RuntimeError('UI workflow page returned '+str(response.status))
            if b'<html' not in response.read(4096).lower():raise RuntimeError('UI workflow route did not return HTML')
        return {'port':ui_port}
    def joined_owner(before):
        c_id=json.loads((SCENARIO/'c.json').read_text())['service_storage']['node_id']
        joined=topology('control-after-c.json')
        owned=ready_assignments(joined,c_id)
        if not owned:return False
        logical=sorted(owned)[0]
        physical=next(p['id'] for p in joined['layout']['partitions'] if p['logical_name']==logical)
        if owned[logical]['assignment_revision']<=before['partitions'][physical]['assignment_revision']:
            raise ScenarioInvariant('join did not advance ownership assignment')
        probe('partition-ready','--storage-address','127.0.0.1:21241','--storage-partition',logical,'--readiness-timeout','60s')
        after=topology('control-after-c-probe.json')
        if not same_authority(owned[logical],after['partitions'][physical]):return False
        return {'node':c_id,'partition':logical,'physical_partition':physical,'generation':owned[logical]['generation']}
    schema_required=False
    def observe_schema(name,config,end):
        observed=schema_ready(config,probe,agent_case['namespace'],end)
        report['events'].append({'event':'agent-schema-ready','node':name,'address':'127.0.0.1:'+str(config['base_port']),'observation':observed})
        return True
    def start(name,label=None,wait_health=True):
        p=launch(label or name,[str(agent),'start','--config',str(SCENARIO/(name+'.json'))])
        config=json.loads((SCENARIO/(name+'.json')).read_text())
        if wait_health:
            ready_end=min(deadline,time.monotonic()+120)
            wait(lambda:agent_ready(config,probe) and (not schema_required or observe_schema(name,config,ready_end)),max(0,ready_end-time.monotonic()))
            if p.poll() is not None:raise RuntimeError('agent exited '+name)
            report['events'].append({'event':'agent-healthy','node':name,'pid':p.pid})
        return p
    try:
        report['revision']=run(['git','rev-parse','HEAD']).strip()
        report['dirty']=bool(run(['git','status','--porcelain=v1','--untracked-files=all']).strip())
        if report['dirty'] and not args.development:raise RuntimeError('clean committed checkout required')
        report['harness_hashes']={name:sha(ROOT/'scripts'/name) for name in ['agent-smoke.py','agent_control.py','omes_workloads.py']}
        report['schema_probe_hashes']={name:sha(ROOT/'cmd/xenon-sdk-probe'/name) for name in ['main.go','schema_readiness.go']}
        report['scenario_hashes']={p.name:sha(p) for p in sorted(SCENARIO.iterdir()) if p.is_file()}
        report['reused_ministack_input_hashes']={'pins.json':sha(ROOT/'test/scenarios/ministack/pins.json')}
        if ministack_pins['ui']['image']!=json.loads((SCENARIO/'compose.json').read_text())['services']['ui']['image']:raise RuntimeError('agent UI image pin differs from ministack pin')
        for port in [19006,17935,17233,17243,18080,18250,19250,21250,*range(18233,18243),*range(19233,19243),*range(21233,21243)]:
            with socket.socket() as s:
                s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                s.bind(('127.0.0.1',port))
        report['profile']=args.profile
        if expanded:
            report['scope']=expanded['profile']['scope']
            save('expanded-profile.json',expanded)
        report['tracked_input_sha256']={name:sha(ROOT/name) for name in run(['git','ls-files']).splitlines() if (ROOT/name).is_file()}
        save('initial.json',report)
        run(['python3','scripts/build-go-node.py'],600)
        for name in ['xenon-sdk-probe','xenon-ingress',*(['xenon-omes-oracle'] if expanded else [])]:
            run(['go','build','-o',str(ROOT/'.local/bin'/name),'./cmd/'+name],600)
        report['binaries']={name:sha(ROOT/'.local/bin'/name) for name in ['xenon','xenon-sdk-probe','xenon-ingress',*(['xenon-omes-oracle'] if expanded else [])]}
        report['native_build']=json.loads((ROOT/'.local/go-node-build.json').read_text())
        report['version']=json.loads(run([str(agent),'version']))
        started=True;run([*compose,'up','-d','--pull','never'],90)
        wait(lambda:urllib.request.urlopen(env['AWS_ENDPOINT']+'/minio/health/live',timeout=2).status==200,30)
        ingress=ROOT/'.local/bin/xenon-ingress'
        launch('storage-ingress',[str(ingress),'--listen','127.0.0.1:17935','--backends','127.0.0.1:18241,127.0.0.1:19241,127.0.0.1:21241'])
        # The pinned UI container reaches the host through its bridge gateway.
        launch('temporal-ingress',[str(ingress),'--listen','0.0.0.0:17233','--backends','127.0.0.1:18233,127.0.0.1:19233,127.0.0.1:21233'])
        launch('temporal-http-ingress',[str(ingress),'--listen','127.0.0.1:17243','--backends','127.0.0.1:18242,127.0.0.1:19242,127.0.0.1:21242'])
        run(['aws','--endpoint-url',env['AWS_ENDPOINT'],'s3api','create-bucket','--bucket','xenon-agent-proof'])
        a=start('a');b=start('b')
        schema_end=min(deadline,time.monotonic()+120)
        wait(lambda:probe('bootstrap'),max(0,schema_end-time.monotonic()))
        for name in ['a','b']:
            config=json.loads((SCENARIO/(name+'.json')).read_text())
            wait(lambda:observe_schema(name,config,schema_end),max(0,schema_end-time.monotonic()))
        schema_required=True
        wait(lambda:ui_surface(),30)
        report['http_surface']=probe_temporal_http()
        report['events'].append({'event':'temporal-http-and-ui-ready','http':report['http_surface'],'ui':{'port':agent_case['ui_port']}})
        worker=launch('worker',[str(sdk),'--mode','worker'])
        execution=probe('start');report['execution']=execution
        wait(lambda:probe('phase').get('phase')=='await-control',120)
        omes_source=ROOT/'.local/omes-source'
        if not omes_source.exists():
            run(['git','clone','--no-checkout',ministack_pins['omes']['repository'],str(omes_source)],900)
        run(['git','checkout','--detach',ministack_pins['omes']['commit']],cwd=omes_source)
        if run(['git','status','--porcelain=v1','--untracked-files=all'],cwd=omes_source).strip():raise RuntimeError('dirty Omes source')
        if expanded:
            from agent_ten_minute import run as run_ten_minute
            probe('control')
            initial_history=receipt/'sdk-acknowledged-before-fuzz';initial_history.mkdir()
            probe('verify','--run-id',execution['run_id'],'--output',str(initial_history))
            agents={'a':a,'b':b}
            run_ten_minute(ROOT,receipt,expanded,command=run,launch=launch,stop=stop,wait=wait,
                probe=probe,topology=topology,joined_owner=joined_owner,start=start,agents=agents,
                check_children=background_health,
                set_budget=set_budget,record=record,report=report)
            a,b,c=agents['a'],agents['b'],agents['c']
        else:
            run(['go','build','-o',str(ROOT/'.local/bin/omes'),'./cmd/omes'],900,cwd=omes_source)
            run([str(ROOT/'.local/bin/omes'),'prepare-worker','--language','go','--version',ministack_pins['omes']['worker_go_sdk'],'--dir-name','prepared'],900,cwd=omes_source)
            report['omes_source_commit']=run(['git','rev-parse','HEAD'],cwd=omes_source).strip()
            if report['omes_source_commit']!=ministack_pins['omes']['commit']:raise RuntimeError('Omes source pin mismatch')
            report['omes_binary_sha256']=sha(ROOT/'.local/bin/omes')
            worker_info=run(['go','version','-m',str(omes_source/'workers/go/prepared/program')]).strip()
            report['effective_omes_worker_sdk']=effective_sdk(worker_info)
            if report['effective_omes_worker_sdk']['version']!=ministack_pins['omes']['worker_go_sdk']:raise RuntimeError('prepared Omes SDK pin mismatch')
            report['omes_worker_build_info']=worker_info
            report['omes_worker_sha256']=sha(omes_source/'workers/go/prepared/program')
            report['omes_prepared_sha256']=tree(omes_source/'workers/go/prepared')
            omes=launch('omes',[str(ROOT/'.local/bin/omes'),*agent_case['omes_command']],cwd=omes_source)
            wait(lambda:probe('visibility-count','--query',"TaskQueue = '"+omes_queue+"'").get('count',0)>0,90)
            join_runs=wait(omes_running_ids,90)
            report['events'].append({'event':'omes-running-observed-before-join','workflow_ids':join_runs})
            before_join=topology('control-before-c.json')
            c=start('c');report['events'].append({'event':'join-started-during-active-sdk-and-omes-workflows'})
            report['events'].append({'event':'joined-agent-served-assigned-partition',**wait(lambda:joined_owner(before_join),90)})
            fault_runs=wait(omes_running_ids,90)
            stop(b,kill=True);report['events'].append({'event':'SIGKILL-during-running-omes-execution','node':'b','workflow_ids':fault_runs})
            if omes.poll() is not None:raise RuntimeError('Omes runner stopped before agent failure and recovery')
            b_id=json.loads((SCENARIO/'b.json').read_text())['service_storage']['node_id']
            wait(lambda:absent_owner(topology('control-after-eviction.json'),b_id),45)
            evicted=topology('control-after-eviction-final.json')
            if not absent_owner(evicted,b_id):raise RuntimeError('failed member retains partition')
            report['events'].append({'event':'failed-member-evicted-and-rebalanced','node':'b'})
            b=start('b','b-restarted')
            wait(lambda:omes.poll() is not None,360)
            if omes.returncode:raise RuntimeError('Omes workload failed')
            probe('visibility','--query',"TaskQueue = '"+omes_queue+"' AND ExecutionStatus = 'Completed'",'--expected-count','20')
            for workflow_id in sorted(set(join_runs+fault_runs)):
                probe('visibility','--query',"WorkflowId = '"+workflow_id+"' AND ExecutionStatus = 'Completed'")
            report['ui_api']=probe_ui_api()
            report['events'].append({'event':'omes-and-ui-passed-through-unified-endpoint','ui':report['ui_api']})
            if sha(ROOT/'.local/bin/omes')!=report['omes_binary_sha256'] or sha(omes_source/'workers/go/prepared/program')!=report['omes_worker_sha256'] or tree(omes_source/'workers/go/prepared')!=report['omes_prepared_sha256'] or run(['git','rev-parse','HEAD'],cwd=omes_source).strip()!=report['omes_source_commit']:raise RuntimeError('Omes inputs changed during workload')
        if not expanded:probe('control')
        before=receipt/'history-before';before.mkdir()
        probe('verify','--run-id',execution['run_id'],'--output',str(before))
        stop(worker)
        for p in [a,b,c]:stop(p)
        a=start('a','a-cold',False);b=start('b','b-cold',False);c=start('c','c-cold',False)
        for name,p in [('a',a),('b',b),('c',c)]:
            config=json.loads((SCENARIO/(name+'.json')).read_text())
            ready_end=min(deadline,time.monotonic()+120)
            wait(lambda:agent_ready(config,probe) and observe_schema(name,config,ready_end),max(0,ready_end-time.monotonic()))
            if p.poll() is not None:raise RuntimeError('cold agent exited '+name)
            report['events'].append({'event':'cold-agent-healthy','node':name,'pid':p.pid})
        after=receipt/'history-after';after.mkdir()
        probe('verify','--run-id',execution['run_id'],'--output',str(after))
        originals=sorted(before.glob('history-*.json'))
        if not originals:raise RuntimeError('missing history oracle')
        for old in originals:
            if json.loads(old.read_text())!=json.loads((after/old.name).read_text()):raise RuntimeError('cold history mismatch')
        if expanded:
            set_budget(expanded['profile']['verification_seconds'])
            run([str(ROOT/'.local/bin/xenon-omes-oracle'),'--omes-run-id','xenon-ministack-fuzz',
                 '--minimum-runs','20','--output',str(receipt/'fuzz-histories-cold')],timeout=expanded['profile']['verification_seconds'])
            original=receipt/'fuzz-histories'
            recovered=receipt/'fuzz-histories-cold'
            if tree(original)!=tree(recovered):raise ScenarioInvariant('cold fuzz histories or inventory changed')
            report['ui_api_after_cold']=probe_temporal_http()
            ui_surface()
            record({'event':'identical-sdk-and-fuzz-histories-after-cold-recovery'})
        else:
            probe('visibility','--query',"TaskQueue = '"+omes_queue+"' AND ExecutionStatus = 'Completed'",'--expected-count','20')
            report['ui_api_after_cold']=probe_ui_api()
            report['events'].append({'event':'identical-histories-after-cold-recovery'})
            report['events'].append({'event':'omes-visibility-and-ui-recovered-after-cold-restart','ui':report['ui_api_after_cold']})
        if run(['git','rev-parse','HEAD']).strip()!=report['revision']:raise RuntimeError('source revision changed')
        if not args.development and run(['git','status','--porcelain=v1','--untracked-files=all']).strip():raise RuntimeError('source changed')
        if any(sha(ROOT/name)!=digest for name,digest in report['tracked_input_sha256'].items()):raise RuntimeError('input bytes changed')
        if any(sha(ROOT/'.local/bin'/name)!=digest for name,digest in report['binaries'].items()):raise RuntimeError('binary changed')
        native=report['native_build']
        if sha(ROOT/native['shared_library'])!=native['shared_library_sha256']:raise RuntimeError('native library changed')
        check_children(children,expected_stops,child_names)
        report['status']='development-passed' if args.development else 'component-passed'
    except BaseException as e:
        report['error']=type(e).__name__+': '+str(e)
        save('first-failure.json',{'error':report['error'],'events':report['events']})
    finally:
        expected_stops.update(p.pid for p in children)
        cleanup=[]
        if report.get('error') and started:
            # A separate bounded diagnostic budget does not extend workload or
            # recovery deadlines. Failure observations cannot replace first cause.
            try:
                deadline=time.monotonic()+agent_case['failure_diagnostics_seconds']
                configs={name:json.loads((SCENARIO/(name+'.json')).read_text()) for name in agent_case['agents']}
                report['failure_diagnostics']=capture_failure_diagnostics(run,configs,
                    ['aws','--endpoint-url',env['AWS_ENDPOINT'],'s3api','get-object','--bucket','xenon-agent-proof',
                     '--key',agent_case['control_key'],str(receipt/'control-at-failure.json')])
            except Exception as error:
                report['failure_diagnostics_error']=type(error).__name__+': '+str(error)
        cleanup_deadline=time.monotonic()+(expanded['profile']['cleanup_seconds'] if expanded else 120)
        # Signal all groups first. Waiting for one slow process must not leave
        # other writers running past the shared cleanup budget.
        for p in reversed(children):
            try:os.killpg(p.pid,signal.SIGTERM)
            except ProcessLookupError:pass
            except Exception as e:cleanup.append(str(e))
        for p in reversed(children):
            try:p.wait(timeout=max(.001,cleanup_deadline-time.monotonic()-20))
            except subprocess.TimeoutExpired:cleanup.append('process exceeded graceful cleanup: '+child_names[p.pid])
            except Exception as e:cleanup.append(str(e))
        for p in reversed(children):
            try:
                try:os.killpg(p.pid,signal.SIGKILL)
                except ProcessLookupError:pass
                p.wait(timeout=max(.001,cleanup_deadline-time.monotonic()-15))
            except Exception as e:cleanup.append(str(e))
        if started:
            deadline=cleanup_deadline
            try:run([*compose,'down','--volumes'],60)
            except Exception as e:cleanup.append(str(e))
        for log in logs:log.close()
        report['log_sha256']={path.name:sha(path) for path in receipt.glob('*.log')}
        try:
            if any(sha(ROOT/name)!=digest for name,digest in report.get('tracked_input_sha256',{}).items()):
                raise ScenarioInvariant('source/input bytes changed during run or cleanup')
            if any(sha(ROOT/'.local/bin'/name)!=digest for name,digest in report.get('binaries',{}).items()):
                raise ScenarioInvariant('binary changed during run or cleanup')
            native=report.get('native_build')
            if native and sha(ROOT/native['shared_library'])!=native['shared_library_sha256']:
                raise ScenarioInvariant('native library changed during run or cleanup')
        except Exception as error:
            cleanup.append(str(error))

        if cleanup:report['cleanup_errors']=cleanup;report['status']='failed'
        save('result.json',report)
        print(json.dumps({'receipt':str(receipt),'status':report['status'],'error':report.get('error')}),flush=True)
    return 0 if report['status'].endswith('passed') else 1
if __name__=='__main__':
    def interrupted(signum,frame):raise RuntimeError('scenario interrupted by signal '+str(signum))
    for signum in (signal.SIGINT,signal.SIGTERM):signal.signal(signum,interrupted)
    raise SystemExit(main())
