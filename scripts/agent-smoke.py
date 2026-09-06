#!/usr/bin/env python3
"""Reproducible combined-agent component proof. No real AWS credentials accepted."""
import argparse, hashlib, json, os, signal, socket, subprocess, time, urllib.request, uuid
from urllib.parse import quote_plus
from pathlib import Path
from omes_workloads import effective_sdk

ROOT=Path(__file__).resolve().parents[1]
SCENARIO=ROOT/'test/scenarios/agent'

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--development',action='store_true');args=parser.parse_args()
    receipt=ROOT/'.local/evidence'/('agent-'+time.strftime('%Y%m%dT%H%M%S')+'-'+uuid.uuid4().hex[:6]);receipt.mkdir(parents=True)
    project='xenon-agent-'+uuid.uuid4().hex[:10]
    env=os.environ.copy()
    for key in list(env):
        if key.startswith(('AWS_','XENON_','OMES_')):env.pop(key)
    lib=ROOT/'.local/slatedb-native-target/debug'
    env.update(GOENV='off',GOWORK='off',GOFLAGS='-mod=readonly',GOTOOLCHAIN='go1.27.1',CGO_ENABLED='1',CGO_LDFLAGS='-L'+str(lib),DYLD_LIBRARY_PATH=str(lib),LD_LIBRARY_PATH=str(lib),SLATEDB_UNIFFI_RUNTIME_THREADS='2',AWS_ACCESS_KEY_ID='xenon-local',AWS_SECRET_ACCESS_KEY='xenon-local-test-only',AWS_DEFAULT_REGION='us-east-1',AWS_ENDPOINT='http://127.0.0.1:19006',AWS_ALLOW_HTTP='true',AWS_VIRTUAL_HOSTED_STYLE_REQUEST='false')
    compose=['docker','compose','--project-name',project,'-f',str(SCENARIO/'compose.json')]
    agent_case=json.loads((SCENARIO/'case.json').read_text())
    ministack_pins=json.loads((ROOT/'test/scenarios/ministack/pins.json').read_text())
    omes_run_id=agent_case['omes_command'][agent_case['omes_command'].index('--run-id')+1]
    omes_queue='omes-'+omes_run_id
    report={'schema':1,'scope':agent_case['scope'],'status':'failed','full_acceptance':False,'development':args.development,'commands':[],'events':[]}
    children=[];logs=[];deadline=time.monotonic()+900;started=False
    def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
    def tree(path):return {str(p.relative_to(path)):sha(p) for p in sorted(path.rglob('*')) if p.is_file()}
    def run(command,timeout=60,cwd=ROOT):
        remaining=deadline-time.monotonic()
        if remaining<=0:raise TimeoutError('scenario deadline')
        p=subprocess.Popen(command,cwd=cwd,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,start_new_session=True)
        timed_out=False
        try:output,_=p.communicate(timeout=min(timeout,remaining))
        except subprocess.TimeoutExpired:
            timed_out=True;os.killpg(p.pid,signal.SIGKILL);tail,_=p.communicate()
            output=tail
        path=receipt/('command-'+str(len(report['commands']))+'.log');path.write_text(output)
        report['commands'].append({'argv':list(map(str,command)),'returncode':p.returncode,'timed_out':timed_out,'output_sha256':sha(path)})
        if timed_out or p.returncode:raise RuntimeError('command failed: '+str(command)+'; see '+str(path))
        return output
    def launch(name,command,cwd=receipt):
        log=(receipt/(name+'.log')).open('w');logs.append(log)
        p=subprocess.Popen(command,cwd=cwd,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        children.append(p);return p
    def stop(p,kill=False):
        if p.poll() is not None:return
        os.killpg(p.pid,signal.SIGKILL if kill else signal.SIGTERM)
        try:p.wait(timeout=35)
        except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait(timeout=10);raise
    def wait(fn,timeout=120):
        end=min(deadline,time.monotonic()+timeout);last=None
        while time.monotonic()<end:
            try:
                v=fn()
                if v:return v
            except Exception as e:last=e
            time.sleep(.5)
        raise TimeoutError(str(last))
    sdk=ROOT/'.local/bin/xenon-sdk-probe';agent=ROOT/'.local/bin/xenon'
    def topology(name):
        path=receipt/name
        run(['aws','--endpoint-url',env['AWS_ENDPOINT'],'s3api','get-object','--bucket','xenon-agent-proof','--key','agent/metadata/topology.json',str(path)])
        return json.loads(path.read_text())
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
        if 'executions' not in workflows:raise RuntimeError('temporal HTTP workflow list payload missing executions')
        return {'namespaces_count':len(listed),'workflow_page_size':len(workflows['executions'])}
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
    def start(name,label=None,wait_health=True):
        p=launch(label or name,[str(agent),'start','--config',str(SCENARIO/(name+'.json'))])
        base=json.loads((SCENARIO/(name+'.json')).read_text())['base_port']
        if wait_health:
            wait(lambda:probe('health','--address','127.0.0.1:'+str(base)),120)
            if p.poll() is not None:raise RuntimeError('agent exited '+name)
            report['events'].append({'event':'agent-healthy','node':name,'pid':p.pid})
        return p
    try:
        report['revision']=run(['git','rev-parse','HEAD']).strip()
        report['dirty']=bool(run(['git','status','--porcelain=v1','--untracked-files=all']).strip())
        if report['dirty'] and not args.development:raise RuntimeError('clean committed checkout required')
        report['scenario_hashes']={p.name:sha(p) for p in sorted(SCENARIO.iterdir()) if p.is_file()}
        report['reused_ministack_input_hashes']={'pins.json':sha(ROOT/'test/scenarios/ministack/pins.json')}
        if ministack_pins['ui']['image']!=json.loads((SCENARIO/'compose.json').read_text())['services']['ui']['image']:raise RuntimeError('agent UI image pin differs from ministack pin')
        for port in [19006,17935,17233,17243,18080,18250,19250,21250,*range(18233,18243),*range(19233,19243),*range(21233,21243)]:
            with socket.socket() as s:
                s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                s.bind(('127.0.0.1',port))
        run(['python3','scripts/build-go-node.py'],600)
        for name in ['xenon-sdk-probe','xenon-ingress']:
            run(['go','build','-o',str(ROOT/'.local/bin'/name),'./cmd/'+name],600)
        report['binaries']={name:sha(ROOT/'.local/bin'/name) for name in ['xenon','xenon-sdk-probe','xenon-ingress']}
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
        wait(lambda:probe('bootstrap'),120)
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
        c=start('c');report['events'].append({'event':'join-started-during-active-sdk-and-omes-workflows'})
        joined=topology('topology-after-c.json')
        if joined['partitions']['history-1']['node']!='c':raise RuntimeError('joined agent did not receive history-1')
        probe('partition-ready','--storage-address','127.0.0.1:21241','--storage-partition','history-1','--readiness-timeout','60s')
        report['events'].append({'event':'joined-agent-served-assigned-partition','node':'c','partition':'history-1'})
        fault_runs=wait(omes_running_ids,90)
        stop(b,kill=True);report['events'].append({'event':'SIGKILL-during-running-omes-execution','node':'b','workflow_ids':fault_runs})
        if omes.poll() is not None:raise RuntimeError('Omes runner stopped before agent failure and recovery')
        wait(lambda:'b' not in topology('topology-after-eviction.json')['members'],45)
        evicted=topology('topology-after-eviction-final.json')
        if any(a['node']=='b' for a in evicted['partitions'].values()):raise RuntimeError('evicted member retains partition')
        report['events'].append({'event':'failed-member-evicted-and-rebalanced','node':'b'})
        b=start('b','b-restarted')
        omes.wait(timeout=min(360,max(1,deadline-time.monotonic())))
        if omes.returncode:raise RuntimeError('Omes workload failed')
        probe('visibility','--query',"TaskQueue = '"+omes_queue+"' AND ExecutionStatus = 'Completed'",'--expected-count','20')
        for workflow_id in sorted(set(join_runs+fault_runs)):
            probe('visibility','--query',"WorkflowId = '"+workflow_id+"' AND ExecutionStatus = 'Completed'")
        report['ui_api']=probe_ui_api()
        report['events'].append({'event':'omes-and-ui-passed-through-unified-endpoint','ui':report['ui_api']})
        if sha(ROOT/'.local/bin/omes')!=report['omes_binary_sha256'] or sha(omes_source/'workers/go/prepared/program')!=report['omes_worker_sha256'] or tree(omes_source/'workers/go/prepared')!=report['omes_prepared_sha256'] or run(['git','rev-parse','HEAD'],cwd=omes_source).strip()!=report['omes_source_commit']:raise RuntimeError('Omes inputs changed during workload')
        probe('control')
        before=receipt/'history-before';before.mkdir()
        probe('verify','--run-id',execution['run_id'],'--output',str(before))
        stop(worker)
        for p in [a,b,c]:stop(p)
        a=start('a','a-cold',False);b=start('b','b-cold',False);c=start('c','c-cold',False)
        for name,base,p in [('a',18233,a),('b',19233,b),('c',21233,c)]:
            wait(lambda base=base:probe('health','--address','127.0.0.1:'+str(base)),120)
            if p.poll() is not None:raise RuntimeError('cold agent exited '+name)
            report['events'].append({'event':'cold-agent-healthy','node':name,'pid':p.pid})
        after=receipt/'history-after';after.mkdir()
        probe('verify','--run-id',execution['run_id'],'--output',str(after))
        originals=sorted(before.glob('history-*.json'))
        if not originals:raise RuntimeError('missing history oracle')
        for old in originals:
            if json.loads(old.read_text())!=json.loads((after/old.name).read_text()):raise RuntimeError('cold history mismatch')
        probe('visibility','--query',"TaskQueue = '"+omes_queue+"' AND ExecutionStatus = 'Completed'",'--expected-count','20')
        report['ui_api_after_cold']=probe_ui_api()
        report['events'].append({'event':'identical-histories-after-cold-recovery'})
        report['events'].append({'event':'omes-visibility-and-ui-recovered-after-cold-restart','ui':report['ui_api_after_cold']})
        if run(['git','rev-parse','HEAD']).strip()!=report['revision']:raise RuntimeError('source revision changed')
        if not args.development and run(['git','status','--porcelain=v1','--untracked-files=all']).strip():raise RuntimeError('source changed')
        report['status']='development-passed' if args.development else 'component-passed'
    except Exception as e:report['error']=str(e)
    finally:
        cleanup=[]
        for p in reversed(children):
            try:stop(p)
            except Exception as e:cleanup.append(str(e))
        if started:
            deadline=time.monotonic()+90
            try:run([*compose,'down','--volumes'],60)
            except Exception as e:cleanup.append(str(e))
        for log in logs:log.close()
        if cleanup:report['cleanup_errors']=cleanup;report['status']='failed'
        (receipt/'result.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps({'receipt':str(receipt),'status':report['status'],'error':report.get('error')}),flush=True)
    return 0 if report['status'].endswith('passed') else 1
if __name__=='__main__':raise SystemExit(main())
