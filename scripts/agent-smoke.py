#!/usr/bin/env python3
"""Reproducible combined-agent component proof. No real AWS credentials accepted."""
import argparse, hashlib, json, os, signal, socket, subprocess, time, urllib.request, uuid
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
SCENARIO=ROOT/'test/scenarios/agent'

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--development',action='store_true');args=parser.parse_args()
    receipt=ROOT/'.local/evidence'/('agent-'+time.strftime('%Y%m%dT%H%M%S')+'-'+uuid.uuid4().hex[:6]);receipt.mkdir(parents=True)
    project='xenon-agent-'+uuid.uuid4().hex[:10]
    env=os.environ.copy()
    for key in list(env):
        if key.startswith(('AWS_','XENON_')):env.pop(key)
    lib=ROOT/'.local/slatedb-native-target/debug'
    env.update(GOENV='off',GOWORK='off',GOFLAGS='-mod=readonly',GOTOOLCHAIN='go1.27.1',CGO_ENABLED='1',CGO_LDFLAGS='-L'+str(lib),DYLD_LIBRARY_PATH=str(lib),LD_LIBRARY_PATH=str(lib),SLATEDB_UNIFFI_RUNTIME_THREADS='2',AWS_ACCESS_KEY_ID='xenon-local',AWS_SECRET_ACCESS_KEY='xenon-local-test-only',AWS_DEFAULT_REGION='us-east-1',AWS_ENDPOINT='http://127.0.0.1:19006',AWS_ALLOW_HTTP='true',AWS_VIRTUAL_HOSTED_STYLE_REQUEST='false')
    compose=['docker','compose','--project-name',project,'-f',str(SCENARIO/'compose.json')]
    report={'schema':1,'scope':json.loads((SCENARIO/'case.json').read_text())['scope'],'status':'failed','full_acceptance':False,'development':args.development,'commands':[],'events':[]}
    children=[];logs=[];deadline=time.monotonic()+900;started=False
    def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
    def run(command,timeout=60):
        remaining=deadline-time.monotonic()
        if remaining<=0:raise TimeoutError('scenario deadline')
        p=subprocess.run(command,cwd=ROOT,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=min(timeout,remaining))
        path=receipt/('command-'+str(len(report['commands']))+'.log');path.write_text(p.stdout)
        report['commands'].append({'argv':list(map(str,command)),'returncode':p.returncode,'output_sha256':sha(path)})
        if p.returncode:raise RuntimeError('command failed: '+str(command)+'; see '+str(path))
        return p.stdout
    def launch(name,command):
        log=(receipt/(name+'.log')).open('w');logs.append(log)
        p=subprocess.Popen(command,cwd=receipt,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
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
    def probe(mode,*flags):
        out=run([str(sdk),'--mode',mode,'--cluster','xenon','--storage-address','127.0.0.1:17935',*flags],timeout=125)
        return json.loads(next(line for line in reversed(out.splitlines()) if line.startswith('{')))
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
        for port in [19006,17935,17233,17243,18250,19250,21250,*range(18233,18243),*range(19233,19243),*range(21233,21243)]:
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
        launch('temporal-ingress',[str(ingress),'--listen','127.0.0.1:17233','--backends','127.0.0.1:18233,127.0.0.1:19233,127.0.0.1:21233'])
        launch('temporal-http-ingress',[str(ingress),'--listen','127.0.0.1:17243','--backends','127.0.0.1:18242,127.0.0.1:19242,127.0.0.1:21242'])
        run(['aws','--endpoint-url',env['AWS_ENDPOINT'],'s3api','create-bucket','--bucket','xenon-agent-proof'])
        a=start('a');b=start('b')
        wait(lambda:probe('bootstrap'),120)
        worker=launch('worker',[str(sdk),'--mode','worker'])
        execution=probe('start');report['execution']=execution
        wait(lambda:probe('phase').get('phase')=='await-control',120)
        c=start('c');report['events'].append({'event':'joined-c-during-active-workflow'})
        topology_file=receipt/'topology-after-c.json'
        run(['aws','--endpoint-url',env['AWS_ENDPOINT'],'s3api','get-object','--bucket','xenon-agent-proof','--key','agent/metadata/topology.json',str(topology_file)])
        topology=json.loads(topology_file.read_text())
        if topology['partitions']['history-1']['node']!='c':raise RuntimeError('joined agent did not receive history-1')
        probe('partition-ready','--storage-address','127.0.0.1:21241','--storage-partition','history-1','--readiness-timeout','60s')
        report['events'].append({'event':'joined-agent-served-assigned-partition','node':'c','partition':'history-1'})
        stop(b,kill=True);report['events'].append({'event':'SIGKILL','node':'b'})
        b=start('b','b-restarted')
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
        report['events'].append({'event':'identical-histories-after-cold-recovery'})
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
