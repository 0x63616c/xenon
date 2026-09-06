#!/usr/bin/env python3
"""Bounded layout comparison; reuse pinned native build, MinIO and S3 meter."""
import argparse
import hashlib
import json
import math
import os
import platform
from pathlib import Path
import queue
import signal
import subprocess
import sys
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[3]
CASE = ROOT / "benchmarks/storage/layout/case.json"

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def run(argv, env, timeout=60):
    r = subprocess.run(argv, cwd=ROOT, env=env, text=True, capture_output=True, timeout=timeout)
    if r.returncode: raise RuntimeError(f"command failed {argv}: {r.stdout}\n{r.stderr}")
    return r.stdout.strip()
def cpu_seconds(value):
    parts = value.split(":")
    return sum(float(v)*60**i for i,v in enumerate(reversed(parts)))
def quantiles(values):
    values = sorted(values)
    return {"count": len(values), **{name: values[max(0,math.ceil(p*len(values))-1)] for name,p in (("p50",.5),("p95",.95),("p99",.99))}}
def delta(before,after):
    fields=("attempts","request_body_bytes_read","response_body_bytes_written","transport_errors","canceled","aborted")
    return {field:after[field]-before[field] for field in fields} | {"inflight_at_start":before["inflight"],"inflight_at_end":after["inflight"],"methods":{k:after['methods'].get(k,0)-before['methods'].get(k,0) for k in set(after['methods'])|set(before['methods'])}}
def execute_case(binary, writers, index, env, cfg, evidence):
    case_env = dict(env, XENON_LAYOUT_BUCKET=f"layout-proof-{index}")
    argv=[str(binary),"-config",str(CASE),"-writers",str(writers)]
    log=evidence/f"case-{index}-writers-{writers}.jsonl"
    errors=evidence/f"case-{index}-stderr.log"
    events=[];samples=[];phase="starting";messages=queue.Queue();started=time.monotonic()
    with log.open('w') as output, errors.open('w') as stderr:
        child=subprocess.Popen(argv,cwd=ROOT,env=case_env,text=True,stdout=subprocess.PIPE,stderr=stderr,start_new_session=True)
        def read():
            for line in child.stdout: messages.put(line)
            messages.put(None)
        reader=threading.Thread(target=read,daemon=True);reader.start()
        eof=False;next_sample=started
        try:
            while not eof or child.poll() is None:
                now=time.monotonic()
                if now-started>cfg['case_timeout_seconds']+5: raise RuntimeError("case wall budget exceeded")
                if child.poll() is None and now>=next_sample:
                    stat=subprocess.run(['ps','-o','rss=,time=','-p',str(child.pid)],text=True,capture_output=True,timeout=2)
                    fields=stat.stdout.split()
                    if fields:
                        rss=int(fields[0])*1024
                        samples.append({'pid':child.pid,'elapsed_seconds':now-started,'phase':phase,'rss_bytes':rss,'cpu_seconds':cpu_seconds(fields[1])})
                        if rss>cfg['max_rss_mib']*1024*1024: raise RuntimeError("declared process RSS resource bound exceeded")
                    next_sample=now+cfg['rss_sample_ms']/1000
                try: line=messages.get(timeout=.02)
                except queue.Empty: continue
                if line is None: eof=True;continue
                output.write(line);output.flush()
                event=json.loads(line);events.append(event)
                if event.get('event')=='failure': raise RuntimeError('workload failure: '+str(event.get('error','missing cause')))
                if event.get('event')=='phase': phase=event['phase']
            code=child.wait(timeout=2)
            if code or not events or events[-1].get('event')!='passed': raise RuntimeError(f"case failed: exit={code}; see {log}")
        finally:
            if child.poll() is None:
                os.killpg(child.pid,signal.SIGKILL)
                child.wait(timeout=5)
            reader.join(timeout=2)
            child.stdout.close()
            (evidence/f"case-{index}-samples.json").write_text(json.dumps(samples,indent=2)+'\n')
    phases={e['phase']:e for e in events if e.get('event')=='phase'}
    workload=next(e for e in events if e.get('event')=='workload_result')
    operations=workload['operations']
    if len(operations)!=256 or len({(x['shard'],x['round']) for x in operations})!=256: raise RuntimeError('workload identity count mismatch')
    memory={}
    for name in ('baseline','idle','loaded','takeover','reopen','recovered_idle'):
        values=[x['rss_bytes'] for x in samples if x['phase']==name]
        if values: memory[name]={'samples':len(values),'min_bytes':min(values),'max_bytes':max(values),'median_bytes':sorted(values)[len(values)//2]}
    if 'baseline' not in memory or 'idle' not in memory or 'loaded' not in memory: raise RuntimeError('missing footprint samples')
    base=memory['baseline']['median_bytes']
    return {'index':index,'writers':writers,'argv':argv,'operations':len(operations),'loaded_seconds':workload['elapsed_ns']/1e9,
            'operations_per_second':len(operations)/(workload['elapsed_ns']/1e9),
            'end_to_end_ms':quantiles([x['end_to_end_ns']/1e6 for x in operations]),
            'own_durable_ms':quantiles([x['own_durable_ns']/1e6 for x in operations]),
            'rss':memory,'idle_rss_delta_bytes':memory['idle']['median_bytes']-base,'loaded_peak_delta_bytes':memory['loaded']['max_bytes']-base,
            'cpu_samples':{'first_seconds':samples[0]['cpu_seconds'],'last_seconds':samples[-1]['cpu_seconds']},
            's3':{'open':delta(phases['open']['meter'],phases['idle']['meter']),
                  'idle':delta(phases['idle']['meter'],phases['loaded']['meter']),
                  'loaded':delta(phases['loaded']['meter'],phases['loaded_done']['meter']),
                  'takeover':delta(phases['takeover']['meter'],phases['close']['meter']),
                  'reopen':delta(phases['reopen']['meter'],phases['recovered_idle']['meter']),
                  'recovered_idle':delta(phases['recovered_idle']['meter'],phases['final_close']['meter'])},
            'open':next(e for e in events if e.get('event')=='open_result'),
            'takeover':next(e for e in events if e.get('event')=='takeover_result'),
            'reopen':next(e for e in events if e.get('event')=='reopen_result')}

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--allow-dirty',action='store_true');args=parser.parse_args()
    cfg=json.loads(CASE.read_text())
    if cfg['schema']!=1 or cfg['layouts']!=[1,4,16,16,4,1]: raise RuntimeError('unregistered layout profile')
    env=dict(os.environ,GOENV='off',GOWORK='off',GOFLAGS='-mod=readonly',GOTOOLCHAIN='go1.27.1',CGO_ENABLED='1',SLATEDB_UNIFFI_RUNTIME_THREADS='2',GOMAXPROCS=str(cfg['gomaxprocs']))
    project='xenon-layout-'+uuid.uuid4().hex[:12]
    evidence=ROOT/'.local/evidence'/project;evidence.mkdir(parents=True)
    report={'schema':1,'project':project,'status':'failed','cases':[],'source_revision':run(['git','rev-parse','HEAD'],env),
            'platform':{'system':platform.system(),'release':platform.release(),'machine':platform.machine(),'logical_cpus':os.cpu_count()},
            'dirty':run(['git','status','--porcelain=v1','--untracked-files=all'],env),'config':cfg,'input_hashes':{}}
    compose=['docker','compose','--project-name',project,'-f',cfg['minio_compose']]
    try:
        if report['dirty'] and not args.allow_dirty: raise RuntimeError('clean checkout required (or development --allow-dirty)')
        if report['dirty']: report['dirty_patch_sha256']=hashlib.sha256(run(['git','diff','HEAD','--binary'],env).encode()).hexdigest()
        inputs=[CASE,Path(__file__).resolve(),ROOT/'benchmarks/storage/layout/main.go',ROOT/'benchmarks/storage/layout/test_run.py',ROOT/'benchmarks/storage/layout/README.md',ROOT/cfg['minio_compose'],ROOT/'scripts/build-go-node.py',ROOT/'tools/slatedb-native.json',ROOT/'go.mod',ROOT/'go.sum',ROOT/'rust-toolchain.toml']
        inputs += list((ROOT/'internal/partitions').rglob('*.go'))+list((ROOT/'internal/proof/s3meter').glob('*.go'))+list((ROOT/'internal/identity').glob('*.go'))
        inputs += list((ROOT/'benchmarks/storage/layout').glob('*_test.go'))
        report['input_hashes']={str(p.relative_to(ROOT)):sha(p) for p in inputs}
        report['versions']={name:run(argv,env) for name,argv in {'go':['go','version'],'rust':['rustc','--version'],'docker':['docker','version','--format','{{.Server.Version}}'],'compose':['docker','compose','version','--short']}.items()}
        for key in report['versions']:
            if not report['versions'][key].startswith(cfg[key]): raise RuntimeError('tool pin mismatch: '+key)
        (evidence/'result.json').write_text(json.dumps(report,indent=2)+'\n')
        build=run([sys.executable,'scripts/build-go-node.py'],env,600);(evidence/'build.log').write_text(build+'\n')
        report['native_build']=json.loads((ROOT/'.local/go-node-build.json').read_text())
        native=ROOT/'.local/slatedb-native-target/debug';env.update(CGO_LDFLAGS='-L'+str(native),DYLD_LIBRARY_PATH=str(native),LD_LIBRARY_PATH=str(native))
        report['worker_failure_control']=run(['go','test','-race','-count=1','-timeout=30s','./benchmarks/storage/layout'],env,60)
        binary=ROOT/'.local/bin/layout-measure';run(['go','build','-o',str(binary),'./benchmarks/storage/layout'],env,120);report['binary_sha256']=sha(binary)
        run([*compose,'up','-d','--wait'],env)
        endpoint=run([*compose,'port','s3','9000'],env)
        if not endpoint.startswith('127.0.0.1:'): raise RuntimeError('non-loopback MinIO bind')
        env.update(AWS_ENDPOINT='http://'+endpoint,AWS_ACCESS_KEY_ID='xenon-local',AWS_SECRET_ACCESS_KEY='xenon-local-test-only',AWS_DEFAULT_REGION='us-east-1',AWS_ALLOW_HTTP='true',AWS_VIRTUAL_HOSTED_STYLE_REQUEST='false')
        import urllib.request
        deadline=time.monotonic()+30
        while True:
            try:
                with urllib.request.urlopen(env['AWS_ENDPOINT']+'/minio/health/live',timeout=2) as response:
                    if response.status==200: break
            except OSError:
                if time.monotonic()>deadline: raise RuntimeError('MinIO readiness budget exceeded')
                time.sleep(.1)
        for index,writers in enumerate(cfg['layouts']):
            print(f'case {index+1}/6: {writers} physical writers, 16 logical shards',flush=True)
            report['cases'].append(execute_case(binary,writers,index,env,cfg,evidence))
            (evidence/'result.json').write_text(json.dumps(report,indent=2)+'\n')
        for filename,digest in report['input_hashes'].items():
            if sha(ROOT/filename)!=digest: raise RuntimeError('input changed during measurement: '+filename)
        if run(['git','rev-parse','HEAD'],env)!=report['source_revision'] or run(['git','status','--porcelain=v1','--untracked-files=all'],env)!=report['dirty']: raise RuntimeError('checkout changed during measurement')
        if sha(binary)!=report['binary_sha256'] or sha(ROOT/report['native_build']['shared_library'])!=report['native_build']['shared_library_sha256']: raise RuntimeError('binary changed during measurement')
        report['status']='development-passed' if report['dirty'] else 'passed'
    except BaseException as error:
        report['error']=str(error)
    finally:
        try:
            run([*compose,'down','--volumes','--timeout','5'],env)
            for base in (['docker','ps','-aq'],['docker','volume','ls','-q'],['docker','network','ls','-q']):
                remaining=run([*base,'--filter','label=com.docker.compose.project='+project],env)
                if remaining: raise RuntimeError('scoped resources survived cleanup: '+remaining)
            report['cleanup']='passed'
        except BaseException as error:
            report['cleanup_error']=str(error);report['status']='failed'
        (evidence/'result.json').write_text(json.dumps(report,indent=2)+'\n')
        print(report['status']+': '+str(evidence/'result.json'),flush=True)
    return 0 if report['status'] in ('passed','development-passed') else 1
if __name__=='__main__':
    def interrupted(signum,frame): raise RuntimeError('layout runner interrupted: '+str(signum))
    signal.signal(signal.SIGTERM,interrupted);signal.signal(signal.SIGINT,interrupted)
    sys.exit(main())
