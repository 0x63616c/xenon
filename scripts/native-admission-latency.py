#!/usr/bin/env python3
"""Bounded native MinIO experiment; no production settings are changed."""
import argparse,hashlib,json,os,pathlib,subprocess,time,urllib.request,uuid
ROOT=pathlib.Path(__file__).resolve().parents[1]
def main():
 parser=argparse.ArgumentParser();parser.add_argument('--native-dir',required=True);parser.add_argument('--output',required=True);args=parser.parse_args()
 out=pathlib.Path(args.output).resolve();out.mkdir(parents=True,exist_ok=False)
 case=ROOT/'test/scenarios/native-latency/case.json';cfg=json.loads(case.read_text());name='xenon-latency-'+uuid.uuid4().hex[:10]
 env=os.environ.copy();env.update(AWS_ACCESS_KEY_ID='xenon-local',AWS_SECRET_ACCESS_KEY='xenon-local-test-only',AWS_DEFAULT_REGION='us-east-1',AWS_ENDPOINT=f'http://127.0.0.1:{cfg["port"]}',AWS_ALLOW_HTTP='true',AWS_VIRTUAL_HOSTED_STYLE_REQUEST='false',XENON_ENGINE_STORE='s3://'+cfg['bucket'],XENON_LATENCY_OUTPUT=str(out/'samples.json'),CGO_LDFLAGS='-L'+args.native_dir,DYLD_LIBRARY_PATH=args.native_dir,LD_LIBRARY_PATH=args.native_dir,SLATEDB_UNIFFI_RUNTIME_THREADS='2',GOTOOLCHAIN='go1.27.1')
 report={'status':'running','commands':[]}
 def run(argv,timeout=90):
  result=subprocess.run(argv,cwd=ROOT,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=timeout)
  log=f'command-{len(report["commands"])}.log';(out/log).write_bytes(result.stdout);report['commands'].append({'argv':argv,'returncode':result.returncode,'log':log,'sha256':hashlib.sha256(result.stdout).hexdigest()})
  if result.returncode:raise RuntimeError(f'{argv}: see {log}')
  return result.stdout.decode()
 try:
  report['revision']=run(['git','rev-parse','HEAD']).strip();report['dirty']=bool(run(['git','status','--porcelain']).strip())
  if report['dirty']:raise RuntimeError('clean committed source required')
  report['inputs']={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [case,pathlib.Path(__file__),ROOT/'internal/partitions/slatedb/latency_experiment_test.go',ROOT/'internal/partitions/slatedb/engine.go',ROOT/'internal/partitions/slatedb/operation.go',ROOT/'go.mod',ROOT/'go.sum']}
  lib=pathlib.Path(args.native_dir)/('libslatedb_uniffi.dylib' if os.uname().sysname=='Darwin' else 'libslatedb_uniffi.so');report['native_sha256']=hashlib.sha256(lib.read_bytes()).hexdigest();report['go']=run(['go','version']).strip()
  run(['docker','run','--detach','--name',name,'--publish',f'127.0.0.1:{cfg["port"]}:9000','--env','MINIO_ROOT_USER=xenon-local','--env','MINIO_ROOT_PASSWORD=xenon-local-test-only',cfg['image'],'server','/data'])
  end=time.monotonic()+30
  while True:
   try:
    if urllib.request.urlopen(env['AWS_ENDPOINT']+'/minio/health/live',timeout=1).status==200:break
   except Exception:
    if time.monotonic()>=end:raise
    time.sleep(.1)
  run(['aws','--endpoint-url',env['AWS_ENDPOINT'],'s3api','create-bucket','--bucket',cfg['bucket']])
  run(['go','test','-race','./internal/partitions/slatedb','-run','^TestNativeAdmissionLatencyExperiment$','-count=1','-timeout=120s'],180)
  report['samples_sha256']=hashlib.sha256((out/'samples.json').read_bytes()).hexdigest();report['status']='passed'
 except BaseException as error:report['status']='failed';report['error']=str(error)
 finally:
  try:run(['docker','rm','--force','--volumes',name],30)
  except Exception as error:report['status']='failed';report['cleanup_error']=str(error)
  (out/'result.json').write_text(json.dumps(report,indent=2)+'\n')
 return 0 if report['status']=='passed' else 1
if __name__=='__main__':raise SystemExit(main())
