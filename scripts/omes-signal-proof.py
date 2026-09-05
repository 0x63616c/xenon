#!/usr/bin/env python3
"""Reproduce the pinned Omes mismatch and audited additive compatibility overlay."""
import signal
import argparse, hashlib, json, os, platform, shutil, subprocess, sys, tarfile, time, uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
CASE=ROOT/'test/scenarios/omes-signals'
OMES='c6978ba39aa03551ce28974117e8d7ecf983d2b3'
API='d96bd55e87799e9f6a33a1c40a56cfa932566bdf'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser();p.add_argument('--source',required=True,type=Path);p.add_argument('--api-source',required=True,type=Path);p.add_argument('--protoc',required=True,type=Path);a=p.parse_args()
 evidence=ROOT/'.local/evidence'/(time.strftime('%Y%m%dT%H%M%SZ',time.gmtime())+'-omes-signals-'+uuid.uuid4().hex[:8]);evidence.mkdir(parents=True)
 report={'proof_pass':False,'result':'failed','runtime_executed':False,'overlay':True,'commands':[],'platform':platform.platform(),'python':sys.version}
 env={**os.environ,'GOENV':'off','GOWORK':'off','GOFLAGS':'-mod=readonly','GOTOOLCHAIN':'go1.27.1'}
 def run(argv,cwd=ROOT,expected=0):
  process=subprocess.Popen([str(v) for v in argv],cwd=cwd,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
  try:out,err=process.communicate(timeout=120)
  except BaseException:
   os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=10);raise
  q=subprocess.CompletedProcess(argv,process.returncode,out,err)
  log=evidence/('command-'+str(len(report['commands']))+'.log');log.write_bytes(q.stdout+q.stderr)
  report['commands'].append({'argv':[str(v) for v in argv],'exit_code':q.returncode,'log':log.name,'sha256':sha(log)})
  if (expected==0 and q.returncode!=0) or (expected!=0 and q.returncode==0):raise ValueError('unexpected command result: '+log.name)
  return q.stdout.decode()
 try:
  contract=json.loads((CASE/'contract.json').read_text())
  for field,path in [('overlay_sha256',CASE/'overlay.patch'),('normalizer_sha256',CASE/'normalize.go.in'),('original_corpus_manifest_sha256',ROOT/'proof/omes-corpus/manifest.json')]:
   if sha(path)!=contract[field]:raise ValueError('contract hash mismatch: '+field)
  for item in contract['inputs']:
   if sha(ROOT/'proof/omes-corpus/inputs'/item['name'])!=item['original_sha256'] or sha(CASE/'corrected-v1'/item['name'])!=item['corrected_sha256']:raise ValueError('corpus contract mismatch')
  report['go_version']=run(['go','version']).strip()
  if not report['go_version'].startswith('go version go1.27.1 '):raise ValueError('Go pin')
  report['git_sha']=run(['git','rev-parse','HEAD']).strip()
  if run(['git','status','--porcelain']).strip():raise ValueError('clean checkout required')
  for source,pin in [(a.source,OMES),(a.api_source,API)]:
   if run(['git','rev-parse','HEAD'],source).strip()!=pin or run(['git','status','--porcelain'],source).strip():raise ValueError('pinned clean upstream checkout required')
  if run([a.protoc,'--version']).strip()!='libprotoc 36.0':raise ValueError('protoc pin')
  report['protoc_sha256']=sha(a.protoc)
  inputs=[Path(v) for v in run(['git','ls-files','test/scenarios/omes-signals','scripts/omes-signal-proof.py','proof/omes-corpus','tools/protoc.json']).splitlines()]
  report['inputs']={str(x):sha(ROOT/x) for x in inputs}
  source=evidence/'source';source.mkdir();archive=evidence/'upstream.tar'
  with archive.open('wb') as stream:subprocess.run(['git','archive',OMES],cwd=a.source,stdout=stream,check=True,timeout=30)
  with tarfile.open(archive) as stream:stream.extractall(source,filter='data')
  worker=source/'workers/go/workerlib/kitchensink';shutil.copyfile(CASE/'red_test.go.in',worker/'xenon_red_test.go')
  run(['go','test','./workerlib/kitchensink','-run','^TestXenonOriginalGeneratedReturn$','-count=1'],source/'workers/go',expected=1)
  if 'ORIGINAL_NUMBERED_RETURN_SKIPPED' not in (evidence/report['commands'][-1]['log']).read_text():raise ValueError('red test failed for unrelated reason')
  (worker/'xenon_red_test.go').unlink()
  run(['git','apply',CASE/'overlay.patch'],source)
  tools=evidence/'tools';tools.mkdir();env['GOBIN']=str(tools)
  run(['go','install','google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0'],source);env['PATH']=str(tools)+os.pathsep+env['PATH']
  report['protoc_gen_go_sha256']=sha(tools/'protoc-gen-go')
  run([a.protoc,'-I'+str(a.api_source),'-I'+str(source/'workers/proto/kitchen_sink'),'--go_out='+str(source/'loadgen/kitchensink'),'--go_opt=paths=source_relative',source/'workers/proto/kitchen_sink/kitchen_sink.proto'],source)
  report['generated_pb_sha256']=sha(source/'loadgen/kitchensink/kitchen_sink.pb.go')
  shutil.copyfile(CASE/'worker_signal_test.go.in',worker/'xenon_signal_test.go')
  run(['go','test','./workerlib/kitchensink','-run','^TestXenon','-count=1'],source/'workers/go')
  shutil.copyfile(CASE/'normalize.go.in',source/'normalize_xenon.go')
  run(['go','build','-o',tools/'normalize','normalize_xenon.go'],source)
  report['normalizer_sha256']=sha(tools/'normalize');report['corpus']=[]
  for original in sorted((ROOT/'proof/omes-corpus/inputs').glob('*.proto')):
   generated=evidence/original.name;second=evidence/(original.stem+'-repeat.proto')
   run([tools/'normalize',original,generated]);run([tools/'normalize',original,second])
   if generated.read_bytes()!=second.read_bytes() or generated.read_bytes()!=(CASE/'corrected-v1'/original.name).read_bytes():raise ValueError('corrected corpus mismatch')
   report['corpus'].append({'input':original.name,'original_sha256':sha(original),'corrected_sha256':sha(generated)})
  if len(report['corpus'])!=20:raise ValueError('corpus count changed')
  if run(['git','rev-parse','HEAD']).strip()!=report['git_sha'] or run(['git','status','--porcelain']).strip():raise ValueError('source changed')
  if any(sha(ROOT/k)!=v for k,v in report['inputs'].items()):raise ValueError('input changed')
  report.update(proof_pass=True,result='passed')
 except Exception as error:report['error']=str(error)
 finally:
  (evidence/'result.json').write_text(json.dumps(report,indent=2)+'\n');print(report['result'].upper()+': '+str(evidence/'result.json'))
 return 0 if report['proof_pass'] else 1
if __name__=='__main__':sys.exit(main())
