#!/usr/bin/env python3
"""Build an explicitly overlaid Omes CLI/worker from a clean pinned source archive."""
import argparse,json,os,subprocess,tarfile
from pathlib import Path
from omes_workloads import ROOT,OMES,sha,tree,effective_sdk,command,output
from corrected_fuzz import source_tree
API='d96bd55e87799e9f6a33a1c40a56cfa932566bdf'
CASE=ROOT/'test/scenarios/omes-signals'
def prepare(base,destination):
 destination.mkdir(parents=True,exist_ok=False)
 report={'schema':1,'prepared':False,'upstream_commit':OMES,'api_commit':API,'compatibility_overlay':True,'commands':[]}
 env=dict(os.environ,GOENV='off',GOWORK='off',GOFLAGS='-mod=readonly -buildvcs=false',GOTOOLCHAIN='go1.27.1')
 def run(argv,cwd=ROOT,timeout=900):
  r=command(list(map(str,argv)),destination/f'command-{len(report["commands"])}.log',timeout,cwd,env);report['commands'].append(r)
  if r['exit_code'] or r['timed_out'] or r.get('interrupted'):raise RuntimeError('overlay build command failed')
 try:
  if output(['git','rev-parse','HEAD'],base)!=OMES or output(['git','status','--porcelain'],base):raise ValueError('clean pinned upstream required')
  contract=json.loads((CASE/'contract.json').read_text());report['contract_sha256']=sha(CASE/'contract.json')
  if sha(CASE/'overlay.patch')!=contract['overlay_sha256']:raise ValueError('overlay patch mismatch')
  report['patch_sha256']=contract['overlay_sha256']
  archive=destination/'source.tar';source=destination/'source';source.mkdir()
  with archive.open('xb') as stream:subprocess.run(['git','archive',OMES],cwd=base,stdout=stream,check=True,timeout=30)
  report['upstream_archive_sha256']=sha(archive)
  with tarfile.open(archive) as stream:stream.extractall(source,filter='data')
  run(['git','init','--quiet'],source,30);run(['git','apply',CASE/'overlay.patch'],source,30)
  api=destination/'api';run(['git','clone','--no-checkout','https://github.com/temporalio/api',api],timeout=120);run(['git','checkout','--detach',API],api,30)
  run(['python3','scripts/install-protoc.py'],timeout=120)
  protoc=ROOT/'.local/protoc/bin/protoc'
  if output([str(protoc),'--version'],ROOT)!='libprotoc 36.0':raise ValueError('protoc version')
  tools=destination/'tools';tools.mkdir();env['GOBIN']=str(tools)
  run(['go','install','google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0'],timeout=120);env['PATH']=str(tools)+os.pathsep+env['PATH']
  report['protoc_sha256']=sha(protoc);report['protoc_gen_go_sha256']=sha(tools/'protoc-gen-go')
  report['protoc_version']='36.0';report['protoc_gen_go_version']='v1.31.0'
  run([protoc,'-I'+str(api),'-I'+str(source/'workers/proto/kitchen_sink'),'--go_out='+str(source/'loadgen/kitchensink'),'--go_opt=paths=source_relative',source/'workers/proto/kitchen_sink/kitchen_sink.proto'],source,30)
  report['generated_pb_sha256']=sha(source/'loadgen/kitchensink/kitchen_sink.pb.go')
  baseline=source_tree(source)
  binary=destination/'omes';run(['go','build','-buildvcs=false','-o',binary,'./cmd/omes'],source)
  run([binary,'prepare-worker','--language','go','--version','v1.48.0','--dir-name','prepared'],source)
  prepared=source/'workers/go/prepared'
  report.update(source=str(source),binary=str(binary),binary_sha256=sha(binary),worker_build_info=output(['go','version','-m',str(prepared/'program')],ROOT),binary_build_info=output(['go','version','-m',str(binary)],ROOT),source_sha256=baseline,prepared_sha256=tree(prepared))
  report['effective_worker_sdk']=effective_sdk(report['worker_build_info'])
  if source_tree(source)!=baseline or not report['prepared_sha256']:raise ValueError('overlay source changed during build')
  if output(['git','rev-parse','HEAD'],api)!=API or output(['git','status','--porcelain'],api):raise ValueError('API changed')
  if sha(protoc)!=report['protoc_sha256']:raise ValueError('protoc changed')
  report['prepared']=True
 except Exception as error:report['error']=str(error)
 finally:(destination/'build.json').write_text(json.dumps(report,indent=2)+'\n')
 return report
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--source',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 r=prepare(a.source.resolve(),a.output.resolve());print(json.dumps({'prepared':r['prepared'],'manifest':str(a.output/'build.json')}));raise SystemExit(0 if r['prepared'] else 1)
