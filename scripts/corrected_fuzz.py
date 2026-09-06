#!/usr/bin/env python3
"""Explicit corrected-corpus soak; original profiles and commands remain unchanged."""
import argparse,json,os,time,signal
from pathlib import Path
from omes_workloads import ROOT,OMES,sha,tree,effective_sdk,command,output
CASE=ROOT/'test/scenarios/omes-signals'
def source_tree(source):
 return {str(p.relative_to(source)):sha(p) for p in sorted(source.rglob('*')) if p.is_file() and '.git' not in p.relative_to(source).parts and not str(p.relative_to(source)).startswith('workers/go/prepared/')}
def configuration(root=ROOT):
 case=root/'test/scenarios/omes-signals';config=json.loads((case/'soak.json').read_text());original=json.loads((root/'proof/omes-corpus/soak.json').read_text())
 for key in ['minimum_seconds','minimum_rounds','maximum_rounds','controller_timeout_seconds','command_timeout_seconds','faults']:
  if config[key]!=original[key]:raise ValueError('corrected soak weakened or changed original limits')
 if config['minimum_seconds']!=3600 or config['minimum_rounds']!=2 or config['command_timeout_seconds']!=900 or config['corpus']!='test/scenarios/omes-signals/replay.json' or config['compatibility_overlay'] is not True or config['corrected_corpus_version']!='corrected-v1':raise ValueError('unregistered corrected profile')
 replay=json.loads((case/'replay.json').read_text());old=json.loads((root/'proof/omes-corpus/replay.json').read_text())
 expected=[]
 for command in old['commands']:
  expected.append([arg.replace('input-file=proof/omes-corpus/inputs/','input-file=test/scenarios/omes-signals/corrected-v1/') for arg in command])
 if replay['commands']!=expected or len(expected)!=20:raise ValueError('corrected command contract changed')
 contract=json.loads((case/'contract.json').read_text())
 if contract['original_source']!=OMES or sha(case/'overlay.patch')!=contract['overlay_sha256'] or sha(case/'normalize.go.in')!=contract['normalizer_sha256']:raise ValueError('overlay contract changed')
 for item in contract['inputs']:
  if sha(root/'proof/omes-corpus/inputs'/item['name'])!=item['original_sha256'] or sha(case/'corrected-v1'/item['name'])!=item['corrected_sha256']:raise ValueError('corpus bytes changed')
 if len(contract['inputs'])!=20:raise ValueError('corrected corpus count')
 return config,replay,contract

def check_build(manifest,build):
 if not manifest['prepared'] or manifest.get('compatibility_overlay') is not True or manifest['upstream_commit']!=OMES or manifest['protoc_version']!='36.0' or manifest['protoc_gen_go_version']!='v1.31.0':raise ValueError('overlay build is not ready/pinned')
 if manifest['patch_sha256']!=sha(CASE/'overlay.patch') or manifest['contract_sha256']!=sha(CASE/'contract.json'):raise ValueError('overlay build contract mismatch')
 source=Path(manifest['source']);binary=Path(manifest['binary'])
 if source.parent!=build.parent or binary.parent!=build.parent:raise ValueError('overlay build paths escape bundle')
 if source_tree(source)!=manifest['source_sha256'] or tree(source/'workers/go/prepared')!=manifest['prepared_sha256'] or sha(binary)!=manifest['binary_sha256']:raise ValueError('overlay build artifacts changed')
 if sha(source/'loadgen/kitchensink/kitchen_sink.pb.go')!=manifest['generated_pb_sha256']:raise ValueError('generated overlay schema changed')
 return source,binary

def soak(evidence,build):
 evidence.mkdir(parents=True,exist_ok=False);started=time.monotonic();report={'schema':1,'soak_pass':False,'full_acceptance':False,'compatibility_overlay':True,'corrected_corpus_version':'corrected-v1','rounds':[],'faults_injected':False}
 try:
  config,replay,contract=configuration();manifest=json.loads(build.read_text());report['build_manifest_sha256']=sha(build);report['build']=manifest
  report['git_sha']=output(['git','rev-parse','HEAD'],ROOT)
  if output(['git','status','--porcelain'],ROOT):raise ValueError('clean Xenon checkout required')
  inputs=output(['git','ls-files'],ROOT).splitlines();report['input_sha256']={p:sha(ROOT/p) for p in inputs}
  source,binary=check_build(manifest,build)
  report['effective_worker_sdk']=effective_sdk(output(['go','version','-m',str(source/'workers/go/prepared/program')],ROOT))
  info=output(['go','version','-m',str(binary)],ROOT)
  if info!=manifest['binary_build_info'] or 'path\tgithub.com/temporalio/omes/cmd/omes' not in info or 'vcs.revision=' in info:raise ValueError('overlay CLI metadata mismatch')
  if any(k.startswith('OMES_') for k in os.environ):raise ValueError('ambient OMES overrides forbidden')
  env=dict(os.environ,GOENV='off',GOWORK='off',GOFLAGS='-mod=readonly',GOTOOLCHAIN='go1.27.1')
  for index in range(config['maximum_rounds']):
   folder=evidence/f'round-{index:04d}';folder.mkdir();round={'index':index,'workload_completed':False,'commands':[]};report['rounds'].append(round)
   try:
    for position,declared in enumerate(replay['commands']):
     configuration();check_build(manifest,build)
     item=command([str(binary),*declared[1:]],folder/f'command-{position:02d}.log',config['command_timeout_seconds'],ROOT,env);round['commands'].append(item)
     if item['exit_code'] or item['timed_out'] or item.get('interrupted'):raise RuntimeError('corrected fuzz input failed')
    round['workload_completed']=True
   finally:(folder/'result.json').write_text(json.dumps(round,indent=2)+'\n')
   if index+1>=config['minimum_rounds'] and time.monotonic()-started>=config['minimum_seconds']:report['soak_pass']=True;break
  if not report['soak_pass']:raise ValueError('round limit before minimum soak duration')
  if sha(build)!=report['build_manifest_sha256'] or output(['git','rev-parse','HEAD'],ROOT)!=report['git_sha'] or output(['git','status','--porcelain'],ROOT):raise ValueError('proof source changed')
  configuration();check_build(manifest,build)
  if any(sha(ROOT/p)!=v for p,v in report['input_sha256'].items()):raise ValueError('proof inputs changed')
 except Exception as error:report.update(soak_pass=False,error=str(error))
 finally:
  report['elapsed_seconds']=time.monotonic()-started;(evidence/'result.json').write_text(json.dumps(report,indent=2)+'\n')
 return report
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--evidence-dir',type=Path,required=True);p.add_argument('--build-manifest',type=Path,required=True);a=p.parse_args()
 def interrupted(signum,frame):raise RuntimeError('corrected soak interrupted')
 for sig in [signal.SIGINT,signal.SIGTERM]:signal.signal(sig,interrupted)
 r=soak(a.evidence_dir.resolve(),a.build_manifest.resolve());print(json.dumps({'soak_pass':r['soak_pass'],'compatibility_overlay':True,'full_acceptance':False}));raise SystemExit(0 if r['soak_pass'] else 1)
