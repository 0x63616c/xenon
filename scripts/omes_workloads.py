#!/usr/bin/env python3
"""Run frozen Omes commands against an already supervised stack; never assert acceptance."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

from validate_acceptance import validate
ROOT=Path(__file__).resolve().parents[1]
OMES='c6978ba39aa03551ce28974117e8d7ecf983d2b3'
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def output(argv,cwd):return subprocess.check_output(argv,cwd=cwd,text=True,timeout=30).strip()
def tree(path):return {str(p.relative_to(path)):sha(p) for p in sorted(path.rglob('*')) if p.is_file()}
def corpus(root):
 manifest=json.loads((root/'proof/omes-corpus/manifest.json').read_text())
 if len(manifest['inputs'])!=20:raise ValueError('expected20savedinputs')
 for item in manifest['inputs']:
  p=root/'proof/omes-corpus'/item['file']
  if p.resolve().parent!=(root/'proof/omes-corpus/inputs').resolve() or sha(p)!=item['sha256'] or p.stat().st_size!=item['bytes']:raise ValueError('saved corpus bytes changed')
 return {item['file']:item['sha256'] for item in manifest['inputs']}
def command(argv,path,timeout,cwd=ROOT,env=None):
 started=time.monotonic();result={'argv':list(map(str,argv)),'timeout_seconds':timeout,'started_unix':time.time(),'log':path.name,'timed_out':False}
 with path.open('xb') as log:
  p=subprocess.Popen(argv,cwd=cwd,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
  try:result['exit_code']=p.wait(timeout=timeout)
  except BaseException as error:
   result['timed_out']=isinstance(error,subprocess.TimeoutExpired)
   try:os.killpg(p.pid,signal.SIGKILL)
   except ProcessLookupError:pass
   p.wait(timeout=10)
   result['exit_code']=p.returncode
   result['interrupted']=not result['timed_out']
  finally:
   # The worker may be a child left behind even after its parent exits.
   try:os.killpg(p.pid,signal.SIGKILL)
   except ProcessLookupError:pass
 result.update(duration_seconds=time.monotonic()-started,log_sha256=sha(path))
 return result

def run_workload(stage,profile,evidence,omes_binary,omes_source,root=ROOT):
 evidence=Path(evidence);evidence.mkdir(parents=True,exist_ok=False)
 report={'schema':1,'stage':stage,'profile':profile,'workload_completed':False,'full_acceptance':False,'commands':[]}
 try:
  config=validate(root)
  report['source_commit']=output(['git','rev-parse','HEAD'],root)
  if output(['git','status','--porcelain=v1','--untracked-files=all'],root):raise ValueError('dirty Xenon source')
  if output(['git','rev-parse','HEAD'],omes_source)!=OMES or output(['git','status','--porcelain=v1','--untracked-files=all'],omes_source):raise ValueError('Omes source pin/cleanliness mismatch')
  if any(k.startswith('OMES_') for k in os.environ):raise ValueError('ambient OMES option overrides forbidden')
  info=output(['go','version','-m',str(omes_binary)],root)
  if 'path\tgithub.com/temporalio/omes/cmd/omes' not in info or 'vcs.revision='+OMES not in info or 'vcs.modified=false' not in info:raise ValueError('Omes binary VCS provenance mismatch')
  report['omes_build_info']=info
  prepared=Path(omes_source)/'workers/go/prepared'
  worker_info=output(['go','version','-m',str(prepared/'program')],root)
  if '\tdep\tgo.temporal.io/sdk\tv1.48.0\t' not in worker_info or '\t=>' in worker_info:raise ValueError('Prepared worker SDK dependency mismatch/replacement')
  report['worker_build_info']=worker_info
  baseline=tree(prepared)
  if not baseline:raise ValueError('missing prepared Go worker')
  report['prepared_sha256']=baseline;report['binary_sha256']=sha(omes_binary)
  report['corpus_sha256']=corpus(root)
  report['profile_sha256']=sha(root/'proof/acceptance/full-profile.json')
  report['replay_sha256']=sha(root/'proof/omes-corpus/replay.json')
  if stage=='fuzz':commands=json.loads((root/'proof/omes-corpus/replay.json').read_text())['commands']
  else:commands=[config['workloads'][stage]]
  env=dict(os.environ,GOENV='off',GOWORK='off',GOFLAGS='-mod=readonly',GOTOOLCHAIN='go1.27.1')
  for index,declared in enumerate(commands):
   # Recheck immutable inputs before each invocation as well as at finalization.
   if corpus(root)!=report['corpus_sha256'] or sha(omes_binary)!=report['binary_sha256'] or tree(prepared)!=baseline:raise ValueError('workload inputs changed')
   argv=[str(omes_binary),*declared[1:]]
   item=command(argv,evidence/f'command-{index:02d}.log',900,cwd=root,env=env);report['commands'].append(item)
   if item['timed_out'] or item.get('interrupted') or item['exit_code']!=0:raise RuntimeError('Omes workload failed or exceeded declared deadline')
  if output(['git','rev-parse','HEAD'],root)!=report['source_commit'] or output(['git','status','--porcelain=v1','--untracked-files=all'],root):raise ValueError('Xenon source changed')
  if output(['git','rev-parse','HEAD'],omes_source)!=OMES or output(['git','status','--porcelain=v1','--untracked-files=all'],omes_source):raise ValueError('Omes source changed')
  validate(root)
  if corpus(root)!=report['corpus_sha256'] or sha(omes_binary)!=report['binary_sha256'] or tree(prepared)!=baseline:raise ValueError('workload artifacts changed')
  report['workload_completed']=True
 except Exception as error:report['error']=type(error).__name__+': '+str(error)
 finally:(evidence/'result.json').write_text(json.dumps(report,indent=2)+'\n')
 return report

def interrupted(signum,frame):raise RuntimeError('workload runner interrupted')
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--stage',choices=['simple','mixed','fuzz'],required=True);p.add_argument('--profile',choices=['without-faults','with-declared-faults'],required=True);p.add_argument('--evidence-dir',type=Path,required=True);p.add_argument('--omes-binary',type=Path,required=True);p.add_argument('--omes-source',type=Path,required=True);a=p.parse_args()
 for sig in [signal.SIGINT,signal.SIGTERM]:signal.signal(sig,interrupted)
 r=run_workload(a.stage,a.profile,a.evidence_dir.resolve(),a.omes_binary.resolve(),a.omes_source.resolve())
 print(json.dumps({'workload_completed':r['workload_completed'],'full_acceptance':False,'report':str(a.evidence_dir/'result.json')}))
 raise SystemExit(0 if r['workload_completed'] else 1)
