#!/usr/bin/env python3
"""Validate the frozen full acceptance input contract, never runtime success."""
import hashlib,json,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
EXPECTED_PROFILE_SHA256="65b548d7f3f920a916dc5c618ec92f88bac993195c50cbfac50d7d5c336097e5"
def validate(root=ROOT):
 path=root/'proof/acceptance/full-profile.json'
 if hashlib.sha256(path.read_bytes()).hexdigest()!=EXPECTED_PROFILE_SHA256:
  raise ValueError('Required acceptance profile changed; prospective reviewed contract update required')
 profile=json.loads(path.read_text())
 fuzz=profile['workloads']['fuzz']
 for key in ('manifest','replay'):
  if hashlib.sha256((root/fuzz[key]).read_bytes()).hexdigest()!=fuzz[key+'_sha256']:
   raise ValueError('Saved fuzz '+key+' changed')
 return profile
if __name__=='__main__':
 validate()
 dirty=bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True,timeout=30).strip())
 if dirty:raise SystemExit('Dirty checkout cannot produce validation PASS')
 print(json.dumps({'profile_validation':'PASS','runtime_status':'NOT_EXECUTED','profile_sha256':EXPECTED_PROFILE_SHA256,'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True,timeout=30).strip()}))
