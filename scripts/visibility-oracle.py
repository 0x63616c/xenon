#!/usr/bin/env python3
"""Recheck committed Text cases against ephemeral pinned PostgreSQL and Go."""
import base64, hashlib, json, os, pathlib, subprocess, sys, time, uuid
ROOT=pathlib.Path(__file__).resolve().parents[1]
def run(args,**kw):
 return subprocess.run(args,cwd=ROOT,text=True,check=True,capture_output=True,timeout=180,**kw).stdout.strip()
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def literal(s):return "convert_from(decode('"+base64.b64encode(s.encode()).decode()+"','base64'),'UTF8')"
def main():
 pins=json.loads((ROOT/'proof/visibility/oracle-pins.json').read_text());cases=json.loads((ROOT/'proof/visibility/oracle-cases.json').read_text())
 clean=run(['git','status','--porcelain=v1','--untracked-files=all'])==''
 if not clean:raise SystemExit('dirty checkout cannot produce oracle PASS')
 head=run(['git','rev-parse','HEAD']);inputs={p:digest(ROOT/p) for p in pins['inputs']}
 if run(['docker','version','--format','{{.Server.Version}}'])!=pins['docker_server']:raise SystemExit('Docker server pin mismatch')
 if run(['go','version'])!=pins['go']:raise SystemExit('Go pin mismatch')
 if sys.version.split()[0]!=pins['python']:raise SystemExit('Python pin mismatch')
 name='xenon-text-oracle-'+uuid.uuid4().hex[:12];result={'source_commit':head,'inputs':inputs,'pins':pins,'case_count':len(cases),'proof_pass':False}
 output=ROOT/'.local/evidence'/name;output.mkdir(parents=True)
 try:
  run(['docker','run','-d','--name',name,'--label','xenon.proof=visibility-text','--platform',pins['platform'],'--network','none','--tmpfs','/var/lib/postgresql/data','-e','POSTGRES_HOST_AUTH_METHOD=trust','-e','POSTGRES_INITDB_ARGS=--locale=C --encoding=UTF8',pins['image']])
  deadline=time.monotonic()+60
  while True:
   ready=subprocess.run(['docker','exec',name,'pg_isready','-U','postgres'],capture_output=True,timeout=10)
   if ready.returncode==0:break
   if time.monotonic()>deadline:raise RuntimeError('oracle startup deadline')
   time.sleep(.25)
  version=run(['docker','exec',name,'psql','-U','postgres','-X','-qAt','-c',"SELECT current_setting('server_version'),datcollate,current_setting('server_encoding') FROM pg_database WHERE datname=current_database()"])
  if version!='16.15 (Debian 16.15-1.pgdg13+2)|C|UTF8':raise RuntimeError('PostgreSQL version/collation pin mismatch: '+version)
  result['postgres']=version;result['image_id']=run(['docker','inspect',name,'--format','{{.Image}}'])
  sql="CREATE OR REPLACE FUNCTION pg_temp.probe(v text,q text) RETURNS jsonb LANGUAGE plpgsql AS $$ DECLARE vv tsvector; qq tsquery; BEGIN vv:=v::tsvector; qq:=q::tsquery; RETURN jsonb_build_object('Match',vv@@qq,'Error',false); EXCEPTION WHEN OTHERS THEN RETURN jsonb_build_object('Match',false,'Error',true); END $$;\n"
  for c in cases:
   query=' | '.join(x for x in c['Query'].split(' ') if x)
   sql+='SELECT pg_temp.probe('+literal(c['Vector'])+','+literal(query)+')::text;\n'
  result['sql_sha256']=hashlib.sha256(sql.encode()).hexdigest()
  observed=run(['docker','exec','-i',name,'psql','-U','postgres','-X','-qAt','-v','ON_ERROR_STOP=1'],input=sql)
  (output/'postgres.jsonl').write_text(observed+'\n');rows=[json.loads(line) for line in observed.splitlines()]
  if len(rows)!=len(cases):raise RuntimeError('oracle result count mismatch')
  for index,(case,row) in enumerate(zip(cases,rows)):
   if row!={'Match':case['Match'],'Error':case['Error']}:raise RuntimeError('oracle expectation mismatch at '+str(index))
  go=run(['go','test','-json','-count=1','./internal/visibility','-run','^TestTextPostgreSQLOracle$']);(output/'go.jsonl').write_text(go+'\n')
  events=[json.loads(line) for line in go.splitlines()];passed=[e.get('Test') for e in events if e.get('Action')=='pass' and 'Test' in e]
  if passed!=['TestTextPostgreSQLOracle'] or any(e.get('Action') in ('skip','fail') for e in events):raise RuntimeError('Go assertions missing')
  if run(['git','rev-parse','HEAD'])!=head or run(['git','status','--porcelain=v1','--untracked-files=all']) or any(digest(ROOT/p)!=h for p,h in inputs.items()):raise RuntimeError('source changed during proof')
  result['proof_pass']=True
 except Exception as e:result['error']=str(e)
 finally:
  cleanup=subprocess.run(['docker','rm','-f',name],capture_output=True,text=True,timeout=30)
  result['cleanup_ok']=cleanup.returncode==0
  if not result['cleanup_ok']:result['proof_pass']=False
  (output/'result.json').write_text(json.dumps(result,indent=2)+'\n')
 print(('PASSED' if result['proof_pass'] else 'FAILED')+': '+str(output/'result.json'))
 return 0 if result['proof_pass'] else 1
if __name__=='__main__':sys.exit(main())
