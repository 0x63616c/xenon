#!/usr/bin/env python3
"""Recreate and verify the pinned official Go+UniFFI feasibility experiment."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time
import uuid
import prove

ROOT = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--allow-dirty', action='store_true')
    args = parser.parse_args()
    manifest = json.loads((ROOT / 'experiments/go-bindings.json').read_text())
    if manifest['schema'] != 1 or manifest['name'] != 'go-bindings':
        raise ValueError('unknown experiment schema')
    env = {key: os.environ[key] for key in ('PATH','HOME','USER','TMPDIR','RUSTUP_HOME','CARGO_HOME') if key in os.environ}
    env.update({'GOENV':'off','GOWORK':'off','GOTOOLCHAIN':manifest['go_toolchain'],'CGO_ENABLED':'1','CARGO_TERM_COLOR':'never','SLATEDB_UNIFFI_RUNTIME_THREADS':manifest['runtime_threads']})
    def git(*args, cwd=ROOT):
        return subprocess.check_output(['git',*args],cwd=cwd,env=env,text=True).strip()
    sha, dirty = git('rev-parse','HEAD'), git('status','--porcelain=v1','--untracked-files=all')
    evidence = ROOT / '.local/evidence' / (time.strftime('%Y%m%dT%H%M%SZ',time.gmtime())+'-go-bindings-'+uuid.uuid4().hex[:8])
    evidence.mkdir(parents=True)
    report = {'schema':1,'experiment':'go-bindings','commit':sha,'dirty_status':dirty,'result':'failed','proof_pass':False,'reproducible_clean_checkout':not bool(dirty),'commands':[], 'config_sha256':{},'tool_versions':{},'platform':platform.platform(),'limitations':manifest['limitations']}
    def run(argv, timeout, cwd=ROOT):
        number=len(report['commands'])+1
        print(f'[{number}] '+ ' '.join(argv),flush=True)
        code,output,expired=prove.run_process(argv,timeout,env,cwd)
        name=f'command-{number}.log';(evidence/name).write_text(output)
        report['commands'].append({'argv':argv,'cwd':str(cwd.relative_to(ROOT)),'exit_code':code,'timed_out':expired,'output':name,'output_sha256':prove.digest(evidence/name)})
        if code or expired:raise ValueError(f'command {number} failed; see {name}')
        return output
    try:
        if dirty and not args.allow_dirty:raise ValueError('clean checkout required; --allow-dirty never produces proof_pass')
        ambient=prove.cargo_configs(ROOT,env)
        if ambient:raise ValueError('ambient Cargo configuration detected: '+str(ambient))
        for name in ['experiments/go-bindings.json',*manifest['inputs']]:
            path=(ROOT/name).resolve()
            if not path.is_relative_to(ROOT) or not path.is_file():raise ValueError('invalid declared input')
            report['config_sha256'][name]=prove.digest(path)
        (evidence/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
        source=ROOT/'.local/slatedb-go-source'
        if not source.exists():
            run(['git','clone','--no-checkout',manifest['source_url'],str(source)],120)
            run(['git','checkout','--detach',manifest['source_commit']],30,source)
        if git('rev-parse','HEAD',cwd=source)!=manifest['source_commit'] or git('status','--porcelain=v1','--untracked-files=all',cwd=source):
            raise ValueError('cached SlateDB source is wrong revision or dirty')
        report['slatedb_commit']=manifest['source_commit']
        report['slatedb_cargo_lock_sha256']=prove.digest(source/'Cargo.lock')
        for tool,argv in [('rustc',['rustc','+'+manifest['rust_toolchain'],'-vV']),('go',['go','version']),('cc',['cc','--version']),('git',['git','--version']),('python',[sys.executable,'--version'])]:
            report['tool_versions'][tool]=run(argv,60).strip()
        target=ROOT/'.local/go-bindings-target';env['CARGO_TARGET_DIR']=str(target)
        run(['cargo','+'+manifest['rust_toolchain'],'build','--locked','-p','slatedb-uniffi'],manifest['build_timeout_seconds'],source)
        suffix={'Darwin':'.dylib','Linux':'.so'}.get(platform.system())
        if suffix is None:raise ValueError('unsupported platform')
        library=target/'debug'/('libslatedb_uniffi'+suffix)
        report['shared_library_sha256']=prove.digest(library)
        scratch=evidence/'module';scratch.mkdir()
        module='module xenon.local/go-bindings-probe\n\ngo 1.27.1\n\nrequire slatedb.io/slatedb-go v0.0.0\nreplace slatedb.io/slatedb-go => '+json.dumps(str(source/'bindings/go'))+'\n'
        (scratch/'go.mod').write_text(module)
        for name in ('probe_test.go','case.json'):shutil.copyfile(ROOT/'experiments/go-bindings'/name,scratch/name)
        report['materialized_module_sha256']={name:prove.digest(scratch/name) for name in ('go.mod','probe_test.go','case.json')}
        env.update({'CGO_LDFLAGS':'-L'+str(target/'debug'),'LD_LIBRARY_PATH':str(target/'debug'),'DYLD_LIBRARY_PATH':str(target/'debug'),'GOFLAGS':'-mod=readonly'})
        report['ffi_environment']={key:env[key] for key in ('CGO_ENABLED','CGO_LDFLAGS','LD_LIBRARY_PATH','DYLD_LIBRARY_PATH','SLATEDB_UNIFFI_RUNTIME_THREADS','GOTOOLCHAIN')}
        output=run(['go','test','-json','-count=1','-timeout',str(manifest['test_timeout_seconds'])+'s','./...'],manifest['test_timeout_seconds']+30,scratch)
        events=[json.loads(line) for line in output.splitlines() if line.strip()]
        actual=[event['Test'] for event in events if event.get('Action')=='pass' and 'Test' in event]
        if any(event.get('Action') in ('fail','skip') for event in events) or sorted(actual)!=sorted(manifest['expected_tests']):raise ValueError('test names/count/status did not match declared assertions')
        package_pass=[event for event in events if event.get('Action')=='pass' and 'Test' not in event]
        if len(package_pass)!=1 or package_pass[0].get('Package')!='xenon.local/go-bindings-probe':raise ValueError('missing package pass')
        report['verified_tests']=actual
        if sorted(p.name for p in scratch.iterdir())!=sorted(report['materialized_module_sha256']):raise ValueError('unexpected scratch module input')
        for name,digest in report['materialized_module_sha256'].items():
            if prove.digest(scratch/name)!=digest:raise ValueError('materialized test input changed during run')
        for name,digest in report['config_sha256'].items():
            if prove.digest(ROOT/name)!=digest:raise ValueError('input changed during run: '+name)
        if git('rev-parse','HEAD')!=sha or git('status','--porcelain=v1','--untracked-files=all')!=dirty:raise ValueError('checkout changed during run')
        if git('rev-parse','HEAD',cwd=source)!=manifest['source_commit'] or git('status','--porcelain=v1','--untracked-files=all',cwd=source):raise ValueError('SlateDB source changed during run')
        if prove.digest(library)!=report['shared_library_sha256']:raise ValueError('native library changed during run')
        report['result'],report['proof_pass']=prove.classify_success(bool(dirty))
    except Exception as error:
        report['error']=str(error)
    finally:
        (evidence/'result.json').write_text(json.dumps(report,indent=2)+'\n')
        print(report['result'].upper()+': '+str(evidence/'result.json'),flush=True)
    return 0 if report['result'] in ('passed','development-passed') else 1

if __name__=='__main__':sys.exit(main())
