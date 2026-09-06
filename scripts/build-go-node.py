#!/usr/bin/env python3
"""Build the Go owner against the exact official SlateDB binding/native source."""
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import prove

ROOT=Path(__file__).resolve().parents[1]
pins=json.loads((ROOT/'tools/slatedb-native.json').read_text())
source=ROOT/'.local/slatedb-native-source'
target=ROOT/'.local/slatedb-native-target'
env=os.environ.copy()
env.update({'CARGO_TARGET_DIR':str(target),'GOTOOLCHAIN':pins['go_toolchain'],'GOENV':'off','GOWORK':'off','GOFLAGS':'-mod=readonly','CGO_ENABLED':'1','CGO_LDFLAGS':'-L'+str(target/'debug'),'LD_LIBRARY_PATH':str(target/'debug'),'DYLD_LIBRARY_PATH':str(target/'debug'),'SLATEDB_UNIFFI_RUNTIME_THREADS':pins['runtime_threads']})
def run(argv,cwd=ROOT,capture=False):
    result=subprocess.run(argv,cwd=cwd,env=env,check=True,text=True,stdout=subprocess.PIPE if capture else None)
    return result.stdout.strip() if capture else None
if not source.exists():
    source.parent.mkdir(exist_ok=True)
    run(['git','clone','--no-checkout',pins['source_url'],str(source)])
    run(['git','checkout','--detach',pins['source_commit']],source)
def verify_source():
    if run(['git','rev-parse','HEAD'],source,True)!=pins['source_commit'] or run(['git','status','--porcelain=v1','--untracked-files=all'],source,True):raise SystemExit('native source is wrong revision or dirty')
    if prove.cargo_configs(source,env):raise SystemExit('ambient Cargo config at native build directory')
verify_source()
module=json.loads(run(['go','mod','download','-json',pins['go_module']+'@'+pins['go_version']],capture=True))
for name in ['uniffi/slatedb.go','uniffi/slatedb.h','uniffi/cgo_flags.go']:
    if prove.digest(Path(module['Dir'])/name)!=prove.digest(source/'bindings/go'/name):raise SystemExit('published binding does not match pinned native source')
run(['cargo','+'+pins['rust_toolchain'],'build','--locked','-p','slatedb-uniffi'],source)
suffix={"Darwin":".dylib","Linux":".so"}.get(platform.system())
if suffix is None:raise SystemExit('unsupported shared library platform')
library=target/'debug'/('libslatedb_uniffi'+suffix)
native_sha=prove.digest(library)
output=ROOT/'.local/bin/xenon-go-node';output.parent.mkdir(exist_ok=True)
run(['go','build','-o',str(output),'./cmd/xenon-go-node'])
agent=ROOT/'.local/bin/xenon'
ldflags=' '.join(['-X','github.com/0x63616c/xenon/internal/buildinfo.NativeCommit='+pins['source_commit'],'-X','github.com/0x63616c/xenon/internal/buildinfo.NativeSHA256='+native_sha])
run(['go','build','-ldflags',ldflags,'-o',str(agent),'./cmd/xenon'])
verify_source()
report={'source_commit':pins['source_commit'],'source_clean':True,'source_cargo_lock_sha256':prove.digest(source/'Cargo.lock'),'binding_module':pins['go_module'],'binding_version':module['Version'],'binding_sum':module['Sum'],'shared_library':str(library.relative_to(ROOT)),'shared_library_sha256':native_sha,'node_binary':str(output.relative_to(ROOT)),'node_binary_sha256':prove.digest(output),'agent_binary':str(agent.relative_to(ROOT)),'agent_binary_sha256':prove.digest(agent),'rustc':run(['rustc','+'+pins['rust_toolchain'],'-vV'],capture=True),'go':run(['go','version'],capture=True),'cc':run(['cc','--version'],capture=True)}
(ROOT/'.local/go-node-build.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
