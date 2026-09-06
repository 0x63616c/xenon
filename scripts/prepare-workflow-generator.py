#!/usr/bin/env python3
"""Build fresh seeded input tools and the explicit compatible Go worker; no services."""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import tarfile

from omes_workloads import ROOT, OMES, sha, command, output


def prepare(source, destination):
    destination.mkdir(parents=True, exist_ok=False)
    report = {'schema': 1, 'prepared': False, 'runtime_executed': False, 'commands': []}
    env = dict(os.environ, GOENV='off', GOWORK='off', GOFLAGS='-mod=readonly',
               GOTOOLCHAIN='go1.27.1', GOMAXPROCS='2', CARGO_BUILD_JOBS='2')
    def run(argv, cwd=ROOT, timeout=1200):
        result = command(list(map(str, argv)), destination/f'command-{len(report["commands"])}.log', timeout, cwd, env)
        report['commands'].append(result)
        if result['exit_code'] or result['timed_out'] or result.get('interrupted'):
            raise RuntimeError('generator preparation failed; see command log')
    try:
        if output(['git','rev-parse','HEAD'],source)!=OMES or output(['git','status','--porcelain'],source):
            raise ValueError('clean pinned Omes source required')
        report['source_commit']=output(['git','rev-parse','HEAD'],ROOT)
        if output(['git','status','--porcelain'],ROOT):raise ValueError('clean Xenon source required')
        run(['python3','scripts/prepare-corrected-omes.py','--source',source,'--output',destination/'worker'])
        worker=json.loads((destination/'worker/build.json').read_text())
        if not worker['prepared']:raise ValueError('compatible worker preparation failed')
        corpus=json.loads((ROOT/'proof/omes-corpus/manifest.json').read_text())
        contract=json.loads((ROOT/'test/scenarios/omes-signals/contract.json').read_text())
        generator_source=destination/'generator-source';generator_source.mkdir()
        archive=destination/'upstream.tar'
        import subprocess
        with archive.open('xb') as stream:
            subprocess.run(['git','archive',OMES],cwd=source,stdout=stream,check=True,timeout=30)
        with tarfile.open(archive) as stream:stream.extractall(generator_source,filter='data')
        shutil.copytree(destination/'worker/api',generator_source/'workers/proto/api_upstream',ignore=shutil.ignore_patterns('.git'),dirs_exist_ok=True)
        env['PROTOC']=str(ROOT/'.local/protoc/bin/protoc')
        env['PATH']=str(destination/'worker/tools')+os.pathsep+env['PATH']
        env['CARGO_TARGET_DIR']=str(destination/'target')
        run(['rustc','+1.94.0','--version'],timeout=30)
        run(['cargo','+1.94.0','build','--locked','--manifest-path',generator_source/'loadgen/kitchen-sink-gen/Cargo.toml'],generator_source)
        shutil.copyfile(destination/'target/debug/kitchen-sink-gen',destination/'generate');(destination/'generate').chmod(0o700)
        normalizer_source=destination/'normalizer-source'
        shutil.copytree(destination/'worker/source',normalizer_source,ignore=shutil.ignore_patterns('prepared','.git'))
        original=(ROOT/'test/scenarios/omes-signals/normalize.go.in').read_text()
        if sha(ROOT/'test/scenarios/omes-signals/normalize.go.in')!=contract['normalizer_sha256']:raise ValueError('normalizer source changed')
        (normalizer_source/'normalize_xenon.go').write_text(original.replace('func main() {','func normalizeMain() {').replace('fmt.Printf("normalized','fmt.Fprintf(os.Stderr, "normalized'))
        shutil.copyfile(ROOT/'internal/simulation/workflow_normalizer.go.txt',normalizer_source/'inspect_xenon.go')
        run(['go','build','-buildvcs=false','-o',destination/'normalize','normalize_xenon.go','inspect_xenon.go'],normalizer_source)
        shutil.copyfile(ROOT/'proof/workflow-generator/config.json',destination/'config.json')
        artifacts={'generator':'generate','normalize':'normalize','config':'config.json','worker':'worker/source/workers/go/prepared/program'}
        bundle=dict(version=1,omes_commit=OMES,api_commit=corpus['api_commit'],
                    generator_source_sha256=sha(generator_source/'loadgen/kitchen-sink-gen/src/main.rs'),
                    cargo_lock_sha256=sha(generator_source/'loadgen/kitchen-sink-gen/Cargo.lock'),
                    normalizer_source_sha256=contract['normalizer_sha256'],
                    inspector_source_sha256=sha(ROOT/'internal/simulation/workflow_normalizer.go.txt'),
                    compatibility_overlay_sha256=contract['overlay_sha256'],worker_sdk='v1.48.0',
                    worker_sha256=sha(destination/artifacts['worker']),
                    tools={name:dict(path=path,sha256=sha(destination/path)) for name,path in artifacts.items()})
        if bundle['generator_source_sha256']!=corpus['generator_source_sha256'] or bundle['cargo_lock_sha256']!=corpus['generator_cargo_lock_sha256']:
            raise ValueError('upstream generator source pin mismatch')
        if output(['git','rev-parse','HEAD'],ROOT)!=report['source_commit'] or output(['git','status','--porcelain'],ROOT):raise ValueError('source changed')
        (destination/'generator.json').write_text(json.dumps(bundle,indent=2)+'\n')
        report.update(prepared=True,generator_manifest_sha256=sha(destination/'generator.json'),worker_manifest_sha256=sha(destination/'worker/build.json'))
    except Exception as error:report['error']=str(error)
    finally:(destination/'build.json').write_text(json.dumps(report,indent=2)+'\n')
    return report


if __name__=='__main__':
    def interrupted(signum,frame):raise RuntimeError('generator preparation interrupted')
    for signum in (signal.SIGINT,signal.SIGTERM):signal.signal(signum,interrupted)
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    result=prepare(args.source.resolve(),args.output.resolve())
    print(json.dumps(result,indent=2))
    raise SystemExit(0 if result['prepared'] else 1)
