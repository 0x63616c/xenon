#!/usr/bin/env python3
"""Build pinned upstream generator or verify immutable saved replay bytes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / 'proof/omes-corpus'
OMES = 'c6978ba39aa03551ce28974117e8d7ecf983d2b3'
API = 'd96bd55e87799e9f6a33a1c40a56cfa932566bdf'

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def run(args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)

def verify():
    manifest = json.loads((CORPUS / 'manifest.json').read_text())
    assert manifest['omes_commit'] == OMES
    assert manifest['api_commit'] == API
    assert sha(CORPUS / 'generator-config.json') == manifest['config_sha256']
    assert len(manifest['inputs']) == 20
    assert len({x['file'] for x in manifest['inputs']}) == 20
    for item in manifest['inputs']:
        path = CORPUS / item['file']
        assert path.parent == CORPUS / 'inputs'
        assert path.stat().st_size == item['bytes'] > 0
        assert sha(path) == item['sha256']
    dirty = subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).strip()
    report = {'schema_version': 1, 'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(), 'dirty': bool(dirty), 'corpus_manifest_sha256': sha(CORPUS / 'manifest.json'), 'verified_inputs': 20, 'runtime_replay_executed': False, 'verdict': 'DEVELOPMENT' if dirty else 'PASS'}
    print(json.dumps(report, indent=2))
    if dirty:
        raise SystemExit('Dirty checkout cannot produce proof PASS')

def generate():
    # New corpus generation is explicit; normal proof never overwrites reviewed bytes.
    if (CORPUS / 'manifest.json').exists():
        raise SystemExit('Existing corpus is immutable; use a new reviewed corpus directory')
    versions = {}
    for name, command, expected in [('rustc', ['rustc', '--version'], 'rustc 1.94.0 (4a4ef493e 2026-03-02)'), ('protoc', ['protoc', '--version'], 'libprotoc 36.0'), ('go', ['go', 'version'], 'go version go1.27.1 darwin/arm64')]:
        versions[name] = subprocess.check_output(command, text=True).strip()
        assert versions[name] == expected, (name, versions[name], expected)
    with tempfile.TemporaryDirectory(prefix='xenon-omes-corpus-') as scratch:
        scratch = Path(scratch)
        source = scratch / 'source'
        run(['git', 'clone', 'https://github.com/temporalio/omes', str(source)])
        run(['git', 'checkout', '--detach', OMES], cwd=source)
        run(['git', 'submodule', 'update', '--init', 'workers/proto/api_upstream'], cwd=source)
        assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=source / 'workers/proto/api_upstream', text=True).strip() == API
        assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=source).strip()
        tools = scratch / 'tools'
        env = dict(os.environ, GOBIN=str(tools), CARGO_TARGET_DIR=str(scratch / 'target'))
        run(['go', 'install', 'google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0'], env=env)
        env['PATH'] = str(tools) + os.pathsep + env['PATH']
        run(['cargo', 'build', '--locked', '--manifest-path', str(source / 'loadgen/kitchen-sink-gen/Cargo.toml')], cwd=source, env=env)
        save_inputs(scratch / 'target/debug/kitchen-sink-gen', source, versions)

def save_inputs(binary, source, versions):
    (CORPUS / 'inputs').mkdir(exist_ok=True)
    items = []
    for seed in range(2026090501, 2026090521):
        path = CORPUS / 'inputs' / f'{seed}.proto'
        command = [str(binary), 'generate', '--explicit-seed', str(seed), '--generator-config-override', str(CORPUS / 'generator-config.json'), '--nexus-endpoint', 'xenon-fuzz']
        result = run(command, capture_output=True)
        path.write_bytes(result.stdout)
        # Decode with the exact pinned schema; reject malformed binary output.
        decoded = run(['protoc', '--decode=temporal.omes.kitchen_sink.TestInput', '-I' + str(source / 'workers/proto/api_upstream'), '-I' + str(source / 'workers/proto/kitchen_sink'), str(source / 'workers/proto/kitchen_sink/kitchen_sink.proto')], input=result.stdout, capture_output=True).stdout
        assert decoded
        items.append({'seed': seed, 'file': str(path.relative_to(CORPUS)), 'bytes': len(result.stdout), 'sha256': sha(path), 'decoded_sha256': hashlib.sha256(decoded).hexdigest()})
    manifest = {'schema_version': 1, 'omes_commit': OMES, 'api_commit': API, 'generator_cargo_lock_sha256': sha(source / 'loadgen/kitchen-sink-gen/Cargo.lock'), 'generator_source_sha256': sha(source / 'loadgen/kitchen-sink-gen/src/main.rs'), 'config_sha256': sha(CORPUS / 'generator-config.json'), 'tools': versions, 'protoc_gen_go': 'v1.31.0', 'nexus_endpoint': 'xenon-fuzz', 'inputs': items}
    (CORPUS / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['generate', 'verify'])
    args = parser.parse_args()
    generate() if args.mode == 'generate' else verify()
