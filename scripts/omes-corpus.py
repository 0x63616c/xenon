#!/usr/bin/env python3
"""Build pinned upstream generator or verify immutable saved replay bytes."""
import argparse
import hashlib
import json
import os
import platform
import signal
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / 'proof/omes-corpus'
OMES = 'c6978ba39aa03551ce28974117e8d7ecf983d2b3'
API = 'd96bd55e87799e9f6a33a1c40a56cfa932566bdf'

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def require(condition, message):
    if not condition:
        raise ValueError(message)

def run(args, **kwargs):
    timeout = kwargs.pop('timeout', 600)
    data = kwargs.pop('input', None)
    if data is not None:
        kwargs['stdin'] = subprocess.PIPE
    if kwargs.pop('capture_output', False):
        kwargs.update(stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    with subprocess.Popen(args, start_new_session=True, **kwargs) as process:
        try:
            stdout, stderr = process.communicate(data, timeout=timeout)
        except BaseException:
            # Kill compiler/git descendants before disposable source cleanup.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)
            raise
        if process.returncode:
            raise subprocess.CalledProcessError(process.returncode, args, stdout, stderr)
        return subprocess.CompletedProcess(args, process.returncode, stdout, stderr)

def output(args, **kwargs):
    return subprocess.check_output(args, timeout=30, **kwargs)

def replay_command(item):
    return ["omes", "run-scenario-with-worker", "--scenario", "fuzzer", "--language", "go", "--version", "v1.48.0", "--dir-name", "prepared", "--namespace", "xenon-ministack", "--server-address", "127.0.0.1:17233", "--run-id", "xenon-ministack-fuzz", "--iterations", "1", "--max-concurrent", "1", "--max-iteration-attempts", "1", "--timeout", "900s", "--option", "nexus-endpoint=xenon-fuzz", "--option", "input-file=proof/omes-corpus/" + item["file"]]

def verify():
    manifest = json.loads((CORPUS / 'manifest.json').read_text())
    require(manifest['omes_commit'] == OMES, 'Omes pin mismatch')
    require(manifest['api_commit'] == API, 'API pin mismatch')
    require(sha(CORPUS / 'generator-config.json') == manifest['config_sha256'], 'Configuration hash mismatch')
    require(len(manifest['inputs']) == 20, 'Expected twenty inputs')
    require(len({x['file'] for x in manifest['inputs']}) == 20, 'Duplicate input')
    for item in manifest['inputs']:
        path = CORPUS / item['file']
        require(path.resolve().parent == (CORPUS / 'inputs').resolve(), 'Input path escapes corpus')
        require(path.stat().st_size == item['bytes'] > 0, 'Input size mismatch')
        require(sha(path) == item['sha256'], 'Input hash mismatch')
    replay = json.loads((CORPUS / 'replay.json').read_text())
    expected = {'schema_version': 1, 'omes_commit': OMES, 'namespace': 'xenon-ministack', 'server_address': '127.0.0.1:17233', 'required_endpoint': {'name': 'xenon-fuzz', 'target_namespace': 'xenon-ministack', 'target_task_queue': 'omes-xenon-ministack-fuzz'}, 'profiles': ['without-faults', 'with-declared-faults'], 'runtime_execution_status': 'NOT_EXECUTED', 'commands': [replay_command(item) for item in manifest['inputs']], 'inputs': [{'file': item['file'], 'sha256': item['sha256']} for item in manifest['inputs']]}
    require(replay == expected, 'Replay profile does not match immutable corpus and command contract')
    dirty = output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).strip()
    report = {'schema_version': 1, 'source_commit': output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(), 'dirty': bool(dirty), 'corpus_manifest_sha256': sha(CORPUS / 'manifest.json'), 'replay_sha256': sha(CORPUS / 'replay.json'), 'verifier_sha256': sha(Path(__file__)), 'verified_inputs': 20, 'runtime_replay_executed': False, 'verdict': 'DEVELOPMENT' if dirty else 'PASS'}
    print(json.dumps(report, indent=2))
    if dirty:
        raise SystemExit('Dirty checkout cannot produce proof PASS')

def generate():
    # New corpus generation is explicit; normal proof never overwrites reviewed bytes.
    if (CORPUS / 'manifest.json').exists():
        raise SystemExit('Existing corpus is immutable; use a new reviewed corpus directory')
    versions = {}
    for name, command, expected in [('rustc', ['rustc', '--version'], 'rustc 1.94.0 (4a4ef493e 2026-03-02)'), ('protoc', ['protoc', '--version'], 'libprotoc 36.0'), ('go', ['go', 'env', 'GOVERSION'], 'go1.27.1')]:
        versions[name] = output(command, text=True).strip()
        require(versions[name] == expected, f'{name} version mismatch: {versions[name]}')
    versions['platform'] = {'system': platform.system(), 'machine': platform.machine(), 'goos': output(['go', 'env', 'GOOS'], text=True).strip(), 'goarch': output(['go', 'env', 'GOARCH'], text=True).strip()}
    with tempfile.TemporaryDirectory(prefix='xenon-omes-corpus-') as scratch:
        scratch = Path(scratch)
        source = scratch / 'source'
        run(['git', 'clone', 'https://github.com/temporalio/omes', str(source)])
        run(['git', 'checkout', '--detach', OMES], cwd=source)
        run(['git', 'submodule', 'update', '--init', 'workers/proto/api_upstream'], cwd=source)
        require(output(['git', 'rev-parse', 'HEAD'], cwd=source / 'workers/proto/api_upstream', text=True).strip() == API, 'API checkout mismatch')
        require(not output(['git', 'status', '--porcelain'], cwd=source).strip(), 'Dirty generator source')
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
        require(bool(decoded), 'Empty decoded input')
        items.append({'seed': seed, 'file': str(path.relative_to(CORPUS)), 'bytes': len(result.stdout), 'sha256': sha(path), 'decoded_sha256': hashlib.sha256(decoded).hexdigest()})
    manifest = {'schema_version': 1, 'omes_commit': OMES, 'api_commit': API, 'generator_cargo_lock_sha256': sha(source / 'loadgen/kitchen-sink-gen/Cargo.lock'), 'generator_source_sha256': sha(source / 'loadgen/kitchen-sink-gen/src/main.rs'), 'config_sha256': sha(CORPUS / 'generator-config.json'), 'tools': versions, 'protoc_gen_go': 'v1.31.0', 'nexus_endpoint': 'xenon-fuzz', 'inputs': items}
    (CORPUS / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['generate', 'verify'])
    args = parser.parse_args()
    generate() if args.mode == 'generate' else verify()
