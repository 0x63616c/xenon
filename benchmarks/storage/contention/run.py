#!/usr/bin/env python3
"""Create one disposable, scoped MinIO project; record evidence; always tear it down."""
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
IMAGE = 'minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e'

def command(args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, **kwargs)

def capture(args):
    return command(args, stdout=subprocess.PIPE, text=True).stdout.strip()

def main():
    if len(sys.argv) != 2:
        raise SystemExit('usage: benchmarks/storage/contention/run.py NEW-EVIDENCE-DIR')
    out = Path(sys.argv[1]).resolve()
    out.mkdir(parents=True, exist_ok=False)
    project = 'xenon-contention-' + str(os.getpid())
    compose = ['docker', 'compose', '-p', project, '-f', str(HERE / 'compose.yaml')]
    inputs = [ROOT / 'go.mod', ROOT / 'go.sum']
    inputs += sorted(HERE.glob('*.go')) + [HERE / 'config.json', HERE / 'compose.yaml', HERE / 'run.py']
    inputs += sorted((ROOT / 'internal/registry').rglob('*.go')) + sorted((ROOT / 'internal/identity').glob('*.go'))
    provenance = {
        'source_revision': capture(['git', 'rev-parse', 'HEAD']),
        'worktree_status': capture(['git', 'status', '--porcelain', '--untracked-files=no']),
        'input_sha256': {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs},
        'go': capture(['go', 'version']), 'host': platform.platform(),
        'docker': capture(['docker', 'version', '--format', '{{.Server.Version}}']),
        'compose': capture(['docker', 'compose', 'version']), 'image_pin': IMAGE,
        'command': 'go build -o EVIDENCE/probe ./benchmarks/storage/contention; EVIDENCE/probe benchmarks/storage/contention/config.json EVIDENCE',
        'utc_started': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'scope': 'real local MinIO registry contention; synthetic owner fields; not production cluster capacity, real AWS S3, or deterministic simulation',
    }
    (out / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
    code = 1
    with (out / 'runner.log').open('w') as log:
        try:
            command(['go', 'build', '-o', str(out / 'probe'), './benchmarks/storage/contention'], stdout=log, stderr=subprocess.STDOUT, timeout=120)
            provenance['binary_sha256'] = hashlib.sha256((out / 'probe').read_bytes()).hexdigest()
            command(compose + ['up', '-d'], stdout=log, stderr=subprocess.STDOUT, timeout=90)
            provenance['image_id'] = capture(['docker', 'image', 'inspect', IMAGE, '--format', '{{.Id}}'])
            provenance['image_architecture'] = capture(['docker', 'image', 'inspect', IMAGE, '--format', '{{.Architecture}}'])
            deadline = time.monotonic() + 30
            while True:
                try:
                    with urllib.request.urlopen('http://127.0.0.1:19316/minio/health/ready', timeout=1) as response:
                        if response.status == 200:
                            break
                except OSError:
                    pass
                if time.monotonic() >= deadline:
                    raise RuntimeError('MinIO readiness deadline')
                time.sleep(0.2)
            command([str(out / 'probe'), str(HERE / 'config.json'), str(out)], stdout=log, stderr=subprocess.STDOUT, timeout=570)
            code = 0
        except Exception as exc:
            (out / 'failure.txt').write_text(repr(exc) + '\n')
        finally:
            logs = subprocess.run(compose + ['logs', '--no-color'], cwd=ROOT, capture_output=True, text=True, timeout=15)
            (out / 'minio.log').write_text(logs.stdout + logs.stderr)
            cleanup = subprocess.run(compose + ['down', '-v', '--remove-orphans'], cwd=ROOT, capture_output=True, text=True, timeout=30)
            (out / 'cleanup.log').write_text(cleanup.stdout + cleanup.stderr)
            provenance['cleanup_exit_code'] = cleanup.returncode
            if cleanup.returncode:
                code = 1
            provenance['utc_finished'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            (out / 'provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
            (out / 'status').write_text('passed\n' if code == 0 else 'failed\n')
            (out / 'exit-code').write_text(str(code) + '\n')
            outputs = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(out.iterdir()) if p.is_file() and p.name != 'probe'}
            (out / 'output-hashes.json').write_text(json.dumps(outputs, indent=2) + '\n')
    return code

if __name__ == '__main__':
    raise SystemExit(main())
