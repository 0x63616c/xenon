#!/usr/bin/env python3
"""Explicit clean-source image setup for the local CLI fixture; never run by help/up."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--evidence', required=True, type=Path)
args = parser.parse_args()

def git(*arguments):
    return subprocess.check_output(['git', *arguments], cwd=ROOT, text=True).strip()

if git('status', '--porcelain=v1', '--untracked-files=all'):
    raise SystemExit('clean committed checkout required')
revision = git('rev-parse', 'HEAD')
evidence = args.evidence.resolve()
evidence.mkdir(parents=True, exist_ok=False)
image_file = evidence / 'image-id.txt'
command = ['docker', 'build', '--progress=plain', '--target', 'dev-runtime',
           '--build-arg', 'XENON_SOURCE_REVISION=' + revision,
           '--iidfile', str(image_file), '-f', 'Dockerfile.unified-agent', '.']
receipt = {'schema': 1, 'source_revision': revision, 'source_dirty': False,
           'scope': 'local shared-network-namespace image setup, not CLI-02 proof',
           'inputs': {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                      for name in ('Dockerfile.unified-agent', '.dockerignore',
                                   'tools/slatedb-native.json', 'test/scenarios/ministack/pins.json')},
           'command': command, 'status': 'building'}
receipt_path = evidence / 'build.json'
receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')
try:
    with (evidence / 'build.log').open('xb') as log:
        subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                       check=True, timeout=3600)
    image = image_file.read_text().strip()
    if len(image) != 71 or not image.startswith('sha256:'):
        raise RuntimeError('invalid immutable image result')
    fixture = {'schema': 1, 'image': image, 's3_port': 30006,
               'temporal_port': 30233, 'http_port': 30243, 'storage_port': 30935,
               'diagnostics_ports': [30250, 31250, 32250]}
    (evidence / 'fixture.json').write_text(json.dumps(fixture, indent=2) + '\n')
    receipt.update(status='built', image=image)
except BaseException as error:
    receipt.update(status='failed', error=str(error))
    raise
finally:
    receipt['build_log_sha256'] = hashlib.sha256((evidence / 'build.log').read_bytes()).hexdigest()
    receipt_path.write_text(json.dumps(receipt, indent=2) + '\n')
