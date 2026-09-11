#!/usr/bin/env python3
"""Restore an explicitly preserved fixture and fetch rejected workflow histories."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

p = argparse.ArgumentParser(description=__doc__)
for name in ('cli', 'oracle', 'state', 'fixture', 'evidence', 'native-library'):
    p.add_argument('--' + name, type=Path, required=True)
p.add_argument('--omes-run-id', required=True)
a = p.parse_args()
state = json.loads((a.state / 'state.json').read_text())
if not state.get('initialized') or not state.get('initialization'):
    p.error('previously initialized preserved fixture required')
a.evidence.mkdir(parents=True, exist_ok=False)
env = dict(os.environ, DYLD_LIBRARY_PATH=str(a.native_library.parent), LD_LIBRARY_PATH=str(a.native_library.parent))
report = dict(schema=1, qualification='restored-fixture-diagnostic', source_revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
              limitations=['Restoring servers can advance workflows and write authority/metadata; only oracle history fetch is read-only. No unchanged-state claim.'], fixture_run=state['run'], omes_run_id=a.omes_run_id, cli_sha256=hashlib.sha256(a.cli.read_bytes()).hexdigest(),
              oracle_sha256=hashlib.sha256(a.oracle.read_bytes()).hexdigest(), commands=[], cleanup_verified=False)

def run(argv, timeout, expected=0):
    i = len(report['commands'])
    report['commands'].append(dict(argv=argv, timeout_seconds=timeout))
    with (a.evidence / f'{i}.out').open('xb') as out, (a.evidence / f'{i}.err').open('xb') as err:
        result = subprocess.run(argv, env=env, stdout=out, stderr=err, timeout=timeout)
    report['commands'][-1]['exit_code'] = result.returncode
    if result.returncode != expected:
        raise RuntimeError(f'command {i} exit {result.returncode}, wanted {expected}')
    return (a.evidence / f'{i}.out').read_text()

try:
    run([str(a.cli), 'dev', 'up', '--state', str(a.state), '--fixture', str(a.fixture), '--timeout', '3m'], 190)
    fixture = json.loads(a.fixture.read_text())
    run([str(a.oracle), '--address', '127.0.0.1:' + str(fixture['temporal_port']), '--namespace', 'xenon-ministack', '--omes-run-id', a.omes_run_id, '--minimum-runs', '1', '--output', str(a.evidence / 'histories')], 90, expected=1)
    failure = json.loads((a.evidence / 'histories/failure.json').read_text())
    history = a.evidence / 'histories' / failure['run']['history_file']
    if hashlib.sha256(history.read_bytes()).hexdigest() != failure['run']['history_sha256']:
        raise RuntimeError('rejected history hash mismatch')
    report['failure'] = failure
    report['result'] = 'rejected-history-captured'
except BaseException as error:
    report.update(result='failed', error=str(error))
finally:
    try:
        down = json.loads(run([str(a.cli), 'dev', 'down', '--state', str(a.state), '--timeout', '90s'], 100))
        remaining = run(['docker', 'ps', '-aq', '--filter', 'label=io.xenon.dev.run=' + state['run']], 10).strip()
        report['cleanup_verified'] = down['cleanup_verified'] and not down.get('pending') and not remaining
        if not report['cleanup_verified']:
            raise RuntimeError('owned fixture cleanup incomplete')
    except Exception as error:
        report['cleanup_error'] = str(error)
    report['files'] = {str(f.relative_to(a.evidence)): hashlib.sha256(f.read_bytes()).hexdigest() for f in a.evidence.rglob('*') if f.is_file()}
    (a.evidence / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    print(a.evidence / 'result.json')
sys.exit(0 if report['result'] == 'rejected-history-captured' and report['cleanup_verified'] else 1)
