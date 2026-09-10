#!/usr/bin/env python3
"""Bounded CLI-02 local journey. Uses only its newly created fixture ownership.

Pass --discovery for an older image; such runs never claim acceptance. The object
volume is preserved by default even on failure. Evidence is never deleted.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check_snapshot(raw, inspection, config):
    envelope = json.loads(raw)
    body = base64.b64decode(envelope['body'], validate=True)
    control = json.loads(body)
    if inspection['status'] != 'observed' or inspection['control'] != control:
        raise ValueError('inspection differs from independently downloaded control')
    if control['cluster'] != config['service_storage']['cluster_id']:
        raise ValueError('cluster identity mismatch')
    expected = {p['id'] for p in config['service_storage']['layout']['partitions']}
    if set(control['partitions']) != expected:
        raise ValueError('partition inventory mismatch')
    owners = set()
    for partition in control['partitions'].values():
        owner = partition['desired']
        if not partition['ready'] or partition['generation'] < 1 or not owner['incarnation']:
            raise ValueError('partition not ready with a generation and incarnation')
        owners.add(owner['node'])
    if len(owners) != 3:
        raise ValueError('expected three distinct ready desired owners')
    return control


def check_cli_identity(info, revision, pins, temporal, native_sha, sums):
    if info.get('revision') != revision or info.get('modified') != 'false':
        raise ValueError('CLI must be built from this clean source revision')
    if info.get('go') != pins['go_toolchain']:
        raise ValueError('CLI Go toolchain mismatch')
    for key, module, version in (('slatedb_go', pins['go_module'], pins['go_version']),
                                 ('temporal', temporal['module'], temporal['version'])):
        actual = info.get(key, {})
        if not sums.get((module, version)) or (actual.get('path'), actual.get('version'), actual.get('sum')) != (module, version, sums.get((module, version))) or actual.get('replacement'):
            raise ValueError('CLI dependency pin mismatch: ' + key)
    native = info.get('slatedb_native', {})
    if native != {'source_commit': pins['source_commit'], 'artifact_sha256': native_sha, 'identity_source': 'build-attestation'}:
        raise ValueError('CLI native attestation differs from pinned source/library')


def reconcile_sentinel(run, obj, name, token, expected, pending):
    found = run(['docker', 'ps', '-aq', '--no-trunc', '--filter', 'name=^/' + name + '$', '--filter', 'label=io.xenon.journey=' + token]).splitlines()
    if len(found) > 1 or expected and found != [expected]:
        raise RuntimeError('sentinel ownership changed or disappeared')
    if found:
        observed = obj(['docker', 'inspect', found[0]])[0]
        if observed['Name'] != '/' + name or observed['Config']['Labels'].get('io.xenon.journey') != token:
            raise RuntimeError('sentinel identity mismatch')
        run(['docker', 'rm', found[0]])
        pending = False
    if pending:
        raise RuntimeError('sentinel create outcome unknown; ownership remains pending')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--cli', required=True, type=Path)
    p.add_argument('--build-receipt', required=True, type=Path)
    p.add_argument('--fixture', required=True, type=Path)
    p.add_argument('--evidence', required=True, type=Path)
    p.add_argument('--discovery', action='store_true')
    p.add_argument('--native-library', type=Path, help='Exact host dynamic library required outside discovery')
    args = p.parse_args()
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    dirty = bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT))
    build = json.loads(args.build_receipt.read_text())
    fixture = json.loads(args.fixture.read_text())
    if build['status'] != 'built' or build['image'] != fixture['image']:
        p.error('fixture must match successful immutable image build receipt')
    if not args.discovery and not args.native_library:
        p.error('--native-library required outside discovery')
    if not args.discovery and (dirty or build['source_dirty'] or build['source_revision'] != revision):
        p.error('clean matching image/source required; --discovery cannot qualify acceptance')
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    state = evidence / 'state'
    cli = str(args.cli.resolve())
    report = dict(schema=1, source_revision=revision, source_dirty=dirty,
                  image_source_revision=build['source_revision'], image=fixture['image'],
                  cli_sha256=sha(args.cli), fixture_sha256=sha(args.fixture),
                  script_sha256=sha(Path(__file__)), discovery=args.discovery,
                  result='failed', acceptance_pass=False, commands=[], assertions=[])
    env = dict(os.environ, AWS_DEFAULT_REGION='us-east-1', AWS_ACCESS_KEY_ID='xenon-local',
               AWS_SECRET_ACCESS_KEY='xenon-local-test-only', AWS_ALLOW_HTTP='true',
               AWS_ENDPOINT=f"http://127.0.0.1:{fixture['s3_port']}")
    if args.native_library:
        library_dir = str(args.native_library.resolve().parent)
        env['DYLD_LIBRARY_PATH'] = library_dir
        env['LD_LIBRARY_PATH'] = library_dir
    sentinel = None
    sentinel_name = 'xenon-journey-sentinel-' + uuid.uuid4().hex
    sentinel_token = uuid.uuid4().hex
    sentinel_pending = False
    (evidence / 'sentinel-plan.json').write_text(json.dumps({'name': sentinel_name, 'label': sentinel_token}) + '\n')
    paused = []

    def run(command, timeout=60, expected=0):
        index = len(report['commands'])
        record = dict(argv=command, timeout_seconds=timeout)
        report['commands'].append(record)
        with (evidence / f'{index:03}.out').open('xb') as out, (evidence / f'{index:03}.err').open('xb') as err:
            result = subprocess.run(command, cwd=ROOT, env=env, stdout=out, stderr=err, timeout=timeout)
        record['exit_code'] = result.returncode
        if result.returncode != expected:
            raise RuntimeError(f'command {index} exit {result.returncode}, wanted {expected}')
        return (evidence / f'{index:03}.out').read_text().strip()

    def obj(command, **kwargs):
        return json.loads(run(command, **kwargs))

    def down():
        result = obj([cli, 'dev', 'down', '--state', str(state), '--timeout', '90s'], timeout=100)
        if not result['cleanup_verified'] or result.get('pending'):
            raise RuntimeError('down has unresolved ownership')
        return result

    try:
        version = obj([cli, 'version'])
        report['cli_version'] = version
        if not args.discovery:
            pins = json.loads((ROOT / 'tools/slatedb-native.json').read_text())
            temporal = json.loads((ROOT / 'test/scenarios/ministack/pins.json').read_text())['temporal']
            sums = {(module, ver): checksum for module, ver, checksum in (line.split() for line in (ROOT / 'go.sum').read_text().splitlines())}
            check_cli_identity(version, revision, pins, temporal, sha(args.native_library), sums)
            report['assertions'].append('matching-clean-host-cli-native-and-dependencies')
        for port in [fixture[k] for k in ('s3_port', 'temporal_port', 'http_port', 'storage_port')] + fixture['diagnostics_ports']:
            with socket.socket() as s:
                s.bind(('127.0.0.1', port))
        sentinel_pending = True
        sentinel = run(['docker', 'create', '--name', sentinel_name, '--label', 'io.xenon.journey=' + sentinel_token,
                        '--entrypoint', '/bin/true', fixture['image']])
        sentinel_pending = False
        up = obj([cli, 'dev', 'up', '--fixture', str(args.fixture.resolve()), '--state', str(state), '--timeout', '4m'], timeout=250)
        if up['status'] != 'ready':
            raise RuntimeError('up did not report ready')
        ownership = json.loads((state / 'state.json').read_text())
        resources = {r['role']: r['id'] for r in ownership['resources']}
        agent = resources['agent-1']
        probe = ['docker', 'exec', agent, '/usr/local/bin/xenon-sdk-probe', '--namespace', 'xenon-ministack', '--address', '127.0.0.1:17233']
        workflow_id = 'wf_' + uuid.uuid4().hex[:22]
        def probe_obj(arguments, **kwargs):
            # The existing diagnostic probe prints SDK human logs before its final JSON.
            return json.loads(run(probe + arguments, **kwargs).splitlines()[-1])
        started = probe_obj(['--mode', 'start', '--workflow-id', workflow_id])
        run(probe + ['--mode', 'control', '--workflow-id', workflow_id], timeout=90)
        run(['docker', 'exec', agent, 'mkdir', '-p', '/tmp/journey-history'])
        verified = run(probe + ['--mode', 'verify', '--workflow-id', workflow_id, '--run-id', started['run_id'], '--output', '/tmp/journey-history'], timeout=90)
        if json.loads(verified.splitlines()[-1])['runs'] != 2:
            raise RuntimeError('SDK history verification missing')
        run(['docker', 'cp', agent + ':/tmp/journey-history', str(evidence / 'sdk-history')])
        report['assertions'].append('sdk-result-and-complete-two-run-history')
        probe_obj(['--mode', 'fuzz-endpoint'])
        nexus = probe_obj(['--mode', 'fuzz-endpoint-ready'], timeout=75)
        if nexus['history_shards_verified'] != 4 or len(nexus['runs']) != 4:
            raise RuntimeError('real Nexus operation coverage missing')
        report['assertions'].append('four-real-nexus-echo-operations')
        # Freeze only our three writers to obtain an identical authority version.
        for role in ('agent-1', 'agent-2', 'agent-3'):
            run(['docker', 'pause', resources[role]])
            paused.append(resources[role])
        url = env['AWS_ENDPOINT'] + '/xenon-agent-proof/agent/metadata/registry/cluster/control'
        curl = ['curl', '--fail', '--silent', '--show-error', '--max-time', '5', '--aws-sigv4', 'aws:amz:us-east-1:s3', '--user', 'xenon-local:xenon-local-test-only', url]
        raw = run(curl)
        inspection = obj([cli, 'inspect', '--config', str(state / 'inspect.json'), '--output', 'json'])
        control = check_snapshot(raw, inspection, json.loads((state / 'inspect.json').read_text()))
        if run(curl) != raw:
            raise RuntimeError('authority changed while writers paused')
        (evidence / 'authority.json').write_text(json.dumps(control, indent=2) + '\n')
        report['assertions'].append('inspect-equals-independent-frozen-authority')
        for container in list(paused):
            run(['docker', 'unpause', container])
            paused.remove(container)
        volume = 'xenon-dev-' + ownership['run'] + '-objects'
        volume_before = obj(['docker', 'volume', 'inspect', volume])
        down()
        down()
        if run(['docker', 'ps', '-aq', '--filter', 'label=io.xenon.dev.run=' + ownership['run']]):
            raise RuntimeError('owned containers survived down')
        if obj(['docker', 'volume', 'inspect', volume]) != volume_before:
            raise RuntimeError('object volume was removed or replaced')
        if obj(['docker', 'inspect', sentinel])[0]['Id'] != sentinel:
            raise RuntimeError('unrelated sentinel did not survive')
        unavailable = obj([cli, 'inspect', '--config', str(state / 'inspect.json'), '--output', 'json', '--timeout', '1s'], expected=1)
        if unavailable['status'] != 'unavailable' or unavailable.get('control'):
            raise RuntimeError('unavailable authority fabricated control')
        report['assertions'].extend(['down-idempotent-zero-owned-containers', 'object-volume-preserved', 'sentinel-survived', 'unavailable-inspect-no-control'])
        report['result'] = 'journey-passed'
        # The isolated runtime journey does not certify all CLI-02 instrumented boundaries.
        report['limitations'] = ['component boundary instrumentation and aggregate same-revision build validation remain separate']
    except BaseException as error:
        report['error'] = str(error)
    finally:
        cleanup_errors = []
        for container in paused:
            try:
                run(['docker', 'unpause', container])
            except Exception as error:
                cleanup_errors.append(str(error))
        if (state / 'state.json').exists():
            try:
                down()
            except Exception as error:
                cleanup_errors.append(str(error))
        try:
            reconcile_sentinel(run, obj, sentinel_name, sentinel_token, sentinel, sentinel_pending)
        except Exception as error:
            cleanup_errors.append(str(error))
        report['cleanup_errors'] = cleanup_errors
        if cleanup_errors:
            report['result'] = 'failed'
        report['files'] = {str(p.relative_to(evidence)): sha(p) for p in evidence.rglob('*') if p.is_file()}
        (evidence / 'journey.json').write_text(json.dumps(report, indent=2) + '\n')
        print(str(evidence / 'journey.json'))
    return 0 if report['result'] == 'journey-passed' else 1


if __name__ == '__main__':
    sys.exit(main())
