#!/usr/bin/env python3
"""Clean-source generated CLI component proof; no real servers or full DST claim."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate_native(pins, receipt, library, go_sum):
    require(receipt.get('source_clean') is True, 'native source was not clean')
    for field, expected in [('source_commit', pins['source_commit']),
                            ('binding_module', pins['go_module']),
                            ('binding_version', pins['go_version'])]:
        require(receipt.get(field) == expected, 'native receipt mismatch: ' + field)
    require(receipt.get('shared_library_sha256') == digest(library), 'native library checksum mismatch')
    require(f"{pins['go_module']} {pins['go_version']} {receipt.get('binding_sum')}" in go_sum.splitlines(),
            'binding checksum does not match go.sum')


def check_output(output, mode, completed=None, reason='completed'):
    require(output['schema'] == 1 and output['qualification'] == 'component', 'unqualified CLI result')
    require(output['mode'] == mode, 'unexpected execution mode')
    result = output['result']
    require(result['stop_reason'] == reason, 'unexpected stop reason')
    if completed is not None:
        require(result['completed'] == completed, 'incorrect completed count')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--native-root', type=Path, default=ROOT,
                        help='Checkout containing pinned .local/go-node-build.json and native source/library')
    parser.add_argument('--evidence', type=Path, required=True, help='New output directory outside source checkout')
    args = parser.parse_args()
    evidence = args.evidence.resolve()
    require(not evidence.is_relative_to(ROOT), 'evidence must be outside source checkout')
    evidence.mkdir(parents=True, exist_ok=False)
    report = dict(schema=1, scope='generated coupled CLI component', result='failed',
                  full_dst_acceptance=False, full_issue_119_acceptance=False,
                  commands=[], assertions=[], limitations=[
                      'fixed workflow bytes and external linearization order; three existing safety cuts only',
                      'no instrumented zero-I/O proof; native library is linked but engine use is modeled',
                      'generator-free replay checks absent source input and empty tool PATH, not a filesystem sandbox'])
    env = {k: v for k, v in os.environ.items() if not k.startswith(('AWS_', 'DYLD_', 'LD_', 'CGO_'))}
    pins = json.loads((ROOT / 'tools/slatedb-native.json').read_text())
    native_root = args.native_root.resolve()
    receipt_path = native_root / '.local/go-node-build.json'
    native = json.loads(receipt_path.read_text())
    library = (native_root / native['shared_library']).resolve()
    require(library.is_relative_to(native_root / '.local'), 'native library escapes declared root')
    env.update(GOTOOLCHAIN=pins['go_toolchain'], GOENV='off', GOWORK='off', GOFLAGS='-mod=readonly',
               CGO_ENABLED='1', CGO_LDFLAGS='-L' + str(library.parent),
               DYLD_LIBRARY_PATH=str(library.parent), LD_LIBRARY_PATH=str(library.parent))

    def run(argv, expected=0, cwd=ROOT, extra_env=None, timeout=60):
        index = len(report['commands'])
        record = dict(argv=[str(x) for x in argv], cwd=str(cwd), timeout_seconds=timeout)
        report['commands'].append(record)
        stdout, stderr = evidence / f'{index:03}.out', evidence / f'{index:03}.err'
        try:
            with stdout.open('xb') as out, stderr.open('xb') as err:
                result = subprocess.run(record['argv'], cwd=cwd, env=env | (extra_env or {}),
                                        stdout=out, stderr=err, timeout=timeout)
            record['exit_code'] = result.returncode
            require(result.returncode == expected, f'command {index} exit {result.returncode}, expected {expected}')
        except BaseException as error:
            record['error'] = str(error)
            raise
        return stdout.read_text()

    try:
        revision = run(['git', 'rev-parse', 'HEAD']).strip()
        require(not run(['git', 'status', '--porcelain', '--untracked-files=all']).strip(), 'clean source checkout required')
        report.update(source_revision=revision, source_dirty=False)
        validate_native(pins, native, library, (ROOT / 'go.sum').read_text())
        source = native_root / '.local/slatedb-native-source'
        require(run(['git', 'rev-parse', 'HEAD'], cwd=source).strip() == pins['source_commit'], 'native source revision mismatch')
        require(not run(['git', 'status', '--porcelain', '--untracked-files=all'], cwd=source).strip(), 'native source modified')
        require(digest(source / 'Cargo.lock') == native['source_cargo_lock_sha256'], 'native lock mismatch')
        report['native_receipt'] = native
        report['native_receipt_sha256'] = digest(receipt_path)
        require(run(['go', 'version']).split()[2] == pins['go_toolchain'], 'wrong Go toolchain')
        binary = evidence / 'xenon'
        flags = f"-X github.com/0x63616c/xenon/internal/buildinfo.NativeCommit={pins['source_commit']} -X github.com/0x63616c/xenon/internal/buildinfo.NativeSHA256={digest(library)}"
        run(['go', 'build', '-buildvcs=true', '-ldflags', flags, '-o', binary, './cmd/xenon'], timeout=180)
        info = json.loads(run([binary, 'version']))
        require(info['revision'] == revision and info['modified'] == 'false' and info['go'] == pins['go_toolchain'], 'built CLI source/tool mismatch')
        report['build'] = info
        report['binary_sha256'] = digest(binary)
        source_input = ROOT / 'test/scenarios/simulation/coordinator-move.json'
        copied = evidence / 'source-input.json'
        copied.write_bytes(source_input.read_bytes())
        report['input_sha256'] = digest(copied)
        mode = 'seeded-coupled-delivery-interleavings'
        common = [binary, 'search', '--interleave', '--scenario', copied, '--workload-seed', '42', '--fault-seed', '119']
        output = json.loads(run([*common, '--max-cases', '4', '--duration', '30s', '--evidence', evidence / 'finite']))
        check_output(output, mode, 4)
        artifacts = sorted((evidence / 'finite').glob('case-*/scenario.json'))
        require(len(artifacts) == 4, 'finite case census mismatch')
        records = [json.loads(p.read_text()) for p in artifacts]
        orders = [hashlib.sha256(json.dumps(r['scenario']['faults'], sort_keys=True).encode()).hexdigest() for r in records]
        require(len(set(orders)) >= 2, 'generated search repeated one order')
        require(all(r['scenario']['workload'] == records[0]['scenario']['workload'] for r in records), 'fixed workload changed')
        report['assertions'].append('four generated completed cases; multiple orders and fixed workload')
        artifact = artifacts[-1]
        before = digest(artifact)
        copied.rename(evidence / 'source-input-unavailable.json')
        replay_cwd = evidence / 'empty-cwd'
        replay_cwd.mkdir()
        replay = json.loads(run([binary, 'replay', '--artifact', artifact, '--evidence', evidence / 'replay'],
                                cwd=replay_cwd, extra_env={'PATH': ''}))
        check_output(replay, 'exact-component-artifact-replay', 1)
        saved = evidence / 'replay/case-00000000000000000000/scenario.json'
        require(json.loads(saved.read_text())['scenario'] == records[-1]['scenario'], 'expanded replay input changed')
        original_trace = artifact.with_name('trace.jsonl')
        replay_trace = saved.with_name('trace.jsonl')
        require(original_trace.read_bytes() == replay_trace.read_bytes(), 'replay decision trace changed')
        require(digest(artifact) == before, 'original artifact changed')
        report['assertions'].append('saved generated case replayed without source input/tool PATH; exact expanded input and trace')
        report['replay_trace_sha256'] = digest(replay_trace)
        # Restore only the harness-owned input for the separate continuous search.
        (evidence / 'source-input-unavailable.json').rename(copied)
        budget = json.loads(run([*common, '--continuous', '--duration', '250ms', '--evidence', evidence / 'continuous'], expected=2))
        check_output(budget, mode, reason='budget')
        report['assertions'].append('continuous budget returns exit 2; no full correctness claim')
        report['outputs'] = dict(finite=output, replay=replay, continuous=budget)
        report['distinct_order_count'] = len(set(orders))
        require(run(['git', 'rev-parse', 'HEAD']).strip() == revision and not run(['git', 'status', '--porcelain', '--untracked-files=all']).strip(), 'source changed during proof')
        validate_native(pins, native, library, (ROOT / 'go.sum').read_text())
        report['result'] = 'component-passed'
    except BaseException as error:
        report['error'] = str(error)
    finally:
        report['files'] = {str(p.relative_to(evidence)): digest(p) for p in evidence.rglob('*') if p.is_file()}
        (evidence / 'receipt.json').write_text(json.dumps(report, indent=2) + '\n')
        print(str(evidence / 'receipt.json'))
    return 0 if report['result'] == 'component-passed' else 1


if __name__ == '__main__':
    sys.exit(main())
