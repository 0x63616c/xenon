#!/usr/bin/env python3
"""Check the built CLI's finite coupled journeys; never starts storage backends."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--evidence', type=Path, required=True, help='New directory')
    parser.add_argument('--source', required=True, help='Expected clean source revision')
    args = parser.parse_args()
    binary = args.binary.resolve()
    evidence = args.evidence.resolve()
    evidence.mkdir(exist_ok=False)
    root = Path(__file__).resolve().parent.parent
    env = {k: v for k, v in os.environ.items() if not k.startswith('AWS_')}

    def run(arguments, code=0):
        result = subprocess.run([str(binary), *arguments], cwd=root, env=env,
                                capture_output=True, text=True, timeout=30)
        if result.returncode != code:
            raise AssertionError((arguments, result.returncode, result.stdout, result.stderr))
        return result

    info = json.loads(run(['version']).stdout)
    assert info['revision'] == args.source and info['modified'] == 'false', info
    source_input = root / 'test/scenarios/simulation/coordinator-move.json'
    results = []
    for name, command in [('test', ['test', 'simulation']), ('search', ['search'])]:
        result = run([*command, '--scenario', str(source_input), '--evidence', str(evidence / name)])
        output = json.loads(result.stdout)
        assert output['qualification'] == 'component' and output['result']['completed'] == 1
        assert not result.stderr
        results.append(output)
    case = 'case-00000000000000000000'
    artifact = evidence / 'test' / case / 'scenario.json'
    result = run(['replay', '--artifact', str(artifact), '--evidence', str(evidence / 'replay')])
    output = json.loads(result.stdout)
    assert output['result']['completed'] == 1 and not result.stderr
    results.append(output)
    traces = [(evidence / name / case / 'trace.jsonl').read_bytes()
              for name in ('test', 'search', 'replay')]
    assert traces[0] == traces[1] == traces[2]
    result = run(['search', '--scenario', str(source_input), '--duration', '1ns',
                  '--evidence', str(evidence / 'budget')], 2)
    assert json.loads(result.stdout)['result']['stop_reason'] == 'budget' and result.stderr
    bad = json.loads(source_input.read_bytes())
    bad['steps'][1]['effect'] = 999
    bad_path = evidence / 'bad.json'
    bad_path.write_text(json.dumps(bad))
    result = run(['search', '--scenario', str(bad_path), '--evidence', str(evidence / 'failure')], 1)
    assert json.loads(result.stdout)['result']['stop_reason'] == 'first_failure' and result.stderr
    receipt = dict(source=info['revision'], modified=info['modified'], go=info['go'],
                   binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                   input_sha256=hashlib.sha256(source_input.read_bytes()).hexdigest(),
                   harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                   trace_sha256=hashlib.sha256(traces[0]).hexdigest(), results=results,
                   controls=['budget exit 2', 'bad effect exit 1'],
                   scope='finite coupled component; no native resources opened')
    (evidence / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(receipt, indent=2))


if __name__ == '__main__':
    main()
