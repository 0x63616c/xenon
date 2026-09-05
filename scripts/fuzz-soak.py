#!/usr/bin/env python3
"""Replay committed Omes corpus for a declared minimum duration on a supervised stack."""
import argparse
import json
from pathlib import Path
import time
from omes_workloads import ROOT, run_workload, sha


def soak(evidence, binary, source):
    config_path = ROOT / 'proof/omes-corpus/soak.json'
    config = json.loads(config_path.read_text())
    evidence.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    report = {'schema': 1, 'soak_pass': False, 'full_acceptance': False,
              'config_sha256': sha(config_path), 'rounds': [], 'faults_injected': False}
    try:
        if config['schema'] != 1 or config['minimum_seconds'] <= 0 or config['minimum_rounds'] < 1:
            raise ValueError('invalid soak contract')
        for index in range(config['maximum_rounds']):
            result = run_workload('fuzz', 'without-faults', evidence / f'round-{index:04d}', binary, source)
            report['rounds'].append({'index': index, 'workload_completed': result['workload_completed'],
                                     'result_sha256': sha(evidence / f'round-{index:04d}/result.json')})
            if not result['workload_completed']:
                raise RuntimeError(f'fuzz round {index} failed; retain exact input and command log')
            if index + 1 >= config['minimum_rounds'] and time.monotonic() - started >= config['minimum_seconds']:
                report['soak_pass'] = True
                break
        if not report['soak_pass']:
            raise RuntimeError('round bound reached before minimum duration')
    except Exception as error:
        report['error'] = str(error)
    finally:
        report['elapsed_seconds'] = time.monotonic() - started
        (evidence / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--omes-binary', type=Path, required=True)
    parser.add_argument('--omes-source', type=Path, required=True)
    args = parser.parse_args()
    report = soak(args.evidence_dir.resolve(), args.omes_binary.resolve(), args.omes_source.resolve())
    print(json.dumps(report))
    raise SystemExit(0 if report['soak_pass'] else 1)
