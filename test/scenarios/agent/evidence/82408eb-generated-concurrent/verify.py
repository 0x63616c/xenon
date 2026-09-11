#!/usr/bin/env python3
"""Verify this bounded checkpoint against its preserved raw output directory."""
import argparse
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parent
receipt = json.loads((root / 'result.json').read_text())
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--evidence', type=Path, default=Path(receipt['source_evidence_directory']))
p.add_argument('--build', type=Path, default=Path(receipt['source_build_directory']))
a = p.parse_args()

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()

assert sha(a.evidence / 'journey.json') == receipt['source_journey_sha256']
assert sha(a.build / 'build.json') == receipt['source_build_receipt_sha256']
assert sha(a.build / 'fixture.json') == receipt['fixture_sha256']
outputs = json.loads((root / 'outputs.sha256.json').read_text())
assert len(outputs) == receipt['verified_output_count']
for name, expected in outputs.items():
    assert Path(name).is_relative_to('.') and '..' not in Path(name).parts
    assert sha(a.evidence / name) == expected, name
raw = json.loads((a.evidence / 'journey.json').read_text())
assert raw['result'] == receipt['result'] == 'journey-passed'
assert raw['source_revision'] == raw['image_source_revision'] == receipt['source_revision']
assert raw['source_dirty'] is False and raw['discovery'] is False
assert raw['cleanup_errors'] == receipt['cleanup_errors'] == []
assert raw['assertions'] == receipt['assertions'] and len(raw['assertions']) == receipt['assertion_count']
assert json.loads((a.evidence / 'workflow-search/result.json').read_text()) == receipt['search_result']
assert receipt['search_result']['completed'] == 3
assert raw['workflow_observations'] == receipt['observations']
assert receipt['observations']['totals']['initial_roots'] == 12
assert list(receipt['observations']['case_execution_interval_peak'].values()) == [4, 4, 4]
for item in receipt['cleanup_enumeration']:
    output = (a.evidence / f"{item['command_index']:03}.out").read_text()
    if 'owned_container_ids' in item: assert output.strip() == '' and item['owned_container_ids'] == []
    else: assert json.loads(output) == item['dev_down'] and item['dev_down']['cleanup_verified'] and not item['dev_down'].get('pending')
assert receipt['full_acceptance'] is False
print(json.dumps({'schema': 1, 'verified_outputs': len(outputs), 'scope': 'component evidence integrity; not full #119 acceptance'}))
