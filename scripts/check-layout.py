#!/usr/bin/env python3
"""Check declared proof inputs and the relocated reference workspace without building."""
import json
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[1]
for manifest in sorted((root / 'experiments').glob('*.json')):
    for name in json.loads(manifest.read_text()).get('inputs', []):
        path = (root / name).resolve()
        if root not in path.parents or not path.is_file():
            raise SystemExit(f'{manifest.name}: missing or external input {name}')
metadata = json.loads(subprocess.check_output([
    'cargo', 'metadata', '--locked', '--no-deps', '--format-version', '1',
    '--manifest-path', 'test/compatibility/rust/Cargo.toml'], cwd=root, timeout=60))
expected = root / 'test/compatibility/rust'
if Path(metadata['workspace_root']).resolve() != expected:
    raise SystemExit('wrong reference workspace')
if {p['name'] for p in metadata['packages']} != {'slatedb-probe', 'xenon-node'}:
    raise SystemExit('reference packages changed')
if not (expected / 'crates/xenon-node/../../../../../proto/xenon/v1/persistence.proto').resolve().is_file():
    raise SystemExit('reference protobuf source missing')
print('Declared inputs and reference workspace paths verified; runtime tests not executed.')
