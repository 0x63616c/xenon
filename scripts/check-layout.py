#!/usr/bin/env python3
"""Validate legacy and scenario inputs without executing native/runtime workloads."""
import hashlib
import json
from pathlib import Path
import subprocess
import prove

ROOT = Path(__file__).resolve().parents[1]


def local_file(root, name):
    path = (root / name).resolve()
    if root not in path.parents or not path.is_file():
        raise ValueError('missing or external input: ' + name)
    return path


def validate_inputs(root=ROOT):
    root = root.resolve()
    seen = set()
    for path in prove.manifest_paths(root):
        manifest = json.loads(path.read_text())
        name = manifest['name']
        if name in seen or prove.manifest_path(name, root) != path:
            raise ValueError('duplicate or unregistered manifest: ' + name)
        seen.add(name)
        for name in manifest.get('inputs', []):
            local_file(root, name)
    for name in prove.SCENARIO_MANIFESTS:
        if not prove.manifest_path(name, root).is_file():
            raise ValueError('registered scenario manifest missing: ' + name)
    for path in sorted((root / 'test/scenarios').glob('*/scenario.json')):
        scenario = json.loads(path.read_text())
        if scenario['schema'] != 1 or scenario['name'] != path.parent.name:
            raise ValueError('scenario identity mismatch')
        for name in scenario['inputs'] + scenario['frozen_shared_inputs']:
            local_file(root, name)
        for name in scenario['component_manifests']:
            if prove.manifest_path(name, root).parent != path.parent / 'manifests':
                raise ValueError('scenario manifest link differs: ' + name)
        for argv in scenario['commands'].values():
            if argv[0] != 'python3':
                raise ValueError('unsupported scenario command')
            local_file(root, argv[1])
        migration = json.loads((path.parent / 'migration.json').read_text())
        for item in migration['preserved_inputs']:
            actual = hashlib.sha256(local_file(root, item['to']).read_bytes()).hexdigest()
            if actual != item['sha256']:
                raise ValueError('relocated input bytes changed: ' + item['to'])
            if (root / item['from']).exists():
                raise ValueError('old scenario copy remains: ' + item['from'])
    return len(seen)


def main():
    count = validate_inputs()
    metadata = json.loads(subprocess.check_output([
        'cargo', 'metadata', '--locked', '--no-deps', '--format-version', '1',
        '--manifest-path', 'test/compatibility/rust/Cargo.toml'], cwd=ROOT, timeout=60))
    expected = ROOT / 'test/compatibility/rust'
    if Path(metadata['workspace_root']).resolve() != expected:
        raise ValueError('wrong reference workspace')
    if {p['name'] for p in metadata['packages']} != {'slatedb-probe', 'xenon-node'}:
        raise ValueError('reference packages changed')
    if not (expected / 'crates/xenon-node/../../../../../proto/xenon/v1/persistence.proto').resolve().is_file():
        raise ValueError('reference protobuf source missing')
    print(f'{count} manifest input sets, scenario links/bytes and reference workspace verified; runtime not executed.')


if __name__ == '__main__':
    main()
