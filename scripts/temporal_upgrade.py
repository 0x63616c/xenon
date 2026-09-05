#!/usr/bin/env python3
"""Inventory explicit clean Temporal revisions; never infer upgrade compatibility."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
SEAMS = [
    ('persistence', ('common/persistence/',), ['internal/adapter', 'internal/node', 'internal/temporalstore'], ['go-runtime-stores']),
    ('serialization', ('api/persistence/', 'proto/internal/', 'common/codec/', 'common/primitives/', 'common/converter/'), ['proto/xenon/v1', 'internal/adapter/execution_codec.go', 'internal/node'], ['go-runtime-stores', 'go-visibility']),
    ('schema', ('schema/',), ['internal/node', 'docs/research'], ['go-runtime-stores', 'go-visibility']),
    ('visibility', ('common/persistence/visibility/', 'common/searchattribute/',), ['internal/query', 'internal/visibility', 'internal/adapter/visibility.go'], ['go-visibility', 'go-visibility-frozen']),
    ('server-integration', ('temporal/', 'common/config/', 'service/',), ['internal/temporalstore', 'cmd', 'proof/ministack'], ['go-runtime-stores']),
    ('dependencies', ('go.mod', 'go.sum'), ['go.mod', 'go.sum', 'tools', 'proof/ministack'], ['go-runtime-stores', 'go-visibility']),
]

def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args], stderr=subprocess.PIPE, timeout=60)

def clean(repo):
    if git(repo, 'status', '--porcelain=v1', '--untracked-files=all'):
        raise ValueError('Temporal source checkout is dirty')

def sha(data):
    return hashlib.sha256(data).hexdigest()

def impact(repo, old, new, output):
    repo = Path(repo).resolve()
    output = Path(output).resolve()
    if output == repo or repo in output.parents:
        raise ValueError('Report must be outside the Temporal source checkout')
    clean(repo)
    commits = [git(repo, 'rev-parse', '--verify', '--end-of-options', ref + '^{commit}').decode().strip() for ref in (old, new)]
    # No rename guessing: deletion and addition remain independently reviewable.
    paths = git(repo, 'diff', '--no-renames', '--name-only', '-z', *commits, '--').decode().split('\0')
    patch = git(repo, 'diff', '--no-ext-diff', '--no-textconv', '--no-renames', '--binary', *commits, '--')
    changes = []
    for path in filter(None, paths):
        matches = [dict(area=area, xenon_seams=seams, suggested_scenarios=tests) for area, prefixes, seams, tests in SEAMS if any(path == prefix or (prefix.endswith("/") and path.startswith(prefix)) for prefix in prefixes)]
        changes.append({'path': path, 'impacts': matches, 'review_required': True, 'unmapped': not matches})
    clean(repo)
    if [git(repo, 'rev-parse', '--verify', '--end-of-options', ref + '^{commit}').decode().strip() for ref in (old, new)] != commits:
        raise ValueError('Temporal references changed during report')
    report = {'schema': 1, 'old_ref': old, 'new_ref': new, 'old_commit': commits[0], 'new_commit': commits[1],
              'temporal_source_clean': True, 'git_version': git(repo, '--version').decode().strip(),
              'xenon_commit': git(ROOT, 'rev-parse', 'HEAD').decode().strip(),
              'xenon_dirty': bool(git(ROOT, 'status', '--porcelain=v1', '--untracked-files=all')),
              'script_sha256': sha(Path(__file__).read_bytes()), 'patch_sha256': sha(patch), 'changes': changes,
              'result': 'impact-inventory', 'fresh_install': 'NOT_TESTED', 'existing_state_upgrade': 'NOT_TESTED',
              'limitations': ['Path classification is a review aid, not a semantic or interface compatibility analysis.', 'All changed paths, including unmapped files, require review.', 'Suggested scenario names are not execution receipts.']}
    output.mkdir(parents=True, exist_ok=False)
    (output / 'temporal.diff').write_bytes(patch)
    (output / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    return report

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--old', required=True)
    p.add_argument('--new', required=True)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    impact(a.source, a.old, a.new, a.output)

if __name__ == '__main__':
    main()
