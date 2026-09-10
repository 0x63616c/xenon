import base64
import copy
import importlib.util
import json
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('journey', Path(__file__).with_name('dev-journey.py'))
journey = importlib.util.module_from_spec(spec)
spec.loader.exec_module(journey)


class SnapshotTest(unittest.TestCase):
    def test_independent_authority_rejects_missing_wrong_or_unready_owners(self):
        config = {'service_storage': {'cluster_id': 'cluster', 'layout': {'partitions': [{'id': str(n)} for n in range(3)]}}}
        control = {'cluster': 'cluster', 'partitions': {str(n): {'ready': True, 'generation': 1, 'desired': {'node': str(n), 'incarnation': str(n)}} for n in range(3)}}
        def verify(candidate, inspection=None):
            raw = json.dumps({'body': base64.b64encode(json.dumps(candidate).encode()).decode()})
            return journey.check_snapshot(raw, inspection or {'status': 'observed', 'control': candidate}, config)
        self.assertEqual(verify(control), control)
        for mutation in ('cluster', 'inventory', 'readiness', 'generation', 'incarnation', 'owners', 'inspection'):
            with self.subTest(mutation=mutation):
                altered = copy.deepcopy(control)
                inspection = None
                if mutation == 'cluster': altered['cluster'] = 'wrong'
                if mutation == 'inventory': del altered['partitions']['0']
                if mutation == 'readiness': altered['partitions']['0']['ready'] = False
                if mutation == 'generation': altered['partitions']['0']['generation'] = 0
                if mutation == 'incarnation': altered['partitions']['0']['desired']['incarnation'] = ''
                if mutation == 'owners': altered['partitions']['0']['desired']['node'] = '1'
                if mutation == 'inspection': inspection = {'status': 'observed', 'control': {}}
                with self.assertRaises(ValueError): verify(altered, inspection)


class SentinelTest(unittest.TestCase):
    def test_unknown_create_is_reconciled_by_exact_identity_or_retained_pending(self):
        for case in ('normal', 'unknown-found', 'unknown-absent', 'replacement', 'wrong-label'):
            with self.subTest(case=case):
                removed = []
                def run(command):
                    if command[1] == 'ps': return '' if case == 'unknown-absent' else 'container'
                    self.assertEqual(command, ['docker', 'rm', 'container'])
                    removed.append(command[-1])
                def obj(command):
                    self.assertEqual(command, ['docker', 'inspect', 'container'])
                    return [{'Name': '/sentinel', 'Config': {'Labels': {'io.xenon.journey': 'wrong' if case == 'wrong-label' else 'token'}}}]
                expected = 'replacement' if case == 'replacement' else ('container' if case == 'normal' else None)
                if case in ('unknown-absent', 'replacement', 'wrong-label'):
                    with self.assertRaises(RuntimeError): journey.reconcile_sentinel(run, obj, 'sentinel', 'token', expected, True)
                    self.assertEqual(removed, [])
                else:
                    journey.reconcile_sentinel(run, obj, 'sentinel', 'token', expected, True)
                    self.assertEqual(removed, ['container'])



class IdentityTest(unittest.TestCase):
    def test_stale_dirty_unknown_native_and_dependency_builds_rejected(self):
        pins = {'go_toolchain': 'go1.27.1', 'go_module': 'slate', 'go_version': 'v1', 'source_commit': 'native'}
        temporal = {'module': 'temporal', 'version': 'v2'}
        sums = {('slate', 'v1'): 'sum1', ('temporal', 'v2'): 'sum2'}
        valid = {'revision': 'source', 'modified': 'false', 'go': 'go1.27.1',
                 'slatedb_go': {'path': 'slate', 'version': 'v1', 'sum': 'sum1'},
                 'temporal': {'path': 'temporal', 'version': 'v2', 'sum': 'sum2'},
                 'slatedb_native': {'source_commit': 'native', 'artifact_sha256': 'hash', 'identity_source': 'build-attestation'}}
        journey.check_cli_identity(valid, 'source', pins, temporal, 'hash', sums)
        for field, replacement in [('revision', 'stale'), ('modified', 'true'), ('modified', 'unknown'), ('go', 'go1.0'),
                                   ('slatedb_native', {}), ('slatedb_go', {}), ('temporal', {})]:
            with self.subTest(field=field, replacement=replacement):
                bad = copy.deepcopy(valid)
                bad[field] = replacement
                with self.assertRaises(ValueError): journey.check_cli_identity(bad, 'source', pins, temporal, 'hash', sums)
        with self.assertRaises(ValueError): journey.check_cli_identity(valid, 'source', pins, temporal, 'wrong-library', sums)

if __name__ == '__main__': unittest.main()
