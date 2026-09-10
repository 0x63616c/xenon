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


if __name__ == '__main__': unittest.main()
