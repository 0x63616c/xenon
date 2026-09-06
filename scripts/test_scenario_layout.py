import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import prove

spec = importlib.util.spec_from_file_location('check_layout', Path(__file__).with_name('check-layout.py'))
layout = importlib.util.module_from_spec(spec)
spec.loader.exec_module(layout)


class ScenarioLayoutTests(unittest.TestCase):
    def test_legacy_and_moved_manifests_remain_resolvable(self):
        paths = prove.manifest_paths()
        self.assertGreater(len(paths), len(prove.SCENARIO_MANIFESTS))
        for path in paths:
            self.assertEqual(path, prove.manifest_path(json.loads(path.read_text())['name']))
        self.assertEqual(layout.validate_inputs(), len(paths))

    def test_missing_external_and_duplicate_inputs_rejected(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(prove, 'SCENARIO_MANIFESTS', {}):
            root = Path(folder)
            (root / 'experiments').mkdir()
            manifest = root / 'experiments/example.json'
            manifest.write_text(json.dumps({'name': 'example', 'inputs': ['missing']}))
            with self.assertRaisesRegex(ValueError, 'missing or external'):
                layout.validate_inputs(root)
            with self.assertRaisesRegex(ValueError, 'missing or external'):
                layout.local_file(root, __file__)
            manifest.write_text(json.dumps({'name': 'example', 'inputs': []}))
            extra = root / 'test/scenarios/example/manifests'
            extra.mkdir(parents=True)
            (extra / 'duplicate.json').write_text(manifest.read_text())
            with self.assertRaisesRegex(ValueError, 'duplicate or unregistered'):
                layout.validate_inputs(root)

    def test_moved_fixture_hash_and_old_path_are_enforced(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(prove, 'SCENARIO_MANIFESTS', {}):
            root = Path(folder)
            scenario = root / 'test/scenarios/example'
            scenario.mkdir(parents=True)
            (scenario / 'scenario.json').write_text(json.dumps({'schema':1,'name':'example','inputs':[],'frozen_shared_inputs':[],'component_manifests':[],'commands':{}}))
            fixture = scenario / 'fixture'
            fixture.write_bytes(b'original')
            import hashlib
            item = {'from':'old-fixture','to':str(fixture.relative_to(root)),'sha256':hashlib.sha256(b'original').hexdigest()}
            (scenario / 'migration.json').write_text(json.dumps({'preserved_inputs':[item]}))
            layout.validate_inputs(root)
            fixture.write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'bytes changed'):
                layout.validate_inputs(root)
            fixture.write_bytes(b'original')
            (root / 'old-fixture').write_bytes(b'original')
            with self.assertRaisesRegex(ValueError, 'old scenario copy'):
                layout.validate_inputs(root)
