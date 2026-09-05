import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('fuzz_soak', Path(__file__).with_name('fuzz-soak.py'))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

class FuzzSoakControls(unittest.TestCase):
    def run_case(self, completed, times):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / 'proof/omes-corpus/soak.json'
            config.parent.mkdir(parents=True)
            config.write_text(json.dumps({'schema':1,'minimum_seconds':3600,'minimum_rounds':2,'maximum_rounds':3}))
            calls = []
            def workload(stage, profile, evidence, binary, source):
                calls.append((stage, profile))
                evidence.mkdir()
                result = {'workload_completed': completed}
                (evidence / 'result.json').write_text(json.dumps(result))
                return result
            with patch.object(runner, 'ROOT', root), patch.object(runner, 'run_workload', workload), patch.object(runner.time, 'monotonic', side_effect=times):
                result = runner.soak(root / 'evidence', root / 'binary', root / 'source')
            self.assertEqual(json.loads((root/'evidence/result.json').read_text()), result)
            return result, calls
    def test_failure_is_preserved_and_never_retried(self):
        result, calls = self.run_case(False, [0, 1])
        self.assertFalse(result['soak_pass'])
        self.assertEqual(len(calls), 1)
        self.assertIn('round 0 failed', result['error'])
    def test_both_duration_and_round_minimum_required(self):
        result, calls = self.run_case(True, [0, 3601, 3602])
        self.assertTrue(result['soak_pass'])
        self.assertEqual(len(calls), 2)
        self.assertFalse(result['full_acceptance'])
    def test_round_cap_cannot_manufacture_a_pass(self):
        result, calls = self.run_case(True, [0, 1, 2, 3])
        self.assertFalse(result['soak_pass'])
        self.assertEqual(len(calls), 3)

if __name__ == '__main__':
    unittest.main()
