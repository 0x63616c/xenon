import os
from pathlib import Path
import sys
import tempfile
import unittest

import prove


class RunnerTests(unittest.TestCase):
    def test_dirty_development_can_never_be_proof_pass(self):
        self.assertEqual(prove.classify_success(True), ("development-passed", False))
        self.assertEqual(prove.classify_success(False), ("passed", True))

    def test_ambient_cargo_config_discovery(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / "repo"
            root.mkdir()
            config = Path(folder) / ".cargo" / "config.toml"
            config.parent.mkdir()
            config.write_text("[build]\n")
            self.assertEqual(prove.cargo_configs(root, {"HOME": str(Path(folder) / "home")}), [str(config.resolve())])

    def test_commands_are_allowlisted(self):
        with self.assertRaises(ValueError):
            prove.command({'runner': 'shell', 'filter': 'x', 'exact': True, 'expected_tests': ['x']})
        with self.assertRaises(ValueError):
            prove.command({'runner': 'cargo-test', 'filter': 'x;echo secret', 'exact': True, 'expected_tests': ['x']})

    def test_zero_skipped_or_different_tests_fail(self):
        for output in ['test result: ok. 0 passed; 0 failed; 0 ignored;',
                       'test wanted ... ignored\ntest result: ok. 0 passed; 0 failed; 1 ignored;',
                       'test other ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;']:
            with self.assertRaises(ValueError):
                prove.verify_tests(output, ['wanted'])
        prove.verify_tests('test wanted ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;', ['wanted'])

    def test_timeout_is_failure(self):
        code, _, expired = prove.run_process([sys.executable, '-c', 'import time; time.sleep(10)'], 0.05, os.environ.copy(), Path.cwd())
        self.assertTrue(expired)
        self.assertNotEqual(code, 0)


if __name__ == '__main__':
    unittest.main()
