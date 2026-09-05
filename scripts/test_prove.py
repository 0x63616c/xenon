import os
import subprocess
import json
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

    def test_all_committed_experiments_keep_registered_commands(self):
        for path in sorted((prove.ROOT / "experiments").glob("*.json")):
            manifest = json.loads(path.read_text())
            if manifest["name"] == "go-bindings":
                self.assertEqual(manifest["source_commit"], "3fb9e8abab0c9f5833f0c154140ceef009fea02a")
                self.assertEqual(len(manifest["expected_tests"]), 4)
                help_text = subprocess.check_output([sys.executable, str(prove.ROOT / "scripts/prove.py"), "--help"], text=True)
                self.assertIn("go-bindings", help_text)
                continue
            for spec in manifest["commands"]:
                with self.subTest(experiment=manifest["name"], runner=spec["runner"]):
                    self.assertTrue(prove.command(spec))

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

    def test_shard_commands_and_go_results(self):
        self.assertEqual(prove.command({"runner": "cargo-build-node"}), ["cargo", "build", "--locked", "-p", "xenon-node"])
        with self.assertRaises(ValueError):
            prove.command({"runner": "cargo-build-node", "shell": "echo unexpected"})
        with self.assertRaises(ValueError):
            prove.command({"runner": "go-test-shard", "filter": "Unknown", "exact": True, "expected_tests": ["Unknown"]})
        import json
        passed = [
            {"Action": "pass", "Test": "TestShardRPC", "Package": "github.com/0x63616c/xenon/internal/adapter"},
            {"Action": "pass", "Package": "github.com/0x63616c/xenon/internal/adapter"},
        ]
        output = "\n".join(json.dumps(event) for event in passed)
        prove.verify_go_tests(output, ["TestShardRPC"])
        prove.verify_go_tests("go: downloading example v1.0\n" + output, ["TestShardRPC"])
        for bad in ("", json.dumps(passed[-1]), output + '\n' + json.dumps({"Action": "skip", "Test": "Other"})):
            with self.assertRaises(ValueError):
                prove.verify_go_tests(bad, ["TestShardRPC"])

    def test_timeout_is_failure(self):
        code, _, expired = prove.run_process([sys.executable, '-c', 'import time; time.sleep(10)'], 0.05, os.environ.copy(), Path.cwd())
        self.assertTrue(expired)
        self.assertNotEqual(code, 0)


if __name__ == '__main__':
    unittest.main()
