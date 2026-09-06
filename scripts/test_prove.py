import os
import subprocess
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import prove


class RunnerTests(unittest.TestCase):
    def test_tool_version_allows_toolchain_manager_diagnostics(self):
        output = "info: syncing pinned channel\nrustc 1.94.0 (4a4ef493e 2026-03-02)\nbinary: rustc"
        self.assertTrue(prove.version_matches(output, "rustc 1.94.0 "))
        self.assertFalse(prove.version_matches(output, "rustc 1.93.0 "))

    def test_meter_registered_commands(self):
        manifest = json.loads((prove.ROOT / "experiments/s3-meter.json").read_text())
        for spec in manifest["commands"]:
            self.assertTrue(prove.command(spec))
        spec = dict(manifest["commands"][0], filter="TestUnknown")
        with self.assertRaises(ValueError):
            prove.command(spec)


    def test_compatibility_cleanup_failure_preserves_evidence(self):
        manifest = (prove.ROOT / "experiments/go-shard-compat.json").read_text()
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "experiments").mkdir()
            (root / "experiments/go-shard-compat.json").write_text(manifest)
            with patch.object(prove, "ROOT", root), patch.object(sys, "argv", ["prove.py", "go-shard-compat"]), patch.object(prove, "cargo_configs", return_value=[]), patch.object(prove.subprocess, "check_output", side_effect=["a" * 40, " M tracked"]), patch.object(prove, "run_process", side_effect=FileNotFoundError("docker unavailable")):
                self.assertEqual(prove.main(), 1)
            reports = list((root / ".local/evidence").glob("*/result.json"))
            self.assertEqual(len(reports), 1)
            report = json.loads(reports[0].read_text())
            self.assertFalse(report["proof_pass"])
            self.assertEqual(report["result"], "failed")
            self.assertEqual(report["cleanup"]["exit_code"], -1)
            self.assertIn("docker unavailable", report["cleanup"]["error"])
    def test_directory_cleanup_failure_retains_failed_report(self):
        manifest = (prove.ROOT / "experiments/directory.json").read_text()
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "experiments").mkdir()
            (root / "experiments/directory.json").write_text(manifest)
            with patch.object(prove, "ROOT", root), patch.object(sys, "argv", ["prove.py", "directory"]), patch.object(prove, "cargo_configs", return_value=[]), patch.object(prove.subprocess, "check_output", side_effect=["a" * 40, " M tracked"]), patch.object(prove, "run_process", side_effect=FileNotFoundError("docker unavailable")):
                self.assertEqual(prove.main(), 1)
            report = json.loads(next((root / ".local/evidence").glob("*/result.json")).read_text())
            self.assertFalse(report["proof_pass"])
            self.assertEqual(report["result"], "failed")
            self.assertEqual(report["cleanup"]["exit_code"], -1)

    def test_long_maintenance_cleanup_failure_retains_failed_report(self):
        manifest = (prove.ROOT / "experiments/maintenance.json").read_text()
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "experiments").mkdir()
            (root / "experiments/maintenance.json").write_text(manifest)
            with patch.object(prove, "ROOT", root), patch.object(sys, "argv", ["prove.py", "maintenance"]), patch.object(prove, "cargo_configs", return_value=[]), patch.object(prove.subprocess, "check_output", side_effect=["a" * 40, " M tracked"]), patch.object(prove, "run_process", side_effect=FileNotFoundError("docker unavailable")):
                self.assertEqual(prove.main(), 1)
            report = json.loads(next((root / ".local/evidence").glob("*/result.json")).read_text())
            self.assertFalse(report["proof_pass"])
            self.assertEqual(report["result"], "failed")
            self.assertEqual(report["cleanup"]["exit_code"], -1)

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
        for path in prove.manifest_paths():
            manifest = json.loads(path.read_text())
            help_text = subprocess.check_output([sys.executable, str(prove.ROOT / "scripts/prove.py"), "--help"], text=True)
            self.assertIn(manifest["name"], help_text)
            if manifest["name"] == "go-bindings":
                self.assertEqual(manifest["source_commit"], "3fb9e8abab0c9f5833f0c154140ceef009fea02a")
                self.assertEqual(len(manifest["expected_tests"]), 4)
                help_text = subprocess.check_output([sys.executable, str(prove.ROOT / "scripts/prove.py"), "--help"], text=True)
                self.assertIn("go-bindings", help_text)
                continue
            for spec in manifest["commands"]:
                with self.subTest(experiment=manifest["name"], runner=spec["runner"]):
                    self.assertTrue(prove.command(spec))

    def test_owner_manager_command_is_exact(self):
        spec = {"runner":"s3-owner-manager", "filter":"TestS3OwnerManager", "exact":True, "expected_tests":["TestS3OwnerManager"]}
        self.assertEqual(prove.command(spec), [sys.executable, "scripts/owner-manager-proof.py"])
        for changed in ({"filter":"Other"}, {"exact":False}):
            with self.assertRaises(ValueError):
                prove.command({**spec, **changed})

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
        self.assertEqual(prove.command({"runner": "cargo-build-node"}), ["cargo", "build", "--manifest-path", "test/compatibility/rust/Cargo.toml", "--target-dir", "target", "--locked", "-p", "xenon-node"])
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

    def test_go_owner_commands_and_result_family(self):
        self.assertEqual(prove.command({"runner": "go-node-build"}), [sys.executable, "scripts/build-go-node.py"])
        spec = {"runner": "go-test-node", "filter": "TestGoOwnerRecoveryAndReplay", "exact": True, "expected_tests": ["TestGoOwnerRecoveryAndReplay"]}
        self.assertIn("./internal/node", prove.command(spec))
        with self.assertRaises(ValueError):
            prove.command({**spec, "filter": "TestOtherPackage"})
        package = "github.com/0x63616c/xenon/internal/node"
        events = [{"Action": "pass", "Test": spec["filter"], "Package": package}, {"Action": "pass", "Package": package}]
        output = "\n".join(json.dumps(event) for event in events)
        prove.verify_go_tests(output, spec["expected_tests"], package)
        with self.assertRaises(ValueError):
            prove.verify_go_tests(output, spec["expected_tests"])

    def test_timeout_is_failure(self):
        code, _, expired = prove.run_process([sys.executable, '-c', 'import time; time.sleep(10)'], 0.05, os.environ.copy(), Path.cwd())
        self.assertTrue(expired)
        self.assertNotEqual(code, 0)


if __name__ == '__main__':
    unittest.main()
