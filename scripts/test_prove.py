import os
from contextlib import ExitStack
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


    def run_cleanup_case(self, name, phase, cleanup_mode):
        manifest = json.loads((prove.ROOT / f"experiments/{name}.json").read_text())
        # Exercise main's receipt handling without invoking Docker or native tools.
        manifest.update(inputs=[], required_tool_prefixes={})
        dirty = "M tracked" if phase == "setup" else ""
        cleanup_result = {"failure": (23, "cleanup failed", False),
                          "timeout": (0, "cleanup timed out", True),
                          "success": (0, "", False)}
        with tempfile.TemporaryDirectory() as folder, ExitStack() as stack:
            root = Path(folder).resolve()
            (root / "experiments").mkdir()
            (root / f"experiments/{name}.json").write_text(json.dumps(manifest))
            (root / "scripts").mkdir()
            (root / "scripts/prove.py").write_text("# fixture runner input\n")
            commands = {tuple(prove.command(spec)): spec for spec in manifest["commands"]}

            def run(argv, timeout, env, cwd):
                if argv[:2] == ["docker", "compose"]:
                    self.assertEqual(timeout, 60)
                    self.assertEqual(argv[-2:], ["down", "--volumes"])
                    if cleanup_mode == "exception":
                        raise FileNotFoundError("docker unavailable")
                    return cleanup_result[cleanup_mode]
                spec = commands.get(tuple(argv))
                if spec is None:
                    return 0, "fixture tool version", False
                if phase == "test":
                    return 17, "original test failure\n", False
                expected = spec["expected_tests"]
                if spec["runner"] == "s3-directory":
                    package = "github.com/0x63616c/xenon/internal/directory"
                    events = [{"Action": "pass", "Test": test, "Package": package} for test in expected]
                    return 0, "\n".join(json.dumps(event) for event in [*events, {"Action": "pass", "Package": package}]), False
                return 0, "".join(f"test {test} ... ok\n" for test in expected) + f"test result: ok. {len(expected)} passed; 0 failed; 0 ignored;", False

            stack.enter_context(patch.object(prove, "ROOT", root))
            stack.enter_context(patch.object(sys, "argv", ["prove.py", name]))
            stack.enter_context(patch.object(prove, "cargo_configs", return_value=[]))
            stack.enter_context(patch.object(prove.subprocess, "check_output", side_effect=["a" * 40, dirty, "a" * 40, dirty]))
            stack.enter_context(patch.object(prove, "run_process", side_effect=run))
            if name == "crash" and cleanup_mode == "exception":
                # Raise outside cleanup_crash's own subprocess exception guard.
                stack.enter_context(patch.object(prove, "cleanup_crash", side_effect=ValueError("crash cleanup exception")))
            expected_pass = phase == "pass" and cleanup_mode == "success"
            self.assertEqual(prove.main(), 0 if expected_pass else 1)
            receipts = list((root / ".local/evidence").glob("*/result.json"))
            self.assertEqual(len(receipts), 1)
            report = json.loads(receipts[0].read_text())
            self.assertEqual(report["schema"], 1)
            self.assertEqual(report["result"], "passed" if expected_pass else "failed")
            self.assertEqual(report["proof_pass"], expected_pass)
            if phase == "setup":
                self.assertEqual(report["commands"], [])
                self.assertEqual(report["error"], "checkout is dirty; commit inputs or explicitly use --allow-dirty for development")
            elif phase == "test":
                self.assertEqual(len(report["commands"]), 1)
                self.assertEqual(report["error"], "command 1 failed (exit=17, timeout=False); see command-1.log")
                log = receipts[0].parent / report["commands"][0]["output"]
                self.assertEqual(log.read_text(), "original test failure\n")
                self.assertEqual(prove.digest(log), report["commands"][0]["output_sha256"])
            else:
                self.assertEqual(len(report["commands"]), len(manifest["commands"]))
                if cleanup_mode == "success":
                    self.assertNotIn("error", report)
                else:
                    self.assertEqual(report["error"], "scoped Compose cleanup failed" if name == "crash" else "directory cleanup failed")
            cleanup = report["cleanup"]
            if cleanup_mode == "exception":
                self.assertEqual(cleanup["exit_code"], -1)
                self.assertEqual(cleanup["error"], "crash cleanup exception" if name == "crash" else "docker unavailable")
            else:
                self.assertEqual(cleanup["exit_code"], cleanup_result[cleanup_mode][0])
            self.assertEqual(cleanup["timed_out"], cleanup_mode == "timeout")

    def test_setup_failure_survives_cleanup_in_every_branch(self):
        for name in ("go-shard-compat", "directory", "owner-manager", "maintenance", "s3-meter", "cas-loss", "process-cut", "crash"):
            for mode in ("failure", "timeout", "exception", "success"):
                with self.subTest(experiment=name, cleanup=mode):
                    self.run_cleanup_case(name, "setup", mode)

    def test_test_failure_survives_cleanup_and_keeps_command_evidence(self):
        for name in ("directory", "crash"):
            for mode in ("failure", "timeout", "exception", "success"):
                with self.subTest(experiment=name, cleanup=mode):
                    self.run_cleanup_case(name, "test", mode)

    def test_cleanup_alone_fails_and_success_still_passes(self):
        for name in ("directory", "crash"):
            for mode in ("failure", "timeout", "exception", "success"):
                with self.subTest(experiment=name, cleanup=mode):
                    self.run_cleanup_case(name, "pass", mode)

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
