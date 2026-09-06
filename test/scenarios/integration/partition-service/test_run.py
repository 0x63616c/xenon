import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("partition_proof_run", Path(__file__).with_name("run.py"))
run = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run)


class RunnerTests(unittest.TestCase):
    def child(self, body):
        return [sys.executable, "-c", body]

    def assert_reaped(self, log):
        pid = json.loads(log.read_text().splitlines()[0])["pid"]
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_failure_streams_before_containment_and_preempts_blocked_child(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "tests.jsonl"
            observed = []
            def latch(error):
                observed.append(str(error))
                self.assertIn('"Action": "fail"', log.read_text())
            started = time.monotonic()
            with self.assertRaisesRegex(RuntimeError, "Go test fail: invariant"):
                run.execute(self.child('import os,json,time; print(json.dumps({"pid":os.getpid(),"Action":"fail","Test":"invariant"}),flush=True); time.sleep(30)'),
                            os.environ.copy(), timeout=10, log=log, failfast_json=True, on_failure=latch)
            self.assertLess(time.monotonic()-started, 5)
            self.assertEqual(observed, ["Go test fail: invariant"])
            self.assert_reaped(log)

    def test_timeout_retains_output_and_reaps(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "child.log"
            with self.assertRaises(subprocess.TimeoutExpired):
                run.execute(self.child('import os,json,time; print(json.dumps({"pid":os.getpid()}),flush=True); time.sleep(30)'),
                            os.environ.copy(), timeout=.2, log=log)
            self.assert_reaped(log)

    def test_exit_race_does_not_mask_timeout_or_skip_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "child.log"
            kill = os.killpg
            def race(pid, sig):
                kill(pid, sig)
                raise ProcessLookupError("modeled group exit before signal")
            with mock.patch.object(run.os, "killpg", side_effect=race):
                with self.assertRaises(subprocess.TimeoutExpired):
                    run.execute(self.child('import os,json,time; print(json.dumps({"pid":os.getpid()}),flush=True); time.sleep(30)'),
                                os.environ.copy(), timeout=.2, log=log)
            self.assert_reaped(log)

    def test_secondary_signal_failure_preserves_primary(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "child.log"
            failures = []
            kill = os.killpg
            def fail_term(pid, sig):
                if sig == signal.SIGTERM:
                    raise PermissionError("modeled TERM failure")
                kill(pid, sig)
            with mock.patch.object(run.os, "killpg", side_effect=fail_term):
                with self.assertRaisesRegex(RuntimeError, "Go test fail: invariant"):
                    run.execute(self.child('import os,json,time; print(json.dumps({"pid":os.getpid(),"Action":"fail","Test":"invariant"}),flush=True); time.sleep(30)'),
                                os.environ.copy(), timeout=10, log=log, failfast_json=True, cleanup_errors=failures)
            self.assertTrue(any("modeled TERM failure" in error for error in failures))
            self.assert_reaped(log)

    def test_nonzero_child_cannot_pass_with_pass_event(self):
        with self.assertRaisesRegex(RuntimeError, "command failed"):
            run.execute(self.child('import sys; print(\'{"Action":"pass"}\',flush=True); sys.exit(7)'),
                        os.environ.copy(), failfast_json=True)

    def test_input_revision_and_native_mutation_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "input.go").write_text("original")
            (root / "native.so").write_text("native")
            report = {"source_revision": "rev1", "dirty_status": "", "input_sha256": {"input.go": run.digest(root / "input.go")},
                      "native_build": {"shared_library": "native.so", "shared_library_sha256": run.digest(root / "native.so")}}
            with mock.patch.object(run, "ROOT", root):
                with mock.patch.object(run, "execute", side_effect=["rev2"]):
                    with self.assertRaisesRegex(RuntimeError, "source revision changed"):
                        run.verify_inputs(report, {})
                with mock.patch.object(run, "execute", side_effect=["rev1", " M input.go"]):
                    with self.assertRaisesRegex(RuntimeError, "source checkout changed"):
                        run.verify_inputs(report, {})
                (root / "input.go").write_text("edited during build")
                with mock.patch.object(run, "execute", side_effect=["rev1", ""]):
                    with self.assertRaisesRegex(RuntimeError, "input changed"):
                        run.verify_inputs(report, {})
                (root / "input.go").write_text("original")
                (root / "native.so").write_text("replacement library")
                with mock.patch.object(run, "execute", side_effect=["rev1", ""]):
                    with self.assertRaisesRegex(RuntimeError, "native library changed"):
                        run.verify_inputs(report, {})

    def test_provenance_exists_before_build_failure(self):
        cfg = json.loads((run.HERE / "case.json").read_text())
        pins = (run.ROOT / "tools/slatedb-native.json").read_text()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            here = root / "test/scenarios/integration/partition-service"
            here.mkdir(parents=True)
            (here / "case.json").write_text(json.dumps(cfg))
            for name in ("go.mod", "go.sum", "rust-toolchain.toml", "tools/slatedb-native.json", "scripts/build-go-node.py", "scripts/prove.py", run.COMPOSE):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(pins if name == "tools/slatedb-native.json" else "fixture")
            def execute(argv, env, **kwargs):
                if argv[:2] == ["git", "rev-parse"]:
                    return "rev1"
                if argv[:2] == ["git", "status"]:
                    return ""
                if argv[:2] == ["docker", "version"]:
                    return cfg["docker"]
                if argv[:3] == ["docker", "compose", "version"]:
                    return cfg["compose"]
                if argv == [sys.executable, "scripts/build-go-node.py"]:
                    report = json.loads(next((root / ".local/evidence").glob("*/report.json")).read_text())
                    self.assertEqual(report["source_revision"], "rev1")
                    self.assertIn("go.mod", report["input_sha256"])
                    raise RuntimeError("modeled build failure")
                self.fail(f"unexpected command {argv}")
            with mock.patch.object(run, "ROOT", root), mock.patch.object(run, "HERE", here), mock.patch.object(run, "execute", side_effect=execute), mock.patch.object(sys, "argv", ["run.py"]):
                self.assertEqual(run.main(), 1)
            report = json.loads(next((root / ".local/evidence").glob("*/report.json")).read_text())
            self.assertEqual(report["primary_failure"], "modeled build failure")
            self.assertEqual(report["cleanup"], "no resources started")


if __name__ == "__main__":
    unittest.main()
