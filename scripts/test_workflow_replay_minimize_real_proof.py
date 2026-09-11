import importlib.util
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("workflow-replay-minimize-real-proof.py")
SPEC = importlib.util.spec_from_file_location("real_replay", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class RealReplayProofTests(unittest.TestCase):
    def test_gate_reduces_stable_real_failure_and_rejects_fixtures(self):
        with tempfile.TemporaryDirectory() as temporary:
            evidence = Path(temporary) / "evidence"
            self.assertEqual(MODULE.prove(evidence), 0)
            result = MODULE.json.loads((evidence / "result.json").read_text())
            self.assertTrue(result["proof_pass"])
            self.assertEqual(result["fingerprint"], MODULE.FINGERPRINT)
            self.assertLess(tuple(result["minimized_size"]), tuple(result["original_size"]))
            self.assertEqual(len(result["final_attempts"]), 3)
            self.assertEqual(
                {control["name"] for control in result["controls"]},
                {"saved-passing-replay", "different-failure-rejected", "nonempty-fixture-rejected-before-mutation", "incompatible-fixture-rejected-before-mutation"},
            )
            self.assertEqual(MODULE.digest(evidence / "original.json"), result["original_sha256"])


if __name__ == "__main__":
    unittest.main()
