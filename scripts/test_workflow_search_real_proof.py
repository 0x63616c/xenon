import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


path = Path(__file__).with_name("workflow-search-real-proof.py")
spec = importlib.util.spec_from_file_location("workflow_search_real_proof", path)
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)


class ReceiptValidationTests(unittest.TestCase):
    revision = "a" * 40

    def journey(self, root, concurrency):
        (root / "detail.json").write_text("{}\n")
        assertions = [
            "matching-clean-host-cli-native-and-dependencies",
            "sdk-result-and-complete-two-run-history",
            "four-real-nexus-echo-operations",
            "three-generated-cases-four-roots-history-observed",
            "inspect-equals-independent-frozen-authority",
            "down-idempotent-zero-owned-containers",
            "object-volume-preserved",
            "sentinel-survived",
            "unavailable-inspect-no-control",
        ]
        receipt = {
            "schema": 1, "result": "journey-passed", "source_revision": self.revision,
            "image_source_revision": self.revision, "source_dirty": False, "discovery": False,
            "cleanup_errors": [], "assertions": assertions,
            "files": {"detail.json": proof.sha(root / "detail.json")},
            "workflow_observations": {
                "totals": {"initial_roots": 12, "child_runs": 7},
                "initial_root_runs": [{} for _ in range(12)],
                "case_execution_interval_peak": {str(i): concurrency for i in range(3)},
                "admission": {"observed_running_peak": concurrency, "admitted_roots": 12,
                              "windows": 12 // concurrency, "receipt_sha256": "b" * 64},
                "limitations": [],
            },
        }
        output = root / "journey.json"
        output.write_text(json.dumps(receipt))
        return output

    def test_journey_requires_receipt_content_and_hashed_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            receipt = self.journey(root, 4)
            proof.verify_journey(receipt, self.revision, 4)
            (root / "detail.json").write_text("changed\n")
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                proof.verify_journey(receipt, self.revision, 4)

    def test_journey_rejects_flag_only_and_wrong_concurrency(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            receipt = self.journey(root, 1)
            with self.assertRaisesRegex(ValueError, "observations mismatch"):
                proof.verify_journey(receipt, self.revision, 4)
            receipt.write_text(json.dumps({"schema": 1, "result": "journey-passed"}))
            with self.assertRaisesRegex(ValueError, "invalid concurrency"):
                proof.verify_journey(receipt, self.revision, 1)

    def test_smoke_requires_complete_fault_and_recovery_sequence(self):
        receipt = {
            "schema": 1, "status": "component-passed", "revision": self.revision,
            "dirty": False, "development": False, "profile": "smoke",
            "tracked_input_sha256": {"input": "digest"},
            "log_sha256": {"run.log": "digest"},
            "binaries": {"xenon": "digest"},
            "native_build": {"shared_library": ".local/native", "shared_library_sha256": "digest"},
            "events": [{"event": event} for event in sorted(proof.SMOKE_EVENTS)]
                      + [{"event": "cold-agent-healthy"}] * 3,
        }
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "input"
            source.write_text("source\n")
            (root / ".local/bin").mkdir(parents=True)
            (root / ".local/bin/xenon").write_text("binary\n")
            (root / ".local/native").write_text("native\n")
            (root / "run.log").write_text("log\n")
            receipt["tracked_input_sha256"]["input"] = proof.sha(source)
            receipt["log_sha256"]["run.log"] = proof.sha(root / "run.log")
            receipt["binaries"]["xenon"] = proof.sha(root / ".local/bin/xenon")
            receipt["native_build"]["shared_library_sha256"] = proof.sha(root / ".local/native")
            path = root / "result.json"
            path.write_text(json.dumps(receipt))
            with patch.object(proof, "ROOT", root):
                proof.verify_smoke(path, self.revision)
                receipt["events"] = receipt["events"][1:]
                path.write_text(json.dumps(receipt))
                with self.assertRaisesRegex(ValueError, "invalid real failure"):
                    proof.verify_smoke(path, self.revision)


if __name__ == "__main__":
    unittest.main()
