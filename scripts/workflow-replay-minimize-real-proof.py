#!/usr/bin/env python3
"""Prove saved-input replay and fingerprint-preserving reduction on a real process."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid


SCHEMA = 1
FIXTURE_VERSION = "xenon-controlled-real-v1"
FINGERPRINT = {"invariant": "acknowledged_write", "mechanism": "missing_after_recovery"}
MAX_PROPOSALS = 32
ATTEMPTS_PER_CANDIDATE = 3
ATTEMPT_TIMEOUT_SECONDS = 10
GATE_TIMEOUT_SECONDS = 30
SCRIPT = Path(__file__).resolve()
ROOT = SCRIPT.parents[1]


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest_bytes(value):
    return hashlib.sha256(value).hexdigest()


def digest(path):
    return digest_bytes(path.read_bytes())


def save(path, value):
    path.write_bytes(json.dumps(value, indent=2, sort_keys=True).encode() + b"\n")


def scenario():
    actions = [{"id": "act_seed", "kind": "seed"}]
    previous = "act_seed"
    for index in range(20):
        item = {"id": f"act_noise_{index:02d}", "kind": "noise", "after": [previous]}
        actions.append(item)
        previous = item["id"]
    actions.extend([
        {"id": "act_write", "kind": "write", "after": [previous]},
        {"id": "act_ack", "kind": "ack", "after": ["act_write"]},
        {"id": "act_recover", "kind": "recover", "after": ["act_ack"]},
        {"id": "act_audit", "kind": "audit", "after": ["act_recover"]},
    ])
    return {
        "schema": SCHEMA,
        "fixture_version": FIXTURE_VERSION,
        "case_id": "case_01K5REALREPLAY00000000000",
        "actions": actions,
        "faults": [
            {"id": "flt_01K5DROP000000000000000", "kind": "drop_after_ack", "at": "act_ack"},
            {"id": "flt_01K5LATENCY000000000000", "kind": "observe_latency", "at": "act_noise_03"},
            {"id": "flt_01K5REORDER000000000000", "kind": "observe_reorder", "at": "act_noise_17"},
        ],
    }


def validate(value):
    if value.get("schema") != SCHEMA or value.get("fixture_version") != FIXTURE_VERSION:
        raise ValueError("incompatible saved scenario")
    actions = value.get("actions")
    faults = value.get("faults")
    if not isinstance(actions, list) or not isinstance(faults, list):
        raise ValueError("invalid saved scenario")
    ids = [item.get("id") for item in actions]
    if len(ids) != len(set(ids)) or any(not isinstance(item, str) for item in ids):
        raise ValueError("invalid action identities")
    known = set(ids)
    for item in actions:
        if any(parent not in known for parent in item.get("after", [])):
            raise ValueError("dangling action dependency")
    if any(fault.get("at") not in known for fault in faults):
        raise ValueError("dangling fault trigger")


def preflight(fixture, expected_version):
    marker = fixture / "fixture.json"
    if not marker.is_file():
        raise ValueError("fixture marker missing")
    value = json.loads(marker.read_text())
    if value != {"version": expected_version}:
        raise ValueError("incompatible fixture")
    extras = [path.name for path in fixture.iterdir() if path.name != marker.name]
    if extras:
        raise ValueError("fixture is nonempty")


def worker(artifact, fixture, receipt):
    value = json.loads(artifact.read_text())
    validate(value)
    preflight(fixture, value["fixture_version"])
    state = fixture / "state"
    state.mkdir()
    volatile = False
    acknowledged = False
    durable = False
    observations = []
    faults = {item["at"]: item for item in value["faults"]}
    for action in value["actions"]:
        kind = action["kind"]
        if kind == "write":
            volatile = True
        elif kind == "ack":
            if not volatile:
                raise ValueError("ack without write")
            acknowledged = True
            (state / "acknowledged").write_text("op_01K5REALWRITE00000000000\n")
        elif kind == "recover":
            if volatile:
                (state / "durable").write_text("op_01K5REALWRITE00000000000\n")
            volatile = False
            durable = (state / "durable").exists()
        elif kind == "audit":
            observations.append({"acknowledged": acknowledged, "durable_after_recovery": durable})
        fault = faults.get(action["id"])
        if fault:
            observations.append({"fault_id": fault["id"], "trigger": action["id"], "kind": fault["kind"]})
            if fault["kind"] == "drop_after_ack":
                volatile = False
    observed = FINGERPRINT if acknowledged and not durable and any(a["kind"] == "audit" for a in value["actions"]) else None
    result = {
        "schema": 1,
        "fixture_version": FIXTURE_VERSION,
        "artifact_sha256": digest(artifact),
        "actions_executed": len(value["actions"]),
        "faults_triggered": len([o for o in observations if "fault_id" in o]),
        "observations": observations,
        "fingerprint": observed,
    }
    save(receipt, result)
    return 23 if observed else 0


def fresh_fixture(parent, name, version=FIXTURE_VERSION):
    path = parent / name
    path.mkdir()
    save(path / "fixture.json", {"version": version})
    return path


def run_once(artifact, root, label):
    fixture = fresh_fixture(root, label + "-fixture")
    receipt = root / (label + "-receipt.json")
    process = subprocess.run(
        [sys.executable, str(SCRIPT), "--worker", "--artifact", str(artifact),
         "--fixture", str(fixture), "--receipt", str(receipt)],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=ATTEMPT_TIMEOUT_SECONDS,
    )
    if not receipt.is_file():
        raise RuntimeError("controlled fixture produced no receipt: " + process.stderr.decode(errors="replace"))
    result = json.loads(receipt.read_text())
    result["exit_code"] = process.returncode
    result["receipt"] = receipt.name
    result["receipt_sha256"] = digest(receipt)
    return result


def qualified(artifact, root, label):
    attempts = [run_once(artifact, root, f"{label}-{index}") for index in range(ATTEMPTS_PER_CANDIDATE)]
    accepted = all(item["exit_code"] == 23 and item["fingerprint"] == FINGERPRINT for item in attempts)
    return accepted, attempts


def repair(value):
    value = json.loads(json.dumps(value))
    present = {item["id"] for item in value["actions"]}
    value["actions"] = [dict(item, after=[parent for parent in item.get("after", []) if parent in present]) for item in value["actions"]]
    value["faults"] = [item for item in value["faults"] if item["at"] in present]
    return value


def size(value):
    return len(value["actions"]), len(value["faults"]), len(canonical(value))


def prove(evidence):
    evidence.mkdir(parents=True, exist_ok=False)
    original = evidence / "original.json"
    save(original, scenario())
    original_hash = digest(original)
    report = {"schema": 1, "gate": "workflow-replay-minimize-real", "result": "failed", "proof_pass": False,
              "source_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "source_dirty": bool(subprocess.check_output(["git", "status", "--porcelain=v1", "--untracked-files=no"], cwd=ROOT, text=True).strip()),
              "tool_sha256": digest(SCRIPT),
              "original": original.name, "original_sha256": original_hash, "attempts": [], "controls": []}
    try:
        deadline = time.monotonic() + GATE_TIMEOUT_SECONDS
        initial_ok, initial_attempts = qualified(original, evidence, "original")
        report["attempts"].append({"candidate": "original", "accepted": initial_ok, "runs": initial_attempts})
        if not initial_ok:
            raise RuntimeError("original controlled failure was not stable three of three")
        passing = json.loads(original.read_text())
        passing["faults"] = []
        passing_path = evidence / "passing-control.json"
        save(passing_path, passing)
        passing_result = run_once(passing_path, evidence, "passing-control")
        if passing_result["exit_code"] != 0 or passing_result["fingerprint"] is not None:
            raise RuntimeError("saved no-fault passing replay failed")
        report["controls"].append({"name": "saved-passing-replay", "result": passing_result})
        best = json.loads(original.read_text())
        proposals = 0
        # Deterministic order: irrelevant actions, then irrelevant faults. Repair
        # dependencies before every real execution.
        for identity in [item["id"] for item in best["actions"] if item["kind"] == "noise"] + [item["id"] for item in best["faults"]]:
            proposals += 1
            if proposals > MAX_PROPOSALS or time.monotonic() >= deadline:
                raise RuntimeError("real minimization budget exhausted")
            candidate = json.loads(json.dumps(best))
            candidate["actions"] = [item for item in candidate["actions"] if item["id"] != identity]
            candidate["faults"] = [item for item in candidate["faults"] if item["id"] != identity]
            candidate = repair(candidate)
            path = evidence / f"candidate-{proposals:03d}.json"
            save(path, candidate)
            ok, attempts = qualified(path, evidence, f"candidate-{proposals:03d}")
            report["attempts"].append({"candidate": path.name, "accepted": ok, "runs": attempts})
            if ok and size(candidate) < size(best):
                best = candidate
        minimized = evidence / "minimized.json"
        save(minimized, best)
        # A different failure must not qualify as the target.
        different = json.loads(json.dumps(best))
        different["actions"] = [item for item in different["actions"] if item["kind"] != "audit"]
        different_path = evidence / "different-failure-control.json"
        save(different_path, repair(different))
        different_result = run_once(different_path, evidence, "different-control")
        if different_result["fingerprint"] == FINGERPRINT or different_result["exit_code"] == 23:
            raise RuntimeError("different-failure control qualified")
        report["controls"].append({"name": "different-failure-rejected", "result": different_result})
        # Both fixture controls are rejected before the marker can be mutated.
        for name, prepare in (
            ("nonempty", lambda path: (path / "prior-state").write_text("occupied\n")),
            ("incompatible", lambda path: save(path / "fixture.json", {"version": "other-v1"})),
        ):
            fixture = fresh_fixture(evidence, name + "-fixture")
            prepare(fixture)
            before = {p.name: digest(p) for p in fixture.iterdir() if p.is_file()}
            receipt = evidence / (name + "-receipt.json")
            process = subprocess.run([sys.executable, str(SCRIPT), "--worker", "--artifact", str(minimized),
                                      "--fixture", str(fixture), "--receipt", str(receipt)], cwd=ROOT,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=ATTEMPT_TIMEOUT_SECONDS)
            after = {p.name: digest(p) for p in fixture.iterdir() if p.is_file()}
            if process.returncode == 0 or receipt.exists() or before != after or (fixture / "state").exists():
                raise RuntimeError(name + " fixture was not rejected before mutation")
            report["controls"].append({"name": name + "-fixture-rejected-before-mutation", "exit_code": process.returncode})
        final_ok, final_attempts = qualified(minimized, evidence, "minimized-final")
        if not final_ok or not size(best) < size(scenario()) or digest(original) != original_hash:
            raise RuntimeError("minimized result did not preserve stable failure or original")
        report.update(result="passed", proof_pass=True, fingerprint=FINGERPRINT,
                      original_size=size(scenario()), minimized_size=size(best), proposals=proposals,
                      bounds={"max_proposals": MAX_PROPOSALS, "attempts_per_candidate": ATTEMPTS_PER_CANDIDATE,
                              "attempt_timeout_seconds": ATTEMPT_TIMEOUT_SECONDS, "gate_timeout_seconds": GATE_TIMEOUT_SECONDS},
                      minimized=minimized.name, minimized_sha256=digest(minimized), final_attempts=final_attempts,
                      qualification="generator-free real external-process replay; saved scheduling is not claimed deterministic")
    except BaseException as error:
        report["error"] = type(error).__name__ + ": " + str(error)
    report["files"] = {path.name: digest(path) for path in sorted(evidence.iterdir()) if path.is_file() and path.name != "result.json"}
    save(evidence / "result.json", report)
    print(json.dumps({"receipt": str(evidence / "result.json"), "result": report["result"]}))
    return 0 if report["proof_pass"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--evidence", type=Path)
    args = parser.parse_args()
    if args.worker:
        if not args.artifact or not args.fixture or not args.receipt:
            parser.error("worker requires artifact, fixture and receipt")
        return worker(args.artifact.resolve(), args.fixture.resolve(), args.receipt.resolve())
    evidence = args.evidence or ROOT / ".local/evidence" / (time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-real-replay-" + uuid.uuid4().hex[:8])
    return prove(evidence.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
