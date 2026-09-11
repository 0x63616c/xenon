#!/usr/bin/env python3
"""Run and verify the issue-119 real workflow-search acceptance gate."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]
PINS = ROOT / "test/scenarios/ministack/pins.json"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path):
    return json.loads(path.read_text())


def verify_file_map(directory, files):
    if not isinstance(files, dict) or not files:
        raise ValueError("child receipt has no evidence file map")
    for relative, expected in files.items():
        path = (directory / relative).resolve()
        if Path(relative).is_absolute() or not path.is_relative_to(directory.resolve()):
            raise ValueError("child evidence path escapes its directory")
        if not path.is_file() or sha(path) != expected:
            raise ValueError("child evidence hash mismatch: " + relative)


def verify_journey(path, revision, concurrency):
    receipt = read_json(path)
    if (receipt.get("schema") != 1 or receipt.get("result") != "journey-passed"
            or receipt.get("source_revision") != revision
            or receipt.get("image_source_revision") != revision
            or receipt.get("source_dirty") is not False
            or receipt.get("discovery") is not False
            or receipt.get("cleanup_errors") != []):
        raise ValueError(f"invalid concurrency-{concurrency} journey receipt")
    required = {
        "matching-clean-host-cli-native-and-dependencies",
        "sdk-result-and-complete-two-run-history",
        "four-real-nexus-echo-operations",
        "three-generated-cases-four-roots-history-observed",
        "inspect-equals-independent-frozen-authority",
        "down-idempotent-zero-owned-containers",
        "object-volume-preserved",
        "sentinel-survived",
        "unavailable-inspect-no-control",
    }
    if not required.issubset(set(receipt.get("assertions", []))):
        raise ValueError(f"concurrency-{concurrency} journey assertions missing")
    observed = receipt.get("workflow_observations", {})
    admission = observed.get("admission", {})
    peaks = observed.get("case_execution_interval_peak", {})
    if (observed.get("totals", {}).get("initial_roots") != 12
            or len(observed.get("initial_root_runs", [])) != 12
            or len(peaks) != 3 or set(peaks.values()) != {concurrency}
            or admission.get("observed_running_peak") != concurrency
            or admission.get("admitted_roots") != 12
            or admission.get("windows") != 12 // concurrency
            or not isinstance(admission.get("receipt_sha256"), str)):
        raise ValueError(f"concurrency-{concurrency} observations mismatch")
    if observed.get("limitations"):
        raise ValueError(f"concurrency-{concurrency} oracle remains explicitly incomplete")
    verify_file_map(path.parent, receipt.get("files"))
    return receipt


SMOKE_EVENTS = {
    "omes-running-observed-before-join",
    "join-started-during-active-sdk-and-omes-workflows",
    "joined-agent-served-assigned-partition",
    "SIGKILL-during-running-omes-execution",
    "failed-member-evicted-and-rebalanced",
    "omes-and-ui-passed-through-unified-endpoint",
    "identical-histories-after-cold-recovery",
    "omes-visibility-and-ui-recovered-after-cold-restart",
}


def verify_smoke(path, revision):
    evidence = path.parent
    receipt = read_json(path)
    events = [event.get("event") for event in receipt.get("events", [])]
    if (receipt.get("schema") != 1 or receipt.get("status") != "component-passed"
            or receipt.get("revision") != revision or receipt.get("dirty") is not False
            or receipt.get("development") is not False or receipt.get("profile") != "smoke"
            or receipt.get("cleanup_errors", []) != []
            or not SMOKE_EVENTS.issubset(events)
            or events.count("cold-agent-healthy") != 3):
        raise ValueError("invalid real failure smoke receipt")
    tracked = receipt.get("tracked_input_sha256", {})
    if not tracked:
        raise ValueError("smoke receipt has no tracked input hashes")
    for relative, expected in tracked.items():
        source = ROOT / relative
        if not source.is_file() or sha(source) != expected:
            raise ValueError("smoke tracked input hash mismatch: " + relative)
    for name, expected in receipt.get("log_sha256", {}).items():
        log = evidence / name
        if not log.is_file() or sha(log) != expected:
            raise ValueError("smoke log hash mismatch: " + name)
    if not receipt.get("log_sha256") or not receipt.get("binaries"):
        raise ValueError("smoke receipt is missing output hashes")
    for name, expected in receipt["binaries"].items():
        binary = ROOT / ".local/bin" / name
        if not binary.is_file() or sha(binary) != expected:
            raise ValueError("smoke binary hash mismatch: " + name)
    native = receipt.get("native_build", {})
    library = ROOT / native.get("shared_library", "missing")
    if not library.is_file() or sha(library) != native.get("shared_library_sha256"):
        raise ValueError("smoke native library hash mismatch")
    return receipt


def run_gate(evidence):
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    dirty = subprocess.check_output(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=ROOT, text=True
    ).strip()
    report = {
        "schema": 1,
        "gate": "workflow-search-real",
        "result": "failed",
        "proof_pass": False,
        "acceptance_pass": False,
        "source_revision": revision,
        "source_dirty": bool(dirty),
        "commands": [],
        "children": [],
    }
    evidence.mkdir(parents=True, exist_ok=False)

    def command(argv, timeout):
        number = len(report["commands"]) + 1
        log = evidence / f"command-{number}.log"
        started = time.monotonic()
        with log.open("xb") as stream:
            result = subprocess.run(argv, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT,
                                    timeout=timeout, env=os.environ.copy())
        record = {"argv": list(map(str, argv)), "exit_code": result.returncode,
                  "elapsed_seconds": round(time.monotonic() - started, 3),
                  "output": log.name, "output_sha256": sha(log)}
        report["commands"].append(record)
        if result.returncode:
            raise RuntimeError(f"command {number} failed; see {log.name}")

    try:
        if dirty:
            raise ValueError("clean committed checkout required")
        pins = read_json(PINS)
        omes = evidence / "omes-source"
        image = evidence / "image"
        generator = evidence / "generator"
        binaries = evidence / "bin"
        binaries.mkdir()
        command(["git", "clone", "--no-checkout", pins["omes"]["repository"], str(omes)], 900)
        command(["git", "-C", str(omes), "checkout", "--detach", pins["omes"]["commit"]], 120)
        command([sys.executable, "scripts/build-go-node.py"], 900)
        command([sys.executable, "scripts/build-dev-fixture.py", "--evidence", str(image)], 3600)
        command([sys.executable, "scripts/prepare-workflow-generator.py", "--source", str(omes),
                 "--output", str(generator)], 1800)
        for name in ("xenon-omes-oracle", "xenon-admission-proxy"):
            command(["go", "build", "-o", str(binaries / name), "./cmd/" + name], 600)
        native = read_json(ROOT / ".local/go-node-build.json")
        library = ROOT / native["shared_library"]
        cli = ROOT / ".local/bin/xenon"
        children = []
        for concurrency in (4, 1):
            destination = evidence / f"journey-c{concurrency}"
            argv = [sys.executable, "scripts/dev-journey.py", "--cli", str(cli),
                    "--native-library", str(library), "--build-receipt", str(image / "build.json"),
                    "--fixture", str(image / "fixture.json"), "--evidence", str(destination),
                    "--search-bundle", str(generator), "--history-oracle", str(binaries / "xenon-omes-oracle"),
                    "--admission-proxy", str(binaries / "xenon-admission-proxy"),
                    "--workflow-concurrency", str(concurrency)]
            command(argv, 1800)
            child = verify_journey(destination / "journey.json", revision, concurrency)
            children.append((f"concurrency-{concurrency}", destination / "journey.json", child))
        smoke = evidence / "agent-smoke"
        command([sys.executable, "scripts/agent-smoke.py", "--profile", "smoke", "--evidence", str(smoke)], 1800)
        children.append(("failure-smoke", smoke / "result.json", verify_smoke(smoke / "result.json", revision)))
        if children[0][2]["workflow_observations"]["totals"] != children[1][2]["workflow_observations"]["totals"]:
            raise ValueError("concurrency variants executed different workload graphs")
        for name, path, _ in children:
            report["children"].append({"name": name, "receipt": str(path.relative_to(evidence)),
                                       "receipt_sha256": sha(path), "receipt_verified": True})
        if subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip() != revision:
            raise ValueError("source revision changed during gate")
        if subprocess.check_output(["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=ROOT, text=True).strip():
            raise ValueError("source changed during gate")
        report.update(result="passed", proof_pass=True, acceptance_pass=True)
    except BaseException as error:
        report["error"] = type(error).__name__ + ": " + str(error)
    finally:
        report["input_sha256"] = {
            relative: sha(ROOT / relative) for relative in (
                "scripts/workflow-search-real-proof.py", "scripts/dev-journey.py",
                "scripts/workflow_journey.py", "scripts/agent-smoke.py",
                "scripts/build-dev-fixture.py", "scripts/prepare-workflow-generator.py",
                "test/scenarios/acceptance/manifests/workflow-search-real.json",
                "test/scenarios/ministack/pins.json",
            )
        }
        report["platform"] = platform.platform()
        (evidence / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({"receipt": str(evidence / "result.json"), "result": report["result"]}), flush=True)
    return 0 if report["proof_pass"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path)
    args = parser.parse_args()
    destination = args.evidence or ROOT / ".local/evidence" / (
        time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-workflow-search-real-" + uuid.uuid4().hex[:8]
    )
    return run_gate(destination.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
