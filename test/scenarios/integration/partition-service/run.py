#!/usr/bin/env python3
"""Pinned partition Service + S3 registry + native SlateDB composition proof."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
COMPOSE = "test/scenarios/integration/native-engine/compose.yaml"
TESTS = {"TestPartitionNativeAcknowledgedMovement", "TestPartitionNativeObsoleteOpenCompletion"}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def execute(argv, env, timeout=60, log=None):
    child = subprocess.Popen(argv, cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, start_new_session=True)
    try:
        output, _ = child.communicate(timeout=timeout)
        if log is not None:
            log.write_text(output)
    except BaseException:
        os.killpg(child.pid, signal.SIGTERM)
        try:
            output, _ = child.communicate(timeout=2)
            if log is not None:
                log.write_text(output)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            output, _ = child.communicate(timeout=5)
            if log is not None:
                log.write_text(output)
        raise
    if child.returncode:
        raise RuntimeError(f"command failed {argv!r}: {output}")
    return output.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-dirty", action="store_true", help="development only; evidence records dirty input hashes")
    args = parser.parse_args()
    env = os.environ.copy()
    cfg = json.loads((HERE / "case.json").read_text())
    pins = json.loads((ROOT / "tools/slatedb-native.json").read_text())
    if cfg["schema"] != 1 or cfg["backend"] != "s3-emulator":
        raise RuntimeError("invalid scenario fixture")
    project = "xenon-partition-" + uuid.uuid4().hex[:12]
    evidence = ROOT / ".local/evidence" / project
    evidence.mkdir(parents=True)
    compose = ["docker", "compose", "--project-name", project, "-f", COMPOSE]
    report = {"schema": 1, "backend": cfg["backend"], "project": project, "result": "failed",
              "primary_failure": None, "cleanup_failures": [], "schedule": cfg["schedule"],
              "limitations": ["MinIO is not real AWS qualification", "Two partition services in one test process, not cmd/xenon or embedded Temporal acceptance", "Coordinator replacement is an explicit production control CAS; no election failure detector or process crash is simulated", "Real Rust/OS/S3 timing, not deterministic simulation", "Atomic application/outcome bytes exercise the native transaction seam, not Temporal replay semantics"]}
    started = False
    try:
        report["source_revision"] = execute(["git", "rev-parse", "HEAD"], env)
        report["dirty_status"] = execute(["git", "status", "--porcelain=v1", "--untracked-files=all"], env)
        if report["dirty_status"] and not args.allow_dirty:
            raise RuntimeError("commit scenario inputs before proof; --allow-dirty is development only")
        paths = [ROOT / name for name in ("go.mod", "go.sum", "rust-toolchain.toml", "tools/slatedb-native.json", "scripts/build-go-node.py", "scripts/prove.py", COMPOSE)]
        for folder in (HERE, ROOT / "internal/partitions", ROOT / "internal/cluster", ROOT / "internal/registry", ROOT / "internal/identity"):
            paths.extend(p for p in folder.rglob("*") if p.is_file() and p.suffix in (".go", ".py", ".json", ".md"))
        report["input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in sorted(set(paths))}
        report["versions"] = {"docker": execute(["docker", "version", "--format", "{{.Server.Version}}"], env),
                              "compose": execute(["docker", "compose", "version", "--short"], env)}
        if any(report["versions"][k] != cfg[k] for k in report["versions"]):
            raise RuntimeError("Docker/Compose version mismatch")
        (evidence / "case.json").write_text(json.dumps(cfg, indent=2) + "\n")
        print(f"Building pinned native library; evidence: {evidence}", flush=True)
        build = execute([sys.executable, "scripts/build-go-node.py"], env, timeout=600, log=evidence / "build.log")
        (evidence / "build.log").write_text(build + "\n")
        report["native_build"] = json.loads((ROOT / ".local/go-node-build.json").read_text())
        lib = ROOT / ".local/slatedb-native-target/debug"
        env.update({"GOTOOLCHAIN": pins["go_toolchain"], "GOENV": "off", "GOWORK": "off", "GOFLAGS": "-mod=readonly", "CGO_ENABLED": "1", "CGO_LDFLAGS": "-L"+str(lib), "LD_LIBRARY_PATH": str(lib), "DYLD_LIBRARY_PATH": str(lib), "SLATEDB_UNIFFI_RUNTIME_THREADS": pins["runtime_threads"]})
        started = True
        execute([*compose, "up", "-d", "--wait"], env)
        address = execute([*compose, "port", "s3", "9000"], env)
        if not address.startswith("127.0.0.1:"):
            raise RuntimeError("unexpected MinIO binding")
        endpoint = "http://" + address
        deadline = time.monotonic() + cfg["phase_timeout_seconds"]
        while True:
            try:
                with urllib.request.urlopen(endpoint + "/minio/health/live", timeout=2) as response:
                    if response.status == 200:
                        break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("MinIO readiness deadline")
                time.sleep(cfg["poll_milliseconds"] / 1000)
        env.update({"AWS_ACCESS_KEY_ID": "xenon-local", "AWS_SECRET_ACCESS_KEY": "xenon-local-test-only", "AWS_DEFAULT_REGION": "us-east-1", "AWS_ENDPOINT": endpoint, "AWS_ALLOW_HTTP": "true", "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false", "XENON_ENGINE_STORE": "s3://" + cfg["bucket"]})
        print("Running real registry/native partition movement and late completion scenarios", flush=True)
        argv = ["go", "test", "-race", "-failfast", "-json", "-count=1", "-timeout", str(cfg["test_timeout_seconds"])+"s", "./test/scenarios/integration/partition-service", "-run", "^TestPartitionNative"]
        report["test_command"] = argv
        output = execute(argv, env, timeout=cfg["test_timeout_seconds"]+10, log=evidence / "tests.jsonl")
        (evidence / "tests.jsonl").write_text(output + "\n")
        events = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
        passed = {e["Test"] for e in events if e.get("Test") in TESTS and e["Action"] == "pass"}
        if passed != TESTS or any(e["Action"] in ("fail", "skip") for e in events):
            raise RuntimeError("required tests did not all execute and pass")
        report["passed_tests"] = sorted(passed)
        report["result"] = "passed"
    except BaseException as error:
        report["primary_failure"] = str(error)
    finally:
        if started:
            try:
                execute([*compose, "down", "--volumes", "--timeout", "5"], env)
                for resource, argv in (("containers", ["docker", "ps", "-aq"]), ("volumes", ["docker", "volume", "ls", "-q"]), ("networks", ["docker", "network", "ls", "-q"])):
                    remaining = execute([*argv, "--filter", "label=com.docker.compose.project="+project], env)
                    if remaining:
                        raise RuntimeError(f"run-owned {resource} remain: {remaining}")
                report["cleanup"] = "passed"
            except BaseException as error:
                report["cleanup_failures"].append(str(error))
        else:
            report["cleanup"] = "no resources started"
        if report["primary_failure"] or report["cleanup_failures"]:
            report["result"] = "failed"
        (evidence / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"result": report["result"], "evidence": str(evidence), "primary_failure": report["primary_failure"], "cleanup_failures": report["cleanup_failures"]}), flush=True)
    return 0 if report["result"] == "passed" else 1


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise RuntimeError(f"partition scenario interrupted: {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    sys.exit(main())
