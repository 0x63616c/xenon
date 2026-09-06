#!/usr/bin/env python3
"""Pinned partition Service + S3 registry + native SlateDB composition proof."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import queue
import threading
import subprocess
import sys
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent
COMPOSE = "test/scenarios/integration/native-engine/compose.yaml"
TESTS = {"TestPartitionNativeCatalogReplay", "TestPartitionNativeAcknowledgedMovement", "TestPartitionNativeObsoleteOpenCompletion", "TestPartitionNativeShardReplay", "TestPartitionNativeLateOpenFencesCurrent"}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save_report(path, report):
    with path.open("w") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())


def execute(argv, env, timeout=60, log=None, failfast_json=False, on_failure=None, cleanup_errors=None):
    """Stream evidence; latch the first cause before bounded process containment."""
    stream = log.open("w") if log is not None else None
    try:
        child = subprocess.Popen(argv, cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, start_new_session=True)
    except BaseException:
        if stream is not None:
            stream.close()
        raise
    messages, lines = queue.Queue(), []
    primary = None
    failures = []

    def read():
        try:
            for line in child.stdout:
                lines.append(line)
                if stream is not None:
                    stream.write(line)
                    stream.flush()
                messages.put(line)
        except BaseException as error:
            messages.put(error)
        finally:
            messages.put(None)

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    deadline = time.monotonic() + timeout
    try:
        eof = False
        while not eof or child.poll() is None:
            if time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired(argv, timeout)
            try:
                line = messages.get(timeout=min(.05, max(.001, deadline-time.monotonic())))
            except queue.Empty:
                continue
            if line is None:
                eof = True
                continue
            if isinstance(line, BaseException):
                raise line
            if failfast_json and line.startswith("{"):
                event = json.loads(line)
                if event.get("Action") in ("fail", "skip"):
                    raise RuntimeError("Go test " + event["Action"] + ": " + event.get("Test", event.get("Package", "unknown")))
        if child.wait(timeout=2):
            raise RuntimeError(f"command failed {argv!r}: {''.join(lines)}")
    except BaseException as error:
        primary = error
        try:
            if stream is not None:
                stream.flush()
                os.fsync(stream.fileno())
            if on_failure is not None:
                on_failure(error)
        except BaseException as evidence_error:
            failures.append(f"persist primary failure: {evidence_error}")
        raise
    finally:
        def signal_group(sig):
            try:
                os.killpg(child.pid, sig)
            except ProcessLookupError:
                pass  # process/group exited between observation and signal
            except BaseException as error:
                failures.append(f"signal group {child.pid}: {error}")
        if primary is not None:
            signal_group(signal.SIGTERM)
        try:
            child.wait(timeout=2)
        except subprocess.TimeoutExpired:
            signal_group(signal.SIGKILL)
            try:
                child.wait(timeout=5)
            except BaseException as error:
                failures.append(f"reap child {child.pid}: {error}")
        except BaseException as error:
            failures.append(f"reap child {child.pid}: {error}")
        reader.join(timeout=2)
        if reader.is_alive():
            signal_group(signal.SIGKILL)
            reader.join(timeout=2)
        if reader.is_alive():
            failures.append(f"output reader for child {child.pid} remains active")
        else:
            child.stdout.close()
            if stream is not None:
                try:
                    stream.flush()
                    os.fsync(stream.fileno())
                except BaseException as error:
                    failures.append(f"persist child output: {error}")
                finally:
                    stream.close()
        if cleanup_errors is not None:
            cleanup_errors.extend(failures)
        if failures and primary is None:
            raise RuntimeError("; ".join(failures))
    return ''.join(lines).strip()


def verify_inputs(report, env):
    if execute(["git", "rev-parse", "HEAD"], env) != report["source_revision"]:
        raise RuntimeError("source revision changed during proof")
    if execute(["git", "status", "--porcelain=v1", "--untracked-files=all"], env) != report["dirty_status"]:
        raise RuntimeError("source checkout changed during proof")
    for name, expected in report["input_sha256"].items():
        if digest(ROOT / name) != expected:
            raise RuntimeError("input changed during proof: " + name)
    native = report["native_build"]
    if digest(ROOT / native["shared_library"]) != native["shared_library_sha256"]:
        raise RuntimeError("native library changed during proof")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-dirty", action="store_true", help="development only; evidence records dirty input hashes")
    args = parser.parse_args()
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
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
              "limitations": ["MinIO is not real AWS qualification", "Two partition services in one test process, not cmd/xenon or embedded Temporal acceptance", "Coordinator replacement is an explicit production control CAS; no election failure detector or process crash is simulated", "Real Rust/OS/S3 timing, not deterministic simulation", "Native cases cover shard, cluster, Nexus and namespace persistence families; not all persistence families or SDK/runtime acceptance"]}
    report_path = evidence / "report.json"
    def primary_failure(error):
        if report["primary_failure"] is None:
            report["primary_failure"] = str(error)
        report["result"] = "failed"
        save_report(report_path, report)
    def command(argv, env, **kwargs):
        return execute(argv, env, on_failure=primary_failure,
                       cleanup_errors=report["cleanup_failures"], **kwargs)
    save_report(report_path, report)
    started = False
    try:
        report["source_revision"] = command(["git", "rev-parse", "HEAD"], env)
        report["dirty_status"] = command(["git", "status", "--porcelain=v1", "--untracked-files=all"], env)
        if report["dirty_status"] and not args.allow_dirty:
            raise RuntimeError("commit scenario inputs before proof; --allow-dirty is development only")
        paths = [ROOT / name for name in ("go.mod", "go.sum", "rust-toolchain.toml", "tools/slatedb-native.json", "scripts/build-go-node.py", "scripts/prove.py", COMPOSE)]
        for folder in (HERE, ROOT / "internal/partitions", ROOT / "internal/cluster", ROOT / "internal/registry", ROOT / "internal/identity", ROOT / "internal/persistence", ROOT / "internal/replay", ROOT / "gen/xenon/v1"):
            paths.extend(p for p in folder.rglob("*") if p.is_file() and p.suffix in (".go", ".py", ".json", ".md"))
        report["input_sha256"] = {str(p.relative_to(ROOT)): digest(p) for p in sorted(set(paths))}
        report["versions"] = {"docker": execute(["docker", "version", "--format", "{{.Server.Version}}"], env),
                              "compose": execute(["docker", "compose", "version", "--short"], env)}
        if any(report["versions"][k] != cfg[k] for k in report["versions"]):
            raise RuntimeError("Docker/Compose version mismatch")
        (evidence / "case.json").write_text(json.dumps(cfg, indent=2) + "\n")
        save_report(report_path, report)
        print(f"Building pinned native library; evidence: {evidence}", flush=True)
        command([sys.executable, "scripts/build-go-node.py"], env, timeout=600, log=evidence / "build.log")
        report["native_build"] = json.loads((ROOT / ".local/go-node-build.json").read_text())
        lib = ROOT / ".local/slatedb-native-target/debug"
        env.update({"GOTOOLCHAIN": pins["go_toolchain"], "GOENV": "off", "GOWORK": "off", "GOFLAGS": "-mod=readonly", "CGO_ENABLED": "1", "CGO_LDFLAGS": "-L"+str(lib), "LD_LIBRARY_PATH": str(lib), "DYLD_LIBRARY_PATH": str(lib), "SLATEDB_UNIFFI_RUNTIME_THREADS": pins["runtime_threads"]})
        save_report(report_path, report)
        started = True
        command([*compose, "up", "-d", "--wait"], env)
        address = command([*compose, "port", "s3", "9000"], env)
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
        save_report(report_path, report)
        output = command(argv, env, timeout=cfg["test_timeout_seconds"]+10, log=evidence / "tests.jsonl", failfast_json=True)
        events = [json.loads(line) for line in output.splitlines() if line.startswith("{")]
        passed = {e["Test"] for e in events if e.get("Test") in TESTS and e["Action"] == "pass"}
        if passed != TESTS or any(e["Action"] in ("fail", "skip") for e in events):
            raise RuntimeError("required tests did not all execute and pass")
        report["passed_tests"] = sorted(passed)
        verify_inputs(report, env)
        report["inputs_unchanged"] = True
        report["result"] = "development-passed" if report["dirty_status"] else "passed"
    except BaseException as error:
        primary_failure(error)
    finally:
        if started:
            try:
                execute([*compose, "down", "--volumes", "--timeout", "5"], env, timeout=cfg["cleanup_timeout_seconds"], log=evidence / "cleanup.log", cleanup_errors=report["cleanup_failures"])
            except BaseException as error:
                report["cleanup_failures"].append(str(error))
            for resource, argv in (("containers", ["docker", "ps", "-aq"]), ("volumes", ["docker", "volume", "ls", "-q"]), ("networks", ["docker", "network", "ls", "-q"])):
                try:
                    remaining = execute([*argv, "--filter", "label=com.docker.compose.project="+project], env, timeout=cfg["cleanup_timeout_seconds"], cleanup_errors=report["cleanup_failures"])
                    if remaining:
                        raise RuntimeError(f"run-owned {resource} remain: {remaining}")
                except BaseException as error:
                    report["cleanup_failures"].append(str(error))
            if not report["cleanup_failures"]:
                report["cleanup"] = "passed"
        else:
            report["cleanup"] = "no resources started"
        if report["result"] in ("passed", "development-passed"):
            try:
                verify_inputs(report, env)
            except BaseException as error:
                primary_failure(error)
        if report["primary_failure"] or report["cleanup_failures"]:
            report["result"] = "failed"
        save_report(report_path, report)
    print(json.dumps({"result": report["result"], "evidence": str(evidence), "primary_failure": report["primary_failure"], "cleanup_failures": report["cleanup_failures"]}), flush=True)
    return 0 if report["result"] in ("passed", "development-passed") else 1


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise RuntimeError(f"partition scenario interrupted: {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    sys.exit(main())
