#!/usr/bin/env python3
"""Execute declared process-kill barriers against an isolated local S3 emulator."""
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]


def execute(argv, env, timeout=120):
    result = subprocess.run(argv, cwd=ROOT, env=env, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"command failed: {argv!r}\n{result.stdout}\n{result.stderr}")
    return result.stdout


def events_until(worker, seconds):
    buffer = b""
    os.set_blocking(worker.stdout.fileno(), False)
    deadline = time.monotonic() + seconds
    with selectors.DefaultSelector() as selector:
        selector.register(worker.stdout, selectors.EVENT_READ)
        while time.monotonic() < deadline:
            if not selector.select(timeout=max(0, deadline-time.monotonic())):
                break
            chunk = os.read(worker.stdout.fileno(), 65536)
            if not chunk:
                raise RuntimeError("process exited before declared event")
            buffer += chunk
            if len(buffer) > 65536:
                raise RuntimeError("process event exceeds line bound")
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                yield json.loads(line)
    raise RuntimeError("declared fault barrier never observed")


def observe_barrier(worker, seconds, sequence):
    events, acknowledgements = [], []
    for event in events_until(worker, seconds):
        events.append(event)
        if event["event"] == "ack":
            acknowledgements.append(event["sequence"])
        elif event["event"] == "fault-ready":
            if event["sequence"] != sequence:
                raise RuntimeError("wrong fault sequence")
            return acknowledgements, events
        else:
            raise RuntimeError("unexpected worker event")


def scenario(binary, stage, manifest, env):
    # Keep the worker in the controller's process group so outer timeout cleanup
    # also kills it. The declared injection targets this child PID only.
    worker = subprocess.Popen([str(binary), "writer", stage], cwd=ROOT, env=env,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
    try:
        acknowledgements, events = observe_barrier(worker, manifest["worker_timeout_seconds"],
                                                  manifest["barrier_sequence"])
        worker.kill()
        if worker.wait(timeout=10) != -signal.SIGKILL:
            raise RuntimeError("writer did not terminate from injected SIGKILL")
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.wait(timeout=10)
        worker.stdout.close()
    expected = [1, 2, 3] if stage == "after-ack" else [1, 2]
    if acknowledgements != expected:
        raise RuntimeError(f"ack oracle mismatch {acknowledgements} != {expected}")
    verified = json.loads(execute([str(binary), "verify", stage, str(len(acknowledgements))], env,
                                 manifest["worker_timeout_seconds"]))
    if verified != {"event": "verified", "sequence": len(acknowledgements)}:
        raise RuntimeError(f"unexpected recovery verdict: {verified}")
    print(json.dumps({"stage": stage, "events": events, "signal": "SIGKILL", "verification": verified}), flush=True)


def main(pause_at_stack_ready=False):
    manifest = json.loads((ROOT / "experiments/crash.json").read_text())
    if manifest["endpoint"] != "http://127.0.0.1:19001" or manifest["backend"] != "s3-emulator":
        raise RuntimeError("local emulator profile required")
    env = {k: os.environ[k] for k in ("PATH", "HOME", "USER", "TMPDIR", "RUSTUP_HOME", "CARGO_HOME") if k in os.environ}
    env.update({"AWS_ACCESS_KEY_ID": "xenon-local", "AWS_SECRET_ACCESS_KEY": "xenon-local-test-only",
                "AWS_REGION": "us-east-1", "AWS_ENDPOINT": manifest["endpoint"], "AWS_ALLOW_HTTP": "true",
                "AWS_EC2_METADATA_DISABLED": "true", "AWS_CONFIG_FILE": os.devnull,
                "AWS_SHARED_CREDENTIALS_FILE": os.devnull, "XENON_PROBE_BACKEND": "s3",
                "XENON_PROBE_BUCKET": manifest["bucket"]})
    env.pop("AWS_SESSION_TOKEN", None)
    env.pop("AWS_PROFILE", None)
    execute(["cargo", "build", "--manifest-path", "test/compatibility/rust/Cargo.toml", "--target-dir", "target", "--locked", "-p", "slatedb-probe", "--bin", "crash-worker"], env, 300)
    binary = ROOT / "target/debug/crash-worker"
    project = os.environ.get("XENON_PROOF_PROJECT", "xenon-crash-" + uuid.uuid4().hex[:12])
    if not __import__("re").fullmatch(r"xenon-crash-[a-f0-9]{12}", project):
        raise RuntimeError("invalid scoped project identity")
    compose = ["docker", "compose", "--project-name", project, "-f", manifest["compose"]]
    print(json.dumps({"docker": execute(["docker", "version", "--format", "{{.Server.Version}}"], env).strip(),
                      "compose": execute(["docker", "compose", "version", "--short"], env).strip(),
                      "aws_cli": execute(["aws", "--version"], env).strip()}), flush=True)
    try:
        execute([*compose, "up", "-d", "--wait"], env)
        print(json.dumps({"event": "stack-ready", "project": project}), flush=True)
        if pause_at_stack_ready:
            signal.pause()  # Controlled cleanup fault; no worker has been started.
            raise RuntimeError("cleanup pause ended without a terminating signal")
        deadline = time.monotonic() + 30
        while True:
            try:
                with urllib.request.urlopen(manifest["endpoint"]+"/minio/health/live", timeout=2) as response:
                    if response.status == 200:
                        break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("local S3 emulator not ready")
                time.sleep(0.1)
        execute(["aws", "--endpoint-url", manifest["endpoint"], "s3api", "create-bucket", "--bucket", manifest["bucket"]], env)
        for stage in manifest["stages"]:
            env["XENON_PROBE_PREFIX"] = project+"/"+stage
            scenario(binary, stage, manifest, env)
            print(f"test crash::{stage.replace('-', '_')} ... ok", flush=True)
    finally:
        # This project and its volume were created solely for this declared run.
        execute([*compose, "down", "--volumes"], env)
    print(f"test result: ok. {len(manifest['stages'])} passed; 0 failed; 0 ignored;", flush=True)


def verify_cleanup():
    # Execute both graceful timeout handling and the outer runner's fallback after
    # an uncatchable controller kill. Resources have a uniquely scoped project.
    import prove
    env = {k: os.environ[k] for k in ("PATH", "HOME", "USER", "TMPDIR", "RUSTUP_HOME", "CARGO_HOME") if k in os.environ}
    project = os.environ.get("XENON_PROOF_PROJECT", "xenon-crash-" + uuid.uuid4().hex[:12])
    env["XENON_PROOF_PROJECT"] = project
    for fault in (signal.SIGTERM, signal.SIGKILL):
        child = subprocess.Popen([sys.executable, "scripts/crash-proof.py", "--pause-at-stack-ready"], cwd=ROOT, env=env,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
        observed = False
        try:
            for event in events_until(child, 120):
                if event.get("event") == "stack-ready":
                    observed = True
                    child.send_signal(fault)
                    break
            if not observed or child.wait(timeout=30) == 0:
                raise RuntimeError("cleanup injection was not observed")
        finally:
            if child.poll() is None:
                child.kill(); child.wait(timeout=10)
            child.stdout.close(); child.stderr.close()
            result = prove.cleanup_crash(project, env, ROOT)
            if result["exit_code"] or result["timed_out"]:
                raise RuntimeError("fallback cleanup failed")
        for kind in ("container", "volume"):
            # Authoritative label-filtered inventory; unrelated projects are untouched.
            remaining = execute(["docker", "ps", "-aq", "--filter", "label=com.docker.compose.project="+project] if kind == "container" else ["docker", "volume", "ls", "-q", "--filter", "label=com.docker.compose.project="+project], env)
            if remaining.strip():
                raise RuntimeError("scoped resources survived timeout cleanup")
        print(f"test crash::cleanup_{fault.name.lower()} ... ok", flush=True)
    print("test result: ok. 2 passed; 0 failed; 0 ignored;", flush=True)


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise RuntimeError(f"controller interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        if sys.argv[1:] == ["--check-cleanup"]:
            verify_cleanup()
        elif sys.argv[1:] in ([], ["--pause-at-stack-ready"]):
            main(bool(sys.argv[1:]))
        else:
            raise RuntimeError("unknown crash proof mode")
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
