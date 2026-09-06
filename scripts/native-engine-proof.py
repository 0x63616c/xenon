#!/usr/bin/env python3
"""Qualify the native seam using the repository's pinned, scoped MinIO setup."""
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]

def execute(argv, env, timeout=60):
    r = subprocess.run(argv, cwd=ROOT, env=env, text=True, capture_output=True, timeout=timeout)
    if r.returncode:
        raise RuntimeError(f"command failed {argv!r}: {r.stdout}\n{r.stderr}")
    return r.stdout.strip()

def main():
    cfg = json.loads((ROOT / "test/scenarios/integration/native-engine/case.json").read_text())
    if cfg["schema"] != 1 or cfg["backend"] != "s3-emulator":
        raise RuntimeError("invalid native fixture")
    project = "xenon-native-" + uuid.uuid4().hex[:12]
    env = os.environ.copy()
    compose = ["docker", "compose", "--project-name", project, "-f", "test/scenarios/integration/native-engine/compose.yaml"]
    versions = {"docker": execute(["docker", "version", "--format", "{{.Server.Version}}"], env), "compose": execute(["docker", "compose", "version", "--short"], env)}
    if any(versions[key] != cfg[key] for key in versions):
        raise RuntimeError(f"tool pin mismatch: {versions}")
    print(json.dumps({"project": project, "versions": versions, "schedule": cfg["schedule"]}), flush=True)
    primary = None
    try:
        execute([*compose, "up", "-d", "--wait"], env)
        address = execute([*compose, "port", "s3", "9000"], env)
        if not address.startswith("127.0.0.1:"):
            raise RuntimeError("unexpected emulator binding")
        endpoint = "http://" + address
        print(json.dumps({"endpoint": endpoint}), flush=True)
        deadline = time.monotonic() + 30
        while True:
            try:
                with urllib.request.urlopen(endpoint + "/minio/health/live", timeout=2) as response:
                    if response.status == 200:
                        break
            except OSError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("emulator readiness deadline")
                time.sleep(0.1)
        env.update({"AWS_ACCESS_KEY_ID": "xenon-local", "AWS_SECRET_ACCESS_KEY": "xenon-local-test-only", "AWS_DEFAULT_REGION": "us-east-1", "AWS_ENDPOINT": endpoint, "AWS_ALLOW_HTTP": "true", "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false", "XENON_ENGINE_STORE": "s3://" + cfg["bucket"]})
        completed = subprocess.run(["go", "test", "-race", "-json", "-count=1", "-timeout", str(cfg["test_timeout_seconds"])+"s", "./internal/partitions/slatedb", "-run", "^TestNativeEngine"], cwd=ROOT, env=env, timeout=cfg["test_timeout_seconds"]+10)
        if completed.returncode:
            raise RuntimeError("native engine contract assertion failed")
    except BaseException as error:
        primary = error
        print(json.dumps({"primary_failure": str(error)}), flush=True)
    finally:
        try:
            execute([*compose, "down", "--volumes", "--timeout", "5"], env)
            for resource, argv in (("containers", ["docker", "ps", "-aq"]), ("volumes", ["docker", "volume", "ls", "-q"]), ("networks", ["docker", "network", "ls", "-q"])):
                remaining = execute([*argv, "--filter", "label=com.docker.compose.project="+project], env)
                if remaining:
                    raise RuntimeError(f"scoped {resource} survived cleanup: {remaining}")
            print(json.dumps({"cleanup": "passed", "project": project}), flush=True)
        except BaseException as error:
            print(json.dumps({"cleanup_failure": str(error)}), flush=True)
            if primary is None:
                primary = error
    if primary is not None:
        raise primary

if __name__ == "__main__":
    def interrupted(signum, frame):
        raise RuntimeError(f"native fixture interrupted: {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    main()
