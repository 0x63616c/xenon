#!/usr/bin/env python3
"""Run actual conditional directory storage against a scoped S3 emulator."""
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]

def execute(argv, env, timeout=120):
    result = subprocess.run(argv, cwd=ROOT, env=env, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"command failed {argv!r}: {result.stdout}\n{result.stderr}")
    return result.stdout.strip()

def main():
    registry = len(sys.argv) == 2 and sys.argv[1] == "registry"
    bootstrap = len(sys.argv) == 2 and sys.argv[1] == "bootstrap"
    if len(sys.argv) > 1 and not (registry or bootstrap): raise RuntimeError("unknown directory proof profile")
    cfg = json.loads((ROOT / ("test/scenarios/bootstrap/s3.json" if bootstrap else ("test/scenarios/registry/s3.json" if registry else "proof/directory/case.json"))).read_text())
    if cfg["schema"] != 1 or cfg.get("backend", "s3-emulator") != "s3-emulator" or cfg["endpoint"] != "http://127.0.0.1:19003":
        raise RuntimeError("unregistered directory fixture")
    project = os.environ["XENON_DIRECTORY_PROJECT"]
    if not re.fullmatch(r"xenon-directory-[a-f0-9]{12}", project):
        raise RuntimeError("invalid scoped project")
    env = os.environ.copy()
    compose = ["docker", "compose", "--project-name", project, "-f", "deploy/directory.compose.yaml"]
    versions = {"docker":execute(["docker","version","--format","{{.Server.Version}}"],env),"compose":execute(["docker","compose","version","--short"],env)}
    print(json.dumps(versions),flush=True)
    if bootstrap:
        if any(versions[name] != cfg[name] for name in versions): raise RuntimeError("bootstrap tool versions differ from fixture")
        print(execute([sys.executable,"scripts/build-go-node.py"],env,timeout=600),flush=True)
        pins=json.loads((ROOT/"tools/slatedb-native.json").read_text())
        lib=ROOT/".local/slatedb-native-target/debug"
        env.update({"GOTOOLCHAIN":pins["go_toolchain"],"GOENV":"off","GOWORK":"off","GOFLAGS":"-mod=readonly","CGO_ENABLED":"1","CGO_LDFLAGS":"-L"+str(lib),"LD_LIBRARY_PATH":str(lib),"DYLD_LIBRARY_PATH":str(lib),"SLATEDB_UNIFFI_RUNTIME_THREADS":pins["runtime_threads"]})
    try:
        execute([*compose,"up","-d","--wait"],env)
        deadline = time.monotonic()+30
        while True:
            try:
                with urllib.request.urlopen(cfg["endpoint"]+"/minio/health/live",timeout=2) as response:
                    if response.status==200: break
            except OSError:
                if time.monotonic()>=deadline: raise RuntimeError("emulator readiness deadline")
                time.sleep(0.1)
        completed = subprocess.run(["go","test","-race","-json","-tags","integration_s3","-count=1","-timeout","180s",("./internal/app" if bootstrap else ("./internal/registry/s3" if registry else "./internal/directory")),"-run",("^TestS3FreshBootstrap$" if bootstrap else ("^TestS3Registry$" if registry else "^TestS3Directory$"))],cwd=ROOT,env=env,timeout=200)
        if completed.returncode: raise RuntimeError("cross-language directory assertion failed")
    finally:
        execute([*compose,"down","--volumes"],env)
        for argv in (["docker","ps","-aq","--filter","label=com.docker.compose.project="+project],["docker","volume","ls","-q","--filter","label=com.docker.compose.project="+project]):
            if execute(argv,env): raise RuntimeError("scoped directory resources survived cleanup")

if __name__=="__main__":
    def interrupted(signum, frame): raise RuntimeError(f"directory controller interrupted: {signum}")
    signal.signal(signal.SIGTERM,interrupted)
    signal.signal(signal.SIGINT,interrupted)
    try: main()
    except Exception as error:
        print(json.dumps({"error":str(error)}),flush=True)
        sys.exit(1)
