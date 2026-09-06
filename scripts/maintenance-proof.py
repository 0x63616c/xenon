#!/usr/bin/env python3
"""Run actual conditional maintenance storage against a scoped S3 emulator."""
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
    cfg = json.loads((ROOT / "proof/maintenance/case.json").read_text())
    if cfg["schema"] != 1 or cfg["backend"] != "s3-emulator" or cfg["endpoint"] != "http://127.0.0.1:19005":
        raise RuntimeError("unregistered maintenance fixture")
    project = os.environ["XENON_MAINTENANCE_PROJECT"]
    if not re.fullmatch(r"xenon-maintenance-[a-f0-9]{12}", project):
        raise RuntimeError("invalid scoped project")
    env = os.environ.copy()
    env.update({"AWS_ACCESS_KEY_ID":"xenon-local", "AWS_SECRET_ACCESS_KEY":"xenon-local-test-only", "AWS_DEFAULT_REGION":"us-east-1", "AWS_ENDPOINT":cfg["endpoint"], "AWS_ALLOW_HTTP":"true", "AWS_VIRTUAL_HOSTED_STYLE_REQUEST":"false", "XENON_BUCKET":cfg["bucket"], "XENON_TOPOLOGY_PREFIX":"metadata", "XENON_GO_NODE_BINARY":str(ROOT / ".local/bin/xenon-go-node")})
    compose = ["docker", "compose", "--project-name", project, "-f", "deploy/maintenance.compose.yaml"]
    print(json.dumps({"docker":execute(["docker","version","--format","{{.Server.Version}}"],env),"compose":execute(["docker","compose","version","--short"],env)}),flush=True)
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
        completed = subprocess.run(["go","test","-race","-json","-tags","integration_s3","-count=1","-timeout","1250s","./internal/ownership","-run","^TestS3Maintenance$"],cwd=ROOT,env=env,timeout=1280)
        if completed.returncode: raise RuntimeError("maintenance assertion failed")
    finally:
        execute([*compose,"down","--volumes"],env)
        for argv in (["docker","ps","-aq","--filter","label=com.docker.compose.project="+project],["docker","volume","ls","-q","--filter","label=com.docker.compose.project="+project]):
            if execute(argv,env): raise RuntimeError("scoped maintenance resources survived cleanup")

if __name__=="__main__":
    def interrupted(signum, frame): raise RuntimeError(f"maintenance controller interrupted: {signum}")
    signal.signal(signal.SIGTERM,interrupted)
    signal.signal(signal.SIGINT,interrupted)
    try: main()
    except Exception as error:
        print(json.dumps({"error":str(error)}),flush=True)
        sys.exit(1)
