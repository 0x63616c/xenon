#!/usr/bin/env python3
"""Run actual conditional s3-meter storage against a scoped S3 emulator."""
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
    cfg = json.loads((ROOT / "proof/s3-meter/case.json").read_text())
    if cfg["schema"] != 1 or cfg["backend"] != "s3-emulator" or cfg["endpoint"] != "http://127.0.0.1:19009":
        raise RuntimeError("unregistered s3-meter fixture")
    project = os.environ["XENON_S3_METER_PROJECT"]
    if not re.fullmatch(r"xenon-s3-meter-[a-f0-9]{12}", project):
        raise RuntimeError("invalid scoped project")
    env = os.environ.copy()
    compose = ["docker", "compose", "--project-name", project, "-f", "deploy/s3-meter.compose.yaml"]
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
        completed = subprocess.run(["go","test","-race","-json","-tags","integration_s3","-count=1","-timeout","180s","./internal/proof/s3meter","-run","^TestMeterSignedS3$"],cwd=ROOT,env=env,timeout=200)
        if completed.returncode: raise RuntimeError("signed S3 metering assertion failed")
    finally:
        execute([*compose,"down","--volumes"],env)
        for argv in (["docker","ps","-aq","--filter","label=com.docker.compose.project="+project],["docker","volume","ls","-q","--filter","label=com.docker.compose.project="+project]):
            if execute(argv,env): raise RuntimeError("scoped s3-meter resources survived cleanup")

if __name__=="__main__":
    def interrupted(signum, frame): raise RuntimeError(f"s3-meter controller interrupted: {signum}")
    signal.signal(signal.SIGTERM,interrupted)
    signal.signal(signal.SIGINT,interrupted)
    try: main()
    except Exception as error:
        print(json.dumps({"error":str(error)}),flush=True)
        sys.exit(1)
