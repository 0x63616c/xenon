#!/usr/bin/env python3
"""Validate declared inputs only. This command never reports a runtime proof pass."""
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
INPUTS = ["test/scenarios/ministack/pins.json", "test/scenarios/ministack/case.json", "test/scenarios/ministack/README.md", "test/scenarios/ministack/config/compose.json", "test/scenarios/ministack/config/haproxy.cfg", "test/scenarios/ministack/config/temporal-a.json", "test/scenarios/ministack/config/temporal-b.json", "cmd/xenon-config-check/main.go", "scripts/check-ministack.py", "go.mod", "go.sum"]
def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
def main():
    env = {k:os.environ[k] for k in ("PATH", "HOME", "USER", "TMPDIR") if k in os.environ}
    env.update(GOENV="off", GOWORK="off", GOFLAGS="-mod=readonly", GOTOOLCHAIN="go1.27.1")
    def execute(argv,timeout=120):
        return subprocess.run(argv,cwd=ROOT,env=env,text=True,capture_output=True,timeout=timeout,check=True).stdout.strip()
    sha=execute(["git","rev-parse","HEAD"])
    dirty=execute(["git","status","--porcelain=v1","--untracked-files=all"])
    evidence=ROOT/".local/evidence"/(time.strftime("%Y%m%dT%H%M%SZ",time.gmtime())+"-ministack-config-"+uuid.uuid4().hex[:8])
    evidence.mkdir(parents=True)
    report={"kind":"configuration-check-only", "proof_pass":False,"runtime_executed":False,"result":"failed","git_sha":sha,"dirty":bool(dirty),"platform":platform.platform(),"machine":platform.machine(),"python":sys.version,"input_sha256":{p:digest(ROOT/p) for p in INPUTS},"commands":[]}
    try:
        pins=json.loads((ROOT/"test/scenarios/ministack/pins.json").read_text())
        case=json.loads((ROOT/"test/scenarios/ministack/case.json").read_text())
        if case["status"] not in ("configuration-checkpoint-not-boot-proof","runtime-candidate") or "--embedded-server" in case["omes_command"]:
            raise ValueError("runtime claim or embedded server not allowed")
        compose=json.loads((ROOT/"test/scenarios/ministack/config/compose.json").read_text())
        for service,pin in (("s3","s3_emulator"),("ingress","proxy"),("ui","ui")):
            if compose["services"][service]["image"]!=pins[pin]["image"] or "@sha256:" not in pins[pin]["image"]:
                raise ValueError("image pin mismatch")
        report["tools"]={"go":execute(["go","version"]),"docker":execute(["docker","version","--format","{{.Server.Version}}"]),"compose":execute(["docker","compose","version","--short"])}
        if not report["tools"]["go"].startswith("go version go1.27.1 "):raise ValueError("Go tool mismatch")
        commands=[
            ["go","run","./cmd/xenon-config-check","test/scenarios/ministack/config/temporal-a.json","test/scenarios/ministack/config/temporal-b.json"],
            ["docker","compose","-f","test/scenarios/ministack/config/compose.json","config","--quiet"],
            ["docker","run","--rm","--add-host","host.docker.internal:host-gateway","-v",str(ROOT/"test/scenarios/ministack/config/haproxy.cfg")+":/usr/local/etc/haproxy/haproxy.cfg:ro",pins["proxy"]["image"],"haproxy","-c","-f","/usr/local/etc/haproxy/haproxy.cfg"],
        ]
        for index,argv in enumerate(commands):
            output=execute(argv)
            if index==0 and output.splitlines()!=["CONFIG_VALID test/scenarios/ministack/config/temporal-a.json","CONFIG_VALID test/scenarios/ministack/config/temporal-b.json"]:raise ValueError("missing Temporal config assertions")
            path=evidence/f"command-{index}.log";path.write_text(output+"\n")
            report["commands"].append({"argv":argv,"exit_code":0,"output":path.name,"sha256":digest(path)})
        if sha!=execute(["git","rev-parse","HEAD"]) or dirty!=execute(["git","status","--porcelain=v1","--untracked-files=all"]):raise ValueError("checkout changed")
        if any(digest(ROOT/p)!=value for p,value in report["input_sha256"].items()):raise ValueError("input changed")
        report["result"]="configuration-valid"
    except Exception as error:
        report["error"]=str(error)
    finally:
        (evidence/"result.json").write_text(json.dumps(report,indent=2)+"\n")
        print(report["result"].upper()+": "+str(evidence/"result.json"))
    return 0 if report["result"]=="configuration-valid" else 1
if __name__=="__main__":sys.exit(main())
