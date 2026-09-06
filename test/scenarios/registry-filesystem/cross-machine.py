#!/usr/bin/env python3
"""Explicit opt-in two-machine shared-mount qualification. Requires Python3 + SSH."""
import argparse
import base64
import hashlib
import json
import os
import re
from pathlib import Path
import selectors
import shlex
import subprocess
import time
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.manifest.read_text())
    assert config["version"] == 1
    assert re.fullmatch(r"[0-9a-f]{40}", config["source_revision"]), "exact source commit required"
    timeout = config["step_timeout_seconds"]
    assert 0 < timeout <= 300
    assert config["a"]["ssh"] != config["b"]["ssh"], "two declared machines required"
    args.evidence.mkdir(parents=True, exist_ok=False)
    run = "xenon-registry-" + uuid.uuid4().hex
    participants = []
    evidence = {"run": run, "manifest": config, "manifest_sha256": hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
                "observations": [], "cleanup_errors": [], "status": "running"}

    def ssh(host, command):
        result = subprocess.run(["ssh", "-oBatchMode=yes", "-oConnectTimeout=" + str(timeout), host["ssh"], command],
                                capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError("SSH failed: " + result.stderr)
        return result.stdout.strip()

    def python(host, code):
        return ssh(host, "python3 -c " + shlex.quote(code))

    class Participant:
        def __init__(self, side, action, **kwargs):
            self.host = config[side]
            self.marker = run + "-" + str(len(participants))
            self.pid = None
            self.finished = False
            self.buffer = b""
            self.selector = selectors.DefaultSelector()
            command = shlex.join(["env", "XENON_FS_PARTICIPANT=1", "XENON_FS_REMOTE=1", self.host["binary"],
                                  "-test.run=^TestProcessParticipant$", "-test.outputdir=" + self.marker,
                                  "-test.timeout=" + str(timeout * 8) + "s"])
            self.log = open(args.evidence / (self.marker + ".stderr"), "wb")
            self.proc = subprocess.Popen(["ssh", "-oBatchMode=yes", "-oConnectTimeout=" + str(timeout), self.host["ssh"], command],
                                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log)
            self.selector.register(self.proc.stdout, selectors.EVENT_READ)
            participants.append(self)
            pidline = self.line()
            assert pidline.startswith("PID "), pidline
            self.pid = int(pidline[4:])
            self.send(json.dumps(dict(Directory=self.host["directory"] + "/" + run, Action=action, **kwargs)))
            assert self.line() == "READY"

        def line(self):
            end = time.monotonic() + timeout
            while b"\n" not in self.buffer:
                if not self.selector.select(max(0, end - time.monotonic())):
                    raise TimeoutError("participant boundary timeout: " + self.marker)
                chunk = os.read(self.proc.stdout.fileno(), 65536)
                if not chunk:
                    raise RuntimeError("participant exited before boundary: " + self.marker)
                self.buffer += chunk
            raw, self.buffer = self.buffer.split(b"\n", 1)
            line = raw.decode()
            evidence["observations"].append({"participant": self.marker, "line": line})
            return line

        def send(self, line="GO"):
            self.proc.stdin.write((line + "\n").encode())
            self.proc.stdin.flush()

        def result(self):
            line = self.line()
            assert line.startswith("RESULT "), line
            result = json.loads(line[7:])
            assert self.proc.wait(timeout=timeout) == 0
            self.finished = True
            return result

        def kill(self):
            if self.finished:
                return
            if self.pid is not None:
                # Verify the unique invocation marker before signaling a PID.
                python(self.host, "import os,signal,subprocess; p=" + repr(self.pid) +
                       "; s=subprocess.run(['ps','-p',str(p),'-o','command='],capture_output=True,text=True).stdout; " +
                       "assert not s or " + repr("-test.outputdir=" + self.marker) + " in s, 'PID no longer owned'; " +
                       "os.kill(p,signal.SIGKILL) if s else None")
            self.proc.stdin.close()
            try:
                self.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=timeout)
            self.finished = True

    def expect_payload(result, payload):
        assert result["Error"] == "", result
        env = json.loads(base64.b64decode(result["Record"]["Body"]))
        assert base64.b64decode(env["body"]).decode() == payload, env

    try:
        for side in ("a", "b"):
            host = config[side]
            assert host["directory"].startswith("/") and host["binary"].startswith("/")
            for field in ("filesystem", "mount_options", "client", "server", "export"):
                assert host["qualification"][field], "missing mount declaration: " + field
            actual = python(host, "import hashlib,pathlib,platform,json; print(json.dumps({'binary_sha256':hashlib.sha256(pathlib.Path(" +
                            repr(host["binary"]) + ").read_bytes()).hexdigest(),'platform':platform.platform()}))")
            evidence[side] = json.loads(actual)
            assert evidence[side]["binary_sha256"] == host["binary_sha256"], "binary differs from manifest"
        # Only A creates the namespace: B must observe that SAME shared directory.
        python(config["a"], "import os; os.mkdir(" + repr(config["a"]["directory"] + "/" + run) + ",0o700)")
        python(config["b"], "import os; assert os.path.isdir(" + repr(config["b"]["directory"] + "/" + run) + ")")
        suite = Participant("a", "contract")
        suite.send()
        assert suite.result()["Error"] == ""
        writer = Participant("a", "create", ID=1, Body="recover", Cut="renamed")
        writer.send()
        assert writer.line() == "CUT renamed"
        writer.kill()
        reader = Participant("b", "read")
        reader.send()
        recovered = reader.result()
        expect_payload(recovered, "recover")
        version = recovered["Record"]["Version"]
        first = Participant("a", "replace", Expected=version, ID=2, Body="winner", Cut="condition-checked")
        second = Participant("b", "replace", Expected=version, ID=3, Body="loser", Cut="contended")
        first.send()
        assert first.line() == "CUT condition-checked"
        second.send()
        assert second.line() == "CUT contended", "cross-machine locks did not exclude contender"
        first.send()
        winner = first.result()
        expect_payload(winner, "winner")
        second.send()
        assert second.result()["Error"] == "conflict"
        for side in ("b", "a"):
            restarted = Participant(side, "read")
            restarted.send()
            fresh = restarted.result()
            expect_payload(fresh, "winner")
            assert fresh["Record"]["Version"] == winner["Record"]["Version"]
        evidence["status"] = "passed"
    except BaseException as exc:
        evidence["status"] = "failed"
        evidence["failure"] = str(exc)
        raise
    finally:
        for participant in reversed(participants):
            try:
                participant.kill()
            except BaseException as exc:
                evidence["cleanup_errors"].append(str(exc))
            participant.selector.close()
            participant.log.close()
        # Leave the tiny explicitly named mount namespace as recovery evidence.
        # Operators can remove it after confirming no participant remains alive.
        evidence["retained_namespace"] = run
        if evidence["cleanup_errors"]:
            evidence["status"] = "failed"
        (args.evidence / "result.json").write_text(json.dumps(evidence, indent=2) + "\n")
        if evidence["cleanup_errors"]:
            raise RuntimeError("participant cleanup failed; see result.json")


if __name__ == "__main__":
    main()
