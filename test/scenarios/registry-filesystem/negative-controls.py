#!/usr/bin/env python3
"""Run exact production-source omissions; require the intended assertion failure."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    source = (ROOT / "internal/registry/filesystem/store.go").read_text()
    read_start = source.index("func (s *Store) readLocked")
    read_end = source.index("func (s *Store) Read(", read_start)
    read_without_recovery = source[:read_start] + source[read_start:read_end].replace(
        "s.fileSync(f)", "error(nil)").replace("s.syncDir()", "error(nil)") + source[read_end:]
    mutants = [
        ("missing-fsync", source.replace("s.fileSync(f)", "error(nil)"),
         "TestSyncSyscallBoundaries", "write must sync file then directory"),
        ("unlocked-replace", source.replace("unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)", "error(nil)"),
         "TestProcessCrashRecoveryAndCAS", "unlocked replace crossed held CAS"),
        ("missing-read-recovery", read_without_recovery,
         "TestSyncSyscallBoundaries", "read must recover file then directory durability"),
    ]
    args.evidence.mkdir(parents=True, exist_ok=False)
    evidence = {"source_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "platform": platform.platform(), "toolchain": "go1.27.1", "results": [],
                "input_hashes": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in sorted((ROOT / "internal/registry").rglob("*.go"))}}
    env = dict(os.environ, GOENV="off", GOWORK="off", GOFLAGS="-mod=readonly", GOTOOLCHAIN="go1.27.1")
    try:
        for name, mutated, test, assertion in mutants:
            if mutated == source:
                raise RuntimeError("source mutation no longer matches: " + name)
            with tempfile.TemporaryDirectory(prefix="xenon-fs-mutant-") as temporary:
                work = Path(temporary)
                for filename in ("go.mod", "go.sum"):
                    shutil.copy2(ROOT / filename, work / filename)
                for package in ("identity", "registry"):
                    shutil.copytree(ROOT / "internal" / package, work / "internal" / package)
                (work / "internal/registry/filesystem/store.go").write_text(mutated)
                command = ["go", "test", "-race", "-count=1", "-timeout=60s", "-run=^" + test + "$", "./internal/registry/filesystem"]
                result = subprocess.run(command, cwd=work, env=env, text=True, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, timeout=80)
                (args.evidence / (name + ".log")).write_text(result.stdout)
                caught = result.returncode != 0 and assertion in result.stdout
                evidence["results"].append({"name": name, "command": command, "returncode": result.returncode,
                                            "caught": caught, "mutated_source_sha256": hashlib.sha256(mutated.encode()).hexdigest()})
                if not caught:
                    raise RuntimeError("negative control did not trigger its invariant: " + name)
        evidence["status"] = "passed"
    except BaseException as exc:
        evidence["status"] = "failed"
        evidence["failure"] = str(exc)
        raise
    finally:
        (args.evidence / "result.json").write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    main()
