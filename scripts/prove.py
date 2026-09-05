#!/usr/bin/env python3
"""Run a committed, allowlisted Xenon experiment and retain provenance (stdlib only)."""
import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def classify_success(dirty):
    return ("development-passed", False) if dirty else ("passed", True)


def cargo_configs(root, env):
    # Cargo searches the invocation directory and ancestors, then CARGO_HOME.
    dirs = [root, *root.parents]
    home = Path(env.get("CARGO_HOME", str(Path(env["HOME"]) / ".cargo")))
    if not home.is_absolute():
        home = root / home
    paths = [d / ".cargo" / n for d in dirs for n in ("config", "config.toml")]
    paths += [home / n for n in ("config", "config.toml")]
    return sorted({str(p.resolve()) for p in paths if p.is_file()})


def command(spec):
    if set(spec) != {"runner", "filter", "exact", "expected_tests"}:
        raise ValueError("invalid command fields")
    if spec["runner"] != "cargo-test" or not isinstance(spec["exact"], bool):
        raise ValueError("only structured cargo-test commands are allowed")
    if not isinstance(spec["filter"], str) or not re.fullmatch(r"[a-zA-Z0-9_:]+", spec["filter"]):
        raise ValueError("invalid test filter")
    tests = spec["expected_tests"]
    if not isinstance(tests, list) or not tests or len(set(tests)) != len(tests):
        raise ValueError("expected_tests must be a nonempty unique list")
    if any(not isinstance(t, str) or not re.fullmatch(r"[a-zA-Z0-9_:]+", t) for t in tests):
        raise ValueError("invalid expected test name")
    return ["cargo", "test", "--locked", "-p", "slatedb-probe", "--lib", spec["filter"], "--", *(["--exact"] if spec["exact"] else []), "--nocapture"]


def verify_tests(output, expected):
    actual = re.findall(r"^test ([a-zA-Z0-9_:]+) \.\.\. ok$", output, re.MULTILINE)
    summaries = re.findall(r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", output)
    if sorted(actual) != sorted(expected) or summaries != [(str(len(expected)), "0", "0")]:
        raise ValueError(f"test assertions mismatch: expected {expected}, observed {actual}, summaries {summaries}")


def run_process(argv, timeout, env, root):
    with subprocess.Popen(argv, cwd=root, env=env, stdin=subprocess.DEVNULL,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, start_new_session=True) as process:
        try:
            output, _ = process.communicate(timeout=timeout)
            return process.returncode, output, False
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            time.sleep(0.2)
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except PermissionError:
                if process.poll() is None:
                    raise
            output, _ = process.communicate(timeout=5)
            return process.returncode, output, True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", choices=["primitive", "ownership"])
    parser.add_argument("--allow-dirty", action="store_true", help="development only; evidence is marked non-reproducible")
    args = parser.parse_args()
    manifest_path = ROOT / "experiments" / (args.name + ".json")
    manifest = json.loads(manifest_path.read_text())
    if manifest["schema"] != 1 or manifest["name"] != args.name or manifest["backend"] != "memory":
        raise ValueError("unsupported manifest identity/schema/backend")
    timeout = manifest["timeout_seconds_per_command"]
    if isinstance(timeout, bool) or not isinstance(timeout, int) or not 1 <= timeout <= 900:
        raise ValueError("invalid timeout")
    commands = [(command(spec), spec["expected_tests"]) for spec in manifest["commands"]]
    if not commands:
        raise ValueError("empty experiment")
    env = {k: os.environ[k] for k in ("PATH", "HOME", "USER", "TMPDIR", "RUSTUP_HOME", "CARGO_HOME") if k in os.environ}
    env.update({"CARGO_TERM_COLOR": "never", "XENON_PROBE_BACKEND": "memory"})
    def git(*argv):
        return subprocess.check_output(["git", *argv], cwd=ROOT, env=env, text=True).strip()
    sha = git("rev-parse", "HEAD")
    dirty = git("status", "--porcelain=v1", "--untracked-files=all")
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + args.name + "-" + uuid.uuid4().hex[:8]
    evidence = ROOT / ".local" / "evidence" / run_id
    evidence.mkdir(parents=True)
    report = {"schema": 1, "experiment": args.name, "commit": sha, "dirty_status": dirty,
              "reproducible_clean_checkout": not bool(dirty), "backend": "memory", "result": "failed", "proof_pass": False,
              "platform": {"system": platform.system(), "release": platform.release(), "machine": platform.machine()},
              "python": {"version": sys.version, "executable": sys.executable},
              "commands": [], "config_sha256": {}, "tool_versions": {}, "assertions": manifest["assertions"],
              "limitations": manifest["limitations"]}
    try:
        report["cargo_config_sha256"] = {p: digest(Path(p)) for p in cargo_configs(ROOT, env)}
        if report["cargo_config_sha256"]:
            raise ValueError("ambient Cargo config detected; use a checkout/environment without these configs (hashes recorded, contents omitted)")
        if dirty and not args.allow_dirty:
            raise ValueError("checkout is dirty; commit inputs or explicitly use --allow-dirty for development")
        if dirty:
            report["tracked_diff_sha256"] = hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD", "--binary"], cwd=ROOT, env=env)).hexdigest()
        for relative in [str(manifest_path.relative_to(ROOT)), "scripts/prove.py", *manifest["inputs"]]:
            path = (ROOT / relative).resolve()
            if not path.is_relative_to(ROOT) or not path.is_file():
                raise ValueError(f"missing or invalid declared input: {relative}")
            report["config_sha256"][relative] = digest(path)
        (evidence / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        for tool in ("git", "rustc", "cargo", "python"):
            argv = [sys.executable, "--version"] if tool == "python" else [tool, "-vV" if tool == "rustc" else "--version"]
            code, output, expired = run_process(argv, 30, env, ROOT)
            if code or expired:
                raise ValueError(f"cannot record tool version: {tool}")
            report["tool_versions"][tool] = output.strip()
        for index, (argv, expected) in enumerate(commands, 1):
            print(f"[{index}/{len(commands)}] {' '.join(argv)}", flush=True)
            started = time.monotonic()
            code, output, expired = run_process(argv, timeout, env, ROOT)
            log = f"command-{index}.log"
            (evidence / log).write_text(output)
            report["commands"].append({"argv": argv, "exit_code": code, "timed_out": expired,
                "elapsed_seconds": round(time.monotonic() - started, 3), "output": log,
                "output_sha256": digest(evidence / log), "expected_tests": expected})
            if code or expired:
                raise ValueError(f"command {index} failed (exit={code}, timeout={expired}); see {log}")
            verify_tests(output, expected)
        # Detect edits made during execution rather than assigning them the starting hash.
        for relative, expected_hash in report["config_sha256"].items():
            if digest(ROOT / relative) != expected_hash:
                raise ValueError(f"input changed while experiment ran: {relative}")
        if git("rev-parse", "HEAD") != sha or git("status", "--porcelain=v1", "--untracked-files=all") != dirty:
            raise ValueError("checkout changed while experiment ran")
        if cargo_configs(ROOT, env):
            raise ValueError("ambient Cargo config appeared during execution")
        report["result"], report["proof_pass"] = classify_success(bool(dirty))
    except Exception as error:
        report["error"] = str(error)
    finally:
        (evidence / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"{report['result'].upper()}: {evidence / 'result.json'}", flush=True)
    return 0 if report["result"] in ("passed", "development-passed") else 1


if __name__ == "__main__":
    sys.exit(main())
