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
    if spec == {"runner": "go-node-build"}:
        return [sys.executable, "scripts/build-go-node.py"]
    if spec == {"runner": "cargo-build-node"}:
        return ["cargo", "build", "--locked", "-p", "xenon-node"]
    if set(spec) != {"runner", "filter", "exact", "expected_tests"}:
        raise ValueError("invalid command fields")
    runner = spec["runner"]
    if runner not in ("cargo-test", "cargo-test-node", "go-test-shard", "go-test-node", "s3-crash", "s3-crash-cleanup", "go-test-routing", "go-shard-compat", "s3-directory", "s3-owner-manager", "s3-maintenance") or not isinstance(spec["exact"], bool):
        raise ValueError("only registered structured test commands are allowed")
    if not isinstance(spec["filter"], str) or not re.fullmatch(r"[a-zA-Z0-9_:]+", spec["filter"]):
        raise ValueError("invalid test filter")
    tests = spec["expected_tests"]
    if not isinstance(tests, list) or not tests or len(set(tests)) != len(tests):
        raise ValueError("expected_tests must be a nonempty unique list")
    if any(not isinstance(t, str) or not re.fullmatch(r"[a-zA-Z0-9_:/]+", t) for t in tests):
        raise ValueError("invalid expected test name")
    if runner == "s3-maintenance":
        if spec["filter"] != "TestS3Maintenance" or not spec["exact"]:
            raise ValueError("unregistered maintenance test")
        return [sys.executable, "scripts/maintenance-proof.py"]
    if runner == "s3-owner-manager":
        if spec["filter"] != "TestS3OwnerManager" or not spec["exact"]:
            raise ValueError("unregistered owner manager test")
        return [sys.executable, "scripts/owner-manager-proof.py"]
    if runner == "s3-directory":
        if spec["filter"] != "TestS3Directory" or not spec["exact"]:
            raise ValueError("unregistered directory test")
        return [sys.executable, "scripts/directory-proof.py"]
    if runner in ("s3-crash", "s3-crash-cleanup"):
        if spec["filter"] != "crash" or spec["exact"]:
            raise ValueError("invalid crash command")
        return [sys.executable, "scripts/crash-proof.py", *(["--check-cleanup"] if runner == "s3-crash-cleanup" else [])]
    if runner == "go-test-routing":
        if not spec["exact"] or spec["filter"] not in ("TestForwardingReplayAndRefresh", "TestForwardingLoopsAndDeadline", "TestForwardingClosedAdmission"):
            raise ValueError("unregistered routing test")
        return ["go", "test", "-race", "-json", "-count=1", "./internal/routing", "-run", "^" + spec["filter"] + "$"]
    if runner == "go-shard-compat":
        if spec["filter"] != "TestShardStoredCompatibility" or not spec["exact"]:
            raise ValueError("unregistered compatibility test")
        return [sys.executable, "scripts/shard-compat-proof.py"]
    if runner == "go-test-node":
        if not spec["exact"] or not spec["filter"].startswith("TestGoOwner"):
            raise ValueError("unregistered Go owner test")
        return ["go", "test", "-json", "-count=1", "./internal/node", "-run", "^" + spec["filter"] + "$"]
    if runner == "go-test-shard":
        if not spec["exact"] or spec["filter"] not in ("TestShardRPC", "TestShardTransportBoundsAndTypes", "TestNamespaceRPC", "TestNamespaceByteBoundedPagination", "TestQueueRPC", "TestQueueByteBoundedPagination", "TestQueueRemoteCancellation", "TestHistoryRPC", "TestHistoryTimeoutTypes", "TestHistoryByteBoundedPagination", "TestNexusTransport", "TestExecutionRPC", "TestExecutionTasksRPC", "TestExecutionTasksUpstream", "TestHistoryTasksRPC", "TestHistoryPartitionDeadline", "TestHistoryPartitionInvalidCursor"):
            raise ValueError("unregistered Go test")
        return ["go", "test", "-json", "-count=1", "./internal/adapter", "-run", "^" + spec["filter"] + "$"]
    package = "xenon-node" if runner == "cargo-test-node" else "slatedb-probe"
    return ["cargo", "test", "--locked", "-p", package, "--lib", spec["filter"], "--", *(["--exact"] if spec["exact"] else []), "--nocapture"]


def verify_go_tests(output, expected, package="github.com/0x63616c/xenon/internal/adapter"):
    events = [json.loads(line) for line in output.splitlines() if line.strip() and not line.startswith("go: downloading ")]
    if any(event.get("Action") in ("skip", "fail") for event in events):
        raise ValueError("Go test skipped or failed")
    actual = [event["Test"] for event in events if event.get("Action") == "pass" and "Test" in event]
    packages = [event for event in events if event.get("Action") == "pass" and "Test" not in event]
    if sorted(actual) != sorted(expected) or len(packages) != 1 or packages[0].get("Package") != package:
        raise ValueError(f"Go test assertions mismatch: expected {expected}, observed {actual}")


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
            try:
                output, _ = process.communicate(timeout=15)
                return process.returncode, output, True
            except subprocess.TimeoutExpired:
                pass
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except PermissionError:
                if process.poll() is None:
                    raise
            output, _ = process.communicate(timeout=5)
            return process.returncode, output, True


def cleanup_crash(project, env, root):
    if not re.fullmatch(r"xenon-crash-[a-f0-9]{12}", project):
        raise ValueError("invalid scoped cleanup identity")
    argv = ["docker", "compose", "--project-name", project, "-f", "deploy/crash.compose.yaml", "down", "--volumes"]
    try:
        code, output, expired = run_process(argv, 60, env, root)
        return {"project": project, "exit_code": code, "timed_out": expired}
    except Exception as error:
        return {"project": project, "exit_code": -1, "timed_out": False, "error": str(error)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", choices=["primitive", "ownership", "shard", "crash", "go-bindings", "go-shard", "go-namespace", "go-cluster", "go-queue", "go-history", "go-nexus", "go-matching", "go-matching-userdata", "go-queuev2", "go-persistence", "go-runtime-stores", "go-history-routing", "go-fair", "go-execution", "go-historytasks", "go-executiontasks", "go-shard-compat", "forwarding", "directory", "owner-manager", "maintenance"])
    parser.add_argument("--allow-dirty", action="store_true", help="development only; evidence is marked non-reproducible")
    args = parser.parse_args()
    if args.name == "go-bindings":
        return subprocess.call([sys.executable, str(ROOT / "scripts/prove-go-bindings.py"), *(["--allow-dirty"] if args.allow_dirty else [])], cwd=ROOT)
    manifest_path = ROOT / "experiments" / (args.name + ".json")
    manifest = json.loads(manifest_path.read_text())
    if manifest["schema"] != 1 or manifest["name"] != args.name or manifest["backend"] != ("s3-emulator" if args.name in ("crash", "go-shard-compat", "directory", "owner-manager", "maintenance") else "memory"):
        raise ValueError("unsupported manifest identity/schema/backend")
    timeout = manifest["timeout_seconds_per_command"]
    if isinstance(timeout, bool) or not isinstance(timeout, int) or not 1 <= timeout <= (1500 if args.name == "maintenance" else 900):
        raise ValueError("invalid timeout")
    commands = [(command(spec), spec.get("expected_tests", []), spec["runner"]) for spec in manifest["commands"]]
    if args.name == "shard" and (not commands or commands[0][2] != "cargo-build-node"):
        raise ValueError("shard proof must build the node before tests")
    if args.name in ("go-shard", "go-namespace", "go-cluster", "go-queue", "go-history", "go-nexus", "go-matching", "go-matching-userdata", "go-queuev2", "go-persistence", "go-runtime-stores", "go-history-routing", "go-fair", "go-execution", "go-historytasks", "go-executiontasks", "go-shard-compat", "owner-manager", "maintenance") and (not commands or commands[0][2] != "go-node-build"):
        raise ValueError("Go shard proof must build native Go node first")
    if not commands:
        raise ValueError("empty experiment")
    env = {k: os.environ[k] for k in ("PATH", "HOME", "USER", "TMPDIR", "RUSTUP_HOME", "CARGO_HOME") if k in os.environ}
    env.update({"CARGO_TERM_COLOR": "never", "XENON_PROBE_BACKEND": "memory"})
    if args.name in ("shard", "go-shard-compat"):
        env.update({"GOENV": "off", "GOWORK": "off", "GOFLAGS": "-mod=readonly", "GOTOOLCHAIN": "go1.27.1", "XENON_NODE_BINARY": str(ROOT / "target/debug/xenon-node")})
    if args.name == "forwarding":
        env.update({"GOENV":"off", "GOWORK":"off", "GOFLAGS":"-mod=readonly", "GOTOOLCHAIN":"go1.27.1"})
    if args.name == "crash":
        env["XENON_PROOF_PROJECT"] = "xenon-crash-" + uuid.uuid4().hex[:12]
    if args.name in ("go-shard", "go-namespace", "go-cluster", "go-queue", "go-history", "go-nexus", "go-matching", "go-matching-userdata", "go-queuev2", "go-persistence", "go-runtime-stores", "go-history-routing", "go-fair", "go-execution", "go-historytasks", "go-executiontasks", "go-shard-compat", "owner-manager", "maintenance"):
        target = ROOT / ".local/slatedb-native-target/debug"
        env.update({"GOENV":"off", "GOWORK":"off", "GOFLAGS":"-mod=readonly", "GOTOOLCHAIN":"go1.27.1", "CGO_ENABLED":"1", "CGO_LDFLAGS":"-L"+str(target), "LD_LIBRARY_PATH":str(target), "DYLD_LIBRARY_PATH":str(target), "SLATEDB_UNIFFI_RUNTIME_THREADS":"2", "XENON_NODE_BINARY":str(ROOT / ".local/bin/xenon-go-node")})
    if args.name == "go-shard-compat":
        env["XENON_COMPAT_PROJECT"] = "xenon-compat-" + uuid.uuid4().hex[:12]
        if [item[2] for item in commands] != ["go-node-build", "cargo-build-node", "go-shard-compat"]:
            raise ValueError("compatibility proof requires both builds then registered fixture")
    if args.name == "maintenance":
        env["XENON_MAINTENANCE_PROJECT"] = "xenon-maintenance-" + uuid.uuid4().hex[:12]
    if args.name == "owner-manager":
        env["XENON_OWNER_MANAGER_PROJECT"] = "xenon-owner-manager-" + uuid.uuid4().hex[:12]
    if args.name == "directory":
        env.update({"GOENV":"off", "GOWORK":"off", "GOFLAGS":"-mod=readonly", "GOTOOLCHAIN":"go1.27.1", "XENON_DIRECTORY_PROJECT":"xenon-directory-" + uuid.uuid4().hex[:12]})
    def git(*argv):
        return subprocess.check_output(["git", *argv], cwd=ROOT, env=env, text=True).strip()
    sha = git("rev-parse", "HEAD")
    dirty = git("status", "--porcelain=v1", "--untracked-files=all")
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + args.name + "-" + uuid.uuid4().hex[:8]
    evidence = ROOT / ".local" / "evidence" / run_id
    evidence.mkdir(parents=True)
    report = {"schema": 1, "experiment": args.name, "commit": sha, "dirty_status": dirty,
              "reproducible_clean_checkout": not bool(dirty), "backend": manifest["backend"], "result": "failed", "proof_pass": False,
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
        if args.name in ("shard", "go-shard-compat"):
            install_argv = [sys.executable, "scripts/install-protoc.py"]
            code, output, expired = run_process(install_argv, 120, env, ROOT)
            (evidence / "compiler-install.log").write_text(output)
            report["compiler_install"] = {"argv": install_argv, "exit_code": code, "timed_out": expired, "output": "compiler-install.log"}
            if code or expired:
                raise ValueError("pinned compiler installation failed")
            env["PATH"] = str(ROOT / ".local/protoc/bin") + os.pathsep + env["PATH"]
            report["protoc_binary_sha256"] = digest(ROOT / ".local/protoc/bin/protoc")
        for tool in ("git", "rustc", "cargo", "python", *(["go", "protoc"] if args.name in ("shard", "go-shard-compat") else (["go"] if args.name in ("go-shard", "go-namespace", "go-cluster", "go-queue", "go-history", "go-nexus", "go-matching", "go-matching-userdata", "go-queuev2", "go-persistence", "go-runtime-stores", "go-history-routing", "go-fair", "go-execution", "go-historytasks", "go-executiontasks", "go-shard-compat", "forwarding", "directory", "owner-manager", "maintenance") else []))):
            argv = [sys.executable, "--version"] if tool == "python" else [tool, "version" if tool == "go" else ("-vV" if tool == "rustc" else "--version")]
            code, output, expired = run_process(argv, 30, env, ROOT)
            if code or expired:
                raise ValueError(f"cannot record tool version: {tool}")
            report["tool_versions"][tool] = output.strip()
            required = manifest.get("required_tool_prefixes", {}).get(tool)
            if required and not output.strip().startswith(required):
                raise ValueError(f"tool version mismatch: {tool} requires {required}")
        for index, (argv, expected, runner) in enumerate(commands, 1):
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
            if runner == "go-node-build":
                report["native_build"] = json.loads((ROOT / ".local/go-node-build.json").read_text())
                report["node_binary_sha256"] = report["native_build"]["node_binary_sha256"]
            elif runner == "cargo-build-node":
                binary = ROOT / "target/debug/xenon-node"
                if not binary.is_file():
                    raise ValueError("node build produced no binary")
                report["rust_node_binary_sha256" if args.name == "go-shard-compat" else "node_binary_sha256"] = digest(binary)
            elif runner in ("s3-owner-manager", "s3-maintenance"):
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/ownership")
                if digest(ROOT / ".local/bin/xenon-go-node") != report["node_binary_sha256"]:
                    raise ValueError("node binary changed during test")
            elif runner == "s3-directory":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/directory")
            elif runner == "go-test-routing":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/routing")
            elif runner in ("go-test-shard", "go-test-node", "go-shard-compat"):
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/node" if runner == "go-test-node" else "github.com/0x63616c/xenon/internal/adapter")
                binary = ROOT / (".local/bin/xenon-go-node" if args.name in ("go-shard", "go-namespace", "go-cluster", "go-queue", "go-history", "go-nexus", "go-matching", "go-matching-userdata", "go-queuev2", "go-persistence", "go-runtime-stores", "go-history-routing", "go-fair", "go-execution", "go-historytasks", "go-executiontasks", "go-shard-compat", "owner-manager", "maintenance") else "target/debug/xenon-node")
                if runner == "go-shard-compat" and digest(ROOT / "target/debug/xenon-node") != report["rust_node_binary_sha256"]:
                    raise ValueError("Rust reference binary changed during test")
                if digest(binary) != report.get("node_binary_sha256"):
                    raise ValueError("node binary changed during tests")
            else:
                verify_tests(output, expected)
        # Detect edits made during execution rather than assigning them the starting hash.
        for relative, expected_hash in report["config_sha256"].items():
            if digest(ROOT / relative) != expected_hash:
                raise ValueError(f"input changed while experiment ran: {relative}")
        if git("rev-parse", "HEAD") != sha or git("status", "--porcelain=v1", "--untracked-files=all") != dirty:
            raise ValueError("checkout changed while experiment ran")
        if cargo_configs(ROOT, env):
            raise ValueError("ambient Cargo config appeared during execution")
        if args.name in ("shard", "go-shard-compat") and digest(ROOT / ".local/protoc/bin/protoc") != report["protoc_binary_sha256"]:
            raise ValueError("compiler binary changed during experiment")
        if args.name in ("go-shard", "go-namespace", "go-cluster", "go-queue", "go-history", "go-nexus", "go-matching", "go-matching-userdata", "go-queuev2", "go-persistence", "go-runtime-stores", "go-history-routing", "go-fair", "go-execution", "go-historytasks", "go-executiontasks", "go-shard-compat", "owner-manager", "maintenance"):
            native_build = report["native_build"]
            source = ROOT / ".local/slatedb-native-source"
            for argv, expected in [(["git", "rev-parse", "HEAD"], native_build["source_commit"]), (["git", "status", "--porcelain=v1", "--untracked-files=all"], "")]:
                code, output, expired = run_process(argv, 30, env, source)
                if code or expired or output.strip() != expected:
                    raise ValueError("native source changed during experiment")
            if cargo_configs(source, env):
                raise ValueError("native source Cargo config appeared during experiment")
            if digest(ROOT / native_build["shared_library"]) != native_build["shared_library_sha256"]:
                raise ValueError("native library changed during experiment")
        report["result"], report["proof_pass"] = classify_success(bool(dirty))
    except Exception as error:
        report["error"] = str(error)
    finally:
        if args.name == "go-shard-compat":
            project = env["XENON_COMPAT_PROJECT"]
            try:
                code, output, expired = run_process(["docker", "compose", "--project-name", project, "-f", "deploy/compat.compose.yaml", "down", "--volumes"], 60, env, ROOT)
                report["cleanup"] = {"project": project, "exit_code": code, "timed_out": expired}
            except Exception as error:
                report["cleanup"] = {"project": project, "exit_code": -1, "timed_out": False, "error": str(error)}
            if report["cleanup"]["exit_code"] or report["cleanup"]["timed_out"]:
                report.update(result="failed", proof_pass=False, error="compatibility Compose cleanup failed")
        if args.name in ("directory", "owner-manager", "maintenance"):
            env_key = {"directory":"XENON_DIRECTORY_PROJECT", "owner-manager":"XENON_OWNER_MANAGER_PROJECT", "maintenance":"XENON_MAINTENANCE_PROJECT"}[args.name]
            compose_path = "deploy/" + args.name + ".compose.yaml"
            try:
                code, output, expired = run_process(["docker", "compose", "--project-name", env[env_key], "-f", compose_path, "down", "--volumes"], 60, env, ROOT)
                report["cleanup"] = {"exit_code": code, "timed_out": expired}
            except Exception as error:
                report["cleanup"] = {"exit_code": -1, "timed_out": False, "error": str(error)}
            if report["cleanup"]["exit_code"] or report["cleanup"]["timed_out"]:
                report.update(result="failed", proof_pass=False, error="directory cleanup failed")
        if args.name == "crash":
            # Out-of-process cleanup also handles a controller killed before finally.
            report["cleanup"] = cleanup_crash(env["XENON_PROOF_PROJECT"], env, ROOT)
            if report["cleanup"]["exit_code"] or report["cleanup"]["timed_out"]:
                report.update(result="failed", proof_pass=False, error="scoped Compose cleanup failed")
        (evidence / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"{report['result'].upper()}: {evidence / 'result.json'}", flush=True)
    return 0 if report["result"] in ("passed", "development-passed") else 1


if __name__ == "__main__":
    sys.exit(main())
