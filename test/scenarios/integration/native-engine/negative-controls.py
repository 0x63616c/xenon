#!/usr/bin/env python3
"""Mutate the production native adapter; require each exact invariant to fail."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[4]

def replace_once(source, old, new):
    if source.count(old) != 1:
        raise RuntimeError("mutation anchor changed: " + old[:80])
    return source.replace(old, new, 1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    source = (ROOT / "internal/partitions/slatedb/engine.go").read_text()
    false_commit = replace_once(source,
        "\t\terr = t.w.failure(err)\n\t\tif err == nil && (h == nil || *h == nil)",
        "\t\terr = t.w.failure(err)\n\t\tif errors.Is(err, p.ErrFenced) { committed <- nil; close(r.done); <-decision; <-t.w.gate; return }\n\t\tif err == nil && (h == nil || *h == nil)")
    false_commit = replace_once(false_commit,
        "err = t.w.failure((*h).AwaitDurable())",
        "err = t.w.failure((*h).AwaitDurable()); if errors.Is(err, p.ErrFenced) { err = nil }")
    mutants = [
        ("stale_commit_success", false_commit,
         "TestNativeEngineFence", "stale user mutation was acknowledged"),
        ("premature_durable_read", replace_once(source,
         "\tcap, err := tx.Commit(ctx)\n\tif err != nil {",
         "\treturn result, nil\n\tcap, err := tx.Commit(ctx)\n\tif err != nil {"),
         "TestNativeEngineReadBarrier", "empty read bypassed durability"),
        ("lost_replay_result", replace_once(source,
         "func (t *transaction) Put(key, value []byte) error {",
         'func (t *transaction) Put(key, value []byte) error {\n\tif string(key) == "outcome" { return nil }'),
         "TestNativeEngineAtomicRecovery", "lost replay result"),
        ("close_while_in_use", replace_once(source,
         "func (w *writer) Close(ctx context.Context) error {",
         "func (w *writer) Close(ctx context.Context) error {\n\treturn nil"),
         "TestNativeEnginePendingLifecycle", "closed active native handle"),
    ]
    args.evidence.mkdir(parents=True, exist_ok=False)
    report = {"source_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "source_sha256": hashlib.sha256(source.encode()).hexdigest(), "results": []}
    env = dict(os.environ, GOENV="off", GOWORK="off", GOFLAGS="-mod=readonly", GOTOOLCHAIN="go1.27.1")
    try:
        for name, mutated, test, assertion in mutants:
            with tempfile.TemporaryDirectory(prefix="xenon-native-mutant-") as temporary:
                work = Path(temporary)
                for filename in ("go.mod", "go.sum"):
                    shutil.copy2(ROOT / filename, work / filename)
                for package in ("identity", "partitions"):
                    shutil.copytree(ROOT / "internal" / package, work / "internal" / package)
                (work / "internal/partitions/slatedb/engine.go").write_text(mutated)
                case_env = env.copy()
                if case_env.get("XENON_ENGINE_STORE", "").startswith("s3://"):
                    case_env["XENON_ENGINE_STORE"] += "-" + name.replace("_", "-")
                command = ["go", "test", "-race", "-count=1", "-timeout=45s", "-run=^"+test+"$", "./internal/partitions/slatedb"]
                result = subprocess.run(command, cwd=work, env=case_env, text=True, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, timeout=55)
                (args.evidence / (name+".log")).write_text(result.stdout)
                caught = result.returncode != 0 and assertion in result.stdout
                report["results"].append({"name": name, "command": command, "returncode": result.returncode,
                                          "caught": caught, "assertion": assertion,
                                          "mutated_source_sha256": hashlib.sha256(mutated.encode()).hexdigest()})
                if not caught:
                    raise RuntimeError("control missed intended invariant: " + name)
        report["status"] = "passed"
    except BaseException as error:
        report.update(status="failed", failure=str(error))
        raise
    finally:
        (args.evidence / "result.json").write_text(json.dumps(report, indent=2)+"\n")
    print(json.dumps({"negative_controls": "passed", "results": str(args.evidence / "result.json")}), flush=True)

if __name__ == "__main__":
    main()
