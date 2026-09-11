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

# Full acceptance is deliberately separate from component experiments. Until a
# gate has an executable verifier, its registration can only produce a non-pass.
ACCEPTANCE_CRITERIA = {
    "cli-contracts": ["CLI-01", "command-exit-compatibility"],
    "workflow-search-dst": ["GEN-01", "DST-01", "FAULT-01:simulation", "ORACLE-01:mutants", "STOP-01", "CLEAN-01:controls"],
    "workflow-search-real": ["CLI-02", "GEN-02", "FAULT-01:real", "ORACLE-01:real", "CLEAN-01:real", "EVID-01"],
    "workflow-replay-minimize": ["REPLAY-01:simulation", "REPLAY-01:real", "MIN-01:simulation", "MIN-01:real"],
    "issue-119-acceptance": ["cli-contracts", "workflow-search-dst", "workflow-search-real", "workflow-replay-minimize", "source-layout", "evidence-validation"],
}

# These suites are executable component evidence for acceptance gates whose
# remaining obligations are still recorded in their registration manifests.
# Passing one of these suites must never be promoted to an acceptance pass.
ACCEPTANCE_COMPONENT_TESTS = {
    "cli-contracts": {
        "package": "./cmd/xenon",
        "required_output": {
            "CLI_CONTRACT_MATRIX": {"rows": 46, "help_paths": 23, "config_sources": 1},
            "CLI_EFFECT_NEGATIVE_CONTROLS": {"boundaries": 4, "counters": 4},
        },
        "expected_tests": [
            "TestLightweightCommandsNeverStartBackend",
            "TestCommandErrorsUseOnlyStderrAndDoNotStart",
            "TestStartReceivesValidatedConfigAndCallerCancellation",
            "TestCanceledInvocationCannotStart",
            "TestOutputFailureCannotStart",
            "TestPackagedExamplePassesCLIValidation",
            "TestDevCLIValidatesBeforeEffects",
            "TestInspectCommandValidationAndDeadline",
            "TestInspectUnavailableRemainsNonzeroJSON",
            "TestCheckConfigJSONContract",
            "TestEveryCommandHelpIsPassive",
            "TestSimulationHelpDoesNotReadInputsOrStartBackend",
            "TestResidentWorkflowHelpDoesNotStartBackend",
            "TestRealProfileHelpNeverInitializesBackend",
            "TestCLIContractMatrixAndZeroEffects",
            "TestCLIContractEffectCountersDetectEveryBoundary",
        ],
    },
    "workflow-search-dst": {
        "package": "./internal/simulation",
        "expected_tests": [
            "TestFreshWorkflowSeedsAndImmutableBundle",
            "TestGeneratedInputDiversityExcludesSeedMetadata",
            "TestGeneratedWorkflowUsesSharedEnvelopeAndReplayWithoutTools",
            "TestCoupledSeededInterleavingsReplay",
            "TestCoupledVirtualDayTakeover",
            "TestCoupledLostPublicationResponse",
            "TestCoupledRenewalRestartsTakeoverSuspicion",
            "TestCoupledUnsupportedFaultRejectedBeforeExecution",
            "TestCoupledPendingOpenAssignmentABA",
            "TestCoupledStorageReadFailureRecovers",
            "TestCoupledRenewalUnknownAfterReplacement",
            "TestCoupledAssignmentABARereservesWriter",
            "TestProductionTransportDropDelayDuplicate",
            "TestCoupledOwnershipCutOccurrenceReceipt",
            "TestSimulationNamedOracleMutants",
            "TestCheckerRejectsNonAtomicOutcome",
            "TestCheckerRejectsStaleOwnerAcknowledgement",
            "TestCoupledCheckerRejectsUnauthorizedReservation",
            "TestFirstFailureBeforeCleanupAndGenerationStops",
            "TestSeparateRNGStreams",
            "TestLogicalBudgetCancelsAndDrainsBeforeNextCase",
            "TestWorkflowBatchStopsQueuedAdmissionOnFailure",
            "TestCleanupCancellationCannotReplaceFirstFailureOutcome",
            "TestInternalOperationContextErrorIsFirstFailure",
            "TestInternalContextFailureOrderingAgainstActualStops",
            "TestSearchWorkloadAndAsyncFailureOrdering",
            "TestSearchHundredCaseResourceBaselineAndEvidenceQuota",
            "TestResidentUncertainCensusRefusesReuse",
            "TestResidentCleanupAccountsForLateCreationBeforeCensus",
            "TestResidentCleanupStagesShareOneLogicalDeadline",
        ],
    },
    "workflow-replay-minimize": {
        "package": "./internal/simulation",
        "expected_tests": [
            "TestReplayRejectsEnvelopeAndProvenanceChangesBeforeEffects",
            "TestReplayLegacyAncestryCannotBecomeExactEvidence",
            "TestMinimizeCoupledArtifactAndDependencyRepair",
            "TestMinimizeCoupledRejectsOversizedEnvelope",
            "TestMinimizeArtifactRejectsInvalidFailureReceipt",
            "TestMinimizeRunnerPreservesSameFailureAndReplay",
            "TestMinimizeRejectsIntermittentAndStopsPending",
            "TestMinimizeInjectedDeadlineAndCancellationPreserveOriginal",
            "TestMinimizeRunnerPredicateProductionCoupledFailure",
        ],
    },
}

ACCEPTANCE_EXECUTABLES = {
    "workflow-search-real": "scripts/workflow-search-real-proof.py",
}

SCENARIO_MANIFESTS = {
    name: "test/scenarios/ministack/manifests/" + name + ".json"
    for name in ("runtime-measurements", "nexus-http", "nexus-readiness", "corrected-fuzz-controls")
}

SCENARIO_MANIFESTS.update({name: "test/scenarios/acceptance/manifests/" + name + ".json" for name in ACCEPTANCE_CRITERIA})

def manifest_path(name, root=None):
    root = ROOT if root is None else root
    return root / SCENARIO_MANIFESTS.get(name, "experiments/" + name + ".json")

def manifest_paths(root=None):
    root = ROOT if root is None else root
    return sorted([*(root / "experiments").glob("*.json"), *(root / "test/scenarios").glob("*/manifests/*.json")])



def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def classify_success(dirty):
    return ("development-passed", False) if dirty else ("passed", True)


def version_matches(output, required):
    """Accept the pinned version line even when a tool manager emits diagnostics first."""
    return any(line.strip().startswith(required) for line in output.splitlines())


def cargo_configs(root, env):
    # Cargo searches the invocation directory and ancestors, then CARGO_HOME.
    dirs = [root, *root.parents]
    home = Path(env.get("CARGO_HOME", str(Path(env["HOME"]) / ".cargo")))
    if not home.is_absolute():
        home = root / home
    paths = [d / ".cargo" / n for d in dirs for n in ("config", "config.toml")]
    paths += [home / n for n in ("config", "config.toml")]
    return sorted({str(p.resolve()) for p in paths if p.is_file()})


def validate_acceptance_manifest(manifest, name, root):
    """Validate registration, never certify criteria from declarations or links."""
    fields = {"schema", "name", "kind", "status", "criteria", "inputs", "child_gates"}
    if set(manifest) != fields or name not in ACCEPTANCE_CRITERIA:
        raise ValueError("invalid acceptance manifest fields or gate name")
    if (type(manifest["schema"]) is not int or manifest["schema"] != 2
            or manifest["name"] != name or manifest["kind"] != "acceptance-registration"
            or manifest["status"] != "incomplete"):
        raise ValueError("acceptance registration cannot assert completion")
    criteria = manifest["criteria"]
    if not isinstance(criteria, dict) or set(criteria) != set(ACCEPTANCE_CRITERIA[name]):
        raise ValueError("acceptance criteria missing or unexpected")
    for criterion, detail in criteria.items():
        if (not isinstance(detail, dict) or set(detail) != {"status", "missing"}
                or detail["status"] != "unverified" or not isinstance(detail["missing"], str)
                or not detail["missing"].strip()):
            raise ValueError(f"criterion {criterion} needs an explicit unverified obligation")
    children = list(ACCEPTANCE_CRITERIA)[:-1] if name == "issue-119-acceptance" else []
    if manifest["child_gates"] != children:
        raise ValueError("acceptance child gates missing or unexpected")
    inputs = manifest["inputs"]
    if (not isinstance(inputs, list) or not inputs or any(not isinstance(p, str) for p in inputs)
            or len(set(inputs)) != len(inputs)
            or "docs/design/issue-119-acceptance.md" not in inputs):
        raise ValueError("acceptance source inputs must include the authoritative spec")
    for relative in inputs:
        path = (root / relative).resolve()
        if Path(relative).is_absolute() or not path.is_relative_to(root.resolve()) or not path.is_file():
            raise ValueError(f"missing or invalid acceptance input: {relative}")


def run_acceptance_registration(name, root):
    """Emit reproducible refusal; component receipts are not acceptance receipts."""
    report = {"schema": 2, "gate": name, "result": "incomplete", "proof_pass": False,
              "acceptance_pass": False, "executed_tests": 0, "criteria": {},
              "child_gates": [], "config_sha256": {}}
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + name + "-" + uuid.uuid4().hex[:8]
    evidence = root / ".local/evidence" / run_id
    evidence.mkdir(parents=True)
    try:
        report["commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
        report["dirty_status"] = subprocess.check_output(["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=root, text=True).strip()
        inputs = {"scripts/prove.py"}
        # Validate the complete registry, including every aggregate dependency.
        for gate in ACCEPTANCE_CRITERIA:
            path = manifest_path(gate, root)
            manifest = json.loads(path.read_text())
            validate_acceptance_manifest(manifest, gate, root)
            inputs.add(str(path.relative_to(root)))
            inputs.update(manifest["inputs"])
            if gate == name:
                report["criteria"] = manifest["criteria"]
                report["child_gates"] = [{"gate": child, "result": "unverified", "receipt_verified": False}
                                         for child in manifest["child_gates"]]
        report["config_sha256"] = {p: digest(root / p) for p in sorted(inputs)}
        report["error"] = "gate incomplete: no registered executable acceptance verifier; zero tests executed"
    except Exception as error:
        report.update(result="failed", error=str(error))
    path = evidence / "result.json"
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"{report['result'].upper()}: {path}", flush=True)
    return 1


def validate_native_build(root):
    pins = json.loads((root / "tools/slatedb-native.json").read_text())
    receipt_path = root / ".local/go-node-build.json"
    receipt = json.loads(receipt_path.read_text())
    library = (root / receipt["shared_library"]).resolve()
    if (not library.is_relative_to((root / ".local").resolve()) or not library.is_file()
            or receipt.get("source_commit") != pins["source_commit"]
            or receipt.get("binding_module") != pins["go_module"]
            or receipt.get("binding_version") != pins["go_version"]
            or receipt.get("source_clean") is not True
            or receipt.get("shared_library_sha256") != digest(library)):
        raise ValueError("native build attestation mismatch")
    return receipt_path, receipt, library


def run_acceptance_component(name, root, allow_dirty=False):
    """Execute registered component checks while refusing a full gate pass."""
    plan = ACCEPTANCE_COMPONENT_TESTS[name]
    expected = plan["expected_tests"]
    pattern = "^(" + "|".join(re.escape(test) for test in expected) + ")$"
    argv = ["go", "test", "-race", "-json", "-count=1", plan["package"], "-run", pattern]
    report = {"schema": 2, "gate": name, "result": "failed", "component_pass": False,
              "proof_pass": False, "acceptance_pass": False, "executed_tests": 0,
              "criteria": {}, "child_gates": [], "config_sha256": {}, "commands": [],
              "tool_versions": {}}
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + name + "-" + uuid.uuid4().hex[:8]
    evidence = root / ".local/evidence" / run_id
    evidence.mkdir(parents=True)
    try:
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
        dirty = subprocess.check_output(["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=root, text=True).strip()
        report.update(commit=sha, dirty_status=dirty, source_clean=not bool(dirty))
        if dirty and not allow_dirty:
            raise ValueError("dirty checkout cannot produce acceptance component evidence; use --allow-dirty for development")
        if dirty:
            patch = subprocess.check_output(["git", "diff", "--binary", "--no-ext-diff", "HEAD"], cwd=root)
            report["tracked_patch_sha256"] = hashlib.sha256(patch).hexdigest()
        inputs = {"scripts/prove.py", "scripts/build-go-node.py", "tools/slatedb-native.json", "go.mod", "go.sum"}
        for gate in ACCEPTANCE_CRITERIA:
            path = manifest_path(gate, root)
            manifest = json.loads(path.read_text())
            validate_acceptance_manifest(manifest, gate, root)
            inputs.add(str(path.relative_to(root)))
            inputs.update(manifest["inputs"])
            if gate == name:
                report["criteria"] = manifest["criteria"]
        report["config_sha256"] = {p: digest(root / p) for p in sorted(inputs)}
        env = os.environ.copy()
        started = time.monotonic()
        code, output, expired = run_process([sys.executable, "scripts/build-go-node.py"], 900, env, root)
        build_log = evidence / "command-1.log"
        build_log.write_text(output)
        report["commands"].append({"argv": [sys.executable, "scripts/build-go-node.py"],
                                   "exit_code": code, "timed_out": expired,
                                   "elapsed_seconds": round(time.monotonic() - started, 3),
                                   "output": build_log.name, "output_sha256": digest(build_log),
                                   "expected_tests": []})
        if code or expired:
            raise ValueError(f"native build failed (exit={code}, timeout={expired})")
        receipt_path, native, library = validate_native_build(root)
        report["native_build"] = native
        report["native_build_receipt_sha256"] = digest(receipt_path)
        target = library.parent
        env.update({"GOENV": "off", "GOWORK": "off", "GOFLAGS": "-mod=readonly",
                    "GOTOOLCHAIN": "go1.27.1", "CGO_ENABLED": "1",
                    "CGO_LDFLAGS": "-L" + str(target), "LD_LIBRARY_PATH": str(target),
                    "DYLD_LIBRARY_PATH": str(target), "SLATEDB_UNIFFI_RUNTIME_THREADS": "2"})
        code, version, expired = run_process(["go", "version"], 30, env, root)
        if code or expired or not version_matches(version, "go version go1.27.1 "):
            raise ValueError("pinned Go toolchain unavailable")
        report["tool_versions"]["go"] = version.strip()
        started = time.monotonic()
        code, output, expired = run_process(argv, 300, env, root)
        log = evidence / "command-2.log"
        log.write_text(output)
        report["commands"].append({"argv": argv, "exit_code": code, "timed_out": expired,
                                   "elapsed_seconds": round(time.monotonic() - started, 3),
                                   "output": log.name, "output_sha256": digest(log),
                                   "expected_tests": expected})
        if code or expired:
            raise ValueError(f"component command failed (exit={code}, timeout={expired})")
        verify_go_top_level_tests(output, expected, "github.com/0x63616c/xenon/" + plan["package"].removeprefix("./"))
        summaries = {}
        for label, required in plan.get("required_output", {}).items():
            match = re.search(r"\b" + re.escape(label) + r"((?: [a-z_]+=[0-9]+)+)", output)
            if match is None:
                raise ValueError(f"missing {label} executable summary")
            actual = {key: int(value) for key, value in re.findall(r"([a-z_]+)=([0-9]+)", match.group(1))}
            if actual != required:
                raise ValueError(f"{label} summary mismatch: {actual} != {required}")
            summaries[label] = actual
        if summaries:
            report["executable_summaries"] = summaries
        for relative, expected_hash in report["config_sha256"].items():
            if digest(root / relative) != expected_hash:
                raise ValueError(f"input changed while component checks ran: {relative}")
        if (subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip() != sha
                or subprocess.check_output(["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=root, text=True).strip() != dirty):
            raise ValueError("checkout changed while component checks ran")
        report.update(result="incomplete" if not dirty else "development-incomplete",
                      component_pass=not bool(dirty), development_checks_passed=True,
                      executed_tests=len(expected),
                      error="component checks passed; acceptance obligations remain unverified")
    except Exception as error:
        report["error"] = str(error)
    path = evidence / "result.json"
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"{report['result'].upper()}: {path}", flush=True)
    return 1


def command(spec):
    if spec == {"runner": "go-node-build"}:
        return [sys.executable, "scripts/build-go-node.py"]
    if spec == {"runner": "cargo-build-node"}:
        return ["cargo", "build", "--manifest-path", "test/compatibility/rust/Cargo.toml", "--target-dir", "target", "--locked", "-p", "xenon-node"]
    if set(spec) != {"runner", "filter", "exact", "expected_tests"}:
        raise ValueError("invalid command fields")
    runner = spec["runner"]
    if runner not in ("cargo-test", "cargo-test-node", "go-test-shard", "go-test-node", "go-test-engine", "go-test-visibility", "go-test-factory", "s3-crash", "s3-crash-cleanup", "go-test-routing", "go-test-simulation", "go-test-rpctrace", "go-test-recorder", "go-test-mixed-oracle", "go-test-observer", "go-test-observer-adapter", "go-test-nexus-config", "go-check-nexus-config", "go-test-sdk-readiness", "go-test-visibility-barrier", "go-test-fanout", "go-test-trace-adapter", "go-test-outcomes", "go-shard-compat", "s3-directory", "s3-owner-manager", "s3-maintenance", "go-test-meter", "go-test-cas-loss", "go-test-meter-cli", "s3-meter", "python-measurements", "go-test-process-cut", "s3-process-cut", "go-test-registry") or not isinstance(spec["exact"], bool):
        raise ValueError("only registered structured test commands are allowed")
    if not isinstance(spec["filter"], str) or not re.fullmatch(r"[a-zA-Z0-9_:]+", spec["filter"]):
        raise ValueError("invalid test filter")
    tests = spec["expected_tests"]
    if not isinstance(tests, list) or not tests or len(set(tests)) != len(tests):
        raise ValueError("expected_tests must be a nonempty unique list")
    if any(not isinstance(t, str) or not re.fullmatch(r"[a-zA-Z0-9_:/-]+", t) for t in tests):
        raise ValueError("invalid expected test name")
    if runner == "go-test-process-cut":
        if spec["filter"] not in ("TestCutIdentityAndOneShot", "TestDiscoveryExactCandidate", "TestDiscoveryLateArmRejected") or not spec["exact"]:
            raise ValueError("unregistered process-cut controls")
        return ["go", "test", "-race", "-json", "-count=1", "./internal/processcut", "-run", "^" + spec["filter"] + "$"]
    if runner == "s3-process-cut":
        if spec["filter"] not in ("TestS3ProcessCuts", "TestS3ExecutionDiscoveryCuts") or not spec["exact"]:
            raise ValueError("unregistered process-cut S3 fixture")
        return [sys.executable, "scripts/process-cut-proof.py", spec["filter"]]
    if runner == "python-measurements":
        if not spec["exact"] or spec["filter"] not in ("test_runtime_measurements", "test_ministack_runtime", "test_resource_samples", "test_scenario_layout", "test_recorder_lifecycle", "test_corrected_fuzz"):
            raise ValueError("unregistered runtime measurement controls")
        return [sys.executable, "-m", "unittest", "discover", "-s", "scripts", "-p", spec["filter"] + ".py", "-v"]
    if runner == "go-test-meter-cli":
        if spec["filter"] != "TestMeterCLI" or not spec["exact"]:
            raise ValueError("unregistered meter CLI control")
        return ["go", "test", "-race", "-json", "-count=1", "./cmd/xenon-s3-meter", "-run", "^TestMeterCLI$"]
    if runner == "go-test-cas-loss":
        if spec["filter"] != "TestCASLossHTTP" or not spec["exact"]: raise ValueError("unregistered CAS loss test")
        return ["go","test","-race","-json","-count=1","./internal/proof/s3meter","-run","^TestCASLossHTTP$"]
    if runner == "go-test-meter":
        if spec["filter"] != "TestMeter" or spec["exact"]:
            raise ValueError("unregistered meter controls")
        return ["go", "test", "-race", "-json", "-count=1", "./internal/proof/s3meter", "-run", "^TestMeter"]
    if runner == "s3-meter":
        if spec["filter"] not in ("TestMeterSignedS3", "TestCASLossSignedS3") or not spec["exact"]:
            raise ValueError("unregistered signed meter test")
        return [sys.executable, "scripts/s3-meter-proof.py", spec["filter"]]
    if runner == "s3-maintenance":
        if spec["filter"] != "TestS3Maintenance" or not spec["exact"]:
            raise ValueError("unregistered maintenance test")
        return [sys.executable, "scripts/maintenance-proof.py"]
    if runner == "s3-owner-manager":
        if spec["filter"] != "TestS3OwnerManager" or not spec["exact"]:
            raise ValueError("unregistered owner manager test")
        return [sys.executable, "scripts/owner-manager-proof.py"]
    if runner == "go-test-registry":
        if spec["filter"] not in ("registry", "filesystem") or spec["exact"]: raise ValueError("unregistered registry contract tests")
        if spec["filter"] == "filesystem":
            # Participant is a subprocess entrypoint, not a standalone test.
            return ["go", "test", "-race", "-json", "-count=1", "-timeout=90s", "-run", "^Test(Contract|RecoveryErrorsAndProtocol|ContainmentCorruptionCancellation|ProcessCrashRecoveryAndCAS|SyncSyscallBoundaries|CancellationAfterRenameAndSyncFailure)$", "./internal/registry/filesystem"]
        return ["go", "test", "-race", "-json", "-count=1", "./internal/registry/s3"]
    if runner == "s3-directory":
        if spec["filter"] not in ("TestS3Directory", "TestS3Registry") or not spec["exact"]:
            raise ValueError("unregistered directory test")
        return [sys.executable, "scripts/directory-proof.py", *(["registry"] if spec["filter"] == "TestS3Registry" else [])]
    if runner in ("s3-crash", "s3-crash-cleanup"):
        if spec["filter"] != "crash" or spec["exact"]:
            raise ValueError("invalid crash command")
        return [sys.executable, "scripts/crash-proof.py", *(["--check-cleanup"] if runner == "s3-crash-cleanup" else [])]
    if runner == "go-test-outcomes":
        if not spec["exact"] or spec["filter"] != "TestOutcomeMetrics":
            raise ValueError("unregistered outcome metrics test")
        return ["go", "test", "-race", "-json", "-count=1", "./internal/ownership", "-run", "^TestOutcomeMetrics$"]
    if runner == "go-test-trace-adapter":
        if not spec["exact"] or spec["filter"] != "TestRPCTraceInvocationAndAttempts":
            raise ValueError("unregistered trace adapter proof")
        return ["go", "test", "-race", "-json", "-count=1", "./internal/temporal/adapter", "-run", "^" + spec["filter"] + "$"]
    if runner in ("go-test-observer", "go-test-observer-adapter"):
        names = ("TestObserverConcurrentRegistration", "TestObserverConfiguration") if runner == "go-test-observer" else ("TestRecorderAdapterTimingAndLoss",)
        if not spec["exact"] or spec["filter"] not in names:
            raise ValueError("unregistered observer proof")
        return ["go", "test", "-race", "-json", "-count=1", "./internal/rpctrace" if runner == "go-test-observer" else "./internal/temporal/adapter", "-run", "^" + spec["filter"] + "$"]
    if runner == "go-test-fanout":
        if not spec["exact"] or spec["filter"] != "TestVisibilityIndependentFanout":
            raise ValueError("unregistered visibility fanout proof")
        return ["go", "test", "-race", "-json", "-count=1", "./internal/temporal/adapter", "-run", "^TestVisibilityIndependentFanout$"]
    if runner == "go-test-mixed-oracle":
        if not spec["exact"] or spec["filter"] not in ("TestMixedSemantics", "TestMixedNexus", "TestMixedNexusCanceled"):
            raise ValueError("unregistered mixed oracle control")
        return ["go", "test", "-race", "-json", "-count=1", "./cmd/xenon-omes-oracle", "-run", "^" + spec["filter"] + "$"]
    if runner == "go-test-visibility-barrier":
        if not spec["exact"] or spec["filter"] not in ("TestVisibilityMovementBarrier", "TestSeedPartitions", "TestSeedPartitionsFailureJoinsPeers"):
            raise ValueError("unregistered visibility barrier control")
        return ["go", "test", "-race", "-json", "-count=1", "./cmd/xenon-visibility-probe", "-run", "^" + spec["filter"] + "$"]
    if runner == "go-test-sdk-readiness":
        if not spec["exact"] or spec["filter"] not in ("TestNexusReadinessShardCoverage", "TestNexusReadinessWorkflowEcho", "TestReadinessWorkerPollerScope", "TestNexusHistoryDiagnostic"):
            raise ValueError("unregistered SDK readiness proof")
        return ["go", "test", "-race", "-json", "-count=1", "./cmd/xenon-sdk-probe", "-run", "^" + spec["filter"] + "$"]
    if runner == "go-test-nexus-config":
        if not spec["exact"] or spec["filter"] != "TestNexusHTTPConfiguration":
            raise ValueError("unregistered Nexus config proof")
        return ["go", "test", "-race", "-json", "-count=1", "./internal/ministack", "-run", "^TestNexusHTTPConfiguration$"]
    if runner == "go-check-nexus-config":
        if not spec["exact"] or spec["filter"] not in ("a", "b") or spec["expected_tests"] != ["TEMPORAL_CONFIG_VALID"]:
            raise ValueError("unregistered actual config check")
        return ["go", "run", "-tags", "ministack", "./cmd/xenon-temporal", "--config", "test/scenarios/ministack/config/temporal-" + spec["filter"] + ".json", "--check-config"]
    if runner == "go-test-recorder":
        if not spec["exact"] or spec["filter"] not in ("TestRecorderKillAndLostAcknowledgment", "TestRecorderSequenceCapacityAndSteady", "TestRecorderRejectsMissingFooter", "TestRecorderCompletedPopulationAndMalformedJournal", "TestRecorderProcessLoss", "TestRecorderMeasurementLinkage", "TestRecorderHTTPProtocolFailure"):
            raise ValueError("unregistered recorder proof")
        return ["go", "test", "-race", "-json", "-count=1", "./internal/proof/recorder", "-run", "^" + spec["filter"] + "$"]
    if runner == "go-test-rpctrace":
        if not spec["exact"] or spec["filter"] not in ("TestTraceOverflowAndDrain", "TestTraceWriteFailure", "TestTraceFinalizationFailures"):
            raise ValueError("unregistered trace proof")
        return ["go", "test", "-race", "-json", "-count=1", "./internal/rpctrace", "-run", "^" + spec["filter"] + "$"]
    if runner == "go-test-routing":
        if not spec["exact"] or spec["filter"] not in ("TestForwardingReplayAndRefresh", "TestForwardingLoopsAndDeadline", "TestForwardingClosedAdmission"):
            raise ValueError("unregistered routing test")
        return ["go", "test", "-race", "-json", "-count=1", "./internal/routing", "-run", "^" + spec["filter"] + "$"]
    if runner == "go-test-simulation":
        if not spec["exact"] or spec["filter"] not in ("TestDeterministicCoordinationLostResponseCrashMove", "TestCheckerRejectsNonAtomicOutcome", "TestCheckerRejectsStaleOwnerAcknowledgement"):
            raise ValueError("unregistered simulation test")
        return ["go", "test", "-race", "-json", "-count=1", "./internal/simulation", "-run", "^" + spec["filter"] + "$"]
    if runner == "go-shard-compat":
        if spec["filter"] != "TestShardStoredCompatibility" or not spec["exact"]:
            raise ValueError("unregistered compatibility test")
        return [sys.executable, "scripts/shard-compat-proof.py"]
    if runner == "go-test-factory":
        if not spec["exact"] or spec["filter"] != "TestVisibilityFactoryConfiguration":
            raise ValueError("unregistered factory configuration test")
        return ["go", "test", "-json", "-count=1", "./internal/temporal/adapter", "-run", "^" + spec["filter"] + "$"]
    if runner == "go-test-visibility":
        if not spec["exact"] or spec["filter"] not in ("TestVisibilityTypedEvaluation", "TestTextPostgreSQLOracle", "TestVisibilitySystemTimeSentinel", "TestVisibilityCustomTimeRounding"):
            raise ValueError("unregistered visibility value test")
        return ["go", "test", "-json", "-count=1", "./internal/visibility", "-run", "^" + spec["filter"] + "$"]
    if runner == "go-test-engine":
        if spec["filter"] != "TestNativeEngine" or spec["exact"]: raise ValueError("unregistered native engine contract tests")
        return [sys.executable, "scripts/native-engine-proof.py"]
    if runner == "go-test-node":
        if not spec["exact"] or not (spec["filter"].startswith("TestGoOwner") or spec["filter"] in ("TestCanceledAdmissionDoesNotRetireOwner", "TestJournalResultBarrier", "TestMatchingManagedBurst", "TestExecutionResultBarrier")):
            raise ValueError("unregistered Go owner test")
        return ["go", "test", "-json", "-count=1", "./internal/node", "-run", "^" + spec["filter"] + "$"]
    if runner == "go-test-shard":
        if not spec["exact"] or spec["filter"] not in ("TestRPCTraceInvocationAndAttempts", "TestShardRPC", "TestShardTransportBoundsAndTypes", "TestNamespaceRPC", "TestNamespaceByteBoundedPagination", "TestQueueRPC", "TestQueueByteBoundedPagination", "TestQueueRemoteCancellation", "TestHistoryRPC", "TestHistoryTimeoutTypes", "TestHistoryByteBoundedPagination", "TestNexusTransport", "TestExecutionRPC", "TestExecutionTasksRPC", "TestExecutionTasksUpstream", "TestHistoryTasksRPC", "TestHistoryPartitionDeadline", "TestHistoryPartitionInvalidCursor", "TestVisibilityRPC", "TestVisibilityUpstream"):
            raise ValueError("unregistered Go test")
        return ["go", "test", "-json", "-count=1", "./internal/temporal/adapter", "-run", "^" + spec["filter"] + "$"]
    package = "xenon-node" if runner == "cargo-test-node" else "slatedb-probe"
    return ["cargo", "test", "--manifest-path", "test/compatibility/rust/Cargo.toml", "--target-dir", "target", "--locked", "-p", package, "--lib", spec["filter"], "--", *(["--exact"] if spec["exact"] else []), "--nocapture"]


def verify_go_tests(output, expected, package="github.com/0x63616c/xenon/internal/temporal/adapter"):
    if not expected or len(set(expected)) != len(expected):
        raise ValueError("Go expected tests must be nonempty and unique")
    events = [json.loads(line) for line in output.splitlines() if line.strip() and not line.startswith("go: downloading ")]
    if any(event.get("Action") in ("skip", "fail") for event in events):
        raise ValueError("Go test skipped or failed")
    actual = [event["Test"] for event in events if event.get("Action") == "pass" and "Test" in event]
    packages = [event for event in events if event.get("Action") == "pass" and "Test" not in event]
    if sorted(actual) != sorted(expected) or len(packages) != 1 or packages[0].get("Package") != package:
        raise ValueError(f"Go test assertions mismatch: expected {expected}, observed {actual}")


def verify_go_top_level_tests(output, expected, package):
    """Count exact public tests while still failing on any nested skip/failure."""
    if not expected or len(set(expected)) != len(expected):
        raise ValueError("Go expected tests must be nonempty and unique")
    events = [json.loads(line) for line in output.splitlines() if line.strip() and not line.startswith("go: downloading ")]
    if any(event.get("Action") in ("skip", "fail") for event in events):
        raise ValueError("Go test skipped or failed")
    actual = [event["Test"] for event in events
              if event.get("Action") == "pass" and "Test" in event and "/" not in event["Test"]]
    packages = [event for event in events if event.get("Action") == "pass" and "Test" not in event]
    if sorted(actual) != sorted(expected) or len(packages) != 1 or packages[0].get("Package") != package:
        raise ValueError(f"Go top-level assertions mismatch: expected {expected}, observed {actual}")


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
    parser.add_argument("name", choices=["native-engine-contracts", "corrected-fuzz-controls", "primitive", "ownership", "simulation", "shard", "crash", "go-bindings", "go-shard", "go-namespace", "go-cluster", "go-queue", "go-history", "go-nexus", "go-matching", "go-matching-userdata", "go-queuev2", "go-persistence", "go-runtime-stores", "go-history-routing", "go-outcomes", "go-visibility", "go-visibility-frozen", "go-fair", "go-execution", "go-historytasks", "go-executiontasks", "go-shard-compat", "forwarding", "rpc-measurement", "external-recorder", "mixed-oracle", "recorder-adapter", "recorder-lifecycle", "visibility-movement", "nexus-http", "nexus-readiness", "visibility-fanout", "registry-contracts", "directory", "owner-manager", "maintenance", "s3-meter", "cas-loss", "runtime-measurements", "process-cut", *ACCEPTANCE_CRITERIA])
    parser.add_argument("--allow-dirty", action="store_true", help="development only; evidence is marked non-reproducible")
    args = parser.parse_args()
    if args.name in ACCEPTANCE_EXECUTABLES:
        if args.allow_dirty:
            print("workflow-search-real requires a clean committed checkout", file=sys.stderr)
            return 1
        return subprocess.call([sys.executable, ACCEPTANCE_EXECUTABLES[args.name]], cwd=ROOT)
    if args.name in ACCEPTANCE_COMPONENT_TESTS:
        return run_acceptance_component(args.name, ROOT, args.allow_dirty)
    if args.name in ACCEPTANCE_CRITERIA:
        return run_acceptance_registration(args.name, ROOT)
    if args.name == "go-bindings":
        return subprocess.call([sys.executable, str(ROOT / "scripts/prove-go-bindings.py"), *(["--allow-dirty"] if args.allow_dirty else [])], cwd=ROOT)
    selected_manifest = manifest_path(args.name)
    manifest = json.loads(selected_manifest.read_text())
    if manifest["schema"] != 1 or manifest["name"] != args.name or manifest["backend"] != ("s3-emulator" if args.name in ("native-engine-contracts", "crash", "go-shard-compat", "registry-contracts", "directory", "owner-manager", "maintenance", "s3-meter", "cas-loss", "process-cut") else "memory"):
        raise ValueError("unsupported manifest identity/schema/backend")
    timeout = manifest["timeout_seconds_per_command"]
    if isinstance(timeout, bool) or not isinstance(timeout, int) or not 1 <= timeout <= (1500 if args.name == "maintenance" else 900):
        raise ValueError("invalid timeout")
    commands = [(command(spec), spec.get("expected_tests", []), spec["runner"]) for spec in manifest["commands"]]
    if args.name == "shard" and (not commands or commands[0][2] != "cargo-build-node"):
        raise ValueError("shard proof must build the node before tests")
    if args.name in ("simulation", "native-engine-contracts", "go-shard", "go-namespace", "go-cluster", "go-queue", "go-history", "go-nexus", "go-matching", "go-matching-userdata", "go-queuev2", "go-persistence", "go-runtime-stores", "go-history-routing", "go-outcomes", "go-visibility", "go-visibility-frozen", "go-fair", "go-execution", "go-historytasks", "go-executiontasks", "go-shard-compat", "owner-manager", "maintenance", "process-cut") and (not commands or commands[0][2] != "go-node-build"):
        raise ValueError("Go shard proof must build native Go node first")
    if not commands:
        raise ValueError("empty experiment")
    env = {k: os.environ[k] for k in ("PATH", "HOME", "USER", "TMPDIR", "RUSTUP_HOME", "CARGO_HOME") if k in os.environ}
    env.update({"CARGO_TERM_COLOR": "never", "XENON_PROBE_BACKEND": "memory"})
    if args.name in ("shard", "go-shard-compat"):
        env.update({"GOENV": "off", "GOWORK": "off", "GOFLAGS": "-mod=readonly", "GOTOOLCHAIN": "go1.27.1", "XENON_NODE_BINARY": str(ROOT / "target/debug/xenon-node")})
    if args.name in ("forwarding", "simulation"):
        env.update({"GOENV":"off", "GOWORK":"off", "GOFLAGS":"-mod=readonly", "GOTOOLCHAIN":"go1.27.1"})
    if args.name == "crash":
        env["XENON_PROOF_PROJECT"] = "xenon-crash-" + uuid.uuid4().hex[:12]
    if args.name in ("simulation", "native-engine-contracts", "go-shard", "go-namespace", "go-cluster", "go-queue", "go-history", "go-nexus", "go-matching", "go-matching-userdata", "go-queuev2", "go-persistence", "go-runtime-stores", "go-history-routing", "go-outcomes", "go-visibility", "go-visibility-frozen", "go-fair", "go-execution", "go-historytasks", "go-executiontasks", "go-shard-compat", "owner-manager", "maintenance", "process-cut"):
        target = ROOT / ".local/slatedb-native-target/debug"
        env.update({"GOENV":"off", "GOWORK":"off", "GOFLAGS":"-mod=readonly", "GOTOOLCHAIN":"go1.27.1", "CGO_ENABLED":"1", "CGO_LDFLAGS":"-L"+str(target), "LD_LIBRARY_PATH":str(target), "DYLD_LIBRARY_PATH":str(target), "SLATEDB_UNIFFI_RUNTIME_THREADS":"2", "XENON_NODE_BINARY":str(ROOT / ".local/bin/xenon-go-node")})
    if args.name == "go-shard-compat":
        env["XENON_COMPAT_PROJECT"] = "xenon-compat-" + uuid.uuid4().hex[:12]
        if [item[2] for item in commands] != ["go-node-build", "cargo-build-node", "go-shard-compat"]:
            raise ValueError("compatibility proof requires both builds then registered fixture")
    if args.name == "native-engine-contracts":
        env["XENON_NATIVE_PROJECT"] = "xenon-native-" + uuid.uuid4().hex[:12]
    if args.name == "process-cut":
        env["XENON_PROCESS_CUT_PROJECT"] = "xenon-process-cut-" + uuid.uuid4().hex[:12]
    if args.name == "maintenance":
        env["XENON_MAINTENANCE_PROJECT"] = "xenon-maintenance-" + uuid.uuid4().hex[:12]
    if args.name == "owner-manager":
        env["XENON_OWNER_MANAGER_PROJECT"] = "xenon-owner-manager-" + uuid.uuid4().hex[:12]
    if args.name in ("s3-meter", "cas-loss"):
        env.update({"GOENV":"off", "GOWORK":"off", "GOFLAGS":"-mod=readonly", "GOTOOLCHAIN":"go1.27.1", "XENON_S3_METER_PROJECT":"xenon-s3-meter-" + uuid.uuid4().hex[:12]})
    if args.name in ("directory", "registry-contracts"):
        env.update({"GOENV":"off", "GOWORK":"off", "GOFLAGS":"-mod=readonly", "GOTOOLCHAIN":"go1.27.1", "XENON_DIRECTORY_PROJECT":"xenon-directory-" + uuid.uuid4().hex[:12]})
    def git(*argv):
        return subprocess.check_output(["git", *argv], cwd=ROOT, env=env, text=True).strip()
    sha = git("rev-parse", "HEAD")
    dirty = git("status", "--porcelain=v1", "--untracked-files=all")
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + args.name + "-" + uuid.uuid4().hex[:8]
    evidence = ROOT / ".local" / "evidence" / run_id
    evidence.mkdir(parents=True)
    if args.name == "native-engine-contracts":
        env["XENON_NATIVE_EVIDENCE"] = str(evidence)
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
        for relative in [str(selected_manifest.relative_to(ROOT)), "scripts/prove.py", *manifest["inputs"]]:
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
        for tool in ("git", "rustc", "cargo", "python", *(["go", "protoc"] if args.name in ("shard", "go-shard-compat") else (["go"] if args.name in ("simulation", "native-engine-contracts", "go-shard", "go-namespace", "go-cluster", "go-queue", "go-history", "go-nexus", "go-matching", "go-matching-userdata", "go-queuev2", "go-persistence", "go-runtime-stores", "go-history-routing", "go-outcomes", "go-visibility", "go-visibility-frozen", "go-fair", "go-execution", "go-historytasks", "go-executiontasks", "go-shard-compat", "forwarding", "rpc-measurement", "external-recorder", "mixed-oracle", "recorder-adapter", "recorder-lifecycle", "visibility-movement", "nexus-http", "nexus-readiness", "visibility-fanout", "registry-contracts", "directory", "owner-manager", "maintenance", "s3-meter", "cas-loss", "process-cut") else []))):
            argv = [sys.executable, "--version"] if tool == "python" else [tool, "version" if tool == "go" else ("-vV" if tool == "rustc" else "--version")]
            code, output, expired = run_process(argv, 30, env, ROOT)
            if code or expired:
                raise ValueError(f"cannot record tool version: {tool}")
            report["tool_versions"][tool] = output.strip()
            required = manifest.get("required_tool_prefixes", {}).get(tool)
            if required and not version_matches(output, required):
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
            elif runner == "go-test-process-cut":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/processcut")
            elif runner == "s3-process-cut":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/node")
                if digest(ROOT / ".local/bin/xenon-go-node") != report["node_binary_sha256"]:
                    raise ValueError("node changed during process-cut proof")
            elif runner == "python-measurements":
                actual = re.findall(r"^(test_[a-zA-Z0-9_]+) \([^\n)]+\) \.\.\. ok$", output, re.MULTILINE)
                if sorted(actual) != sorted(expected) or not re.search(r"^Ran " + str(len(expected)) + r" tests? in [^\n]+\n\nOK$", output.strip(), re.MULTILINE):
                    raise ValueError("Python measurement assertions mismatch")
            elif runner == "go-test-meter-cli":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/cmd/xenon-s3-meter")
            elif runner in ("go-test-meter", "go-test-cas-loss", "s3-meter"):
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/proof/s3meter")
            elif runner == "go-test-registry":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/" + argv[-1].removeprefix("./"))
            elif runner == "go-test-engine":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/partitions/slatedb")
            elif runner == "s3-directory":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/registry/s3" if args.name == "registry-contracts" else "github.com/0x63616c/xenon/internal/directory")
            elif runner == "go-test-outcomes":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/ownership")
            elif runner == "go-test-trace-adapter":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/temporal/adapter")
            elif runner in ("go-test-observer", "go-test-observer-adapter"):
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/rpctrace" if runner == "go-test-observer" else "github.com/0x63616c/xenon/internal/temporal/adapter")
            elif runner == "go-test-fanout":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/temporal/adapter")
            elif runner == "go-test-mixed-oracle":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/cmd/xenon-omes-oracle")
            elif runner == "go-test-visibility-barrier":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/cmd/xenon-visibility-probe")
            elif runner == "go-test-sdk-readiness":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/cmd/xenon-sdk-probe")
            elif runner == "go-test-nexus-config":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/ministack")
            elif runner == "go-check-nexus-config":
                if output.strip() != "TEMPORAL_CONFIG_VALID":
                    raise ValueError("missing actual config validation result")
            elif runner == "go-test-recorder":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/proof/recorder")
            elif runner == "go-test-rpctrace":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/rpctrace")
            elif runner == "go-test-routing":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/routing")
            elif runner == "go-test-simulation":
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/simulation")
            elif runner in ("go-test-shard", "go-test-node", "go-test-visibility", "go-test-factory", "go-shard-compat"):
                verify_go_tests(output, expected, "github.com/0x63616c/xenon/internal/node" if runner == "go-test-node" else ("github.com/0x63616c/xenon/internal/visibility" if runner == "go-test-visibility" else "github.com/0x63616c/xenon/internal/temporal/adapter"))
                binary = ROOT / (".local/bin/xenon-go-node" if args.name in ("go-shard", "go-namespace", "go-cluster", "go-queue", "go-history", "go-nexus", "go-matching", "go-matching-userdata", "go-queuev2", "go-persistence", "go-runtime-stores", "go-history-routing", "go-outcomes", "go-visibility", "go-visibility-frozen", "go-fair", "go-execution", "go-historytasks", "go-executiontasks", "go-shard-compat", "owner-manager", "maintenance", "process-cut") else "target/debug/xenon-node")
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
        if args.name in ("simulation", "native-engine-contracts", "go-shard", "go-namespace", "go-cluster", "go-queue", "go-history", "go-nexus", "go-matching", "go-matching-userdata", "go-queuev2", "go-persistence", "go-runtime-stores", "go-history-routing", "go-outcomes", "go-visibility", "go-visibility-frozen", "go-fair", "go-execution", "go-historytasks", "go-executiontasks", "go-shard-compat", "owner-manager", "maintenance", "process-cut"):
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
                report.update(result="failed", proof_pass=False)
                report.setdefault("error", "compatibility Compose cleanup failed")
        if args.name in ("native-engine-contracts", "registry-contracts", "directory", "owner-manager", "maintenance", "s3-meter", "cas-loss", "process-cut"):
            env_key = {"native-engine-contracts":"XENON_NATIVE_PROJECT", "registry-contracts":"XENON_DIRECTORY_PROJECT", "directory":"XENON_DIRECTORY_PROJECT", "owner-manager":"XENON_OWNER_MANAGER_PROJECT", "maintenance":"XENON_MAINTENANCE_PROJECT", "s3-meter":"XENON_S3_METER_PROJECT", "cas-loss":"XENON_S3_METER_PROJECT", "process-cut":"XENON_PROCESS_CUT_PROJECT"}[args.name]
            compose_path = "test/scenarios/integration/native-engine/compose.yaml" if args.name == "native-engine-contracts" else "deploy/" + ("s3-meter" if args.name=="cas-loss" else "directory" if args.name=="registry-contracts" else args.name) + ".compose.yaml"
            try:
                code, output, expired = run_process(["docker", "compose", "--project-name", env[env_key], "-f", compose_path, "down", "--volumes"], 60, env, ROOT)
                report["cleanup"] = {"project": env[env_key], "exit_code": code, "timed_out": expired}
            except Exception as error:
                report["cleanup"] = {"project": env[env_key], "exit_code": -1, "timed_out": False, "error": str(error)}
            if report["cleanup"]["exit_code"] or report["cleanup"]["timed_out"]:
                report.update(result="failed", proof_pass=False)
                report.setdefault("error", "directory cleanup failed")
        if args.name == "crash":
            # Out-of-process cleanup also handles a controller killed before finally.
            project = env["XENON_PROOF_PROJECT"]
            try:
                report["cleanup"] = cleanup_crash(project, env, ROOT)
            except Exception as error:
                report["cleanup"] = {"project": project, "exit_code": -1, "timed_out": False, "error": str(error)}
            if report["cleanup"]["exit_code"] or report["cleanup"]["timed_out"]:
                report.update(result="failed", proof_pass=False)
                report.setdefault("error", "scoped Compose cleanup failed")
        (evidence / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"{report['result'].upper()}: {evidence / 'result.json'}", flush=True)
    return 0 if report["result"] in ("passed", "development-passed") else 1


if __name__ == "__main__":
    sys.exit(main())
