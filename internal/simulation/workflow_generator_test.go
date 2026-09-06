package simulation

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func workflowTestBundle(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	config, err := os.ReadFile("../../proof/workflow-generator/config.json")
	if err != nil {
		t.Fatal(err)
	}
	// Executable controls exercise the contract, not upstream-generation evidence.
	tools := map[string][]byte{
		"config": config, "worker": []byte("test-compatible-worker"),
		"generator": []byte("#!/bin/sh\nprintf '%s' \"$3\"\n"),
		"normalize": []byte("#!/bin/sh\ncat \"$1\" > \"$2\"\nprintf '{\"operations\":1,\"depth\":1}'\n"),
	}
	bundle := WorkflowGeneratorBundle{Version: 1, OmesCommit: omesGeneratorCommit, APICommit: "d96bd55e87799e9f6a33a1c40a56cfa932566bdf", GeneratorSourceSHA256: "c6333d94427a7bc2b55731cfa9b4f9ff9b7145017ef816a1ae5c4a9ba5c220ca", CargoLockSHA256: "3924e08587393d264993a5dade6e894b620ff44619f15a8d6d2f46d9ee61809a", NormalizerSourceSHA256: "529a56005dafde138773d420cec66a631bb4caadef50427ba575d388f87f7a32", InspectorSourceSHA256: hash(workflowInspectorSource), CompatibilityOverlaySHA256: "cdb70939ac3a6f0449534421dd13579984c69fcb1c5b737c72d744a63b47bc09", WorkerSDK: "v1.48.0", WorkerSHA256: hash(tools["worker"]), Tools: map[string]WorkflowTool{}}
	for name, raw := range tools {
		if err = os.WriteFile(filepath.Join(directory, name), raw, 0700); err != nil {
			t.Fatal(err)
		}
		bundle.Tools[name] = WorkflowTool{name, hash(raw)}
	}
	raw, _ := json.Marshal(bundle)
	if err = os.WriteFile(filepath.Join(directory, "generator.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	return directory
}
func workflowLimits() WorkloadLimits {
	return WorkloadLimits{2048, 64, 1 << 20, []string{WorkflowInputKind}}
}
func TestFreshWorkflowSeedsAndImmutableBundle(t *testing.T) {
	directory := workflowTestBundle(t)
	generator, err := NewWorkflowGenerator(directory)
	if err != nil {
		t.Fatal(err)
	}
	request := GenerateRequest{WorkloadSeed: 42, FaultSeed: 99, Limits: workflowLimits()}
	first, err := generator.Next(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	same, err := generator.Next(context.Background(), request)
	if err != nil || !bytes.Equal(first.Workload, same.Workload) {
		t.Fatal("same seed changed", err)
	}
	request.FaultSeed++
	otherFault, err := generator.Next(context.Background(), request)
	if err != nil || !bytes.Equal(first.Workload, otherFault.Workload) || bytes.Equal(first.Faults, otherFault.Faults) {
		t.Fatal("seed streams coupled", err)
	}
	request.WorkloadSeed++
	fresh, err := generator.Next(context.Background(), request)
	if err != nil || bytes.Equal(first.Workload, fresh.Workload) {
		t.Fatal("not fresh", err)
	}
	if err = os.WriteFile(filepath.Join(directory, "worker"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = generator.Next(context.Background(), request); err == nil {
		t.Fatal("changed worker accepted")
	}
}
func TestWorkflowGeneratorRejectsUnsupportedBeforeLaunch(t *testing.T) {
	directory := workflowTestBundle(t)
	generator, err := NewWorkflowGenerator(directory)
	if err != nil {
		t.Fatal(err)
	}
	request := GenerateRequest{Limits: workflowLimits()}
	request.Limits.Features = append(request.Limits.Features, "local-activities")
	if _, err = generator.Next(context.Background(), request); err == nil {
		t.Fatal("unsupported capabilities accepted")
	}
	request.Limits = workflowLimits()
	request.Limits.MaxDepth = 2
	if _, err = generator.Next(context.Background(), request); err == nil {
		t.Fatal("unsupported depth accepted")
	}
}
func TestActualPreparedWorkflowGenerator(t *testing.T) {
	directory := os.Getenv("XENON_WORKFLOW_GENERATOR_BUNDLE")
	if directory == "" {
		t.Skip("opt-in pinned local generator build; no workflow services")
	}
	generator, err := NewWorkflowGenerator(directory)
	if err != nil {
		t.Fatal(err)
	}
	var previous []byte
	for _, seed := range []uint64{1, 42, 2026090701} {
		request := GenerateRequest{WorkloadSeed: seed, FaultSeed: 99, Limits: workflowLimits()}
		first, err := generator.Next(context.Background(), request)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		repeated, err := generator.Next(context.Background(), request)
		if err != nil || !bytes.Equal(first.Workload, repeated.Workload) {
			t.Fatalf("non-deterministic seed %d: %v", seed, err)
		}
		if bytes.Equal(first.Workload, previous) {
			t.Fatal("distinct seed did not expand fresh input")
		}
		previous = first.Workload
		t.Logf("seed=%d workload_sha256=%s generator_identity=%s", seed, hash(first.Workload), generator.Info().SHA256)
	}
}

func TestGeneratedWorkflowUsesSharedEnvelopeAndReplayWithoutTools(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	directory := workflowTestBundle(t)
	generator, err := NewWorkflowGenerator(directory)
	if err != nil {
		t.Fatal(err)
	}
	evidence := filepath.Join(t.TempDir(), "generation")
	runner := &Runner{Driver: &WorkflowPreparationDriver{}, Clock: &manualClock{}, Directory: evidence, Provenance: Provenance{Source: "test-source", Versions: map[string]string{"toolchain": "test", "native": "not-executed", "images": "none"}}}
	config := SearchConfig{MaxCases: 2, MaxDuration: time.Second, MaxInFlight: 1, SettleBudget: time.Second, CleanupBudget: time.Second, MaxTraceBytes: 1 << 20, Limits: workflowLimits(), WorkloadSeed: 42, FaultSeed: 99}
	result, err := Search(ctx, config, generator, runner)
	if err != nil || result.Completed != 2 {
		t.Fatal(result, err)
	}
	if err = os.Rename(directory, directory+"-unavailable"); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(directory+"-unavailable", directory)
	replay := filepath.Join(t.TempDir(), "replay")
	runner.Directory = replay
	runner.Driver = &ArtifactDriver{}
	casePath := "case-00000000000000000000"
	result, err = Replay(ctx, filepath.Join(evidence, casePath, "scenario.json"), runner)
	if err != nil || result.Completed != 1 {
		t.Fatal(result, err)
	}
	first, err := os.ReadFile(filepath.Join(evidence, casePath, "trace.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(filepath.Join(replay, casePath, "trace.jsonl"))
	if err != nil || !bytes.Equal(first, second) {
		t.Fatal("replay changed or invoked absent generator", err)
	}
}
