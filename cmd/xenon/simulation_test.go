package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/0x63616c/xenon/internal/app"
	"github.com/0x63616c/xenon/internal/simulation"
)

const coupledInput = "../../test/scenarios/simulation/coordinator-move.json"

func runSimulationCLI(t *testing.T, ctx context.Context, args ...string) (int, simulationOutput, string) {
	t.Helper()
	var out, diagnostics bytes.Buffer
	code := execute(ctx, args, forbiddenInput{t}, &out, &diagnostics, func(context.Context, app.Config) error {
		t.Fatal("simulation command started server backend")
		return nil
	})
	var output simulationOutput
	if out.Len() != 0 {
		if err := json.Unmarshal(out.Bytes(), &output); err != nil {
			t.Fatalf("not one clean JSON result: %q (%v)", &out, err)
		}
	}
	return code, output, diagnostics.String()
}
func TestSimulationCLIAndExactArtifactReplay(t *testing.T) {
	evidence := filepath.Join(t.TempDir(), "test")
	code, out, diagnostics := runSimulationCLI(t, context.Background(), "test", "simulation", "--scenario", coupledInput, "--evidence", evidence, "--development")
	if code != 0 || diagnostics != "" || out.Result.Completed != 1 || out.Qualification != "development" || out.Result.EvidencePath != evidence {
		t.Fatal(code, out, diagnostics)
	}
	artifact := filepath.Join(evidence, "case-00000000000000000000", "scenario.json")
	replay := filepath.Join(t.TempDir(), "replay")
	code, out, diagnostics = runSimulationCLI(t, context.Background(), "replay", "--artifact", artifact, "--evidence", replay, "--development")
	if code != 0 || diagnostics != "" || out.Result.Completed != 1 || out.Mode != "exact-component-artifact-replay" {
		t.Fatal(code, out, diagnostics)
	}
	first, err := os.ReadFile(filepath.Join(evidence, "case-00000000000000000000", "trace.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(filepath.Join(replay, "case-00000000000000000000", "trace.jsonl"))
	if err != nil || !bytes.Equal(first, second) {
		t.Fatal("replay trace changed", err)
	}
}
func TestSearchFiniteCorpusFailureAndPrefixBound(t *testing.T) {
	raw, err := os.ReadFile(coupledInput)
	if err != nil {
		t.Fatal(err)
	}
	var bad simulation.CoupledScenario
	if err = json.Unmarshal(raw, &bad); err != nil {
		t.Fatal(err)
	}
	bad.Steps[1].Effect = 999
	raw, _ = json.Marshal(bad)
	broken := filepath.Join(t.TempDir(), "broken.json")
	if err = os.WriteFile(broken, raw, 0600); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []string{"1", "2"} {
		evidence := filepath.Join(t.TempDir(), "search")
		code, out, diagnostics := runSimulationCLI(t, context.Background(), "search", "--scenario", coupledInput, "--scenario", broken, "--max-cases", limit, "--evidence", evidence, "--development")
		if out.Result.Completed != 1 || out.Mode != "finite-coupled-corpus" {
			t.Fatal(out)
		}
		if limit == "1" && (code != 0 || diagnostics != "") {
			t.Fatal(code, diagnostics)
		}
		if limit == "2" && (code != 1 || diagnostics == "" || out.Result.StopReason != "first_failure") {
			t.Fatal(code, out, diagnostics)
		}
	}
}
func TestSimulationCanceledAndBudgetAreNotPasses(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, item := range []struct {
		ctx    context.Context
		extra  []string
		code   int
		reason string
	}{
		{canceled, nil, 130, "canceled"},
		{context.Background(), []string{"--duration", "1ns"}, 2, "budget"},
	} {
		args := []string{"search", "--scenario", coupledInput, "--evidence", filepath.Join(t.TempDir(), "run"), "--development"}
		args = append(args, item.extra...)
		code, out, diagnostics := runSimulationCLI(t, item.ctx, args...)
		if code != item.code || out.Result.StopReason != item.reason || out.Result.Completed != 0 || diagnostics == "" {
			t.Fatal(code, out, diagnostics)
		}
	}
}
func TestSimulationHelpDoesNotReadInputsOrStartBackend(t *testing.T) {
	for _, args := range [][]string{{"test", "--help"}, {"test", "simulation", "--help"}, {"search", "--help"}, {"replay", "--help"}, {"minimize", "--help"}, {"generate", "workflow", "--help"}} {
		var out, diagnostics bytes.Buffer
		code := execute(context.Background(), args, forbiddenInput{t}, &out, &diagnostics, func(context.Context, app.Config) error { t.Fatal("backend started"); return nil })
		if code != 0 || diagnostics.Len() != 0 || !strings.Contains(out.String(), "Usage:") {
			t.Fatal(code, &out, &diagnostics)
		}
	}
}
func TestSimulationRequiresProvenanceAndExplicitInputs(t *testing.T) {
	for _, args := range [][]string{{"search"}, {"replay"}, {"test", "simulation", "--scenario", coupledInput, "--evidence", filepath.Join(t.TempDir(), "run")}} {
		code, out, diagnostics := runSimulationCLI(t, context.Background(), args...)
		if code != 1 || out.Schema != 0 || diagnostics == "" {
			t.Fatal(code, out, diagnostics)
		}
	}
}
