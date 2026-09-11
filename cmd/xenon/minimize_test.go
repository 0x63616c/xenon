package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/0x63616c/xenon/internal/app"
)

func TestMinimizeCLIActualCoupledFailure(t *testing.T) {
	original := filepath.Join(t.TempDir(), "original")
	code, _, diagnostics := runSimulationCLI(t, t.Context(), "test", "simulation", "--scenario", "../../test/scenarios/simulation/minimize-missing-commit.json", "--evidence", original, "--development")
	if code != 1 || diagnostics == "" {
		t.Fatal("expected controlled failure", code, diagnostics)
	}
	var out, stderr bytes.Buffer
	artifact := filepath.Join(original, "case-00000000000000000000", "scenario.json")
	failure := filepath.Join(filepath.Dir(artifact), "failure.json")
	before, err := os.ReadFile(artifact)
	if err != nil {
		t.Fatal(err)
	}
	code = execute(t.Context(), []string{"minimize", failure, "--development", "--max-attempts", "3"}, forbiddenInput{t}, &out, &stderr, func(context.Context, app.Config) error { t.Fatal("backend started"); return nil })
	var receipt struct {
		Result struct {
			StopReason       string `json:"stop_reason"`
			OriginalVerified bool   `json:"original_verified"`
			Complete         bool   `json:"complete"`
		} `json:"result"`
		BestArtifact string `json:"best_artifact"`
	}
	if err := json.Unmarshal(out.Bytes(), &receipt); err != nil {
		t.Fatalf("not one terminal JSON: %s %v", &out, err)
	}
	if code != 2 || receipt.Result.StopReason != "budget" || !receipt.Result.OriginalVerified || receipt.Result.Complete || receipt.BestArtifact == "" {
		t.Fatalf("bad budget receipt: %d %s %s", code, &out, &stderr)
	}
	defer os.RemoveAll(filepath.Dir(receipt.BestArtifact))
	after, err := os.ReadFile(artifact)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("minimize changed original", err)
	}
	code, _, _ = runSimulationCLI(t, t.Context(), "replay", "--artifact", receipt.BestArtifact, "--evidence", filepath.Join(t.TempDir(), "replay"), "--development")
	if code != 1 {
		t.Fatal("replay of retained failure must fail", code)
	}
}
