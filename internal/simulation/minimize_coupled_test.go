package simulation

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const minimizeCoupledFixture = "../../test/scenarios/simulation/minimize-missing-commit.json"

func TestMinimizeCoupledArtifactAndDependencyRepair(t *testing.T) {
	raw, err := os.ReadFile(minimizeCoupledFixture)
	if err != nil {
		t.Fatal(err)
	}
	gen, err := NewCoupledCorpus(raw)
	if err != nil {
		t.Fatal(err)
	}
	runner := testRunner(t, &CoupledDriver{})
	_, err = Search(t.Context(), searchConfig(), gen, runner)
	want := FailureFingerprint{"progress", "missing_live_commit"}
	if f, ok := FingerprintOf(err); !ok || f != want {
		t.Fatalf("fixture must fail intended invariant: %v", err)
	}
	before, err := os.ReadFile(scenarioFile(runner))
	if err != nil {
		t.Fatal(err)
	}
	cfg := reduceConfig(t)
	cfg.MaxDuration = 2 * time.Minute
	cfg.MaxProposals = 2048
	cfg.MaxAttempts = 256
	cfg.AttemptBudget = 3 * time.Second
	out, err := MinimizeArtifact(t.Context(), scenarioFile(runner), cfg, testRunner(t, &CoupledDriver{}))
	if err != nil {
		t.Fatalf("reduction %+v %v", out, err)
	}
	space := CoupledReductionSpace{Limits: searchConfig().Limits}
	originalSize, _ := space.Measure(out.Original)
	if !out.Complete || !out.OriginalVerified || originalSize.Actions-out.BestSize.Actions < 20 || originalSize.Faults != 3 {
		t.Fatalf("insufficient verified reduction %v -> %v (%s)", originalSize, out.BestSize, out.StopReason)
	}
	if err = space.Validate(out.Best); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(scenarioFile(runner))
	if string(before) != string(after) {
		t.Fatal("original overwritten")
	}
	best := filepath.Join(cfg.Directory, "best-scenario.json")
	for i := 0; i < 3; i++ {
		_, err = Replay(t.Context(), best, testRunner(t, &CoupledDriver{}))
		if f, ok := FingerprintOf(err); !ok || f != want {
			t.Fatalf("best replay lost identity %v", err)
		}
	}
	// Removing the first effect creator also removes its transitive consumers.
	s, _ := ExpandCoupled(raw)
	candidate, err := space.Candidate(t.Context(), s, 0)
	if err != nil {
		t.Fatal(err)
	}
	var c CoupledScenario
	c, err = decodeCoupled(candidate)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range c.Steps {
		if step.Actor == "writer-old" && step.Effect == 1 {
			t.Fatal("dangling removed effect")
		}
	}
	// Modified expanded bytes fail the exact same checksum boundary as Replay.
	var record artifact
	if err = json.Unmarshal(before, &record); err != nil {
		t.Fatal(err)
	}
	record.Scenario.Faults = append(record.Scenario.Faults, ' ')
	bad, _ := json.Marshal(record)
	tampered := filepath.Join(t.TempDir(), "scenario.json")
	if err = os.WriteFile(tampered, bad, 0600); err != nil {
		t.Fatal(err)
	}
	cfg.Directory = filepath.Join(t.TempDir(), "must-not-exist")
	if _, err = MinimizeArtifact(t.Context(), tampered, cfg, runner); err == nil {
		t.Fatal("tampered input accepted")
	}
	if _, err = os.Stat(cfg.Directory); !os.IsNotExist(err) {
		t.Fatal("tampered input created evidence")
	}
}

func TestMinimizeCoupledRejectsOversizedEnvelope(t *testing.T) {
	raw, err := os.ReadFile(minimizeCoupledFixture)
	if err != nil {
		t.Fatal(err)
	}
	s, err := ExpandCoupled(raw)
	if err != nil {
		t.Fatal(err)
	}
	limits := searchConfig().Limits
	limits.MaxPayloadBytes = 1
	if err = (CoupledReductionSpace{Limits: limits}).Validate(s); err == nil {
		t.Fatal("oversized envelope accepted")
	}
}
