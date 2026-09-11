package simulation

import (
	"os"
	"path/filepath"
	"testing"
)

const minimizeCoupledFixture = "../../test/scenarios/simulation/minimize-missing-commit.json"

func TestCoupledReductionDependencyRepair(t *testing.T) {
	raw, err := os.ReadFile(minimizeCoupledFixture)
	if err != nil {
		t.Fatal(err)
	}
	// Removing the first effect creator also removes its transitive consumers.
	s, err := ExpandCoupled(raw)
	if err != nil {
		t.Fatal(err)
	}
	space := CoupledReductionSpace{Limits: searchConfig().Limits}
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

// A saved receipt is only a target hypothesis: malformed or non-invariant
// failures must be refused before a reduction directory or replay is created.
func TestMinimizeArtifactRejectsInvalidFailureReceipt(t *testing.T) {
	raw, err := os.ReadFile(minimizeCoupledFixture)
	if err != nil {
		t.Fatal(err)
	}
	gen, err := NewCoupledCorpus(raw)
	if err != nil {
		t.Fatal(err)
	}
	runner := testRunner(t, &CoupledDriver{})
	if _, err = Search(t.Context(), searchConfig(), gen, runner); err == nil {
		t.Fatal("expected fixture failure")
	}
	original, err := os.ReadFile(scenarioFile(runner))
	if err != nil {
		t.Fatal(err)
	}
	for name, receipt := range map[string]string{
		"missing-fingerprint": `{"failure_phase":"run"}`,
		"cleanup-only":        `{"failure_phase":"cleanup","failure_fingerprint":{"invariant":"progress","mechanism":"missing_live_commit"}}`,
		"invalid-fingerprint": `{"failure_phase":"run","failure_fingerprint":{"invariant":"Progress!","mechanism":"missing_live_commit"}}`,
		"unknown-field":       `{"unrecognized":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "scenario.json")
			if err := os.WriteFile(path, original, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "failure.json"), []byte(receipt), 0600); err != nil {
				t.Fatal(err)
			}
			cfg := reduceConfig(t)
			if _, err := MinimizeArtifact(t.Context(), path, cfg, testRunner(t, &CoupledDriver{})); err == nil {
				t.Fatal("invalid receipt accepted")
			}
			if _, err := os.Stat(cfg.Directory); !os.IsNotExist(err) {
				t.Fatal("invalid receipt created reduction evidence", err)
			}
		})
	}
}
