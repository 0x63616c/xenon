package simulation

import (
	"os"
	"path/filepath"
	"testing"
)

type forbiddenReplayDriver struct {
	Driver
	t *testing.T
}

func (d forbiddenReplayDriver) Validate(Scenario, WorkloadLimits) error {
	d.t.Fatal("driver reached before artifact rejection")
	return nil
}

func TestReplayRejectsEnvelopeAndProvenanceChangesBeforeEffects(t *testing.T) {
	raw, err := os.ReadFile(minimizeCoupledFixture)
	if err != nil {
		t.Fatal(err)
	}
	gen, err := NewCoupledCorpus(raw)
	if err != nil {
		t.Fatal(err)
	}
	original := testRunner(t, &CoupledDriver{})
	if _, err := Search(t.Context(), searchConfig(), gen, original); err == nil {
		t.Fatal("expected controlled failure")
	}
	originalBytes, err := os.ReadFile(scenarioFile(original))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*artifact)
		reseal bool
	}{
		{"config", func(a *artifact) { a.Config.WorkloadSeed++ }, false},
		{"schema", func(a *artifact) { a.Version++ }, false},
		{"scenario-hash", func(a *artifact) { a.SHA256 = hash([]byte("changed")) }, false},
		{"event-removal", func(a *artifact) { a.Scenario.Faults = []byte(`[]`) }, false},
		{"generator-hash", func(a *artifact) { a.Generator.SHA256 = hash([]byte("changed")) }, false},
		{"source", func(a *artifact) { a.Provenance.Source = "different-source" }, false},
		{"tool", func(a *artifact) { a.Provenance.Versions["toolchain"] = "different-tool" }, false},
		{"valid-envelope-wrong-source", func(a *artifact) { a.Provenance.Source = "different-source" }, true},
		{"valid-envelope-wrong-tool", func(a *artifact) { a.Provenance.Versions["toolchain"] = "different-tool" }, true},
		{"valid-envelope-extra-tool", func(a *artifact) { a.Provenance.Versions["extra"] = "unexpected" }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := loadArtifact(t, scenarioFile(original))
			tc.mutate(&a)
			path := filepath.Join(t.TempDir(), "scenario.json")
			if tc.reseal {
				err = saveArtifact(path, a)
			} else {
				err = save(path, a)
			}
			if err != nil {
				t.Fatal(err)
			}
			runner := testRunner(t, forbiddenReplayDriver{t: t})
			if _, err := Replay(t.Context(), path, runner); err == nil {
				t.Fatal("replay accepted incompatible artifact")
			}
			if _, err := os.Stat(runner.Directory); !os.IsNotExist(err) {
				t.Fatal("replay created evidence before rejection", err)
			}
			cfg := reduceConfig(t)
			if _, err := MinimizeArtifact(t.Context(), path, cfg, runner); err == nil {
				t.Fatal("minimizer accepted incompatible artifact")
			}
			if _, err := os.Stat(cfg.Directory); !os.IsNotExist(err) {
				t.Fatal("minimizer created evidence before rejection", err)
			}
		})
	}
	after, err := os.ReadFile(scenarioFile(original))
	if err != nil || string(originalBytes) != string(after) {
		t.Fatal("original changed", err)
	}
}

func TestReplayLegacyAncestryCannotBecomeExactEvidence(t *testing.T) {
	original := testRunner(t, testDriver{})
	if _, err := Search(t.Context(), searchConfig(), &testGenerator{}, original); err != nil {
		t.Fatal(err)
	}
	legacy := loadArtifact(t, scenarioFile(original))
	legacy.Version = 1
	legacy.EnvelopeSHA256 = ""
	path := filepath.Join(t.TempDir(), "legacy.json")
	if err := save(path, legacy); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		allowed := testRunner(t, testDriver{})
		allowed.AllowLegacyArtifact = true
		if _, err := Replay(t.Context(), path, allowed); err != nil {
			t.Fatal(err)
		}
		path = scenarioFile(allowed)
		rejected := testRunner(t, forbiddenReplayDriver{t: t})
		if _, err := Replay(t.Context(), path, rejected); err == nil {
			t.Fatal("legacy replay laundered into exact evidence")
		}
		if _, err := os.Stat(rejected.Directory); !os.IsNotExist(err) {
			t.Fatal("rejection created evidence", err)
		}
		if _, err := MinimizeArtifact(t.Context(), path, reduceConfig(t), rejected); err == nil {
			t.Fatal("minimizer accepted legacy ancestry")
		}
	}
}
