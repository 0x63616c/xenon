package simulation

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"runtime"
	"strings"
	"testing"
)

const coupledScenarioPath = "../../test/scenarios/simulation/coordinator-move.json"
const coupledTracePath = "../../test/scenarios/simulation/coordinator-move-trace.json"

type coupledArtifact struct {
	Version            int           `json:"version"`
	ModelBoundary      string        `json:"model_boundary"`
	ProductionRevision string        `json:"production_revision"`
	Toolchain          string        `json:"toolchain"`
	InputSHA256        string        `json:"input_sha256"`
	Result             CoupledResult `json:"result"`
}

func loadCoupled(t *testing.T) (CoupledScenario, []byte) {
	t.Helper()
	raw, err := os.ReadFile(coupledScenarioPath)
	if err != nil {
		t.Fatal(err)
	}
	var scenario CoupledScenario
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&scenario); err != nil {
		t.Fatal(err)
	}
	return scenario, raw
}
func TestCoupledCoordinatorMoveExactReplay(t *testing.T) {
	scenario, raw := loadCoupled(t)
	if runtime.Version() != scenario.Toolchain {
		t.Fatalf("toolchain mismatch: got %s want %s", runtime.Version(), scenario.Toolchain)
	}
	result, err := RunCoupled(scenario, "")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	artifact := coupledArtifact{Version: 1, ModelBoundary: "Production cluster.Step and partitions.Step; registry CAS and native epochs modeled. No real SlateDB, network, Temporal or durability qualification.", ProductionRevision: scenario.ProductionRevision, Toolchain: scenario.Toolchain, InputSHA256: hex.EncodeToString(sum[:]), Result: result}
	encoded, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	// Explicit fixture update only. Normal CI/replay never rewrites evidence.
	if os.Getenv("XENON_UPDATE_COUPLED_TRACE") == "1" {
		if err := os.WriteFile(coupledTracePath, encoded, 0644); err != nil {
			t.Fatal(err)
		}
	}
	golden, err := os.ReadFile(coupledTracePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, golden) {
		t.Fatal("exact expanded-input/effect/observation replay changed; inspect before updating fixture")
	}
	again, err := RunCoupled(scenario, "")
	if err != nil {
		t.Fatal(err)
	}
	replay, _ := json.Marshal(again)
	first, _ := json.Marshal(result)
	if !bytes.Equal(replay, first) {
		t.Fatal("same expanded schedule produced another trace")
	}
}
func TestCoupledNegativeControls(t *testing.T) {
	scenario, _ := loadCoupled(t)
	for _, negative := range []string{"stale_plan", "old_ready", "post_fence_commit"} {
		t.Run(negative, func(t *testing.T) {
			result, err := RunCoupled(scenario, negative)
			if err == nil || !strings.Contains(err.Error(), negative+":") {
				t.Fatalf("same checker missed %s: %v", negative, err)
			}
			if len(result.Trace) == 0 || result.Trace[len(result.Trace)-1].Input.Fault != negative {
				t.Fatal("did not fail at first injected violation")
			}
		})
	}
}
func TestCoupledReplayRejectsChangedEffectPrecondition(t *testing.T) {
	scenario, _ := loadCoupled(t)
	scenario.Steps[1].Effect = 99
	if _, err := RunCoupled(scenario, ""); err == nil || !strings.Contains(err.Error(), "pending effect is absent") {
		t.Fatal("changed precondition silently adapted", err)
	}
}

func TestCoupledSettleRejectsPreRecoveryCommitOnly(t *testing.T) {
	scenario, _ := loadCoupled(t)
	// The original current writer commits before the delayed old open fences it.
	// Remove the post-recovery commit: old success must not satisfy healthy settle.
	earlier := CoupledInput{Action: "commit", Actor: "writer-current", Effect: 3, At: 0}
	steps := append([]CoupledInput{}, scenario.Steps[:51]...)
	steps = append(steps, earlier)
	steps = append(steps, scenario.Steps[51:len(scenario.Steps)-1]...)
	scenario.Steps = steps
	if _, err := RunCoupled(scenario, ""); err == nil || !strings.Contains(err.Error(), "required final live owner") {
		t.Fatalf("pre-fault success satisfied recovered progress: %v", err)
	}
}
