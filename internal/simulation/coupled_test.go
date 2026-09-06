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

func TestCoupledCheckerRejectsUnauthorizedReservation(t *testing.T) {
	scenario, _ := loadCoupled(t)
	result, err := RunCoupled(scenario, "")
	if err != nil {
		t.Fatal(err)
	}
	original := result.Trace[3] // the old writer's first reservation publication
	if err := newCoupledChecker(scenario, original.Before).observe(original); err != nil {
		t.Fatal("valid reservation rejected", err)
	}
	for _, name := range []string{"coordinator actor", "different owner actor", "wrong partition actor", "generation jump", "missing reservation", "reused reservation", "assignment rewrite", "global revision rewrite"} {
		t.Run(name, func(t *testing.T) {
			entry := original
			if name == "reused reservation" {
				entry = result.Trace[69]
			}
			checker := newCoupledChecker(scenario, entry.Before)
			after, err := checkerControl(entry.After)
			if err != nil {
				t.Fatal(err)
			}
			id := scenario.RequiredPartition
			part := after.Partitions[id]
			switch name {
			case "coordinator actor":
				entry.Input.Actor = "coordinator-new"
			case "different owner actor":
				entry.Input.Actor = "writer-target"
			case "wrong partition actor":
				actor := checker.actors[entry.Input.Actor]
				actor.Partition = scenario.Initial.Layout.Partitions[1].ID
				checker.actors[entry.Input.Actor] = actor
			case "generation jump":
				part.Generation++
			case "missing reservation":
				part.Reservation = ""
			case "reused reservation":
				before, err := checkerControl(entry.Before)
				if err != nil {
					t.Fatal(err)
				}
				part.Reservation = before.Partitions[id].Reservation
			case "assignment rewrite":
				part.AssignmentRevision++
			case "global revision rewrite":
				after.AssignmentRevision++
			}
			after.Partitions[id] = part
			// Mutate the observation independently of production mutation helpers.
			var envelope map[string]json.RawMessage
			if err = json.Unmarshal(entry.After.Body, &envelope); err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(after)
			if err != nil {
				t.Fatal(err)
			}
			envelope["body"], err = json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			entry.After.Body, err = json.Marshal(envelope)
			if err != nil {
				t.Fatal(err)
			}
			if err := checker.observe(entry); err == nil || !strings.Contains(err.Error(), "reservation_authority:") {
				t.Fatalf("accepted unauthorized reservation %s: %v", name, err)
			}
		})
	}
}
