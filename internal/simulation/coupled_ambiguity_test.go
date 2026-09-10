package simulation

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestCoupledLostPublicationResponse(t *testing.T) {
	for _, cut := range []struct {
		name  string
		index int
	}{{"renewal", 9}, {"assignment", 14}} {
		t.Run(cut.name, func(t *testing.T) {
			c, _ := loadCoupled(t)
			c.Steps[cut.index].Fault = "lost_publish_response"
			result, err := RunCoupled(c, "")
			if err != nil {
				t.Fatal(err)
			}
			observed := 0
			for _, entry := range result.Trace {
				if entry.Result == "unknown_publication" {
					observed++
					if len(entry.After.Body) != 0 || entry.After.Version != "" {
						t.Fatal("lost response disclosed receipt")
					}
				}
			}
			if observed != 1 {
				t.Fatalf("fault observed %d times", observed)
			}
			replay, err := RunCoupled(c, "")
			if err != nil {
				t.Fatal(err)
			}
			a, _ := json.Marshal(result)
			b, _ := json.Marshal(replay)
			if !bytes.Equal(a, b) {
				t.Fatal("lost-response replay changed")
			}
			for _, mutant := range []string{"stale_plan", "old_ready", "post_fence_commit"} {
				_, err := RunCoupled(c, mutant)
				fp, ok := FingerprintOf(err)
				if !ok || fp.Invariant != mutant {
					t.Fatalf("mutant %s: %v", mutant, err)
				}
			}
		})
	}
}

func TestCoupledRenewalRestartsTakeoverSuspicion(t *testing.T) {
	c, _ := loadCoupled(t)
	original := c.Steps
	// Contender observes before the old coordinator renews. Its timeout read
	// then sees real renewed authority and must restart suspicion, not publish.
	steps := append([]CoupledInput{}, original[:5]...)
	steps = append(steps, original[27:30]...)
	steps = append(steps, original[5:27]...)
	steps = append(steps, original[30:33]...)
	poll := original[30]
	poll.At = 40
	read := original[31]
	read.At = 40
	read.Effect = 3
	deliver := original[32]
	deliver.At = 40
	deliver.Effect = 3
	deliver.Transition = "trn_0000000000000000000499"
	steps = append(steps, poll, read, deliver)
	for _, step := range original[33:] {
		if step.Actor == "coordinator-new" {
			step.At += 20
			if step.Effect >= 3 {
				step.Effect++
			}
		}
		steps = append(steps, step)
	}
	c.Steps = steps
	result, err := RunCoupled(c, "")
	if err != nil {
		t.Fatal(err)
	}
	reset, takeovers := 0, 0
	for _, entry := range result.Trace {
		if entry.Input.Actor != "coordinator-new" {
			continue
		}
		if entry.Input.Action == "deliver" && entry.Input.Effect == 2 {
			if len(entry.ClusterEffects) != 0 {
				t.Fatal("changed renewal failed to restart suspicion")
			}
			reset++
		}
		if entry.Input.Action == "publish" && entry.Input.At == 40 && entry.Accepted {
			takeovers++
		}
	}
	replay, err := RunCoupled(c, "")
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(result)
	b, _ := json.Marshal(replay)
	if !bytes.Equal(a, b) {
		t.Fatal("renewal/takeover replay changed")
	}
	if reset != 1 || takeovers != 1 {
		t.Fatalf("reset=%d takeover=%d", reset, takeovers)
	}
}

func TestCoupledUnsupportedFaultRejectedBeforeExecution(t *testing.T) {
	for _, fault := range []string{"drop_arbitrary_message", "lost_publish_response"} {
		c, _ := loadCoupled(t)
		c.Steps[0].Fault = fault
		result, err := RunCoupled(c, "")
		if err == nil || len(result.Trace) != 0 {
			t.Fatalf("unsupported fault executed: %s %v", fault, err)
		}
	}
}

func TestCoupledLostReadResponseUnsupported(t *testing.T) {
	c, _ := loadCoupled(t)
	c.Steps[2].Fault = "lost_publish_response"
	result, err := RunCoupled(c, "")
	if err == nil || len(result.Trace) != 0 {
		t.Fatalf("read mislabeled publication fault: %v", err)
	}
}
