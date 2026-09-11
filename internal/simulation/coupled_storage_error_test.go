package simulation

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func TestCoupledStorageReadFailureRecovers(t *testing.T) {
	for _, actor := range []string{"coordinator-old", "writer-old"} {
		t.Run(actor, func(t *testing.T) {
			c, _ := loadCoupled(t)
			prefix := []CoupledInput{{Action: "poll", Actor: actor}, {Action: "read", Actor: actor, Effect: 1, Fault: "storage_read_error"}, {Action: "deliver", Actor: actor, Effect: 1}}
			for _, step := range c.Steps {
				if step.Actor == actor && step.Effect != 0 {
					step.Effect++
				}
				prefix = append(prefix, step)
			}
			c.Steps = prefix
			raw, _ := json.Marshal(c)
			generator, err := NewCoupledInterleavings(raw)
			if err != nil {
				t.Fatal(err)
			}
			orders := map[string]bool{}
			for seed := uint64(1); seed <= 10; seed++ {
				s, err := generator.Next(context.Background(), GenerateRequest{FaultSeed: seed})
				if err != nil {
					t.Fatal(err)
				}
				orders[hash(s.Faults)] = true
				expanded, err := decodeCoupled(s)
				if err != nil {
					t.Fatal(err)
				}
				first, err := RunCoupled(expanded, "")
				if err != nil {
					t.Fatal(err)
				}
				second, err := RunCoupled(expanded, "")
				if err != nil {
					t.Fatal(err)
				}
				a, _ := json.Marshal(first)
				b, _ := json.Marshal(second)
				if !bytes.Equal(a, b) {
					t.Fatal("read-error replay changed")
				}
				errors, delivered := 0, 0
				for _, entry := range first.Trace {
					if entry.Result == "storage_read_error" {
						errors++
						// The same independent observer must reject a backend that changes
						// authority while claiming the read failed.
						checker := newCoupledChecker(expanded, entry.Before)
						bad := entry
						bad.After = entry.After.Clone()
						bad.After.Version = "mutated"
						fp, ok := FingerprintOf(checker.observe(bad))
						if !ok || fp.Invariant != "registry_read" {
							t.Fatal("read-mutation control escaped")
						}
					}
					if entry.Input.Actor == actor && entry.Input.Action == "deliver" && entry.Input.Effect == 1 {
						delivered++
						if entry.Result != "storage_error" || len(entry.After.Body) != 0 || entry.After.Version != "" || len(entry.ClusterEffects)+len(entry.PartitionEffects) != 0 {
							t.Fatal("failed read disclosed authority or drove a decision")
						}
					}
				}
				if errors != 1 || delivered != 1 {
					t.Fatal("declared read fault did not execute exactly once")
				}
			}
			if len(orders) < 2 {
				t.Fatal("no varied read-error event order")
			}
			t.Logf("actor=%s schedules=10 replays=10 read_errors=10 error_deliveries=10 negative_controls=10 distinct_orders=%d", actor, len(orders))
		})
	}
}

func TestCoupledStorageErrorUnsupportedBeforeEffects(t *testing.T) {
	c, _ := loadCoupled(t)
	c.Steps[3].Fault = "storage_read_error"
	result, err := RunCoupled(c, "")
	if err == nil || len(result.Trace) != 0 {
		t.Fatal("storage read fault applied to a publication")
	}
}
