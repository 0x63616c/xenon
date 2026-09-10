package simulation

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/cluster"
)

func TestCoupledSeededInterleavingsReplay(t *testing.T) {
	_, raw := loadCoupled(t)
	g, err := NewCoupledInterleavings(raw)
	if err != nil {
		t.Fatal(err)
	}
	// This external watchdog diagnoses a stuck harness; it is never a logical
	// timer and cannot turn an unfinished schedule into a pass.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	distinct := map[string]bool{}
	counts := map[string]int{}
	started := time.Now()
	for seed := uint64(1); seed <= 100; seed++ {
		for index := uint64(0); index < 10; index++ {
			request := GenerateRequest{Index: index, WorkloadSeed: 42, FaultSeed: seed}
			s, err := g.Next(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := g.Next(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(s.Faults, replay.Faults) {
				t.Fatal("expanded order differs")
			}
			distinct[hash(s.Faults)] = true
			request.WorkloadSeed++
			changed, err := g.Next(ctx, request)
			if err != nil || !bytes.Equal(s.Workload, changed.Workload) || !bytes.Equal(s.Faults, changed.Faults) {
				t.Fatal("unused workload RNG changed scenario", err)
			}
			c, err := decodeCoupled(s)
			if err != nil {
				t.Fatal(err)
			}
			first, err := runCoupled(ctx, c, "", nil)
			if err != nil {
				t.Fatalf("seed %d index %d: %v", seed, index, err)
			}
			second, err := runCoupled(ctx, c, "", nil)
			if err != nil {
				t.Fatal(err)
			}
			a, _ := json.Marshal(first)
			b, _ := json.Marshal(second)
			if !bytes.Equal(a, b) {
				t.Fatal("decision trace changed")
			}
			for _, entry := range first.Trace {
				if entry.Input.Fault == "stale_plan" && entry.Result == "conflict" {
					counts["stale_plan_rejected"]++
				}
				if entry.Input.Fault == "old_ready" && entry.Result == "conflict" {
					counts["stale_ready_rejected"]++
				}
				if entry.Input.Fault == "post_fence_commit" && entry.Result == "fenced" {
					counts["displaced_writer_commit_fenced"]++
				}
			}
		}
	}
	if len(distinct) < 2 {
		t.Fatal("no distinct expanded event order")
	}
	for _, name := range []string{"stale_plan_rejected", "stale_ready_rejected", "displaced_writer_commit_fenced"} {
		if counts[name] != 1000 {
			t.Fatalf("executed coverage %s=%d", name, counts[name])
		}
	}
	t.Logf("1000 schedules, 100 seeds, 2 executions each; unique expanded orders=%d; observed cuts=%v; elapsed=%s; average execution=%s; max logical events=84; wall watchdog=3m; remaining FAULT-01 cuts unqualified", len(distinct), counts, time.Since(started), time.Since(started)/2000)
}

func TestCoupledSeededNegativeControls(t *testing.T) {
	_, raw := loadCoupled(t)
	g, err := NewCoupledInterleavings(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, seed := range []uint64{1, 42, 2026090701} {
		s, err := g.Next(context.Background(), GenerateRequest{FaultSeed: seed})
		if err != nil {
			t.Fatal(err)
		}
		c, err := decodeCoupled(s)
		if err != nil {
			t.Fatal(err)
		}
		for _, mutant := range []string{"stale_plan", "old_ready", "post_fence_commit"} {
			_, err := RunCoupled(c, mutant)
			fingerprint, ok := FingerprintOf(err)
			if !ok || fingerprint.Invariant != mutant {
				t.Fatalf("mutant %s failed wrong invariant: %v", mutant, err)
			}
		}
	}
}

func TestCoupledVirtualDayTakeover(t *testing.T) {
	c, _ := loadCoupled(t)
	// The new coordinator first observes at zero. Its next observation after 24h
	// must take over using production suspicion logic; later relative ticks stay
	// unchanged. No sleep, native engine or transport participates in RunCoupled.
	for i := range c.Steps {
		s := &c.Steps[i]
		if s.Actor == "coordinator-new" && s.At >= 20 {
			s.At += cluster.Tick(24*time.Hour) - 20
		}
	}
	result, err := RunCoupled(c, "")
	if err != nil {
		t.Fatal(err)
	}
	observed := false
	for _, entry := range result.Trace {
		if entry.Input.Actor == "coordinator-new" && entry.Input.Action == "publish" && entry.Input.At == cluster.Tick(24*time.Hour) && entry.Accepted {
			observed = true
		}
	}
	if !observed {
		t.Fatal("24h takeover publication not observed")
	}
}

func TestCoupledInterleavingsCancellationAndInputIsolation(t *testing.T) {
	_, raw := loadCoupled(t)
	g, err := NewCoupledInterleavings(raw)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.Next(ctx, GenerateRequest{}); err != context.Canceled {
		t.Fatalf("cancellation: %v", err)
	}
	first, err := g.Next(context.Background(), GenerateRequest{})
	if err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), first.Faults...)
	first.Faults[0] = '!'
	next, err := g.Next(context.Background(), GenerateRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, next.Faults) {
		t.Fatal("caller mutated generator state")
	}
}
