package simulation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var reduceFingerprint = FailureFingerprint{"acknowledged_write", "missing_after_recovery"}

type byteSpace struct{}

func (byteSpace) Validate(s Scenario) error {
	if len(s.Workload) == 0 {
		return errors.New("empty")
	}
	return nil
}
func (byteSpace) Measure(s Scenario) (ReductionSize, error) {
	return ReductionSize{uint64(len(s.Workload)), uint64(len(s.Faults)), uint64(len(s.Workload) + len(s.Faults))}, nil
}
func (byteSpace) Candidate(ctx context.Context, s Scenario, index uint64) (Scenario, error) {
	if index == 0 && len(s.Workload) > 1 {
		s.Workload = s.Workload[:len(s.Workload)-1]
		return s, nil
	}
	if index == 1 && len(s.Faults) > 0 {
		s.Faults = s.Faults[:len(s.Faults)-1]
		return s, nil
	}
	return Scenario{}, io.EOF
}
func reduceConfig(t *testing.T) MinimizeConfig {
	return MinimizeConfig{Clock: WallClock{}, MaxDuration: time.Minute, AttemptBudget: time.Second, MaxAttempts: 200, MaxProposals: 100, Simulation: true, Directory: filepath.Join(t.TempDir(), "minimize")}
}
func reduceScenario() Scenario {
	return Scenario{Version: 1, Kind: "controlled-reduction", Workload: []byte("abcdefghijklmnopqrstu"), Faults: []byte("abc")}
}
func TestMinimizeRunnerPreservesSameFailureAndReplay(t *testing.T) {
	calls := 0
	driver := testDriver{run: func(_ context.Context, s Scenario, emit func(json.RawMessage) error) error {
		calls++
		raw, _ := json.Marshal(string(s.Workload))
		if err := emit(raw); err != nil {
			return err
		}
		fingerprint := reduceFingerprint
		// Removing the last action produces an unrelated failure and must be rejected.
		if len(s.Workload) == 1 {
			fingerprint.Mechanism = "different_bug"
		}
		e, _ := NewInvariantFailure(fingerprint, errors.New("controlled negative fixture"))
		return e
	}}
	runner := testRunner(t, driver)
	if err := os.Mkdir(runner.Directory, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := searchConfig()
	cfg.CleanupBudget = time.Millisecond * 20
	predicate := &RunnerPredicate{Runner: *runner, Config: cfg}
	original := reduceScenario()
	result, err := Minimize(t.Context(), reduceConfig(t), original, reduceFingerprint, byteSpace{}, predicate)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Complete || !result.OriginalVerified || len(result.Best.Workload) != 2 || len(result.Best.Faults) != 0 || len(original.Workload) != 21 {
		t.Fatalf("bad reduction: %+v", result)
	}
	if calls != len(result.Attempts) {
		t.Fatal("attempt accounting mismatch")
	}
	last := result.Attempts[len(result.Attempts)-1]
	if last.Outcome.Fingerprint == reduceFingerprint {
		t.Fatal("expected final rejected unrelated failure")
	}
	// Replay the saved expanded original through the existing Replay API.
	replay := testRunner(t, driver)
	_, err = Replay(t.Context(), filepath.Join(result.Attempts[0].Outcome.EvidencePath, "scenario.json"), replay)
	if f, ok := FingerprintOf(err); !ok || f != reduceFingerprint {
		t.Fatalf("replay: %v", err)
	}
}

type predicateFunc func(context.Context, Scenario, time.Duration) (PredicateResult, error)

func (f predicateFunc) Evaluate(c context.Context, s Scenario, d time.Duration) (PredicateResult, error) {
	return f(c, s, d)
}
func TestMinimizeRejectsIntermittentAndStopsPending(t *testing.T) {
	for _, mode := range []string{"trace", "fingerprint", "pending", "cleanup", "budget"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			cfg := reduceConfig(t)
			if mode == "budget" {
				cfg.MaxAttempts = 4
			}
			result, err := Minimize(t.Context(), cfg, reduceScenario(), reduceFingerprint, byteSpace{}, predicateFunc(func(context.Context, Scenario, time.Duration) (PredicateResult, error) {
				calls++
				r := PredicateResult{Fingerprint: reduceFingerprint, TraceSHA256: "stable", CleanupVerified: true}
				if calls > 3 {
					switch mode {
					case "trace":
						r.TraceSHA256 = string(rune(calls))
					case "fingerprint":
						r.Fingerprint.Mechanism = "different"
					case "pending":
						r.Pending = true
					case "cleanup":
						r.CleanupVerified = false
					}
				}
				return r, nil
			}))
			if string(result.Best.Workload) != string(reduceScenario().Workload) || len(result.Best.Faults) != 3 {
				t.Fatal("unverified candidate accepted")
			}
			if mode == "pending" || mode == "cleanup" || mode == "budget" {
				if err == nil || calls != 4 {
					t.Fatalf("did not stop: %v %d", err, calls)
				}
			}
		})
	}
}

func TestMinimizeInjectedDeadlineAndCancellationPreserveOriginal(t *testing.T) {
	for _, cancelRun := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelRun), func(t *testing.T) {
			clock := &manualClock{}
			cfg := reduceConfig(t)
			cfg.Clock = clock
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered := make(chan struct{})
			release := make(chan struct{})
			returned := make(chan struct{})
			type result struct {
				out MinimizeResult
				err error
			}
			finished := make(chan result, 1)
			go func() {
				out, err := Minimize(ctx, cfg, reduceScenario(), reduceFingerprint, byteSpace{}, predicateFunc(func(context.Context, Scenario, time.Duration) (PredicateResult, error) {
					close(entered)
					<-release
					close(returned)
					return PredicateResult{CleanupVerified: true, Fingerprint: reduceFingerprint, TraceSHA256: "stable"}, nil
				}))
				finished <- result{out, err}
			}()
			<-entered
			if cancelRun {
				cancel()
			} else {
				clock.advance(cfg.MaxDuration)
			}
			r := <-finished
			close(release)
			<-returned
			if !errors.Is(r.err, ErrPending) || r.out.StopReason != "pending" || r.out.Complete || len(r.out.Attempts) != 1 || !r.out.Attempts[0].Outcome.Pending {
				t.Fatalf("bad interrupted receipt: %+v %v", r.out, r.err)
			}
			if string(r.out.Best.Workload) != string(reduceScenario().Workload) {
				t.Fatal("original lost")
			}
			if _, err := os.Stat(filepath.Join(cfg.Directory, "result.json")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMinimizeRunnerPredicateProductionCoupledFailure(t *testing.T) {
	raw, err := os.ReadFile(coupledScenarioPath)
	if err != nil {
		t.Fatal(err)
	}
	scenario, err := ExpandCoupled(raw)
	if err != nil {
		t.Fatal(err)
	}
	var steps []CoupledInput
	if err = json.Unmarshal(scenario.Faults, &steps); err != nil {
		t.Fatal(err)
	}
	for i := len(steps) - 1; i >= 0; i-- {
		if steps[i].Action == "commit" {
			steps = append(steps[:i], steps[i+1:]...)
			break
		}
	}
	scenario.Faults, _ = json.Marshal(steps)
	runner := testRunner(t, &CoupledDriver{})
	if err = os.Mkdir(runner.Directory, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := searchConfig()
	cfg.CleanupBudget = 20 * time.Millisecond
	p := &RunnerPredicate{Runner: *runner, Config: cfg}
	var trace string
	for i := 0; i < 3; i++ {
		r, e := p.Evaluate(t.Context(), scenario, time.Second)
		if e != nil || !r.CleanupVerified || r.Pending || r.Fingerprint != (FailureFingerprint{"progress", "missing_live_commit"}) {
			t.Fatalf("production steps failure: %+v %v", r, e)
		}
		if i > 0 && trace != r.TraceSHA256 {
			t.Fatal("production trace changed")
		}
		trace = r.TraceSHA256
	}
}
