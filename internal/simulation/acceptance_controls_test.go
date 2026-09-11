package simulation

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// oracleObservation is deliberately only expected and observed data. It cannot
// call the persistence, routing, ownership, or workflow implementations whose
// results it checks.
type oracleObservation struct {
	expected, acknowledged map[string]string
	applied                map[string]int
	digests                map[string]string
	staleAcknowledged      bool
	atomic                 bool
	children, results      int
	durableResults         int
	deadlineReset          bool
	postFenceCommit        bool
	settledProgress        bool
}

func checkOracle(o oracleObservation) error {
	fail := func(invariant, mechanism string) error {
		err, _ := NewInvariantFailure(FailureFingerprint{invariant, mechanism}, errors.New("controlled oracle observation"))
		return err
	}
	for id, want := range o.expected {
		got, ok := o.acknowledged[id]
		if !ok {
			return fail("acknowledged_write", "missing_after_recovery")
		}
		if got != want {
			return fail("workflow_result", "corrupt_result")
		}
		if o.applied[id] != 1 {
			return fail("application", "duplicate")
		}
		if o.digests[id] != "sha256:"+id {
			return fail("operation_digest", "changed")
		}
	}
	if o.staleAcknowledged {
		return fail("authority", "stale_acknowledgment")
	}
	if !o.atomic {
		return fail("atomic_record", "partial_recovery")
	}
	if o.children != len(o.expected) {
		return fail("execution_graph", "omitted_child")
	}
	if o.results != len(o.expected) {
		return fail("workflow_result", "omitted_result")
	}
	if o.durableResults != len(o.expected) {
		return fail("durable_replay", "omitted_result")
	}
	if o.deadlineReset {
		return fail("retry_deadline", "reset")
	}
	if o.postFenceCommit {
		return fail("writer_fence", "post_fence_commit")
	}
	if !o.settledProgress {
		return fail("progress", "stalled")
	}
	return nil
}

func validOracleObservation() oracleObservation {
	return oracleObservation{
		expected:        map[string]string{"op_1": "ok"},
		acknowledged:    map[string]string{"op_1": "ok"},
		applied:         map[string]int{"op_1": 1},
		digests:         map[string]string{"op_1": "sha256:op_1"},
		atomic:          true,
		children:        1,
		results:         1,
		durableResults:  1,
		settledProgress: true,
	}
}

func TestSimulationNamedOracleMutants(t *testing.T) {
	if err := checkOracle(validOracleObservation()); err != nil {
		t.Fatal(err)
	}
	tests := map[string]struct {
		want   FailureFingerprint
		mutate func(*oracleObservation)
	}{
		"lost acknowledged write": {FailureFingerprint{"acknowledged_write", "missing_after_recovery"}, func(o *oracleObservation) { delete(o.acknowledged, "op_1") }},
		"duplicate application":   {FailureFingerprint{"application", "duplicate"}, func(o *oracleObservation) { o.applied["op_1"] = 2 }},
		"wrong digest":            {FailureFingerprint{"operation_digest", "changed"}, func(o *oracleObservation) { o.digests["op_1"] = "sha256:other" }},
		"stale acknowledgment":    {FailureFingerprint{"authority", "stale_acknowledgment"}, func(o *oracleObservation) { o.staleAcknowledged = true }},
		"partial atomic recovery": {FailureFingerprint{"atomic_record", "partial_recovery"}, func(o *oracleObservation) { o.atomic = false }},
		"omitted child":           {FailureFingerprint{"execution_graph", "omitted_child"}, func(o *oracleObservation) { o.children = 0 }},
		"corrupt result":          {FailureFingerprint{"workflow_result", "corrupt_result"}, func(o *oracleObservation) { o.acknowledged["op_1"] = "bad" }},
		"omitted durable result":  {FailureFingerprint{"durable_replay", "omitted_result"}, func(o *oracleObservation) { o.durableResults = 0 }},
		"retry deadline reset":    {FailureFingerprint{"retry_deadline", "reset"}, func(o *oracleObservation) { o.deadlineReset = true }},
		"post-fence commit":       {FailureFingerprint{"writer_fence", "post_fence_commit"}, func(o *oracleObservation) { o.postFenceCommit = true }},
		"stalled progress":        {FailureFingerprint{"progress", "stalled"}, func(o *oracleObservation) { o.settledProgress = false }},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			o := validOracleObservation()
			tc.mutate(&o)
			got, ok := FingerprintOf(checkOracle(o))
			if !ok || got != tc.want {
				t.Fatalf("mutant failed wrong invariant: got=%+v valid=%t want=%+v", got, ok, tc.want)
			}
		})
	}
}

type resourceAccountingDriver struct {
	mu             sync.Mutex
	live, peak     int
	runs, cleanups int
	emitBytes      int
}

func (*resourceAccountingDriver) Validate(Scenario, WorkloadLimits) error { return nil }
func (d *resourceAccountingDriver) Run(_ context.Context, _ Scenario, emit func(json.RawMessage) error) error {
	d.mu.Lock()
	d.live++
	d.runs++
	if d.live > d.peak {
		d.peak = d.live
	}
	d.mu.Unlock()
	if d.emitBytes > 0 {
		return emit(json.RawMessage(`{"event":"` + strings.Repeat("x", d.emitBytes) + `"}`))
	}
	return emit(json.RawMessage(`{"event":"run"}`))
}
func (*resourceAccountingDriver) Settle(context.Context) error { return nil }
func (d *resourceAccountingDriver) Cleanup(context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.live != 1 {
		return errors.New("resource census mismatch")
	}
	d.live--
	d.cleanups++
	return nil
}

func TestSearchHundredCaseResourceBaselineAndEvidenceQuota(t *testing.T) {
	t.Run("100-case baseline", func(t *testing.T) {
		d := &resourceAccountingDriver{}
		cfg := searchConfig()
		cfg.MaxCases = 100
		r := testRunner(t, d)
		result, err := Search(context.Background(), cfg, &testGenerator{}, r)
		if err != nil || result.Completed != 100 || d.runs != 100 || d.cleanups != 100 || d.live != 0 || d.peak != 1 {
			t.Fatalf("unbounded or leaked resources: result=%+v err=%v driver=%+v", result, err, d)
		}
	})
	t.Run("trace quota", func(t *testing.T) {
		d := &resourceAccountingDriver{emitBytes: 128}
		cfg := searchConfig()
		cfg.MaxCases = 2
		cfg.MaxTraceBytes = 32
		r := testRunner(t, d)
		result, err := Search(context.Background(), cfg, &testGenerator{}, r)
		if err == nil || result.Completed != 0 || result.StopReason != "first_failure" || d.runs != 1 || d.cleanups != 1 || d.live != 0 {
			t.Fatalf("quota passed or leaked: result=%+v err=%v driver=%+v", result, err, d)
		}
		raw, readErr := os.ReadFile(filepath.Join(r.Directory, "case-00000000000000000000", "failure.json"))
		if readErr != nil || !strings.Contains(string(raw), "trace budget exceeded") {
			t.Fatalf("quota outcome not durable: %v %s", readErr, raw)
		}
	})
}

func TestSearchWorkloadAndAsyncFailureOrdering(t *testing.T) {
	for _, first := range []string{"workload", "async"} {
		t.Run(first, func(t *testing.T) {
			workloadFailure := errors.New("workload failed")
			asyncFailure := errors.New("node died")
			release := make(chan struct{})
			d := &reportingDriver{}
			d.run = func(ctx context.Context, _ Scenario, _ func(json.RawMessage) error) error {
				if first == "async" {
					d.report(asyncFailure)
					<-ctx.Done()
					return workloadFailure
				}
				close(release)
				return workloadFailure
			}
			d.cleanup = func(context.Context) error {
				if first == "workload" {
					<-release
					d.report(asyncFailure)
				}
				return nil
			}
			result, err := Search(context.Background(), searchConfig(), &testGenerator{}, testRunner(t, d))
			want := workloadFailure
			if first == "async" {
				want = asyncFailure
			}
			if result.Completed != 0 || !errors.Is(err, want) {
				t.Fatalf("earliest failure changed: result=%+v err=%v want=%v", result, err, want)
			}
		})
	}
}
