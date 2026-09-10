package simulation

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type batchProbe struct {
	mu                    sync.Mutex
	active, peak, started int
	entered               chan struct{}
	release               chan struct{}
	fail                  bool
}
type batchRuntimeControl struct{ probe *batchProbe }

func (c *batchRuntimeControl) Check(context.Context, ResidentTopology) error { return nil }
func (c *batchRuntimeControl) Empty(context.Context, ResidentTopology) error { return nil }
func (c *batchRuntimeControl) Execute(ctx context.Context, _ ResidentTopology, _, _ string) error {
	p := c.probe
	p.mu.Lock()
	p.started++
	p.active++
	if p.active > p.peak {
		p.peak = p.active
	}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); p.active--; p.mu.Unlock() }()
	p.entered <- struct{}{}
	select {
	case <-p.release:
		if p.fail {
			return errors.New("controlled workflow failure")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (*batchRuntimeControl) Audit(context.Context, ResidentTopology, string) (json.RawMessage, error) {
	return json.RawMessage(`{"verified":"test-control"}`), nil
}
func (*batchRuntimeControl) Cleanup(context.Context, ResidentTopology, string) error { return nil }
func TestWorkflowBatchConcurrencyAndCleanup(t *testing.T) {
	for _, concurrency := range []int{1, 4} {
		t.Run(string(rune('0'+concurrency)), func(t *testing.T) {
			probe := &batchProbe{entered: make(chan struct{}, 12), release: make(chan struct{})}
			d := &BatchWorkflowDriver{Directory: filepath.Join(t.TempDir(), "members"), NewRuntime: func() (WorkflowRuntime, error) { return &batchRuntimeControl{probe}, nil }}
			r := testRunner(t, d)
			cfg := residentTestConfig()
			cfg.MaxDuration = 30 * time.Second
			cfg.MaxCases = 3
			gen := WorkflowBatchGenerator{Input: residentInputControl{}, Topology: residentTestTopology(), Count: 4, Concurrency: concurrency}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				result, e := Search(ctx, cfg, gen, r)
				if e == nil && result.Completed != 3 {
					e = errors.New("case not completed")
				}
				done <- e
			}()
			for i := 0; i < concurrency; i++ {
				select {
				case <-probe.entered:
				case err := <-done:
					t.Fatalf("batch stopped before barrier: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			probe.mu.Lock()
			active := probe.active
			probe.mu.Unlock()
			if active != concurrency {
				t.Fatalf("active %d", active)
			}
			close(probe.release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			probe.mu.Lock()
			defer probe.mu.Unlock()
			if probe.peak != concurrency || probe.started != 12 || probe.active != 0 {
				t.Fatalf("counts %d/%d/%d", probe.peak, probe.started, probe.active)
			}
		})
	}
}
func TestWorkflowBatchStopsQueuedAdmissionOnFailure(t *testing.T) {
	probe := &batchProbe{entered: make(chan struct{}, 4), release: make(chan struct{}), fail: true}
	close(probe.release)
	d := &BatchWorkflowDriver{Directory: filepath.Join(t.TempDir(), "members"), NewRuntime: func() (WorkflowRuntime, error) { return &batchRuntimeControl{probe}, nil }}
	r := testRunner(t, d)
	cfg := residentTestConfig()
	gen := WorkflowBatchGenerator{Input: residentInputControl{}, Topology: residentTestTopology(), Count: 4, Concurrency: 1}
	result, err := Search(context.Background(), cfg, gen, r)
	if err == nil || result.Completed != 0 || probe.started != 1 || probe.active != 0 {
		t.Fatalf("continued failedbatch: %+v %v starts%d", result, err, probe.started)
	}
}

func TestWorkflowBatchFailureReporterRejectsEarlierGenerations(t *testing.T) {
	d := &BatchWorkflowDriver{}
	for _, transition := range []string{"run-to-settle", "previous-case-to-next-run"} {
		t.Run(transition, func(t *testing.T) {
			d.SetFailureReporter(func(error) { t.Fatal("stale outer reporter") })
			old := d.failureReporter()
			calls := 0
			d.SetFailureReporter(func(error) { calls++ })
			d.mu.Lock()
			d.first = nil
			d.mu.Unlock()
			old(errors.New("late callback"))
			if d.first != nil || calls != 0 {
				t.Fatal("stale callback entered new phase")
			}
			current := errors.New("current failure")
			d.failureReporter()(current)
			if d.first != current || calls != 1 {
				t.Fatal("current failure not delivered")
			}
		})
	}
}

func TestWorkflowBatchNewPhaseDoesNotInheritPriorLatch(t *testing.T) {
	d := &BatchWorkflowDriver{}
	d.SetFailureReporter(func(error) {})
	d.failureReporter()(errors.New("late previous phase failure"))
	calls := 0
	d.SetFailureReporter(func(error) { calls++ })
	current := errors.New("settle failure")
	d.failureReporter()(current)
	if calls != 1 || d.first != current {
		t.Fatal("previous phase suppressed current failure")
	}
}
