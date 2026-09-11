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
	"time"
)

type uncertainResidentCleanup struct {
	residentControl
	mode string
}

func (r *uncertainResidentCleanup) Cleanup(context.Context, ResidentTopology, string) error {
	r.cleanups++
	if r.mode == "exception" {
		panic("cleanup census exception")
	}
	return errors.New("visibility unavailable; exact census unknown")
}

func TestResidentUncertainCensusRefusesReuse(t *testing.T) {
	for _, mode := range []string{"unavailable", "exception"} {
		t.Run(mode, func(t *testing.T) {
			runtime := &uncertainResidentCleanup{mode: mode}
			runner := residentTestRunner(t, runtime)
			runner.Clock = &manualClock{}
			cfg := residentTestConfig()
			cfg.MaxCases = 2
			result, err := Search(context.Background(), cfg, ResidentGenerator{residentInputControl{}, residentTestTopology()}, runner)
			if err == nil || result.Completed != 0 || runtime.cleanups != 1 || len(runtime.inputs) != 1 {
				t.Fatalf("uncertain cleanup counted as completed: %+v %v %+v", result, err, runtime)
			}
			artifact := scenarioFile(runner)
			before, err := os.ReadFile(artifact)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join(filepath.Dir(artifact), "result.json"))
			if err != nil {
				t.Fatal(err)
			}
			var detail caseResult
			if err := json.Unmarshal(raw, &detail); err != nil {
				t.Fatal(err)
			}
			if detail.Cleaned || detail.FailurePhase != "cleanup" || len(detail.Secondary) == 0 {
				t.Fatalf("missing uncertain cleanup ownership: %s", raw)
			}
			driver := runner.Driver.(*ResidentWorkflowDriver)
			driver.mu.Lock()
			active, cleaning := driver.active, driver.cleaning
			driver.mu.Unlock()
			if !active || !cleaning {
				t.Fatal("uncertain fixture was released")
			}
			// A fresh run ID must not bypass the driver's unresolved ownership latch.
			next, err := (ResidentGenerator{residentInputControl{}, residentTestTopology()}).Next(context.Background(), GenerateRequest{Index: 1})
			if err != nil {
				t.Fatal(err)
			}
			checks := runtime.checks
			err = driver.Run(context.Background(), next, func(json.RawMessage) error { return nil })
			if err == nil || !strings.Contains(err.Error(), "not cleaned") || runtime.checks != checks || len(runtime.inputs) != 1 {
				t.Fatal("uncertain fixture reused", err)
			}
			after, err := os.ReadFile(artifact)
			if err != nil || string(before) != string(after) {
				t.Fatal("failure artifact changed", err)
			}
		})
	}
}

type lateCreationRuntime struct {
	residentControl
	entered, release, created chan struct{}
	live                      int
	cleanupEntered            chan struct{}
}

func (r *lateCreationRuntime) Execute(ctx context.Context, _ ResidentTopology, _, _ string) error {
	close(r.entered)
	<-ctx.Done()
	<-r.release
	// A request admitted before cancellation may create an execution afterward.
	r.live++
	close(r.created)
	return ctx.Err()
}
func (r *lateCreationRuntime) Cleanup(ctx context.Context, _ ResidentTopology, _ string) error {
	select {
	case <-r.created:
	default:
		return errors.New("census ran before admitted request drained")
	}
	if r.cleanupEntered != nil {
		close(r.cleanupEntered)
		<-ctx.Done()
		return ctx.Err()
	}
	r.cleanups++
	if r.live != 1 {
		return errors.New("late execution missing from cleanup census")
	}
	r.live = 0
	return nil
}

func TestResidentCleanupAccountsForLateCreationBeforeCensus(t *testing.T) {
	runtime := &lateCreationRuntime{entered: make(chan struct{}), release: make(chan struct{}), created: make(chan struct{})}
	driver := &ResidentWorkflowDriver{Runtime: runtime, Directory: filepath.Join(t.TempDir(), "runtime")}
	scenario, err := (ResidentGenerator{residentInputControl{}, residentTestTopology()}).Next(context.Background(), GenerateRequest{})
	if err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = driver.Run(parent, scenario, func(json.RawMessage) error { return nil })
	}()
	t.Cleanup(func() { drainResidentCleanupTest(t, cancel, runtime.release, runDone) })
	waitResidentCleanupControl(t, runtime.entered)
	cancel()
	// An expired cleanup budget must retain ownership and must not call the
	// remote census while the admitted request can still create an execution.
	expired, stop := context.WithCancel(context.Background())
	stop()
	if err := driver.Cleanup(expired); !errors.Is(err, ErrPending) || runtime.cleanups != 0 {
		t.Fatal("pending producer certified empty", err)
	}
	if err := driver.Run(context.Background(), scenario, func(json.RawMessage) error { return nil }); err == nil {
		t.Fatal("pending producer reused")
	}
	close(runtime.release)
	waitResidentCleanupControl(t, runDone)
	if err := driver.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runtime.cleanups != 1 || runtime.live != 0 {
		t.Fatal("late execution leaked", runtime.cleanups, runtime.live)
	}
	driver.mu.Lock()
	active := driver.active
	driver.mu.Unlock()
	if active {
		t.Fatal("verified recovery remained pending")
	}
}

func TestResidentCleanupStagesShareOneLogicalDeadline(t *testing.T) {
	runtime := &lateCreationRuntime{entered: make(chan struct{}), release: make(chan struct{}), created: make(chan struct{}), cleanupEntered: make(chan struct{})}
	driver := &ResidentWorkflowDriver{Runtime: runtime, Directory: filepath.Join(t.TempDir(), "runtime")}
	scenario, err := (ResidentGenerator{residentInputControl{}, residentTestTopology()}).Next(context.Background(), GenerateRequest{})
	if err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	var cleanupWorkerDone, supervisorDone chan struct{}
	cleanupCancel := func() {}
	go func() {
		defer close(runDone)
		_ = driver.Run(parent, scenario, func(json.RawMessage) error { return nil })
	}()
	t.Cleanup(func() {
		drainResidentCleanupTest(t, func() { cancel(); cleanupCancel() }, runtime.release, runDone, cleanupWorkerDone, supervisorDone)
	})
	waitResidentCleanupControl(t, runtime.entered)
	clock := &observedTimers{manualClock: &manualClock{}, created: make(chan struct{}, 8)}
	type outcome struct {
		err     error
		pending <-chan error
	}
	finished := make(chan outcome, 1)
	cleanupWorkerDone = make(chan struct{})
	supervisorDone = make(chan struct{})
	cleanupContext, stopCleanup := context.WithCancel(context.Background())
	cleanupCancel = stopCleanup
	var workerClosed sync.Once
	closeWorker := func() { workerClosed.Do(func() { close(cleanupWorkerDone) }) }
	go func() {
		defer close(supervisorDone)
		err, pending := bounded(cleanupContext, clock, time.Second, func(ctx context.Context) error { defer closeWorker(); return driver.Cleanup(ctx) })
		if pending == nil {
			closeWorker()
		}
		finished <- outcome{err, pending}
	}()
	waitResidentCleanupControl(t, clock.created)
	clock.advance(500 * time.Millisecond)
	close(runtime.release)
	waitResidentCleanupControl(t, runDone)
	waitResidentCleanupControl(t, runtime.cleanupEntered)
	// Census begins halfway through the same deadline; it does not get a fresh
	// second after producer-drain consumed the first half.
	clock.advance(500 * time.Millisecond)
	var result outcome
	select {
	case result = <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup renewed its deadline")
	}
	if !errors.Is(result.err, context.DeadlineExceeded) {
		t.Fatal("logical deadline missing", result.err)
	}
	if result.pending != nil {
		select {
		case <-result.pending:
		case <-time.After(3 * time.Second):
			t.Fatal("cleanup callback did not drain")
		}
	}
	if !clock.Now().Equal(time.Time{}.Add(time.Second)) || runtime.live != 1 {
		t.Fatal("cleanup changed time or lost unresolved execution")
	}
	if err := driver.Run(context.Background(), scenario, func(json.RawMessage) error { return nil }); err == nil {
		t.Fatal("expired census allowed reuse")
	}
	// Explicit recovery of this same owned case can finish cleanup; no new case
	// was admitted while its execution remained unresolved.
	runtime.cleanupEntered = nil
	if err := driver.Cleanup(context.Background()); err != nil || runtime.live != 0 {
		t.Fatal("same-case recovery failed", err)
	}
}

func waitResidentCleanupControl(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup test control did not become observable")
	}
}

// Test teardown cancels both owners and drains their goroutines even when a
// watchdog assertion aborts before logical-time advancement. Closed completion
// channels permit both the assertion and teardown to observe termination.
func drainResidentCleanupTest(t *testing.T, cancel func(), release chan struct{}, done ...<-chan struct{}) {
	t.Helper()
	cancel()
	select {
	case <-release:
	default:
		close(release)
	}
	for _, ch := range done {
		if ch == nil {
			continue
		}
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Error("cleanup test goroutine failed to drain during teardown")
		}
	}
}
