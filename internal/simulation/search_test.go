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

func searchConfig() SearchConfig {
	return SearchConfig{MaxCases: 2, MaxDuration: time.Minute, MaxInFlight: 1, SettleBudget: time.Second, CleanupBudget: time.Second, MaxTraceBytes: 8 << 20, WorkloadSeed: 17, FaultSeed: 29, Limits: WorkloadLimits{MaxOperations: 256, MaxDepth: 1, MaxPayloadBytes: 1 << 20, Features: []string{CoupledKind}}}
}
func testRunner(t *testing.T, d Driver) *Runner {
	return &Runner{Driver: d, Clock: WallClock{}, Directory: filepath.Join(t.TempDir(), "run"), Provenance: Provenance{Source: "test-source", Versions: map[string]string{"toolchain": "go1.27.1", "native": "modeled; no native", "images": "none"}}}
}
func scenarioFile(r *Runner) string {
	return filepath.Join(r.Directory, "case-00000000000000000000", "scenario.json")
}
func loadArtifact(t *testing.T, path string) artifact {
	t.Helper()
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var a artifact
	if e = json.Unmarshal(raw, &a); e != nil {
		t.Fatal(e)
	}
	return a
}

func TestSearchCoupledSavedReplayAndTamper(t *testing.T) {
	raw, err := os.ReadFile(coupledScenarioPath)
	if err != nil {
		t.Fatal(err)
	}
	gen, err := NewCoupledCorpus(raw)
	if err != nil {
		t.Fatal(err)
	}
	r := testRunner(t, &CoupledDriver{})
	result, err := Search(context.Background(), searchConfig(), gen, r)
	if err != nil || result.Completed != 1 || result.StopReason != "completed" {
		t.Fatal(result, err)
	}
	original := loadArtifact(t, scenarioFile(r))
	// Change the generator's in-memory future output. Replay takes no generator.
	gen.scenarios[0].Faults = []byte(`[]`)
	replay := testRunner(t, &CoupledDriver{})
	result, err = Replay(context.Background(), scenarioFile(r), replay)
	if err != nil || result.Completed != 1 {
		t.Fatal(result, err)
	}
	saved := loadArtifact(t, scenarioFile(replay))
	if saved.SHA256 != original.SHA256 || saved.Request.WorkloadSeed != original.Request.WorkloadSeed || saved.Request.FaultSeed != original.Request.FaultSeed {
		t.Fatal("replay identity changed")
	}
	a, _ := os.ReadFile(filepath.Join(r.Directory, "case-00000000000000000000", "trace.jsonl"))
	b, _ := os.ReadFile(filepath.Join(replay.Directory, "case-00000000000000000000", "trace.jsonl"))
	if string(a) != string(b) || len(a) == 0 {
		t.Fatal("event order changed")
	}
	original.Scenario.Faults = []byte(`[]`)
	bad := filepath.Join(t.TempDir(), "tampered.json")
	if err = save(bad, original); err != nil {
		t.Fatal(err)
	}
	if _, err = Replay(context.Background(), bad, testRunner(t, &CoupledDriver{})); err == nil {
		t.Fatal("tamper accepted")
	}
	if _, err = Search(context.Background(), searchConfig(), gen, r); err == nil {
		t.Fatal("existing evidence overwritten")
	}
}

type testGenerator struct {
	calls    int
	fail     error
	requests []GenerateRequest
}

func (g *testGenerator) Info() GeneratorInfo {
	return GeneratorInfo{"test-v1", hash([]byte("test")), []string{"test"}}
}
func (g *testGenerator) Next(_ context.Context, r GenerateRequest) (Scenario, error) {
	g.calls++
	g.requests = append(g.requests, r)
	return Scenario{1, "test", []byte(`{}`), []byte(`{}`), []byte(`[]`)}, g.fail
}

type testDriver struct {
	validate func(Scenario, WorkloadLimits) error
	run      func(context.Context, Scenario, func(json.RawMessage) error) error
	settle   func(context.Context) error
	cleanup  func(context.Context) error
}

func (d testDriver) Validate(s Scenario, l WorkloadLimits) error {
	if d.validate != nil {
		return d.validate(s, l)
	}
	return nil
}
func (d testDriver) Run(c context.Context, s Scenario, e func(json.RawMessage) error) error {
	if d.run != nil {
		return d.run(c, s, e)
	}
	return nil
}
func (d testDriver) Settle(c context.Context) error {
	if d.settle != nil {
		return d.settle(c)
	}
	return nil
}
func (d testDriver) Cleanup(c context.Context) error {
	if d.cleanup != nil {
		return d.cleanup(c)
	}
	return nil
}

func TestFirstFailureBeforeCleanupAndGenerationStops(t *testing.T) {
	primary := errors.New("invariant failed")
	gen := &testGenerator{}
	var r *Runner
	d := testDriver{run: func(ctx context.Context, s Scenario, emit func(json.RawMessage) error) error {
		record := loadArtifact(t, scenarioFile(r))
		if record.SHA256 == "" {
			t.Fatal("not saved before launch")
		}
		if err := emit(json.RawMessage(`{"event":"before failure"}`)); err != nil {
			return err
		}
		return primary
	}, cleanup: func(context.Context) error { return errors.New("cleanup failed") }}
	r = testRunner(t, d)
	result, err := Search(context.Background(), searchConfig(), gen, r)
	if !errors.Is(err, primary) || result.Completed != 0 || result.StopReason != "first_failure" || gen.calls != 1 {
		t.Fatal(result, err, gen.calls)
	}
	raw, _ := os.ReadFile(filepath.Join(r.Directory, "case-00000000000000000000", "result.json"))
	var detail caseResult
	_ = json.Unmarshal(raw, &detail)
	if detail.FirstFailure != primary.Error() || len(detail.Secondary) != 1 || detail.Settled {
		t.Fatal(string(raw))
	}
}
func TestGenerationAndSettleFailuresAreNotPasses(t *testing.T) {
	for _, phase := range []string{"generation", "settle", "trace"} {
		t.Run(phase, func(t *testing.T) {
			gen := &testGenerator{}
			d := testDriver{}
			if phase == "generation" {
				gen.fail = errors.New("unsupported generated combination")
			}
			if phase == "settle" {
				d.settle = func(context.Context) error { return errors.New("recovery absent") }
			}
			if phase == "trace" {
				d.run = func(_ context.Context, _ Scenario, emit func(json.RawMessage) error) error {
					_ = emit(json.RawMessage(`bad-json`))
					return nil
				}
			}
			result, err := Search(context.Background(), searchConfig(), gen, testRunner(t, d))
			if err == nil || result.Completed != 0 || gen.calls != 1 {
				t.Fatal(result, err, gen.calls)
			}
		})
	}
}
func TestSeparateRNGStreams(t *testing.T) {
	a, b := &testGenerator{}, &testGenerator{}
	cfg := searchConfig()
	if _, err := Search(context.Background(), cfg, a, testRunner(t, testDriver{})); err != nil {
		t.Fatal(err)
	}
	cfg.WorkloadSeed++
	if _, err := Search(context.Background(), cfg, b, testRunner(t, testDriver{})); err != nil {
		t.Fatal(err)
	}
	for i := range a.requests {
		if a.requests[i].FaultSeed != b.requests[i].FaultSeed || a.requests[i].WorkloadSeed == b.requests[i].WorkloadSeed {
			t.Fatal("RNG streams coupled")
		}
	}
}

type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*manualTimer
}
type manualTimer struct {
	clock   *manualClock
	at      time.Time
	ch      chan time.Time
	stopped bool
}

func (c *manualClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *manualClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := &manualTimer{clock: c, at: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, v)
	return v
}
func (t *manualTimer) C() <-chan time.Time { return t.ch }
func (t *manualTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	was := t.stopped
	t.stopped = true
	return !was
}
func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, t := range c.timers {
		if !t.stopped && !t.at.After(c.now) {
			t.stopped = true
			t.ch <- c.now
		}
	}
}
func TestLogicalBudgetCancelsAndDrainsBeforeNextCase(t *testing.T) {
	entered := make(chan struct{})
	returned := make(chan struct{})
	cleaned := false
	d := testDriver{run: func(ctx context.Context, _ Scenario, _ func(json.RawMessage) error) error {
		close(entered)
		<-ctx.Done()
		close(returned)
		return ctx.Err()
	}, cleanup: func(context.Context) error { <-returned; cleaned = true; return nil }}
	clock := &manualClock{}
	r := testRunner(t, d)
	r.Clock = clock
	gen := &testGenerator{}
	done := make(chan error, 1)
	go func() {
		result, err := Search(context.Background(), searchConfig(), gen, r)
		if result.Completed != 0 {
			done <- errors.New("budget counted as pass")
			return
		}
		done <- err
	}()
	<-entered
	clock.advance(time.Minute)
	select {
	case err := <-done:
		if err == nil || errors.Is(err, ErrPending) || !cleaned || gen.calls != 1 {
			t.Fatal(err, cleaned, gen.calls)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("logical timer did not stop driver")
	}
}
func TestCanceledRunPreservesEvidenceAndCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cleaned := false
	d := testDriver{run: func(ctx context.Context, _ Scenario, emit func(json.RawMessage) error) error {
		_ = emit(json.RawMessage(`{"started":true}`))
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}, cleanup: func(context.Context) error { cleaned = true; return nil }}
	r := testRunner(t, d)
	result, err := Search(ctx, searchConfig(), &testGenerator{}, r)
	if !errors.Is(err, context.Canceled) || result.StopReason != "canceled" || result.Completed != 0 || !cleaned {
		t.Fatal(result, err)
	}
	if _, err = os.Stat(scenarioFile(r)); err != nil {
		t.Fatal(err)
	}
}

func TestBlockedCleanupRetainsFirstFailureAndCannotPass(t *testing.T) {
	primary := errors.New("original checker violation")
	cleanupEntered := make(chan struct{})
	release := make(chan struct{})
	cleanupExited := make(chan struct{})
	clock := &manualClock{}
	var r *Runner
	d := testDriver{run: func(context.Context, Scenario, func(json.RawMessage) error) error { return primary }, cleanup: func(context.Context) error {
		raw, err := os.ReadFile(filepath.Join(r.Directory, "case-00000000000000000000", "failure.json"))
		if err != nil || !json.Valid(raw) {
			return errors.New("primary was not durable before cleanup")
		}
		close(cleanupEntered)
		<-release
		close(cleanupExited)
		return nil
	}}
	r = testRunner(t, d)
	r.Clock = clock
	done := make(chan error, 1)
	go func() { _, err := Search(context.Background(), searchConfig(), &testGenerator{}, r); done <- err }()
	<-cleanupEntered
	clock.advance(time.Second)
	select {
	case err := <-done:
		if !errors.Is(err, primary) || !errors.Is(err, ErrPending) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup exceeded logical budget")
	}
	close(release)
	<-cleanupExited
	raw, _ := os.ReadFile(filepath.Join(r.Directory, "case-00000000000000000000", "result.json"))
	var result caseResult
	_ = json.Unmarshal(raw, &result)
	if result.FirstFailure != primary.Error() || result.Cleaned {
		t.Fatal(string(raw))
	}
}

func TestSearchRejectsUnboundedOrUnsupportedInputs(t *testing.T) {
	for _, change := range []func(*SearchConfig){func(c *SearchConfig) { c.MaxInFlight = 2 }, func(c *SearchConfig) { c.MaxCases = 0 }, func(c *SearchConfig) { c.MaxDuration = 0 }, func(c *SearchConfig) { c.CleanupBudget = 0 }} {
		cfg := searchConfig()
		change(&cfg)
		gen := &testGenerator{}
		if _, err := Search(context.Background(), cfg, gen, testRunner(t, testDriver{})); err == nil || gen.calls != 0 {
			t.Fatal("invalid config launched generation", err)
		}
	}
	raw, _ := os.ReadFile(coupledScenarioPath)
	gen, _ := NewCoupledCorpus(raw)
	cfg := searchConfig()
	cfg.Limits.Features = []string{"arbitrary-temporal-workflows"}
	result, err := Search(context.Background(), cfg, gen, testRunner(t, &CoupledDriver{}))
	if err == nil || result.Completed != 0 {
		t.Fatal("unsupported capability passed", result, err)
	}
}

func TestDriverPanicIsRetainedAndCleaned(t *testing.T) {
	cleaned := false
	d := testDriver{run: func(context.Context, Scenario, func(json.RawMessage) error) error { panic("broken invariant") }, cleanup: func(context.Context) error { cleaned = true; return nil }}
	result, err := Search(context.Background(), searchConfig(), &testGenerator{}, testRunner(t, d))
	if err == nil || !cleaned || result.Completed != 0 || result.StopReason != "first_failure" {
		t.Fatal(result, err, cleaned)
	}
}

func TestCoupledFailureReplaysExactSavedBadEffect(t *testing.T) {
	raw, err := os.ReadFile(coupledScenarioPath)
	if err != nil {
		t.Fatal(err)
	}
	var scenario CoupledScenario
	if err = json.Unmarshal(raw, &scenario); err != nil {
		t.Fatal(err)
	}
	scenario.Steps[1].Effect = 999
	raw, _ = json.Marshal(scenario)
	gen, err := NewCoupledCorpus(raw)
	if err != nil {
		t.Fatal(err)
	}
	first := testRunner(t, &CoupledDriver{})
	result, firstErr := Search(context.Background(), searchConfig(), gen, first)
	if firstErr == nil || result.Completed != 0 {
		t.Fatal(result, firstErr)
	}
	trace, err := os.ReadFile(filepath.Join(first.Directory, "case-00000000000000000000", "trace.jsonl"))
	if err != nil || len(trace) == 0 {
		t.Fatal("partial real-step trace missing", err)
	}
	second := testRunner(t, &CoupledDriver{})
	result, replayErr := Replay(context.Background(), scenarioFile(first), second)
	if replayErr == nil || result.Completed != 0 || replayErr.Error() != firstErr.Error() {
		t.Fatal("failure changed", firstErr, replayErr)
	}
}

func TestRecoveryTraceFailuresCannotPass(t *testing.T) {
	for _, phase := range []string{"settle", "cleanup"} {
		t.Run(phase, func(t *testing.T) {
			var emit func(json.RawMessage) error
			var r *Runner
			driver := testDriver{run: func(_ context.Context, _ Scenario, observe func(json.RawMessage) error) error {
				emit = observe
				return nil
			}}
			fail := func(context.Context) error { _ = emit(json.RawMessage(`invalid`)); return nil }
			if phase == "settle" {
				driver.settle = fail
				driver.cleanup = func(context.Context) error {
					// Settling failure must be durable before cleanup is allowed to begin.
					if _, err := os.Stat(filepath.Join(r.Directory, "case-00000000000000000000", "failure.json")); err != nil {
						return err
					}
					return nil
				}
			} else {
				driver.cleanup = fail
			}
			r = testRunner(t, driver)
			result, err := Search(context.Background(), searchConfig(), &testGenerator{}, r)
			if err == nil || result.Completed != 0 || result.StopReason != "first_failure" {
				t.Fatal("invalid recovery trace passed", result, err)
			}
			raw, _ := os.ReadFile(filepath.Join(r.Directory, "case-00000000000000000000", "result.json"))
			var detail caseResult
			_ = json.Unmarshal(raw, &detail)
			if detail.FailurePhase != phase || detail.FirstFailure == "" {
				t.Fatal(string(raw))
			}
			if phase == "settle" && detail.Settled {
				t.Fatal("failed trace marked settled")
			}
			if phase == "cleanup" && detail.Cleaned {
				t.Fatal("failed trace marked cleaned")
			}
			if err := emit(json.RawMessage(`{"late":true}`)); err == nil {
				t.Fatal("finished trace still writable")
			}
		})
	}
}

func TestSearchRecordsPrimaryFingerprintBeforeCleanup(t *testing.T) {
	fingerprint := FailureFingerprint{"acknowledged_state", "missing_after_recovery"}
	primary, err := NewInvariantFailure(fingerprint, errors.New("lost value on node A"))
	if err != nil {
		t.Fatal(err)
	}
	r := testRunner(t, testDriver{
		run:     func(context.Context, Scenario, func(json.RawMessage) error) error { return primary },
		cleanup: func(context.Context) error { return errors.New("cleanup unavailable") },
	})
	result, err := Search(context.Background(), searchConfig(), &testGenerator{}, r)
	if err == nil || result.Completed != 0 {
		t.Fatal(result, err)
	}
	for _, name := range []string{"failure.json", "result.json"} {
		raw, e := os.ReadFile(filepath.Join(r.Directory, "case-00000000000000000000", name))
		if e != nil {
			t.Fatal(e)
		}
		var detail caseResult
		if e = json.Unmarshal(raw, &detail); e != nil {
			t.Fatal(e)
		}
		if detail.FailureFingerprint == nil || *detail.FailureFingerprint != fingerprint || detail.FirstFailure != primary.Error() {
			t.Fatalf("%s: %+v", name, detail)
		}
	}
}

type reportingDriver struct {
	testDriver
	report func(error)
}

func (d *reportingDriver) SetFailureReporter(report func(error)) { d.report = report }
func TestReportedFailureOrderedAgainstBudgetAndTrace(t *testing.T) {
	for _, order := range []string{"failure-first", "budget-first", "cancel-first", "budget-first-returned", "cancel-first-returned", "trace-first", "failure-before-trace"} {
		t.Run(order, func(t *testing.T) {
			clock := &manualClock{}
			parent, stop := context.WithCancel(context.Background())
			defer stop()
			entered := make(chan struct{})
			continueRun := make(chan struct{})
			drained := make(chan struct{})
			primary := errors.New("observed workflow invariant")
			d := &reportingDriver{}
			d.run = func(ctx context.Context, _ Scenario, emit func(json.RawMessage) error) error {
				close(entered)
				<-continueRun
				switch order {
				case "trace-first":
					_ = emit(json.RawMessage("invalid"))
					d.report(primary)
				case "failure-before-trace":
					d.report(primary)
					_ = emit(json.RawMessage("invalid"))
				default:
					d.report(primary)
				}
				<-ctx.Done()
				close(drained)
				if strings.HasSuffix(order, "-returned") {
					return primary
				}
				return ctx.Err()
			}
			d.cleanup = func(ctx context.Context) error {
				select {
				case <-drained:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			r := testRunner(t, d)
			r.Clock = clock
			done := make(chan error, 1)
			go func() { _, e := Search(parent, searchConfig(), &testGenerator{}, r); done <- e }()
			<-entered
			if strings.HasPrefix(order, "budget-first") {
				clock.advance(time.Minute)
			}
			if strings.HasPrefix(order, "cancel-first") {
				stop()
			}
			close(continueRun)
			select {
			case e := <-done:
				if e == nil {
					t.Fatal("false pass")
				}
				raw, re := os.ReadFile(filepath.Join(r.Directory, "case-00000000000000000000", "failure.json"))
				if re != nil {
					t.Fatal(re)
				}
				switch order {
				case "budget-first", "budget-first-returned":
					if errors.Is(e, primary) || !errors.Is(e, context.DeadlineExceeded) {
						t.Fatalf("late error replaced deadline: %v %s", e, raw)
					}
				case "cancel-first", "cancel-first-returned":
					if errors.Is(e, primary) || !errors.Is(e, context.Canceled) {
						t.Fatalf("late error replaced cancel: %v %s", e, raw)
					}
				case "trace-first":
					if errors.Is(e, primary) || !strings.Contains(string(raw), "invalid observation") {
						t.Fatalf("trace replaced: %v %s", e, raw)
					}
				default:
					if !errors.Is(e, primary) {
						t.Fatalf("primary replaced: %v %s", e, raw)
					}
				}
			case <-time.After(3 * time.Second):
				t.Fatal("did not drain")
			}
		})
	}
}
func TestReportingBoundaryRaceKeepsOnePrimary(t *testing.T) {
	for i := 0; i < 20; i++ {
		clock := &manualClock{}
		entered := make(chan struct{})
		start := make(chan struct{})
		reported := make(chan struct{})
		d := &reportingDriver{}
		invariant := errors.New("racing invariant")
		d.run = func(ctx context.Context, _ Scenario, _ func(json.RawMessage) error) error {
			close(entered)
			<-start
			d.report(invariant)
			close(reported)
			<-ctx.Done()
			return ctx.Err()
		}
		d.cleanup = func(ctx context.Context) error {
			select {
			case <-reported:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		r := testRunner(t, d)
		r.Clock = clock
		done := make(chan error, 1)
		go func() { _, e := Search(context.Background(), searchConfig(), &testGenerator{}, r); done <- e }()
		<-entered
		advanced := make(chan struct{})
		go func() { clock.advance(time.Minute); close(advanced) }()
		close(start)
		select {
		case e := <-done:
			if !errors.Is(e, invariant) && !errors.Is(e, context.DeadlineExceeded) {
				t.Fatalf("lost cause %v", e)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("race failed to drain")
		}
		<-advanced
	}
}

func TestSettleReportingPreservesFailureAndRejectsRunCallback(t *testing.T) {
	clock := &manualClock{}
	auditFailure := errors.New("audit invariant before shutdown")
	lateRunFailure := errors.New("late run callback")
	d := &reportingDriver{}
	var oldReport func(error)
	drained := make(chan struct{})
	d.run = func(context.Context, Scenario, func(json.RawMessage) error) error { oldReport = d.report; return nil }
	d.settle = func(ctx context.Context) error {
		oldReport(lateRunFailure)
		if ctx.Err() != nil {
			close(drained)
			return lateRunFailure
		}
		d.report(auditFailure)
		clock.advance(time.Second)
		<-ctx.Done()
		close(drained)
		return ctx.Err()
	}
	d.cleanup = func(ctx context.Context) error {
		select {
		case <-drained:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r := testRunner(t, d)
	r.Clock = clock
	result, err := Search(context.Background(), searchConfig(), &testGenerator{}, r)
	if result.Completed != 0 || !errors.Is(err, auditFailure) || errors.Is(err, lateRunFailure) {
		t.Fatalf("wrong first failure: %+v %v", result, err)
	}
	raw, e := os.ReadFile(filepath.Join(r.Directory, "case-00000000000000000000", "failure.json"))
	if e != nil {
		t.Fatal(e)
	}
	var detail caseResult
	if e = json.Unmarshal(raw, &detail); e != nil {
		t.Fatal(e)
	}
	if detail.FailurePhase != "settle" || detail.FirstFailure != auditFailure.Error() {
		t.Fatalf("wrong receipt: %+v", detail)
	}
}

// Observe timer installation so the test advances logical time only once drain
// is actually waiting. The wall timeout below is solely a harness watchdog.
type observedTimers struct {
	*manualClock
	created chan struct{}
}

func (c *observedTimers) NewTimer(d time.Duration) Timer {
	timer := c.manualClock.NewTimer(d)
	c.created <- struct{}{}
	return timer
}

func TestSuccessfulCleanupCannotCertifyPendingRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	started := false
	defer func() {
		close(release)
		if !started {
			return
		}
		select {
		case <-exited:
		case <-time.After(3 * time.Second):
			t.Error("released operation did not exit")
		}
	}()
	clock := &observedTimers{manualClock: &manualClock{}, created: make(chan struct{}, 8)}
	d := testDriver{run: func(context.Context, Scenario, func(json.RawMessage) error) error {
		close(entered)
		<-release // An admitted operation ignores cancellation and stays owned.
		close(exited)
		return nil
	}, cleanup: func(context.Context) error { return nil }}
	r := testRunner(t, d)
	r.Clock = clock
	gen := &testGenerator{}
	done := make(chan error, 1)
	go func() {
		result, err := Search(ctx, searchConfig(), gen, r)
		if result.Completed != 0 {
			err = errors.New("pending operation counted as completed")
		}
		done <- err
	}()
	watchdog := time.NewTimer(3 * time.Second)
	defer watchdog.Stop()
	select {
	case <-entered:
		started = true
	case err := <-done:
		t.Fatalf("run never started: %v", err)
	case <-watchdog.C:
		t.Fatal("run admission timed out")
	}
	cancel()
	// Generation, run, cleanup, then the pending run's drain timer.
	for i := 0; i < 4; i++ {
		select {
		case <-clock.created:
		case <-watchdog.C:
			t.Fatal("drain timer not installed")
		}
	}
	clock.advance(time.Second)
	select {
	case err := <-done:
		if !errors.Is(err, ErrPending) || !errors.Is(err, context.Canceled) || gen.calls != 1 {
			t.Fatal(err, gen.calls)
		}
	case <-watchdog.C:
		t.Fatal("pending operation exceeded logical drain budget")
	}
	raw, err := os.ReadFile(filepath.Join(r.Directory, "case-00000000000000000000", "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result caseResult
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if result.Cleaned || len(result.Secondary) == 0 {
		t.Fatalf("pending operation falsely certified clean: %s", raw)
	}
}

func TestCleanupCancellationCannotReplaceFirstFailureOutcome(t *testing.T) {
	for _, secondary := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(secondary.Error(), func(t *testing.T) {
			primary := errors.New("original workload failure")
			r := testRunner(t, testDriver{
				run:     func(context.Context, Scenario, func(json.RawMessage) error) error { return primary },
				cleanup: func(context.Context) error { return secondary },
			})
			result, err := Search(context.Background(), searchConfig(), &testGenerator{}, r)
			if !errors.Is(err, primary) || result.StopReason != "first_failure" || result.Completed != 0 {
				t.Fatalf("cleanup replaced primary outcome: %+v %v", result, err)
			}
			raw, readErr := os.ReadFile(filepath.Join(r.Directory, "case-00000000000000000000", "result.json"))
			if readErr != nil {
				t.Fatal(readErr)
			}
			var detail caseResult
			if decodeErr := json.Unmarshal(raw, &detail); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if detail.FirstFailure != primary.Error() || len(detail.Secondary) != 1 || detail.Secondary[0] != secondary.Error() || detail.Cleaned {
				t.Fatalf("lost primary/secondary distinction: %s", raw)
			}
		})
	}
}
