package simulation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

type WorkloadLimits struct {
	MaxOperations   int      `json:"max_operations"`
	MaxDepth        int      `json:"max_depth"`
	MaxPayloadBytes int      `json:"max_payload_bytes"`
	Features        []string `json:"features"`
}
type GenerateRequest struct {
	Index        uint64         `json:"index"`
	WorkloadSeed uint64         `json:"workload_seed"`
	FaultSeed    uint64         `json:"fault_seed"`
	Limits       WorkloadLimits `json:"limits"`
}
type Scenario struct {
	Version  uint32 `json:"version"`
	Kind     string `json:"kind"`
	Workload []byte `json:"workload"`
	Topology []byte `json:"topology"`
	Faults   []byte `json:"faults"`
}
type GeneratorInfo struct {
	Version      string   `json:"version"`
	SHA256       string   `json:"sha256"`
	Capabilities []string `json:"capabilities"`
}

// Next returns io.EOF when its supported corpus is exhausted. A generator must
// honor cancellation; it must not start external/native resources.
type Generator interface {
	Info() GeneratorInfo
	Next(context.Context, GenerateRequest) (Scenario, error)
}

// Driver owns all per-case work. Cleanup must stop and drain Run/Settle even when
// they are still returning after cancellation, before destructive scoped teardown.
// Cleanup never deletes evidence.
// A runner with unresolved work is not reusable. Real-stack drivers must supply
// observed recovery assertions in Settle; successful submission is insufficient.
// FailureReportingDriver accepts a per-case runner-owned first-failure latch.
// Report before beginning failure cleanup. Reports after cancellation/deadline
// cannot supersede that earlier stop; cleanup errors must never be reported.
type FailureReportingDriver interface{ SetFailureReporter(func(error)) }

type Driver interface {
	Validate(Scenario, WorkloadLimits) error
	Run(context.Context, Scenario, func(json.RawMessage) error) error
	Settle(context.Context) error
	Cleanup(context.Context) error
}
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}
type WallClock struct{}

func (WallClock) Now() time.Time                 { return time.Now() }
func (WallClock) NewTimer(d time.Duration) Timer { return wallTimer{time.NewTimer(d)} }

type wallTimer struct{ *time.Timer }

func (t wallTimer) C() <-chan time.Time { return t.Timer.C }

type Provenance struct {
	Source   string            `json:"source"`
	Versions map[string]string `json:"versions"` // tool/native/image versions, or explicit modeled/not-used values
}
type Runner struct {
	Driver     Driver
	Clock      Clock
	Directory  string // new run-scoped directory; existing paths are never overwritten
	Provenance Provenance
}
type SearchConfig struct {
	MaxCases      uint64         `json:"max_cases"`
	Continuous    bool           `json:"continuous"`
	MaxDuration   time.Duration  `json:"max_duration"`
	MaxInFlight   int            `json:"max_in_flight"`
	SettleBudget  time.Duration  `json:"settle_budget"`
	CleanupBudget time.Duration  `json:"cleanup_budget"`
	MaxTraceBytes int            `json:"max_trace_bytes"`
	WorkloadSeed  uint64         `json:"workload_seed"`
	FaultSeed     uint64         `json:"fault_seed"`
	Limits        WorkloadLimits `json:"limits"`
}
type RunResult struct {
	Completed    uint64 `json:"completed"`
	StopReason   string `json:"stop_reason"`
	EvidencePath string `json:"evidence_path"`
}
type artifact struct {
	Version    int             `json:"version"`
	Config     SearchConfig    `json:"config"`
	Request    GenerateRequest `json:"request"`
	Generator  GeneratorInfo   `json:"generator"`
	Provenance Provenance      `json:"provenance"`
	Scenario   Scenario        `json:"scenario"`
	SHA256     string          `json:"scenario_sha256"`
	ReplayOf   string          `json:"replay_of,omitempty"`
}
type caseResult struct {
	FailureFingerprint *FailureFingerprint `json:"failure_fingerprint,omitempty"`
	FailurePhase       string              `json:"failure_phase,omitempty"`
	FirstFailure       string              `json:"first_failure,omitempty"`
	Secondary          []string            `json:"secondary,omitempty"`
	Settled            bool                `json:"settled"`
	Cleaned            bool                `json:"cleaned"`
}

var ErrPending = errors.New("driver or generator still running; process exit required")

func hash(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func streamSeed(domain string, seed, index uint64) uint64 {
	raw := []byte(domain)
	raw = binary.BigEndian.AppendUint64(raw, seed)
	raw = binary.BigEndian.AppendUint64(raw, index)
	sum := sha256.Sum256(raw)
	return binary.BigEndian.Uint64(sum[:8])
}
func save(path string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(append(raw, '\n'))
	syncErr := f.Sync()
	closeErr := f.Close()
	if err = errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
func validateRunner(cfg SearchConfig, r *Runner) error {
	if r == nil || r.Driver == nil || r.Clock == nil || r.Directory == "" || r.Provenance.Source == "" || (r.Provenance.Versions["toolchain"] == "" || r.Provenance.Versions["native"] == "" || r.Provenance.Versions["images"] == "") || cfg.MaxInFlight != 1 || (!cfg.Continuous && cfg.MaxCases == 0) || cfg.MaxDuration <= 0 || cfg.SettleBudget <= 0 || cfg.CleanupBudget <= 0 || cfg.MaxTraceBytes <= 0 || cfg.MaxTraceBytes > 64<<20 || cfg.Limits.MaxOperations <= 0 || cfg.Limits.MaxDepth <= 0 || cfg.Limits.MaxPayloadBytes <= 0 || cfg.Limits.MaxPayloadBytes > 4<<20 {
		return errors.New("invalid explicit search limits or runner")
	}
	return nil
}

// bounded uses injected time. A timed-out goroutine retains its ownership; callers
// may not start the next case until cleanup and the original call both complete.
func bounded(ctx context.Context, clock Clock, budget time.Duration, fn func(context.Context) error) (error, <-chan error) {
	if err := ctx.Err(); err != nil {
		return err, nil
	}
	if budget <= 0 {
		return context.DeadlineExceeded, nil
	}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := clock.NewTimer(budget)
	defer timer.Stop()
	done := make(chan error, 1)
	go func() { done <- guarded(func() error { return fn(child) }) }()
	select {
	case err := <-done:
		return err, nil
	case <-ctx.Done():
		select {
		case err := <-done:
			return err, nil
		default:
		}
		return ctx.Err(), done
	case <-timer.C():
		select {
		case err := <-done:
			return err, nil
		default:
		}
		return context.DeadlineExceeded, done
	}
}

// Search executes one case at a time, persisting expanded bytes before Validate
// or Run. Generation errors are failures, never completed cases. Budget is an
// explored-prefix result, not a correctness pass.
func Search(ctx context.Context, cfg SearchConfig, gen Generator, r *Runner) (RunResult, error) {
	return search(ctx, cfg, gen, r, "")
}
func search(ctx context.Context, cfg SearchConfig, gen Generator, r *Runner, replayOf string) (result RunResult, resultErr error) {
	if ctx == nil || gen == nil {
		return result, errors.New("context and generator required")
	}
	if err := validateRunner(cfg, r); err != nil {
		return result, err
	}
	cfg.Limits.Features = slices.Clone(cfg.Limits.Features)
	rCopy := *r
	r = &rCopy
	r.Provenance.Versions = maps.Clone(r.Provenance.Versions)
	info := gen.Info()
	info.Capabilities = slices.Clone(info.Capabilities)
	_, hashErr := hex.DecodeString(info.SHA256)
	if info.Version == "" || hashErr != nil || len(info.SHA256) != 64 || len(info.Capabilities) == 0 {
		return result, errors.New("generator metadata required")
	}
	if err := makeDir(r.Directory); err != nil {
		return result, err
	}
	result.EvidencePath = r.Directory
	result.StopReason = "first_failure"
	defer func() {
		if err := save(filepath.Join(r.Directory, "result.json"), result); err != nil {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	if err := save(filepath.Join(r.Directory, "run.json"), struct {
		Config     SearchConfig
		Generator  GeneratorInfo
		Provenance Provenance
	}{cfg, info, r.Provenance}); err != nil {
		return result, err
	}
	end := r.Clock.Now().Add(cfg.MaxDuration)
	for index := uint64(0); cfg.MaxCases == 0 || index < cfg.MaxCases; index++ {
		if err := ctx.Err(); err != nil {
			result.StopReason = "canceled"
			return result, err
		}
		if !r.Clock.Now().Before(end) {
			result.StopReason = "budget"
			return result, nil
		}
		dir := filepath.Join(r.Directory, fmt.Sprintf("case-%020d", index))
		if err := makeDir(dir); err != nil {
			return result, err
		}
		request := GenerateRequest{index, streamSeed("workload/v1", cfg.WorkloadSeed, index), streamSeed("fault/v1", cfg.FaultSeed, index), cfg.Limits}
		request.Limits.Features = slices.Clone(request.Limits.Features)
		if saved, ok := gen.(*savedGenerator); ok {
			request = saved.record.Request
		}
		if err := save(filepath.Join(dir, "request.json"), request); err != nil {
			return result, err
		}
		var scenario Scenario
		err, pending := bounded(ctx, r.Clock, end.Sub(r.Clock.Now()), func(c context.Context) error {
			var e error
			generatedRequest := request
			generatedRequest.Limits.Features = slices.Clone(request.Limits.Features)
			scenario, e = gen.Next(c, generatedRequest)
			return e
		})
		if err != nil {
			result.StopReason = "first_failure"
			phase := "generation"
			if errors.Is(err, io.EOF) {
				result.StopReason = "completed"
				err = nil
			}
			if errors.Is(err, context.Canceled) {
				result.StopReason = "canceled"
			}
			if errors.Is(err, context.DeadlineExceeded) {
				result.StopReason = "budget"
			}
			detail := caseResult{FailurePhase: phase}
			if err != nil {
				detail.FirstFailure = err.Error()
			}
			if pending != nil && joinPending(r.Clock, pending, cfg.CleanupBudget) != nil {
				err = errors.Join(err, ErrPending)
				detail.Secondary = append(detail.Secondary, ErrPending.Error())
			}
			return result, errors.Join(err, save(filepath.Join(dir, "result.json"), detail))
		}
		if scenario.Version != 1 || scenario.Kind == "" || len(scenario.Workload)+len(scenario.Topology)+len(scenario.Faults) > cfg.Limits.MaxPayloadBytes {
			err = errors.New("generated scenario exceeds envelope limits")
			result.StopReason = "first_failure"
			return result, errors.Join(err, save(filepath.Join(dir, "result.json"), caseResult{FailurePhase: "generation", FirstFailure: err.Error()}))
		}
		raw, _ := json.Marshal(scenario)
		record := artifact{1, cfg, request, info, r.Provenance, scenario, hash(raw), replayOf}
		if err = save(filepath.Join(dir, "scenario.json"), record); err != nil {
			return result, err
		}
		detail, err := r.runCase(ctx, cfg, scenario, dir, end)
		err = errors.Join(err, save(filepath.Join(dir, "result.json"), detail))
		if err != nil {
			result.StopReason = "first_failure"
			if errors.Is(err, context.Canceled) {
				result.StopReason = "canceled"
			} else if errors.Is(err, context.DeadlineExceeded) && detail.FailurePhase == "run" {
				result.StopReason = "budget"
			}
			return result, err
		}
		result.Completed++
	}
	result.StopReason = "completed"
	return result, nil
}

func (r *Runner) runCase(ctx context.Context, cfg SearchConfig, s Scenario, dir string, end time.Time) (detail caseResult, primary error) {
	limits := cfg.Limits
	limits.Features = slices.Clone(limits.Features)
	if err := guarded(func() error { return r.Driver.Validate(cloneScenario(s), limits) }); err != nil {
		detail.FailurePhase = "validation"
		detail.FirstFailure = err.Error()
		return detail, err
	}
	trace, err := os.OpenFile(filepath.Join(dir, "trace.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return detail, err
	}
	var mu sync.Mutex
	sealed := false
	size := 0
	var traceFailure error
	tracePrimary := false
	var reportedFailure error
	reportOpen := false
	phaseGeneration := uint64(0)
	phaseEnd := end
	operation, cancel := context.WithCancel(ctx)
	defer cancel()
	emit := func(raw json.RawMessage) error {
		mu.Lock()
		defer mu.Unlock()
		if sealed {
			return errors.New("trace sealed")
		}
		if traceFailure != nil {
			return traceFailure
		}
		if !json.Valid(raw) || len(raw)+size+1 > cfg.MaxTraceBytes {
			tracePrimary = operation.Err() == nil && r.Clock.Now().Before(phaseEnd)
			traceFailure = errors.New("invalid observation or trace budget exceeded")
			cancel()
			return traceFailure
		}
		_, err := trace.Write(append(append([]byte(nil), raw...), '\n'))
		if err == nil {
			err = trace.Sync()
		}
		size += len(raw) + 1
		if err != nil {
			tracePrimary = operation.Err() == nil && r.Clock.Now().Before(phaseEnd)
			traceFailure = err
			cancel()
		}
		return err
	}
	// Observe the same latch at every phase boundary. Drivers may emit recovery
	// observations until Cleanup has drained; ignoring emit's error cannot pass.
	traceError := func() error { mu.Lock(); defer mu.Unlock(); return traceFailure }
	// Each phase owns a reporting generation and deadline. A late callback or
	// returned error cannot supersede an earlier stop, nor enter a later phase.
	runPhase := func(budget time.Duration, fn func(context.Context) error) (error, <-chan error) {
		mu.Lock()
		phaseGeneration++
		generation := phaseGeneration
		phaseEnd = minTime(end, r.Clock.Now().Add(budget))
		deadline := phaseEnd
		reportOpen = true
		mu.Unlock()
		report := func(e error) {
			if e == nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if sealed || !reportOpen || generation != phaseGeneration || traceFailure != nil || reportedFailure != nil || operation.Err() != nil || !r.Clock.Now().Before(deadline) {
				return
			}
			reportedFailure = e
			cancel()
		}
		var result error
		if reporting, ok := r.Driver.(FailureReportingDriver); ok {
			result = guarded(func() error { reporting.SetFailureReporter(report); return nil })
			report(result)
		}
		var pending <-chan error
		if result == nil {
			result, pending = bounded(operation, r.Clock, deadline.Sub(r.Clock.Now()), func(c context.Context) error {
				e := guarded(func() error { return fn(c) })
				report(e)
				return e
			})
		}
		mu.Lock()
		reportOpen = false
		reported, observedTrace, earlierTrace := reportedFailure, traceFailure, tracePrimary
		mu.Unlock()
		switch {
		case earlierTrace:
			result = observedTrace
		case reported != nil:
			result = reported
		case ctx.Err() != nil:
			result = ctx.Err()
		case !r.Clock.Now().Before(deadline):
			result = context.DeadlineExceeded
		}
		return result, pending
	}
	phase := "run"
	primary, pending := runPhase(end.Sub(r.Clock.Now()), func(c context.Context) error { return r.Driver.Run(c, cloneScenario(s), emit) })
	if primary == nil {
		phase = "settle"
		primary, pending = runPhase(min(cfg.SettleBudget, end.Sub(r.Clock.Now())), r.Driver.Settle)
		detail.Settled = primary == nil
	}
	if primary != nil {
		detail.FailurePhase = phase
		detail.FirstFailure = primary.Error()
		if fingerprint, ok := FingerprintOf(primary); ok {
			detail.FailureFingerprint = &fingerprint
		}
		// Flush the primary before cleanup can fail or become stuck.
		primary = errors.Join(primary, save(filepath.Join(dir, "failure.json"), detail))
	}
	cleanupEnd := r.Clock.Now().Add(cfg.CleanupBudget)
	cancel()
	cleanup, cleanupPending := bounded(context.Background(), r.Clock, cfg.CleanupBudget, r.Driver.Cleanup)
	if err := traceError(); err != nil && primary == nil {
		cleanup = errors.Join(err, cleanup)
	}
	detail.Cleaned = cleanup == nil
	if cleanup != nil {
		detail.Secondary = append(detail.Secondary, cleanup.Error())
		if primary == nil {
			primary = cleanup
			detail.FirstFailure = cleanup.Error()
			detail.FailurePhase = "cleanup"
			primary = errors.Join(primary, save(filepath.Join(dir, "failure.json"), detail))
		}
	}
	for _, done := range []<-chan error{pending, cleanupPending} {
		if done != nil && joinPending(r.Clock, done, cleanupEnd.Sub(r.Clock.Now())) != nil {
			detail.Secondary = append(detail.Secondary, ErrPending.Error())
			primary = errors.Join(primary, ErrPending)
		}
	}
	// Seal under the same lock used by emit, after all bounded drain attempts.
	// No observation can race the final latch check or alter a finished trace.
	mu.Lock()
	sealed = true
	finalTraceError, closeErr := traceFailure, trace.Close()
	mu.Unlock()
	if primary == nil && (finalTraceError != nil || closeErr != nil) {
		primary = errors.Join(finalTraceError, closeErr)
		detail.FailurePhase = "cleanup"
		detail.FirstFailure = primary.Error()
		if fingerprint, ok := FingerprintOf(primary); ok {
			detail.FailureFingerprint = &fingerprint
		}
		detail.Cleaned = false
		primary = errors.Join(primary, save(filepath.Join(dir, "failure.json"), detail))
	} else {
		for _, secondary := range []error{finalTraceError, closeErr} {
			if secondary != nil && !errors.Is(primary, secondary) {
				detail.Secondary = append(detail.Secondary, secondary.Error())
				primary = errors.Join(primary, secondary)
			}
		}
	}
	return detail, primary
}

// Replay reads saved expanded bytes, never a generator. Original artifacts remain
// untouched; Runner.Directory receives a new trace and current tool provenance.
func Replay(ctx context.Context, path string, r *Runner) (RunResult, error) {
	record, err := readReplayArtifact(path)
	if err != nil {
		return RunResult{}, err
	}
	cfg := record.Config
	cfg.MaxCases = 1
	cfg.Continuous = false
	return search(ctx, cfg, &savedGenerator{record: record}, r, path)
}

func readReplayArtifact(path string) (artifact, error) {
	f, err := os.Open(path)
	if err != nil {
		return artifact{}, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, (16<<20)+1))
	if err != nil {
		return artifact{}, err
	}
	if len(raw) > 16<<20 {
		return artifact{}, errors.New("oversized scenario artifact")
	}
	var record artifact
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&record) != nil || decoder.Decode(new(any)) != io.EOF || record.Version != 1 {
		return artifact{}, errors.New("invalid scenario artifact")
	}
	scenarioRaw, _ := json.Marshal(record.Scenario)
	if hash(scenarioRaw) != record.SHA256 {
		return artifact{}, errors.New("scenario hash mismatch")
	}
	return record, nil
}

type savedGenerator struct{ record artifact }

func (g *savedGenerator) Info() GeneratorInfo { return g.record.Generator }
func (g *savedGenerator) Next(context.Context, GenerateRequest) (Scenario, error) {
	return g.record.Scenario, nil
}

func joinPending(clock Clock, done <-chan error, budget time.Duration) error {
	select {
	case <-done:
		return nil
	default:
	}
	if budget <= 0 {
		return ErrPending
	}
	timer := clock.NewTimer(budget)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C():
		return ErrPending
	}
}

func makeDir(path string) error {
	if err := os.Mkdir(path, 0700); err != nil {
		return err
	}
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(parent.Sync(), parent.Close())
}

func cloneScenario(s Scenario) Scenario {
	s.Workload = bytes.Clone(s.Workload)
	s.Topology = bytes.Clone(s.Topology)
	s.Faults = bytes.Clone(s.Faults)
	return s
}

func guarded(fn func() error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("scenario callback panic: %v", value)
		}
	}()
	return fn()
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
