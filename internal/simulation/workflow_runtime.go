package simulation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
)

const WorkflowRuntimeKind = "omes-resident-workflow-v1"

// ResidentTopology binds an exact input to an externally supervised Temporal
// fixture. The runner never starts/stops that fixture. Replay requires the same
// logical binding with an empty run queue; it never adopts old executions.
type ResidentTopology struct {
	Version        int    `json:"version"`
	Address        string `json:"address"`
	Namespace      string `json:"namespace"`
	RunID          string `json:"run_id"`
	NexusEndpoint  string `json:"nexus_endpoint"`
	NexusTaskQueue string `json:"nexus_task_queue"`
	FixtureSHA256  string `json:"fixture_sha256"`
}

var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var residentName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,100}$`)

func (t ResidentTopology) validate() error {
	if t.Version != 1 || t.Address == "" || !residentName.MatchString(t.Namespace) || !residentName.MatchString(t.RunID) || t.NexusEndpoint != "xenon-fuzz" || t.NexusTaskQueue != "omes-xenon-ministack-fuzz" || !digestPattern.MatchString(t.FixtureSHA256) {
		return errors.New("invalid explicit resident fixture binding")
	}
	return nil
}

// ResidentGenerator changes only execution topology/kind. Its underlying
// generator still expands and normalizes every new input before launch.
type ResidentGenerator struct {
	Input    Generator
	Topology ResidentTopology
}

func (g ResidentGenerator) Info() GeneratorInfo {
	i := g.Input.Info()
	i.Version += "/resident-v1"
	i.Capabilities = []string{WorkflowInputKind, WorkflowRuntimeKind}
	return i
}
func (g ResidentGenerator) Next(ctx context.Context, r GenerateRequest) (Scenario, error) {
	s, e := g.Input.Next(ctx, r)
	if e != nil {
		return s, e
	}
	t := g.Topology
	t.RunID = fmt.Sprintf("xenon-generated-%016x-%016x", r.WorkloadSeed, r.Index)
	if e = t.validate(); e != nil {
		return Scenario{}, e
	}
	s.Kind = WorkflowRuntimeKind
	s.Topology, _ = json.Marshal(t)
	return s, nil
}

// WorkflowRuntime owns only its case's worker/process and remote workflows.
// Check is read-only and must reject stale tool/fixture pins. Empty must establish
// no prior case executions. Audit checks independent history/outcome evidence.
// Cleanup must terminate scoped unfinished executions and verify quiescence;
// failure to establish that is an error, never successful cleanup.
type WorkflowRuntime interface {
	Check(context.Context, ResidentTopology) error
	Empty(context.Context, ResidentTopology) error
	Execute(context.Context, ResidentTopology, string, string) error
	Audit(context.Context, ResidentTopology, string) (json.RawMessage, error)
	Cleanup(context.Context, ResidentTopology, string) error
}

type ResidentWorkflowDriver struct {
	initialized  bool
	Runtime      WorkflowRuntime
	Directory    string // new artifact-only directory, separate from shared Runner.Directory
	mu           sync.Mutex
	active       bool
	topology     ResidentTopology
	path         string
	runDone      chan struct{}
	runCancel    context.CancelFunc
	settleDone   chan struct{}
	settleCancel context.CancelFunc
	cleaning     bool
	admitted     bool
	emit         func(json.RawMessage) error
}

func (d *ResidentWorkflowDriver) Validate(s Scenario, l WorkloadLimits) error {
	if d.Runtime == nil || d.Directory == "" || s.Kind != WorkflowRuntimeKind {
		return errors.New("resident driver requires explicit runtime")
	}
	var t ResidentTopology
	if e := strictJSON(s.Topology, &t); e != nil {
		return e
	}
	if e := t.validate(); e != nil {
		return e
	}
	prepared := s
	prepared.Kind = WorkflowInputKind
	prepared.Topology = []byte(`{"version":1,"runtime":"not-started","nexus_endpoint":"xenon-fuzz","sdk":"v1.48.0"}`)
	return (&WorkflowPreparationDriver{}).Validate(prepared, l)
}

// validateRuntimeInput is pure and runs before any case member is dispatched.
func validateRuntimeInput(runtime WorkflowRuntime, s Scenario) error {
	validator, ok := runtime.(interface{ ValidateInput([]byte) error })
	if !ok {
		return nil
	}
	var input GeneratedWorkflow
	if err := strictJSON(s.Workload, &input); err != nil {
		return err
	}
	return validator.ValidateInput(input.Input)
}

func (d *ResidentWorkflowDriver) Run(ctx context.Context, s Scenario, emit func(json.RawMessage) error) error {
	if err := validateRuntimeInput(d.Runtime, s); err != nil {
		return err
	}
	d.mu.Lock()
	if d.active {
		d.mu.Unlock()
		return errors.New("resident driver not cleaned")
	}
	var t ResidentTopology
	if e := strictJSON(s.Topology, &t); e != nil {
		d.mu.Unlock()
		return e
	}
	path := filepath.Join(d.Directory, t.RunID)
	if !d.initialized {
		if e := os.MkdirAll(filepath.Dir(d.Directory), 0700); e != nil {
			d.mu.Unlock()
			return e
		}
		if e := os.Mkdir(d.Directory, 0700); e != nil {
			d.mu.Unlock()
			return e
		}
		d.initialized = true
	}
	if e := os.Mkdir(path, 0700); e != nil {
		d.mu.Unlock()
		return e
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	d.active = true
	d.cleaning = false
	d.settleDone = nil
	d.settleCancel = nil
	d.admitted = false
	d.topology = t
	d.path = path
	d.runDone = done
	d.runCancel = cancel
	d.emit = emit
	d.mu.Unlock()
	defer close(done)
	defer cancel()
	var input GeneratedWorkflow
	if e := strictJSON(s.Workload, &input); e != nil {
		return e
	}
	// The shared envelope is already fsynced by Runner. Save the exact executable
	// protobuf and binding before any tool or network action too.
	if e := save(filepath.Join(path, "binding.json"), t); e != nil {
		return e
	}
	f, e := os.OpenFile(filepath.Join(path, "input.proto"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	_, we := f.Write(input.Input)
	e = errors.Join(we, f.Sync(), f.Close())
	if e != nil {
		return e
	}
	parent, pe := os.Open(path)
	if pe != nil {
		return pe
	}
	if e = errors.Join(parent.Sync(), parent.Close()); e != nil {
		return e
	}
	if e = d.Runtime.Check(ctx, t); e != nil {
		return e
	}
	if e = d.Runtime.Empty(ctx, t); e != nil {
		return e
	}
	d.mu.Lock()
	d.admitted = true
	d.mu.Unlock()
	if e = emit(json.RawMessage(`{"event":"resident-empty-queue-verified"}`)); e != nil {
		return e
	}
	e = d.Runtime.Execute(ctx, t, filepath.Join(path, "input.proto"), path)
	return errors.Join(e, checkFile(filepath.Join(path, "input.proto"), input.SHA256))
}
func (d *ResidentWorkflowDriver) Settle(ctx context.Context) error {
	d.mu.Lock()
	if !d.active || d.cleaning {
		d.mu.Unlock()
		return errors.New("resident case not started or cleaning")
	}
	ctx, cancel := context.WithCancel(ctx)
	d.settleCancel = cancel
	defer cancel()
	d.settleDone = make(chan struct{})
	defer close(d.settleDone)
	t, path, done, emit := d.topology, d.path, d.runDone, d.emit
	d.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	proof, e := d.Runtime.Audit(ctx, t, path)
	if e != nil {
		return e
	}
	if !json.Valid(proof) {
		return errors.New("invalid runtime audit")
	}
	if e = emit(proof); e != nil {
		return e
	}
	return d.Runtime.Check(ctx, t)
}
func (d *ResidentWorkflowDriver) Cleanup(ctx context.Context) error {
	d.mu.Lock()
	if !d.active {
		d.mu.Unlock()
		return nil
	}
	d.cleaning = true
	if d.settleCancel != nil {
		d.settleCancel()
	}
	t, path, done, cancel, settle := d.topology, d.path, d.runDone, d.runCancel, d.settleDone
	d.mu.Unlock()
	cancel()
	select {
	case <-done:
	case <-ctx.Done():
		return errors.Join(ErrPending, ctx.Err())
	}
	if settle != nil {
		select {
		case <-settle:
		case <-ctx.Done():
			return errors.Join(ErrPending, ctx.Err())
		}
	}
	d.mu.Lock()
	admitted := d.admitted
	d.mu.Unlock()
	if admitted {
		if e := d.Runtime.Cleanup(ctx, t, path); e != nil {
			return e
		}
	}
	d.mu.Lock()
	d.active = false
	d.mu.Unlock()
	return nil
}

func (d *ResidentWorkflowDriver) SetFailureReporter(report func(error)) {
	if runtime, ok := d.Runtime.(FailureReportingDriver); ok {
		runtime.SetFailureReporter(report)
	}
}
