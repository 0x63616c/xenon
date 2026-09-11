package simulation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
)

const WorkflowBatchKind = "omes-resident-batch-v1"

type WorkflowBatch struct {
	Concurrency int                 `json:"concurrency"`
	Inputs      []GeneratedWorkflow `json:"inputs"`
}

// WorkflowBatchGenerator expands every member before the shared Runner persists
// and admits the case. Faults remain explicitly unsupported by this component.
type WorkflowBatchGenerator struct {
	Input              Generator
	Topology           ResidentTopology
	Count, Concurrency int
}

func (g WorkflowBatchGenerator) Info() GeneratorInfo {
	i := g.Input.Info()
	i.Version += "/resident-batch-v1"
	i.Capabilities = []string{WorkflowBatchKind, "concurrent-roots", "no-fault-exploration"}
	return i
}
func (g WorkflowBatchGenerator) Next(ctx context.Context, r GenerateRequest) (Scenario, error) {
	if g.Count < 1 || g.Count > 16 || g.Concurrency < 1 || g.Concurrency > g.Count {
		return Scenario{}, errors.New("invalid workflow batch bounds")
	}
	b := WorkflowBatch{Concurrency: g.Concurrency}
	for i := 0; i < g.Count; i++ {
		member := r
		member.WorkloadSeed = streamSeed("workflow-member/v1", r.WorkloadSeed, uint64(i))
		s, err := g.Input.Next(ctx, member)
		if err != nil {
			return Scenario{}, err
		}
		if s.Kind != WorkflowInputKind {
			return Scenario{}, errors.New("batch requires generated workflow input")
		}
		var input GeneratedWorkflow
		if err = strictJSON(s.Workload, &input); err != nil {
			return Scenario{}, err
		}
		b.Inputs = append(b.Inputs, input)
	}
	t := g.Topology
	t.RunID = fmt.Sprintf("xenon-generated-%016x-%016x", r.WorkloadSeed, r.Index)
	if err := t.validate(); err != nil {
		return Scenario{}, err
	}
	raw, _ := json.Marshal(b)
	topology, _ := json.Marshal(t)
	faults, _ := json.Marshal(map[string]any{"seed": r.FaultSeed, "mode": "none; fault exploration not implemented"})
	return Scenario{Version: 1, Kind: WorkflowBatchKind, Workload: raw, Topology: topology, Faults: faults}, nil
}

// BatchWorkflowDriver composes the existing per-workflow lifecycle under one
// scenario. Each runtime instance owns its own audit state and processes.
type BatchWorkflowDriver struct {
	NewRuntime  func() (WorkflowRuntime, error)
	Directory   string
	mu          sync.Mutex
	children    []*ResidentWorkflowDriver
	scenarios   []Scenario
	concurrency int
	running     bool
	cancel      context.CancelFunc
	done        chan struct{}
	report      func(error)
	first       error
	generation  uint64
}

func splitWorkflowBatch(s Scenario, l WorkloadLimits) ([]Scenario, int, error) {
	if s.Kind == WorkflowRuntimeKind {
		validator := ResidentWorkflowDriver{Runtime: batchValidationRuntime{}, Directory: "validation"}
		if err := validator.Validate(s, l); err != nil {
			return nil, 0, err
		}
		return []Scenario{s}, 1, nil
	}
	if s.Version != 1 || s.Kind != WorkflowBatchKind {
		return nil, 0, errors.New("invalid workflow batch kind")
	}
	var b WorkflowBatch
	if err := strictJSON(s.Workload, &b); err != nil {
		return nil, 0, err
	}
	if len(b.Inputs) < 1 || len(b.Inputs) > 16 || b.Concurrency < 1 || b.Concurrency > len(b.Inputs) {
		return nil, 0, errors.New("invalid workflow batch bounds")
	}
	var t ResidentTopology
	if err := strictJSON(s.Topology, &t); err != nil {
		return nil, 0, err
	}
	if err := t.validate(); err != nil {
		return nil, 0, err
	}
	members := make([]Scenario, 0, len(b.Inputs))
	operations := 0
	for i, input := range b.Inputs {
		operations += input.Operations
		if input.Operations < 1 || operations > l.MaxOperations {
			return nil, 0, errors.New("batch exceeds action bound")
		}
		member := t
		member.RunID = fmt.Sprintf("%s-%02d", t.RunID, i)
		raw, _ := json.Marshal(input)
		topology, _ := json.Marshal(member)
		child := Scenario{Version: 1, Kind: WorkflowRuntimeKind, Workload: raw, Topology: topology, Faults: s.Faults}
		validator := ResidentWorkflowDriver{Runtime: batchValidationRuntime{}, Directory: "validation"}
		if err := validator.Validate(child, l); err != nil {
			return nil, 0, err
		}
		members = append(members, child)
	}
	return members, b.Concurrency, nil
}

// Validation delegates to the existing driver without constructing runtime tools.
type batchValidationRuntime struct{ WorkflowRuntime }

func (d *BatchWorkflowDriver) Validate(s Scenario, l WorkloadLimits) error {
	if d.NewRuntime == nil || d.Directory == "" {
		return errors.New("batch runtime and evidence required")
	}
	_, _, err := splitWorkflowBatch(s, l)
	return err
}
func (d *BatchWorkflowDriver) SetFailureReporter(report func(error)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.report = report
	d.generation++
	d.first = nil
}
func (d *BatchWorkflowDriver) failureReporter() func(error) {
	d.mu.Lock()
	generation := d.generation
	d.mu.Unlock()
	return func(err error) { d.fail(generation, err) }
}
func (d *BatchWorkflowDriver) fail(generation uint64, err error) {
	if err == nil {
		return
	}
	d.mu.Lock()
	if generation != d.generation || d.first != nil {
		d.mu.Unlock()
		return
	}
	d.first = err
	report, cancel := d.report, d.cancel
	d.mu.Unlock()
	if report != nil {
		report(err)
	}
	if cancel != nil {
		cancel()
	}
}
func (d *BatchWorkflowDriver) Run(ctx context.Context, s Scenario, emit func(json.RawMessage) error) error {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return errors.New("batch not cleaned")
	}
	d.running = true
	d.first = nil
	d.done = make(chan struct{})
	done := d.done
	ctx, d.cancel = context.WithCancel(ctx)
	d.children = nil
	d.scenarios = nil
	d.mu.Unlock()
	defer close(done)
	// Runner validated with caller limits; decode only to recover exact members.
	members, concurrency, err := splitWorkflowBatch(s, WorkloadLimits{MaxOperations: 2048, MaxDepth: 64, MaxPayloadBytes: 1 << 20, Features: []string{WorkflowInputKind}})
	if err != nil {
		return err
	}
	var topology ResidentTopology
	if err := strictJSON(s.Topology, &topology); err != nil {
		return err
	}
	report := d.failureReporter()
	for i, member := range members {
		runtime, e := d.NewRuntime()
		if e != nil {
			return e
		}
		child := &ResidentWorkflowDriver{Runtime: runtime, Directory: filepath.Join(d.Directory, topology.RunID, fmt.Sprintf("member-%02d", i))}
		if err := validateRuntimeInput(runtime, member); err != nil {
			return err
		}
		child.SetFailureReporter(report)
		d.children = append(d.children, child)
		d.scenarios = append(d.scenarios, member)
	}
	d.concurrency = concurrency
	jobs := make(chan int)
	var workers sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				// Admission and the first-failure latch share a lock. Work admitted
				// before failure may drain, but no later member is admitted.
				d.mu.Lock()
				admitted := d.first == nil && ctx.Err() == nil
				d.mu.Unlock()
				if !admitted {
					continue
				}
				e := guarded(func() error { return d.children[i].Run(ctx, d.scenarios[i], emit) })
				if e != nil {
					report(e)
				}
			}
		}()
	}
launch:
	for i := range members {
		select {
		case <-ctx.Done():
			break launch
		case jobs <- i:
		}
	}
	close(jobs)
	workers.Wait()
	d.mu.Lock()
	first := d.first
	d.mu.Unlock()
	if first != nil {
		return first
	}
	return ctx.Err()
}
func (d *BatchWorkflowDriver) Settle(ctx context.Context) error {
	report := d.failureReporter()
	for _, child := range d.children {
		// Shared Runner has installed the current settle-phase reporter.
		child.SetFailureReporter(report)
		if err := child.Settle(ctx); err != nil {
			report(err)
			return err
		}
	}
	return nil
}
func (d *BatchWorkflowDriver) Cleanup(ctx context.Context) error {
	d.mu.Lock()
	done, cancel := d.done, d.cancel
	d.mu.Unlock()
	if done == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
	case <-ctx.Done():
		return errors.Join(ErrPending, ctx.Err())
	}
	var result error
	for _, child := range d.children {
		result = errors.Join(result, child.Cleanup(ctx))
		if ctx.Err() != nil {
			break
		}
	}
	if result == nil {
		d.mu.Lock()
		d.running = false
		d.mu.Unlock()
	}
	return result
}
