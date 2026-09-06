package simulation

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type residentControl struct {
	inputs                            [][]byte
	checks, empties, audits, cleanups int
	emptyErr, auditErr                error
}

func (r *residentControl) Check(context.Context, ResidentTopology) error { r.checks++; return nil }
func (r *residentControl) Empty(context.Context, ResidentTopology) error {
	r.empties++
	return r.emptyErr
}
func (r *residentControl) Execute(_ context.Context, _ ResidentTopology, input, path string) error {
	raw, e := os.ReadFile(input)
	if e != nil {
		return e
	}
	if _, e = os.Stat(filepath.Join(path, "binding.json")); e != nil {
		return e
	}
	r.inputs = append(r.inputs, raw)
	return nil
}
func (r *residentControl) Audit(context.Context, ResidentTopology, string) (json.RawMessage, error) {
	r.audits++
	return json.RawMessage(`{"event":"test-history-observed"}`), r.auditErr
}
func (r *residentControl) Cleanup(context.Context, ResidentTopology, string) error {
	r.cleanups++
	return nil
}

type residentInputControl struct{}

func (residentInputControl) Info() GeneratorInfo {
	return GeneratorInfo{"test-only", strings.Repeat("a", 64), []string{WorkflowInputKind}}
}
func (residentInputControl) Next(_ context.Context, r GenerateRequest) (Scenario, error) {
	raw := []byte("test control, not upstream protobuf")
	input, _ := json.Marshal(GeneratedWorkflow{Input: raw, SHA256: hash(raw), Operations: 1, Depth: 1, WorkloadSeed: r.WorkloadSeed})
	return Scenario{Version: 1, Kind: WorkflowInputKind, Workload: input, Faults: []byte(`{"seed":1,"mode":"none; fault exploration not implemented"}`)}, nil
}
func residentTestConfig() SearchConfig {
	return SearchConfig{MaxCases: 1, MaxDuration: time.Second * 3, MaxInFlight: 1, SettleBudget: time.Second, CleanupBudget: time.Second, MaxTraceBytes: 1 << 20, Limits: WorkloadLimits{2048, 64, 1 << 20, []string{WorkflowInputKind}}}
}
func residentTestTopology() ResidentTopology {
	return ResidentTopology{1, "127.0.0.1:1", "test", "case", "xenon-fuzz", "omes-xenon-ministack-fuzz", strings.Repeat("b", 64)}
}
func residentTestRunner(t *testing.T, r WorkflowRuntime) *Runner {
	t.Helper()
	root := t.TempDir()
	return &Runner{Driver: &ResidentWorkflowDriver{Runtime: r, Directory: filepath.Join(root, "runtime")}, Directory: filepath.Join(root, "shared"), Clock: WallClock{}, Provenance: Provenance{"test-only", map[string]string{"toolchain": "test", "native": "none", "images": "none"}}}
}
func TestResidentSharedExactReplayAndIndependentAudit(t *testing.T) {
	original := &residentControl{}
	runner := residentTestRunner(t, original)
	result, e := Search(context.Background(), residentTestConfig(), ResidentGenerator{residentInputControl{}, residentTestTopology()}, runner)
	if e != nil || result.Completed != 1 {
		t.Fatalf("%+v %v", result, e)
	}
	if len(original.inputs) != 1 || original.audits != 1 || original.cleanups != 1 {
		t.Fatalf("missing lifecycle: %+v", original)
	}
	next := &residentControl{}
	replay := residentTestRunner(t, next)
	result, e = Replay(context.Background(), filepath.Join(runner.Directory, "case-00000000000000000000", "scenario.json"), replay)
	if e != nil || result.Completed != 1 || string(next.inputs[0]) != string(original.inputs[0]) {
		t.Fatalf("exact replay %+v %v", result, e)
	}
}
func TestResidentRejectsOldResultsWithoutTouchingThem(t *testing.T) {
	r := &residentControl{emptyErr: errors.New("existing executions")}
	runner := residentTestRunner(t, r)
	out, e := Search(context.Background(), residentTestConfig(), ResidentGenerator{residentInputControl{}, residentTestTopology()}, runner)
	if e == nil || out.Completed != 0 || r.cleanups != 0 || len(r.inputs) != 0 {
		t.Fatalf("unsafe reuse %+v %v %+v", out, e, r)
	}
}
func TestResidentLeafSuccessCannotReplaceHistoryAudit(t *testing.T) {
	r := &residentControl{auditErr: errors.New("history missing")}
	runner := residentTestRunner(t, r)
	out, e := Search(context.Background(), residentTestConfig(), ResidentGenerator{residentInputControl{}, residentTestTopology()}, runner)
	if e == nil || out.Completed != 0 || r.cleanups != 1 {
		t.Fatalf("false pass %+v %v", out, e)
	}
	raw, e := os.ReadFile(filepath.Join(runner.Directory, "case-00000000000000000000", "failure.json"))
	if e != nil || !strings.Contains(string(raw), "history missing") {
		t.Fatalf("primary lost %s %v", raw, e)
	}
}
func TestResidentPreparationDriverCannotExecuteRuntimeArtifact(t *testing.T) {
	s, e := (ResidentGenerator{residentInputControl{}, residentTestTopology()}).Next(context.Background(), GenerateRequest{})
	if e != nil {
		t.Fatal(e)
	}
	if e = (&ArtifactDriver{}).Validate(s, residentTestConfig().Limits); e == nil {
		t.Fatal("silent preparation downgrade")
	}
}

type blockedResident struct {
	residentControl
	entered, release chan struct{}
}

func (r *blockedResident) Execute(ctx context.Context, _ ResidentTopology, _, _ string) error {
	close(r.entered)
	<-ctx.Done()
	<-r.release
	return ctx.Err()
}
func TestResidentUndrainedProcessPreventsRemoteCleanup(t *testing.T) {
	runtime := &blockedResident{entered: make(chan struct{}), release: make(chan struct{})}
	d := &ResidentWorkflowDriver{Runtime: runtime, Directory: filepath.Join(t.TempDir(), "new")}
	s, e := (ResidentGenerator{residentInputControl{}, residentTestTopology()}).Next(context.Background(), GenerateRequest{})
	if e != nil {
		t.Fatal(e)
	}
	finished := make(chan error, 1)
	go func() { finished <- d.Run(context.Background(), s, func(json.RawMessage) error { return nil }) }()
	<-runtime.entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	e = d.Cleanup(ctx)
	if !errors.Is(e, ErrPending) || runtime.cleanups != 0 {
		t.Fatalf("unsafe remote teardown %v %d", e, runtime.cleanups)
	}
	close(runtime.release)
	<-finished
	if e = d.Cleanup(context.Background()); e != nil || runtime.cleanups != 1 {
		t.Fatalf("failed drain %v", e)
	}
}
func TestResidentExistingArtifactDirectoryRejectedBeforeRuntime(t *testing.T) {
	r := &residentControl{}
	d := &ResidentWorkflowDriver{Runtime: r, Directory: t.TempDir()}
	s, e := (ResidentGenerator{residentInputControl{}, residentTestTopology()}).Next(context.Background(), GenerateRequest{})
	if e != nil {
		t.Fatal(e)
	}
	e = d.Run(context.Background(), s, func(json.RawMessage) error { return nil })
	if e == nil || r.checks != 0 {
		t.Fatalf("reused evidence %v", e)
	}
}
