//go:build darwin || linux

package simulation

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type intentRuntimeControl struct {
	residentControl
	strict bool
}

func (r *intentRuntimeControl) ValidateInput(raw []byte) error {
	return (&OmesRuntime{RequireExpectedGraph: r.strict}).ValidateInput(raw)
}

func TestResidentExpectedGraphRejectsBeforeDispatch(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "entire batch"}[batch], func(t *testing.T) {
			var runtimes []*intentRuntimeControl
			newRuntime := func() (WorkflowRuntime, error) {
				// The first batch member permits broad input; the second rejects it. Even
				// the first must not execute before the complete batch is preflighted.
				r := &intentRuntimeControl{strict: !batch || len(runtimes) > 0}
				runtimes = append(runtimes, r)
				return r, nil
			}
			cfg := residentTestConfig()
			var gen Generator = ResidentGenerator{residentInputControl{}, residentTestTopology()}
			var driver Driver
			if batch {
				gen = WorkflowBatchGenerator{Input: residentInputControl{}, Topology: residentTestTopology(), Count: 4, Concurrency: 4}
				driver = &BatchWorkflowDriver{NewRuntime: newRuntime, Directory: filepath.Join(t.TempDir(), "runtime")}
			} else {
				r, _ := newRuntime()
				driver = &ResidentWorkflowDriver{Runtime: r, Directory: filepath.Join(t.TempDir(), "runtime")}
			}
			runner := testRunner(t, driver)
			result, err := Search(context.Background(), cfg, gen, runner)
			if err == nil || result.Completed != 0 || len(runtimes) == 0 {
				t.Fatal(result, err)
			}
			for _, r := range runtimes {
				if r.checks != 0 || r.empties != 0 || len(r.inputs) != 0 {
					t.Fatal("effects before intent validation", r)
				}
			}
		})
	}
}
func TestOmesExpectedGraphDirectExecutePreflight(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "unsupported.proto")
	// TestInput.workflow_input with an empty WorkflowInput: implicit completion
	// is outside this strict semantic contract.
	if e := os.WriteFile(input, []byte{0x0a, 0}, 0600); e != nil {
		t.Fatal(e)
	}
	runtime := &OmesRuntime{RequireExpectedGraph: true}
	e := runtime.Execute(context.Background(), residentTestTopology(), input, dir)
	if e == nil || !strings.Contains(e.Error(), "explicit terminal") {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(dir, "invocation.json")); !os.IsNotExist(e) {
		t.Fatal("dispatch evidence created", e)
	}
	if runtime.Provenance()["expected_graph_contract"] == (&OmesRuntime{}).Provenance()["expected_graph_contract"] {
		t.Fatal("capabilities indistinguishable")
	}
	// Existing exploratory mode stays explicitly weaker and accepts unsupported
	// grammar at preflight; this is not acceptance proof.
	if e = (&OmesRuntime{}).ValidateInput([]byte("unsupported")); e != nil {
		t.Fatal(e)
	}
}
