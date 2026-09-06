package main

import (
	"errors"
	"github.com/0x63616c/xenon/internal/buildinfo"
	"github.com/0x63616c/xenon/internal/simulation"
	"github.com/spf13/cobra"
	"path/filepath"
	"time"
)

type residentFlags struct{ build, fixture, oracle, oracleSHA, logs string }

func (f *residentFlags) add(c *cobra.Command) {
	c.Flags().StringVar(&f.build, "runtime-build", "", "Pinned prepared corrected Omes build.json (trusted executable bundle)")
	c.Flags().StringVar(&f.fixture, "resident-fixture", "", "Explicit externally supervised fixture JSON; exclusive case queues required")
	c.Flags().StringVar(&f.oracle, "history-oracle", "", "Trusted xenon-omes-oracle executable")
	c.Flags().StringVar(&f.oracleSHA, "history-oracle-sha256", "", "Expected history oracle executable SHA256")
	c.Flags().StringVar(&f.logs, "runtime-evidence", "", "New directory for exact protobuf, process logs and independent histories")
}
func (f residentFlags) bind(r *simulation.Runner) (*simulation.OmesRuntime, error) {
	if f.build == "" || f.fixture == "" || f.oracle == "" || f.oracleSHA == "" || f.logs == "" {
		return nil, errors.New("all explicit resident runtime/tool/evidence flags are required")
	}
	runtime, e := simulation.NewOmesRuntime(f.build, f.fixture, f.oracle, f.oracleSHA)
	if e != nil {
		return nil, e
	}
	path, e := filepath.Abs(f.logs)
	if e != nil {
		return nil, e
	}
	if path == r.Directory {
		return nil, errors.New("runtime evidence must be separate from shared evidence")
	}
	r.Driver = &simulation.ResidentWorkflowDriver{Runtime: runtime, Directory: path}
	r.Provenance.Versions["native"] = "external resident fixture; see exact fixture pin"
	r.Provenance.Versions["images"] = "external resident fixture; not independently qualified by CLI"
	for key, value := range runtime.Provenance() {
		r.Provenance.Versions[key] = value
	}
	r.Provenance.Versions["history_oracle_sha256"] = f.oracleSHA
	return runtime, nil
}
func workflowCommand() *cobra.Command {
	var f residentFlags
	var bundle, evidence string
	var development bool
	var cases, seed, fault uint64
	c := &cobra.Command{Use: "workflow", Short: "Execute fresh seeded inputs on an explicit resident fixture; no faults", Args: cobra.NoArgs, PersistentPreRunE: func(*cobra.Command, []string) error { return nil }}
	f.add(c)
	c.Flags().StringVar(&bundle, "bundle", "", "Prepared fresh input generator bundle")
	c.Flags().StringVar(&evidence, "evidence", "", "New shared scenario evidence directory")
	c.Flags().BoolVar(&development, "development", false, "Allow weaker CLI build provenance")
	c.Flags().Uint64Var(&cases, "max-cases", 1, "Bounded sequential inputs (1 to 10)")
	c.Flags().Uint64Var(&seed, "workload-seed", 0, "Fresh input seed")
	c.Flags().Uint64Var(&fault, "fault-seed", 0, "Recorded seed only; no fault exploration")
	_ = c.MarkFlagRequired("bundle")
	_ = c.MarkFlagRequired("evidence")
	c.RunE = func(c *cobra.Command, _ []string) error {
		if cases < 1 || cases > 10 {
			return errors.New("max-cases must be 1 to 10")
		}
		r, e := simulationRunner(evidence, development, buildinfo.Read(), simulation.WallClock{})
		if e != nil {
			return e
		}
		runtime, e := f.bind(r)
		if e != nil {
			return e
		}
		generator, e := simulation.NewWorkflowGenerator(bundle)
		if e != nil {
			return e
		}
		cfg := simulation.SearchConfig{MaxCases: cases, MaxDuration: time.Duration(cases) * 370 * time.Second, MaxInFlight: 1, SettleBudget: 60 * time.Second, CleanupBudget: 30 * time.Second, MaxTraceBytes: 8 << 20, WorkloadSeed: seed, FaultSeed: fault, Limits: simulation.WorkloadLimits{MaxOperations: 2048, MaxDepth: 64, MaxPayloadBytes: 1 << 20, Features: []string{simulation.WorkflowInputKind}}}
		result, e := simulation.Search(c.Context(), cfg, simulation.ResidentGenerator{Input: generator, Topology: runtime.Topology()}, r)
		return simulationResult(c, development, "resident-generated-workflow-component", result, e)
	}
	return c
}
