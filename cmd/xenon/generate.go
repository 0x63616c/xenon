package main

import (
	"github.com/0x63616c/xenon/internal/buildinfo"
	"github.com/0x63616c/xenon/internal/simulation"
	"github.com/spf13/cobra"
	"time"
)

func generateCommand() *cobra.Command {
	group := &cobra.Command{Use: "generate", Short: "Prepare new inputs without executing workflows", Args: cobra.NoArgs, RunE: func(c *cobra.Command, _ []string) error { return c.Help() }}
	var bundle, evidence string
	var development bool
	var cases, workloadSeed, faultSeed uint64
	command := &cobra.Command{Use: "workflow", Short: "Generate fresh seeded Omes inputs; no runtime execution", Args: cobra.NoArgs, PersistentPreRunE: func(*cobra.Command, []string) error { return nil }}
	command.Flags().StringVar(&bundle, "bundle", "", "Prepared pinned workflow-generator bundle")
	command.Flags().StringVar(&evidence, "evidence", "", "New evidence directory")
	command.Flags().BoolVar(&development, "development", false, "Allow unknown/dirty CLI build provenance")
	command.Flags().Uint64Var(&cases, "max-cases", 1, "Maximum fresh inputs to prepare")
	command.Flags().Uint64Var(&workloadSeed, "workload-seed", 0, "Workload seed; shared runner derives independent per-case identities")
	command.Flags().Uint64Var(&faultSeed, "fault-seed", 0, "Recorded independent fault seed; no fault exploration implemented")
	_ = command.MarkFlagRequired("bundle")
	_ = command.MarkFlagRequired("evidence")
	command.RunE = func(c *cobra.Command, _ []string) error {
		generator, err := simulation.NewWorkflowGenerator(bundle)
		if err != nil {
			return err
		}
		runner, err := simulationRunner(evidence, development, buildinfo.Read(), simulation.WallClock{})
		if err != nil {
			return err
		}
		runner.Driver = &simulation.WorkflowPreparationDriver{}
		config := simulation.SearchConfig{MaxCases: cases, MaxDuration: 5 * time.Minute, MaxInFlight: 1, SettleBudget: time.Second, CleanupBudget: time.Second, MaxTraceBytes: 8 << 20, WorkloadSeed: workloadSeed, FaultSeed: faultSeed, Limits: simulation.WorkloadLimits{MaxOperations: 2048, MaxDepth: 64, MaxPayloadBytes: 1 << 20, Features: []string{simulation.WorkflowInputKind}}}
		result, err := simulation.Search(c.Context(), config, generator, runner)
		return simulationResult(c, development, "workflow-input-preparation-only", result, err)
	}
	group.AddCommand(command)
	return group
}
