package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/0x63616c/xenon/internal/buildinfo"
	"github.com/0x63616c/xenon/internal/simulation"
	"github.com/spf13/cobra"
)

type commandExit struct {
	code int
	err  error
}

func (e *commandExit) Error() string { return e.err.Error() }
func (e *commandExit) Unwrap() error { return e.err }

type simulationOutput struct {
	Schema        int                  `json:"schema"`
	Mode          string               `json:"mode"`
	Qualification string               `json:"qualification"`
	Result        simulation.RunResult `json:"result"`
}

// These commands bind only the implemented finite coupled corpus. Real-stack
// drivers and generated workflow exploration are deliberately not advertised.
func simulationCommands(build func() buildinfo.Info, clock simulation.Clock) []*cobra.Command {
	// Work commands let the runner record cancellation as evidence instead of
	// stopping in the generic lightweight-command pre-run check.
	recordCancellation := func(*cobra.Command, []string) error { return nil }
	makeSearch := func(use, short string, single bool) *cobra.Command {
		var paths []string
		var evidence string
		var development bool
		var maxCases, workloadSeed, faultSeed uint64
		var duration time.Duration
		cmd := &cobra.Command{Use: use, Short: short, Args: cobra.NoArgs, PersistentPreRunE: recordCancellation}
		cmd.Flags().StringArrayVar(&paths, "scenario", nil, "Saved coupled production-Step JSON; repeat for a finite corpus")
		cmd.Flags().StringVar(&evidence, "evidence", "", "New evidence directory (must not exist)")
		cmd.Flags().BoolVar(&development, "development", false, "Allow unknown/modified build provenance; label output development")
		cmd.Flags().Uint64Var(&maxCases, "max-cases", 0, "Maximum cases (0 means all supplied scenarios)")
		cmd.Flags().DurationVar(&duration, "duration", time.Minute, "Total generation/run/settle time budget")
		cmd.Flags().Uint64Var(&workloadSeed, "workload-seed", 0, "Recorded workload stream seed (unused by fixed corpus)")
		cmd.Flags().Uint64Var(&faultSeed, "fault-seed", 0, "Recorded fault stream seed (unused by fixed corpus)")
		_ = cmd.MarkFlagRequired("scenario")
		_ = cmd.MarkFlagRequired("evidence")
		_ = cmd.MarkFlagFilename("scenario", "json")
		cmd.RunE = func(cmd *cobra.Command, _ []string) error {
			if len(paths) == 0 || (single && len(paths) != 1) {
				return errors.New("provide exactly one --scenario for test simulation; search accepts a finite corpus")
			}
			runner, err := simulationRunner(evidence, development, build(), clock)
			if err != nil {
				return err
			}
			inputs := make([][]byte, 0, len(paths))
			for index, path := range paths {
				raw, err := readScenario(path)
				if err != nil {
					return err
				}
				sum := sha256.Sum256(raw)
				runner.Provenance.Versions[fmt.Sprintf("scenario_input_%d_sha256", index)] = hex.EncodeToString(sum[:])
				inputs = append(inputs, raw)
			}
			gen, err := simulation.NewCoupledCorpus(inputs...)
			if err != nil {
				return err
			}
			count := uint64(len(inputs))
			if maxCases > 0 && maxCases < count {
				count = maxCases
			}
			cfg := simulation.SearchConfig{MaxCases: count, MaxDuration: duration, MaxInFlight: 1, SettleBudget: time.Second, CleanupBudget: time.Second, MaxTraceBytes: 8 << 20, WorkloadSeed: workloadSeed, FaultSeed: faultSeed, Limits: simulation.WorkloadLimits{MaxOperations: 256, MaxDepth: 1, MaxPayloadBytes: 1 << 20, Features: []string{simulation.CoupledKind}}}
			result, err := simulation.Search(cmd.Context(), cfg, gen, runner)
			return simulationResult(cmd, development, "finite-coupled-corpus", result, err)
		}
		return cmd
	}
	test := &cobra.Command{Use: "test", Short: "Run an implemented verification profile", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() }}
	test.AddCommand(makeSearch("simulation", "Verify one saved coupled production-Step scenario", true))
	search := makeSearch("search", "Explore a finite corpus of saved coupled production-Step scenarios", false)
	var artifact, evidence string
	var development bool
	replay := &cobra.Command{Use: "replay", Short: "Replay exact expanded simulation artifact bytes", Args: cobra.NoArgs, PersistentPreRunE: recordCancellation}
	replay.Flags().StringVar(&artifact, "artifact", "", "Saved case scenario.json artifact")
	replay.Flags().StringVar(&evidence, "evidence", "", "New replay evidence directory (must not exist)")
	replay.Flags().BoolVar(&development, "development", false, "Allow unknown/modified build provenance; label output development")
	_ = replay.MarkFlagRequired("artifact")
	_ = replay.MarkFlagRequired("evidence")
	_ = replay.MarkFlagFilename("artifact", "json")
	replay.RunE = func(cmd *cobra.Command, _ []string) error {
		runner, err := simulationRunner(evidence, development, build(), clock)
		if err != nil {
			return err
		}
		result, err := simulation.Replay(cmd.Context(), artifact, runner)
		return simulationResult(cmd, development, "exact-component-artifact-replay", result, err)
	}
	return []*cobra.Command{test, search, replay}
}
func simulationRunner(directory string, development bool, info buildinfo.Info, clock simulation.Clock) (*simulation.Runner, error) {
	if directory == "" {
		return nil, errors.New("--evidence DIRECTORY required")
	}
	source, err := hex.DecodeString(info.Revision)
	if !development && (err != nil || len(source) != 20 || info.Modified != "false" || info.Go == "unknown") {
		return nil, errors.New("clean known source build required; --development records weaker provenance")
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	return &simulation.Runner{Driver: &simulation.ArtifactDriver{}, Clock: clock, Directory: absolute, Provenance: simulation.Provenance{Source: info.Revision, Versions: map[string]string{"toolchain": info.Go, "native": "modeled; no native engine executed", "images": "none", "build_modified": info.Modified}}}, nil
}
func readScenario(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 1<<20 {
		return nil, fmt.Errorf("scenario %q exceeds 1 MiB", path)
	}
	return raw, nil
}
func simulationResult(cmd *cobra.Command, development bool, mode string, result simulation.RunResult, err error) error {
	qualification := "component"
	if development {
		qualification = "development"
	}
	outputErr := json.NewEncoder(cmd.OutOrStdout()).Encode(simulationOutput{1, mode, qualification, result})
	if outputErr != nil {
		return errors.Join(err, outputErr)
	}
	switch result.StopReason {
	case "canceled":
		if err == nil {
			err = context.Canceled
		}
		return &commandExit{130, err}
	case "budget":
		if err == nil {
			err = errors.New("exploration budget ended; only the completed prefix was checked")
		}
		return &commandExit{2, err}
	}
	return err
}
