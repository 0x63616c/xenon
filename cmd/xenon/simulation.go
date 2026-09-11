package main

import (
	"context"
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

// Search selects a finite coupled corpus or explicitly bound generated real workflows.
func simulationCommands(build func() buildinfo.Info, clock simulation.Clock) []*cobra.Command {
	// Work commands let the runner record cancellation as evidence instead of
	// stopping in the generic lightweight-command pre-run check.
	recordCancellation := func(*cobra.Command, []string) error { return nil }
	makeSearch := func(use, short string, single bool) *cobra.Command {
		var paths []string
		var evidence, mode, bundle string
		var development, continuous, interleave bool
		var workflows, concurrency int
		var resident residentFlags
		var maxCases, workloadSeed, faultSeed uint64
		var duration time.Duration
		cmd := &cobra.Command{Use: use, Short: short, Args: cobra.NoArgs, PersistentPreRunE: recordCancellation}
		cmd.Flags().StringArrayVar(&paths, "scenario", nil, "Saved coupled production-Step JSON; repeat for a finite corpus")
		cmd.Flags().StringVar(&evidence, "evidence", "", "New evidence directory (must not exist)")
		cmd.Flags().BoolVar(&development, "development", false, "Allow unknown/modified build provenance; label output development")
		cmd.Flags().Uint64Var(&maxCases, "max-cases", 0, "Maximum cases (0 means all supplied scenarios)")
		cmd.Flags().DurationVar(&duration, "duration", time.Minute, "Total generation/run/settle time budget")
		cmd.Flags().Uint64Var(&workloadSeed, "workload-seed", 0, "Workflow generator seed; unused by fixed simulation workload")
		cmd.Flags().Uint64Var(&faultSeed, "fault-seed", 0, "Simulation interleaving seed; real mode currently injects no faults")
		if !single {
			cmd.Flags().StringVar(&mode, "mode", "simulation", "Execution mode: simulation or real")
			cmd.Flags().StringVar(&bundle, "bundle", "", "Prepared fresh workflow generator bundle (real mode)")
			cmd.Flags().BoolVar(&continuous, "continuous", false, "Generate cases until failure or duration budget")
			cmd.Flags().BoolVar(&interleave, "interleave", false, "Generate seeded actor-delivery orders from one simulation scenario; fixed external linearization")
			cmd.Flags().IntVar(&workflows, "workflows-per-case", 1, "Root workflows per real case (1 to 16)")
			cmd.Flags().IntVar(&concurrency, "workflow-concurrency", 1, "Maximum concurrent roots per real case")
			resident.add(cmd)
		}
		_ = cmd.MarkFlagRequired("evidence")
		_ = cmd.MarkFlagFilename("scenario", "json")
		cmd.RunE = func(cmd *cobra.Command, _ []string) error {
			if !single && mode != "simulation" && mode != "real" {
				return errors.New("--mode must be simulation or real")
			}
			if mode == "real" {
				if len(paths) != 0 || interleave {
					return errors.New("real search generates fresh inputs; --scenario is only supported in simulation mode")
				}
				runner, err := simulationRunner(evidence, development, build(), clock)
				if err != nil {
					return err
				}
				cfg := simulation.SearchConfig{MaxCases: maxCases, Continuous: continuous, MaxDuration: duration, WorkloadSeed: workloadSeed, FaultSeed: faultSeed}
				return runResidentSearch(cmd, runner, resident, bundle, cfg, workflows, concurrency, development)
			}
			if !single && ((continuous && !interleave) || bundle != "" || workflows != 1 || concurrency != 1 || resident.build != "" || resident.fixture != "" || resident.oracle != "" || resident.oracleSHA != "" || resident.logs != "") {
				return errors.New("resident workflow flags require --mode real")
			}
			if len(paths) == 0 || (single && len(paths) != 1) {
				return errors.New("provide exactly one --scenario for test simulation; search accepts a finite corpus")
			}
			if interleave && (len(paths) != 1 || (!continuous && maxCases == 0) || (continuous && maxCases != 0)) {
				return errors.New("--interleave requires one --scenario and either positive --max-cases or --continuous")
			}
			runner, err := simulationRunner(evidence, development, build(), clock)
			if err != nil {
				return err
			}
			inputs := make([][]byte, 0, len(paths))
			for _, path := range paths {
				raw, err := readScenario(path)
				if err != nil {
					return err
				}
				inputs = append(inputs, raw)
			}
			corpus, err := simulation.NewCoupledCorpus(inputs...)
			if err != nil {
				return err
			}
			var gen simulation.Generator = corpus
			label := "finite-coupled-corpus"
			count := uint64(len(inputs))
			if maxCases > 0 && maxCases < count {
				count = maxCases
			}
			if interleave {
				gen, err = simulation.NewCoupledInterleavings(inputs[0])
				if err != nil {
					return err
				}
				count = maxCases
				label = "seeded-coupled-delivery-interleavings"
			}
			cfg := simulation.SearchConfig{MaxCases: count, Continuous: continuous, MaxDuration: duration, MaxInFlight: 1, SettleBudget: time.Second, CleanupBudget: time.Second, MaxTraceBytes: 8 << 20, WorkloadSeed: workloadSeed, FaultSeed: faultSeed, Limits: simulation.WorkloadLimits{MaxOperations: 256, MaxDepth: 1, MaxPayloadBytes: 1 << 20, Features: []string{simulation.CoupledKind}}}
			result, err := simulation.Search(cmd.Context(), cfg, gen, runner)
			return simulationResult(cmd, development, label, result, err)
		}
		return cmd
	}
	test := &cobra.Command{Use: "test", Short: "Run an implemented verification profile", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() }}
	test.AddCommand(dstCommand(build, clock))
	test.AddCommand(makeSearch("simulation", "Verify one saved coupled production-Step scenario", true))
	search := makeSearch("search", "Search saved simulation scenarios or fresh real workflows", false)
	var artifact, evidence string
	var development bool
	replay := &cobra.Command{Use: "replay FILE", Short: "Replay exact expanded simulation artifact bytes", Args: cobra.MaximumNArgs(1), PersistentPreRunE: recordCancellation}
	var allowLegacy bool
	var resident residentFlags
	resident.add(replay)
	replay.Flags().BoolVar(&allowLegacy, "allow-legacy-artifact", false, "Allow schema 1 replay without envelope integrity; never exact acceptance evidence")
	replay.Flags().StringVar(&artifact, "artifact", "", "Saved case scenario.json artifact")
	replay.Flags().StringVar(&evidence, "evidence", "", "New replay evidence directory (must not exist)")
	replay.Flags().BoolVar(&development, "development", false, "Allow unknown/modified build provenance; label output development")
	_ = replay.MarkFlagFilename("artifact", "json")
	replay.RunE = func(cmd *cobra.Command, args []string) error {
		if len(args) == 1 {
			if artifact != "" {
				return errors.New("provide FILE or --artifact, not both")
			}
			artifact = args[0]
		}
		if artifact == "" {
			return errors.New("artifact FILE required")
		}
		if evidence == "" {
			var err error
			evidence, err = unusedTempPath("xenon-replay-")
			if err != nil {
				return err
			}
		}
		runner, err := simulationRunner(evidence, development, build(), clock)
		if err != nil {
			return err
		}
		runner.AllowLegacyArtifact = allowLegacy
		mode := "exact-component-artifact-replay"
		if resident.build != "" {
			if _, err = resident.bind(runner); err != nil {
				return err
			}
			mode = "saved-input-resident-workflow-replay; real scheduling is not deterministic"
		}
		if allowLegacy {
			mode = "legacy-unverified-artifact-replay"
		}
		result, err := simulation.Replay(cmd.Context(), artifact, runner)
		return simulationResult(cmd, development, mode, result, err)
	}
	return []*cobra.Command{test, search, replay, minimizeCommand(build, clock)}
}
func unusedTempPath(prefix string) (string, error) {
	path, err := os.MkdirTemp("", prefix)
	if err != nil {
		return "", err
	}
	if err = os.Remove(path); err != nil {
		return "", err
	}
	return path, nil
}

func dstCommand(build func() buildinfo.Info, clock simulation.Clock) *cobra.Command {
	const defaultCases = 100
	var seed, cases uint64
	cmd := &cobra.Command{
		Use:   "dst",
		Short: "Run fast deterministic ownership simulations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if cases == 0 {
				return errors.New("--cases must be positive")
			}
			parent, err := os.MkdirTemp("", "xenon-dst-")
			if err != nil {
				return err
			}
			evidence := filepath.Join(parent, "evidence")
			keep := false
			defer func() {
				if !keep {
					_ = os.RemoveAll(parent)
				}
			}()
			info := build()
			provenance := simulation.Provenance{Source: info.Revision, Versions: map[string]string{
				"toolchain": info.Go, "native": "modeled; no native engine executed", "images": "none", "build_modified": info.Modified,
			}}
			if provenance.Source == "" {
				provenance.Source = "development"
			}
			if provenance.Versions["toolchain"] == "" {
				provenance.Versions["toolchain"] = "unknown"
			}
			result, runErr := simulation.RunDST(cmd.Context(), seed, cases, evidence, provenance, clock)
			if runErr != nil {
				keep = true
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "DST failure retained at %s\n", evidence)
			}
			return simulationResult(cmd, false, "go-dst", result, runErr)
		},
	}
	cmd.Flags().Uint64Var(&seed, "seed", 1, "Deterministic schedule seed")
	cmd.Flags().Uint64Var(&cases, "cases", defaultCases, "Number of schedules to explore")
	return cmd
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
