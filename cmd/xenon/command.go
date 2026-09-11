package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/0x63616c/xenon/internal/app"
	"github.com/0x63616c/xenon/internal/buildinfo"
	"github.com/0x63616c/xenon/internal/integration"
	"github.com/0x63616c/xenon/internal/simulation"
	"github.com/0x63616c/xenon/internal/temporal"
	"github.com/spf13/cobra"
)

const startupBanner = `
 __  __  _____  _   _   ___   _   _
 \ \/ / | ____|| \ | | / _ \ | \ | |
  \  /  |  _|  |  \| || | | ||  \| |
  /  \  | |___ | |\  || |_| || |\  |
 /_/\_\ |_____||_| \_| \___/ |_| \_|

`

// execute owns CLI diagnostics and the existing 0/1 exit convention. The runtime
// owns bounded shutdown; a successful foreground shutdown remains exit 0.
func execute(ctx context.Context, args []string, in io.Reader, out, diagnostics io.Writer, start func(context.Context, app.Config) error) int {
	return executeWithDependencies(ctx, args, in, out, diagnostics, commandDependencies{
		start: start, inspect: app.Inspect, dev: app.RunDev, profile: runRealProfile, getenv: os.Getenv,
	})
}

func executeWithDependencies(ctx context.Context, args []string, in io.Reader, out, diagnostics io.Writer, dependencies commandDependencies) int {
	command := newCommandWithDependencies(in, out, diagnostics, dependencies)
	command.SetArgs(args)
	if err := command.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(diagnostics, err)
		var exit *commandExit
		if errors.As(err, &exit) {
			return exit.code
		}
		return 1
	}
	return 0
}

func newCommand(in io.Reader, out, diagnostics io.Writer, start func(context.Context, app.Config) error) *cobra.Command {
	return newCommandWithDependencies(in, out, diagnostics, commandDependencies{
		start: start, inspect: app.Inspect, dev: app.RunDev, profile: runRealProfile,
		getenv: os.Getenv,
	})
}

type commandEffect uint8

const (
	effectBackendMutation commandEffect = iota
	effectNativeOpen
	effectChildLaunch
	effectNetworkCall
)

type commandDependencies struct {
	start   func(context.Context, app.Config) error
	inspect func(context.Context, app.Config) (app.Inspection, error)
	dev     func(context.Context, string, string, string, bool) (app.DevResult, error)
	profile profileExecutor
	getenv  func(string) string
	observe func(...commandEffect)
}

func newCommandWithDependencies(in io.Reader, out, diagnostics io.Writer, dependencies commandDependencies) *cobra.Command {
	rejectConfigEnvironment := func() error {
		for _, variable := range []string{"XENON_CONFIG", "XENON_CONFIG_FILE"} {
			if dependencies.getenv != nil && dependencies.getenv(variable) != "" {
				return fmt.Errorf("unsupported configuration override %s; use --config FILE", variable)
			}
		}
		return nil
	}
	observe := func(effects ...commandEffect) {
		if dependencies.observe != nil {
			dependencies.observe(effects...)
		}
	}
	root := &cobra.Command{
		Use: "xenon", Short: "Run Temporal with S3-backed Xenon persistence",
		SilenceErrors: true, SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error { return cmd.Context().Err() },
	}
	root.AddCommand(&cobra.Command{
		Use: "version", Short: "Print build metadata as JSON", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return json.NewEncoder(cmd.OutOrStdout()).Encode(buildinfo.Read())
		},
	})
	for _, name := range []string{"check-config", "start"} {
		var path, output string
		command := &cobra.Command{Use: name, Args: cobra.NoArgs}
		if name == "check-config" {
			command.Short = "Validate explicit JSON configuration without starting services"
			command.Flags().StringVar(&output, "output", "text", "Output format: text or json")
		} else {
			command.Short = "Run one foreground Xenon server"
		}
		command.Flags().StringVar(&path, "config", "", "Xenon JSON configuration file (required)")
		if name == "start" {
			_ = command.MarkFlagRequired("config")
		}
		_ = command.MarkFlagFilename("config", "json")
		command.RunE = func(cmd *cobra.Command, _ []string) (result error) {
			if name == "check-config" {
				if output != "text" && output != "json" {
					return fmt.Errorf("--output must be text or json")
				}
				if output == "json" {
					defer func() {
						status := "valid"
						message := ""
						if result != nil {
							status = "invalid"
							message = result.Error()
						}
						if err := json.NewEncoder(cmd.OutOrStdout()).Encode(configCheckResult{1, status, message}); err != nil {
							result = errors.Join(result, err)
						}
					}()
				}
			}
			if err := rejectConfigEnvironment(); err != nil {
				return err
			}
			if path == "" {
				return fmt.Errorf("--config FILE required")
			}
			c, err := app.Load(path)
			if err != nil {
				return err
			}
			if err = app.ValidateServiceLayout(c); err != nil {
				return err
			}
			tc, err := c.TemporalConfig()
			if err != nil {
				return err
			}
			if _, err = temporal.Configuration(tc); err != nil {
				return err
			}
			if name == "check-config" {
				if output == "text" {
					_, err = fmt.Fprintln(cmd.OutOrStdout(), "XENON_CONFIG_VALID")
				}
				return err
			}
			// Preserve the existing secret-free startup metadata before network work.
			if err = json.NewEncoder(cmd.OutOrStdout()).Encode(buildinfo.Read()); err != nil {
				return err
			}
			if _, err = io.WriteString(cmd.ErrOrStderr(), startupBanner); err != nil {
				return err
			}
			observe(effectBackendMutation, effectNativeOpen, effectNetworkCall)
			return dependencies.start(cmd.Context(), c)
		}
		root.AddCommand(command)
	}
	root.AddCommand(inspectCommandWithPreflight(func(ctx context.Context, config app.Config) (app.Inspection, error) {
		observe(effectNetworkCall)
		return dependencies.inspect(ctx, config)
	}, rejectConfigEnvironment))
	root.AddCommand(devCommand(func(ctx context.Context, action, fixture, state string, ephemeral bool) (app.DevResult, error) {
		observe(effectBackendMutation, effectChildLaunch, effectNetworkCall)
		return dependencies.dev(ctx, action, fixture, state, ephemeral)
	}))
	root.AddCommand(generateCommand())
	simulation := simulationCommands(buildinfo.Read, simulation.WallClock{})
	for _, command := range simulation {
		if command.Name() == "test" {
			command.AddCommand(integrationCommand(func(ctx context.Context, diagnostics io.Writer) (integration.JourneyResult, error) {
				observe(effectBackendMutation, effectNativeOpen, effectChildLaunch, effectNetworkCall)
				return integration.RunSlateDBMinIO(ctx, diagnostics)
			}))
			command.AddCommand(realProfileCommands(func(ctx context.Context, request profileRequest, diagnostics io.Writer) (profileResult, error) {
				observe(effectBackendMutation, effectNativeOpen, effectChildLaunch, effectNetworkCall)
				return dependencies.profile(ctx, request, diagnostics)
			})...)
			command.AddCommand(workflowCommand())
		}
	}
	root.AddCommand(simulation...)
	root.SetIn(in)
	root.SetOut(out)
	root.SetErr(diagnostics)
	// Cobra captures completion output when constructing these subcommands.
	root.InitDefaultCompletionCmd()
	for _, command := range root.Commands() {
		if command.Name() == "completion" {
			command.Args = cobra.NoArgs
			command.RunE = func(cmd *cobra.Command, _ []string) error { return cmd.Help() }
		}
	}
	return root
}

type configCheckResult struct {
	Schema int    `json:"schema"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}
