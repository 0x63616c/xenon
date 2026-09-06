package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/0x63616c/xenon/internal/agent"
	"github.com/0x63616c/xenon/internal/app"
	"github.com/0x63616c/xenon/internal/buildinfo"
	"github.com/0x63616c/xenon/internal/simulation"
	"github.com/0x63616c/xenon/internal/temporal"
	"github.com/spf13/cobra"
)

// execute owns CLI diagnostics and the existing 0/1 exit convention. The runtime
// owns bounded shutdown; a successful foreground shutdown remains exit 0.
func execute(ctx context.Context, args []string, in io.Reader, out, diagnostics io.Writer, start func(context.Context, agent.Config) error) int {
	command := newCommand(in, out, diagnostics, start)
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

func newCommand(in io.Reader, out, diagnostics io.Writer, start func(context.Context, agent.Config) error) *cobra.Command {
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
		var path string
		command := &cobra.Command{Use: name, Args: cobra.NoArgs}
		if name == "check-config" {
			command.Short = "Validate explicit JSON configuration without starting services"
		} else {
			command.Short = "Run one foreground Xenon server"
		}
		command.Flags().StringVar(&path, "config", "", "Xenon JSON configuration file (required)")
		_ = command.MarkFlagRequired("config")
		_ = command.MarkFlagFilename("config", "json")
		command.RunE = func(cmd *cobra.Command, _ []string) error {
			if path == "" {
				return fmt.Errorf("--config FILE required")
			}
			c, err := agent.Load(path)
			if err != nil {
				return err
			}
			if err = app.ValidateServiceLayout(c); err != nil {
				return err
			}
			if _, err = temporal.Configuration(c); err != nil {
				return err
			}
			if name == "check-config" {
				_, err = fmt.Fprintln(cmd.OutOrStdout(), "XENON_CONFIG_VALID")
				return err
			}
			// Preserve the existing secret-free startup metadata before network work.
			if err = json.NewEncoder(cmd.OutOrStdout()).Encode(buildinfo.Read()); err != nil {
				return err
			}
			return start(cmd.Context(), c)
		}
		root.AddCommand(command)
	}
	simulation := simulationCommands(buildinfo.Read, simulation.WallClock{})
	for _, command := range simulation {
		if command.Name() == "test" {
			command.AddCommand(realProfileCommands(runRealProfile)...)
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
