package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/0x63616c/xenon/internal/integration"
	"github.com/spf13/cobra"
)

type integrationRunner func(context.Context, io.Writer) (integration.JourneyResult, error)

func integrationCommand(run integrationRunner) *cobra.Command {
	var only string
	cmd := &cobra.Command{
		Use:   "integration",
		Short: "Run a bounded real-system journey",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if only != "slatedb-minio" {
				return fmt.Errorf("--only must be slatedb-minio")
			}
			result, err := run(cmd.Context(), cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			return json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
				Schema int                       `json:"schema"`
				Result integration.JourneyResult `json:"result"`
			}{1, result})
		},
	}
	cmd.Flags().StringVar(&only, "only", "slatedb-minio", "Integration journey to run")
	return cmd
}
