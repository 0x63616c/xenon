package main

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/0x63616c/xenon/internal/integration"
)

func TestIntegrationCommand(t *testing.T) {
	called := false
	cmd := integrationCommand(func(context.Context, io.Writer) (integration.JourneyResult, error) {
		called = true
		return integration.JourneyResult{Name: "slatedb-minio", Assertions: []string{"durable"}}, nil
	})
	out := new(strings.Builder)
	cmd.SetOut(out)
	cmd.SetErr(io.Discard)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !called || !strings.Contains(out.String(), `"name":"slatedb-minio"`) {
		t.Fatal("journey result missing", out.String())
	}

	called = false
	cmd = integrationCommand(func(context.Context, io.Writer) (integration.JourneyResult, error) {
		called = true
		return integration.JourneyResult{}, nil
	})
	cmd.SetArgs([]string{"--only", "unknown"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	if err := cmd.Execute(); err == nil || called {
		t.Fatal("unsupported journey reached runner", err)
	}
}
