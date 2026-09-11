package main

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/0x63616c/xenon/internal/integration"
)

func TestIntegrationCommandRunsRegistryInOrder(t *testing.T) {
	var called []string
	cmd := integrationCommand(func(_ context.Context, name string, _ io.Writer) (integration.JourneyResult, error) {
		called = append(called, name)
		return integration.JourneyResult{Name: name}, nil
	})
	out := new(strings.Builder)
	cmd.SetOut(out)
	cmd.SetErr(io.Discard)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(called, integrationJourneys) || !strings.Contains(out.String(), `"name":"temporal-compatibility"`) {
		t.Fatal(called, out.String())
	}
}

func TestIntegrationCommandOnlyAndFailFast(t *testing.T) {
	var called []string
	cmd := integrationCommand(func(_ context.Context, name string, _ io.Writer) (integration.JourneyResult, error) {
		called = append(called, name)
		if name == "multi-node-ownership" {
			return integration.JourneyResult{}, errors.New("failed")
		}
		return integration.JourneyResult{Name: name}, nil
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	if err := cmd.Execute(); err == nil || !reflect.DeepEqual(called, integrationJourneys[:2]) {
		t.Fatal("registry did not fail fast", err, called)
	}
	called = nil
	cmd = integrationCommand(func(_ context.Context, name string, _ io.Writer) (integration.JourneyResult, error) {
		called = append(called, name)
		return integration.JourneyResult{Name: name}, nil
	})
	cmd.SetArgs([]string{"--only", "temporal-compatibility"})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	if err := cmd.Execute(); err != nil || !reflect.DeepEqual(called, []string{"temporal-compatibility"}) {
		t.Fatal(err, called)
	}
	called = nil
	cmd.SetArgs([]string{"--only", "unknown"})
	if err := cmd.Execute(); err == nil || len(called) != 0 {
		t.Fatal("unknown journey reached runner", err, called)
	}
}
