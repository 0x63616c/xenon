package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResidentWorkflowHelpDoesNotStartBackend(t *testing.T) {
	var out, errout bytes.Buffer
	c := workflowCommand()
	c.SetOut(&out)
	c.SetErr(&errout)
	c.SetArgs([]string{"--help"})
	if e := c.ExecuteContext(context.Background()); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), "resident-fixture") || errout.Len() != 0 {
		t.Fatalf("%s %s", &out, &errout)
	}
}

func TestSearchModeAndRealLimitsRejectBeforeEffects(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--mode", "unknown"}, "--mode must"},
		{[]string{"--mode", "simulation", "--continuous"}, "require --mode real"},
		{[]string{"--mode", "real", "--scenario", "missing"}, "only supported in simulation"},
		{[]string{"--mode", "real", "--bundle", "missing"}, "either positive --max-cases"},
		{[]string{"--mode", "real", "--bundle", "missing", "--max-cases", "1", "--continuous"}, "either positive --max-cases"},
		{[]string{"--mode", "real", "--bundle", "missing", "--max-cases", "1", "--workflow-concurrency", "2"}, "workflow-concurrency must"},
		{[]string{"--mode", "real", "--bundle", "missing", "--max-cases", "1"}, "explicit resident"},
		{[]string{"--mode", "real", "--bundle", "missing", "--continuous"}, "explicit resident"},
	} {
		evidence := filepath.Join(t.TempDir(), "new")
		args := append([]string{"search", "--development", "--evidence", evidence}, tc.args...)
		code, _, diagnostics := runSimulationCLI(t, context.Background(), args...)
		if code != 1 || !strings.Contains(diagnostics, tc.want) {
			t.Fatalf("%v: code %d diagnostics %s", tc.args, code, diagnostics)
		}
		if _, err := os.Stat(evidence); !os.IsNotExist(err) {
			t.Fatalf("validation created evidence: %v", err)
		}
	}
}
func TestResidentWorkflowRequiresExplicitRuntime(t *testing.T) {
	var out bytes.Buffer
	c := workflowCommand()
	c.SetOut(&out)
	c.SetErr(&out)
	c.SetArgs([]string{"--bundle", "missing", "--evidence", t.TempDir() + "/new", "--development"})
	if e := c.ExecuteContext(context.Background()); e == nil || !strings.Contains(e.Error(), "explicit resident") {
		t.Fatalf("%v", e)
	}
}

func TestResidentWorkflowRejectsConcurrencyBeforeToolLoading(t *testing.T) {
	for _, flags := range [][]string{
		{"--workflows-per-case", "0"},
		{"--workflows-per-case", "17"},
		{"--workflow-concurrency", "0"},
		{"--workflows-per-case", "4", "--workflow-concurrency", "5"},
	} {
		c := workflowCommand()
		var out bytes.Buffer
		c.SetOut(&out)
		c.SetErr(&out)
		c.SetArgs(append([]string{"--bundle", "missing", "--evidence", t.TempDir() + "/new", "--development"}, flags...))
		if err := c.ExecuteContext(context.Background()); err == nil || !strings.Contains(err.Error(), "workflows-per-case") {
			t.Fatalf("invalid concurrency reached tool loading: %v", err)
		}
	}
}
