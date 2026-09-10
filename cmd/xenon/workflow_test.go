package main

import (
	"bytes"
	"context"
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
