package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/app"
)

func TestDevCLIValidatesBeforeEffects(t *testing.T) {
	for _, args := range [][]string{{"up"}, {"down"}, {"up", "--fixture", "f", "--state", "s", "--timeout", "0s"}, {"up", "--fixture", "f", "--state", "s", "--ephemeral"}} {
		command := devCommand(func(context.Context, string, string, string, bool) (app.DevResult, error) {
			t.Fatal("invalid command caused effects")
			return app.DevResult{}, nil
		})
		command.SilenceErrors = true
		command.SilenceUsage = true
		command.SetArgs(args)
		command.SetOut(new(bytes.Buffer))
		if err := command.ExecuteContext(context.Background()); err == nil {
			t.Fatal(args)
		}
	}
}
func TestDevCLIPreservesDeadlineAndEphemeralIntent(t *testing.T) {
	var out bytes.Buffer
	command := devCommand(func(ctx context.Context, action, fixture, state string, ephemeral bool) (app.DevResult, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > time.Second || action != "down" || fixture != "" || state != "s" || !ephemeral {
			t.Fatal("lost request contract")
		}
		return app.DevResult{Schema: 1, Status: "stopped", CleanupVerified: true}, nil
	})
	command.SetOut(&out)
	command.SetArgs([]string{"down", "--state", "s", "--ephemeral", "--timeout", "1s"})
	if err := command.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	var result app.DevResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil || !result.CleanupVerified {
		t.Fatal(result, err)
	}
}
