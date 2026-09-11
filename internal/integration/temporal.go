package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type temporalCase struct {
	Schema        int      `json:"schema"`
	HistoryShards int      `json:"history_shards"`
	Namespace     string   `json:"namespace"`
	WorkflowID    string   `json:"workflow_id"`
	TaskQueue     string   `json:"task_queue"`
	Partitions    []string `json:"partitions"`
	MaxOutcomes   int      `json:"max_outcomes"`
}

type ownedProcess struct {
	lifecycle *processLifecycle
	log       *os.File
	lines     chan string
	once      sync.Once
	stopErr   error
}

type processLog struct {
	sync.Mutex
	file    *os.File
	lines   chan string
	pending string
	written int
}

func (l *processLog) Write(raw []byte) (int, error) {
	l.Lock()
	defer l.Unlock()
	n := len(raw)
	if remaining := diagnosticLimit - l.written; remaining > 0 {
		part := raw
		if len(part) > remaining {
			part = part[:remaining]
		}
		written, err := l.file.Write(part)
		l.written += written
		if err != nil {
			return 0, err
		}
	}
	l.pending += string(raw)
	for {
		index := strings.IndexByte(l.pending, '\n')
		if index < 0 {
			break
		}
		line := l.pending[:index]
		select {
		case l.lines <- line:
		default:
		}
		l.pending = l.pending[index+1:]
	}
	if len(l.pending) > diagnosticLimit {
		l.pending = l.pending[len(l.pending)-diagnosticLimit:]
	}
	return n, nil
}
func startOwned(ctx context.Context, dir, logName string, env []string, argv ...string) (*ownedProcess, error) {
	log, err := os.Create(logName)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir, cmd.Env, cmd.SysProcAttr = dir, env, &syscall.SysProcAttr{Setpgid: true}
	p := &ownedProcess{log: log, lines: make(chan string, 128)}
	output := &processLog{file: log, lines: p.lines}
	cmd.Stdout, cmd.Stderr, cmd.WaitDelay = output, output, time.Second
	if err = cmd.Start(); err != nil {
		return nil, errors.Join(err, log.Close())
	}
	p.lifecycle = trackProcess(cmd)
	return p, nil
}

func (p *ownedProcess) waitLine(ctx context.Context, prefix string) (string, error) {
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-p.lifecycle.done:
			return "", fmt.Errorf("process exited before %q: %v", prefix, p.lifecycle.waitErr)
		case line, ok := <-p.lines:
			if !ok {
				return "", fmt.Errorf("process exited before %q", prefix)
			}
			if strings.HasPrefix(line, prefix) {
				return line, nil
			}
		}
	}
}

func (p *ownedProcess) stop(kill bool) error {
	p.once.Do(func() { p.stopErr = errors.Join(p.lifecycle.stop(kill), p.log.Close()) })
	return p.stopErr
}

func repositoryRoot(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "--show-toplevel").Output()
	return strings.TrimSpace(string(out)), err
}
func decodeFile(path string, value any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, value)
}
func runOutput(ctx context.Context, dir string, env []string, argv ...string) (string, error) {
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir, cmd.Env = dir, env
	raw, err := outputCommand(cmd)
	if err != nil {
		return string(raw), fmt.Errorf("%s: %w: %s", argv[0], err, strings.TrimSpace(string(raw)))
	}
	return string(raw), nil
}
func probe(ctx context.Context, root string, env []string, bin string, fixture temporalCase, mode string, extra ...string) ([]byte, error) {
	timeout := 15 * time.Second
	switch mode {
	case "bootstrap", "fuzz-endpoint", "fuzz-endpoint-ready", "control", "visibility":
		timeout = 90 * time.Second
	case "verify":
		// After both Temporal processes are killed, the retained SDK worker may
		// need to exhaust transport backoff before it can finish the workflow.
		// The journey's six-minute context remains the outer hard bound.
		timeout = 4 * time.Minute
	}
	attempt, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	args := []string{"--mode", mode, "--address", "127.0.0.1:17233", "--cluster", "integration", "--namespace", fixture.Namespace, "--workflow-id", fixture.WorkflowID, "--task-queue", fixture.TaskQueue}
	args = append(args, extra...)
	out, err := runOutput(attempt, root, env, append([]string{filepath.Join(bin, "xenon-sdk-probe")}, args...)...)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if json.Valid([]byte(lines[i])) {
			return []byte(lines[i]), nil
		}
	}
	return nil, fmt.Errorf("probe %s returned no JSON: %s", mode, strings.TrimSpace(out))
}
func probeUntil(ctx context.Context, root string, env []string, bin string, fixture temporalCase, mode string) error {
	for {
		if _, err := probe(ctx, root, env, bin, fixture, mode); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}
func probePhase(ctx context.Context, root string, env []string, bin string, fixture temporalCase, expected string) error {
	for {
		raw, err := probe(ctx, root, env, bin, fixture, "phase")
		if err == nil {
			var v struct {
				Phase string `json:"phase"`
			}
			if json.Unmarshal(raw, &v) == nil && v.Phase == expected {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}
