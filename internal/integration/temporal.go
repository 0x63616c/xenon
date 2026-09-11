package integration

import (
	"bufio"
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
	cmd   *exec.Cmd
	log   *os.File
	lines chan string
	once  sync.Once
}

func startOwned(ctx context.Context, dir, logName string, env []string, argv ...string) (*ownedProcess, error) {
	log, err := os.Create(logName)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir, cmd.Env, cmd.SysProcAttr = dir, env, &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Close()
		return nil, err
	}
	cmd.Stderr = cmd.Stdout
	p := &ownedProcess{cmd: cmd, log: log, lines: make(chan string, 128)}
	if err = cmd.Start(); err != nil {
		log.Close()
		return nil, err
	}
	go func() {
		s := bufio.NewScanner(stdout)
		for s.Scan() {
			line := s.Text()
			_, _ = fmt.Fprintln(log, line)
			select {
			case p.lines <- line:
			default:
			}
		}
		close(p.lines)
	}()
	return p, nil
}

func (p *ownedProcess) waitLine(ctx context.Context, prefix string) (string, error) {
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
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
	var result error
	p.once.Do(func() {
		signal := syscall.SIGTERM
		if kill {
			signal = syscall.SIGKILL
		}
		_ = syscall.Kill(-p.cmd.Process.Pid, signal)
		done := make(chan error, 1)
		go func() { done <- p.cmd.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				var exit *exec.ExitError
				if !errors.As(err, &exit) {
					result = err
				}
			}
		case <-time.After(10 * time.Second):
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
			result = <-done
		}
		result = errors.Join(result, p.log.Close())
	})
	return result
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
	raw, err := cmd.CombinedOutput()
	if err != nil {
		return string(raw), fmt.Errorf("%s: %w: %s", argv[0], err, strings.TrimSpace(string(raw)))
	}
	return string(raw), nil
}
func probe(ctx context.Context, root string, env []string, bin string, fixture temporalCase, mode string, extra ...string) ([]byte, error) {
	timeout := 15 * time.Second
	switch mode {
	case "bootstrap", "fuzz-endpoint", "fuzz-endpoint-ready", "control", "verify", "visibility":
		timeout = 90 * time.Second
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
