//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// superviseProfile owns only the helper process group. The helper owns its
// separate workload groups and Compose project; forced termination cannot claim
// those resources were cleaned. Raw helper stdout never contaminates CLI JSON.
func superviseProfile(ctx context.Context, program string, args []string, directory, logPath string, diagnostics io.Writer, grace time.Duration) (error, bool) {
	log, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err, false
	}
	defer log.Close()
	command := exec.Command(program, args...)
	command.Dir = directory
	command.Stdout = log
	command.Stderr = log
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err = ctx.Err(); err != nil {
		return err, false
	}
	if err = command.Start(); err != nil {
		return err, false
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	var primary error
	var exitErr error
	forced := false
	finished := false
	for !finished && primary == nil {
		select {
		case exitErr = <-done:
			finished = true
		case <-ctx.Done():
			primary = ctx.Err()
		case <-tick.C:
			info, statErr := log.Stat()
			if statErr != nil {
				primary = statErr
			} else if info.Size() > 8<<20 {
				primary = errors.New("helper output exceeds 8 MiB")
			}
		}
	}
	if primary != nil {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGTERM)
		timer := time.NewTimer(grace)
		select {
		case exitErr = <-done:
		case <-timer.C:
			forced = true
			_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
			select {
			case exitErr = <-done: // Wait reaps even when the process exits before kill.
			case <-time.After(5 * time.Second):
				return errors.Join(primary, errors.New("helper did not reap after forced termination; cleanup unverified")), true
			}
		}
		timer.Stop()
	}
	// A normal helper must not leave a member of its own group alive either.
	_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	if syncErr := log.Sync(); syncErr != nil {
		primary = errors.Join(primary, syncErr)
	}
	if info, statErr := log.Stat(); statErr != nil {
		primary = errors.Join(primary, statErr)
	} else if info.Size() > 8<<20 {
		primary = errors.Join(primary, errors.New("helper output exceeds 8 MiB"))
	}
	if primary != nil {
		return errors.Join(primary, exitErr), forced
	}
	if exitErr != nil {
		fmt.Fprintln(diagnostics, "Real profile failed; raw helper output:", logPath)
		return exitErr, false
	}
	return nil, false
}
