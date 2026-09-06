//go:build darwin || linux

package simulation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"regexp"
	"syscall"
	"time"
)

func hashWorkflowFile(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil {
		return "", e
	}
	if !st.Mode().IsRegular() || st.Size() > 1<<30 {
		return "", errors.New("runtime tool must be a regular file below1GiB")
	}
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return "", e
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Omes and its prepared worker share this process group. Unlike the full-stack
// Python helper, no independently grouped Compose or agent children are owned.
func runOmesProcess(ctx context.Context, program string, args []string, dir, logPath string, observers ...func(error)) error {
	f, e := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	if e = ctx.Err(); e != nil {
		return e
	}
	cmd := exec.Command(program, args...)
	cmd.Dir = dir
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if e = cmd.Start(); e != nil {
		return e
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var primary, exit error
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	terminal := regexp.MustCompile(`iteration [0-9]+ (?:encountered error|failed):`)
	inspect := func() error {
		raw, e := readWorkflowFile(logPath, 8<<20)
		if e != nil {
			return e
		}
		if terminal.Match(raw) {
			return errors.New("terminal Omes iteration failure")
		}
		return nil
	}
	finished := false
	for primary == nil && !finished {
		select {
		case exit = <-done:
			finished = true
		case <-ctx.Done():
			primary = inspect()
			if primary == nil {
				primary = ctx.Err()
			}
		case <-ticker.C:
			primary = inspect()
			if st, e := f.Stat(); e != nil {
				primary = e
			} else if st.Size() > 8<<20 {
				primary = errors.New("Omes log exceeded 8MiB")
			}
		}
	}
	if primary == nil {
		primary = inspect()
	}
	if primary != nil {
		for _, observe := range observers {
			if observe != nil {
				observe(primary)
			}
		}
		primary = errors.Join(primary, save(logPath+".failure.json", map[string]string{"first_failure": primary.Error()}))
		if !finished {
			// SIGINT is the pinned Omes cooperative cancellation signal.
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
			timer := time.NewTimer(7 * time.Second)
			select {
			case exit = <-done:
			case <-timer.C:
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				select {
				case exit = <-done:
				case <-time.After(5 * time.Second):
					return errors.Join(primary, ErrPending)
				}
			}
			timer.Stop()
		}
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	if st, e := f.Stat(); e != nil {
		primary = errors.Join(primary, e)
	} else if st.Size() > 8<<20 {
		primary = errors.Join(primary, errors.New("Omes log exceeded 8MiB"))
	}
	return errors.Join(primary, exit, f.Sync())
}
