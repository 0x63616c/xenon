// Package agent assembles the runtimes without depending on either engine.
package agent

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// Component starts its background work before returning. Ready must verify that
// work can be served, not merely that a listener was opened. Stop is idempotent.
type Component interface {
	Start(context.Context) error
	Ready(context.Context) error
	Stop(context.Context) error
}

// ErrProcessExitRequired means an effect may still use runtime resources.
// The caller must terminate the process without further component teardown.
var ErrProcessExitRequired = errors.New("process exit required")

// PermanentError marks a completed component failure that cannot recover in
// this process incarnation, such as durable membership replacement.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }
func Permanent(err error) error         { return &PermanentError{Err: err} }

type Runtime struct {
	Storage, Temporal               Component
	StartupTimeout, ShutdownTimeout time.Duration
	ready                           atomic.Bool
}

func (r *Runtime) Ready() bool { return r.ready.Load() }

// Run keeps storage alive while Temporal stops. The caller must exit the entire
// process after Run returns: a timeout cannot cancel an in-flight native call.
func (r *Runtime) Run(ctx context.Context) (result error) {
	if r.Storage == nil || r.Temporal == nil || r.StartupTimeout <= 0 || r.ShutdownTimeout <= 0 {
		return errors.New("invalid agent lifecycle configuration")
	}
	defer r.ready.Store(false)
	// Startup cancellation must not cancel a successfully started storage runtime.
	lifetime, cancelLifetime := context.WithCancel(context.Background())
	unsafeTeardown := false
	defer func() {
		if !unsafeTeardown {
			cancelLifetime()
		}
	}()
	call := func(ctx context.Context, fn func() error) error {
		err, completed := bounded(ctx, fn)
		if !completed {
			unsafeTeardown = true
			return errors.Join(err, ErrProcessExitRequired)
		}
		return err
	}
	start, cancelStart := context.WithTimeout(ctx, r.StartupTimeout)
	defer cancelStart()
	storageStarted, temporalStarted := false, false
	defer func() {
		r.ready.Store(false)
		if unsafeTeardown {
			return
		}
		stop, cancel := context.WithTimeout(context.Background(), r.ShutdownTimeout)
		defer cancel()
		if temporalStarted {
			if err := call(stop, func() error { return r.Temporal.Stop(stop) }); err != nil {
				// Do not shut storage beneath a Temporal stop that still uses it.
				unsafeTeardown = true
				result = errors.Join(result, fmt.Errorf("Temporal shutdown: %w", err), ErrProcessExitRequired)
				return
			}
		}
		if storageStarted {
			result = errors.Join(result, call(stop, func() error { return r.Storage.Stop(stop) }))
		}
	}()
	storageStarted = true // Start may fail after allocating resources.
	if err := call(start, func() error { return r.Storage.Start(lifetime) }); err != nil {
		return fmt.Errorf("storage start: %w", err)
	}
	if err := call(start, func() error { return r.Storage.Ready(start) }); err != nil {
		return fmt.Errorf("storage readiness: %w", err)
	}
	temporalStarted = true
	if err := call(start, func() error { return r.Temporal.Start(lifetime) }); err != nil {
		return fmt.Errorf("Temporal start: %w", err)
	}
	if err := call(start, func() error { return r.Temporal.Ready(start) }); err != nil {
		return fmt.Errorf("Temporal readiness: %w", err)
	}
	r.ready.Store(true)
	// Recheck both components; a live listener alone must not keep a broken agent ready.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			probe, cancel := context.WithTimeout(ctx, 5*time.Second)
			err, completed := r.healthProbe(ctx, probe, func() error { return r.Storage.Ready(probe) })
			if !completed {
				unsafeTeardown = true
				err = errors.Join(err, ErrProcessExitRequired)
			}
			if err == nil {
				err, completed = r.healthProbe(ctx, probe, func() error { return r.Temporal.Ready(probe) })
				if !completed {
					unsafeTeardown = true
					err = errors.Join(err, ErrProcessExitRequired)
				}
			}
			cancel()
			if err != nil {
				r.ready.Store(false)
				// Ownership movement can make routed persistence briefly unavailable.
				// A completed negative probe changes readiness, but does not make the
				// process unsafe. Shutdown with an outstanding probe still requires
				// process exit; teardown cannot race retained work.
				if errors.Is(err, ErrProcessExitRequired) {
					return fmt.Errorf("agent health: %w", err)
				}
				var permanent *PermanentError
				if errors.As(err, &permanent) {
					return fmt.Errorf("agent health: %w", err)
				}
				continue
			}
			r.ready.Store(true)
		}
	}
}

// completed=false means fn may still be executing; cancellation does not join it.
func bounded(ctx context.Context, fn func() error) (err error, completed bool) {
	if err := ctx.Err(); err != nil {
		return err, true
	}
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err, true
	case <-ctx.Done():
		// Prefer observed completion if both signals became ready together.
		select {
		case err := <-done:
			return err, true
		default:
		}
		return ctx.Err(), false
	}
}

// healthProbe separates a negative observation deadline from process shutdown.
// Expiry revokes readiness immediately but retains the one pending call until
// completion. Cancellation alone cannot prove a native/resource user finished.
// No replacement probe or teardown runs while this call remains outstanding.
func (r *Runtime) healthProbe(lifetime, probe context.Context, fn func() error) (error, bool) {
	if err := probe.Err(); err != nil {
		return err, true
	}
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		if probe.Err() != nil {
			r.ready.Store(false)
			return errors.Join(probe.Err(), err), true
		}
		return err, true
	case <-probe.Done():
		r.ready.Store(false)
	}
	select {
	case err := <-done:
		return errors.Join(probe.Err(), err), true
	case <-lifetime.Done():
		select {
		case err := <-done:
			return errors.Join(probe.Err(), err), true
		default:
			return lifetime.Err(), false
		}
	}
}
