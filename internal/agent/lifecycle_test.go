package agent

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type component struct{ start, ready, stop func(context.Context) error }

func (c component) Start(ctx context.Context) error {
	if c.start != nil {
		return c.start(ctx)
	}
	return nil
}
func (c component) Ready(ctx context.Context) error {
	if c.ready != nil {
		return c.ready(ctx)
	}
	return nil
}
func (c component) Stop(ctx context.Context) error {
	if c.stop != nil {
		return c.stop(ctx)
	}
	return nil
}

func TestLifecycleOrder(t *testing.T) {
	var mu sync.Mutex
	var events []string
	record := func(s string) { mu.Lock(); defer mu.Unlock(); events = append(events, s) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var lifetime context.Context
	r := &Runtime{StartupTimeout: time.Second, ShutdownTimeout: time.Second}
	r.Storage = component{start: func(c context.Context) error { lifetime = c; record("storage start"); return nil }, ready: func(context.Context) error { record("storage ready"); return nil }, stop: func(context.Context) error { record("storage stop"); return nil }}
	r.Temporal = component{start: func(context.Context) error { record("temporal start"); return nil }, ready: func(context.Context) error { record("temporal ready"); return nil }, stop: func(context.Context) error {
		if lifetime.Err() != nil {
			t.Error("storage lifetime canceled before Temporal stop")
		}
		record("temporal stop")
		return nil
	}}
	go func() {
		for !r.Ready() {
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(events, []string{"storage start", "storage ready", "temporal start", "temporal ready", "temporal stop", "storage stop"}) {
		t.Fatal(events)
	}
	if r.Ready() || lifetime.Err() == nil {
		t.Fatal("lifecycle not finished")
	}
}

func TestUnfinishedEffectRequiresExitWithoutTeardown(t *testing.T) {
	for _, stage := range []string{"storage start", "temporal start", "storage ready", "temporal ready", "temporal stop", "storage stop"} {
		t.Run(stage, func(t *testing.T) {
			release := make(chan struct{})
			finished := make(chan struct{})
			var lifetime context.Context
			var mu sync.Mutex
			stops := []string{}
			block := func(context.Context) error { <-release; close(finished); return nil }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			storage := component{start: func(c context.Context) error { lifetime = c; return nil }, stop: func(context.Context) error {
				mu.Lock()
				defer mu.Unlock()
				stops = append(stops, "storage")
				return nil
			}}
			temporal := component{ready: func(context.Context) error {

				return nil
			}, stop: func(context.Context) error {
				mu.Lock()
				defer mu.Unlock()
				stops = append(stops, "temporal")
				return nil
			}}
			switch stage {
			case "storage start":
				storage.start = func(c context.Context) error { mu.Lock(); lifetime = c; mu.Unlock(); return block(c) }
			case "temporal start":
				temporal.start = block
			case "storage ready":
				storage.ready = block
			case "temporal ready":
				temporal.ready = block
			case "temporal stop":
				temporal.stop = block
			case "storage stop":
				storage.stop = block
			}
			r := Runtime{Storage: storage, Temporal: temporal, StartupTimeout: 30 * time.Millisecond, ShutdownTimeout: 30 * time.Millisecond}
			if stage == "temporal stop" || stage == "storage stop" {
				go func() {
					for !r.Ready() {
						time.Sleep(time.Millisecond)
					}
					cancel()
				}()
			}
			err := r.Run(ctx)
			if !errors.Is(err, ErrProcessExitRequired) {
				t.Errorf("expected exit requirement: %v", err)
			}
			mu.Lock()
			if stage != "storage stop" && len(stops) != 0 {
				t.Error("unsafe concurrent teardown", stops)
			}
			if lifetime != nil && lifetime.Err() != nil {
				t.Error("unsafe lifetime cancellation")
			}
			mu.Unlock()
			close(release)
			<-finished
		})
	}
}

func TestCompletedStartFailureCleansUp(t *testing.T) {
	failure := errors.New("start failed")
	var stopped bool
	r := Runtime{Storage: component{start: func(context.Context) error { return failure }, stop: func(context.Context) error { stopped = true; return nil }}, Temporal: component{}, StartupTimeout: time.Second, ShutdownTimeout: time.Second}
	if err := r.Run(context.Background()); !errors.Is(err, failure) || errors.Is(err, ErrProcessExitRequired) {
		t.Fatal(err)
	}
	if !stopped {
		t.Fatal("partial startup not cleaned")
	}
}

func TestCanceledHealthProbeDoesNotRaceTeardown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	calls := 0
	r := Runtime{StartupTimeout: time.Second, ShutdownTimeout: time.Second, Storage: component{ready: func(context.Context) error {
		calls++
		if calls == 2 {
			close(entered)
			<-release
			close(finished)
		}
		return nil
	}, stop: func(context.Context) error { t.Error("stop raced outstanding health probe"); return nil }}, Temporal: component{stop: func(context.Context) error { t.Error("stop raced outstanding health probe"); return nil }}}
	go func() { <-entered; cancel() }()
	if err := r.Run(ctx); !errors.Is(err, ErrProcessExitRequired) {
		t.Fatal(err)
	}
	close(release)
	<-finished
	if r.Ready() {
		t.Fatal("unhealthy agent remained ready")
	}
}

func TestTransientHealthFailureChangesReadinessAndRecovers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failed, recovered := make(chan struct{}), make(chan struct{})
	calls := 0
	r := Runtime{StartupTimeout: time.Second, ShutdownTimeout: time.Second, Storage: component{ready: func(context.Context) error {
		calls++
		switch calls {
		case 2:
			close(failed)
			return errors.New("ownership moving")
		case 3:
			close(recovered)
		}
		return nil
	}}, Temporal: component{}}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	<-failed
	for r.Ready() {
		time.Sleep(time.Millisecond)
	}
	<-recovered
	for !r.Ready() {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPermanentHealthFailureStopsProcess(t *testing.T) {
	calls := 0
	r := Runtime{StartupTimeout: time.Second, ShutdownTimeout: time.Second, Storage: component{ready: func(context.Context) error {
		calls++
		if calls == 2 {
			return Permanent(errors.New("membership replaced"))
		}
		return nil
	}}, Temporal: component{}}
	if err := r.Run(context.Background()); err == nil || err.Error() != "agent health: membership replaced" {
		t.Fatal(err)
	}
}
