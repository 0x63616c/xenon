package partitions

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/0x63616c/xenon/internal/registry"
)

// Driver tests use the same encoded control/CAS protocol, with channel-controlled
// native completion. Native SlateDB contract tests qualify the real engine separately.
type driverStore struct {
	mu     sync.Mutex
	record registry.Record
	lost   bool
}

func (s *driverStore) Read(context.Context, registry.Key) (registry.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.record.Clone(), nil
}
func (s *driverStore) Create(context.Context, registry.Key, registry.Write) (registry.Record, error) {
	panic("unexpected create")
}
func (s *driverStore) Replace(_ context.Context, k registry.Key, v registry.Version, w registry.Write) (registry.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v != s.record.Version {
		return registry.Record{}, &registry.Conflict{Key: k}
	}
	b, e := registry.Encode(k, v, w)
	if e != nil {
		return registry.Record{}, e
	}
	s.record = registry.Record{Body: b, Version: registry.Version(w.Transition)}
	if s.lost {
		s.lost = false
		return registry.Record{}, &registry.UnknownOutcome{Key: k, Transition: w.Transition, Cause: context.Canceled}
	}
	return s.record.Clone(), nil
}

type driverIDs struct{ n int }

func (s *driverIDs) NewID(p string) (string, error) {
	s.n++
	return fmt.Sprintf("%s_%022d", p, s.n+100), nil
}

type heldEngine struct {
	release chan struct{}
	writer  *heldWriter
	opens   int
}

func (e *heldEngine) Open(context.Context, OpenRequest) (Writer, error) {
	e.opens++
	<-e.release
	return e.writer, nil
}

type heldWriter struct {
	Writer
	release chan struct{}
	closes  int
}

func (w *heldWriter) Close(context.Context) error { w.closes++; <-w.release; return nil }
func TestServiceOwnsLateNativeCompletionThroughCanceledDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writer := &heldWriter{release: make(chan struct{})}
		engine := &heldEngine{release: make(chan struct{}), writer: writer}
		service, e := NewService(context.Background(), testConfig(), &driverStore{record: fixture(t), lost: true}, engine, &driverIDs{})
		if e != nil {
			t.Fatal(e)
		}
		service.Poll()
		synctest.Wait()
		if engine.opens != 1 || service.Snapshot().Phase != Opening {
			t.Fatal("unknown reservation did not reconcile")
		}
		for range 3 {
			service.Poll()
			synctest.Wait()
		}
		if engine.opens != 1 {
			t.Fatal("duplicate native open")
		}
		service.Stop()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if e := service.Drain(ctx); e != context.Canceled {
			t.Fatal(e)
		}
		if len(service.Snapshot().Pending()) != 1 {
			t.Fatal("canceled drain released pending open")
		}
		close(engine.release)
		synctest.Wait()
		if writer.closes != 1 || service.Snapshot().Phase != Closing {
			t.Fatal("late open was not closed")
		}
		if _, _, ready := service.Writer(); ready {
			t.Fatal("late writer admitted")
		}
		close(writer.release)
		synctest.Wait()
		if e := service.Drain(context.Background()); e != nil {
			t.Fatal(e)
		}
		if service.Snapshot().Phase != Stopped || engine.opens != 1 || writer.closes != 1 {
			t.Fatal("lifecycle incomplete")
		}
	})
}
func TestServiceReadyAndFenceRetiresExactHandle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writer := &heldWriter{release: make(chan struct{})}
		engine := &heldEngine{release: make(chan struct{}), writer: writer}
		close(engine.release)
		close(writer.release)
		service, e := NewService(context.Background(), testConfig(), &driverStore{record: fixture(t)}, engine, &driverIDs{})
		if e != nil {
			t.Fatal(e)
		}
		service.Poll()
		synctest.Wait()
		w, a, ready := service.Writer()
		if !ready || w != writer || a.Generation != 1 {
			t.Fatal("ready writer unavailable")
		}
		service.ObserveFence(service.Snapshot().Handle() + 1)
		if _, _, ok := service.Writer(); !ok {
			t.Fatal("wrong handle fence revoked writer")
		}
		service.Stop()
		synctest.Wait()
		if e := service.Drain(context.Background()); e != nil {
			t.Fatal(e)
		}
		if writer.closes != 1 {
			t.Fatal("writer not closed once")
		}
	})
}
