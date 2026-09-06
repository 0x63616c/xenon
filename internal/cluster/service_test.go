package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/0x63616c/xenon/internal/registry"
)

type controllerIDs struct{ n int }

func (i *controllerIDs) NewID(prefix string) (string, error) {
	i.n++
	return fmt.Sprintf("%s_%022d", prefix, i.n+20), nil
}

// This service fixture tests dispatch/completion ownership, not registry backend
// durability (covered by the shared contract suite and real CAS election tests).
type controllerStore struct {
	mu           sync.Mutex
	record       registry.Record
	replacements int
	reads        int
	block        chan struct{}
	started      chan struct{}
	unknown      bool
	readErr      error
}

func (f *controllerStore) Read(context.Context, registry.Key) (registry.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	return f.record.Clone(), f.readErr
}
func (f *controllerStore) Create(context.Context, registry.Key, registry.Write) (registry.Record, error) {
	panic("unexpected bootstrap")
}
func (f *controllerStore) Replace(ctx context.Context, key registry.Key, expected registry.Version, w registry.Write) (registry.Record, error) {
	f.mu.Lock()
	f.replacements++
	block, started, unknown := f.block, f.started, f.unknown
	f.mu.Unlock()
	if started != nil {
		close(started)
	}
	if block != nil {
		<-block
	} // deliberately retain dispatched work past cancellation
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.record.Version != expected {
		return registry.Record{}, &registry.Conflict{Key: key}
	}
	raw, err := registry.Encode(key, expected, w)
	if err != nil {
		return registry.Record{}, err
	}
	f.record = registry.Record{Body: raw, Version: registry.Version(fmt.Sprintf("v%d", f.replacements+1))}
	if unknown {
		return registry.Record{}, &registry.UnknownOutcome{Key: key, Transition: w.Transition, Cause: ctx.Err()}
	}
	return f.record.Clone(), nil
}
func TestServiceSinglePendingCASAndDrainRetainsLateOutcome(t *testing.T) {
	r := controlRecord(t, fixtureControl(), "v1", 1)
	synctest.Test(t, func(t *testing.T) {
		store := &controllerStore{record: r, block: make(chan struct{}), started: make(chan struct{}), unknown: true}
		service, err := NewService(context.Background(), controllerConfig(leader), store, &controllerIDs{})
		if err != nil {
			t.Fatal(err)
		}
		service.Poll(0, nil)
		<-store.started
		for at := Tick(1); at < 10; at++ {
			service.Poll(at, nil)
		}
		snap := service.Snapshot()
		if len(snap.Pending()) != 1 || snap.Pending()[0].Kind != PublishControl {
			t.Fatal("lost pending CAS")
		}
		service.Stop()
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if err := service.Drain(canceled); !errors.Is(err, context.Canceled) {
			t.Fatal("drain erased pending work", err)
		}
		if service.Snapshot().Stopped() {
			t.Fatal("stopped before dispatched operation returned")
		}
		close(store.block)
		if err := service.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		snap = service.Snapshot()
		if snap.LastUnknown == nil || snap.LastUnknown.Effect.Expected != "v1" || !snap.Stopped() {
			t.Fatal("late committed/unknown outcome lost")
		}
		store.mu.Lock()
		calls, reads := store.replacements, store.reads
		store.mu.Unlock()
		if calls != 1 || reads != 1 {
			t.Fatal("overlapping or autonomous calls", calls, reads)
		}
	})
}
func TestServiceExplicitPollDrivesFailureRecovery(t *testing.T) {
	r := controlRecord(t, fixtureControl(), "v1", 1)
	synctest.Test(t, func(t *testing.T) {
		store := &controllerStore{record: r, readErr: &registry.Unavailable{Key: controlKey}}
		service, err := NewService(context.Background(), controllerConfig(leader), store, &controllerIDs{})
		if err != nil {
			t.Fatal(err)
		}
		service.Poll(0, nil)
		synctest.Wait()
		if service.Snapshot().LastError == nil || len(service.Snapshot().Pending()) != 0 {
			t.Fatal("read failure not retained")
		}
		store.mu.Lock()
		store.readErr = nil
		store.mu.Unlock()
		service.Poll(1, nil)
		synctest.Wait()
		if service.Snapshot().LastError != nil || len(service.Snapshot().Pending()) != 0 {
			t.Fatal("renewal did not complete", service.Snapshot().LastError)
		}
		store.mu.Lock()
		calls := store.replacements
		store.mu.Unlock()
		if calls != 1 {
			t.Fatal("expected one renewal", calls)
		}
		service.Stop()
		if err := service.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}
