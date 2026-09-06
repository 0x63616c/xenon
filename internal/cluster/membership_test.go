package cluster

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
	"github.com/0x63616c/xenon/internal/registry/filesystem"
)

func membershipFixture(t *testing.T) (*Membership, *Membership, *filesystem.Store) {
	t.Helper()
	store, err := filesystem.New(filesystem.Config{Directory: t.TempDir(), MaxRecordBytes: 1 << 20, Wait: func(ctx context.Context) error { return ctx.Err() }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	digest, _ := fixtureLayout().Digest()
	config := MembershipConfig{Prefix: "membership", Cluster: fixtureControl().Cluster, ExpectedLayoutDigest: digest, Self: owner(leader), FailureAfter: 20 * time.Nanosecond, MaxEntries: 4, MaxRecordBytes: 1 << 20, ReadsPerScan: 4}
	a, err := NewMembership(config, store, identity.Generator{})
	if err != nil {
		t.Fatal(err)
	}
	config.Self = owner(contender)
	config.Self.Node = "nod_0000000000000000000002"
	b, err := NewMembership(config, store, identity.Generator{})
	if err != nil {
		t.Fatal(err)
	}
	return a, b, store
}
func beat(t *testing.T, m *Membership, at Tick) {
	t.Helper()
	if err := m.Heartbeat(context.Background(), at); err != nil {
		t.Fatal(err)
	}
}
func discover(t *testing.T, m *Membership, at Tick) MembershipView {
	t.Helper()
	v, err := m.Discover(context.Background(), at, Coordinator{Incarnation: m.config.Self.Incarnation, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func TestMembershipRealRegistryProgressFailureAndRejoin(t *testing.T) {
	a, b, _ := membershipFixture(t)
	beat(t, a, 0)
	beat(t, b, 0)
	if v := discover(t, a, 0); v.Ready || len(v.Members) != 0 {
		t.Fatal("first reads must not admit", v)
	}
	beat(t, a, 1)
	beat(t, b, 1)
	if v := discover(t, a, 1); !v.Ready || len(v.Members) != 2 {
		t.Fatal(v)
	}
	beat(t, a, 22)
	if v := discover(t, a, 22); !v.Ready || len(v.Members) != 1 || v.Members[0] != a.config.Self {
		t.Fatal("stopped sequence", v)
	}
	index, _, err := a.readIndex(context.Background())
	if err != nil || len(index.Entries) != 1 {
		t.Fatal(index, err)
	}
	beat(t, b, 23) // live process recovers mistaken removal through index verification
	index, _, err = a.readIndex(context.Background())
	if err != nil || len(index.Entries) != 2 {
		t.Fatal(index, err)
	}
	if v := discover(t, a, 23); v.Ready {
		t.Fatal("rejoin requires fresh baseline", v)
	}
	beat(t, a, 24)
	beat(t, b, 24)
	if v := discover(t, a, 24); !v.Ready || len(v.Members) != 2 {
		t.Fatal(v)
	}
}
func TestMembershipDuplicateIncarnationsAndTakeoverPartialScan(t *testing.T) {
	a, b, _ := membershipFixture(t)
	b.config.Self.Node = a.config.Self.Node
	beat(t, a, 0)
	beat(t, b, 0)
	discover(t, a, 0)
	beat(t, a, 1)
	beat(t, b, 1)
	if v := discover(t, a, 1); !v.Ready || len(v.Members) != 0 {
		t.Fatal("two live incarnations must both be excluded", v)
	}
	beat(t, a, 22)
	if v := discover(t, a, 22); !v.Ready || len(v.Members) != 1 {
		t.Fatal(v)
	}
	a.config.ReadsPerScan = 1
	beat(t, b, 23)
	v, err := a.Discover(context.Background(), 23, Coordinator{Incarnation: leader, Generation: 2})
	if err != nil || v.Ready {
		t.Fatal("takeover reused prior view", v, err)
	}
	v, err = a.Discover(context.Background(), 24, Coordinator{Incarnation: leader, Generation: 3})
	if err != nil || v.Ready {
		t.Fatal("partial old tenure reused", v, err)
	}
	if a.scan == nil || a.scan.cursor != 1 {
		t.Fatal("scan was not restarted")
	}
}
func TestMembershipIndexFullAndOverflow(t *testing.T) {
	a, b, _ := membershipFixture(t)
	a.config.MaxEntries = 1
	b.config.MaxEntries = 1
	b.config.ReadsPerScan = 1
	a.config.ReadsPerScan = 1
	beat(t, a, 0)
	if err := b.Heartbeat(context.Background(), 0); !errors.Is(err, ErrMembershipFull) {
		t.Fatal(err)
	}
	key := a.heartbeatKey(a.config.Self)
	r, err := a.store.Read(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	h := heartbeat{1, a.config.Cluster, a.config.ExpectedLayoutDigest, a.config.Self, ^uint64(0)}
	if err = a.publish(context.Background(), key, r.Version, h); err != nil {
		t.Fatal(err)
	}
	if err = a.Heartbeat(context.Background(), 1); !errors.Is(err, ErrMembership) {
		t.Fatal("sequence wrapped", err)
	}
}

type membershipFaultStore struct {
	registry.Store
	readError registry.Key
	lose      registry.Key
	commit    bool
	attempts  []membershipAttempt
}

func (f *membershipFaultStore) Read(ctx context.Context, key registry.Key) (registry.Record, error) {
	if key == f.readError {
		return registry.Record{}, &registry.Unavailable{Key: key}
	}
	return f.Store.Read(ctx, key)
}
func (f *membershipFaultStore) Replace(ctx context.Context, key registry.Key, v registry.Version, w registry.Write) (registry.Record, error) {
	return f.write(ctx, key, v, w)
}
func (f *membershipFaultStore) Create(ctx context.Context, key registry.Key, w registry.Write) (registry.Record, error) {
	return f.write(ctx, key, "", w)
}
func (f *membershipFaultStore) write(ctx context.Context, key registry.Key, v registry.Version, w registry.Write) (registry.Record, error) {
	f.attempts = append(f.attempts, membershipAttempt{key, v, w})
	if key == f.lose {
		f.lose = ""
		if f.commit {
			if v == "" {
				_, _ = f.Store.Create(ctx, key, w)
			} else {
				_, _ = f.Store.Replace(ctx, key, v, w)
			}
		}
		return registry.Record{}, &registry.UnknownOutcome{Key: key, Transition: w.Transition}
	}
	if v == "" {
		return f.Store.Create(ctx, key, w)
	}
	return f.Store.Replace(ctx, key, v, w)
}
func TestMembershipReadFailureIsNotAbsenceAndUnknownRetainsExactWrite(t *testing.T) {
	a, b, store := membershipFixture(t)
	beat(t, a, 0)
	beat(t, b, 0)
	discover(t, a, 0)
	beat(t, a, 1)
	beat(t, b, 1)
	discover(t, a, 1)
	fault := &membershipFaultStore{Store: store, readError: a.heartbeatKey(b.config.Self)}
	a.store = fault
	v, err := a.Discover(context.Background(), 22, Coordinator{Incarnation: leader, Generation: 1})
	if err == nil || v.Ready {
		t.Fatal(v, err)
	}
	index, _, err := a.readIndex(context.Background())
	if err != nil || len(index.Entries) != 2 {
		t.Fatal("error pruned peer", index, err)
	}
	fault.readError = ""
	fault.lose = a.heartbeatKey(a.config.Self)
	if err = a.Heartbeat(context.Background(), 23); err == nil {
		t.Fatal("missing unknown")
	}
	old := fault.attempts[len(fault.attempts)-1]
	beat(t, a, 24)
	retry := fault.attempts[len(fault.attempts)-2]
	if !reflect.DeepEqual(old, retry) {
		t.Fatal("retry changed original condition/body/ID", old, retry)
	}
}
func TestMembershipViewCannotPlaceDuringTakeoverOrIncompleteScan(t *testing.T) {
	s := controllerState(t, leader)
	c := fixtureControl()
	r := controlRecord(t, c, "v1", 1)
	for _, view := range []MembershipView{{}, {Coordinator: leader, Generation: 2, Ready: true, Members: []Owner{owner(contender)}}} {
		state, effects := Step(s, Event{Kind: Poll, At: 0, Incarnation: leader, Membership: view})
		state, effects = completeRead(state, effects[0], r, 2)
		p := onlyPublication(t, effects)
		snap, err := DecodeControl(controlKey, effectRecord(t, p, "v2"), controlLimit)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Control().AssignmentRevision != c.AssignmentRevision {
			t.Fatal("unready membership moved partition")
		}
		state, _ = completePublish(state, p, effectRecord(t, p, "v2"), nil)
		state, read := pollController(t, state, 1)
		state, effects = completeRead(state, read, effectRecord(t, p, "v2"), 3)
		if len(effects) != 0 {
			t.Fatal("empty view dispatched placement", effects)
		}
	}
}

func TestMembershipUnknownIndexThenUnrelatedWriteMakesFreshProgress(t *testing.T) {
	a, b, store := membershipFixture(t)
	fault := &membershipFaultStore{Store: store, lose: a.indexKey(), commit: true}
	a.store = fault
	if err := a.Heartbeat(context.Background(), 0); err == nil {
		t.Fatal("expected lost index receipt")
	}
	old := a.LastUnknown
	beat(t, b, 1) // overwrites index envelope, retaining a's successful registration
	beat(t, a, 1)
	if a.LastUnknown == nil || a.LastUnknown.Transition != old.Transition {
		t.Fatal("historical ambiguity was relabeled")
	}
	index, _, err := a.readIndex(context.Background())
	if err != nil || len(index.Entries) != 2 {
		t.Fatal(index, err)
	}
	if len(a.pending) != 0 {
		t.Fatal("newer coherent index starves heartbeat loop")
	}
}
func TestMembershipRejectsSequenceRegressionAndChangedAddress(t *testing.T) {
	for _, changed := range []string{"sequence", "address", "layout"} {
		t.Run(changed, func(t *testing.T) {
			a, b, _ := membershipFixture(t)
			beat(t, a, 0)
			beat(t, b, 0)
			discover(t, a, 0)
			beat(t, a, 1)
			beat(t, b, 1)
			discover(t, a, 1)
			key := b.heartbeatKey(b.config.Self)
			r, err := b.store.Read(context.Background(), key)
			if err != nil {
				t.Fatal(err)
			}
			h := heartbeat{1, b.config.Cluster, b.config.ExpectedLayoutDigest, b.config.Self, 2}
			switch changed {
			case "sequence":
				h.Sequence = 1
			case "address":
				h.Owner.Address = "other:80"
			case "layout":
				h.Layout[0] ^= 1
			}
			if err = b.publish(context.Background(), key, r.Version, h); err != nil {
				t.Fatal(err)
			}
			view, err := a.Discover(context.Background(), 2, Coordinator{Incarnation: leader, Generation: 1})
			if err == nil || view.Ready {
				t.Fatal(view, err)
			}
			view, err = a.Discover(context.Background(), 3, Coordinator{Incarnation: leader, Generation: 1})
			if err == nil || view.Ready {
				t.Fatal("repeated invalid record escaped high-water validation", view, err)
			}
		})
	}
}

type pruneConflictStore struct {
	registry.Store
	before   func()
	conflict registry.Key
}

func (s *pruneConflictStore) Replace(ctx context.Context, key registry.Key, v registry.Version, w registry.Write) (registry.Record, error) {
	if key == s.conflict {
		s.conflict = ""
		s.before()
	}
	return s.Store.Replace(ctx, key, v, w)
}
func TestMembershipPruneConflictCannotReuseOldExpiry(t *testing.T) {
	a, b, store := membershipFixture(t)
	beat(t, a, 0)
	beat(t, b, 0)
	discover(t, a, 0)
	beat(t, a, 1)
	beat(t, b, 1)
	discover(t, a, 1)
	beat(t, a, 22)
	a.store = &pruneConflictStore{Store: store, conflict: a.indexKey(), before: func() {
		index, r, err := b.readIndex(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		// Same entry removed/re-registered (ABA) or unrelated index update. The fresh
		// registry envelope changes version even when application bytes match.
		if err = b.publish(context.Background(), b.indexKey(), r.Version, index); err != nil {
			t.Fatal(err)
		}
	}}
	view, err := a.Discover(context.Background(), 22, Coordinator{Incarnation: leader, Generation: 1})
	var conflict *registry.Conflict
	if !errors.As(err, &conflict) || view.Ready {
		t.Fatal(view, err)
	}
	if v := discover(t, a, 23); v.Ready {
		t.Fatal("old expiry rebased after conflict", v)
	}
	index, _, err := a.readIndex(context.Background())
	if err != nil || len(index.Entries) != 2 {
		t.Fatal("re-registered entry pruned", index, err)
	}
}
