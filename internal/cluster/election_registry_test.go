//go:build linux || darwin

package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
	"github.com/0x63616c/xenon/internal/registry/filesystem"
)

// Exercise proposal preconditions against a qualified real CAS implementation.
// This fixture is not the production control schema or a runtime election test.
func TestDelayedRenewalAndCompetingTakeoverCannotOverwriteWinner(t *testing.T) {
	ctx := context.Background()
	store, err := filesystem.New(filesystem.Config{Directory: t.TempDir(), MaxRecordBytes: 1 << 20, Wait: func(context.Context) error { return errors.New("unexpected concurrent lock") }})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	key := registry.Key("cluster/control")
	type control struct {
		Leader     CoordinatorChange
		Assignment string
	}
	write := func(expected registry.Version, id identity.TransitionID, body control) registry.Write {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		w, err := registry.NewWrite(key, expected, id, raw)
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	initialBody := control{CoordinatorChange{Incarnation: leader, Generation: 1, Renewal: 4}, "stable-partition-path"}
	r, err := store.Create(ctx, key, write("", "trn_0000000000000000000001", initialBody))
	if err != nil {
		t.Fatal(err)
	}
	a := initial()
	a.Version = r.Version
	s, err := (Election{}).Observe(0, a)
	if err != nil {
		t.Fatal(err)
	}
	oldRenewal, err := Renew(leader, a)
	if err != nil {
		t.Fatal(err)
	}
	winner, ok, err := s.Propose(10, 10*time.Nanosecond, contender, a)
	if err != nil || !ok {
		t.Fatalf("proposal: %v %v", ok, err)
	}
	other := identity.IncarnationID("inc_0000000000000000000003")
	loser, ok, err := s.Propose(10, 10*time.Nanosecond, other, a)
	if err != nil || !ok {
		t.Fatalf("proposal: %v %v", ok, err)
	}
	if _, err := store.Replace(ctx, key, winner.Expected, write(winner.Expected, "trn_0000000000000000000002", control{winner, initialBody.Assignment})); err != nil {
		t.Fatal(err)
	}
	for i, delayed := range []CoordinatorChange{oldRenewal, loser} {
		id := []identity.TransitionID{"trn_0000000000000000000003", "trn_0000000000000000000004"}[i]
		_, err := store.Replace(ctx, key, delayed.Expected, write(delayed.Expected, id, control{delayed, "obsolete-plan"}))
		var conflict *registry.Conflict
		if !errors.As(err, &conflict) {
			t.Fatalf("delayed writer changed control: %v", err)
		}
	}
	r, err = store.Read(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	env, err := registry.Decode(key, r)
	if err != nil {
		t.Fatal(err)
	}
	var recovered control
	if err := json.Unmarshal(env.Body, &recovered); err != nil {
		t.Fatal(err)
	}
	if recovered.Leader.Incarnation != contender || recovered.Leader.Generation != 2 || recovered.Assignment != initialBody.Assignment {
		t.Fatalf("winner lost: %+v", recovered)
	}
}
