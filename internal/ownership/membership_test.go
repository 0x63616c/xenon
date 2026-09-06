package ownership

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/directory"
)

func membershipFixture(t *testing.T) (*joinS3, *TopologyStore, *Membership, *Membership) {
	t.Helper()
	fake := &joinS3{}
	store, err := NewTopologyStore(fake, "bucket", "metadata")
	if err != nil {
		t.Fatal(err)
	}
	a, b := joinID("a", 1), joinID("b", 2)
	if err = Join(context.Background(), store, a, "data", true); err != nil {
		t.Fatal(err)
	}
	if err = Join(context.Background(), store, b, "data", false); err != nil {
		t.Fatal(err)
	}
	ma, _ := NewMembership(store, a, 10*time.Second)
	mb, _ := NewMembership(store, b, 10*time.Second)
	return fake, store, ma, mb
}

func TestMembershipHeartbeatPreventsEvictionAndLostResponseIsRecovered(t *testing.T) {
	fake, store, a, b := membershipFixture(t)
	ctx := context.Background()
	t0 := time.Unix(1, 0)
	if err := a.Step(ctx, t0); err != nil {
		t.Fatal(err)
	}
	if err := b.Step(ctx, t0.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := a.Step(ctx, t0.Add(11*time.Second)); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := store.Read(ctx)
	if len(snapshot.data.Members) != 2 {
		t.Fatal("live peer evicted")
	}
	fake.lose = true
	if err := a.Step(ctx, t0.Add(12*time.Second)); err != nil {
		t.Fatal("lost exact heartbeat response not recovered", err)
	}
}

func TestMembershipEvictsUnchangedPeerAndRebalances(t *testing.T) {
	_, store, a, _ := membershipFixture(t)
	ctx := context.Background()
	t0 := time.Unix(1, 0)
	if err := a.Step(ctx, t0); err != nil {
		t.Fatal(err)
	}
	if err := a.Step(ctx, t0.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := store.Read(ctx)
	if len(snapshot.data.Members) != 1 {
		t.Fatal(snapshot.data.Members)
	}
	for id, assignment := range snapshot.data.Partitions {
		if assignment.Node != "a" {
			t.Fatalf("partition %s not recovered: %+v", id, assignment)
		}
	}
}

func TestMembershipCASCannotRemoveFreshIncarnation(t *testing.T) {
	fake, store, a, _ := membershipFixture(t)
	ctx := context.Background()
	t0 := time.Unix(1, 0)
	if err := a.Step(ctx, t0); err != nil {
		t.Fatal(err)
	}
	fresh := joinID("b", 3)
	fake.beforePut = func() {
		if err := Join(ctx, store, fresh, "data", false); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Step(ctx, t0.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := store.Read(ctx)
	if got := snapshot.data.Members["b"]; got.Incarnation != fresh.Incarnation {
		t.Fatal("fresh incarnation removed", got)
	}
}

func TestMembershipCompetingEvictorLosesAuthority(t *testing.T) {
	_, store, a, b := membershipFixture(t)
	ctx := context.Background()
	t0 := time.Unix(1, 0)
	snapshot, err := store.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a.observe(snapshot.data, t0)
	b.observe(snapshot.data, t0)
	if err := a.Step(ctx, t0.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := b.Step(ctx, t0.Add(10*time.Second)); !errors.Is(err, directory.ErrConflict) {
		t.Fatal("removed controller retained authority", err)
	}
	snapshot, _ = store.Read(ctx)
	if len(snapshot.data.Members) != 1 || snapshot.data.Members["a"].Incarnation == "" {
		t.Fatal(snapshot.data.Members)
	}
}
