//go:build integration_s3

package s3

import (
	"context"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
)

func membershipProof(t *testing.T, ctx context.Context, store *Store) {
	layout := cluster.Layout{Version: 1, Placement: cluster.DefaultPlacementConfig(), Partitions: []cluster.PhysicalPartition{{LogicalName: "shard", ID: "prt_0000000000000000000001", Path: "data/shard"}}}
	digest, err := layout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	config := cluster.MembershipConfig{Prefix: "advisory-membership", Cluster: "clu_0000000000000000000001", ExpectedLayoutDigest: digest, Self: cluster.Owner{Node: "nod_0000000000000000000001", Incarnation: "inc_0000000000000000000001", Address: "a:8080"}, FailureAfter: 20 * time.Nanosecond, MaxEntries: 4, MaxRecordBytes: 1 << 20, ReadsPerScan: 1}
	a, err := cluster.NewMembership(config, store, identity.Generator{})
	if err != nil {
		t.Fatal(err)
	}
	config.Self = cluster.Owner{Node: "nod_0000000000000000000002", Incarnation: "inc_0000000000000000000002", Address: "b:8080"}
	b, err := cluster.NewMembership(config, store, identity.Generator{})
	if err != nil {
		t.Fatal(err)
	}
	beat := func(m *cluster.Membership, at cluster.Tick) {
		t.Helper()
		if err := m.Heartbeat(ctx, at); err != nil {
			t.Fatal(err)
		}
	}
	c := cluster.Coordinator{Incarnation: "inc_0000000000000000000001", Generation: 1}
	scan := func(at cluster.Tick) cluster.MembershipView {
		t.Helper()
		v, err := a.Discover(ctx, at, c)
		if err != nil {
			t.Fatal(err)
		}
		if v.Ready {
			t.Fatal("partial scan ready", v)
		}
		v, err = a.Discover(ctx, at, c)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	beat(a, 0)
	beat(b, 0)
	if v := scan(0); v.Ready {
		t.Fatal("first baseline admitted", v)
	}
	beat(a, 1)
	beat(b, 1)
	if v := scan(1); !v.Ready || len(v.Members) != 2 {
		t.Fatal(v)
	}
	beat(a, 22)
	if v := scan(22); !v.Ready || len(v.Members) != 1 {
		t.Fatal(v)
	}
	beat(b, 23)
	if v := scan(23); v.Ready {
		t.Fatal("rejoin reused suspicion", v)
	}
	beat(a, 24)
	beat(b, 24)
	if v := scan(24); !v.Ready || len(v.Members) != 2 {
		t.Fatal(v)
	}
}
