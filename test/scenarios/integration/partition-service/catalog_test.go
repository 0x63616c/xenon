package partitionservice_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	p "github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/persistence"
	"google.golang.org/protobuf/proto"
)

func TestPartitionNativeCatalogReplay(t *testing.T) {
	f := newFixture(t)
	bind := func(service *p.Service) *persistence.Service {
		f.ready(service)
		writer, attempt, handle, ok := service.Writer()
		if !ok {
			t.Fatal("missing writer")
		}
		check := func(ctx context.Context) error {
			r, err := f.store.Read(ctx, key)
			if err != nil {
				return err
			}
			s, err := cluster.DecodeControl(key, r, 1<<20)
			if err != nil {
				return err
			}
			current := s.Control().Partitions[partition]
			if !current.Ready || current.Desired.Incarnation != attempt.Incarnation || current.AssignmentRevision != attempt.AssignmentRevision || current.Generation != attempt.Generation || current.Reservation != attempt.Reservation {
				return cluster.ErrStaleControl
			}
			return nil
		}
		s, err := persistence.NewService(writer, partition, 100, check, func(err error) { service.ObserveFailure(handle, err) })
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	digest := func(c proto.Message) []byte {
		raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		d := sha256.Sum256(raw)
		return d[:]
	}
	id := bytes.Repeat([]byte{1}, 16)
	cc := &wire.ClusterCommand{Kind: wire.ClusterCommand_SAVE, ClusterName: "catalog", Blob: &wire.ClusterBlob{Data: []byte("cluster"), Encoding: 1}}
	nc := &wire.NexusCommand{Endpoint: &wire.NexusEndpoint{Id: id, Data: []byte("endpoint"), Encoding: 1}}
	mc := &wire.MetadataCommand{Kind: wire.MetadataCommand_CREATE, Id: id, Name: "namespace", Data: []byte("namespace"), Encoding: 1}
	cq := &wire.ClusterRequest{ProtocolVersion: 1, Partition: string(partition), OperationId: "op_0000000000000000000001", Command: cc, CommandSha256: digest(cc)}
	nq := &wire.NexusRequest{ProtocolVersion: 1, Partition: string(partition), OperationId: "op_0000000000000000000002", Command: nc, CommandSha256: digest(nc)}
	mq := &wire.MetadataRequest{ProtocolVersion: 1, Partition: string(partition), OperationId: "op_0000000000000000000003", Command: mc, CommandSha256: digest(mc)}
	run := func(base *persistence.Service) []proto.Message {
		cs, e := persistence.NewClusterService(base, func() time.Time { return time.Unix(100, 0) })
		if e != nil {
			t.Fatal(e)
		}
		ns, e := persistence.NewNexusService(base)
		if e != nil {
			t.Fatal(e)
		}
		ms, e := persistence.NewMetadataService(base)
		if e != nil {
			t.Fatal(e)
		}
		c, e := cs.Execute(f.ctx, cq)
		if e != nil || c.GetError() != wire.ClusterResult_NONE {
			t.Fatalf("cluster: %+v %v", c, e)
		}
		n, e := ns.Execute(f.ctx, nq)
		if e != nil || n.GetError() != wire.NexusResult_NONE {
			t.Fatalf("nexus: %+v %v", n, e)
		}
		m, e := ms.Execute(f.ctx, mq)
		if e != nil || m.GetError() != wire.MetadataResult_NONE {
			t.Fatalf("namespace: %+v %v", m, e)
		}
		return []proto.Message{c, n, m}
	}
	a := f.service(ownerA, f.engine)
	before := run(bind(a))
	s := f.snapshot()
	w, err := s.Assign(ownerA, f.ids.transition(), map[identity.PartitionID]cluster.Owner{partition: owner(ownerB)})
	f.replace(s, w, err)
	b := f.service(ownerB, f.engine)
	a.Poll()
	base := bind(b)
	after := run(base)
	for i := range before {
		if !proto.Equal(before[i], after[i]) {
			t.Fatalf("family %d replay changed", i)
		}
	}
	// Replay must not create additional outcomes or reapply the three catalog writes.
	stored, err := f.ready(b).ReadDurable(f.ctx, p.ReadRequest{Keys: [][]byte{[]byte("v1/outcome_count")}})
	if err != nil || len(stored.Entries) != 1 || len(stored.Entries[0].Value) != 8 || binary.BigEndian.Uint64(stored.Entries[0].Value) != 3 {
		t.Fatalf("shared replay accounting: %+v %v", stored, err)
	}
	// New IDs force actual application reads rather than replaying prior outcomes.
	cc = &wire.ClusterCommand{Kind: wire.ClusterCommand_GET, ClusterName: "catalog"}
	cq = &wire.ClusterRequest{ProtocolVersion: 1, Partition: string(partition), OperationId: "op_0000000000000000000004", Command: cc, CommandSha256: digest(cc)}
	nc = &wire.NexusCommand{Kind: wire.NexusCommand_GET, Id: id}
	nq = &wire.NexusRequest{ProtocolVersion: 1, Partition: string(partition), OperationId: "op_0000000000000000000005", Command: nc, CommandSha256: digest(nc)}
	mc = &wire.MetadataCommand{Kind: wire.MetadataCommand_GET, Name: "namespace"}
	mq = &wire.MetadataRequest{ProtocolVersion: 1, Partition: string(partition), OperationId: "op_0000000000000000000006", Command: mc, CommandSha256: digest(mc)}
	got := run(base)
	c := got[0].(*wire.ClusterResult).GetRecord()
	n := got[1].(*wire.NexusResult).GetEndpoint()
	m := got[2].(*wire.MetadataResult).GetNamespaces()
	if c.GetVersion() != 1 || string(c.GetBlob().GetData()) != "cluster" || n.GetVersion() != 1 || string(n.GetData()) != "endpoint" || len(m) != 1 || string(m[0].Data) != "namespace" || !bytes.Equal(m[0].Id, id) {
		t.Fatalf("catalog state missing: %+v %+v %+v", c, n, m)
	}
	t.Log("cluster, Nexus and namespace writes and replay survived native ownership movement")
}
