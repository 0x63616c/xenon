package partitionservice_test

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	p "github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/persistence"
	"google.golang.org/protobuf/proto"
)

func TestPartitionNativeQueueReplay(t *testing.T) {
	f := newFixture(t)
	a := f.service(ownerA, f.engine)
	var qs *persistence.QueueService
	var vs *persistence.QueueV2Service
	bind := func(service *p.Service) {
		base := bindPersistence(t, f, service)
		var err error
		qs, err = persistence.NewQueueService(base)
		if err != nil {
			t.Fatal(err)
		}
		vs, err = persistence.NewQueueV2Service(base)
		if err != nil {
			t.Fatal(err)
		}
	}
	digest := func(c proto.Message) []byte {
		raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		d := sha256.Sum256(raw)
		return d[:]
	}
	queue := func(id int, c *wire.QueueCommand) *wire.QueueResult {
		c.QueueType = 1
		r, err := qs.Execute(f.ctx, &wire.QueueRequest{ProtocolVersion: 1, Partition: string(partition), OperationId: fmt.Sprintf("op_%022d", id), Command: c, CommandSha256: digest(c)})
		if err != nil || r.GetError() != wire.QueueResult_NONE {
			t.Fatalf("queue: %+v %v", r, err)
		}
		return r
	}
	v2 := func(id int, c *wire.QueueV2Command) *wire.QueueV2Result {
		c.QueueType, c.QueueName = 1, "jobs"
		r, err := vs.Execute(f.ctx, &wire.QueueV2Request{ProtocolVersion: 1, Partition: string(partition), OperationId: fmt.Sprintf("op_%022d", id), Command: c, CommandSha256: digest(c)})
		if err != nil || r.GetError() != wire.QueueV2Result_NONE {
			t.Fatalf("queuev2: %+v %v", r, err)
		}
		return r
	}
	bind(a)
	queue(1, &wire.QueueCommand{Kind: wire.QueueCommand_INIT})
	qc := &wire.QueueCommand{Kind: wire.QueueCommand_ENQUEUE, Data: []byte("normal")}
	qbefore := queue(2, qc)
	queue(3, &wire.QueueCommand{Kind: wire.QueueCommand_DELETE_BEFORE, LastId: 1})
	dlq := queue(4, &wire.QueueCommand{Kind: wire.QueueCommand_ENQUEUE_DLQ, Data: []byte("dead")})
	v2(5, &wire.QueueV2Command{Kind: wire.QueueV2Command_CREATE})
	vc := &wire.QueueV2Command{Kind: wire.QueueV2Command_ENQUEUE, HasBlob: true, Data: []byte("v2"), Encoding: 1}
	vbefore := v2(6, vc)
	dc := &wire.QueueV2Command{Kind: wire.QueueV2Command_DELETE_RANGE, InclusiveMaxId: 0}
	deleted := v2(7, dc)
	if qbefore.MessageId != 0 || dlq.MessageId != 0 || vbefore.MessageId != 0 || deleted.Deleted != 1 {
		t.Fatal("initial allocation/deletion", qbefore, dlq, vbefore, deleted)
	}
	s := f.snapshot()
	w, err := s.Assign(ownerA, f.ids.transition(), map[identity.PartitionID]cluster.Owner{partition: owner(ownerB)})
	f.replace(s, w, err)
	b := f.service(ownerB, f.engine)
	a.Poll()
	bind(b)
	if !proto.Equal(queue(2, qc), qbefore) || !proto.Equal(v2(6, vc), vbefore) || !proto.Equal(v2(7, dc), deleted) {
		t.Fatal("replay changed after movement")
	}
	stored, err := f.ready(b).ReadDurable(f.ctx, p.ReadRequest{Keys: [][]byte{[]byte("v1/outcome_count")}})
	if err != nil || len(stored.Entries) != 1 || len(stored.Entries[0].Value) != 8 || binary.BigEndian.Uint64(stored.Entries[0].Value) != 7 {
		t.Fatalf("shared outcome count: %+v %v", stored, err)
	}
	if r := queue(8, &wire.QueueCommand{Kind: wire.QueueCommand_READ, FirstId: -1, PageSize: 10}); len(r.Messages) != 0 {
		t.Fatal("deleted normal message resurrected", r)
	}
	if r := queue(9, &wire.QueueCommand{Kind: wire.QueueCommand_READ_DLQ, FirstId: -1, LastId: 100, PageSize: 10}); len(r.Messages) != 1 || r.Messages[0].Id != 0 || string(r.Messages[0].Data) != "dead" {
		t.Fatal("DLQ state lost", r)
	}
	if r := v2(10, &wire.QueueV2Command{Kind: wire.QueueV2Command_READ, PageSize: 10}); len(r.Messages) != 0 {
		t.Fatal("deleted V2 message resurrected", r)
	}
	if r := v2(11, &wire.QueueV2Command{Kind: wire.QueueV2Command_LIST, PageSize: 10}); len(r.Queues) != 1 || r.Queues[0].Count != 0 {
		t.Fatal("V2 metadata disagrees with deleted range", r)
	}
	if r := queue(12, qc); r.MessageId != 1 {
		t.Fatal("normal allocation reset", r)
	}
	if r := v2(13, vc); r.MessageId != 1 {
		t.Fatal("V2 allocation reset", r)
	}
	t.Log("queue and QueueV2 deletion, replay and counters survived native ownership movement; DLQ stayed isolated")
}
