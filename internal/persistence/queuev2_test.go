package persistence

import (
	"context"
	"crypto/sha256"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"google.golang.org/protobuf/proto"
	"maps"
	"testing"
)

func TestQueueV2ReplayRetainsAtomicDeletionAndAllocation(t *testing.T) {
	w := &clusterWriter{testWriter: &testWriter{durable: map[string][]byte{}}}
	bind := func(w *clusterWriter) *QueueV2Service {
		base, err := NewService(w, "prt_0000000000000000000001", 100, func(context.Context) error { return nil }, func(error) {})
		if err != nil {
			t.Fatal(err)
		}
		s, err := NewQueueV2Service(base)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := bind(w)
	call := func(id int, c *wire.QueueV2Command) *wire.QueueV2Result {
		c = proto.Clone(c).(*wire.QueueV2Command)
		c.QueueType = 1
		c.QueueName = "jobs"
		raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		d := sha256.Sum256(raw)
		r, err := s.Execute(context.Background(), &wire.QueueV2Request{ProtocolVersion: 1, Partition: "prt_0000000000000000000001", OperationId: fmt.Sprintf("op_%022d", id), Command: c, CommandSha256: d[:]})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if r := call(1, &wire.QueueV2Command{Kind: wire.QueueV2Command_CREATE}); r.Error != wire.QueueV2Result_NONE {
		t.Fatal(r)
	}
	enqueue := &wire.QueueV2Command{Kind: wire.QueueV2Command_ENQUEUE, HasBlob: true, Data: []byte("payload"), Encoding: 1}
	first := call(2, enqueue)
	if first.MessageId != 0 {
		t.Fatal(first)
	}
	if r := call(3, enqueue); r.MessageId != 1 {
		t.Fatal(r)
	}
	deleted := call(4, &wire.QueueV2Command{Kind: wire.QueueV2Command_DELETE_RANGE, InclusiveMaxId: 0})
	if deleted.Deleted != 1 {
		t.Fatal(deleted)
	}
	w = &clusterWriter{testWriter: &testWriter{durable: maps.Clone(w.durable)}}
	s = bind(w)
	if r := call(2, enqueue); !proto.Equal(r, first) {
		t.Fatal("replay changed allocation", r)
	}
	if r := call(4, &wire.QueueV2Command{Kind: wire.QueueV2Command_DELETE_RANGE, InclusiveMaxId: 0}); !proto.Equal(r, deleted) {
		t.Fatal("deletion replay changed", r)
	}
	r := call(5, &wire.QueueV2Command{Kind: wire.QueueV2Command_LIST, PageSize: 10})
	if len(r.Queues) != 1 || r.Queues[0].Count != 1 || r.Queues[0].LastId != 1 {
		t.Fatal("metadata diverged from deleted range", r)
	}
	r = call(6, &wire.QueueV2Command{Kind: wire.QueueV2Command_READ, PageSize: 1})
	if len(r.Messages) != 1 || r.Messages[0].Id != 1 || string(r.Messages[0].Data) != "payload" {
		t.Fatal("deleted message reappeared", r)
	}
	if r = call(7, enqueue); r.MessageId != 2 {
		t.Fatal("deleted ID reused", r)
	}
	if r = call(8, &wire.QueueV2Command{Kind: wire.QueueV2Command_DELETE_RANGE, InclusiveMaxId: 999}); r.Deleted != 2 {
		t.Fatal("delete not bounded by allocated range", r)
	}
	if r = call(9, enqueue); r.MessageId != 3 {
		t.Fatal("empty queue allocation reset", r)
	}
	invalid := call(10, &wire.QueueV2Command{Kind: wire.QueueV2Command_READ, PageSize: 0})
	if invalid.Error != wire.QueueV2Result_NONPOSITIVE_READ_SIZE {
		t.Fatal("logical condition flattened", invalid)
	}
	if r = call(10, &wire.QueueV2Command{Kind: wire.QueueV2Command_READ, PageSize: 0}); !proto.Equal(invalid, r) {
		t.Fatal("condition replay changed", r)
	}
}
