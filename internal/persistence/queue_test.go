package persistence

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"maps"
	"math"
	"testing"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestQueueReplayPreservesDeletedIDsAndDLQIsolation(t *testing.T) {
	w := &clusterWriter{testWriter: &testWriter{durable: map[string][]byte{}}}
	bind := func(w *clusterWriter) *QueueService {
		base, err := NewService(w, "prt_0000000000000000000001", 100, func(context.Context) error { return nil }, func(error) {})
		if err != nil {
			t.Fatal(err)
		}
		s, err := NewQueueService(base)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := bind(w)
	request := func(id int, c *wire.QueueCommand) *wire.QueueRequest {
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		d := sha256.Sum256(b)
		return &wire.QueueRequest{ProtocolVersion: 1, Partition: "prt_0000000000000000000001", OperationId: fmt.Sprintf("op_%022d", id), Command: c, CommandSha256: d[:]}
	}
	call := func(id int, c *wire.QueueCommand) *wire.QueueResult {
		r, err := s.Execute(context.Background(), request(id, c))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	call(1, &wire.QueueCommand{Kind: wire.QueueCommand_INIT, QueueType: 1})
	enqueue := &wire.QueueCommand{Kind: wire.QueueCommand_ENQUEUE, QueueType: 1, Data: []byte("original")}
	first := call(2, enqueue)
	if first.MessageId != 0 {
		t.Fatal(first)
	}
	call(3, &wire.QueueCommand{Kind: wire.QueueCommand_DELETE_BEFORE, QueueType: 1, LastId: 1})
	if r := call(4, &wire.QueueCommand{Kind: wire.QueueCommand_ENQUEUE_DLQ, QueueType: 1, Data: []byte("dead")}); r.MessageId != 0 {
		t.Fatal(r)
	}
	if r := call(5, enqueue); r.MessageId != 1 {
		t.Fatal("deleted ID reused", r)
	}
	rejected := call(6, &wire.QueueCommand{Kind: wire.QueueCommand_UPDATE_ACK, QueueType: 1, Version: 3})
	if rejected.Error != wire.QueueResult_UNAVAILABLE {
		t.Fatal("wrong ACK version accepted", rejected)
	}
	recovered := &clusterWriter{testWriter: &testWriter{durable: maps.Clone(w.durable)}}
	s = bind(recovered)
	if r := call(2, enqueue); !proto.Equal(r, first) {
		t.Fatal("replay changed allocation", r)
	}
	if binary.BigEndian.Uint64(recovered.durable["v1/outcome_count"]) != 6 {
		t.Fatal("replay reapplied")
	}
	r := call(7, &wire.QueueCommand{Kind: wire.QueueCommand_READ, QueueType: 1, FirstId: -1, PageSize: 10})
	if len(r.Messages) != 1 || r.Messages[0].Id != 1 || string(r.Messages[0].Data) != "original" {
		t.Fatal("normal queue was rewritten by replay or DLQ", r)
	}
	r = call(8, &wire.QueueCommand{Kind: wire.QueueCommand_READ_DLQ, QueueType: 1, FirstId: -1, LastId: math.MaxInt64, PageSize: 1})
	if len(r.Messages) != 1 || r.Messages[0].Id != 0 || string(r.Messages[0].Data) != "dead" {
		t.Fatal("DLQ isolation", r)
	}
	call(9, &wire.QueueCommand{Kind: wire.QueueCommand_DELETE_DLQ, QueueType: 1, LastId: 0})
	if r = call(10, &wire.QueueCommand{Kind: wire.QueueCommand_ENQUEUE_DLQ, QueueType: 1}); r.MessageId != 1 {
		t.Fatal("deleted DLQ ID reused", r)
	}
	changed := proto.Clone(enqueue).(*wire.QueueCommand)
	changed.Data = []byte("changed")
	if _, err := s.Execute(context.Background(), request(2, changed)); status.Code(err) != codes.InvalidArgument {
		t.Fatal("changed digest accepted", err)
	}
}
