package node

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/partitions/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"os"
	"testing"
)

func queueRequest(id string, c *wire.QueueCommand) *wire.QueueRequest {
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	d := sha256.Sum256(raw)
	return &wire.QueueRequest{ProtocolVersion: 1, Partition: "p", OperationId: id, CommandSha256: d[:], Command: c}
}
func TestGoOwnerQueueRecovery(t *testing.T) {
	raw, e := os.ReadFile("../../proof/queue/case.json")
	if e != nil {
		t.Fatal(e)
	}
	var fixture struct {
		Prefix string `json:"prefix"`
		Schema int    `json:"schema_version"`
	}
	if e = json.Unmarshal(raw, &fixture); e != nil || fixture.Schema != 1 {
		t.Fatal(e)
	}
	objects := memory.New()
	path := fixture.Prefix + "-recovery"
	o := memoryOwner(t, objects, path, "p")
	server := &QueueServer{Owner: o}
	ctx := context.Background()
	call := func(id string, c *wire.QueueCommand) *wire.QueueResult {
		t.Helper()
		r, e := server.Execute(ctx, queueRequest(id, c))
		if e != nil || r.Error != wire.QueueResult_NONE {
			t.Fatal(id, r, e)
		}
		return r
	}
	call("init", &wire.QueueCommand{Kind: wire.QueueCommand_INIT, QueueType: 1, Data: []byte{1}, Encoding: 2})
	enqueue := queueRequest("enqueue", &wire.QueueCommand{Kind: wire.QueueCommand_ENQUEUE_DLQ, QueueType: 1, Data: []byte{0, 255}, Encoding: 2})
	initial, e := server.Execute(ctx, enqueue)
	if e != nil || initial.MessageId != 0 {
		t.Fatal(initial, e)
	}
	call("delete", &wire.QueueCommand{Kind: wire.QueueCommand_DELETE_DLQ, QueueType: 1, LastId: 0})
	closeMemoryOwner(t, o)
	o = memoryOwner(t, objects, path, "p")
	defer closeMemoryOwner(t, o)
	server = &QueueServer{Owner: o}
	replay, e := server.Execute(ctx, enqueue)
	if e != nil || !proto.Equal(initial, replay) {
		t.Fatal(replay, e)
	}
	empty := call("empty", &wire.QueueCommand{Kind: wire.QueueCommand_READ_DLQ, QueueType: 1, FirstId: -1, LastId: 100, PageSize: 10})
	if len(empty.Messages) != 0 {
		t.Fatal("replay recreated deleted entry")
	}
	next := call("next", &wire.QueueCommand{Kind: wire.QueueCommand_ENQUEUE_DLQ, QueueType: 1, Data: []byte{9}, Encoding: 2})
	if next.MessageId != 1 {
		t.Fatal("reopen reset allocator", next)
	}
	// Abort after staging a delete inside the exact shared journal; no delete or
	// journal outcome can escape the failed transaction.
	_, e = o.Run(ctx, func(partitions.Writer) ([]byte, error) {
		_, e := o.journal("abort-delete", []byte("digest"), queueFamily, func(tx partitions.Transaction) (*wire.StoredOutcome, error) {
			if e := tx.Delete([]byte(queueEntryKey(-1, 1))); e != nil {
				return nil, backend(e)
			}
			return nil, status.Error(codes.ResourceExhausted, "declared rollback barrier")
		})
		return nil, e
	})
	if status.Code(e) != codes.ResourceExhausted || o.Quarantined() {
		t.Fatal("rollback disposition", e)
	}
	rows := call("after-abort", &wire.QueueCommand{Kind: wire.QueueCommand_READ_DLQ, QueueType: 1, FirstId: -1, LastId: 100, PageSize: 10})
	if len(rows.Messages) != 1 || rows.Messages[0].Id != 1 {
		t.Fatal("aborted delete escaped", rows)
	}
	_, e = o.Run(ctx, func(db partitions.Writer) ([]byte, error) {
		tx, e := db.Begin(context.Background())
		if e != nil {
			return nil, backend(e)
		}
		defer tx.Abort()
		saved, e := get(tx, "v1/outcome/abort-delete")
		if e == nil && saved != nil {
			t.Error("aborted outcome journaled")
		}
		return nil, e
	})
	if e != nil {
		t.Fatal(e)
	}
	competitor := memoryOwner(t, objects, path, "p")
	defer closeMemoryOwner(t, competitor)
	if _, e = server.Execute(ctx, enqueue); status.Code(e) != codes.Unavailable || !o.Quarantined() {
		t.Fatal("fenced replay succeeded", e)
	}
}
