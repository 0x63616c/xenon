//go:build slatedb

package node

import (
	"context"
	"crypto/sha256"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"google.golang.org/protobuf/proto"
	"math"
	native "slatedb.io/slatedb-go/uniffi"
	"testing"
)

func historyTasksRequest(id string, c *wire.HistoryTasksCommand) *wire.HistoryTasksRequest {
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	d := sha256.Sum256(b)
	return &wire.HistoryTasksRequest{ProtocolVersion: 1, Partition: "p", OperationId: id, CommandSha256: d[:], Command: c}
}
func TestGoOwnerHistoryTasksRecovery(t *testing.T) {
	ctx := context.Background()
	store := objects(t)
	o := owner(t, engine(t, store, "historytasks-recovery", false))
	s := &HistoryTasksServer{Owner: o}
	_, e := o.Run(ctx, func(db *native.Db) ([]byte, error) {
		tx, e := db.Begin(native.IsolationLevelSerializableSnapshot)
		if e != nil {
			return nil, e
		}
		defer tx.Destroy()
		for _, task := range []*wire.ExecutionTask{
			{CategoryId: 42, CategoryType: 2, TaskId: 1, FireSeconds: -1, FireNanos: 999999999, Blob: &wire.HistoryBlob{Data: []byte("first"), Encoding: 2}},
			{CategoryId: 42, CategoryType: 2, TaskId: math.MaxInt64, FireSeconds: -1, FireNanos: 999999999, Blob: &wire.HistoryBlob{Data: []byte("max"), Encoding: 2}},
			{CategoryId: 42, CategoryType: 2, TaskId: 0, FireSeconds: 0, Blob: &wire.HistoryBlob{Data: []byte("next"), Encoding: 2}},
			{CategoryId: 42, CategoryType: 1, TaskId: 1, Blob: &wire.HistoryBlob{Data: []byte("isolated"), Encoding: 2}},
		} {
			if e = stageExecutionTasks(tx, 7, []*wire.ExecutionTask{task}); e != nil {
				return nil, e
			}
		}
		return nil, commit(tx)
	})
	if e != nil {
		t.Fatal(e)
	}
	c := &wire.HistoryTasksCommand{Kind: wire.HistoryTasksCommand_READ, ShardId: 7, CategoryId: 42, CategoryType: 2, Minimum: &wire.ExecutionTask{FireSeconds: -1}, Maximum: &wire.ExecutionTask{FireSeconds: 1}, PageSize: 2}
	call := func(id string, c *wire.HistoryTasksCommand) *wire.HistoryTasksResult {
		t.Helper()
		r, e := s.Execute(ctx, historyTasksRequest(id, c))
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	first := call("read-first", c)
	if len(first.Tasks) != 2 || first.Tasks[1].TaskId != math.MaxInt64 || first.Tasks[0].FireNanos != 999999000 {
		t.Fatal(first)
	}
	c.NextPageToken = first.NextPageToken
	second := call("read-second", c)
	if len(second.Tasks) != 1 || second.Tasks[0].FireSeconds != 0 {
		t.Fatal(second)
	}
	bad := proto.Clone(c).(*wire.HistoryTasksCommand)
	bad.ShardId = 8
	if call("wrong-bound", bad).Error != wire.HistoryTasksResult_INTERNAL {
		t.Fatal("unbound cursor")
	}
	done := &wire.HistoryTasksCommand{Kind: wire.HistoryTasksCommand_COMPLETE, ShardId: 7, CategoryId: 42, CategoryType: 2, Minimum: first.Tasks[0], Maximum: first.Tasks[0], BestEffort: true}
	call("best-effort", done)
	c.NextPageToken = nil
	if len(call("still-there", c).Tasks) != 2 {
		t.Fatal("best effort removed task")
	}
	done.BestEffort = false
	call("complete", done)
	if e = o.Close(ctx); e != nil {
		t.Fatal(e)
	}
	o = owner(t, engine(t, store, "historytasks-recovery", false))
	s.Owner = o
	if !proto.Equal(call("read-first", c), first) {
		t.Fatal("durable read replay changed")
	}
	fresh := call("after-reopen", c)
	if len(fresh.Tasks) != 2 || fresh.Tasks[0].TaskId != math.MaxInt64 {
		t.Fatal(fresh)
	}
	del := proto.Clone(c).(*wire.HistoryTasksCommand)
	del.Kind = wire.HistoryTasksCommand_RANGE_COMPLETE
	del.Maximum = &wire.ExecutionTask{FireSeconds: 0}
	call("range-delete", del)
	remaining := call("after-range", c)
	if len(remaining.Tasks) != 1 || remaining.Tasks[0].FireSeconds != 0 {
		t.Fatal(remaining)
	}
	c.CategoryType = 1
	c.Minimum = &wire.ExecutionTask{TaskId: 0}
	c.Maximum = &wire.ExecutionTask{TaskId: 2}
	if len(call("immediate-isolation", c).Tasks) != 1 {
		t.Fatal("category type isolation lost")
	}
}

func TestGoOwnerHistoryTasksBytePages(t *testing.T) {
	ctx := context.Background()
	store := objects(t)
	o := owner(t, engine(t, store, "historytasks-byte-pages", false))
	s := &HistoryTasksServer{Owner: o}
	_, e := o.Run(ctx, func(db *native.Db) ([]byte, error) {
		tx, e := db.Begin(native.IsolationLevelSerializableSnapshot)
		if e != nil {
			return nil, e
		}
		defer tx.Destroy()
		for id := int64(1); id <= 3; id++ {
			if e = stageExecutionTasks(tx, 7, []*wire.ExecutionTask{{CategoryId: 1, CategoryType: 1, TaskId: id, Blob: &wire.HistoryBlob{Data: make([]byte, 1024*1024), Encoding: 2}}}); e != nil {
				return nil, e
			}
		}
		return nil, commit(tx)
	})
	if e != nil {
		t.Fatal(e)
	}
	c := &wire.HistoryTasksCommand{Kind: wire.HistoryTasksCommand_READ, ShardId: 7, CategoryId: 1, CategoryType: 1, Minimum: &wire.ExecutionTask{TaskId: 0}, Maximum: &wire.ExecutionTask{TaskId: 4}, PageSize: 1000}
	r, e := s.Execute(ctx, historyTasksRequest("large-first", c))
	if e != nil || len(r.Tasks) != 2 || proto.Size(r) > 3*1024*1024 {
		t.Fatal(e, len(r.GetTasks()))
	}
	c.NextPageToken = r.NextPageToken
	r, e = s.Execute(ctx, historyTasksRequest("large-second", c))
	if e != nil || len(r.Tasks) != 1 || r.Tasks[0].TaskId != 3 {
		t.Fatal(r, e)
	}
	// The successor owner fences every path, including replay of acknowledged reads.
	replacement := owner(t, engine(t, store, "historytasks-byte-pages", false))
	_ = replacement
	if _, e = s.Execute(ctx, historyTasksRequest("large-second", c)); e == nil {
		t.Fatal("fenced read replay succeeded")
	}
}
