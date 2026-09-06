package persistence

import (
	"context"
	"crypto/sha256"
	"fmt"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/proto"
	"math"
	"testing"
)

func TestTaskServicesRollbackBoundsAndReplay(t *testing.T) {
	w := &executionWriter{historyWriter: &historyWriter{testWriter: &testWriter{durable: map[string][]byte{}}}}
	shard, _ := proto.Marshal(&wire.StoredShard{RangeId: 31})
	w.durable["v1/shard/0000000007"] = shard
	base, _ := NewService(w, "prt_0000000000000000000001", 1000, func(context.Context) error { return nil }, func(error) {})
	execution, _ := NewExecutionTasksService(base)
	readBase, _ := NewService(w.historyWriter, "prt_0000000000000000000001", 1000, func(context.Context) error { return nil }, func(error) {})
	history, _ := NewHistoryTasksService(readBase)
	counter := 0
	nextID := func() string { counter++; return fmt.Sprintf("op_%022d", counter) }
	executionRequest := func(id string, c *wire.ExecutionTasksCommand) *wire.ExecutionTasksRequest {
		raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		digest := sha256.Sum256(raw)
		return &wire.ExecutionTasksRequest{ProtocolVersion: 1, Partition: "prt_0000000000000000000001", OperationId: id, Command: c, CommandSha256: digest[:]}
	}
	call := func(id string, c *wire.ExecutionTasksCommand) *wire.ExecutionTasksResult {
		t.Helper()
		r, err := execution.Execute(context.Background(), executionRequest(id, c))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	readRequest := func(id string, c *wire.HistoryTasksCommand) *wire.HistoryTasksRequest {
		raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		digest := sha256.Sum256(raw)
		return &wire.HistoryTasksRequest{ProtocolVersion: 1, Partition: "prt_0000000000000000000001", OperationId: id, Command: c, CommandSha256: digest[:]}
	}
	read := func(id string, c *wire.HistoryTasksCommand) *wire.HistoryTasksResult {
		t.Helper()
		r, err := history.Execute(context.Background(), readRequest(id, c))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	scheduled := &wire.ExecutionTask{CategoryId: 42, CategoryType: 2, TaskId: 1, FireSeconds: -1, FireNanos: 999999999, Blob: &wire.HistoryBlob{Data: []byte("first")}}
	maximum := proto.Clone(scheduled).(*wire.ExecutionTask)
	maximum.TaskId = math.MaxInt64
	later := proto.Clone(scheduled).(*wire.ExecutionTask)
	later.TaskId = 0
	later.FireSeconds = 0
	later.FireNanos = 0
	immediate := proto.Clone(scheduled).(*wire.ExecutionTask)
	immediate.CategoryType = 1
	add := &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_ADD, ShardId: 7, RangeId: 31, Tasks: []*wire.ExecutionTask{scheduled, maximum, later, immediate}}
	if r := call(nextID(), add); r.Error != wire.ExecutionTasksResult_NONE {
		t.Fatal(r)
	}
	stale := proto.Clone(add).(*wire.ExecutionTasksCommand)
	stale.RangeId = 32
	if r := call(nextID(), stale); r.Error != wire.ExecutionTasksResult_OWNERSHIP_LOST || r.ShardId != 7 {
		t.Fatal(r)
	}
	fresh := proto.Clone(scheduled).(*wire.ExecutionTask)
	fresh.TaskId = 2
	duplicate := proto.Clone(add).(*wire.ExecutionTasksCommand)
	duplicate.Tasks = []*wire.ExecutionTask{fresh, scheduled}
	duplicateID := nextID()
	if r := call(duplicateID, duplicate); r.Error != wire.ExecutionTasksResult_UNAVAILABLE || w.durable[executionTaskKey(7, fresh)] != nil {
		t.Fatal("partial task add", r)
	}
	query := &wire.HistoryTasksCommand{Kind: wire.HistoryTasksCommand_READ, ShardId: 7, CategoryId: 42, CategoryType: 2, Minimum: &wire.ExecutionTask{FireSeconds: -1}, Maximum: &wire.ExecutionTask{FireSeconds: 1}, PageSize: 2}
	firstID := nextID()
	first := read(firstID, query)
	if len(first.Tasks) != 2 || first.Tasks[1].TaskId != math.MaxInt64 || first.Tasks[0].FireNanos != 999999000 {
		t.Fatal(first)
	}
	page := proto.Clone(query).(*wire.HistoryTasksCommand)
	page.NextPageToken = first.NextPageToken
	if r := read(nextID(), page); len(r.Tasks) != 1 || r.Tasks[0].FireSeconds != 0 {
		t.Fatal(r)
	}
	page.ShardId = 8
	if r := read(nextID(), page); r.Error != wire.HistoryTasksResult_INTERNAL {
		t.Fatal("cursor changed shard", r)
	}
	complete := &wire.HistoryTasksCommand{Kind: wire.HistoryTasksCommand_COMPLETE, ShardId: 7, CategoryId: 42, CategoryType: 2, Minimum: first.Tasks[0], Maximum: first.Tasks[0], BestEffort: true}
	read(nextID(), complete)
	if w.durable[executionTaskKey(7, scheduled)] == nil {
		t.Fatal("best effort removed task")
	}
	complete.BestEffort = false
	read(nextID(), complete)
	if !proto.Equal(first, read(firstID, query)) {
		t.Fatal("read replay changed after completion")
	}
	rangeDelete := proto.Clone(query).(*wire.HistoryTasksCommand)
	rangeDelete.Kind = wire.HistoryTasksCommand_RANGE_COMPLETE
	rangeDelete.Maximum = &wire.ExecutionTask{FireSeconds: 0}
	read(nextID(), rangeDelete)
	if r := read(nextID(), query); len(r.Tasks) != 1 || r.Tasks[0].FireSeconds != 0 {
		t.Fatal("scheduled upper bound changed", r)
	}
	if w.durable[executionTaskKey(7, immediate)] == nil {
		t.Fatal("category type isolation lost")
	}
	if r := call(duplicateID, duplicate); r.Error != wire.ExecutionTasksResult_UNAVAILABLE || w.durable[executionTaskKey(7, fresh)] != nil {
		t.Fatal("logical replay reinserted task", r)
	}
	// DLQ IDs are idempotent and the durable operation journal prevents replay
	// from recreating a deleted task. Cursor bindings include source and bounds.
	info, _ := proto.Marshal(&persistencespb.ReplicationTaskInfo{TaskId: 99, WorkflowId: "original"})
	put := &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_PUT_DLQ, ShardId: 7, SourceCluster: "source", TaskId: 99, TaskInfo: &wire.HistoryBlob{Data: info, Encoding: int32(enumspb.ENCODING_TYPE_PROTO3)}}
	putID := nextID()
	result := call(putID, put)
	dlq := &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_READ_DLQ, ShardId: 7, SourceCluster: "source", MinimumId: 0, MaximumId: 100, PageSize: 1}
	pageResult := call(nextID(), dlq)
	if len(pageResult.Tasks) != 1 || pageResult.Tasks[0].TaskId != 99 {
		t.Fatal(pageResult)
	}
	dlq.NextPageToken = pageResult.NextPageToken
	dlq.SourceCluster = "other"
	if r := call(nextID(), dlq); r.Error != wire.ExecutionTasksResult_INTERNAL {
		t.Fatal("unbound DLQ cursor", r)
	}
	call(nextID(), &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_DELETE_DLQ, ShardId: 7, SourceCluster: "source", TaskId: 99})
	if !proto.Equal(result, call(putID, put)) || w.durable[replicationKey(put, 99)] != nil {
		t.Fatal("DLQ replay resurrected deletion")
	}
	invalidTask := proto.Clone(fresh).(*wire.ExecutionTask)
	invalidTask.CategoryType = 99
	invalidAdd := proto.Clone(add).(*wire.ExecutionTasksCommand)
	invalidAdd.Tasks = []*wire.ExecutionTask{fresh, invalidTask}
	if r := call(nextID(), invalidAdd); r.Error != wire.ExecutionTasksResult_INTERNAL || w.durable[executionTaskKey(7, fresh)] != nil {
		t.Fatal("internal category failure failed rollback", r)
	}
	w.scanErr = partitions.ErrFenced
	if _, err := history.Execute(context.Background(), readRequest(nextID(), query)); err != partitions.ErrFenced {
		t.Fatal("history task native error translated", err)
	}
	dlq.SourceCluster = "source"
	dlq.NextPageToken = nil
	if _, err := execution.Execute(context.Background(), executionRequest(nextID(), dlq)); err != partitions.ErrFenced {
		t.Fatal("DLQ native error translated", err)
	}

}
