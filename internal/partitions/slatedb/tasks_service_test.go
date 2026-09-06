package slatedb

import (
	"context"
	"crypto/sha256"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/persistence"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestNativeTaskServicesShareWriterRollbackAndRecovery(t *testing.T) {
	_, open := setup(t)
	w := open("db", false)
	base, err := persistence.NewService(w, "prt_0000000000000000000001", 1000, func(context.Context) error { return nil }, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := persistence.NewExecutionTasksService(base)
	if err != nil {
		t.Fatal(err)
	}
	history, err := persistence.NewHistoryTasksService(base)
	if err != nil {
		t.Fatal(err)
	}
	digest := func(c proto.Message) []byte {
		raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		sum := sha256.Sum256(raw)
		return sum[:]
	}
	add := func(id int, c *wire.ExecutionTasksCommand) *wire.ExecutionTasksResult {
		t.Helper()
		r, err := execution.Execute(context.Background(), &wire.ExecutionTasksRequest{ProtocolVersion: 1, Partition: "prt_0000000000000000000001", OperationId: fmt.Sprintf("op_%022d", id), Command: c, CommandSha256: digest(c)})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	tasks := func(id int, c *wire.HistoryTasksCommand) *wire.HistoryTasksResult {
		t.Helper()
		r, err := history.Execute(context.Background(), &wire.HistoryTasksRequest{ProtocolVersion: 1, Partition: "prt_0000000000000000000001", OperationId: fmt.Sprintf("op_%022d", id), Command: c, CommandSha256: digest(c)})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	tx := begin(t, w)
	raw, _ := proto.Marshal(&wire.StoredShard{RangeId: 31})
	if err = tx.Put([]byte("v1/shard/0000000007"), raw); err != nil {
		t.Fatal(err)
	}
	finish(t, w, tx)
	task := &wire.ExecutionTask{CategoryId: 42, CategoryType: 2, TaskId: 1, FireSeconds: -1, FireNanos: 999999999, Blob: &wire.HistoryBlob{Data: []byte("original")}}
	command := &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_ADD, ShardId: 7, RangeId: 31, Tasks: []*wire.ExecutionTask{task}}
	if r := add(1, command); r.Error != wire.ExecutionTasksResult_NONE {
		t.Fatal(r)
	}
	fresh := proto.Clone(task).(*wire.ExecutionTask)
	fresh.TaskId = 2
	duplicate := proto.Clone(command).(*wire.ExecutionTasksCommand)
	duplicate.Tasks = []*wire.ExecutionTask{fresh, task}
	rejected := add(2, duplicate)
	if rejected.Error != wire.ExecutionTasksResult_UNAVAILABLE || read(t, w, persistence.ExecutionTaskKey(7, fresh)) != "" {
		t.Fatal("partial add survived", rejected)
	}
	query := &wire.HistoryTasksCommand{Kind: wire.HistoryTasksCommand_READ, ShardId: 7, CategoryId: 42, CategoryType: 2, Minimum: &wire.ExecutionTask{FireSeconds: -1}, Maximum: &wire.ExecutionTask{FireSeconds: 0}, PageSize: 10}
	first := tasks(3, query)
	if len(first.Tasks) != 1 || first.Tasks[0].FireNanos != 999999000 {
		t.Fatal(first)
	}
	tasks(4, &wire.HistoryTasksCommand{Kind: wire.HistoryTasksCommand_COMPLETE, ShardId: 7, CategoryId: 42, CategoryType: 2, Minimum: task, Maximum: task})
	if err = w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	w = open("db", false)
	base, err = persistence.NewService(w, "prt_0000000000000000000001", 1000, func(context.Context) error { return nil }, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	execution, _ = persistence.NewExecutionTasksService(base)
	history, _ = persistence.NewHistoryTasksService(base)
	if !proto.Equal(add(2, duplicate), rejected) || read(t, w, persistence.ExecutionTaskKey(7, fresh)) != "" {
		t.Fatal("logical failure replay changed after recovery")
	}
	if !proto.Equal(tasks(3, query), first) || len(tasks(5, query).Tasks) != 0 {
		t.Fatal("durable read replay or completion changed")
	}
	// Same borrowed writer means same global replay identity namespace.
	if _, err = history.Execute(context.Background(), &wire.HistoryTasksRequest{ProtocolVersion: 1, Partition: "prt_0000000000000000000001", OperationId: "op_0000000000000000000001", Command: query, CommandSha256: digest(query)}); err == nil {
		t.Fatal("cross-family operation ID reused")
	}
}
