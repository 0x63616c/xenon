package node

import (
	"context"
	"crypto/sha256"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/partitions/memory"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"testing"
)

func executionTasksRequest(id string, c *wire.ExecutionTasksCommand) *wire.ExecutionTasksRequest {
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	d := sha256.Sum256(b)
	return &wire.ExecutionTasksRequest{ProtocolVersion: 1, Partition: "p", OperationId: id, CommandSha256: d[:], Command: c}
}
func TestGoOwnerExecutionTasksRecovery(t *testing.T) {
	ctx := context.Background()
	objects := memory.New()
	path := "executiontasks-recovery"
	o := memoryOwner(t, objects, path, "p")
	s := &ExecutionTasksServer{Owner: o}
	call := func(id string, c *wire.ExecutionTasksCommand) *wire.ExecutionTasksResult {
		t.Helper()
		r, e := s.Execute(ctx, executionTasksRequest(id, c))
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	_, e := o.Run(ctx, func(db partitions.Writer) ([]byte, error) {
		tx, e := db.Begin(context.Background())
		if e != nil {
			return nil, backend(e)
		}
		defer tx.Abort()
		if e = putMessage(tx, "v1/shard/0000000007", &wire.StoredShard{RangeId: 31}); e != nil {
			return nil, e
		}
		return nil, commitTest(db, tx)
	})
	if e != nil {
		t.Fatal(e)
	}
	task := &wire.ExecutionTask{CategoryId: 1, CategoryType: 1, TaskId: 1, Blob: &wire.HistoryBlob{Data: []byte{255, 0}, Encoding: 1}}
	add := &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_ADD, ShardId: 7, RangeId: 31, Tasks: []*wire.ExecutionTask{task}}
	missing := proto.Clone(add).(*wire.ExecutionTasksCommand)
	missing.ShardId = 9999
	if r := call("missing-shard", missing); r.Error != wire.ExecutionTasksResult_UNAVAILABLE || o.Quarantined() {
		t.Fatal(r, o.Quarantined())
	}
	if r := call("add", add); r.Error != wire.ExecutionTasksResult_NONE {
		t.Fatal(r)
	}
	bad := proto.Clone(add).(*wire.ExecutionTasksCommand)
	bad.RangeId = 32
	if r := call("stale-range", bad); r.Error != wire.ExecutionTasksResult_OWNERSHIP_LOST || r.ShardId != 7 {
		t.Fatal(r)
	}
	fresh := proto.Clone(task).(*wire.ExecutionTask)
	fresh.TaskId = 2
	duplicate := proto.Clone(add).(*wire.ExecutionTasksCommand)
	duplicate.Tasks = []*wire.ExecutionTask{fresh, task}
	if r := call("atomic-duplicate", duplicate); r.Error != wire.ExecutionTasksResult_UNAVAILABLE || o.Quarantined() {
		t.Fatal(r, o.Quarantined())
	}
	_, e = o.Run(ctx, func(db partitions.Writer) ([]byte, error) {
		tx, e := db.Begin(context.Background())
		if e != nil {
			return nil, backend(e)
		}
		defer tx.Abort()
		b, e := get(tx, executionTaskKey(7, fresh))
		if e != nil || b != nil {
			t.Fatal("partial AddHistoryTasks", e)
		}
		return nil, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	info, _ := proto.Marshal(&persistencespb.ReplicationTaskInfo{TaskId: 99, WorkflowId: "original"})
	put := &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_PUT_DLQ, ShardId: 7, SourceCluster: "s", TaskId: 99, TaskInfo: &wire.HistoryBlob{Data: info, Encoding: int32(enumspb.ENCODING_TYPE_PROTO3)}}
	first := call("put", put)
	if first.Error != wire.ExecutionTasksResult_NONE {
		t.Fatal(first)
	}
	call("delete", &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_DELETE_DLQ, ShardId: 7, SourceCluster: "s", TaskId: 99})
	if e = o.Close(ctx); e != nil {
		t.Fatal(e)
	}
	o = memoryOwner(t, objects, path, "p")
	s.Owner = o
	if !proto.Equal(call("put", put), first) {
		t.Fatal("changed put replay")
	}
	if r := call("empty-after-replay", &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_IS_EMPTY_DLQ, ShardId: 7, SourceCluster: "s", MinimumId: 0}); !r.Empty {
		t.Fatal("replay resurrected deleted DLQ task")
	}
	// A duplicate failure remains a stable logical outcome even after recovery.
	if r := call("atomic-duplicate", duplicate); r.Error != wire.ExecutionTasksResult_UNAVAILABLE {
		t.Fatal(r)
	}
	contender := memoryOwner(t, objects, path, "p")
	_ = contender
	if _, e = s.Execute(ctx, executionTasksRequest("put", put)); status.Code(e) != codes.Unavailable || !o.Quarantined() {
		t.Fatal("fenced DLQ replay", e, o.Quarantined())
	}
}
func TestGoOwnerExecutionTasksBytePages(t *testing.T) {
	ctx := context.Background()
	o := memoryOwner(t, memory.New(), "executiontasks-pages", "p")
	s := &ExecutionTasksServer{Owner: o}
	for id := int64(1); id <= 3; id++ {
		info, _ := proto.Marshal(&persistencespb.ReplicationTaskInfo{TaskId: id, WorkflowId: string(make([]byte, 1024*1024))})
		c := &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_PUT_DLQ, ShardId: 7, SourceCluster: "large", TaskId: id, TaskInfo: &wire.HistoryBlob{Data: info, Encoding: int32(enumspb.ENCODING_TYPE_PROTO3)}}
		if _, e := s.Execute(ctx, executionTasksRequest(string(rune('a'+id)), c)); e != nil {
			t.Fatal(e)
		}
	}
	c := &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_READ_DLQ, ShardId: 7, SourceCluster: "large", MinimumId: 1, MaximumId: 4, PageSize: 1000}
	r, e := s.Execute(ctx, executionTasksRequest("page1", c))
	if e != nil || len(r.GetTasks()) != 2 || proto.Size(r) > 3<<20 {
		t.Fatal(r, e)
	}
	c.NextPageToken = r.NextPageToken
	r, e = s.Execute(ctx, executionTasksRequest("page2", c))
	if e != nil || len(r.GetTasks()) != 1 || r.Tasks[0].TaskId != 3 {
		t.Fatal(r, e)
	}
	changed := proto.Clone(c).(*wire.ExecutionTasksCommand)
	changed.SourceCluster = "other"
	r, e = s.Execute(ctx, executionTasksRequest("bad-token", changed))
	if e != nil || r.Error != wire.ExecutionTasksResult_INTERNAL {
		t.Fatal("unbound cursor", r, e)
	}
}
