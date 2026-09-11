package node

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/partitions/memory"
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"os"
	"testing"
)

func executionRequest(id string, c *wire.ExecutionCommand) *wire.ExecutionRequest {
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	d := sha256.Sum256(raw)
	return &wire.ExecutionRequest{ProtocolVersion: 1, Partition: "p", OperationId: id, CommandSha256: d[:], Command: c}
}
func TestGoOwnerExecutionRecovery(t *testing.T) {
	raw, e := os.ReadFile("../../proof/execution/case.json")
	if e != nil {
		t.Fatal(e)
	}
	var fixture struct {
		Schema    int      `json:"schema_version"`
		Prefix    string   `json:"prefix"`
		Namespace string   `json:"namespace_id"`
		Runs      []string `json:"runs"`
		Shard     int32    `json:"shard_id"`
		Range     int64    `json:"range_id"`
	}
	if e = json.Unmarshal(raw, &fixture); e != nil || fixture.Schema != 1 || len(fixture.Runs) != 3 {
		t.Fatal(e)
	}
	ctx := context.Background()
	store := memory.New()
	path := fixture.Prefix + "-recovery"
	o := memoryOwner(t, store, path, "p")
	s := &ExecutionServer{Owner: o}
	_, e = o.Run(ctx, func(db partitions.Writer) ([]byte, error) {
		tx, e := db.Begin(context.Background())
		if e != nil {
			return nil, backend(e)
		}
		defer tx.Abort()
		if e = putMessage(tx, "v1/shard/0000000007", &wire.StoredShard{RangeId: fixture.Range}); e != nil {
			return nil, e
		}
		return nil, commitTest(db, tx)
	})
	if e != nil {
		t.Fatal(e)
	}
	// Exercise successful writes, logical aborts, history prewrites and replay
	// through the managed authority path rather than only unmanaged storage.
	o.config.Authority = func(context.Context) error { return nil }
	image := func(run string, version int64) *wire.ExecutionImage {
		state, _ := proto.Marshal(&persistencespb.WorkflowExecutionState{RunId: run, State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED})
		return &wire.ExecutionImage{NamespaceId: fixture.Namespace, WorkflowId: "w", RunId: run, ExecutionStateProto: state, ExecutionInfoBlob: &wire.HistoryBlob{Data: []byte{0, 255}, Encoding: 2}, ExecutionStateBlob: &wire.HistoryBlob{Data: state, Encoding: int32(enumspb.ENCODING_TYPE_PROTO3)}, NextEventId: 11, LastWriteVersion: 9100, DbRecordVersion: version}
	}
	call := func(id string, c *wire.ExecutionCommand) *wire.ExecutionResult {
		t.Helper()
		c.ShardId = fixture.Shard
		c.RangeId = fixture.Range
		r, e := s.Execute(ctx, executionRequest(id, c))
		if e != nil {
			t.Fatal(id, e)
		}
		return r
	}
	missing := &wire.ExecutionCommand{Kind: wire.ExecutionCommand_SET, ShardId: 9999, RangeId: fixture.Range, Snapshot: image(fixture.Runs[0], 1)}
	if result, err := s.Execute(ctx, executionRequest("missing-shard", missing)); err != nil || result.Error != wire.ExecutionResult_UNAVAILABLE || o.Quarantined() {
		t.Fatal(result, err, o.Quarantined())
	}
	first := image(fixture.Runs[0], 1)
	task := &wire.ExecutionTask{CategoryId: 2, CategoryType: 2, TaskId: 1, FireSeconds: -1, FireNanos: 999999999, Blob: &wire.HistoryBlob{Data: []byte{255, 0}, Encoding: 2}}
	task2 := proto.Clone(task).(*wire.ExecutionTask)
	task2.TaskId = 2
	first.Tasks = []*wire.ExecutionTask{task, task2}
	tree := uuid.MustParse(fixture.Runs[1])
	branch := uuid.MustParse(fixture.Runs[2])
	history := &wire.HistoryCommand{Kind: wire.HistoryCommand_APPEND, ShardId: fixture.Shard, TreeId: tree[:], BranchId: branch[:], IsNewBranch: true, TreeInfo: &wire.HistoryBlob{Data: []byte("tree"), Encoding: 2}, Node: &wire.HistoryNodeRecord{NodeId: 1, TransactionId: 17, Events: &wire.HistoryBlob{Data: []byte("events"), Encoding: 2}}}
	create := &wire.ExecutionCommand{Kind: wire.ExecutionCommand_CREATE, Snapshot: first, HistoryPrewrites: []*wire.HistoryCommand{history}}
	r := call("create", create)
	if r.Error != wire.ExecutionResult_NONE {
		t.Fatal(r)
	}
	barrier, err := o.writer.ReadDurable(ctx, partitions.ReadRequest{Keys: [][]byte{[]byte("v1/ownership/read-barrier")}})
	if err != nil || len(barrier.Entries) != 1 || barrier.Entries[0].Value != nil {
		t.Fatal("execution added a redundant post-root barrier", err)
	}
	// Both scheduled IDs persist separately; PostgreSQL microsecond truncation
	// also applies for negative Unix seconds rather than rounding toward epoch.
	_, e = o.Run(ctx, func(db partitions.Writer) ([]byte, error) {
		tx, e := db.Begin(context.Background())
		if e != nil {
			return nil, backend(e)
		}
		defer tx.Abort()
		for _, task := range first.Tasks {
			b, e := get(tx, executionTaskKey(fixture.Shard, task))
			if e != nil {
				return nil, e
			}
			stored := new(wire.ExecutionTask)
			if e = proto.Unmarshal(b, stored); e != nil || stored.FireSeconds != -1 || stored.FireNanos != 999999000 {
				t.Fatal("scheduled normalization", stored, e)
			}
		}
		return nil, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	// Root identity mismatch must not prewrite injected history.
	changed := proto.Clone(create).(*wire.ExecutionCommand)
	changed.HistoryPrewrites[0].Node.NodeId = 2
	_, e = s.Execute(ctx, executionRequest("create", changed))
	if status.Code(e) != codes.InvalidArgument {
		t.Fatal("digest reuse", e)
	}
	// Bypass permits another run, but cannot mutate the current run.
	second := image(fixture.Runs[1], 1)
	if r = call("bypass-create", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_CREATE, Mode: 2, Snapshot: second}); r.Error != wire.ExecutionResult_NONE {
		t.Fatal(r)
	}
	third := image(fixture.Runs[2], 1)
	if r = call("replace-wrong-version", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_CREATE, Mode: 1, PreviousRunId: first.RunId, PreviousLastWriteVersion: 9101, Snapshot: third}); r.Error != wire.ExecutionResult_CURRENT_CONDITION_FAILED {
		t.Fatal(r)
	}
	if r = call("replace-completed", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_CREATE, Mode: 1, PreviousRunId: first.RunId, PreviousLastWriteVersion: 9100, Snapshot: third}); r.Error != wire.ExecutionResult_NONE {
		t.Fatal(r)
	}
	if r = call("bypass-reset", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_CONFLICT_RESOLVE, Mode: 1, Snapshot: image(second.RunId, 2)}); r.Error != wire.ExecutionResult_NONE {
		t.Fatal(r)
	}
	badRange := &wire.ExecutionCommand{Kind: wire.ExecutionCommand_SET, ShardId: fixture.Shard, RangeId: fixture.Range + 1, Snapshot: image(first.RunId, 2)}
	if result, err := s.Execute(ctx, executionRequest("stale-shard", badRange)); err != nil || result.Error != wire.ExecutionResult_OWNERSHIP_LOST || result.ShardId != fixture.Shard {
		t.Fatal(result, err)
	}
	m := &wire.ExecutionMutation{Upsert: image(third.RunId, 2)}
	if r = call("bypass-current", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_UPDATE, Mode: 1, Mutation: m}); r.Error != wire.ExecutionResult_CURRENT_CONDITION_FAILED {
		t.Fatal(r)
	}
	absent := image(first.RunId, 2)
	absent.WorkflowId = "absent-current"
	if r = call("missing-update-current", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_UPDATE, Mutation: &wire.ExecutionMutation{Upsert: absent}}); r.Error != wire.ExecutionResult_UNAVAILABLE {
		t.Fatal(r)
	}
	if r = call("missing-conflict-current", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_CONFLICT_RESOLVE, Snapshot: absent}); r.Error != wire.ExecutionResult_UNAVAILABLE {
		t.Fatal(r)
	}
	for index, name := range []string{"bypass-cross-namespace", "ignore-cross-namespace"} {
		base := image(first.RunId, 1)
		base.WorkflowId = name
		if r = call(name+"-create", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_CREATE, Mode: 2, Snapshot: base}); r.Error != wire.ExecutionResult_NONE {
			t.Fatal(r)
		}
		mutation := image(first.RunId, 2)
		mutation.WorkflowId = name
		next := image(second.RunId, 1)
		next.WorkflowId = name
		next.NamespaceId = third.RunId
		if r = call(name, &wire.ExecutionCommand{Kind: wire.ExecutionCommand_UPDATE, Mode: int32(index + 1), Mutation: &wire.ExecutionMutation{Upsert: mutation}, NewSnapshot: next}); r.Error != wire.ExecutionResult_NONE {
			t.Fatal(r)
		}
	}
	// Legacy NextEventID guard when DBRecordVersion is zero.
	legacy := image(second.RunId, 0)
	legacy.Condition = 11
	legacy.NextEventId = 19
	if r = call("legacy-set", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_SET, Snapshot: legacy}); r.Error != wire.ExecutionResult_NONE {
		t.Fatal(r)
	}
	legacy = proto.Clone(legacy).(*wire.ExecutionImage)
	legacy.Condition = 11
	if r = call("legacy-failed", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_SET, Snapshot: legacy}); r.Error != wire.ExecutionResult_WORKFLOW_CONDITION_FAILED || r.ActualNextEventId != 19 {
		t.Fatal(r)
	}
	// History remains durable when a later mutable-state guard fails.
	failed := &wire.ExecutionCommand{Kind: wire.ExecutionCommand_SET, Snapshot: image(first.RunId, 99), HistoryPrewrites: []*wire.HistoryCommand{proto.Clone(history).(*wire.HistoryCommand)}}
	failed.HistoryPrewrites[0].Node.NodeId = 3
	if r = call("failed-state", failed); r.Error != wire.ExecutionResult_WORKFLOW_CONDITION_FAILED {
		t.Fatal(r)
	}
	// New task insertion followed by duplicate task failure must roll back both
	// the first inserted task and mutable state, without quarantining the owner.
	collision := image(first.RunId, 2)
	fresh := proto.Clone(task).(*wire.ExecutionTask)
	fresh.TaskId = 3
	collision.Tasks = []*wire.ExecutionTask{fresh, task}
	if r = call("task-rollback", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_SET, Snapshot: collision}); r.Error != wire.ExecutionResult_UNAVAILABLE || o.Quarantined() {
		t.Fatal(r, o.Quarantined())
	}
	_, e = o.Run(ctx, func(db partitions.Writer) ([]byte, error) {
		tx, e := db.Begin(context.Background())
		if e != nil {
			return nil, backend(e)
		}
		defer tx.Abort()
		b, e := get(tx, executionTaskKey(fixture.Shard, fresh))
		if e != nil || b != nil {
			t.Fatal("partial task commit", b, e)
		}
		b, e = get(tx, testHistoryNodeKey(fixture.Shard, tree[:], branch[:], failed.HistoryPrewrites[0].Node))
		if e != nil || b == nil {
			t.Fatal("lost history prewrite", e)
		}
		return nil, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	// Delete independently, then replay acknowledged create after engine reopen:
	// neither state nor independently journaled history may be resurrected.
	if r = call("delete", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_DELETE, NamespaceId: first.NamespaceId, WorkflowId: first.WorkflowId, RunId: first.RunId}); r.Error != wire.ExecutionResult_NONE {
		t.Fatal(r)
	}
	_, e = o.Run(ctx, func(db partitions.Writer) ([]byte, error) {
		tx, e := db.Begin(context.Background())
		if e != nil {
			return nil, backend(e)
		}
		defer tx.Abort()
		if e = tx.Delete([]byte(testHistoryNodeKey(fixture.Shard, tree[:], branch[:], history.Node))); e != nil {
			return nil, backend(e)
		}
		return nil, commitTest(db, tx)
	})
	if e != nil {
		t.Fatal(e)
	}
	if e = o.Close(ctx); e != nil {
		t.Fatal(e)
	}
	o = memoryOwner(t, store, path, "p")
	o.config.Authority = func(context.Context) error { return nil }
	s = &ExecutionServer{Owner: o}
	if r = call("create", create); r.Error != wire.ExecutionResult_NONE {
		t.Fatal(r)
	}
	if r = call("failed-state", failed); r.Error != wire.ExecutionResult_WORKFLOW_CONDITION_FAILED {
		t.Fatal("logical error replay changed", r)
	}
	if r = call("get-deleted", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_GET, NamespaceId: first.NamespaceId, WorkflowId: first.WorkflowId, RunId: first.RunId}); r.Error != wire.ExecutionResult_NOT_FOUND {
		t.Fatal(r)
	}
	_, e = o.Run(ctx, func(db partitions.Writer) ([]byte, error) {
		tx, e := db.Begin(context.Background())
		if e != nil {
			return nil, backend(e)
		}
		defer tx.Abort()
		b, e := get(tx, testHistoryNodeKey(fixture.Shard, tree[:], branch[:], history.Node))
		if e != nil || b != nil {
			t.Fatal("replayed prewrite resurrected", e)
		}
		b, e = get(tx, testHistoryNodeKey(fixture.Shard, tree[:], branch[:], failed.HistoryPrewrites[0].Node))
		if e != nil || b == nil {
			t.Fatal("failed-state history missing after reopen", e)
		}
		return nil, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	competitor := memoryWriter(t, store, path)
	defer competitor.Close(context.Background())
	_, e = s.Execute(ctx, executionRequest("create", create))
	if status.Code(e) != codes.Unavailable || !o.Quarantined() {
		t.Fatal("fenced replay", e, o.Quarantined())
	}
}
