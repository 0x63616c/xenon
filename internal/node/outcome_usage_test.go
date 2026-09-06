package node

import (
	"context"
	"encoding/binary"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/google/uuid"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
	"testing"
)

func capacityOwner(t *testing.T, db *native.Db, limit uint64) *Owner {
	t.Helper()
	c := DefaultConfig("p")
	c.MaxOutcomes = limit
	o, e := NewOwner(db, c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { closeOwner(t, o) })
	return o
}
func TestGoOwnerOutcomeCapacity(t *testing.T) {
	ctx := context.Background()
	store := objects(t)
	o := capacityOwner(t, engine(t, store, "capacity", false), 2)
	read := request("read", &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: 7})
	first, e := o.Execute(ctx, read)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = o.Execute(ctx, request("create", &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: 7, RangeId: 9, Data: []byte("data"), Encoding: 1})); e != nil {
		t.Fatal(e)
	}
	if _, e = o.Execute(ctx, request("exhausted", &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: 7})); status.Code(e) != codes.ResourceExhausted || o.Quarantined() {
		t.Fatal(e)
	}
	replay, e := o.Execute(ctx, read)
	if e != nil || !proto.Equal(replay, first) {
		t.Fatal(replay, e)
	}
	usage, e := o.OutcomeUsage(ctx)
	if e != nil || usage.Entries != 2 || usage.Remaining != 0 || !usage.AccountingComplete || usage.Families["shard_result"].Entries != 2 {
		t.Fatal(usage, e)
	}
	_, e = o.Run(ctx, func(db *native.Db) ([]byte, error) {
		tx, e := db.Begin(native.IsolationLevelSerializableSnapshot)
		if e != nil {
			return nil, e
		}
		defer tx.Destroy()
		var total uint64
		e = historyScan(tx, "v1/outcome/", false, func(_, v []byte) (bool, error) { total += uint64(len(v)); return false, nil })
		if total != usage.EncodedOutcomeBytes {
			t.Fatal("encoded byte accounting mismatch", total, usage)
		}
		return nil, e
	})
	if e != nil {
		t.Fatal(e)
	}
	if e = o.Close(ctx); e != nil {
		t.Fatal(e)
	}
	o = capacityOwner(t, engine(t, store, "capacity", false), 2)
	after, e := o.OutcomeUsage(ctx)
	if e != nil || after.Entries != usage.Entries || after.EncodedOutcomeBytes != usage.EncodedOutcomeBytes {
		t.Fatal(after, e)
	}
	if _, e = o.Execute(ctx, request("new-after-reopen", &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: 7})); status.Code(e) != codes.ResourceExhausted {
		t.Fatal(e)
	}
	if _, e = o.Execute(ctx, read); e != nil {
		t.Fatal(e)
	}
}
func TestGoOwnerOutcomeLegacy(t *testing.T) {
	ctx := context.Background()
	o := capacityOwner(t, engine(t, objects(t), "legacy-accounting", false), 5)
	_, e := o.Run(ctx, func(db *native.Db) ([]byte, error) {
		tx, e := db.Begin(native.IsolationLevelSerializableSnapshot)
		if e != nil {
			return nil, e
		}
		defer tx.Destroy()
		count := make([]byte, 8)
		binary.BigEndian.PutUint64(count, 1)
		if e = put(tx, "v1/outcome_count", count); e != nil {
			return nil, e
		}
		if e = putHistory(tx, "v1/outcome/old", &wire.StoredOutcome{Result: &wire.StoredOutcome_ShardResult{ShardResult: &wire.ShardResult{}}}); e != nil {
			return nil, e
		}
		return nil, commit(tx)
	})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = o.Execute(ctx, request("new", &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: 7})); e != nil {
		t.Fatal(e)
	}
	u, e := o.OutcomeUsage(ctx)
	if e != nil || u.AccountingComplete || u.UnaccountedLegacyEntries != 1 || u.Entries != 2 || u.Families["shard_result"].Entries != 1 {
		t.Fatal(u, e)
	}
}
func TestGoOwnerOutcomePrewriteCapacity(t *testing.T) {
	ctx := context.Background()
	store := objects(t)
	o := capacityOwner(t, engine(t, store, "prewrite-capacity", false), 1)
	_, e := o.Run(ctx, func(db *native.Db) ([]byte, error) {
		tx, e := db.Begin(native.IsolationLevelSerializableSnapshot)
		if e != nil {
			return nil, e
		}
		defer tx.Destroy()
		if e = putHistory(tx, "v1/shard/0000000007", &wire.StoredShard{RangeId: 9}); e != nil {
			return nil, e
		}
		return nil, commit(tx)
	})
	if e != nil {
		t.Fatal(e)
	}
	run := "00000000-0000-0000-0000-000000000001"
	state, _ := proto.Marshal(&persistencespb.WorkflowExecutionState{RunId: run, State: 1, Status: 1})
	id := uuid.MustParse(run)
	c := &wire.ExecutionCommand{Kind: wire.ExecutionCommand_CREATE, ShardId: 7, RangeId: 9, Snapshot: &wire.ExecutionImage{NamespaceId: run, WorkflowId: "capacity", RunId: run, ExecutionStateProto: state, ExecutionInfoBlob: &wire.HistoryBlob{Data: []byte("info"), Encoding: 2}, ExecutionStateBlob: &wire.HistoryBlob{Data: state, Encoding: 1}, NextEventId: 3, DbRecordVersion: 1}, HistoryPrewrites: []*wire.HistoryCommand{{Kind: wire.HistoryCommand_APPEND, ShardId: 7, TreeId: id[:], BranchId: id[:], IsNewBranch: true, TreeInfo: &wire.HistoryBlob{Data: []byte("tree"), Encoding: 2}, Node: &wire.HistoryNodeRecord{NodeId: 1, TransactionId: 7, Events: &wire.HistoryBlob{Data: []byte("history"), Encoding: 2}}}}}
	q := executionRequest("root", c)
	if _, e = (&ExecutionServer{Owner: o}).Execute(ctx, q); status.Code(e) != codes.ResourceExhausted || o.Quarantined() {
		t.Fatal(e)
	}
	u, e := o.OutcomeUsage(ctx)
	if e != nil || u.Entries != 1 || u.Families["history_result"].Entries != 1 || u.Families["execution_result"].Entries != 0 {
		t.Fatal(u, e)
	}
	if e = o.Close(ctx); e != nil {
		t.Fatal(e)
	}
	o = capacityOwner(t, engine(t, store, "prewrite-capacity", false), 2)
	result, e := (&ExecutionServer{Owner: o}).Execute(ctx, q)
	if e != nil || result.Error != wire.ExecutionResult_NONE {
		t.Fatal(result, e)
	}
	u, e = o.OutcomeUsage(ctx)
	if e != nil || u.Entries != 2 || u.Families["history_result"].Entries != 1 || u.Families["execution_result"].Entries != 1 {
		t.Fatal(u, e)
	}
	if _, e = (&ExecutionServer{Owner: o}).Execute(ctx, q); e != nil {
		t.Fatal(e)
	}
}
