package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/proto"
	"testing"
)

// The fixture enforces explicit admission and child completion. Durable maps are
// inspected independently below, rather than using production eligibility logic.
type executionWriter struct {
	*historyWriter
	held, active bool
	begins       int
}
type executionOperation struct {
	w        *executionWriter
	released bool
}
type executionTx struct {
	partitions.Transaction
	w         *executionWriter
	committed bool
}

func (w *executionWriter) BeginOperation(context.Context) (partitions.Operation, error) {
	if w.held {
		return nil, partitions.ErrBusy
	}
	w.held = true
	return &executionOperation{w: w}, nil
}
func (w *executionWriter) Begin(context.Context) (partitions.Transaction, error) {
	return nil, fmt.Errorf("execution bypassed operation admission")
}
func (o *executionOperation) Begin(ctx context.Context) (partitions.Transaction, error) {
	if o.released {
		return nil, partitions.ErrOperationDone
	}
	if o.w.active {
		return nil, partitions.ErrBusy
	}
	tx, err := o.w.historyWriter.Begin(ctx)
	if err != nil {
		return nil, err
	}
	o.w.active = true
	o.w.begins++
	return &executionTx{Transaction: tx, w: o.w}, nil
}
func (o *executionOperation) Release() {
	if o.w.active {
		panic("released before child settled")
	}
	o.released = true
	o.w.held = false
}
func (tx *executionTx) Commit(ctx context.Context) (partitions.CommitReceipt, error) {
	tx.committed = true
	return tx.Transaction.Commit(ctx)
}
func (tx *executionTx) Abort() error {
	if !tx.committed {
		tx.w.active = false
	}
	return tx.Transaction.Abort()
}
func (w *executionWriter) AwaitDurable(ctx context.Context, r partitions.CommitReceipt) error {
	err := w.historyWriter.AwaitDurable(ctx, r)
	w.active = false
	return err
}
func executionRequest(id int, c *wire.ExecutionCommand) *wire.ExecutionRequest {
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	digest := sha256.Sum256(raw)
	return &wire.ExecutionRequest{ProtocolVersion: 1, Partition: "prt_0000000000000000000001", OperationId: fmt.Sprintf("op_%022d", id), Command: c, CommandSha256: digest[:]}
}
func TestExecutionScopedChildrenRollbackAndReplay(t *testing.T) {
	w := &executionWriter{historyWriter: &historyWriter{testWriter: &testWriter{durable: map[string][]byte{}}}}
	seed := func(key string, m proto.Message) {
		raw, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		w.durable[key] = raw
	}
	seed("v1/shard/0000000007", &wire.StoredShard{RangeId: 3})
	ns, run := "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"
	state, _ := proto.Marshal(&persistencespb.WorkflowExecutionState{RunId: run})
	image := &wire.ExecutionImage{NamespaceId: ns, WorkflowId: "workflow", RunId: run, ExecutionStateProto: state, ExecutionInfoBlob: &wire.HistoryBlob{Data: []byte("info")}, DbRecordVersion: 1}
	base, _ := NewService(w, "prt_0000000000000000000001", 1000, func(context.Context) error {
		if !w.held {
			t.Fatal("authority outside admission")
		}
		return nil
	}, func(error) {})
	service, _ := NewExecutionService(base)
	execute := func(id int, c *wire.ExecutionCommand) *wire.ExecutionResult {
		t.Helper()
		r, err := service.Execute(context.Background(), executionRequest(id, c))
		if err != nil {
			t.Fatal(err)
		}
		if w.held || w.active {
			t.Fatal("leaked admission")
		}
		return r
	}
	create := &wire.ExecutionCommand{Kind: wire.ExecutionCommand_CREATE, ShardId: 7, RangeId: 3, Snapshot: image}
	if r := execute(1, create); r.Error != wire.ExecutionResult_NONE {
		t.Fatal(r)
	}
	original := bytes.Clone(w.durable[execKey(7, ns, "workflow", run)])
	duplicate := &wire.ExecutionTask{CategoryId: 1, CategoryType: 1, TaskId: 9, Blob: &wire.HistoryBlob{Data: []byte("existing")}}
	seed(executionTaskKey(7, duplicate), duplicate)
	next := proto.Clone(image).(*wire.ExecutionImage)
	next.DbRecordVersion = 2
	fresh := proto.Clone(duplicate).(*wire.ExecutionTask)
	fresh.TaskId = 10
	next.Tasks = []*wire.ExecutionTask{fresh, duplicate}
	history := &wire.HistoryCommand{Kind: wire.HistoryCommand_APPEND, ShardId: 7, TreeId: bytes.Repeat([]byte{1}, 16), BranchId: bytes.Repeat([]byte{2}, 16), Node: &wire.HistoryNodeRecord{NodeId: 1, TransactionId: 7, Events: &wire.HistoryBlob{Data: []byte("durable child")}}}
	update := &wire.ExecutionCommand{Kind: wire.ExecutionCommand_UPDATE, ShardId: 7, RangeId: 3, Mutation: &wire.ExecutionMutation{Upsert: next}, HistoryPrewrites: []*wire.HistoryCommand{history}}
	before := w.commits
	result := execute(2, update)
	if result.Error != wire.ExecutionResult_UNAVAILABLE || w.commits != before+2 {
		t.Fatal("expected durable history and logical root", result, w.commits-before)
	}
	if !bytes.Equal(original, w.durable[execKey(7, ns, "workflow", run)]) || w.durable[executionTaskKey(7, fresh)] != nil {
		t.Fatal("partial root changes survived rollback")
	}
	childKey := historyNodeKey(7, history.TreeId, history.BranchId, history.Node)
	if w.durable[childKey] == nil || w.durable["v1/outcome/op_0000000000000000000002-h-0"] == nil {
		t.Fatal("history/private journal missing")
	}
	delete(w.durable, childKey)
	before = w.commits
	replayed := execute(2, update)
	if !proto.Equal(result, replayed) || w.commits != before+1 || w.durable[childKey] != nil {
		t.Fatal("replay resurrected history or changed logical result")
	}
}
