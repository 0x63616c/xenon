package slatedb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	p "github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/persistence"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/proto"
)

type pauseDurabilityWriter struct {
	p.Writer
	entered, release chan struct{}
	pause            bool
}

func (w *pauseDurabilityWriter) AwaitDurable(ctx context.Context, r p.CommitReceipt) error {
	err := w.Writer.AwaitDurable(ctx, r)
	if err == nil && w.pause {
		w.pause = false
		close(w.entered)
		<-w.release
	}
	return err
}
func TestNativeExecutionServiceKeepsOperationAcrossHistoryAndRoot(t *testing.T) {
	_, open := setup(t)
	w := open("db", false)
	paused := &pauseDurabilityWriter{Writer: w, entered: make(chan struct{}), release: make(chan struct{})}
	base, err := persistence.NewService(paused, "prt_0000000000000000000001", 1000, func(context.Context) error { return nil }, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	service, err := persistence.NewExecutionService(base)
	if err != nil {
		t.Fatal(err)
	}
	request := func(id int, c *wire.ExecutionCommand) *wire.ExecutionRequest {
		raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		digest := sha256.Sum256(raw)
		return &wire.ExecutionRequest{ProtocolVersion: 1, Partition: "prt_0000000000000000000001", OperationId: fmt.Sprintf("op_%022d", id), Command: c, CommandSha256: digest[:]}
	}
	execute := func(id int, c *wire.ExecutionCommand) *wire.ExecutionResult {
		t.Helper()
		r, err := service.Execute(context.Background(), request(id, c))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	tx := begin(t, w)
	shard, _ := proto.Marshal(&wire.StoredShard{RangeId: 3})
	if err = tx.Put([]byte("v1/shard/0000000007"), shard); err != nil {
		t.Fatal(err)
	}
	finish(t, w, tx)
	ns, run := "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"
	state, _ := proto.Marshal(&persistencespb.WorkflowExecutionState{RunId: run})
	duplicate := &wire.ExecutionTask{CategoryId: 1, CategoryType: 1, TaskId: 9, Blob: &wire.HistoryBlob{Data: []byte("existing")}}
	image := &wire.ExecutionImage{NamespaceId: ns, WorkflowId: "workflow", RunId: run, ExecutionStateProto: state, ExecutionInfoBlob: &wire.HistoryBlob{Data: []byte("info")}, DbRecordVersion: 1, Tasks: []*wire.ExecutionTask{duplicate}}
	if r := execute(1, &wire.ExecutionCommand{Kind: wire.ExecutionCommand_CREATE, ShardId: 7, RangeId: 3, Snapshot: image}); r.Error != wire.ExecutionResult_NONE {
		t.Fatal(r)
	}
	key := persistence.ExecutionKey(7, ns, "workflow", run)
	original := read(t, w, key)
	next := proto.Clone(image).(*wire.ExecutionImage)
	next.DbRecordVersion = 2
	fresh := proto.Clone(duplicate).(*wire.ExecutionTask)
	fresh.TaskId = 10
	next.Tasks = []*wire.ExecutionTask{fresh, duplicate}
	history := &wire.HistoryCommand{Kind: wire.HistoryCommand_APPEND, ShardId: 7, TreeId: bytes.Repeat([]byte{1}, 16), BranchId: bytes.Repeat([]byte{2}, 16), Node: &wire.HistoryNodeRecord{NodeId: 1, TransactionId: 7, Events: &wire.HistoryBlob{Data: []byte("durable child")}}}
	update := &wire.ExecutionCommand{Kind: wire.ExecutionCommand_UPDATE, ShardId: 7, RangeId: 3, Mutation: &wire.ExecutionMutation{Upsert: next}, HistoryPrewrites: []*wire.HistoryCommand{history}}
	paused.pause = true
	type response struct {
		result *wire.ExecutionResult
		err    error
	}
	done := make(chan response, 1)
	go func() { r, err := service.Execute(context.Background(), request(2, update)); done <- response{r, err} }()
	<-paused.entered // real native child is durable; root transaction has not begun
	assertAdmissionHeld(t, w)
	close(paused.release)
	completed := <-done
	if completed.err != nil || completed.result.Error != wire.ExecutionResult_UNAVAILABLE {
		t.Fatal(completed)
	}
	if got := read(t, w, key); got != original {
		t.Fatal("root image changed on task conflict")
	}
	if got := read(t, w, persistence.ExecutionTaskKey(7, fresh)); got != "" {
		t.Fatal("partial task survived root rollback")
	}
	childKey := fmt.Sprintf("v1/history/node/%010d/%x/%x/%016x/%016x", 7, history.TreeId, history.BranchId, uint64(1)^1<<63, ^(uint64(7) ^ 1<<63))
	if read(t, w, childKey) == "" || read(t, w, "v1/outcome/op_0000000000000000000002-h-0") == "" {
		t.Fatal("durable child/private journal missing")
	}
	tx = begin(t, w)
	if err = tx.Delete([]byte(childKey)); err != nil {
		t.Fatal(err)
	}
	finish(t, w, tx)
	replayed := execute(2, update)
	if !proto.Equal(replayed, completed.result) || read(t, w, childKey) != "" {
		t.Fatal("root replay recreated history")
	}
	if err = w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered := open("db", false)
	if read(t, recovered, key) != original || read(t, recovered, childKey) != "" || read(t, recovered, persistence.ExecutionTaskKey(7, fresh)) != "" {
		t.Fatal("recovery changed transaction boundaries")
	}
}
