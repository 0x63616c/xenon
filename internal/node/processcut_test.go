package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/processcut"
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"net/http/httptest"
	native "slatedb.io/slatedb-go/uniffi"
	"testing"
	"time"
)

func TestGoOwnerProcessCutTimeout(t *testing.T) {
	for _, stage := range []string{processcut.BeforeAwait, processcut.AfterAwait, processcut.BeforeReply} {
		t.Run(stage, func(t *testing.T) {
			o, e := NewOwner(engine(t, objects(t), "cut-timeout-"+stage, false), DefaultConfig("history-0"))
			if e != nil {
				t.Fatal(e)
			}
			defer o.Close(context.Background())
			request := func(id string, c *wire.ShardCommand) *wire.ShardRequest {
				b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
				h := sha256.Sum256(b)
				return &wire.ShardRequest{ProtocolVersion: 1, Partition: "history-0", OperationId: id, CommandSha256: h[:], Command: c}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, e = o.Execute(ctx, request("init", &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: 1, RangeId: 1, Data: []byte("before")})); e != nil {
				t.Fatal(e)
			}
			q := request("target", &wire.ShardCommand{Kind: wire.ShardCommand_UPDATE, ShardId: 1, PreviousRangeId: 1, RangeId: 2, Data: []byte("after")})
			cut, e := processcut.New(processcut.Plan{Schema: 1, Session: uuid.NewString(), Listen: "127.0.0.1:0", Selector: processcut.Selector{OperationID: q.OperationId, Partition: q.Partition, Family: "shard", Kind: "UPDATE", Digest: hex.EncodeToString(q.CommandSha256)}, Stage: stage, TimeoutMS: 5})
			if e != nil {
				t.Fatal(e)
			}
			b, _ := json.Marshal(map[string]string{"session": cut.Snapshot().Session, "incarnation": cut.Snapshot().Incarnation})
			w := httptest.NewRecorder()
			cut.ServeHTTP(w, httptest.NewRequest("POST", "http://local/arm", bytes.NewReader(b)))
			_, e = cut.Interceptor()(ctx, q, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, func(ctx context.Context, r any) (any, error) { return o.Execute(ctx, r.(*wire.ShardRequest)) })
			if status.Code(e) != codes.Unavailable || !o.Quarantined() || cut.Snapshot().State != "timed_out" {
				t.Fatal("timeout did not retire owner", e, cut.Snapshot())
			}
			if _, e = o.Execute(ctx, q); status.Code(e) != codes.Unavailable {
				t.Fatal("quarantined replay admitted", e)
			}
		})
	}
}

func TestGoOwnerProcessCutRetainsNativeWait(t *testing.T) {
	objects := objects(t)
	initial, e := NewOwner(engine(t, objects, "cut-retained", false), DefaultConfig("history-0"))
	if e != nil {
		t.Fatal(e)
	}
	request := func(id string, c *wire.ShardCommand) *wire.ShardRequest {
		b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		h := sha256.Sum256(b)
		return &wire.ShardRequest{ProtocolVersion: 1, Partition: "history-0", OperationId: id, CommandSha256: h[:], Command: c}
	}
	if _, e = initial.Execute(context.Background(), request("init", &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: 1, RangeId: 1})); e != nil {
		t.Fatal(e)
	}
	if e = initial.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
	db := engine(t, objects, "cut-retained", true)
	config := DefaultConfig("history-0")
	config.OperationTimeout = 40 * time.Millisecond
	config.AdmissionTimeout = 10 * time.Millisecond
	o, e := NewOwner(db, config)
	if e != nil {
		t.Fatal(e)
	}
	q := request("target", &wire.ShardCommand{Kind: wire.ShardCommand_UPDATE, ShardId: 1, PreviousRangeId: 1, RangeId: 2})
	cut, e := processcut.New(processcut.Plan{Schema: 1, Session: uuid.NewString(), Listen: "127.0.0.1:0", Selector: processcut.Selector{OperationID: q.OperationId, Partition: q.Partition, Family: "shard", Kind: "UPDATE", Digest: hex.EncodeToString(q.CommandSha256)}, Stage: processcut.BeforeAwait, TimeoutMS: 5})
	if e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(map[string]string{"session": cut.Snapshot().Session, "incarnation": cut.Snapshot().Incarnation})
	cut.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "http://local/arm", bytes.NewReader(b)))
	_, e = cut.Interceptor()(context.Background(), q, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, func(ctx context.Context, r any) (any, error) { return o.Execute(ctx, r.(*wire.ShardRequest)) })
	if status.Code(e) != codes.Unavailable || !o.Quarantined() || o.ActiveNativeOperations() != 1 || cut.Snapshot().State != "timed_out" {
		t.Fatal("native wait was not retained after cut timeout", e)
	}
	if e = o.Close(context.Background()); e == nil || o.destroyed.Load() {
		t.Fatal("close freed an active native wait")
	}
	// Test-only flush releases the real blocked AwaitDurable, not the cut barrier.
	if e = db.FlushWithOptions(native.FlushOptions{FlushType: native.FlushTypeWal}); e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(time.Second)
	for o.ActiveNativeOperations() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if o.ActiveNativeOperations() != 0 || !o.Quarantined() {
		t.Fatal("drain reopened or failed to complete")
	}
	if e = o.Close(context.Background()); e != nil {
		t.Fatal(e)
	}
}

func TestGoOwnerDiscoveryAbortsCandidate(t *testing.T) {
	ctx := context.Background()
	store := objects(t)
	prefix := "discovery-abort"
	o := owner(t, engine(t, store, prefix, false))
	server := &ExecutionServer{Owner: o}
	ns, run := uuid.NewString(), uuid.NewString()
	state, _ := proto.Marshal(&persistencespb.WorkflowExecutionState{RunId: run, State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING})
	image := &wire.ExecutionImage{NamespaceId: ns, WorkflowId: "watched", RunId: run, ExecutionStateProto: state, ExecutionInfoBlob: &wire.HistoryBlob{Data: []byte("before"), Encoding: 2}, ExecutionStateBlob: &wire.HistoryBlob{Data: state, Encoding: 3}, NextEventId: 2, DbRecordVersion: 1}
	_, e := o.Run(ctx, func(db *native.Db) ([]byte, error) {
		tx, e := db.Begin(native.IsolationLevelSerializableSnapshot)
		if e != nil {
			return nil, e
		}
		defer tx.Destroy()
		if e = putHistory(tx, "v1/shard/0000000001", &wire.StoredShard{RangeId: 1}); e != nil {
			return nil, e
		}
		return nil, commit(tx)
	})
	if e != nil {
		t.Fatal(e)
	}
	initial := executionRequest("initial", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_CREATE, ShardId: 1, RangeId: 1, Snapshot: image})
	if r, e := server.Execute(ctx, initial); e != nil || r.Error != wire.ExecutionResult_NONE {
		t.Fatal(r, e)
	}
	cut, e := processcut.New(processcut.Plan{Schema: 1, Session: uuid.NewString(), Listen: "127.0.0.1:0", Discovery: true, Stage: processcut.BeforeAwait, TimeoutMS: 5})
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(map[string]any{"session": cut.Snapshot().Session, "incarnation": cut.Snapshot().Incarnation, "watch": processcut.Workflow{Namespace: ns, Workflow: "watched", Run: run}})
	w := httptest.NewRecorder()
	cut.ServeHTTP(w, httptest.NewRequest("POST", "http://local/watch", bytes.NewReader(raw)))
	if w.Code != 202 {
		t.Fatal(w.Code)
	}
	call := func(q *wire.ExecutionRequest) (*wire.ExecutionResult, error) {
		r, e := cut.Interceptor()(ctx, q, &grpc.UnaryServerInfo{FullMethod: wire.ExecutionPersistence_Execute_FullMethodName}, func(ctx context.Context, r any) (any, error) { return server.Execute(ctx, r.(*wire.ExecutionRequest)) })
		if r == nil {
			return nil, e
		}
		return r.(*wire.ExecutionResult), e
	}
	// A successfully journaled replay is never offered, and a failed range guard
	// leaves discovery watching for the real successful root mutation.
	if r, e := call(initial); e != nil || r.Error != wire.ExecutionResult_NONE || cut.Snapshot().State != "watching" {
		t.Fatal(r, e)
	}
	next := proto.Clone(image).(*wire.ExecutionImage)
	next.DbRecordVersion = 2
	next.ExecutionInfoBlob.Data = []byte("after")
	command := &wire.ExecutionCommand{Kind: wire.ExecutionCommand_UPDATE, ShardId: 1, RangeId: 999, Mode: 2, Mutation: &wire.ExecutionMutation{Upsert: next}}
	if r, e := call(executionRequest("failed", command)); e != nil || r.Error != wire.ExecutionResult_OWNERSHIP_LOST || cut.Snapshot().State != "watching" {
		t.Fatal(r, e)
	}
	command.RangeId = 1
	if _, e := call(executionRequest("candidate", command)); status.Code(e) != codes.Unavailable || !o.Quarantined() || cut.Snapshot().State != "timed_out" {
		t.Fatal(e, cut.Snapshot())
	}
	if e = o.Close(ctx); e != nil {
		t.Fatal(e)
	}
	reopened := owner(t, engine(t, store, prefix, false))
	defer reopened.Close(ctx)
	_, e = reopened.Run(ctx, func(db *native.Db) ([]byte, error) {
		tx, e := db.Begin(native.IsolationLevelSerializableSnapshot)
		if e != nil {
			return nil, e
		}
		defer tx.Destroy()
		r, e := loadImage(tx, execKey(1, ns, "watched", run))
		if e != nil {
			return nil, e
		}
		if r.DbRecordVersion != 1 || string(r.ExecutionInfoBlob.Data) != "before" {
			t.Fatal("uncommitted candidate changed state", r)
		}
		b, e := get(tx, "v1/outcome/candidate")
		if e != nil {
			return nil, e
		}
		if b != nil {
			t.Fatal("aborted candidate outcome persisted")
		}
		return nil, nil
	})
	if e != nil {
		t.Fatal(e)
	}
}
