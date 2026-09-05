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
