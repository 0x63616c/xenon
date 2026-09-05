package processcut

import (
	"bytes"
	"context"
	"encoding/json"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCutIdentityAndOneShot(t *testing.T) {
	p := Plan{Schema: 1, Session: uuid.NewString(), Listen: "127.0.0.1:0", Selector: Selector{"target", "history-0", "shard", "UPDATE", strings.Repeat("00", 32)}, Stage: BeforeAwait, TimeoutMS: 1}
	c, e := New(p)
	if e != nil {
		t.Fatal(e)
	}
	arm := func(boot string) int {
		b, _ := json.Marshal(map[string]string{"session": p.Session, "incarnation": boot})
		w := httptest.NewRecorder()
		c.ServeHTTP(w, httptest.NewRequest("POST", "http://local/arm", bytes.NewReader(b)))
		return w.Code
	}
	if arm(uuid.NewString()) != http.StatusConflict {
		t.Fatal("stale incarnation armed")
	}
	if arm(c.Snapshot().Incarnation) != 202 || arm(c.Snapshot().Incarnation) != 409 {
		t.Fatal("not one shot")
	}
	var cut *Invocation
	handler := func(ctx context.Context, _ any) (any, error) { cut = FromContext(ctx); return nil, nil }
	q := &wire.ShardRequest{OperationId: "target", Partition: "history-0", CommandSha256: make([]byte, 32), Command: &wire.ShardCommand{Kind: wire.ShardCommand_GET}}
	interceptor := c.Interceptor()
	_, _ = interceptor(context.Background(), q, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, handler)
	if cut != nil {
		t.Fatal("read selected")
	}
	q.Command.Kind = wire.ShardCommand_UPDATE
	q.OperationId = "wrong"
	_, _ = interceptor(context.Background(), q, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, handler)
	if cut != nil {
		t.Fatal("wrong ID selected")
	}
	q.OperationId = "target"
	_, _ = interceptor(context.Background(), q, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, handler)
	if cut == nil {
		t.Fatal("exact selector missing")
	}
	if e = cut.Stage(AfterAwait); e != nil || c.Snapshot().State != "armed" {
		t.Fatal("wrong stage fired")
	}
	if e = cut.Stage(BeforeAwait); e != ErrTimeout || c.Snapshot().State != "timed_out" {
		t.Fatal("timeout not observed", e)
	}
	if e = cut.Stage(BeforeAwait); e != nil {
		t.Fatal("repeated hit triggered")
	}
	restarted, _ := New(p)
	if restarted.Snapshot().State != "unarmed" || restarted.Snapshot().Incarnation == c.Snapshot().Incarnation {
		t.Fatal("restart retained arm")
	}
}
