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
	"time"
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

func TestDiscoveryExactCandidate(t *testing.T) {
	p := Plan{Schema: 1, Session: uuid.NewString(), Listen: "127.0.0.1:0", Discovery: true, Stage: AfterAwait, TimeoutMS: 1000}
	c, e := New(p)
	if e != nil {
		t.Fatal(e)
	}
	w := Workflow{uuid.NewString(), "real-workflow", uuid.NewString()}
	post := func(path string, value any) int {
		b, _ := json.Marshal(value)
		r := httptest.NewRecorder()
		c.ServeHTTP(r, httptest.NewRequest("POST", "http://local"+path, bytes.NewReader(b)))
		return r.Code
	}
	boot := c.Snapshot().Incarnation
	if post("/watch", map[string]any{"session": p.Session, "incarnation": boot, "watch": w}) != 202 {
		t.Fatal("watch rejected")
	}
	q := &wire.ExecutionRequest{OperationId: "actual-operation", Partition: "history-1", CommandSha256: make([]byte, 32), Command: &wire.ExecutionCommand{Kind: wire.ExecutionCommand_UPDATE, Mutation: &wire.ExecutionMutation{Upsert: &wire.ExecutionImage{NamespaceId: w.Namespace, WorkflowId: w.Workflow, RunId: w.Run}}}}
	var invocation *Invocation
	selectRequest := func() {
		_, _ = c.Interceptor()(context.Background(), q, &grpc.UnaryServerInfo{FullMethod: wire.ExecutionPersistence_Execute_FullMethodName}, func(ctx context.Context, _ any) (any, error) { invocation = FromContext(ctx); return nil, nil })
	}
	q.Command.Mutation.Upsert.RunId = uuid.NewString()
	selectRequest()
	if invocation != nil {
		t.Fatal("wrong run selected")
	}
	q.Command.Mutation.Upsert.RunId = w.Run
	q.Command.Mutation.Upsert.NamespaceId = uuid.NewString()
	selectRequest()
	if invocation != nil {
		t.Fatal("wrong namespace selected")
	}
	q.Command.Mutation.Upsert.NamespaceId = w.Namespace
	q.Command.Mutation.Upsert.WorkflowId = "different"
	selectRequest()
	if invocation != nil {
		t.Fatal("wrong workflow selected")
	}
	q.Command.Mutation.Upsert.WorkflowId = w.Workflow
	q.Command.Mutation.Upsert.RunId = w.Run
	q.Command.Kind = wire.ExecutionCommand_GET
	selectRequest()
	if invocation != nil {
		t.Fatal("read selected")
	}
	q.Command.Kind = wire.ExecutionCommand_UPDATE
	selectRequest()
	if invocation == nil {
		t.Fatal("matching update absent")
	}
	done := make(chan error, 1)
	go func() { done <- invocation.Candidate() }()
	deadline := time.Now().Add(time.Second)
	for c.Snapshot().State != "candidate" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	state := c.Snapshot()
	if state.State != "candidate" {
		t.Fatal("no candidate")
	}
	wrong := state.Selector
	wrong.Digest = strings.Repeat("11", 32)
	arm := func(selector Selector) int {
		return post("/arm", map[string]any{"session": p.Session, "incarnation": boot, "selector": selector})
	}
	if arm(wrong) != 409 || arm(state.Selector) != 202 || arm(state.Selector) != 409 {
		t.Fatal("candidate ARM not exact one shot")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if c.Snapshot().State != "armed" {
		t.Fatal("candidate not advanced")
	}
}

func TestDiscoveryLateArmRejected(t *testing.T) {
	c, _ := New(Plan{Schema: 1, Session: uuid.NewString(), Listen: "127.0.0.1:0", Discovery: true, Stage: BeforeAwait, TimeoutMS: 1})
	c.state.State = "watching"
	i := &Invocation{controller: c, selector: Selector{"real", "history-0", "execution", "UPDATE", strings.Repeat("00", 32)}}
	if e := i.Candidate(); e != ErrTimeout {
		t.Fatal(e)
	}
	b, _ := json.Marshal(map[string]any{"session": c.state.Session, "incarnation": c.state.Incarnation, "selector": i.selector})
	r := httptest.NewRecorder()
	c.ServeHTTP(r, httptest.NewRequest("POST", "http://local/arm", bytes.NewReader(b)))
	if r.Code != 409 || c.Snapshot().State != "timed_out" {
		t.Fatal("late ARM revived expired candidate")
	}
}
