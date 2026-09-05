// Package processcut exposes default-off, one-shot local fault barriers. It never
// changes a persistence result or pretends to perform a process failure.
package processcut

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/proof/s3meter"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	BeforeAwait = "commit_before_await"
	AfterAwait  = "after_await"
	BeforeReply = "before_result_publication"
)

var ErrTimeout = errors.New("process-cut barrier timed out")

type Selector struct {
	OperationID string `json:"operation_id"`
	Partition   string `json:"partition"`
	Family      string `json:"family"`
	Kind        string `json:"mutation_kind"`
	Digest      string `json:"command_sha256"`
}
type Plan struct {
	Schema    int      `json:"schema"`
	Session   string   `json:"session"`
	Listen    string   `json:"listen"`
	Selector  Selector `json:"selector"`
	Stage     string   `json:"stage"`
	TimeoutMS int      `json:"timeout_ms"`
}
type State struct {
	Schema      int      `json:"schema"`
	Session     string   `json:"session"`
	Incarnation string   `json:"incarnation"`
	PID         int      `json:"pid"`
	State       string   `json:"state"`
	Stage       string   `json:"stage"`
	Selector    Selector `json:"selector"`
	HitUTC      string   `json:"hit_utc,omitempty"`
}
type Controller struct {
	plan  Plan
	mu    sync.Mutex
	state State
}

func New(plan Plan) (*Controller, error) {
	if plan.Schema != 1 || plan.TimeoutMS < 1 || plan.TimeoutMS > 5000 {
		return nil, errors.New("invalid process-cut bounds")
	}
	if _, e := uuid.Parse(plan.Session); e != nil {
		return nil, errors.New("invalid process-cut session")
	}
	if e := s3meter.LoopbackAddress(plan.Listen); e != nil {
		return nil, e
	}
	s := plan.Selector
	if s.OperationID == "" || len(s.OperationID) > 128 || s.Partition == "" || len(s.Partition) > 128 || s.Kind != "UPDATE" || (s.Family != "shard" && s.Family != "execution") {
		return nil, errors.New("unsupported exact mutation selector")
	}
	if b, e := hex.DecodeString(s.Digest); e != nil || len(b) != 32 {
		return nil, errors.New("invalid command digest")
	}
	if plan.Stage != BeforeAwait && plan.Stage != AfterAwait && plan.Stage != BeforeReply {
		return nil, errors.New("invalid process-cut stage")
	}
	return &Controller{plan: plan, state: State{Schema: 1, Session: plan.Session, Incarnation: uuid.NewString(), PID: os.Getpid(), State: "unarmed", Stage: plan.Stage, Selector: s}}, nil
}
func (c *Controller) Snapshot() State { c.mu.Lock(); defer c.mu.Unlock(); return c.state }
func (c *Controller) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == "GET" && r.URL.Path == "/state" {
		_ = json.NewEncoder(w).Encode(c.Snapshot())
		return
	}
	if r.Method != "POST" || r.URL.Path != "/arm" {
		http.NotFound(w, r)
		return
	}
	var arm struct {
		Session     string `json:"session"`
		Incarnation string `json:"incarnation"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	decoder.DisallowUnknownFields()
	if e := decoder.Decode(&arm); e != nil {
		http.Error(w, "invalid arm identity", 400)
		return
	}
	if e := decoder.Decode(new(any)); e != io.EOF {
		http.Error(w, "trailing arm data", 400)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if arm.Session != c.state.Session || arm.Incarnation != c.state.Incarnation || c.state.State != "unarmed" {
		http.Error(w, "stale or consumed arm", 409)
		return
	}
	c.state.State = "armed"
	w.WriteHeader(202)
}

type contextKey struct{}
type Invocation struct {
	controller *Controller
	selector   Selector
}

func FromContext(ctx context.Context) *Invocation {
	v, _ := ctx.Value(contextKey{}).(*Invocation)
	return v
}
func (i *Invocation) Matches(id, family, partition string) bool {
	return i != nil && i.selector.OperationID == id && i.selector.Family == family && i.selector.Partition == partition
}
func (i *Invocation) Stage(stage string) error {
	if i == nil || i.controller.plan.Stage != stage {
		return nil
	}
	c := i.controller
	c.mu.Lock()
	if c.state.State != "armed" {
		c.mu.Unlock()
		return nil
	}
	c.state.State = "paused"
	c.state.HitUTC = time.Now().UTC().Format(time.RFC3339Nano)
	c.mu.Unlock()
	// No in-process resume or synthetic success: external supervision kills this
	// exact incarnation. A missed kill reaches the bounded failure path.
	timer := time.NewTimer(time.Duration(c.plan.TimeoutMS) * time.Millisecond)
	defer timer.Stop()
	<-timer.C
	c.mu.Lock()
	c.state.State = "timed_out"
	c.mu.Unlock()
	return ErrTimeout
}
func (c *Controller) Interceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		var selected Selector
		switch q := req.(type) {
		case *wire.ShardRequest:
			if info.FullMethod != wire.ShardPersistence_Execute_FullMethodName || q.GetCommand().GetKind() != wire.ShardCommand_UPDATE {
				return handler(ctx, req)
			}
			selected = Selector{q.OperationId, q.Partition, "shard", "UPDATE", hex.EncodeToString(q.CommandSha256)}
		case *wire.ExecutionRequest:
			if info.FullMethod != wire.ExecutionPersistence_Execute_FullMethodName || q.GetCommand().GetKind() != wire.ExecutionCommand_UPDATE {
				return handler(ctx, req)
			}
			selected = Selector{q.OperationId, q.Partition, "execution", "UPDATE", hex.EncodeToString(q.CommandSha256)}
		default:
			return handler(ctx, req)
		}
		if selected == c.plan.Selector {
			ctx = context.WithValue(ctx, contextKey{}, &Invocation{c, selected})
		}
		return handler(ctx, req)
	}
}
