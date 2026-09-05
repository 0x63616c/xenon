// Package rpctrace records optional bounded RPC measurements without payloads.
package rpctrace

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type Event struct {
	Kind        string `json:"kind"`
	ID          string `json:"invocation_id,omitempty"`
	Family      string `json:"family,omitempty"`
	Method      string `json:"method,omitempty"`
	Partition   string `json:"partition,omitempty"`
	OperationID string `json:"operation_id,omitempty"`
	Status      string `json:"status,omitempty"`
	Started     string `json:"started,omitempty"`
	Duration    int64  `json:"duration_ns,omitempty"`
}

type Sink struct {
	queue  chan Event
	done   chan struct{}
	failed atomic.Bool
	mu     sync.RWMutex
	closed bool
}

func NewSink(w io.Writer, capacity int) *Sink {
	s := &Sink{queue: make(chan Event, capacity), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		if closer, ok := w.(io.Closer); ok {
			defer closer.Close()
		}
		enc := json.NewEncoder(w)
		for e := range s.queue {
			if enc.Encode(e) != nil {
				s.failed.Store(true)
			}
		}
		if s.failed.Load() {
			_ = enc.Encode(Event{Kind: "trace_error", Status: "overflow_or_write_failure"})
		}
		_ = enc.Encode(Event{Kind: "trace_end", Status: fmt.Sprint(!s.failed.Load())})
	}()
	return s
}
func (s *Sink) emit(e Event) {
	if s == nil {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		s.failed.Store(true)
		return
	}
	select {
	case s.queue <- e:
	default:
		s.failed.Store(true)
	}
}
func (s *Sink) Close(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.queue)
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		if s.failed.Load() {
			return fmt.Errorf("RPC trace incomplete")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var once sync.Once
var global *Sink

func configured() *Sink {
	once.Do(func() {
		path := os.Getenv("XENON_RPC_TRACE_PATH")
		if path == "" {
			return
		}
		f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			panic("RPC trace output cannot be created")
		}
		global = NewSink(f, 65536)
	})
	return global
}

// Close drains the trace. A missing successful trace_end means incomplete evidence.
func Close(ctx context.Context) error {
	s := configured()
	if s == nil {
		return nil
	}
	return s.Close(ctx)
}

type key struct{}
type invocation struct {
	sink  *Sink
	event Event
	start time.Time
}

type sinkKey struct{}

// WithSink scopes a recorder to a caller, including deterministic proof controls.
func WithSink(ctx context.Context, sink *Sink) context.Context {
	return context.WithValue(ctx, sinkKey{}, sink)
}
func Begin(ctx context.Context, family string) (context.Context, func(error)) {
	if sink, ok := ctx.Value(sinkKey{}).(*Sink); ok {
		return begin(ctx, family, sink)
	}
	return begin(ctx, family, configured())
}
func begin(ctx context.Context, family string, s *Sink) (context.Context, func(error)) {
	if s == nil {
		return ctx, func(error) {}
	}
	start := time.Now()
	v := &invocation{sink: s, start: start, event: Event{Kind: "rpc_invocation", ID: uuid.NewString(), Family: family, Started: start.UTC().Format(time.RFC3339Nano)}}
	return context.WithValue(ctx, key{}, v), func(e error) {
		v.event.Duration = time.Since(start).Nanoseconds()
		v.event.Status = status.Code(e).String()
		s.emit(v.event)
	}
}

// Unary is installed on adapter-owned client connections. Forwarded server hops
// are not adapter attempts and are deliberately outside this observer.
func Unary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	v, _ := ctx.Value(key{}).(*invocation)
	if v == nil {
		return invoke(ctx, method, req, reply, cc, opts...)
	}
	v.event.Method = method
	if q, ok := req.(interface {
		GetPartition() string
		GetOperationId() string
	}); ok {
		v.event.Partition = q.GetPartition()
		v.event.OperationID = q.GetOperationId()
	}
	start := time.Now()
	e := invoke(ctx, method, req, reply, cc, opts...)
	event := v.event
	event.Kind = "rpc_attempt"
	event.Started = start.UTC().Format(time.RFC3339Nano)
	event.Duration = time.Since(start).Nanoseconds()
	event.Status = status.Code(e).String()
	v.sink.emit(event)
	return e
}
