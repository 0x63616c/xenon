package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/rpctrace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"net"
	"testing"
	"time"
)

type delayedTraceShard struct {
	wire.UnimplementedShardPersistenceServer
	calls int
}

func (s *delayedTraceShard) Execute(ctx context.Context, q *wire.ShardRequest) (*wire.ShardResult, error) {
	s.calls++
	time.Sleep(10 * time.Millisecond)
	if s.calls < 3 {
		return nil, status.Error(codes.Unavailable, "secret error text must not leak")
	}
	return &wire.ShardResult{}, nil
}
func TestRPCTraceInvocationAndAttempts(t *testing.T) {
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server := grpc.NewServer()
	wire.RegisterShardPersistenceServer(server, &delayedTraceShard{})
	go server.Serve(listener)
	defer server.Stop()
	store, e := NewShardStore(listener.Addr().String(), "trace-partition", "test")
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	var buffer bytes.Buffer
	sink := rpctrace.NewSink(&buffer, 16)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, e = store.invoke(rpctrace.WithSink(ctx, sink), &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: 1})
	if e != nil {
		t.Fatal(e)
	}
	if e = sink.Close(ctx); e != nil {
		t.Fatal(e)
	}
	if bytes.Contains(buffer.Bytes(), []byte("secret")) {
		t.Fatal("error text leaked")
	}
	dec := json.NewDecoder(&buffer)
	var events []rpctrace.Event
	for {
		var event rpctrace.Event
		e = dec.Decode(&event)
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		events = append(events, event)
	}
	if len(events) != 5 {
		t.Fatal(events)
	}
	inv := events[3]
	if inv.Kind != "rpc_invocation" || inv.Status != "OK" || inv.Duration <= 0 {
		t.Fatal(inv)
	}
	var attempts int64
	for i := 0; i < 3; i++ {
		a := events[i]
		if a.Kind != "rpc_attempt" || a.ID != inv.ID || a.OperationID != inv.OperationID || a.Partition != "trace-partition" || a.Duration <= 0 {
			t.Fatal(a, inv)
		}
		attempts += a.Duration
		if i < 2 && a.Status != "Unavailable" {
			t.Fatal(a)
		}
	}
	// Explicit20ms+40ms adapter backoff lies outside individual attempts.
	if inv.Duration-attempts < int64(50*time.Millisecond) {
		t.Fatal("invocation omitted retry backoff", inv.Duration, attempts)
	}
	if events[4].Kind != "trace_end" || events[4].Status != "true" {
		t.Fatal(events[4])
	}
}
