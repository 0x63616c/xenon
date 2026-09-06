package routing

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// A fixed logical deadline avoids wall-clock timers in this routing-only test.
// Storage durability and crash recovery are intentionally outside its scope.
type logicalContext struct {
	context.Context
	deadline time.Time
}

func (c logicalContext) Deadline() (time.Time, bool) { return c.deadline, true }

func TestInjectedTransportLostResponseRefresh(t *testing.T) {
	ctx := logicalContext{context.Background(), time.Unix(123, 0)}
	req := &wire.ShardRequest{Partition: "p", OperationId: "same-operation", CommandSha256: make([]byte, 32)}
	events := make(chan Event, 8)
	calls := 0
	refreshes := []bool{}
	saved := &wire.ShardResult{}
	r := &Router{Node: "ingress", Events: events, Local: func(context.Context, string, proto.Message) (proto.Message, error) {
		t.Fatal("unexpected local execution")
		return nil, nil
	}}
	r.Directory = directoryFunc(func(_ context.Context, partition string, refresh bool) (Route, error) {
		if partition != "p" {
			t.Fatal(partition)
		}
		refreshes = append(refreshes, refresh)
		if refresh {
			return Route{"replacement", "new"}, nil
		}
		return Route{"prior", "old"}, nil
	})
	r.Invoke = func(c context.Context, address, method string, q, out proto.Message) error {
		calls++
		deadline, ok := c.Deadline()
		if !ok || !deadline.Equal(ctx.deadline) || !proto.Equal(req, q) || method != wire.ShardPersistence_Execute_FullMethodName {
			t.Fatal("forwarding envelope changed")
		}
		md, _ := metadata.FromOutgoingContext(c)
		if !reflect.DeepEqual(md.Get(hopHeader), []string{"1"}) {
			t.Fatal(md)
		}
		if calls == 1 {
			if address != "old" {
				t.Fatal(address)
			}
			return UnknownOutcome()
		}
		if address != "new" {
			t.Fatal(address)
		}
		proto.Merge(out, saved)
		return nil
	}
	reply, err := r.Interceptor(func(string) proto.Message { return new(wire.ShardResult) })(ctx, req, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, nil)
	if err != nil || !proto.Equal(reply.(proto.Message), saved) || calls != 2 || !reflect.DeepEqual(refreshes, []bool{false, true}) {
		t.Fatal(reply, err, calls, refreshes)
	}
	got := []Event{}
	for len(events) > 0 {
		got = append(got, <-events)
	}
	want := []Event{{"resolve", 0, codes.OK}, {"forward", 0, codes.Unavailable}, {"resolve", 1, codes.OK}, {"forward", 1, codes.OK}}
	if !reflect.DeepEqual(got, want) {
		t.Fatal(got)
	}
}

func TestInjectedTypedErrorAndSaturatedObserver(t *testing.T) {
	calls := 0
	expected := status.Error(codes.FailedPrecondition, "original typed failure")
	r := &Router{Node: "a", Events: make(chan Event), Directory: directoryFunc(func(context.Context, string, bool) (Route, error) { return Route{"b", "b"}, nil }), Local: func(context.Context, string, proto.Message) (proto.Message, error) { t.Fatal("local"); return nil, nil }}
	r.Invoke = func(context.Context, string, string, proto.Message, proto.Message) error { calls++; return expected }
	_, err := r.Interceptor(func(string) proto.Message { return new(wire.ShardResult) })(logicalContext{context.Background(), time.Unix(123, 0)}, &wire.ShardRequest{Partition: "p"}, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, nil)
	if err != expected || calls != 1 {
		t.Fatal(err, calls)
	}
}

func TestPoolEvictionIsStable(t *testing.T) {
	r := &Router{}
	defer r.Close()
	for i := 0; i < 64; i++ {
		address := fmt.Sprintf("127.0.0.1:%05d", 10000+i)
		if _, err := r.connection(address); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.connection("127.0.0.1:20000"); err != nil {
		t.Fatal(err)
	}
	if _, exists := r.connections["127.0.0.1:10000"]; exists || len(r.connections) != 64 {
		t.Fatal("unstable eviction")
	}
}

func TestForwardedReceiverNeverForwardsOrRetries(t *testing.T) {
	ctx := metadata.NewIncomingContext(logicalContext{context.Background(), time.Unix(123, 0)}, metadata.Pairs(hopHeader, "1"))
	calls := 0
	r := &Router{Node: "receiver", Directory: directoryFunc(func(context.Context, string, bool) (Route, error) { return Route{"other", "other"}, nil }), Local: func(context.Context, string, proto.Message) (proto.Message, error) { calls++; return nil, StaleOwner() }, Invoke: func(context.Context, string, string, proto.Message, proto.Message) error {
		t.Fatal("recursive forwarding")
		return nil
	}}
	invoke := r.Interceptor(func(string) proto.Message { return new(wire.ShardResult) })
	req := &wire.ShardRequest{Partition: "p"}
	_, err := invoke(ctx, req, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, nil)
	if !retryable(err, req) || calls != 0 {
		t.Fatalf("stale receiver: %v calls=%d", err, calls)
	}
	r.Directory = directoryFunc(func(context.Context, string, bool) (Route, error) { return Route{"receiver", "receiver"}, nil })
	_, err = invoke(ctx, req, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, nil)
	if !retryable(err, req) || calls != 1 {
		t.Fatalf("receiver retried: %v calls=%d", err, calls)
	}
}

func TestOriginRetriesOnlyTypedSafeFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure error
		id      string
		digest  []byte
		want    int
	}{
		{"untyped unavailable", status.Error(codes.Unavailable, "storage corruption"), "fixed-id", make([]byte, 32), 1},
		{"typed stale bounded", StaleOwner(), "", nil, 3},
		{"unknown protected", UnknownOutcome(), "fixed-id", make([]byte, 32), 3},
		{"unknown missing identity", UnknownOutcome(), "", make([]byte, 32), 1},
		{"unknown missing digest", UnknownOutcome(), "fixed-id", nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			r := &Router{Node: "origin", Directory: directoryFunc(func(context.Context, string, bool) (Route, error) { return Route{"owner", "owner"}, nil }), Local: func(context.Context, string, proto.Message) (proto.Message, error) { t.Fatal("local"); return nil, nil }, Invoke: func(context.Context, string, string, proto.Message, proto.Message) error { calls++; return tc.failure }}
			_, err := r.Interceptor(func(string) proto.Message { return new(wire.ShardResult) })(logicalContext{context.Background(), time.Unix(123, 0)}, &wire.ShardRequest{Partition: "p", OperationId: tc.id, CommandSha256: tc.digest}, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, nil)
			if err != tc.failure || calls != tc.want {
				t.Fatalf("error=%v calls=%d want=%d", err, calls, tc.want)
			}
		})
	}
}
