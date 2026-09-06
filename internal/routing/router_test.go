package routing

import (
	"context"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"net"
	"sync"
	"testing"
	"time"
)

type directoryFunc func(context.Context, string, bool) (Route, error)

func (f directoryFunc) Resolve(c context.Context, p string, refresh bool) (Route, error) {
	return f(c, p, refresh)
}
func serve(t *testing.T, r *Router) string {
	t.Helper()
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	s := grpc.NewServer(grpc.UnaryInterceptor(r.Interceptor(func(method string) proto.Message {
		if method == wire.ShardPersistence_Execute_FullMethodName {
			return new(wire.ShardResult)
		}
		return nil
	})))
	wire.RegisterShardPersistenceServer(s, &wire.UnimplementedShardPersistenceServer{})
	go s.Serve(ln)
	t.Cleanup(func() { s.Stop(); r.Close() })
	return ln.Addr().String()
}
func client(t *testing.T, address string) wire.ShardPersistenceClient {
	t.Helper()
	c, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	return wire.NewShardPersistenceClient(c)
}
func TestForwardingReplayAndRefresh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req := &wire.ShardRequest{ProtocolVersion: 1, Partition: "history-17", OperationId: "fixed-id", CommandSha256: make([]byte, 32), Command: &wire.ShardCommand{}}
	var mu sync.Mutex
	calls, executions, refreshes := 0, 0, 0
	var saved *wire.ShardResult
	b := &Router{Node: "b"}
	baddr := serve(t, b)
	b.Directory = directoryFunc(func(context.Context, string, bool) (Route, error) { return Route{"b", baddr}, nil })
	b.Local = func(ctx context.Context, method string, m proto.Message) (proto.Message, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if !proto.Equal(req, m) {
			t.Error("forwarded operation changed")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("forwarded deadline missing")
		}
		if saved == nil {
			executions++
			saved = &wire.ShardResult{}
			return nil, UnknownOutcome()
		}
		return proto.Clone(saved), nil
	}
	// The destination loses its completed response. Only the origin retries,
	// keeping the unchanged invocation and its already recorded logical result.
	a := &Router{Node: "a", Local: func(context.Context, string, proto.Message) (proto.Message, error) {
		t.Error("nonowner executed")
		return nil, nil
	}}
	a.Directory = directoryFunc(func(_ context.Context, p string, refresh bool) (Route, error) {
		if p != "history-17" {
			t.Error(p)
		}
		if refresh {
			mu.Lock()
			refreshes++
			mu.Unlock()
			return Route{"b", baddr}, nil
		}
		return Route{"b", baddr}, nil
	})
	aaddr := serve(t, a)
	if _, e := client(t, aaddr).Execute(ctx, req); e != nil {
		t.Fatal(e)
	}
	mu.Lock()
	defer mu.Unlock()
	if executions != 1 || calls != 2 || refreshes != 1 {
		t.Fatal(executions, calls, refreshes)
	}
}
func TestForwardingLoopsAndDeadline(t *testing.T) {
	a, b := &Router{Node: "a"}, &Router{Node: "b"}
	aa, bb := serve(t, a), serve(t, b)
	a.Directory = directoryFunc(func(context.Context, string, bool) (Route, error) { return Route{"b", bb}, nil })
	b.Directory = directoryFunc(func(context.Context, string, bool) (Route, error) { return Route{"a", aa}, nil })
	for _, r := range []*Router{a, b} {
		r.Local = func(context.Context, string, proto.Message) (proto.Message, error) {
			t.Error("loop executed")
			return nil, nil
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	q := &wire.ShardRequest{Partition: "p"}
	if _, e := client(t, aa).Execute(ctx, q); status.Code(e) != codes.Unavailable {
		t.Fatal("loop not bounded", e)
	}
	bad := metadata.NewOutgoingContext(ctx, metadata.Pairs(hopHeader, "-1"))
	if _, e := client(t, aa).Execute(bad, q); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
	if _, e := client(t, aa).Execute(context.Background(), q); status.Code(e) != codes.InvalidArgument {
		t.Fatal("missing deadline", e)
	}
	c := &Router{Node: "c"}
	cc := serve(t, c)
	c.Directory = directoryFunc(func(context.Context, string, bool) (Route, error) { return Route{"c", cc}, nil })
	destinationCanceled := make(chan struct{}, 1)
	c.Local = func(ctx context.Context, _ string, _ proto.Message) (proto.Message, error) {
		<-ctx.Done()
		destinationCanceled <- struct{}{}
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	forward := &Router{Node: "forward", Directory: directoryFunc(func(context.Context, string, bool) (Route, error) { return Route{"c", cc}, nil }), Local: func(context.Context, string, proto.Message) (proto.Message, error) {
		t.Error("nonowner executed")
		return nil, nil
	}}
	fa := serve(t, forward)
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if _, e := client(t, fa).Execute(short, q); status.Code(e) != codes.DeadlineExceeded {
		t.Fatal(e)
	}
	select {
	case <-destinationCanceled:
	case <-time.After(time.Second):
		t.Fatal("destination did not observe caller cancellation")
	}
}

func TestForwardingClosedAdmission(t *testing.T) {
	r := &Router{Node: "a"}
	address := serve(t, r)
	r.Directory = directoryFunc(func(context.Context, string, bool) (Route, error) { return Route{"a", address}, nil })
	r.Local = func(context.Context, string, proto.Message) (proto.Message, error) {
		t.Error("closed router executed")
		return &wire.ShardResult{}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	want, _ := ctx.Deadline()
	r.Local = func(got context.Context, _ string, _ proto.Message) (proto.Message, error) {
		d, ok := got.Deadline()
		if !ok || !d.Equal(want) {
			t.Fatal("local admission reset caller deadline")
		}
		return &wire.ShardResult{}, nil
	}
	interceptor := r.Interceptor(func(string) proto.Message { return &wire.ShardResult{} })
	if _, e := interceptor(ctx, &wire.ShardRequest{Partition: "p"}, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, nil); e != nil {
		t.Fatal(e)
	}
	r.Close()
	r.Local = func(context.Context, string, proto.Message) (proto.Message, error) {
		t.Error("closed router executed")
		return nil, nil
	}
	if _, e := client(t, address).Execute(ctx, &wire.ShardRequest{Partition: "p"}); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
}
