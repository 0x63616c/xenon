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
	deadline, _ := ctx.Deadline()
	req := &wire.ShardRequest{ProtocolVersion: 1, Partition: "history-17", OperationId: "fixed-id", CommandSha256: []byte{0, 255}, Command: &wire.ShardCommand{}}
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
		d, ok := ctx.Deadline()
		if !ok || d.After(deadline.Add(time.Millisecond)) {
			t.Error("deadline extended")
		}
		if saved == nil {
			executions++
			saved = &wire.ShardResult{}
			return nil, status.Error(codes.Unavailable, "completed response loss")
		}
		return proto.Clone(saved), nil
	}
	// First route is stale and unreachable, explicit refresh finds b. Owner-side
	// retry uses the unchanged invocation and its already recorded logical result.
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
		return Route{"old", "127.0.0.1:1"}, nil
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
	c.Local = func(ctx context.Context, _ string, _ proto.Message) (proto.Message, error) {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if _, e := client(t, cc).Execute(short, q); status.Code(e) != codes.DeadlineExceeded {
		t.Fatal(e)
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
	r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, e := client(t, address).Execute(ctx, &wire.ShardRequest{Partition: "p"}); status.Code(e) != codes.Unavailable {
		t.Fatal(e)
	}
}
