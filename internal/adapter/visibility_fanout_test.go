package adapter

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fanoutVisibility struct {
	wire.UnimplementedVisibilityPersistenceServer
	entered chan string
	release chan struct{}
	active  atomic.Int32
	fail    bool
}

func (s *fanoutVisibility) Execute(ctx context.Context, q *wire.VisibilityRequest) (*wire.VisibilityResult, error) {
	if q.Command.Kind == wire.VisibilityCommand_GET_SCHEMA {
		return &wire.VisibilityResult{}, nil
	}
	s.active.Add(1)
	defer s.active.Add(-1)
	s.entered <- q.Partition
	if s.fail && q.Partition == "vis-v1-2" {
		return nil, status.Error(codes.InvalidArgument, "declared partition failure")
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &wire.VisibilityResult{Count: 1}, nil
}
func TestVisibilityIndependentFanout(t *testing.T) {
	for _, kind := range []string{"list", "count", "failure"} {
		t.Run(kind, func(t *testing.T) {
			backend := &fanoutVisibility{entered: make(chan string, 4), release: make(chan struct{}), fail: kind == "failure"}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			wire.RegisterVisibilityPersistenceServer(server, backend)
			go server.Serve(listener)
			defer server.Stop()
			store, err := NewVisibilityStore(listener.Addr().String(), "test", "global", searchattribute.NewTestEsProvider(), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				if kind == "list" {
					r, e := store.listVisibility(ctx, "11111111-1111-1111-1111-111111111111", namespace.Name("test"), "", 1, nil, 0)
					if e == nil && len(r.Executions) != 0 {
						e = fmt.Errorf("unexpected rows")
					}
					result <- e
				} else {
					r, e := store.countVisibility(ctx, "11111111-1111-1111-1111-111111111111", namespace.Name("test"), "", 0)
					if e == nil && r.Count != 4 {
						e = fmt.Errorf("wrong count")
					}
					result <- e
				}
			}()
			if kind != "failure" {
				seen := map[string]bool{}
				for i := 0; i < 4; i++ {
					select {
					case p := <-backend.entered:
						seen[p] = true
					case <-ctx.Done():
						t.Fatal("independent calls were serialized")
					}
				}
				if len(seen) != 4 {
					t.Fatal(seen)
				}
				close(backend.release)
			}
			select {
			case err = <-result:
				if (kind == "failure") != (err != nil) {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("fanout did not settle")
			}
			// RPC cancellation may reach the server just after the client worker joins.
			end := time.Now().Add(time.Second)
			for backend.active.Load() != 0 && time.Now().Before(end) {
				time.Sleep(time.Millisecond)
			}
			if backend.active.Load() != 0 {
				t.Fatal("canceled partition still running")
			}
		})
	}
}
