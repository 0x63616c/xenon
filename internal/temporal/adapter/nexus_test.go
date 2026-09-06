package adapter

import (
	"context"
	"errors"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

type nexusWireFunc func(context.Context, *wire.NexusRequest) (*wire.NexusResult, error)

func (f nexusWireFunc) Execute(ctx context.Context, q *wire.NexusRequest, _ ...grpc.CallOption) (*wire.NexusResult, error) {
	return f(ctx, q)
}
func TestNexusTransport(t *testing.T) {
	var previous *wire.NexusRequest
	calls := 0
	s := &NexusStore{partition: "p", invocationTimeout: time.Second, client: nexusWireFunc(func(_ context.Context, q *wire.NexusRequest) (*wire.NexusResult, error) {
		calls++
		if calls == 1 {
			previous = proto.Clone(q).(*wire.NexusRequest)
			return nil, status.Error(codes.Unavailable, "lost response")
		}
		if !proto.Equal(previous, q) {
			t.Fatal("retry identity changed")
		}
		return &wire.NexusResult{}, nil
	})}
	if _, e := s.invokeNexus(context.Background(), &wire.NexusCommand{Kind: wire.NexusCommand_LIST}); e != nil || calls != 2 {
		t.Fatal(calls, e)
	}
	for _, code := range []codes.Code{codes.Canceled, codes.DeadlineExceeded} {
		s.client = nexusWireFunc(func(context.Context, *wire.NexusRequest) (*wire.NexusResult, error) {
			return nil, status.Error(code, "remote timer")
		})
		_, e := s.invokeNexus(context.Background(), &wire.NexusCommand{Kind: wire.NexusCommand_LIST})
		want := context.Canceled
		if code == codes.DeadlineExceeded {
			want = context.DeadlineExceeded
		}
		if !errors.Is(e, want) {
			t.Fatal(e)
		}
	}
}
