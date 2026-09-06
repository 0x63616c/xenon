package adapter

import (
	"context"
	"errors"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

type retryConn struct {
	t        *testing.T
	calls    int
	first    proto.Message
	deadline time.Time
}

func (c *retryConn) Invoke(ctx context.Context, _ string, args, reply any, _ ...grpc.CallOption) error {
	c.calls++
	deadline, ok := ctx.Deadline()
	if !ok {
		c.t.Fatal("unbounded operation")
	}
	if c.calls == 1 {
		c.deadline = deadline
		c.first = proto.Clone(args.(proto.Message))
	} else if !deadline.Equal(c.deadline) || !proto.Equal(c.first, args.(proto.Message)) {
		c.t.Fatal("retry changed deadline or request")
	}
	if c.calls <= 3 {
		return status.Error(codes.Unavailable, "owner recovering")
	}
	return nil
}
func (*retryConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	panic("unexpected stream")
}

func TestAllFamiliesRetainRequestAcrossAdmissionRecovery(t *testing.T) {
	tests := map[string]func(context.Context, *retryConn) error{
		"shard": func(c context.Context, conn *retryConn) error {
			s := &ShardStore{client: wire.NewShardPersistenceClient(conn), invocationTimeout: time.Second}
			_, e := s.invoke(c, &wire.ShardCommand{})
			return e
		},
		"cluster": func(c context.Context, conn *retryConn) error {
			s := &ClusterStore{client: wire.NewClusterPersistenceClient(conn), invocationTimeout: time.Second}
			_, e := s.invokeCluster(c, &wire.ClusterCommand{})
			return e
		},
		"metadata": func(c context.Context, conn *retryConn) error {
			s := &MetadataStore{client: wire.NewMetadataPersistenceClient(conn), invocationTimeout: time.Second}
			_, e := s.invokeMetadata(c, &wire.MetadataCommand{})
			return e
		},
		"execution": func(c context.Context, conn *retryConn) error {
			s := &WorkflowStore{client: wire.NewExecutionPersistenceClient(conn), invocationTimeout: time.Second}
			_, e := s.invokeExecution(c, &wire.ExecutionCommand{})
			return e
		},
		"history": func(c context.Context, conn *retryConn) error {
			s := &HistoryStore{client: wire.NewHistoryPersistenceClient(conn), invocationTimeout: time.Second}
			_, e := s.invokeHistory(c, &wire.HistoryCommand{})
			return e
		},
		"historytasks": func(c context.Context, conn *retryConn) error {
			s := &HistoryTasksStore{client: wire.NewHistoryTasksPersistenceClient(conn), invocationTimeout: time.Second}
			_, e := s.invokeHistoryTasks(c, &wire.HistoryTasksCommand{})
			return e
		},
		"executiontasks": func(c context.Context, conn *retryConn) error {
			s := &ExecutionTasksStore{client: wire.NewExecutionTasksPersistenceClient(conn), invocationTimeout: time.Second}
			_, e := s.invokeExecutionTasks(c, &wire.ExecutionTasksCommand{})
			return e
		},
		"matching": func(c context.Context, conn *retryConn) error {
			s := &MatchingStore{client: wire.NewMatchingPersistenceClient(conn), invocationTimeout: time.Second}
			_, e := s.invokeMatching(c, &wire.MatchingCommand{})
			return e
		},
		"visibility": func(c context.Context, conn *retryConn) error {
			s := &VisibilityStore{client: wire.NewVisibilityPersistenceClient(conn)}
			_, e := s.invokeVisibility(c, "p", &wire.VisibilityCommand{})
			return e
		},
		"queue": func(c context.Context, conn *retryConn) error {
			s := &Queue{client: wire.NewQueuePersistenceClient(conn), invocationTimeout: time.Second}
			_, e := s.invokeQueue(c, &wire.QueueCommand{})
			return e
		},
		"queuev2": func(c context.Context, conn *retryConn) error {
			s := &QueueV2{client: wire.NewQueueV2PersistenceClient(conn)}
			_, e := s.invoke(c, &wire.QueueV2Command{})
			return e
		},
		"nexus": func(c context.Context, conn *retryConn) error {
			s := &NexusStore{client: wire.NewNexusPersistenceClient(conn), invocationTimeout: time.Second}
			_, e := s.invokeNexus(c, &wire.NexusCommand{})
			return e
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			conn := &retryConn{t: t}
			if e := run(ctx, conn); e != nil || conn.calls != 4 {
				t.Fatal(e, conn.calls)
			}
		})
	}
}

func TestRetryUnknownCannotResolveWithMalformedResult(t *testing.T) {
	unknown, _ := status.New(codes.Unavailable, "unknown").WithDetails(&errdetails.ErrorInfo{Domain: "xenon.routing.v1", Reason: "UNKNOWN_OUTCOME"})
	for _, result := range []*wire.ShardResult{nil, {Error: 999}} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		calls := 0
		_, err := retryOperation(ctx, func(context.Context) (*wire.ShardResult, error) {
			calls++
			if calls == 1 {
				return nil, unknown.Err()
			}
			return result, nil
		})
		cancel()
		st := serviceerror.ToStatus(err)
		if st.Code() != codes.Unavailable || len(st.Details()) != 1 || st.Details()[0].(*errdetails.ErrorInfo).Reason != "UNKNOWN_OUTCOME" || calls != 2 {
			t.Fatal(err, calls)
		}
	}
	if _, err := retryOperation(context.Background(), func(context.Context) (*wire.ShardResult, error) { t.Fatal("unbounded dispatch"); return nil, nil }); err == nil {
		t.Fatal("unbounded accepted")
	}
}

func TestVisibilityBoundsOnlyUnboundedCaller(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		ctx := context.Background()
		cancel := func() {}
		var expected time.Time
		if bounded {
			ctx, cancel = context.WithTimeout(ctx, time.Minute)
			expected, _ = ctx.Deadline()
		}
		conn := &retryConn{t: t}
		s := &VisibilityStore{client: wire.NewVisibilityPersistenceClient(conn)}
		before := time.Now()
		_, err := s.invokeVisibility(ctx, "p", &wire.VisibilityCommand{})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if bounded {
			if !conn.deadline.Equal(expected) {
				t.Fatal("caller deadline changed")
			}
		} else if conn.deadline.Before(before.Add(29*time.Second)) || conn.deadline.After(before.Add(31*time.Second)) {
			t.Fatal("missing30s fallback")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn := &retryConn{t: t}
	s := &VisibilityStore{client: wire.NewVisibilityPersistenceClient(conn)}
	if _, err := s.invokeVisibility(ctx, "p", &wire.VisibilityCommand{}); !errors.Is(err, context.Canceled) || conn.calls != 0 {
		t.Fatal(err, conn.calls)
	}
}
