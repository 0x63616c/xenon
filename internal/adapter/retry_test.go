package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/proof/recorder"
	"github.com/0x63616c/xenon/internal/rpctrace"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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

func TestPermanentTransportErrorSurvivesObserverReportingFailure(t *testing.T) {
	attempts := 0
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event recorder.Event
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		if event.Measurement == "execute_attempt" && event.Kind == "register" {
			attempts++
		}
		if event.Kind == "terminal" {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(204)
	}))
	defer endpoint.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	wire.RegisterShardPersistenceServer(server, &statusProxy{code: codes.InvalidArgument})
	go server.Serve(listener)
	defer server.Stop()
	store, err := NewShardStore(listener.Addr().String(), "p", "c")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	observer, err := rpctrace.NewObserver(endpoint.URL, "producer", "proof")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = store.invoke(rpctrace.WithObserver(ctx, observer), &wire.ShardCommand{})
	if serviceerror.ToStatus(err).Code() != codes.InvalidArgument || observer.Failure() == nil || attempts != 1 {
		t.Fatal("terminal status overwritten or retried", err, attempts)
	}
}

func TestOperationTerminalErrorMatrix(t *testing.T) {
	unknown, _ := status.New(codes.Unavailable, "unknown").WithDetails(&errdetails.ErrorInfo{Domain: "xenon.routing.v1", Reason: "UNKNOWN_OUTCOME"})
	cases := []struct {
		name     string
		err      error
		result   *wire.ShardResult
		code     codes.Code
		sentinel error
		cancel   bool
	}{
		{name: "grpc_cancel", err: status.Error(codes.Canceled, "remote"), code: codes.Canceled, sentinel: context.Canceled},
		{name: "grpc_deadline", err: status.Error(codes.DeadlineExceeded, "remote"), code: codes.DeadlineExceeded, sentinel: context.DeadlineExceeded},
		{name: "context_cancel", err: context.Canceled, code: codes.Canceled, sentinel: context.Canceled},
		{name: "context_deadline", err: context.DeadlineExceeded, code: codes.DeadlineExceeded, sentinel: context.DeadlineExceeded},
		{name: "local_cancel", err: status.Error(codes.Unavailable, "interrupted"), code: codes.Canceled, sentinel: context.Canceled, cancel: true},
		{name: "permanent", err: status.Error(codes.InvalidArgument, "bad digest"), code: codes.InvalidArgument},
		{name: "current_unknown", err: unknown.Err(), code: codes.Unavailable, sentinel: context.Canceled, cancel: true},
		{name: "nil", code: codes.Internal},
		{name: "malformed", result: &wire.ShardResult{Error: 999}, code: codes.Internal},
		{name: "durable", result: &wire.ShardResult{}, code: codes.OK},
		{name: "durable_logical_failure", result: &wire.ShardResult{Error: wire.ShardResult_UNAVAILABLE}, code: codes.OK},
	}
	for _, tc := range cases {
		for _, prior := range []bool{false, true} {
			for _, observerFailure := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/prior=%v/observer=%v", tc.name, prior, observerFailure), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					finish := func(e error) error { return e }
					failAfter := int32(0)
					if prior {
						failAfter = 1
					}
					var terminals atomic.Int32
					if observerFailure {
						endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							var event recorder.Event
							if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
								t.Error(err)
							}
							if event.Kind == "terminal" && terminals.Add(1) > failAfter {
								w.WriteHeader(500)
								return
							}
							w.WriteHeader(204)
						}))
						defer endpoint.Close()
						observer, e := rpctrace.NewObserver(endpoint.URL, "producer", "proof")
						if e != nil {
							t.Fatal(e)
						}
						var e2 error
						ctx, finish, e2 = rpctrace.BeginObserved(rpctrace.WithObserver(ctx, observer), "shard")
						if e2 != nil {
							t.Fatal(e2)
						}
					}
					calls := 0
					result, err := retryOperation(ctx, func(callCtx context.Context) (*wire.ShardResult, error) {
						calls++
						r, e := tc.result, tc.err
						first := prior && calls == 1
						if first {
							r = nil
							e = unknown.Err()
						}
						attempt := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
							if !first && tc.cancel {
								cancel()
							}
							return e
						}
						return r, rpctrace.Unary(callCtx, "method", &wire.ShardRequest{Partition: "p", OperationId: "same"}, r, nil, attempt)
					})
					err = finish(err)
					expected := tc.code
					unresolved := (prior && (observerFailure || tc.code != codes.OK)) || tc.name == "current_unknown"
					if unresolved {
						expected = codes.Unavailable
					} else if observerFailure && tc.err == nil {
						expected = codes.Unavailable
					}
					if serviceerror.ToStatus(err).Code() != expected {
						t.Fatal("status", err, expected)
					}
					if tc.sentinel != nil && !errors.Is(err, tc.sentinel) {
						t.Fatal("cancellation identity lost", err)
					}
					if unresolved {
						details := serviceerror.ToStatus(err).Details()
						if len(details) != 1 || details[0].(*errdetails.ErrorInfo).Reason != "UNKNOWN_OUTCOME" {
							t.Fatal("ambiguity lost", err)
						}
					}
					wantCalls := 1
					if prior {
						wantCalls = 2
					}
					if calls != wantCalls {
						t.Fatal("terminal retried", calls)
					}
					if !observerFailure && tc.code == codes.OK && !proto.Equal(result, tc.result) {
						t.Fatal("durable result lost")
					}
				})
			}
		}
	}
}
