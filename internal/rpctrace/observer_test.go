package rpctrace

import (
	"bytes"
	"context"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/0x63616c/xenon/internal/proof/recorder"
)

func TestObserverConcurrentRegistration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	r, err := recorder.New(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Accept(recorder.Event{Kind: "open", Phase: "steady", Status: "steady"}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(r)
	defer server.Close()
	o, err := NewObserver(server.URL, "producer", "steady")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, finish, err := BeginObserved(WithObserver(context.Background(), o), "shard")
			if err != nil {
				t.Error(err)
				return
			}
			if err = finish(nil); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err = o.Failure(); err != nil {
		t.Fatal(err)
	}
	if err = r.Accept(recorder.Event{Kind: "close", Phase: "steady"}); err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	summary, err := recorder.Validate(path)
	if err != nil || summary.Registered != 20 || summary.Completed != 20 {
		t.Fatal(summary, err)
	}
}
func TestObserverConfiguration(t *testing.T) {
	for _, endpoint := range []string{"https://127.0.0.1:1", "http://localhost:1", "http://127.0.0.1:1/path", "http://user@127.0.0.1:1"} {
		if _, err := NewObserver(endpoint, "producer", "steady"); err == nil {
			t.Fatal("invalid endpoint passed", endpoint)
		}
	}
	o, err := NewObserver("http://127.0.0.1:1", "producer", "steady")
	if err != nil {
		t.Fatal(err)
	}
	var buffer bytes.Buffer
	sink := NewSink(&buffer, 1)
	ctx := WithObserver(WithSink(context.Background(), sink), o)
	if _, _, err = BeginObserved(ctx, "shard"); err == nil {
		t.Fatal("conflicting observers passed")
	}
	if err = sink.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalObserverFailurePreservesOperationUnknown(t *testing.T) {
	r, err := recorder.New(filepath.Join(t.TempDir(), "journal"), 10)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = r.Accept(recorder.Event{Kind: "open", Phase: "steady", Status: "steady"}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(r)
	defer server.Close()
	o, err := NewObserver(server.URL, "producer", "steady")
	if err != nil {
		t.Fatal(err)
	}
	ctx, finish, err := BeginObserved(WithObserver(context.Background(), o), "shard")
	if err != nil {
		t.Fatal(err)
	}
	if ObservationError(ctx) != nil {
		t.Fatal("active invocation mistaken for failed observer")
	}
	original, _ := status.New(codes.Unavailable, "durable outcome unknown").WithDetails(&errdetails.ErrorInfo{Domain: "xenon.routing.v1", Reason: "UNKNOWN_OUTCOME"})
	attempt := observedUnary(ctx, ctx.Value(observedKey{}).(*observedInvocation), "method", nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		o.failed.Store(true)
		return original.Err()
	})
	attemptStatus := status.Convert(attempt)
	if len(attemptStatus.Details()) != 1 {
		t.Fatal("attempt reporting erased unknown", attempt)
	}
	if ObservationError(ctx) == nil {
		t.Fatal("terminal failure missing")
	}
	returned := finish(serviceerror.FromStatus(attemptStatus))
	st := serviceerror.ToStatus(returned)
	if st.Code() != codes.Unavailable || len(st.Details()) != 1 || st.Details()[0].(*errdetails.ErrorInfo).Reason != "UNKNOWN_OUTCOME" {
		t.Fatal("observer failure erased ambiguity", returned)
	}
}
