package main

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"sync"
	"testing"
	"time"

	enums "go.temporal.io/api/enums/v1"
	taskqueue "go.temporal.io/api/taskqueue/v1"
	info "go.temporal.io/api/workflow/v1"
	workflow "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fake struct {
	workflow.WorkflowServiceClient
	mu       sync.Mutex
	statuses map[string]enums.WorkflowExecutionStatus
	observed []string
	starts   int
}

func (f *fake) StartWorkflowExecution(_ context.Context, r *workflow.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflow.StartWorkflowExecutionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts++
	f.statuses[r.WorkflowId] = enums.WORKFLOW_EXECUTION_STATUS_RUNNING
	return &workflow.StartWorkflowExecutionResponse{RunId: "run-" + r.WorkflowId}, nil
}
func (f *fake) DescribeWorkflowExecution(_ context.Context, r *workflow.DescribeWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflow.DescribeWorkflowExecutionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observed = append(f.observed, r.Execution.WorkflowId)
	return &workflow.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &info.WorkflowExecutionInfo{Status: f.statuses[r.Execution.WorkflowId]}}, nil
}
func request(caseIndex, member int) *workflow.StartWorkflowExecutionRequest {
	return &workflow.StartWorkflowExecutionRequest{Namespace: "test", WorkflowId: fmt.Sprintf("root-%d-%d", caseIndex, member), TaskQueue: &taskqueue.TaskQueue{Name: fmt.Sprintf("omes-xenon-generated-%016x-%016x-%02d", 42, caseIndex, member)}}
}
func setup(size int) (*barrier, *fake) {
	f := &fake{statuses: map[string]enums.WorkflowExecutionStatus{}}
	b := &barrier{client: f, namespace: "test", size: size, queues: map[string]*window{}, failed: make(chan struct{}), save: func([]*window) error { return nil }}
	return b, f
}
func TestFourObservedRunningBeforeRelease(t *testing.T) {
	b, f := setup(4)
	ctx := context.Background()
	for i := range 3 {
		if _, err := b.admit(ctx, request(0, i)); err != nil {
			t.Fatal(err)
		}
	}
	for _, w := range b.windows {
		select {
		case <-w.ready:
			t.Fatal("released before fourth admission")
		default:
		}
	}
	persisted := false
	b.save = func(w []*window) error {
		if len(f.observed) != 4 {
			t.Fatalf("observed %d roots", len(f.observed))
		}
		for _, r := range w[0].Roots {
			if r.Status != "Running" {
				t.Fatal(r.Status)
			}
		}
		select {
		case <-w[0].ready:
			t.Fatal("released before receipt")
		default:
		}
		persisted = true
		return nil
	}
	if _, err := b.admit(ctx, request(0, 3)); err != nil {
		t.Fatal(err)
	}
	if !persisted {
		t.Fatal("no receipt")
	}
	for i := range 4 {
		if err := b.wait(ctx, request(0, i).TaskQueue.Name); err != nil {
			t.Fatal(err)
		}
	}
	// A fifth root on a subsequent case is rejected while any prior chain runs.
	if _, err := b.admit(ctx, request(1, 0)); status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	if f.starts != 4 {
		t.Fatalf("forwarded %d starts", f.starts)
	}
}
func TestConcurrencyOneRequiresPriorChainTerminal(t *testing.T) {
	b, f := setup(1)
	ctx := context.Background()
	for i := range 4 {
		if _, err := b.admit(ctx, request(0, i)); err != nil {
			t.Fatal(err)
		}
		if len(b.windows[i].Roots) != 1 || !b.windows[i].Released {
			t.Fatal("invalid single-root window")
		}
		f.statuses[request(0, i).WorkflowId] = enums.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}
	if len(b.windows) != 4 {
		t.Fatal(len(b.windows))
	}
}
func TestFailedPersistenceNeverReleases(t *testing.T) {
	b, _ := setup(1)
	b.save = func([]*window) error { return errors.New("disk failed") }
	if _, err := b.admit(context.Background(), request(0, 0)); err == nil {
		t.Fatal("accepted failed receipt")
	}
	select {
	case <-b.windows[0].ready:
		t.Fatal("released without receipt")
	default:
	}
}
func TestDuplicateAndFifthMemberRejected(t *testing.T) {
	for _, member := range []int{0, 4} {
		t.Run(fmt.Sprint(member), func(t *testing.T) {
			b, f := setup(4)
			if _, err := b.admit(context.Background(), request(0, 0)); err != nil {
				t.Fatal(err)
			}
			if _, err := b.admit(context.Background(), request(0, member)); err == nil {
				t.Fatal("accepted extra root")
			}
			if f.starts != 1 {
				t.Fatal(f.starts)
			}
		})
	}
}
func TestWaitingPollHonorsCancellation(t *testing.T) {
	b, _ := setup(4)
	b.admit(context.Background(), request(0, 0))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(b.wait(ctx, request(0, 0).TaskQueue.Name), context.Canceled) {
		t.Fatal("cancellation lost")
	}
}
func TestPollBeforeAdmissionRetryable(t *testing.T) {
	b, _ := setup(4)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if status.Code(b.wait(ctx, request(0, 0).TaskQueue.Name)) != codes.Unavailable {
		t.Fatal("expected retryable not yet admitted")
	}
}

// The component control uses actual gRPC encoding and a fake Temporal server;
// no wall-clock sleeps are needed to prove that the task is held at the seam.
type upstreamServer struct {
	workflow.UnimplementedWorkflowServiceServer
	f     *fake
	polls chan struct{}
}

func (s *upstreamServer) StartWorkflowExecution(c context.Context, r *workflow.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error) {
	return s.f.StartWorkflowExecution(c, r)
}
func (s *upstreamServer) DescribeWorkflowExecution(c context.Context, r *workflow.DescribeWorkflowExecutionRequest) (*workflow.DescribeWorkflowExecutionResponse, error) {
	return s.f.DescribeWorkflowExecution(c, r)
}
func (s *upstreamServer) PollWorkflowTaskQueue(context.Context, *workflow.PollWorkflowTaskQueueRequest) (*workflow.PollWorkflowTaskQueueResponse, error) {
	s.polls <- struct{}{}
	return &workflow.PollWorkflowTaskQueueResponse{}, nil
}
func localConnection(t *testing.T, s *grpc.Server) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	go s.Serve(listener)
	t.Cleanup(func() { s.Stop(); listener.Close() })
	c, err := grpc.NewClient("passthrough:///fixture", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func TestRelayReleasesActualRPCOnlyAfterRecordedBarrier(t *testing.T) {
	for _, bypass := range []bool{false, true} {
		t.Run(fmt.Sprintf("bypass=%v", bypass), func(t *testing.T) {
			b, f := setup(4)
			up := grpc.NewServer()
			polls := make(chan struct{}, 1)
			entered := make(chan struct{}, 1)
			b.waitEntered = func(string) { entered <- struct{}{} }
			workflow.RegisterWorkflowServiceServer(up, &upstreamServer{f: f, polls: polls})
			conn := localConnection(t, up)
			b.client = workflow.NewWorkflowServiceClient(conn)
			handler := b.relay(conn)
			if bypass {
				// Deliberately broken relay: identical forwarding, but no held-poll seam.
				handler = b.relayWithWait(conn, func(context.Context, string) error { return nil })
			}
			relay := grpc.NewServer(grpc.ForceServerCodec(codec{}), grpc.UnknownServiceHandler(handler))
			client := workflow.NewWorkflowServiceClient(localConnection(t, relay))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for i := range 3 {
				if _, err := client.StartWorkflowExecution(ctx, request(0, i)); err != nil {
					t.Fatal(err)
				}
			}
			result := make(chan error, 1)
			go func() {
				_, err := client.PollWorkflowTaskQueue(ctx, &workflow.PollWorkflowTaskQueueRequest{Namespace: "test", TaskQueue: request(0, 0).TaskQueue})
				result <- err
			}()
			// No fourth start is issued until the relay has actually entered its wait.
			// The broken control reaches upstream instead, so this guard rejects it.
			observedWait := false
			select {
			case <-entered:
				observedWait = true
			case <-polls:
			case <-ctx.Done():
				t.Fatal("neither wait entry nor bypass was observed")
			}
			if bypass {
				if observedWait {
					t.Fatal("negative control was not rejected")
				}
				if err := <-result; err != nil {
					t.Fatal(err)
				}
				return
			}
			if !observedWait {
				t.Fatal("poll bypassed gate before fourth admission")
			}
			b.save = func([]*window) error {
				select {
				case <-polls:
					return errors.New("poll reached server before persisted release")
				default:
				}
				return nil
			}
			if _, err := client.StartWorkflowExecution(ctx, request(0, 3)); err != nil {
				t.Fatal(err)
			}
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			select {
			case <-polls:
			default:
				t.Fatal("released poll never reached upstream")
			}
		})
	}
}

func TestEagerStartRejectedBeforeUpstreamEffect(t *testing.T) {
	b, f := setup(4)
	up := grpc.NewServer()
	workflow.RegisterWorkflowServiceServer(up, &upstreamServer{f: f, polls: make(chan struct{}, 1)})
	conn := localConnection(t, up)
	b.client = workflow.NewWorkflowServiceClient(conn)
	relay := grpc.NewServer(grpc.ForceServerCodec(codec{}), grpc.UnknownServiceHandler(b.relay(conn)))
	client := workflow.NewWorkflowServiceClient(localConnection(t, relay))
	r := request(0, 0)
	r.RequestEagerExecution = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.StartWorkflowExecution(ctx, r); status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.starts != 0 {
		t.Fatalf("eager start had %d upstream effects", f.starts)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.windows) != 0 || len(b.queues) != 0 {
		t.Fatal("eager request was admitted")
	}
}

func TestBarrierRejectsNonRunningObservation(t *testing.T) {
	b, f := setup(4)
	for i := range 3 {
		if _, err := b.admit(context.Background(), request(0, i)); err != nil {
			t.Fatal(err)
		}
	}
	f.statuses[request(0, 0).WorkflowId] = enums.WORKFLOW_EXECUTION_STATUS_TERMINATED
	if _, err := b.admit(context.Background(), request(0, 3)); err == nil {
		t.Fatal("released terminal root")
	}
	select {
	case <-b.windows[0].ready:
		t.Fatal("released failed observation")
	default:
	}
}
func TestFailureUnblocksWaitingPoll(t *testing.T) {
	b, _ := setup(4)
	b.admit(context.Background(), request(0, 0))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- b.wait(ctx, request(0, 0).TaskQueue.Name) }()
	b.admit(ctx, request(0, 4))
	if status.Code(<-result) != codes.FailedPrecondition {
		t.Fatal("failed gate did not fail poll promptly")
	}
}
