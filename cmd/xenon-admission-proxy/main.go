// xenon-admission-proxy is a fixture-only Temporal unary RPC relay. It prevents
// generated roots from executing until a complete admission window is Running.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	common "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	workflow "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type wire []byte
type codec struct{}

func (codec) Name() string { return "proto" }
func (codec) Marshal(v any) ([]byte, error) {
	if b, ok := v.(*wire); ok {
		return *b, nil
	}
	return proto.Marshal(v.(proto.Message))
}
func (codec) Unmarshal(b []byte, v any) error {
	if p, ok := v.(*wire); ok {
		*p = append((*p)[:0], b...)
		return nil
	}
	return proto.Unmarshal(b, v.(proto.Message))
}

var generatedQueue = regexp.MustCompile(`^omes-xenon-generated-[0-9a-f]{16}-[0-9a-f]{16}-0[0-3]$`)

type root struct {
	Queue      string `json:"queue"`
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	Status     string `json:"status"`
}
type window struct {
	Roots    []root `json:"roots"`
	Released bool   `json:"released"`
	ready    chan struct{}
}
type barrier struct {
	mu        sync.Mutex
	client    workflow.WorkflowServiceClient
	namespace string
	size      int
	windows   []*window
	queues    map[string]*window
	save      func([]*window) error
	failure   error
	failed    chan struct{}
	// waitEntered observes entry into the held-poll seam for deterministic controls.
	waitEntered func(string)
}

func (b *barrier) fail(err error) error {
	if b.failure == nil && b.failed != nil {
		close(b.failed)
	}
	b.failure = err
	return status.Errorf(codes.FailedPrecondition, "admission proof failed: %v", err)
}
func (b *barrier) admit(ctx context.Context, r *workflow.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error) {
	return b.admitWithStart(ctx, r, func(ctx context.Context, r *workflow.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error) {
		return b.client.StartWorkflowExecution(ctx, r)
	})
}

func (b *barrier) admitWithStart(ctx context.Context, r *workflow.StartWorkflowExecutionRequest, start func(context.Context, *workflow.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error)) (*workflow.StartWorkflowExecutionResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failure != nil {
		return nil, b.fail(b.failure)
	}
	if !generatedQueue.MatchString(r.GetTaskQueue().GetName()) {
		return nil, b.fail(fmt.Errorf("invalid generated root queue"))
	}
	if r.GetRequestEagerExecution() {
		return nil, b.fail(fmt.Errorf("eager generated-root execution bypasses admission barrier"))
	}
	if r.Namespace != b.namespace {
		return nil, b.fail(fmt.Errorf("unexpected namespace"))
	}
	if b.queues[r.TaskQueue.Name] != nil {
		return nil, b.fail(fmt.Errorf("second start on root queue %s", r.TaskQueue.Name))
	}
	var w *window
	if len(b.windows) > 0 {
		w = b.windows[len(b.windows)-1]
	}
	if w == nil || w.Released {
		// A subsequent window cannot hide unfinished roots from the previous one.
		if w != nil {
			for _, p := range w.Roots {
				d, err := b.client.DescribeWorkflowExecution(ctx, &workflow.DescribeWorkflowExecutionRequest{Namespace: b.namespace, Execution: &common.WorkflowExecution{WorkflowId: p.WorkflowID}})
				if err != nil {
					return nil, b.fail(err)
				}
				switch d.GetWorkflowExecutionInfo().GetStatus() {
				case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, enums.WORKFLOW_EXECUTION_STATUS_FAILED,
					enums.WORKFLOW_EXECUTION_STATUS_CANCELED, enums.WORKFLOW_EXECUTION_STATUS_TERMINATED,
					enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
				default:
					return nil, b.fail(fmt.Errorf("previous root chain is not terminal"))
				}
			}
		}
		w = &window{ready: make(chan struct{})}
		b.windows = append(b.windows, w)
	}
	// Record the queue before forwarding: ambiguous upstream failure must never
	// permit a retry to be mistaken for a new admission or yield passing evidence.
	b.queues[r.TaskQueue.Name] = w
	out, err := start(ctx, r)
	if err != nil {
		return nil, b.fail(err)
	}
	w.Roots = append(w.Roots, root{Queue: r.TaskQueue.Name, WorkflowID: r.WorkflowId, RunID: out.RunId})
	if len(w.Roots) == b.size {
		for i, p := range w.Roots {
			d, err := b.client.DescribeWorkflowExecution(ctx, &workflow.DescribeWorkflowExecutionRequest{Namespace: b.namespace, Execution: &common.WorkflowExecution{WorkflowId: p.WorkflowID, RunId: p.RunID}})
			if err != nil {
				return nil, b.fail(err)
			}
			if d.GetWorkflowExecutionInfo().GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
				return nil, b.fail(fmt.Errorf("root is not Running"))
			}
			w.Roots[i].Status = d.GetWorkflowExecutionInfo().GetStatus().String()
		}
		w.Released = true
		// Persist observations before any workflow-task poll reaches the server.
		if err := b.save(b.windows); err != nil {
			w.Released = false
			return nil, b.fail(err)
		}
		close(w.ready)
	}
	return out, nil
}
func (b *barrier) wait(ctx context.Context, queue string) error {
	// Pollers may arrive before StartWorkflowExecution; return retryable
	// Unavailable until their root has been registered. No task reaches upstream.
	for {
		b.mu.Lock()
		w := b.queues[queue]
		failure := b.failure
		b.mu.Unlock()
		if failure != nil {
			return status.Error(codes.FailedPrecondition, failure.Error())
		}
		if w != nil {
			if b.waitEntered != nil {
				b.waitEntered(queue)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-b.failed:
				return status.Error(codes.FailedPrecondition, "admission proof failed")
			case <-w.ready:
				return nil
			}
		}
		return status.Error(codes.Unavailable, "root has not been admitted yet")
	}
}
func (b *barrier) relay(conn *grpc.ClientConn) grpc.StreamHandler {
	return b.relayWithWait(conn, b.wait)
}

// Keeping the wait seam explicit lets the component control prove it detects a
// relay that bypasses the gate. The executable always uses b.wait.
func (b *barrier) relayWithWait(conn *grpc.ClientConn, wait func(context.Context, string) error) grpc.StreamHandler {
	return func(_ any, s grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(s)
		var request wire
		if err := s.RecvMsg(&request); err != nil {
			return err
		}
		ctx := s.Context()
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = metadata.NewOutgoingContext(ctx, md)
		}
		if strings.HasSuffix(method, "/StartWorkflowExecution") {
			var r workflow.StartWorkflowExecutionRequest
			if err := proto.Unmarshal(request, &r); err != nil {
				return err
			}
			if strings.HasPrefix(r.GetTaskQueue().GetName(), "omes-xenon-generated-") {
				out, err := b.admit(ctx, &r)
				if err != nil {
					return err
				}
				return s.SendMsg(out)
			}
		}
		// Composite starts must retain their original atomic RPC. In particular,
		// update-with-start can wait for workflow execution before returning;
		// observe creation independently so its held poll can be released.
		var composite *workflow.StartWorkflowExecutionRequest
		if strings.HasSuffix(method, "/SignalWithStartWorkflowExecution") {
			var r workflow.SignalWithStartWorkflowExecutionRequest
			if err := proto.Unmarshal(request, &r); err != nil {
				return err
			}
			composite = &workflow.StartWorkflowExecutionRequest{Namespace: r.Namespace, WorkflowId: r.WorkflowId, TaskQueue: r.TaskQueue}
		}
		if strings.HasSuffix(method, "/ExecuteMultiOperation") {
			var r workflow.ExecuteMultiOperationRequest
			if err := proto.Unmarshal(request, &r); err != nil {
				return err
			}
			for _, op := range r.Operations {
				if start := op.GetStartWorkflow(); start != nil {
					if composite != nil {
						return status.Error(codes.InvalidArgument, "multiple starts are unsupported by admission fixture")
					}
					composite = proto.Clone(start).(*workflow.StartWorkflowExecutionRequest)
					composite.Namespace = r.Namespace
				}
			}
		}
		if composite != nil && strings.HasPrefix(composite.GetTaskQueue().GetName(), "omes-xenon-generated-") {
			if prior, ok := b.knownRoot(composite); ok {
				return b.followupComposite(ctx, s, conn, method, request, composite, prior)
			}
			return b.compositeStart(ctx, s, conn, method, request, composite)
		}
		if strings.HasSuffix(method, "/PollWorkflowTaskQueue") {
			var r workflow.PollWorkflowTaskQueueRequest
			if err := proto.Unmarshal(request, &r); err != nil {
				return err
			}
			if strings.HasPrefix(r.GetTaskQueue().GetName(), "omes-xenon-generated-") {
				if r.Namespace != b.namespace {
					return status.Error(codes.FailedPrecondition, "unexpected poll namespace")
				}
				if err := wait(ctx, r.TaskQueue.Name); err != nil {
					return err
				}
			}
		}
		var response wire
		var header, trailer metadata.MD
		err := conn.Invoke(ctx, method, &request, &response, grpc.ForceCodec(codec{}), grpc.Header(&header), grpc.Trailer(&trailer))
		s.SetTrailer(trailer)
		if len(header) > 0 {
			if e := s.SendHeader(header); e != nil {
				return e
			}
		}
		if err != nil {
			return err
		}
		return s.SendMsg(&response)
	}
}

// knownRoot distinguishes a generated client action targeting an admitted root
// from another independent admission. Queue reuse alone is never sufficient.
func (b *barrier) knownRoot(r *workflow.StartWorkflowExecutionRequest) (root, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if w := b.queues[r.GetTaskQueue().GetName()]; w != nil {
		for _, p := range w.Roots {
			if p.WorkflowID == r.WorkflowId {
				return p, true
			}
		}
	}
	return root{}, false
}

func (b *barrier) followupComposite(ctx context.Context, s grpc.ServerStream, conn *grpc.ClientConn, method string, request wire, r *workflow.StartWorkflowExecutionRequest, prior root) error {
	fail := func(err error) error { b.mu.Lock(); defer b.mu.Unlock(); return b.fail(err) }
	b.mu.Lock()
	failure := b.failure
	b.mu.Unlock()
	if failure != nil {
		return status.Error(codes.FailedPrecondition, failure.Error())
	}
	if r.Namespace != b.namespace || r.RequestEagerExecution {
		return fail(fmt.Errorf("invalid follow-up namespace or eager execution"))
	}
	var out wire
	var header, trailer metadata.MD
	err := conn.Invoke(ctx, method, &request, &out, grpc.ForceCodec(codec{}), grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return fail(err)
	}
	var runID string
	var started bool
	if strings.HasSuffix(method, "/SignalWithStartWorkflowExecution") {
		var result workflow.SignalWithStartWorkflowExecutionResponse
		if err := proto.Unmarshal(out, &result); err != nil {
			return fail(err)
		}
		runID, started = result.RunId, result.Started
	} else {
		var result workflow.ExecuteMultiOperationResponse
		if err := proto.Unmarshal(out, &result); err != nil {
			return fail(err)
		}
		for _, response := range result.Responses {
			if start := response.GetStartWorkflow(); start != nil {
				runID, started = start.RunId, start.Started
				if start.EagerWorkflowTask != nil {
					return fail(fmt.Errorf("unexpected eager follow-up task"))
				}
			}
		}
	}
	// A terminal-race replacement is an invalid run, never counted as an existing
	// root. Preserve the original request semantics rather than rewrite its policy.
	if runID == "" || started {
		return fail(fmt.Errorf("follow-up created an unadmitted root or omitted its run ID"))
	}
	if runID != prior.RunID {
		d, err := b.client.DescribeWorkflowExecution(ctx, &workflow.DescribeWorkflowExecutionRequest{Namespace: b.namespace, Execution: &common.WorkflowExecution{WorkflowId: prior.WorkflowID, RunId: runID}})
		if err != nil {
			return fail(err)
		}
		if d.GetWorkflowExecutionInfo().GetFirstRunId() != prior.RunID {
			return fail(fmt.Errorf("follow-up execution is outside admitted root chain"))
		}
	}
	s.SetTrailer(trailer)
	if len(header) > 0 {
		if err := s.SendHeader(header); err != nil {
			return err
		}
	}
	return s.SendMsg(&out)
}

// compositeStart forwards exactly one original RPC; it never substitutes a
// separate StartWorkflowExecution and thereby weakens update-with-start atomicity.
func (b *barrier) compositeStart(ctx context.Context, s grpc.ServerStream, conn *grpc.ClientConn, method string, request wire, r *workflow.StartWorkflowExecutionRequest) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		response        wire
		header, trailer metadata.MD
		err             error
	}
	done := make(chan result, 1)
	_, err := b.admitWithStart(ctx, r, func(ctx context.Context, r *workflow.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error) {
		// A signal-with-start retry must not adopt an older execution as new evidence.
		_, err := b.client.DescribeWorkflowExecution(ctx, &workflow.DescribeWorkflowExecutionRequest{Namespace: r.Namespace, Execution: &common.WorkflowExecution{WorkflowId: r.WorkflowId}})
		if status.Code(err) != codes.NotFound {
			return nil, fmt.Errorf("composite root must be absent before start: %v", err)
		}
		go func() {
			var out result
			out.err = conn.Invoke(ctx, method, &request, &out.response, grpc.ForceCodec(codec{}), grpc.Header(&out.header), grpc.Trailer(&out.trailer))
			done <- out
		}()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			d, err := b.client.DescribeWorkflowExecution(ctx, &workflow.DescribeWorkflowExecutionRequest{Namespace: r.Namespace, Execution: &common.WorkflowExecution{WorkflowId: r.WorkflowId}})
			if err == nil {
				if d.GetWorkflowExecutionInfo().GetStatus() != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
					return nil, fmt.Errorf("composite root is not Running")
				}
				id := d.GetWorkflowExecutionInfo().GetExecution().GetRunId()
				if id == "" {
					return nil, fmt.Errorf("composite root missing exact run ID")
				}
				return &workflow.StartWorkflowExecutionResponse{RunId: id}, nil
			}
			if status.Code(err) != codes.NotFound {
				return nil, err
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case out := <-done:
				// A successful completed RPC is also observed via Describe, never guessed.
				done <- out
				if out.err != nil {
					return nil, out.err
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-ticker.C:
				}
			case <-ticker.C:
			}
		}
	})
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case out := <-done:
		s.SetTrailer(out.trailer)
		if len(out.header) > 0 {
			if err := s.SendHeader(out.header); err != nil {
				return err
			}
		}
		if out.err != nil {
			return out.err
		}
		return s.SendMsg(&out.response)
	}
}

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "local listening address")
	upstream := flag.String("upstream", "", "real Temporal address")
	namespace := flag.String("namespace", "xenon-ministack", "namespace")
	size := flag.Int("concurrency", 4, "admission window: 1 or 4")
	receipt := flag.String("receipt", "", "exclusive new receipt path")
	flag.Parse()
	if *upstream == "" || *receipt == "" || (*size != 1 && *size != 4) {
		panic("upstream, receipt and concurrency 1 or 4 required")
	}
	f, err := os.OpenFile(*receipt, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	f.Close()
	conn, err := grpc.NewClient(*upstream, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	b := &barrier{client: workflow.NewWorkflowServiceClient(conn), namespace: *namespace, size: *size, queues: map[string]*window{}, failed: make(chan struct{})}
	b.save = func(w []*window) error {
		raw, err := json.MarshalIndent(struct {
			Schema      int       `json:"schema"`
			Concurrency int       `json:"concurrency"`
			Windows     []*window `json:"windows"`
		}{1, *size, w}, "", "  ")
		if err != nil {
			return err
		}
		if err = os.WriteFile(*receipt+".tmp", append(raw, '\n'), 0600); err != nil {
			return err
		}
		return os.Rename(*receipt+".tmp", *receipt)
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		panic(err)
	}
	if err = json.NewEncoder(os.Stdout).Encode(map[string]string{"address": listener.Addr().String()}); err != nil {
		panic(err)
	}
	server := grpc.NewServer(grpc.ForceServerCodec(codec{}), grpc.UnknownServiceHandler(b.relay(conn)))
	if err = server.Serve(listener); err != nil {
		panic(err)
	}
}
