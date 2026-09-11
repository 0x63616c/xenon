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
}

func (b *barrier) fail(err error) error {
	if b.failure == nil && b.failed != nil {
		close(b.failed)
	}
	b.failure = err
	return status.Errorf(codes.FailedPrecondition, "admission proof failed: %v", err)
}
func (b *barrier) admit(ctx context.Context, r *workflow.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failure != nil {
		return nil, b.fail(b.failure)
	}
	if !generatedQueue.MatchString(r.GetTaskQueue().GetName()) {
		return nil, b.fail(fmt.Errorf("invalid generated root queue"))
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
	out, err := b.client.StartWorkflowExecution(ctx, r)
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
		if strings.HasSuffix(method, "/PollWorkflowTaskQueue") {
			var r workflow.PollWorkflowTaskQueueRequest
			if err := proto.Unmarshal(request, &r); err != nil {
				return err
			}
			if strings.HasPrefix(r.GetTaskQueue().GetName(), "omes-xenon-generated-") {
				if r.Namespace != b.namespace {
					return status.Error(codes.FailedPrecondition, "unexpected poll namespace")
				}
				if err := b.wait(ctx, r.TaskQueue.Name); err != nil {
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
