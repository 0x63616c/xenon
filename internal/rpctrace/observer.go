package rpctrace

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/0x63616c/xenon/internal/proof/recorder"
	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Observer is an explicit proof-only producer. The controller opens/closes its
// fixed phase and observes process termination independently.
type Observer struct {
	endpoint, producer, phase string
	client                    *http.Client
	serial                    chan struct{}
	sequence                  uint64
	failed                    atomic.Bool
	active                    atomic.Int64
}

func NewObserver(endpoint, producer, phase string) (*Observer, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
		return nil, fmt.Errorf("invalid recorder endpoint")
	}
	host, _, err := net.SplitHostPort(u.Host)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return nil, fmt.Errorf("numeric loopback recorder required")
	}
	// Validate all identity fields through the same strict recorder grammar.
	state := recorder.NewState(1)
	if _, err = state.Apply(recorder.Event{Kind: "open", Phase: phase, Status: "steady"}); err != nil {
		return nil, err
	}
	if _, err = state.Apply(recorder.Event{Kind: "register", Phase: phase, Producer: producer, ID: "validate", Sequence: 1, Family: "validate", Measurement: "rpc_invocation"}); err != nil {
		return nil, err
	}
	return &Observer{endpoint: endpoint + "/event", producer: producer, phase: phase, client: &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil, MaxConnsPerHost: 4}}, serial: make(chan struct{}, 1)}, nil
}
func (o *Observer) Failure() error {
	if o.failed.Load() || o.active.Load() != 0 {
		return serviceerror.NewUnavailable("RPC measurement incomplete")
	}
	return nil
}
func measurementError() error { return serviceerror.NewUnavailable("RPC measurement unavailable") }
func (o *Observer) post(ctx context.Context, e recorder.Event) error {
	raw, err := json.Marshal(e)
	if err != nil {
		o.failed.Store(true)
		return measurementError()
	}
	// Exactly two identical deliveries share one bounded budget. A dropped ACK can
	// be reconciled by recorder idempotence, never by assigning a new identity.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", o.endpoint, bytes.NewReader(raw))
		if err != nil {
			break
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := o.client.Do(req)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == 204 {
				return nil
			}
			break
		}
		if ctx.Err() != nil {
			break
		}
	}
	o.failed.Store(true)
	return measurementError()
}
func (o *Observer) register(ctx context.Context, e recorder.Event) (recorder.Event, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	select {
	case o.serial <- struct{}{}:
		defer func() { <-o.serial }()
	case <-ctx.Done():
		o.failed.Store(true)
		return e, measurementError()
	}
	if o.failed.Load() {
		return e, measurementError()
	}
	o.sequence++
	e.Sequence = o.sequence
	e.Producer = o.producer
	e.Phase = o.phase
	e.ID = uuid.NewString()
	e.Kind = "register"
	if err := o.post(ctx, e); err != nil {
		return e, err
	}
	return e, nil
}

type observedInvocation struct {
	o            *Observer
	registration recorder.Event
}
type observerKey struct{}
type observedKey struct{}

func WithObserver(ctx context.Context, o *Observer) context.Context {
	return context.WithValue(ctx, observerKey{}, o)
}

var observerOnce sync.Once
var globalObserver *Observer
var observerError error

func configuredObserver() (*Observer, error) {
	observerOnce.Do(func() {
		endpoint := os.Getenv("XENON_RPC_RECORDER_URL")
		if endpoint == "" {
			if os.Getenv("XENON_RPC_RECORDER_PRODUCER") != "" || os.Getenv("XENON_RPC_RECORDER_PHASE") != "" {
				observerError = fmt.Errorf("incomplete recorder configuration")
			}
			return
		}
		if os.Getenv("XENON_RPC_TRACE_PATH") != "" {
			observerError = fmt.Errorf("trace modes are mutually exclusive")
			return
		}
		globalObserver, observerError = NewObserver(endpoint, os.Getenv("XENON_RPC_RECORDER_PRODUCER"), os.Getenv("XENON_RPC_RECORDER_PHASE"))
	})
	return globalObserver, observerError
}

// BeginObserved preserves the legacy async sink when no external recorder is set.
// Its timer includes registration waiting/ACK overhead, excludes terminal delivery.
func BeginObserved(ctx context.Context, family string) (context.Context, func(error) error, error) {
	o, explicit := ctx.Value(observerKey{}).(*Observer)
	if explicit {
		if _, conflict := ctx.Value(sinkKey{}).(*Sink); conflict {
			return ctx, nil, measurementError()
		}
	}
	if !explicit {
		var err error
		o, err = configuredObserver()
		if err != nil {
			return ctx, nil, measurementError()
		}
	}
	if o != nil {
		if _, conflict := ctx.Value(sinkKey{}).(*Sink); conflict {
			return ctx, nil, measurementError()
		}
		if os.Getenv("XENON_RPC_TRACE_PATH") != "" {
			return ctx, nil, measurementError()
		}
	}
	if o == nil {
		next, finish := Begin(ctx, family)
		return next, func(err error) error { finish(err); return err }, nil
	}
	start := time.Now()
	r, err := o.register(ctx, recorder.Event{Measurement: "rpc_invocation", Family: family})
	if err != nil {
		return ctx, nil, err
	}
	handshake := time.Since(start).Nanoseconds()
	o.active.Add(1)
	v := &observedInvocation{o: o, registration: r}
	return context.WithValue(ctx, observedKey{}, v), func(result error) error {
		defer o.active.Add(-1)
		e := recorder.Event{Kind: "terminal", Phase: r.Phase, Producer: r.Producer, ID: r.ID, Sequence: r.Sequence, Status: "completed", ResultStatus: rpcStatus(result), DurationNS: time.Since(start).Nanoseconds(), HandshakeNS: handshake}
		// Terminal reporting has its own bounded budget even if invocation ctx expired.
		if err := o.post(context.Background(), e); err != nil {
			return err
		}
		return result
	}, nil
}
func rpcStatus(err error) string {
	if err == context.Canceled {
		return codes.Canceled.String()
	}
	if err == context.DeadlineExceeded {
		return codes.DeadlineExceeded.String()
	}
	return serviceerror.ToStatus(err).Code().String()
}
func observedUnary(ctx context.Context, v *observedInvocation, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	start := time.Now()
	e := recorder.Event{Measurement: "execute_attempt", ParentID: v.registration.ID, Family: v.registration.Family, Method: method}
	if q, ok := req.(interface {
		GetPartition() string
		GetOperationId() string
	}); ok {
		e.Partition = q.GetPartition()
		e.OperationID = q.GetOperationId()
	}
	r, err := v.o.register(ctx, e)
	if err != nil {
		return status.Error(codes.Unavailable, "RPC measurement unavailable")
	}
	handshake := time.Since(start).Nanoseconds()
	result := invoke(ctx, method, req, reply, cc, opts...)
	end := recorder.Event{Kind: "terminal", Phase: r.Phase, Producer: r.Producer, ID: r.ID, Sequence: r.Sequence, Status: "completed", ResultStatus: rpcStatus(result), DurationNS: time.Since(start).Nanoseconds(), HandshakeNS: handshake}
	if err = v.o.post(context.Background(), end); err != nil {
		return status.Error(codes.Unavailable, "RPC measurement unavailable")
	}
	return result
}
