// Package routing forwards complete persistence operations. Resolver and Local
// must be backed by an ownership manager; this package does not grant authority.
package routing

import (
	"context"
	"strconv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const hopHeader = "x-xenon-forward-hops"
const maxHops = 2

type Route struct{ Node, Address string }

// Resolve must honor ctx. refresh bypasses disposable caches. Only ready owners
// may be returned. Local dispatch must independently enforce ownership admission.
type Resolver interface {
	Resolve(ctx context.Context, partition string, refresh bool) (Route, error)
}
type Local func(context.Context, string, proto.Message) (proto.Message, error)

// Invoke sends one complete operation. Nil uses the production gRPC transport.
// Implementations honor ctx and fill response without modifying request. Their
// lifecycle belongs to the caller; Router.Close closes only its own gRPC pool.
type Invoke func(ctx context.Context, address, method string, request, response proto.Message) error

// Event contains only bounded routing categories, never operation IDs or payloads.
// Attempt is zero-based and Code is the resulting gRPC status.
type Event struct {
	Kind    string // resolve, local, forward
	Attempt int
	Code    codes.Code
}

type Router struct {
	Node      string
	Directory Resolver
	Local     Local
	// Configure before serving; do not mutate while requests are active.
	Invoke Invoke
	// Events is optional, caller-owned, and must remain open while serving.
	// Delivery is nonblocking and lossy: exporters cannot stall routing.
	Events      chan<- Event
	mu          sync.Mutex
	connections map[string]*grpc.ClientConn
	closed      bool
}

// Close rejects new admissions and closes outbound connections. Already admitted
// local work retains its caller deadline and ownership lifecycle.
func (r *Router) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for _, c := range r.connections {
		_ = c.Close()
	}
	r.connections = nil
	return nil
}
func (r *Router) connection(address string) (*grpc.ClientConn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, status.Error(codes.Unavailable, "router closed")
	}
	if c := r.connections[address]; c != nil {
		return c, nil
	}
	// Keep a bounded disposable pool even when owner addresses change repeatedly.
	if len(r.connections) >= 64 {
		// Stable lexical eviction avoids map iteration affecting routing effects.
		var victim string
		for key := range r.connections {
			if victim == "" || key < victim {
				victim = key
			}
		}
		_ = r.connections[victim].Close()
		delete(r.connections, victim)
	}
	c, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		return nil, e
	}
	if r.connections == nil {
		r.connections = make(map[string]*grpc.ClientConn)
	}
	r.connections[address] = c
	return c, nil
}

func (r *Router) emit(kind string, attempt int, err error) {
	if r.Events != nil {
		select {
		case r.Events <- Event{Kind: kind, Attempt: attempt, Code: status.Code(err)}:
		default:
		}
	}
}

// Interceptor operates only on protobuf messages exposing stable GetPartition.
// reply creates the concrete response for a registered full RPC method.
func (r *Router) Interceptor(reply func(string) proto.Message) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		r.mu.Lock()
		closed := r.closed
		r.mu.Unlock()
		if closed {
			return nil, status.Error(codes.Unavailable, "router closed")
		}
		if r.Node == "" || r.Directory == nil || r.Local == nil {
			return nil, status.Error(codes.Unavailable, "router not configured")
		}
		req, ok := request.(interface {
			proto.Message
			GetPartition() string
		})
		if !ok || req.GetPartition() == "" {
			return nil, status.Error(codes.InvalidArgument, "missing logical partition")
		}
		if _, ok := ctx.Deadline(); !ok {
			return nil, status.Error(codes.InvalidArgument, "persistence deadline required")
		}
		hops := 0
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			values := md.Get(hopHeader)
			if len(values) > 1 {
				return nil, status.Error(codes.InvalidArgument, "invalid forwarding hop count")
			}
			if len(values) == 1 {
				n, e := strconv.Atoi(values[0])
				if e != nil || n < 0 || n > maxHops {
					return nil, status.Error(codes.Unavailable, "forwarding hop limit")
				}
				hops = n
			}
		}
		for attempt := 0; attempt < 2; attempt++ {
			if e := ctx.Err(); e != nil {
				return nil, status.FromContextError(e).Err()
			}
			route, e := r.Directory.Resolve(ctx, req.GetPartition(), attempt > 0)
			r.emit("resolve", attempt, e)
			if e != nil {
				return nil, e
			}
			if route.Node == "" || route.Address == "" {
				return nil, status.Error(codes.Unavailable, "no ready partition owner")
			}
			if route.Node == r.Node {
				response, e := r.Local(ctx, info.FullMethod, req)
				r.emit("local", attempt, e)
				if status.Code(e) == codes.Unavailable && attempt == 0 {
					continue
				}
				return response, e
			}
			if hops >= maxHops {
				return nil, status.Error(codes.Unavailable, "forwarding hop limit")
			}
			response := reply(info.FullMethod)
			if response == nil {
				return nil, status.Error(codes.Unimplemented, "unknown persistence method")
			}
			invoke := r.Invoke
			if invoke == nil {
				c, err := r.connection(route.Address)
				if err != nil {
					return nil, err
				}
				invoke = func(ctx context.Context, _, method string, request, response proto.Message) error {
					return c.Invoke(ctx, method, request, response)
				}
			}
			// Copy metadata rather than nesting the incoming routing counter. The same
			// message instance preserves operation identity/digest and opaque payloads.
			md, _ := metadata.FromIncomingContext(ctx)
			md = md.Copy()
			md.Set(hopHeader, strconv.Itoa(hops+1))
			e = invoke(metadata.NewOutgoingContext(ctx, md), route.Address, info.FullMethod, req, response)
			r.emit("forward", attempt, e)
			if e == nil {
				return response, nil
			}
			if status.Code(e) != codes.Unavailable || attempt == 1 {
				return nil, e
			}
		}
		return nil, status.Error(codes.Unavailable, "routing attempts exhausted")
	}
}
