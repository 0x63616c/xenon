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
type Router struct {
	Node        string
	Directory   Resolver
	Local       Local
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
		for k, c := range r.connections {
			_ = c.Close()
			delete(r.connections, k)
			break
		}
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
			if e != nil {
				return nil, e
			}
			if route.Node == "" || route.Address == "" {
				return nil, status.Error(codes.Unavailable, "no ready partition owner")
			}
			if route.Node == r.Node {
				response, e := r.Local(ctx, info.FullMethod, req)
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
			c, e := r.connection(route.Address)
			if e != nil {
				return nil, e
			}
			// Copy metadata rather than nesting the incoming routing counter. The same
			// message instance preserves operation identity/digest and opaque payloads.
			md, _ := metadata.FromIncomingContext(ctx)
			md = md.Copy()
			md.Set(hopHeader, strconv.Itoa(hops+1))
			e = c.Invoke(metadata.NewOutgoingContext(ctx, md), info.FullMethod, req, response)
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
