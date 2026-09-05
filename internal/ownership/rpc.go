package ownership

import (
	"context"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/node"
	"github.com/0x63616c/xenon/internal/routing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Server binds every registered family to the same manager and owner admission.
func (m *Manager) Server() (*grpc.Server, *routing.Router) {
	r := &routing.Router{Node: m.identity.Node + "/" + m.identity.Incarnation, Directory: m, Local: m.dispatch}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(2*1024*1024), grpc.UnaryInterceptor(r.Interceptor(reply)))
	wire.RegisterShardPersistenceServer(server, &wire.UnimplementedShardPersistenceServer{})
	wire.RegisterQueuePersistenceServer(server, &wire.UnimplementedQueuePersistenceServer{})
	wire.RegisterQueueV2PersistenceServer(server, &wire.UnimplementedQueueV2PersistenceServer{})
	wire.RegisterHistoryPersistenceServer(server, &wire.UnimplementedHistoryPersistenceServer{})
	wire.RegisterMetadataPersistenceServer(server, &wire.UnimplementedMetadataPersistenceServer{})
	wire.RegisterMatchingPersistenceServer(server, &wire.UnimplementedMatchingPersistenceServer{})
	wire.RegisterClusterPersistenceServer(server, &wire.UnimplementedClusterPersistenceServer{})
	wire.RegisterNexusPersistenceServer(server, &wire.UnimplementedNexusPersistenceServer{})
	wire.RegisterExecutionPersistenceServer(server, &wire.UnimplementedExecutionPersistenceServer{})
	wire.RegisterHistoryTasksPersistenceServer(server, &wire.UnimplementedHistoryTasksPersistenceServer{})
	wire.RegisterExecutionTasksPersistenceServer(server, &wire.UnimplementedExecutionTasksPersistenceServer{})
	return server, r
}

type dispatchMethod struct {
	response func() proto.Message
	execute  func(context.Context, *node.Owner, proto.Message) (proto.Message, error)
}

var methods = map[string]dispatchMethod{
	wire.ExecutionTasksPersistence_Execute_FullMethodName: {
		response: func() proto.Message { return &wire.ExecutionTasksResult{} },
		execute: func(ctx context.Context, o *node.Owner, q proto.Message) (proto.Message, error) {
			request, ok := q.(*wire.ExecutionTasksRequest)
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "wrong request type")
			}
			return (&node.ExecutionTasksServer{Owner: o}).Execute(ctx, request)
		},
	},

	wire.HistoryTasksPersistence_Execute_FullMethodName: {
		response: func() proto.Message { return &wire.HistoryTasksResult{} },
		execute: func(ctx context.Context, o *node.Owner, q proto.Message) (proto.Message, error) {
			request, ok := q.(*wire.HistoryTasksRequest)
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "wrong request type")
			}
			return (&node.HistoryTasksServer{Owner: o}).Execute(ctx, request)
		},
	},

	wire.ExecutionPersistence_Execute_FullMethodName: {
		response: func() proto.Message { return &wire.ExecutionResult{} },
		execute: func(ctx context.Context, o *node.Owner, q proto.Message) (proto.Message, error) {
			request, ok := q.(*wire.ExecutionRequest)
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "wrong request type")
			}
			return (&node.ExecutionServer{Owner: o}).Execute(ctx, request)
		},
	},

	wire.ShardPersistence_Execute_FullMethodName: {
		response: func() proto.Message { return &wire.ShardResult{} },
		execute: func(ctx context.Context, o *node.Owner, q proto.Message) (proto.Message, error) {
			request, ok := q.(*wire.ShardRequest)
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "wrong request type")
			}
			return o.Execute(ctx, request)
		},
	},
	wire.QueuePersistence_Execute_FullMethodName: {
		response: func() proto.Message { return &wire.QueueResult{} },
		execute: func(ctx context.Context, o *node.Owner, q proto.Message) (proto.Message, error) {
			request, ok := q.(*wire.QueueRequest)
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "wrong request type")
			}
			return (&node.QueueServer{Owner: o}).Execute(ctx, request)
		},
	},
	wire.QueueV2Persistence_Execute_FullMethodName: {
		response: func() proto.Message { return &wire.QueueV2Result{} },
		execute: func(ctx context.Context, o *node.Owner, q proto.Message) (proto.Message, error) {
			request, ok := q.(*wire.QueueV2Request)
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "wrong request type")
			}
			return (&node.QueueV2Server{Owner: o}).Execute(ctx, request)
		},
	},
	wire.HistoryPersistence_Execute_FullMethodName: {
		response: func() proto.Message { return &wire.HistoryResult{} },
		execute: func(ctx context.Context, o *node.Owner, q proto.Message) (proto.Message, error) {
			request, ok := q.(*wire.HistoryRequest)
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "wrong request type")
			}
			return (&node.HistoryServer{Owner: o}).Execute(ctx, request)
		},
	},
	wire.MetadataPersistence_Execute_FullMethodName: {
		response: func() proto.Message { return &wire.MetadataResult{} },
		execute: func(ctx context.Context, o *node.Owner, q proto.Message) (proto.Message, error) {
			request, ok := q.(*wire.MetadataRequest)
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "wrong request type")
			}
			return (&node.MetadataServer{Owner: o}).Execute(ctx, request)
		},
	},
	wire.MatchingPersistence_Execute_FullMethodName: {
		response: func() proto.Message { return &wire.MatchingResult{} },
		execute: func(ctx context.Context, o *node.Owner, q proto.Message) (proto.Message, error) {
			request, ok := q.(*wire.MatchingRequest)
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "wrong request type")
			}
			return (&node.MatchingServer{Owner: o}).Execute(ctx, request)
		},
	},
	wire.ClusterPersistence_Execute_FullMethodName: {
		response: func() proto.Message { return &wire.ClusterResult{} },
		execute: func(ctx context.Context, o *node.Owner, q proto.Message) (proto.Message, error) {
			request, ok := q.(*wire.ClusterRequest)
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "wrong request type")
			}
			return (&node.ClusterServer{Owner: o}).Execute(ctx, request)
		},
	},
	wire.NexusPersistence_Execute_FullMethodName: {
		response: func() proto.Message { return &wire.NexusResult{} },
		execute: func(ctx context.Context, o *node.Owner, q proto.Message) (proto.Message, error) {
			request, ok := q.(*wire.NexusRequest)
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "wrong request type")
			}
			return (&node.NexusServer{Owner: o}).Execute(ctx, request)
		},
	},
}

func reply(method string) proto.Message {
	entry, ok := methods[method]
	if !ok {
		return nil
	}
	return entry.response()
}
func (m *Manager) dispatch(ctx context.Context, method string, request proto.Message) (proto.Message, error) {
	entry, ok := methods[method]
	if !ok {
		return nil, status.Error(codes.Unimplemented, "unknown persistence method")
	}
	p, ok := request.(interface{ GetPartition() string })
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "missing partition")
	}
	owner, e := m.Owner(p.GetPartition())
	if e != nil {
		return nil, e
	}

	id := p.GetPartition()
	m.mu.Lock()
	counts := m.dispatchCounts[id]
	counts.Attempts++
	m.dispatchCounts[id] = counts
	m.mu.Unlock()
	result, err := entry.execute(ctx, owner, request)
	if err == nil && result != nil {
		m.mu.Lock()
		counts = m.dispatchCounts[id]
		counts.RPCResults++
		ref := result.ProtoReflect()
		field := ref.Descriptor().Fields().ByName("error")
		if field != nil && ref.Get(field).Enum() == 0 {
			counts.SuccessfulOperations++
		}
		m.dispatchCounts[id] = counts
		m.mu.Unlock()
	}
	return result, err
}
