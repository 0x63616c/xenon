package routing

import (
	"context"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/persistence"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// NewServer assembles all persistence families on the partition service runtime.
// The caller serves/stops the server and closes the returned router's pool.
func NewServer(config ServerConfig) (*grpc.Server, *Router, error) {
	binding, err := newBinding(config)
	if err != nil {
		return nil, nil, err
	}
	router := &Router{Node: string(config.Owner.Node) + "/" + string(config.Owner.Incarnation), Directory: binding, Local: binding.dispatch, RequireAuthority: true, Events: config.Events}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(2*1024*1024), grpc.UnaryInterceptor(router.Interceptor(serviceReply)))
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
	wire.RegisterVisibilityPersistenceServer(server, &wire.UnimplementedVisibilityPersistenceServer{})
	return server, router, nil
}

type serviceMethod struct {
	response func() proto.Message
	execute  func(context.Context, *binding, *persistence.Service, proto.Message) (proto.Message, error)
}

var serviceMethods = map[string]serviceMethod{
	wire.ShardPersistence_Execute_FullMethodName: {response: func() proto.Message { return &wire.ShardResult{} }, execute: func(ctx context.Context, b *binding, base *persistence.Service, q proto.Message) (proto.Message, error) {
		request, ok := q.(*wire.ShardRequest)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "wrong request type")
		}
		return base.Execute(ctx, request)
	}},
	wire.QueuePersistence_Execute_FullMethodName: {response: func() proto.Message { return &wire.QueueResult{} }, execute: func(ctx context.Context, b *binding, base *persistence.Service, q proto.Message) (proto.Message, error) {
		request, ok := q.(*wire.QueueRequest)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "wrong request type")
		}
		service, err := persistence.NewQueueService(base)
		if err != nil {
			return nil, err
		}
		return service.Execute(ctx, request)
	}},
	wire.QueueV2Persistence_Execute_FullMethodName: {response: func() proto.Message { return &wire.QueueV2Result{} }, execute: func(ctx context.Context, b *binding, base *persistence.Service, q proto.Message) (proto.Message, error) {
		request, ok := q.(*wire.QueueV2Request)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "wrong request type")
		}
		service, err := persistence.NewQueueV2Service(base)
		if err != nil {
			return nil, err
		}
		return service.Execute(ctx, request)
	}},
	wire.HistoryPersistence_Execute_FullMethodName: {response: func() proto.Message { return &wire.HistoryResult{} }, execute: func(ctx context.Context, b *binding, base *persistence.Service, q proto.Message) (proto.Message, error) {
		request, ok := q.(*wire.HistoryRequest)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "wrong request type")
		}
		service, err := persistence.NewHistoryService(base)
		if err != nil {
			return nil, err
		}
		return service.Execute(ctx, request)
	}},
	wire.MetadataPersistence_Execute_FullMethodName: {response: func() proto.Message { return &wire.MetadataResult{} }, execute: func(ctx context.Context, b *binding, base *persistence.Service, q proto.Message) (proto.Message, error) {
		request, ok := q.(*wire.MetadataRequest)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "wrong request type")
		}
		service, err := persistence.NewMetadataService(base)
		if err != nil {
			return nil, err
		}
		return service.Execute(ctx, request)
	}},
	wire.MatchingPersistence_Execute_FullMethodName: {response: func() proto.Message { return &wire.MatchingResult{} }, execute: func(ctx context.Context, b *binding, base *persistence.Service, q proto.Message) (proto.Message, error) {
		request, ok := q.(*wire.MatchingRequest)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "wrong request type")
		}
		service, err := persistence.NewMatchingService(base)
		if err != nil {
			return nil, err
		}
		return service.Execute(ctx, request)
	}},
	wire.ClusterPersistence_Execute_FullMethodName: {response: func() proto.Message { return &wire.ClusterResult{} }, execute: func(ctx context.Context, b *binding, base *persistence.Service, q proto.Message) (proto.Message, error) {
		request, ok := q.(*wire.ClusterRequest)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "wrong request type")
		}
		service, err := persistence.NewClusterService(base, b.config.Now)
		if err != nil {
			return nil, err
		}
		return service.Execute(ctx, request)
	}},
	wire.NexusPersistence_Execute_FullMethodName: {response: func() proto.Message { return &wire.NexusResult{} }, execute: func(ctx context.Context, b *binding, base *persistence.Service, q proto.Message) (proto.Message, error) {
		request, ok := q.(*wire.NexusRequest)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "wrong request type")
		}
		service, err := persistence.NewNexusService(base)
		if err != nil {
			return nil, err
		}
		return service.Execute(ctx, request)
	}},
	wire.ExecutionPersistence_Execute_FullMethodName: {response: func() proto.Message { return &wire.ExecutionResult{} }, execute: func(ctx context.Context, b *binding, base *persistence.Service, q proto.Message) (proto.Message, error) {
		request, ok := q.(*wire.ExecutionRequest)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "wrong request type")
		}
		service, err := persistence.NewExecutionService(base)
		if err != nil {
			return nil, err
		}
		return service.Execute(ctx, request)
	}},
	wire.HistoryTasksPersistence_Execute_FullMethodName: {response: func() proto.Message { return &wire.HistoryTasksResult{} }, execute: func(ctx context.Context, b *binding, base *persistence.Service, q proto.Message) (proto.Message, error) {
		request, ok := q.(*wire.HistoryTasksRequest)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "wrong request type")
		}
		service, err := persistence.NewHistoryTasksService(base)
		if err != nil {
			return nil, err
		}
		return service.Execute(ctx, request)
	}},
	wire.ExecutionTasksPersistence_Execute_FullMethodName: {response: func() proto.Message { return &wire.ExecutionTasksResult{} }, execute: func(ctx context.Context, b *binding, base *persistence.Service, q proto.Message) (proto.Message, error) {
		request, ok := q.(*wire.ExecutionTasksRequest)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "wrong request type")
		}
		service, err := persistence.NewExecutionTasksService(base)
		if err != nil {
			return nil, err
		}
		return service.Execute(ctx, request)
	}},
	wire.VisibilityPersistence_Execute_FullMethodName: {response: func() proto.Message { return &wire.VisibilityResult{} }, execute: func(ctx context.Context, b *binding, base *persistence.Service, q proto.Message) (proto.Message, error) {
		request, ok := q.(*wire.VisibilityRequest)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "wrong request type")
		}
		service, err := persistence.NewVisibilityService(base, b.config.Layout)
		if err != nil {
			return nil, err
		}
		return service.Execute(ctx, request)
	}},
}

func serviceReply(method string) proto.Message {
	entry, ok := serviceMethods[method]
	if !ok {
		return nil
	}
	return entry.response()
}
