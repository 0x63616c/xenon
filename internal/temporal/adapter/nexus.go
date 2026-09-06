package adapter

import (
	"context"
	"crypto/sha256"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/rpctrace"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"time"
)

type NexusStore struct {
	operations        *operationIDs
	connection        *grpc.ClientConn
	client            wire.NexusPersistenceClient
	partition         string
	invocationTimeout time.Duration
}

var _ persistence.NexusEndpointStore = (*NexusStore)(nil)

func NewNexusStore(address, partition string, options ...StoreOption) (*NexusStore, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithChainUnaryInterceptor(rpctrace.Unary))
	if err != nil {
		return nil, err
	}
	return &NexusStore{operations: newOperationIDs(options...), connection: conn, client: wire.NewNexusPersistenceClient(conn), partition: partition, invocationTimeout: 30 * time.Second}, nil
}
func (s *NexusStore) Close()          { _ = s.connection.Close() }
func (s *NexusStore) GetName() string { return "xenon" }
func nexusError(r *wire.NexusResult) error {
	switch r.Error {
	case wire.NexusResult_NONE:
		return nil
	case wire.NexusResult_NOT_FOUND:
		return serviceerror.NewNotFound(r.Message)
	case wire.NexusResult_UNAVAILABLE:
		return serviceerror.NewUnavailable(r.Message)
	case wire.NexusResult_CONDITION_FAILED:
		return &persistence.ConditionFailedError{Msg: r.Message}
	case wire.NexusResult_RESOURCE_EXHAUSTED:
		return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, r.Message)
	default:
		return serviceerror.NewInternal(r.Message)
	}
}
func nexusID(id string) ([]byte, error) {
	v, err := uuid.Parse(id)
	if err != nil {
		return nil, serviceerror.NewInternal("invalid nexus endpoint UUID")
	}
	return v[:], nil
}
func nexusEndpoint(e *wire.NexusEndpoint) persistence.InternalNexusEndpoint {
	return persistence.InternalNexusEndpoint{ID: uuid.UUID(e.Id).String(), Version: e.Version, Data: &commonpb.DataBlob{Data: e.Data, EncodingType: enumspb.EncodingType(e.Encoding)}}
}
func (s *NexusStore) CreateOrUpdateNexusEndpoint(ctx context.Context, q *persistence.InternalCreateOrUpdateNexusEndpointRequest) error {
	if q == nil || q.Endpoint.Data == nil {
		return serviceerror.NewInvalidArgument("nil nexus request/blob")
	}
	id, err := nexusID(q.Endpoint.ID)
	if err != nil {
		return err
	}
	_, err = s.invokeNexus(ctx, &wire.NexusCommand{Kind: wire.NexusCommand_UPSERT, TableVersion: q.LastKnownTableVersion, Endpoint: &wire.NexusEndpoint{Id: id, Version: q.Endpoint.Version, Data: q.Endpoint.Data.Data, Encoding: int32(q.Endpoint.Data.EncodingType)}})
	return err
}
func (s *NexusStore) DeleteNexusEndpoint(ctx context.Context, q *persistence.DeleteNexusEndpointRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil nexus request")
	}
	id, err := nexusID(q.ID)
	if err != nil {
		return err
	}
	_, err = s.invokeNexus(ctx, &wire.NexusCommand{Kind: wire.NexusCommand_DELETE, Id: id, TableVersion: q.LastKnownTableVersion})
	return err
}
func (s *NexusStore) GetNexusEndpoint(ctx context.Context, q *persistence.GetNexusEndpointRequest) (*persistence.InternalNexusEndpoint, error) {
	if q == nil {
		return nil, serviceerror.NewInvalidArgument("nil nexus request")
	}
	id, err := nexusID(q.ID)
	if err != nil {
		return nil, err
	}
	r, err := s.invokeNexus(ctx, &wire.NexusCommand{Kind: wire.NexusCommand_GET, Id: id})
	if err != nil {
		return nil, err
	}
	if r.Endpoint == nil || len(r.Endpoint.Id) != 16 {
		return nil, serviceerror.NewInternal("invalid nexus response")
	}
	e := nexusEndpoint(r.Endpoint)
	return &e, nil
}
func (s *NexusStore) ListNexusEndpoints(ctx context.Context, q *persistence.ListNexusEndpointsRequest) (*persistence.InternalListNexusEndpointsResponse, error) {
	if q == nil {
		return nil, serviceerror.NewInvalidArgument("nil nexus request")
	}
	r, err := s.invokeNexus(ctx, &wire.NexusCommand{Kind: wire.NexusCommand_LIST, TableVersion: q.LastKnownTableVersion, PageSize: int64(q.PageSize), NextPageToken: q.NextPageToken})
	if r == nil {
		return nil, err
	}
	out := &persistence.InternalListNexusEndpointsResponse{TableVersion: r.TableVersion, NextPageToken: r.NextPageToken}
	if err != nil {
		return out, err
	}
	for _, e := range r.Endpoints {
		if len(e.Id) != 16 {
			return nil, serviceerror.NewInternal("invalid nexus ID")
		}
		out.Endpoints = append(out.Endpoints, nexusEndpoint(e))
	}
	return out, nil
}
func (s *NexusStore) invokeNexus(ctx context.Context, command *wire.NexusCommand) (traceResult *wire.NexusResult, traceErr error) {
	ctx, traceFinish, traceBeginErr := rpctrace.BeginObserved(ctx, "nexus")
	if traceBeginErr != nil {
		return nil, traceBeginErr
	}
	defer func() { traceErr = traceFinish(traceErr) }()
	ctx, cancel := context.WithTimeout(ctx, s.invocationTimeout)
	defer cancel()
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	operation, operationErr := s.operations.next()
	if operationErr != nil {
		return nil, operationErr
	}
	request := &wire.NexusRequest{ProtocolVersion: 1, Partition: s.partition, OperationId: operation, CommandSha256: digest[:], Command: command}
	result, callErr := retryOperation(ctx, func(ctx context.Context) (*wire.NexusResult, error) { return s.client.Execute(ctx, request) })
	if callErr != nil {
		return nil, callErr
	}

	return result, nexusError(result)

}
