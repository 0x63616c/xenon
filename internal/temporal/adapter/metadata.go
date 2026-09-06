package adapter

import (
	"context"
	"crypto/sha256"
	"github.com/0x63616c/xenon/internal/rpctrace"
	"time"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// MetadataStore implements the pinned namespace catalog contract in one control partition.
type MetadataStore struct {
	operations        *operationIDs
	connection        *grpc.ClientConn
	client            wire.MetadataPersistenceClient
	partition         string
	invocationTimeout time.Duration
}

var _ persistence.MetadataStore = (*MetadataStore)(nil)

func NewMetadataStore(address, partition string, options ...StoreOption) (*MetadataStore, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithChainUnaryInterceptor(rpctrace.Unary))
	if err != nil {
		return nil, err
	}
	return &MetadataStore{operations: newOperationIDs(options...), connection: conn, client: wire.NewMetadataPersistenceClient(conn), partition: partition, invocationTimeout: 30 * time.Second}, nil
}
func (s *MetadataStore) Close()          { _ = s.connection.Close() }
func (s *MetadataStore) GetName() string { return "xenon" }

func namespaceID(id string) ([]byte, error) {
	value, err := uuid.Parse(id)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument("invalid namespace UUID")
	}
	return value[:], nil
}
func (s *MetadataStore) invokeMetadata(ctx context.Context, command *wire.MetadataCommand) (traceResult *wire.MetadataResult, traceErr error) {
	ctx, traceFinish, traceBeginErr := rpctrace.BeginObserved(ctx, "metadata")
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
	request := &wire.MetadataRequest{ProtocolVersion: 1, Partition: s.partition, OperationId: operation, CommandSha256: digest[:], Command: command}
	result, callErr := retryOperation(ctx, func(ctx context.Context) (*wire.MetadataResult, error) { return s.client.Execute(ctx, request) })
	if callErr != nil {
		return nil, callErr
	}

	return result, metadataError(result)

}
func metadataError(result *wire.MetadataResult) error {
	switch result.Error {
	case wire.MetadataResult_NONE:
		return nil
	case wire.MetadataResult_NOT_FOUND:
		return serviceerror.NewNamespaceNotFound(result.Message)
	case wire.MetadataResult_ALREADY_EXISTS:
		return serviceerror.NewNamespaceAlreadyExists(result.Message)
	// Pinned SQL metadata txExecute converts stale catalog versions and rename
	// uniqueness failures into Unavailable, rather than ConditionFailedError.
	case wire.MetadataResult_CONDITION_FAILED, wire.MetadataResult_UNAVAILABLE:
		return serviceerror.NewUnavailable(result.Message)
	default:
		return serviceerror.NewInternal("unknown Xenon metadata error")
	}
}
func namespaceResponse(record *wire.NamespaceRecord) *persistence.InternalGetNamespaceResponse {
	return &persistence.InternalGetNamespaceResponse{Namespace: &commonpb.DataBlob{Data: record.Data, EncodingType: enumspb.EncodingType(record.Encoding)}, IsGlobal: record.IsGlobal, NotificationVersion: record.NotificationVersion}
}
func (s *MetadataStore) CreateNamespace(ctx context.Context, r *persistence.InternalCreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
	if r == nil || r.Namespace == nil {
		return nil, serviceerror.NewInvalidArgument("nil namespace request/blob")
	}
	id, err := namespaceID(r.ID)
	if err != nil {
		return nil, err
	}
	_, err = s.invokeMetadata(ctx, &wire.MetadataCommand{Kind: wire.MetadataCommand_CREATE, Id: id, Name: r.Name, Data: r.Namespace.Data, Encoding: int32(r.Namespace.EncodingType), IsGlobal: r.IsGlobal})
	if err != nil {
		return nil, err
	}
	return &persistence.CreateNamespaceResponse{ID: r.ID}, nil
}
func (s *MetadataStore) GetNamespace(ctx context.Context, r *persistence.GetNamespaceRequest) (*persistence.InternalGetNamespaceResponse, error) {
	if r == nil || (r.ID == "") == (r.Name == "") {
		return nil, serviceerror.NewInvalidArgument("exactly one namespace ID or name is required")
	}
	command := &wire.MetadataCommand{Kind: wire.MetadataCommand_GET, Name: r.Name}
	if r.ID != "" {
		id, err := namespaceID(r.ID)
		if err != nil {
			return nil, err
		}
		command.Id = id
	}
	result, err := s.invokeMetadata(ctx, command)
	if err != nil {
		return nil, err
	}
	if len(result.Namespaces) != 1 {
		return nil, serviceerror.NewInternal("invalid namespace response cardinality")
	}
	return namespaceResponse(result.Namespaces[0]), nil
}
func (s *MetadataStore) updateNamespace(ctx context.Context, r *persistence.InternalUpdateNamespaceRequest, kind wire.MetadataCommand_Kind, previousName string) error {
	if r == nil || r.Namespace == nil {
		return serviceerror.NewInvalidArgument("nil namespace request/blob")
	}
	id, err := namespaceID(r.Id)
	if err != nil {
		return err
	}
	_, err = s.invokeMetadata(ctx, &wire.MetadataCommand{Kind: kind, Id: id, Name: r.Name, PreviousName: previousName, Data: r.Namespace.Data, Encoding: int32(r.Namespace.EncodingType), IsGlobal: r.IsGlobal, NotificationVersion: r.NotificationVersion})
	return err
}
func (s *MetadataStore) UpdateNamespace(ctx context.Context, r *persistence.InternalUpdateNamespaceRequest) error {
	return s.updateNamespace(ctx, r, wire.MetadataCommand_UPDATE, "")
}
func (s *MetadataStore) RenameNamespace(ctx context.Context, r *persistence.InternalRenameNamespaceRequest) error {
	if r == nil {
		return serviceerror.NewInvalidArgument("nil rename request")
	}
	return s.updateNamespace(ctx, r.InternalUpdateNamespaceRequest, wire.MetadataCommand_RENAME, r.PreviousName)
}
func (s *MetadataStore) DeleteNamespace(ctx context.Context, r *persistence.DeleteNamespaceRequest) error {
	if r == nil {
		return serviceerror.NewInvalidArgument("nil delete request")
	}
	id, err := namespaceID(r.ID)
	if err != nil {
		return err
	}
	_, err = s.invokeMetadata(ctx, &wire.MetadataCommand{Kind: wire.MetadataCommand_DELETE, Id: id})
	return err
}
func (s *MetadataStore) DeleteNamespaceByName(ctx context.Context, r *persistence.DeleteNamespaceByNameRequest) error {
	if r == nil {
		return serviceerror.NewInvalidArgument("nil delete request")
	}
	_, err := s.invokeMetadata(ctx, &wire.MetadataCommand{Kind: wire.MetadataCommand_DELETE_BY_NAME, Name: r.Name})
	return err
}
func (s *MetadataStore) ListNamespaces(ctx context.Context, r *persistence.InternalListNamespacesRequest) (*persistence.InternalListNamespacesResponse, error) {
	if r == nil || r.PageSize < 1 || r.PageSize > 1000 || (len(r.NextPageToken) != 0 && len(r.NextPageToken) != 16) {
		return nil, serviceerror.NewInvalidArgument("invalid namespace page size or token")
	}
	result, err := s.invokeMetadata(ctx, &wire.MetadataCommand{Kind: wire.MetadataCommand_LIST, PageSize: int32(r.PageSize), NextPageToken: r.NextPageToken})
	if err != nil {
		return nil, err
	}
	response := &persistence.InternalListNamespacesResponse{NextPageToken: result.NextPageToken}
	for _, record := range result.Namespaces {
		response.Namespaces = append(response.Namespaces, namespaceResponse(record))
	}
	return response, nil
}
func (s *MetadataStore) GetMetadata(ctx context.Context) (*persistence.GetMetadataResponse, error) {
	result, err := s.invokeMetadata(ctx, &wire.MetadataCommand{Kind: wire.MetadataCommand_GET_METADATA})
	if err != nil {
		return nil, err
	}
	return &persistence.GetMetadataResponse{NotificationVersion: result.NotificationVersion}, nil
}
