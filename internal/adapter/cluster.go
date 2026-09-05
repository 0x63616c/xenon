package adapter

import (
	"context"
	"crypto/sha256"
	"net"
	"time"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ClusterStore transports complete control-partition operations. The storage
// handler must implement the pinned contract; this adapter does not emulate it.
type ClusterStore struct {
	connection        *grpc.ClientConn
	client            wire.ClusterPersistenceClient
	partition         string
	invocationTimeout time.Duration
}

var _ p.ClusterMetadataStore = (*ClusterStore)(nil)

func NewClusterStore(address, partition string) (*ClusterStore, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &ClusterStore{connection: conn, client: wire.NewClusterPersistenceClient(conn), partition: partition, invocationTimeout: 30 * time.Second}, nil
}
func (s *ClusterStore) Close() {
	if s.connection != nil {
		_ = s.connection.Close()
	}
}
func (s *ClusterStore) GetName() string { return "xenon" }
func (s *ClusterStore) invokeCluster(ctx context.Context, c *wire.ClusterCommand) (*wire.ClusterResult, error) {
	ctx, cancel := context.WithTimeout(ctx, s.invocationTimeout)
	defer cancel()
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(data)
	req := &wire.ClusterRequest{ProtocolVersion: 1, Partition: s.partition, OperationId: uuid.NewString(), CommandSha256: hash[:], Command: c}
	for attempt := 0; attempt < 3; attempt++ {
		r, e := s.client.Execute(ctx, req)
		if e == nil {
			if r == nil {
				return nil, serviceerror.NewInternal("nil cluster RPC response")
			}
			return r, clusterError(r)
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		switch status.Code(e) {
		case codes.DeadlineExceeded:
			return nil, context.DeadlineExceeded
		case codes.Canceled:
			return nil, context.Canceled
		}
		if status.Code(e) != codes.Unavailable || attempt == 2 {
			return nil, serviceerror.FromStatus(status.Convert(e))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 20 * time.Millisecond):
		}
	}
	panic("unreachable")
}
func clusterError(r *wire.ClusterResult) error {
	switch r.Error {
	case wire.ClusterResult_NONE:
		return nil
	case wire.ClusterResult_NOT_FOUND:
		return serviceerror.NewNotFound(r.Message)
	case wire.ClusterResult_UNAVAILABLE:
		return serviceerror.NewUnavailable(r.Message)
	case wire.ClusterResult_INTERNAL:
		return serviceerror.NewInternal(r.Message)
	case wire.ClusterResult_INVALID_ARGUMENT:
		return serviceerror.NewInvalidArgument(r.Message)
	case wire.ClusterResult_RESOURCE_EXHAUSTED:
		return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, r.Message)
	default:
		return serviceerror.NewInternal("unknown cluster logical error")
	}
}
func clusterInstant(t time.Time) *wire.ClusterTime {
	return &wire.ClusterTime{Seconds: t.Unix(), Nanos: int32(t.Nanosecond())}
}
func clusterTime(t *wire.ClusterTime) (time.Time, error) {
	if t == nil || t.Nanos < 0 || t.Nanos >= 1e9 {
		return time.Time{}, serviceerror.NewInternal("invalid cluster timestamp")
	}
	return time.Unix(t.Seconds, int64(t.Nanos)).UTC(), nil
}
func clusterRecord(r *wire.ClusterRecord) (*p.InternalGetClusterMetadataResponse, error) {
	if r == nil || r.Blob == nil {
		return nil, serviceerror.NewInternal("missing cluster record/blob")
	}
	return &p.InternalGetClusterMetadataResponse{Version: r.Version, ClusterMetadata: &commonpb.DataBlob{Data: r.Blob.Data, EncodingType: enumspb.EncodingType(r.Blob.Encoding)}}, nil
}
func (s *ClusterStore) ListClusterMetadata(ctx context.Context, q *p.InternalListClusterMetadataRequest) (*p.InternalListClusterMetadataResponse, error) {
	if q == nil {
		return nil, serviceerror.NewInvalidArgument("nil cluster request")
	}
	r, e := s.invokeCluster(ctx, &wire.ClusterCommand{Kind: wire.ClusterCommand_LIST, PageSize: int64(q.PageSize), NextPageToken: q.NextPageToken})
	if e != nil {
		return nil, e
	}
	out := &p.InternalListClusterMetadataResponse{NextPageToken: r.NextPageToken}
	for _, v := range r.Records {
		item, err := clusterRecord(v)
		if err != nil {
			return nil, err
		}
		out.ClusterMetadata = append(out.ClusterMetadata, item)
	}
	return out, nil
}
func (s *ClusterStore) GetClusterMetadata(ctx context.Context, q *p.InternalGetClusterMetadataRequest) (*p.InternalGetClusterMetadataResponse, error) {
	if q == nil {
		return nil, serviceerror.NewInvalidArgument("nil cluster request")
	}
	r, e := s.invokeCluster(ctx, &wire.ClusterCommand{Kind: wire.ClusterCommand_GET, ClusterName: q.ClusterName})
	if e != nil {
		return nil, e
	}
	return clusterRecord(r.Record)
}
func (s *ClusterStore) SaveClusterMetadata(ctx context.Context, q *p.InternalSaveClusterMetadataRequest) (bool, error) {
	if q == nil || q.ClusterMetadata == nil {
		return false, serviceerror.NewInvalidArgument("nil cluster request/blob")
	}
	r, e := s.invokeCluster(ctx, &wire.ClusterCommand{Kind: wire.ClusterCommand_SAVE, ClusterName: q.ClusterName, Version: q.Version, Blob: &wire.ClusterBlob{Data: q.ClusterMetadata.Data, Encoding: int32(q.ClusterMetadata.EncodingType)}})
	if e != nil {
		return false, e
	}
	return r.Applied, nil
}
func (s *ClusterStore) DeleteClusterMetadata(ctx context.Context, q *p.InternalDeleteClusterMetadataRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil cluster request")
	}
	_, e := s.invokeCluster(ctx, &wire.ClusterCommand{Kind: wire.ClusterCommand_DELETE, ClusterName: q.ClusterName})
	return e
}
func (s *ClusterStore) GetClusterMembers(ctx context.Context, q *p.GetClusterMembersRequest) (*p.GetClusterMembersResponse, error) {
	if q == nil {
		return nil, serviceerror.NewInvalidArgument("nil members request")
	}
	r, e := s.invokeCluster(ctx, &wire.ClusterCommand{Kind: wire.ClusterCommand_GET_MEMBERS, PageSize: int64(q.PageSize), NextPageToken: q.NextPageToken, LastHeartbeatWithinNanos: int64(q.LastHeartbeatWithin), RpcAddressEquals: q.RPCAddressEquals, HostIdEquals: q.HostIDEquals, RoleEquals: int32(q.RoleEquals), SessionStartedAfter: clusterInstant(q.SessionStartedAfter)})
	if e != nil {
		return nil, e
	}
	out := &p.GetClusterMembersResponse{NextPageToken: r.NextPageToken}
	for _, v := range r.Members {
		if v == nil || len(v.HostId) != 16 || (len(v.RpcAddress) != 4 && len(v.RpcAddress) != 16) || v.RpcPort > 65535 {
			return nil, serviceerror.NewInternal("invalid cluster member")
		}
		session, e := clusterTime(v.SessionStart)
		if e != nil {
			return nil, e
		}
		heartbeat, e := clusterTime(v.LastHeartbeat)
		if e != nil {
			return nil, e
		}
		expiry, e := clusterTime(v.RecordExpiry)
		if e != nil {
			return nil, e
		}
		out.ActiveMembers = append(out.ActiveMembers, &p.ClusterMember{HostID: v.HostId, Role: p.ServiceType(v.Role), RPCAddress: net.IP(v.RpcAddress), RPCPort: uint16(v.RpcPort), SessionStart: session, LastHeartbeat: heartbeat, RecordExpiry: expiry})
	}
	return out, nil
}
func (s *ClusterStore) UpsertClusterMembership(ctx context.Context, q *p.UpsertClusterMembershipRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil membership request")
	}
	_, e := s.invokeCluster(ctx, &wire.ClusterCommand{Kind: wire.ClusterCommand_UPSERT_MEMBER, Member: &wire.ClusterMemberRecord{HostId: q.HostID, Role: int32(q.Role), RpcAddress: q.RPCAddress, RpcPort: uint32(q.RPCPort), SessionStart: clusterInstant(q.SessionStart)}, RecordExpiryNanos: int64(q.RecordExpiry)})
	return e
}
func (s *ClusterStore) PruneClusterMembership(ctx context.Context, q *p.PruneClusterMembershipRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil prune request")
	}
	_, e := s.invokeCluster(ctx, &wire.ClusterCommand{Kind: wire.ClusterCommand_PRUNE_MEMBERS, MaxRecordsPruned: int64(q.MaxRecordsPruned)})
	return e
}
