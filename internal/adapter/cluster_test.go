package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
	"net"
	"testing"
	"time"
)

type clusterWireFunc func(context.Context, *wire.ClusterRequest) (*wire.ClusterResult, error)

func (f clusterWireFunc) Execute(c context.Context, q *wire.ClusterRequest, _ ...grpc.CallOption) (*wire.ClusterResult, error) {
	return f(c, q)
}
func TestClusterContract(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 5, 11, 12, 13, 987654321, time.FixedZone("offset", -25200))
	id := bytes.Repeat([]byte{0xff}, 16)
	ip := net.ParseIP("2001:db8::1234")
	blob := &commonpb.DataBlob{Data: []byte{0, 255, 128, 0}, EncodingType: enumspb.EncodingType(1234)}
	record := &wire.ClusterRecord{Version: math.MaxInt64, Blob: &wire.ClusterBlob{Data: blob.Data, Encoding: int32(blob.EncodingType)}}
	member := &wire.ClusterMemberRecord{HostId: id, Role: 2, RpcAddress: ip, RpcPort: 65535, SessionStart: clusterInstant(now), LastHeartbeat: clusterInstant(now.Add(time.Nanosecond)), RecordExpiry: clusterInstant(now.Add(time.Hour))}
	var captured *wire.ClusterCommand
	reply := &wire.ClusterResult{Record: record, Records: []*wire.ClusterRecord{record}, Members: []*wire.ClusterMemberRecord{member}, NextPageToken: []byte{255, 0}, Applied: true}
	store := &ClusterStore{partition: "control-v1", invocationTimeout: time.Second, client: clusterWireFunc(func(_ context.Context, q *wire.ClusterRequest) (*wire.ClusterResult, error) {
		raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(q.Command)
		if e != nil {
			t.Fatal(e)
		}
		digest := sha256.Sum256(raw)
		if q.ProtocolVersion != 1 || q.Partition != "control-v1" || q.OperationId == "" || !bytes.Equal(digest[:], q.CommandSha256) {
			t.Fatal("bad operation envelope")
		}
		// Exercise real protobuf encode/decode, including optional empty bytes.
		captured = new(wire.ClusterCommand)
		if e = proto.Unmarshal(raw, captured); e != nil {
			t.Fatal(e)
		}
		return reply, nil
	})}
	got, e := store.GetClusterMetadata(ctx, &p.InternalGetClusterMetadataRequest{ClusterName: "alpha"})
	if e != nil || got.Version != math.MaxInt64 || !proto.Equal(got.ClusterMetadata, blob) || captured.Kind != wire.ClusterCommand_GET || captured.ClusterName != "alpha" {
		t.Fatal(got, e, captured)
	}
	list, e := store.ListClusterMetadata(ctx, &p.InternalListClusterMetadataRequest{PageSize: 57, NextPageToken: []byte{8, 0}})
	if e != nil || len(list.ClusterMetadata) != 1 || !bytes.Equal(list.NextPageToken, reply.NextPageToken) || captured.PageSize != 57 || !bytes.Equal(captured.NextPageToken, []byte{8, 0}) {
		t.Fatal(list, e, captured)
	}
	_, e = store.ListClusterMetadata(ctx, &p.InternalListClusterMetadataRequest{NextPageToken: []byte{}})
	if e != nil || captured.NextPageToken == nil {
		t.Fatal("empty non-nil token presence lost", e)
	}
	_, e = store.GetClusterMembers(ctx, &p.GetClusterMembersRequest{})
	if e != nil || captured.HostIdEquals != nil || captured.RpcAddressEquals != nil {
		t.Fatal("absent filter became equality filter", e)
	}
	applied, e := store.SaveClusterMetadata(ctx, &p.InternalSaveClusterMetadataRequest{ClusterName: "alpha", Version: math.MaxInt64, ClusterMetadata: blob})
	if e != nil || !applied || captured.Version != math.MaxInt64 || !bytes.Equal(captured.Blob.Data, blob.Data) || captured.Blob.Encoding != 1234 {
		t.Fatal(applied, e, captured)
	}
	reply.Applied = false
	applied, e = store.SaveClusterMetadata(ctx, &p.InternalSaveClusterMetadataRequest{ClusterMetadata: blob})
	if applied || e != nil {
		t.Fatal("false result changed", applied, e)
	}
	reply.Error = wire.ClusterResult_UNAVAILABLE
	applied, e = store.SaveClusterMetadata(ctx, &p.InternalSaveClusterMetadataRequest{ClusterMetadata: blob})
	var unavailable *serviceerror.Unavailable
	if applied || !errors.As(e, &unavailable) {
		t.Fatal("CAS outcome type", applied, e)
	}
	reply.Error = wire.ClusterResult_NONE
	if e = store.DeleteClusterMetadata(ctx, &p.InternalDeleteClusterMetadataRequest{ClusterName: "missing"}); e != nil || captured.Kind != wire.ClusterCommand_DELETE {
		t.Fatal(e)
	}
	members, e := store.GetClusterMembers(ctx, &p.GetClusterMembersRequest{PageSize: 9, NextPageToken: id, HostIDEquals: []byte{}, RPCAddressEquals: ip, LastHeartbeatWithin: 17 * time.Nanosecond, SessionStartedAfter: now, RoleEquals: 2})
	if e != nil {
		t.Fatal(e)
	}
	m := members.ActiveMembers[0]
	if !m.SessionStart.Equal(now) || m.LastHeartbeat.Nanosecond() != 987654322 || !m.RPCAddress.Equal(ip) || m.RPCPort != 65535 || !bytes.Equal(m.HostID, id) || captured.HostIdEquals == nil || captured.LastHeartbeatWithinNanos != 17 || captured.PageSize != 9 || !bytes.Equal(captured.NextPageToken, id) || captured.RoleEquals != 2 {
		t.Fatal(m, captured)
	}
	if e = store.UpsertClusterMembership(ctx, &p.UpsertClusterMembershipRequest{HostID: id, Role: 2, RPCAddress: net.IP{127, 0, 0, 1}, RPCPort: 65535, SessionStart: now, RecordExpiry: math.MaxInt64}); e != nil || captured.RecordExpiryNanos != math.MaxInt64 || captured.Member.LastHeartbeat != nil || captured.Member.RecordExpiry != nil {
		t.Fatal(e, captured)
	}
	if e = store.PruneClusterMembership(ctx, &p.PruneClusterMembershipRequest{MaxRecordsPruned: 123}); e != nil || captured.MaxRecordsPruned != 123 || captured.Kind != wire.ClusterCommand_PRUNE_MEMBERS {
		t.Fatal(e, captured)
	}
	zero, e := clusterTime(clusterInstant(time.Time{}))
	if e != nil || !zero.IsZero() {
		t.Fatal(zero, e)
	}
}
func TestClusterRetryAndErrors(t *testing.T) {
	var requests []*wire.ClusterRequest
	s := &ClusterStore{partition: "control", invocationTimeout: time.Second, client: clusterWireFunc(func(_ context.Context, q *wire.ClusterRequest) (*wire.ClusterResult, error) {
		requests = append(requests, proto.Clone(q).(*wire.ClusterRequest))
		if len(requests) == 1 {
			return nil, status.Error(codes.Unavailable, "lost reply")
		}
		return &wire.ClusterResult{}, nil
	})}
	if e := s.DeleteClusterMetadata(context.Background(), &p.InternalDeleteClusterMetadataRequest{}); e != nil {
		t.Fatal(e)
	}
	if len(requests) != 2 || !proto.Equal(requests[0], requests[1]) {
		t.Fatal("retry identity changed")
	}
	for _, code := range []wire.ClusterResult_Error{wire.ClusterResult_NOT_FOUND, wire.ClusterResult_UNAVAILABLE, wire.ClusterResult_INTERNAL, wire.ClusterResult_INVALID_ARGUMENT, wire.ClusterResult_RESOURCE_EXHAUSTED, 999} {
		if clusterError(&wire.ClusterResult{Error: code}) == nil {
			t.Fatal(code)
		}
	}
	s.client = clusterWireFunc(func(context.Context, *wire.ClusterRequest) (*wire.ClusterResult, error) {
		return nil, status.Error(codes.NotFound, "missing")
	})
	e := s.DeleteClusterMetadata(context.Background(), &p.InternalDeleteClusterMetadataRequest{})
	var missing *serviceerror.NotFound
	if !errors.As(e, &missing) {
		t.Fatal(e)
	}
	s.invocationTimeout = 10 * time.Millisecond
	s.client = clusterWireFunc(func(ctx context.Context, _ *wire.ClusterRequest) (*wire.ClusterResult, error) {
		<-ctx.Done()
		return nil, status.Error(codes.DeadlineExceeded, "timeout")
	})
	if e = s.DeleteClusterMetadata(context.Background(), &p.InternalDeleteClusterMetadataRequest{}); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
	if _, e = clusterTime(&wire.ClusterTime{Nanos: 1000000000}); e == nil {
		t.Fatal("invalid nanos accepted")
	}
	if _, e = clusterRecord(&wire.ClusterRecord{}); e == nil {
		t.Fatal("missing blob accepted")
	}
}
