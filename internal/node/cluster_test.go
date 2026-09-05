package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/adapter"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
)

type clusterFixture struct {
	Schema              int    `json:"schema"`
	Clock               string `json:"clock"`
	Members             int    `json:"members"`
	PageSize            int    `json:"page_size"`
	CASCallers          int    `json:"cas_callers"`
	LargeBlobBytes      int    `json:"large_blob_bytes"`
	ResponseBudgetBytes int    `json:"response_budget_bytes"`
}

func clusterCase(t *testing.T) clusterFixture {
	t.Helper()
	b, e := os.ReadFile("../../proof/go-cluster/case.json")
	if e != nil {
		t.Fatal(e)
	}
	var f clusterFixture
	if e = json.Unmarshal(b, &f); e != nil || f.Schema != 1 || f.Members < 1 || f.CASCallers < 2 {
		t.Fatal("invalid cluster fixture", e)
	}
	return f
}
func clusterRequest(id string, c *wire.ClusterCommand) *wire.ClusterRequest {
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	h := sha256.Sum256(b)
	return &wire.ClusterRequest{ProtocolVersion: 1, Partition: "p", OperationId: id, CommandSha256: h[:], Command: c}
}
func clusterCall(t *testing.T, s *ClusterServer, c *wire.ClusterCommand) *wire.ClusterResult {
	t.Helper()
	r, e := s.Execute(context.Background(), clusterRequest(uuid.NewString(), c))
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestGoOwnerClusterRecovery(t *testing.T) {
	objects := objects(t)
	path := cfg(t).Prefix + "-cluster"
	o := owner(t, engine(t, objects, path, false))
	s := &ClusterServer{Owner: o}
	ctx := context.Background()
	q := clusterRequest("cluster-save", &wire.ClusterCommand{Kind: wire.ClusterCommand_SAVE, ClusterName: "alpha", Blob: &wire.ClusterBlob{Data: []byte{0, 255}, Encoding: 2}})
	first, e := s.Execute(ctx, q)
	if e != nil || !first.Applied {
		t.Fatal(first, e)
	}
	f := clusterCase(t)
	var wg sync.WaitGroup
	results := make(chan *wire.ClusterResult, f.CASCallers)
	errs := make(chan error, f.CASCallers)
	for i := 0; i < f.CASCallers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, e := s.Execute(ctx, clusterRequest(fmt.Sprintf("cas-%d", i), &wire.ClusterCommand{Kind: wire.ClusterCommand_SAVE, ClusterName: "alpha", Version: 1, Blob: &wire.ClusterBlob{Data: []byte{byte(i)}, Encoding: 2}}))
			results <- r
			errs <- e
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)
	success := 0
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	for r := range results {
		if r.Applied {
			success++
		} else if r.Error != wire.ClusterResult_UNAVAILABLE {
			t.Fatal(r)
		}
	}
	if success != 1 {
		t.Fatal("CAS winners", success)
	}
	closeOwner(t, o)
	o = owner(t, engine(t, objects, path, false))
	defer closeOwner(t, o)
	s.Owner = o
	replay, e := s.Execute(ctx, q)
	if e != nil || !proto.Equal(first, replay) {
		t.Fatal(replay, e)
	}
	current := clusterCall(t, s, &wire.ClusterCommand{Kind: wire.ClusterCommand_GET, ClusterName: "alpha"})
	if current.Record.Version != 2 {
		t.Fatal(current)
	}
	// Same serialized command bytes can belong to a different protobuf family.
	_, e = o.Run(ctx, func(_ *native.Db) ([]byte, error) {
		_, e := o.journal(q.OperationId, q.CommandSha256, metadataFamily, func(*native.DbTransaction) (*wire.StoredOutcome, error) {
			t.Error("cross-family callback ran")
			return nil, nil
		})
		return nil, e
	})
	if status.Code(e) != codes.InvalidArgument {
		t.Fatal("cross family replay", e)
	}
	rival := owner(t, engine(t, objects, path, false))
	defer closeOwner(t, rival)
	if _, e = s.Execute(ctx, q); status.Code(e) != codes.Unavailable || !o.Quarantined() {
		t.Fatal("fenced replay", e)
	}
}
func TestGoOwnerClusterMembership(t *testing.T) {
	f := clusterCase(t)
	now, e := time.Parse(time.RFC3339Nano, f.Clock)
	if e != nil {
		t.Fatal(e)
	}
	o := owner(t, engine(t, objects(t), cfg(t).Prefix+"-members", false))
	defer closeOwner(t, o)
	s := &ClusterServer{Owner: o, Now: func() time.Time { return now }}
	list := func(c *wire.ClusterCommand) *wire.ClusterResult {
		c.Kind = wire.ClusterCommand_GET_MEMBERS
		if c.SessionStartedAfter == nil {
			c.SessionStartedAfter = clusterStamp(time.Time{})
		}
		return clusterCall(t, s, c)
	}
	if len(list(&wire.ClusterCommand{}).Members) != 0 {
		t.Fatal("nonempty initial membership")
	}
	var first *wire.ClusterRequest
	for i := 0; i < f.Members; i++ {
		id := make([]byte, 16)
		id[15] = byte(i + 1)
		q := clusterRequest(fmt.Sprintf("member-%d", i), &wire.ClusterCommand{Kind: wire.ClusterCommand_UPSERT_MEMBER, Member: &wire.ClusterMemberRecord{HostId: id, Role: 1, RpcAddress: net.IP{127, 0, 0, 1}, RpcPort: 7233, SessionStart: clusterStamp(now)}, RecordExpiryNanos: int64(time.Hour)})
		if i == 0 {
			first = q
		}
		r, e := s.Execute(context.Background(), q)
		if e != nil || r.Error != wire.ClusterResult_NONE {
			t.Fatal(r, e)
		}
	}
	seen := map[string]bool{}
	var token []byte
	for {
		r := list(&wire.ClusterCommand{PageSize: int64(f.PageSize), NextPageToken: token})
		for _, m := range r.Members {
			if seen[string(m.HostId)] {
				t.Fatal("duplicate page member")
			}
			seen[string(m.HostId)] = true
		}
		token = r.NextPageToken
		if len(token) == 0 {
			break
		}
	}
	if len(seen) != f.Members {
		t.Fatal("missing members", len(seen))
	}
	id := first.Command.Member.HostId
	later := bytes.Repeat([]byte{255}, 16)
	if len(list(&wire.ClusterCommand{HostIdEquals: id, NextPageToken: later}).Members) != 1 {
		t.Fatal("host filter did not override cursor")
	}
	if len(list(&wire.ClusterCommand{HostIdEquals: []byte{}}).Members) != 0 {
		t.Fatal("empty host equality became absent")
	}
	if len(list(&wire.ClusterCommand{RpcAddressEquals: net.ParseIP("127.0.0.1"), RoleEquals: 1, SessionStartedAfter: clusterStamp(now)}).Members) != f.Members {
		t.Fatal("filter equality")
	}
	if len(list(&wire.ClusterCommand{SessionStartedAfter: clusterStamp(now.Add(time.Nanosecond))}).Members) != 0 {
		t.Fatal("session boundary")
	}
	now = now.Add(time.Minute)
	if len(list(&wire.ClusterCommand{LastHeartbeatWithinNanos: int64(time.Minute)}).Members) != 0 {
		t.Fatal("heartbeat strict boundary")
	}
	if _, e = s.Execute(context.Background(), first); e != nil {
		t.Fatal(e)
	}
	one := list(&wire.ClusterCommand{HostIdEquals: id})
	if !instant(one.Members[0].RecordExpiry).Equal(now.Add(59 * time.Minute)) {
		t.Fatal("replay extended lease")
	}
	if list(&wire.ClusterCommand{NextPageToken: []byte{1}}).Error != wire.ClusterResult_INTERNAL {
		t.Fatal("invalid token")
	}
	now = now.Add(59 * time.Minute)
	if len(list(&wire.ClusterCommand{}).Members) != 0 {
		t.Fatal("expiry strict boundary")
	}
	clusterCall(t, s, &wire.ClusterCommand{Kind: wire.ClusterCommand_PRUNE_MEMBERS, MaxRecordsPruned: 1})
	// Equal-expiry records remain physically stored, then prune ignores requested limit.
	count := func() int {
		n := 0
		_, e := o.Run(context.Background(), func(db *native.Db) ([]byte, error) {
			tx, e := db.Begin(native.IsolationLevelSerializableSnapshot)
			if e != nil {
				return nil, backend(e)
			}
			defer tx.Destroy()
			e = scanCluster(tx, memberPrefix, func([]byte, []byte) (bool, error) { n++; return false, nil })
			return nil, e
		})
		if e != nil {
			t.Fatal(e)
		}
		return n
	}
	if count() != f.Members {
		t.Fatal("pruned equality")
	}
	now = now.Add(time.Nanosecond)
	clusterCall(t, s, &wire.ClusterCommand{Kind: wire.ClusterCommand_PRUNE_MEMBERS, MaxRecordsPruned: 1})
	if count() != 0 {
		t.Fatal("prune count incorrectly honored")
	}
}
func TestGoOwnerClusterRPC(t *testing.T) {
	f := clusterCase(t)
	o := owner(t, engine(t, objects(t), cfg(t).Prefix+"-cluster-rpc", false))
	defer closeOwner(t, o)
	s := &ClusterServer{Owner: o}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	var dropped atomic.Bool
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		result, err := handler(ctx, req)
		if q, ok := req.(*wire.ClusterRequest); ok && q.Command.Kind == wire.ClusterCommand_SAVE && err == nil && dropped.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "declared completed response loss")
		}
		return result, err
	}))
	wire.RegisterClusterPersistenceServer(server, s)
	go server.Serve(listener)
	defer server.Stop()
	store, e := adapter.NewClusterStore(listener.Addr().String(), "p")
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	ctx := context.Background()
	var missing *serviceerror.NotFound
	if _, e = store.GetClusterMetadata(ctx, &p.InternalGetClusterMetadataRequest{ClusterName: "missing"}); !errors.As(e, &missing) {
		t.Fatal(e)
	}
	for _, name := range []string{"a", "b", "c"} {
		ok, e := store.SaveClusterMetadata(ctx, &p.InternalSaveClusterMetadataRequest{ClusterName: name, ClusterMetadata: &commonpb.DataBlob{Data: bytes.Repeat([]byte{255}, f.LargeBlobBytes), EncodingType: 2}})
		if e != nil || !ok {
			t.Fatal(ok, e)
		}
	}
	r, e := store.ListClusterMetadata(ctx, &p.InternalListClusterMetadataRequest{PageSize: 1000})
	if e != nil || len(r.ClusterMetadata) != 2 || len(r.NextPageToken) == 0 {
		t.Fatal("byte limited first page", r, e)
	}
	next, e := store.ListClusterMetadata(ctx, &p.InternalListClusterMetadataRequest{PageSize: 1000, NextPageToken: r.NextPageToken})
	if e != nil || len(next.ClusterMetadata) != 1 || next.NextPageToken != nil {
		t.Fatal("byte limited final page", next, e)
	}
	var internal *serviceerror.Internal
	if _, e = store.ListClusterMetadata(ctx, &p.InternalListClusterMetadataRequest{PageSize: 1, NextPageToken: []byte{}}); !errors.As(e, &internal) {
		t.Fatal("empty token presence", e)
	}
	if !dropped.Load() {
		t.Fatal("response loss fault did not execute")
	}
	for _, name := range []string{"a", "b", "c", "missing"} {
		if e = store.DeleteClusterMetadata(ctx, &p.InternalDeleteClusterMetadataRequest{ClusterName: name}); e != nil {
			t.Fatal(e)
		}
	}
	manager := p.NewClusterMetadataManagerImpl(store, serialization.NewSerializer(), "current", log.NewNoopLogger())
	if e = manager.UpsertClusterMembership(ctx, &p.UpsertClusterMembershipRequest{}); !errors.Is(e, p.ErrInvalidMembershipExpiry) {
		t.Fatal("manager expiry validation", e)
	}

	metadata := &persistencespb.ClusterMetadata{ClusterName: "remote", HistoryShardCount: 43, ClusterId: "fixed-cluster", FailoverVersionIncrement: 10, InitialFailoverVersion: 1, IsGlobalNamespaceEnabled: true}
	applied, e := manager.SaveClusterMetadata(ctx, &p.SaveClusterMetadataRequest{ClusterMetadata: metadata})
	if e != nil || !applied {
		t.Fatal(applied, e)
	}
	saved, e := manager.GetClusterMetadata(ctx, &p.GetClusterMetadataRequest{ClusterName: "remote"})
	if e != nil || !proto.Equal(saved.ClusterMetadata, metadata) || saved.Version != 1 {
		t.Fatal(saved, e)
	}
	incompatible := proto.Clone(metadata).(*persistencespb.ClusterMetadata)
	incompatible.HistoryShardCount = 77
	applied, e = manager.SaveClusterMetadata(ctx, &p.SaveClusterMetadataRequest{ClusterMetadata: incompatible})
	if e != nil || applied {
		t.Fatal("immutable metadata changed", applied, e)
	}
	saved.ClusterAddress = "new-address"
	applied, e = manager.SaveClusterMetadata(ctx, &p.SaveClusterMetadataRequest{ClusterMetadata: saved.ClusterMetadata, Version: saved.Version})
	if e != nil || !applied {
		t.Fatal(applied, e)
	}
	updated, e := manager.GetClusterMetadata(ctx, &p.GetClusterMetadataRequest{ClusterName: "remote"})
	if e != nil || updated.Version != 2 || updated.ClusterAddress != "new-address" {
		t.Fatal(updated, e)
	}
	if e = manager.DeleteClusterMetadata(ctx, &p.DeleteClusterMetadataRequest{ClusterName: "current"}); e == nil {
		t.Fatal("current cluster delete accepted")
	}
	if e = manager.DeleteClusterMetadata(ctx, &p.DeleteClusterMetadataRequest{ClusterName: "remote"}); e != nil {
		t.Fatal(e)
	}
	id := bytes.Repeat([]byte{1}, 16)
	if e = manager.UpsertClusterMembership(ctx, &p.UpsertClusterMembershipRequest{HostID: id, Role: p.Frontend, RPCAddress: net.ParseIP("::1"), RPCPort: 7233, SessionStart: time.Now().UTC(), RecordExpiry: time.Hour}); e != nil {
		t.Fatal(e)
	}
	members, e := manager.GetClusterMembers(ctx, &p.GetClusterMembersRequest{HostIDEquals: id})
	if e != nil || len(members.ActiveMembers) != 1 || !members.ActiveMembers[0].RPCAddress.Equal(net.ParseIP("::1")) {
		t.Fatal(members, e)
	}
}
