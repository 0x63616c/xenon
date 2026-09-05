package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/adapter"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"net"
	"sync/atomic"
	"testing"
)

func nexusRequest(id string, c *wire.NexusCommand) *wire.NexusRequest {
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	h := sha256.Sum256(b)
	return &wire.NexusRequest{ProtocolVersion: 1, Partition: "p", OperationId: id, CommandSha256: h[:], Command: c}
}
func TestGoOwnerNexusRecovery(t *testing.T) {
	obj := objects(t)
	path := cfg(t).Prefix + "-nexus"
	o := owner(t, engine(t, obj, path, false))
	s := &NexusServer{Owner: o}
	ctx := context.Background()
	call := func(c *wire.NexusCommand) *wire.NexusResult {
		t.Helper()
		r, e := s.Execute(ctx, nexusRequest(uuid.NewString(), c))
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	id := bytes.Repeat([]byte{1}, 16)
	q := nexusRequest("nexus-create", &wire.NexusCommand{Endpoint: &wire.NexusEndpoint{Id: id, Data: []byte{0, 255}, Encoding: 2}})
	first, e := s.Execute(ctx, q)
	if e != nil || first.Error != wire.NexusResult_NONE || first.TableVersion != 1 {
		t.Fatal(first, e)
	}
	// An endpoint CAS failure must roll back the catalog version too.
	bad := call(&wire.NexusCommand{TableVersion: 1, Endpoint: &wire.NexusEndpoint{Id: id, Version: 7}})
	if bad.Error != wire.NexusResult_UNAVAILABLE {
		t.Fatal(bad)
	}
	list := call(&wire.NexusCommand{Kind: wire.NexusCommand_LIST})
	if list.TableVersion != 1 || len(list.Endpoints) != 0 {
		t.Fatal(list)
	}
	updated := call(&wire.NexusCommand{TableVersion: 1, Endpoint: &wire.NexusEndpoint{Id: id, Version: 1, Data: []byte("updated")}})
	if updated.Error != wire.NexusResult_NONE || updated.TableVersion != 2 {
		t.Fatal(updated)
	}
	stale := call(&wire.NexusCommand{Kind: wire.NexusCommand_LIST, TableVersion: 1, PageSize: 1})
	if stale.Error != wire.NexusResult_UNAVAILABLE || stale.TableVersion != 2 {
		t.Fatal(stale)
	}
	missing := call(&wire.NexusCommand{Kind: wire.NexusCommand_DELETE, TableVersion: 2, Id: bytes.Repeat([]byte{2}, 16)})
	if missing.Error != wire.NexusResult_NOT_FOUND {
		t.Fatal(missing)
	}
	closeOwner(t, o)
	o = owner(t, engine(t, obj, path, false))
	defer closeOwner(t, o)
	s.Owner = o
	replay, e := s.Execute(ctx, q)
	if e != nil || !proto.Equal(first, replay) {
		t.Fatal(replay, e)
	}
	got := call(&wire.NexusCommand{Kind: wire.NexusCommand_GET, Id: id})
	if got.Endpoint.Version != 2 || string(got.Endpoint.Data) != "updated" || got.TableVersion != 2 {
		t.Fatal(got)
	}
	changed := proto.Clone(q).(*wire.NexusRequest)
	changed.Command.Endpoint.Data = []byte("different")
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(changed.Command)
	h := sha256.Sum256(b)
	changed.CommandSha256 = h[:]
	if _, e = s.Execute(ctx, changed); status.Code(e) != codes.InvalidArgument {
		t.Fatal("changed replay", e)
	}
	rival := owner(t, engine(t, obj, path, false))
	defer closeOwner(t, rival)
	if _, e = s.Execute(ctx, q); status.Code(e) != codes.Unavailable || !o.Quarantined() {
		t.Fatal("fenced replay", e)
	}
}
func TestGoOwnerNexusPages(t *testing.T) {
	o := owner(t, engine(t, objects(t), cfg(t).Prefix+"-nexus-pages", false))
	defer closeOwner(t, o)
	s := &NexusServer{Owner: o}
	call := func(c *wire.NexusCommand) *wire.NexusResult {
		t.Helper()
		r, e := s.Execute(context.Background(), nexusRequest(uuid.NewString(), c))
		if e != nil || r.Error != wire.NexusResult_NONE {
			t.Fatal(r, e)
		}
		return r
	}
	for i := 1; i <= 5; i++ {
		id := make([]byte, 16)
		id[15] = byte(i)
		call(&wire.NexusCommand{TableVersion: int64(i - 1), Endpoint: &wire.NexusEndpoint{Id: id, Data: bytes.Repeat([]byte{byte(i)}, 1024*1024)}})
	}
	var token []byte
	count := 0
	for {
		r := call(&wire.NexusCommand{Kind: wire.NexusCommand_LIST, TableVersion: 5, PageSize: 100, NextPageToken: token})
		if proto.Size(r) > 3*1024*1024 {
			t.Fatal("page budget")
		}
		for _, ep := range r.Endpoints {
			count++
			if int(ep.Id[15]) != count || len(ep.Data) != 1024*1024 {
				t.Fatal("page completeness", count)
			}
		}
		token = r.NextPageToken
		if len(token) == 0 {
			break
		}
		if count > 5 {
			t.Fatal("page loop")
		}
	}
	if count != 5 {
		t.Fatal(count)
	}
	for i := 1; i <= 5; i++ {
		id := make([]byte, 16)
		id[15] = byte(i)
		call(&wire.NexusCommand{Kind: wire.NexusCommand_DELETE, TableVersion: int64(4 + i), Id: id})
	}
	r := call(&wire.NexusCommand{Kind: wire.NexusCommand_LIST, PageSize: 10})
	if r.TableVersion != 10 || len(r.Endpoints) != 0 {
		t.Fatal(r)
	}
}

func TestGoOwnerNexusRPC(t *testing.T) {
	o := owner(t, engine(t, objects(t), cfg(t).Prefix+"-nexus-rpc", false))
	defer closeOwner(t, o)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	var dropped atomic.Bool
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		r, e := handler(ctx, req)
		if q, ok := req.(*wire.NexusRequest); ok && q.Command.Kind == wire.NexusCommand_UPSERT && e == nil && dropped.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "declared completed response loss")
		}
		return r, e
	}))
	wire.RegisterNexusPersistenceServer(server, &NexusServer{Owner: o})
	go server.Serve(listener)
	defer server.Stop()
	store, e := adapter.NewNexusStore(listener.Addr().String(), "p")
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	ctx := context.Background()
	id := "00000000-0000-0000-0000-000000000001"
	var missing *serviceerror.NotFound
	if _, e = store.GetNexusEndpoint(ctx, &p.GetNexusEndpointRequest{ID: id}); !errors.As(e, &missing) {
		t.Fatal(e)
	}
	blob := &commonpb.DataBlob{Data: []byte{0, 255, 128}, EncodingType: 2}
	if e = store.CreateOrUpdateNexusEndpoint(ctx, &p.InternalCreateOrUpdateNexusEndpointRequest{Endpoint: p.InternalNexusEndpoint{ID: id, Data: blob}}); e != nil {
		t.Fatal(e)
	}
	got, e := store.GetNexusEndpoint(ctx, &p.GetNexusEndpointRequest{ID: id})
	if e != nil || got.Version != 1 || !proto.Equal(got.Data, blob) || !dropped.Load() {
		t.Fatal(got, e)
	}
	r, e := store.ListNexusEndpoints(ctx, &p.ListNexusEndpointsRequest{PageSize: 1})
	if e != nil || r.TableVersion != 1 || len(r.Endpoints) != 1 {
		t.Fatal(r, e)
	}
	stale, e := store.ListNexusEndpoints(ctx, &p.ListNexusEndpointsRequest{PageSize: 1, LastKnownTableVersion: 2})
	var unavailable *serviceerror.Unavailable
	if !errors.As(e, &unavailable) || stale.TableVersion != 1 {
		t.Fatal(stale, e)
	}
	if e = store.DeleteNexusEndpoint(ctx, &p.DeleteNexusEndpointRequest{ID: id, LastKnownTableVersion: 1}); e != nil {
		t.Fatal(e)
	}
	if _, e = store.GetNexusEndpoint(ctx, &p.GetNexusEndpointRequest{ID: id}); !errors.As(e, &missing) {
		t.Fatal(e)
	}
}
