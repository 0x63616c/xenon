//go:build slatedb

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"net"
	"testing"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
)

// Both valid commands serialize to 08 01 10 01. A digest alone cannot identify
// an operation family. Exercise real gRPC registrations in both directions,
// including after reopening the same durable shard/outcome database.
func TestGoOwnerCrossFamilyCollision(t *testing.T) {
	shardCommand := &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: 1}
	queueCommand := &wire.QueueCommand{Kind: wire.QueueCommand_INIT, QueueType: 1}
	shardBytes, e := proto.MarshalOptions{Deterministic: true}.Marshal(shardCommand)
	if e != nil {
		t.Fatal(e)
	}
	queueBytes, e := proto.MarshalOptions{Deterministic: true}.Marshal(queueCommand)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(shardBytes, []byte{8, 1, 16, 1}) || !bytes.Equal(shardBytes, queueBytes) {
		t.Fatal("fixture commands no longer collide", shardBytes, queueBytes)
	}
	digest := sha256.Sum256(shardBytes)
	for _, queueFirst := range []bool{false, true} {
		name := "shard_first"
		if queueFirst {
			name = "queue_first"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			objects := objects(t)
			path := cfg(t).Prefix + "-family-collision-" + name
			o := owner(t, engine(t, objects, path, false))
			start := func(o *Owner) (wire.ShardPersistenceClient, wire.QueuePersistenceClient, func()) {
				l, e := net.Listen("tcp", "127.0.0.1:0")
				if e != nil {
					t.Fatal(e)
				}
				server := grpc.NewServer()
				wire.RegisterShardPersistenceServer(server, o)
				wire.RegisterQueuePersistenceServer(server, &QueueServer{Owner: o})
				go server.Serve(l)
				conn, e := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if e != nil {
					server.Stop()
					t.Fatal(e)
				}
				stop := func() { _ = conn.Close(); server.Stop() }
				t.Cleanup(stop)
				return wire.NewShardPersistenceClient(conn), wire.NewQueuePersistenceClient(conn), stop
			}
			shards, queues, stop := start(o)
			shardReq := &wire.ShardRequest{ProtocolVersion: 1, Partition: "p", OperationId: "same-id", CommandSha256: digest[:], Command: shardCommand}
			queueReq := &wire.QueueRequest{ProtocolVersion: 1, Partition: "p", OperationId: "same-id", CommandSha256: digest[:], Command: queueCommand}
			var original proto.Message
			first := func() proto.Message {
				t.Helper()
				if queueFirst {
					r, e := queues.Execute(ctx, queueReq)
					if e != nil || r.Error != wire.QueueResult_NONE {
						t.Fatal(r, e)
					}
					return r
				}
				r, e := shards.Execute(ctx, shardReq)
				if e != nil || r.Error != wire.ShardResult_NOT_FOUND {
					t.Fatal(r, e)
				}
				return r
			}
			second := func(id string) error {
				if queueFirst {
					q := proto.Clone(shardReq).(*wire.ShardRequest)
					q.OperationId = id
					_, e := shards.Execute(ctx, q)
					return e
				}
				q := proto.Clone(queueReq).(*wire.QueueRequest)
				q.OperationId = id
				_, e := queues.Execute(ctx, q)
				return e
			}
			inspect := func(wantCount uint64, wantQueue bool) {
				t.Helper()
				_, e := o.Run(ctx, func(db *native.Db) ([]byte, error) {
					tx, e := db.Begin(native.IsolationLevelSerializableSnapshot)
					if e != nil {
						return nil, backend(e)
					}
					defer tx.Destroy()
					b, e := get(tx, "v1/outcome_count")
					if e != nil {
						return nil, e
					}
					if len(b) != 8 || binary.BigEndian.Uint64(b) != wantCount {
						t.Errorf("journal count changed on collision: %x", b)
					}
					m, e := queueMetadata(tx, 1)
					if e != nil {
						return nil, e
					}
					if (m != nil) != wantQueue {
						t.Error("collision changed queue initialization")
					}
					b, e = get(tx, "v1/shard/0000000001")
					if e == nil && b != nil {
						t.Error("GET unexpectedly changed shard state")
					}
					return nil, e
				})
				if e != nil {
					t.Fatal(e)
				}
			}
			original = first()
			if e := second("same-id"); status.Code(e) != codes.InvalidArgument {
				t.Fatal("cross-family collision accepted", e)
			}
			inspect(1, queueFirst)
			stop()
			closeOwner(t, o)
			o = owner(t, engine(t, objects, path, false))
			defer closeOwner(t, o)
			shards, queues, stop = start(o)
			defer stop()
			if e := second("same-id"); status.Code(e) != codes.InvalidArgument {
				t.Fatal("reopened collision accepted", e)
			}
			inspect(1, queueFirst)
			if e := second("independent-id"); e != nil {
				t.Fatal("distinct identity rejected", e)
			}
			inspect(2, true)
			if replay := first(); !proto.Equal(original, replay) {
				t.Fatal("original outcome changed", replay)
			}
			inspect(2, true)
		})
	}
}
