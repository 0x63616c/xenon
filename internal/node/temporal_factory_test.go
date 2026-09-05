package node

import (
	"context"
	"net"
	"testing"
	"time"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/temporalstore"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/config"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestGoOwnerTemporalFactory(t *testing.T) {
	objects := objects(t)
	makeOwner := func(name string) *Owner {
		o, e := NewOwner(engine(t, objects, cfg(t).Prefix+"-factory-"+name, false), DefaultConfig(name))
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { closeOwner(t, o) })
		return o
	}
	history, matching, global := makeOwner("history"), makeOwner("matching"), makeOwner("global")
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server := grpc.NewServer()
	wire.RegisterShardPersistenceServer(server, history)
	wire.RegisterMatchingPersistenceServer(server, &MatchingServer{Owner: matching})
	wire.RegisterMetadataPersistenceServer(server, &MetadataServer{Owner: global})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	cfg := config.CustomDatastoreConfig{Name: "xenon", Options: map[string]any{"address": listener.Addr().String(), "historyPartition": "history", "matchingPartition": "matching", "globalPartition": "global"}}
	factory := func() p.DataStoreFactory {
		return (temporalstore.AbstractFactory{}).NewFactory(cfg, nil, "cluster", nil, nil, nil)
	}
	f := factory()
	t.Cleanup(f.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shard, e := f.NewShardStore()
	if e != nil {
		t.Fatal(e)
	}
	blob := &commonpb.DataBlob{Data: []byte{0, 255, 7}, EncodingType: enumspb.ENCODING_TYPE_PROTO3}
	if shard.GetClusterName() != "cluster" {
		t.Fatal("cluster identity lost")
	}
	created, e := shard.GetOrCreateShard(ctx, &p.InternalGetOrCreateShardRequest{ShardID: 17, CreateShardInfo: func() (int64, *commonpb.DataBlob, error) { return 1, blob, nil }})
	if e != nil || !proto.Equal(created.ShardInfo, blob) {
		t.Fatal(created, e)
	}
	metadata, e := f.NewMetadataStore()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = metadata.GetMetadata(ctx); e != nil {
		t.Fatal(e)
	}
	legacy, e := f.NewTaskStore()
	if e != nil {
		t.Fatal(e)
	}
	fair, e := f.NewFairTaskStore()
	if e != nil {
		t.Fatal(e)
	}
	q := &p.InternalCreateTaskQueueRequest{NamespaceID: "00000000-0000-0000-0000-000000000001", TaskQueue: "same", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, RangeID: 1, TaskQueueInfo: blob}
	if e = legacy.CreateTaskQueue(ctx, q); e != nil {
		t.Fatal(e)
	}
	if e = fair.CreateTaskQueue(ctx, q); e != nil {
		t.Fatal("fair did not use separate version/keyspace", e)
	}
	f.Close()
	f.Close()
	if _, e = f.NewExecutionStore(); e == nil {
		t.Fatal("closed factory accepted new store")
	}
	again := factory()
	defer again.Close()
	reopened, e := again.NewShardStore()
	if e != nil {
		t.Fatal(e)
	}
	got, e := reopened.GetOrCreateShard(ctx, &p.InternalGetOrCreateShardRequest{ShardID: 17})
	if e != nil || !proto.Equal(got.ShardInfo, blob) {
		t.Fatal("factory close affected durable state", got, e)
	}
	cfg.Options["typo"] = true
	invalid := factory()
	defer invalid.Close()
	if _, e = invalid.NewShardStore(); e == nil {
		t.Fatal("unknown configuration silently accepted")
	}
}
