package node

import (
	"context"
	"crypto/sha256"
	"testing"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func metadataRequest(id string, c *wire.MetadataCommand) *wire.MetadataRequest {
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	digest := sha256.Sum256(raw)
	return &wire.MetadataRequest{ProtocolVersion: 1, Partition: "p", OperationId: id, CommandSha256: digest[:], Command: c}
}

func TestGoOwnerNamespaceRecovery(t *testing.T) {
	objects := memory.New()
	path := "memory" + "-namespace-recovery"
	first := memoryOwner(t, objects, path, "p")
	server := &MetadataServer{Owner: first}
	ctx := context.Background()
	id := make([]byte, 16)
	id[15] = 1
	create := metadataRequest("namespace-create", &wire.MetadataCommand{Kind: wire.MetadataCommand_CREATE, Id: id, Name: "original", Data: []byte{0, 255}, Encoding: 2, IsGlobal: true})
	initial, err := server.Execute(ctx, create)
	if err != nil || initial.Error != wire.MetadataResult_NONE {
		t.Fatal(initial, err)
	}
	renamed, err := server.Execute(ctx, metadataRequest("namespace-rename", &wire.MetadataCommand{Kind: wire.MetadataCommand_RENAME, Id: id, Name: "renamed", PreviousName: "intentionally-ignored", Data: []byte{9, 8}, Encoding: 2, IsGlobal: true, NotificationVersion: 2}))
	if err != nil || renamed.Error != wire.MetadataResult_NONE {
		t.Fatal(renamed, err)
	}
	closeMemoryOwner(t, first)
	reopened := memoryOwner(t, objects, path, "p")
	defer closeMemoryOwner(t, reopened)
	server = &MetadataServer{Owner: reopened}
	replay, err := server.Execute(ctx, create)
	if err != nil || !proto.Equal(initial, replay) {
		t.Fatal("replay changed", replay, err)
	}
	current, err := server.Execute(ctx, metadataRequest("namespace-current", &wire.MetadataCommand{Kind: wire.MetadataCommand_GET, Name: "renamed"}))
	if err != nil || len(current.Namespaces) != 1 {
		t.Fatal(current, err)
	}
	expected := &wire.NamespaceRecord{Id: id, Name: "renamed", Data: []byte{9, 8}, Encoding: 2, IsGlobal: true, NotificationVersion: 2}
	if !proto.Equal(current.Namespaces[0], expected) {
		t.Fatal("recovered namespace differs", current)
	}
	version, err := server.Execute(ctx, metadataRequest("namespace-version", &wire.MetadataCommand{Kind: wire.MetadataCommand_GET_METADATA}))
	if err != nil || version.NotificationVersion != 3 {
		t.Fatal("replay advanced version", version, err)
	}
	if _, err = reopened.Execute(ctx, request("namespace-create", &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: 1})); status.Code(err) != codes.InvalidArgument {
		t.Fatal("cross-family ID reuse", err)
	}
	// A new writer fences replay too, even when the original result is journaled.
	competitor := memoryOwner(t, objects, path, "p")
	defer closeMemoryOwner(t, competitor)
	if _, err = server.Execute(ctx, create); status.Code(err) != codes.Unavailable {
		t.Fatal("stale owner published replay", err)
	}
}
