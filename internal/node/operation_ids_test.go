package node

import (
	"context"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestGoOwnerOperationIDMigrationReplay(t *testing.T) {
	objects := memory.New()
	path := "operation-id-compat"
	first := memoryOwner(t, objects, path, "p")
	old := request("723ef4ba-ea7b-4c25-8b7a-bf19074691f4", &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: 7, RangeId: 11, Data: []byte("legacy")})
	original, err := first.Execute(context.Background(), old)
	if err != nil {
		closeMemoryOwner(t, first)
		t.Fatal(err)
	}
	closeMemoryOwner(t, first)
	recovered := memoryOwner(t, objects, path, "p")
	defer closeMemoryOwner(t, recovered)
	replay, err := recovered.Execute(context.Background(), old)
	if err != nil || !proto.Equal(original, replay) {
		t.Fatal("legacy replay changed", replay, err)
	}
	changed := request(old.OperationId, &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: 7})
	if _, err = recovered.Execute(context.Background(), changed); status.Code(err) != codes.InvalidArgument {
		t.Fatal("legacy digest reuse accepted", err)
	}
	newer := request("op_0000000000000000000001", &wire.ShardCommand{Kind: wire.ShardCommand_UPDATE, ShardId: 7, PreviousRangeId: 11, RangeId: 12, Data: []byte("new")})
	updated, err := recovered.Execute(context.Background(), newer)
	if err != nil || updated.RangeId != 12 {
		t.Fatal("new ID rejected by old runtime", updated, err)
	}
	replay, err = recovered.Execute(context.Background(), old)
	if err != nil || !proto.Equal(original, replay) {
		t.Fatal("new ID rewrote legacy result", replay, err)
	}
}
