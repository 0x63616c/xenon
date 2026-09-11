package node

import (
	"context"
	"crypto/sha256"
	"testing"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/partitions/memory"
	"google.golang.org/protobuf/proto"
)

func TestOwnerRunsOnPartitionWriter(t *testing.T) {
	ids := identity.Generator{}
	partition, _ := identity.NewPartitionID(ids)
	reservation, _ := identity.NewTransitionID(ids)
	incarnation, _ := identity.NewIncarnationID(ids)
	writer, err := memory.New().Open(context.Background(), partitions.OpenRequest{
		Path: "node-test", Partition: partition, AssignmentRevision: 1,
		Reservation: reservation, Incarnation: incarnation, Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := NewOwner(writer, DefaultConfig("p"))
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())

	operation, _ := identity.NewOperationID(ids)
	command := &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: 7, RangeId: 11}
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	digest := sha256.Sum256(raw)
	request := &wire.ShardRequest{ProtocolVersion: 1, Partition: "p", OperationId: string(operation), CommandSha256: digest[:], Command: command}

	first, err := owner.Execute(context.Background(), request)
	if err != nil || first.Error != wire.ShardResult_NONE {
		t.Fatalf("first execution: result=%v err=%v", first, err)
	}
	replayed, err := owner.Execute(context.Background(), request)
	if err != nil || !proto.Equal(first, replayed) {
		t.Fatalf("replay changed: result=%v err=%v", replayed, err)
	}
}
