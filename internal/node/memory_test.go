package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/partitions/memory"
	"google.golang.org/protobuf/proto"
)

func commitTest(writer partitions.Writer, tx partitions.Transaction) error {
	receipt, err := tx.Commit(context.Background())
	if err != nil {
		return err
	}
	return writer.AwaitDurable(context.Background(), receipt)
}

func putMessage(tx partitions.Transaction, key string, message proto.Message) error {
	raw, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	return tx.Put([]byte(key), raw)
}

func scanPrefix(tx partitions.Transaction, prefix string, reverse bool, visit func([]byte, []byte) (bool, error)) error {
	result, err := tx.Scan(context.Background(), partitions.ScanRequest{Start: []byte(prefix), End: append(bytes.Clone([]byte(prefix)), 0xff), Reverse: reverse, Limit: 100000})
	if err != nil {
		return err
	}
	for _, entry := range result.Entries {
		stop, err := visit(entry.Key, entry.Value)
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	return nil
}

func testHistoryNodeKey(shard int32, tree, branch []byte, node *wire.HistoryNodeRecord) string {
	return fmt.Sprintf("v1/history/node/%010d/%x/%x/%016x/%016x", shard, tree, branch, uint64(node.NodeId)^1<<63, ^(uint64(node.TransactionId) ^ 1<<63))
}

func memoryWriter(t *testing.T, engine *memory.Engine, path string) partitions.Writer {
	t.Helper()
	ids := identity.Generator{}
	partition, _ := identity.NewPartitionID(ids)
	reservation, _ := identity.NewTransitionID(ids)
	incarnation, _ := identity.NewIncarnationID(ids)
	writer, err := engine.Open(context.Background(), partitions.OpenRequest{
		Path: path, Partition: partition, AssignmentRevision: 1,
		Reservation: reservation, Incarnation: incarnation, Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

func request(id string, command *wire.ShardCommand) *wire.ShardRequest {
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	digest := sha256.Sum256(raw)
	return &wire.ShardRequest{ProtocolVersion: 1, Partition: "p", OperationId: id, CommandSha256: digest[:], Command: command}
}

func memoryOwner(t *testing.T, engine *memory.Engine, path, partition string) *Owner {
	t.Helper()
	owner, err := NewOwner(memoryWriter(t, engine, path), DefaultConfig(partition))
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func closeMemoryOwner(t *testing.T, owner *Owner) {
	t.Helper()
	if err := owner.Close(context.Background()); err != nil {
		t.Error(err)
	}
}
