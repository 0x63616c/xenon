package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"slices"
	"testing"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type historyWriter struct {
	*testWriter
	scanErr error
}
type historyTx struct {
	*testTx
	scanErr error
}

func (w *historyWriter) Begin(context.Context) (partitions.Transaction, error) {
	return &historyTx{&testTx{w.testWriter, maps.Clone(w.durable)}, w.scanErr}, nil
}
func (tx *historyTx) Scan(_ context.Context, r partitions.ScanRequest) (partitions.ReadResult, error) {
	if !r.RemoteDurable {
		return partitions.ReadResult{}, fmt.Errorf("history lost remote durability")
	}
	if tx.scanErr != nil {
		return partitions.ReadResult{}, tx.scanErr
	}
	keys := slices.Sorted(maps.Keys(tx.data))
	if r.Reverse {
		slices.Reverse(keys)
	}
	result := partitions.ReadResult{}
	for _, key := range keys {
		b := []byte(key)
		if r.Start != nil && (bytes.Compare(b, r.Start) < 0 || r.StartExclusive && bytes.Equal(b, r.Start)) {
			continue
		}
		if r.End != nil && (bytes.Compare(b, r.End) > 0 || !r.EndInclusive && bytes.Equal(b, r.End)) {
			continue
		}
		if len(result.Entries) == r.Limit {
			result.More = true
			break
		}
		result.Entries = append(result.Entries, partitions.Entry{Key: b, Value: bytes.Clone(tx.data[key])})
	}
	return result, nil
}
func historyRequest(id int, c *wire.HistoryCommand) *wire.HistoryRequest {
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	digest := sha256.Sum256(raw)
	return &wire.HistoryRequest{ProtocolVersion: 1, Partition: "prt_0000000000000000000001", OperationId: fmt.Sprintf("op_%022d", id), Command: c, CommandSha256: digest[:]}
}
func TestHistoryOrderingPaginationReplayAndTypedFailure(t *testing.T) {
	w := &historyWriter{testWriter: &testWriter{durable: map[string][]byte{}}}
	var failure error
	base, _ := NewService(w, "prt_0000000000000000000001", 1000, func(context.Context) error { return nil }, func(err error) { failure = err })
	service, _ := NewHistoryService(base)
	execute := func(id int, c *wire.HistoryCommand) *wire.HistoryResult {
		t.Helper()
		r, err := service.Execute(context.Background(), historyRequest(id, c))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	tree, branch := bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 16)
	appendCommand := &wire.HistoryCommand{Kind: wire.HistoryCommand_APPEND, ShardId: 7, TreeId: tree, BranchId: branch, IsNewBranch: true, TreeInfo: &wire.HistoryBlob{Data: []byte("tree")}, Node: &wire.HistoryNodeRecord{NodeId: 1, TransactionId: 10, Events: &wire.HistoryBlob{Data: []byte("events")}}}
	execute(1, appendCommand)
	for index, pair := range [][2]int64{{1, 20}, {2, 5}, {3, 1}} {
		c := proto.Clone(appendCommand).(*wire.HistoryCommand)
		c.IsNewBranch = false
		c.Node.NodeId = pair[0]
		c.Node.TransactionId = pair[1]
		execute(index+2, c)
	}
	read := &wire.HistoryCommand{Kind: wire.HistoryCommand_READ, ShardId: 7, TreeId: tree, BranchId: branch, MinNodeId: 1, MaxNodeId: 4, PageSize: 2}
	first := execute(10, read)
	if len(first.Nodes) != 2 || first.Nodes[0].TransactionId != 20 || first.Nodes[1].TransactionId != 10 || len(first.NextPageToken) == 0 {
		t.Fatal(first)
	}
	read.NextPageToken = first.NextPageToken
	second := execute(11, read)
	if len(second.Nodes) != 2 || second.Nodes[0].NodeId != 2 || second.Nodes[1].NodeId != 3 {
		t.Fatal(second)
	}
	read.NextPageToken = second.NextPageToken
	if tail := execute(12, read); len(tail.Nodes) != 0 || len(tail.NextPageToken) != 0 {
		t.Fatal(tail)
	}
	read.NextPageToken = nil
	read.ReverseOrder = true
	read.MetadataOnly = true
	reverse := execute(13, read)
	if len(reverse.Nodes) != 2 || reverse.Nodes[0].NodeId != 3 || reverse.Nodes[1].NodeId != 2 || reverse.Nodes[0].Events != nil {
		t.Fatal(reverse)
	}
	read.NextPageToken = reverse.NextPageToken
	reverse = execute(14, read)
	if len(reverse.Nodes) != 2 || reverse.Nodes[0].TransactionId != 10 || reverse.Nodes[1].TransactionId != 20 {
		t.Fatal(reverse)
	}
	execute(20, &wire.HistoryCommand{Kind: wire.HistoryCommand_DELETE_BRANCH, ShardId: 7, TreeId: tree, BranchId: branch, Ranges: []*wire.HistoryDeleteRange{{BranchId: branch, BeginNodeId: 1}}})
	execute(1, appendCommand) // durable replay must not recreate deleted events/tree
	for key := range w.durable {
		if bytes.HasPrefix([]byte(key), []byte("v1/history/")) {
			t.Fatal("replay resurrected history", key)
		}
	}
	w.scanErr = partitions.ErrFenced
	read.NextPageToken = nil
	if _, err := service.Execute(context.Background(), historyRequest(30, read)); err != partitions.ErrFenced || failure != partitions.ErrFenced {
		t.Fatal("typed native failure changed", err, failure)
	}
	invalid := proto.Clone(appendCommand).(*wire.HistoryCommand)
	invalid.Node = nil
	if _, err := service.Execute(context.Background(), historyRequest(31, invalid)); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
}
