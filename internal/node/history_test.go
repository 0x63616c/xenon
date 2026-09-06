package node

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"os"
	native "slatedb.io/slatedb-go/uniffi"
	"testing"
)

func historyRequest(id string, c *wire.HistoryCommand) *wire.HistoryRequest {
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	d := sha256.Sum256(raw)
	return &wire.HistoryRequest{ProtocolVersion: 1, Partition: "p", OperationId: id, CommandSha256: d[:], Command: c}
}
func TestGoOwnerHistoryRecovery(t *testing.T) {
	raw, e := os.ReadFile("../../proof/history/case.json")
	if e != nil {
		t.Fatal(e)
	}
	var c struct {
		Tree   string `json:"tree_id"`
		Branch string `json:"branch_id"`
		Prefix string `json:"prefix"`
		Schema int    `json:"schema_version"`
	}
	if e = json.Unmarshal(raw, &c); e != nil || c.Schema != 1 {
		t.Fatal(e)
	}
	tree := uuid.MustParse(c.Tree)
	branch := uuid.MustParse(c.Branch)
	objects := objects(t)
	path := c.Prefix + "-recovery"
	o := owner(t, engine(t, objects, path, false))
	server := &HistoryServer{Owner: o}
	ctx := context.Background()
	appendCommand := &wire.HistoryCommand{Kind: wire.HistoryCommand_APPEND, ShardId: 7, TreeId: tree[:], BranchId: branch[:], IsNewBranch: true, TreeInfo: &wire.HistoryBlob{Data: []byte{0, 255}, Encoding: 2}, Node: &wire.HistoryNodeRecord{NodeId: 1, TransactionId: 9991, Events: &wire.HistoryBlob{Data: []byte{255, 0}, Encoding: 1}}}
	// The exact apply function stages both tree and node, then the shared journal
	// callback aborts before publishing either state or outcome.
	_, e = o.Run(ctx, func(*native.Db) ([]byte, error) {
		_, e := o.journal("aborted-new-branch", []byte("abort"), historyFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
			if _, e := applyHistory(tx, appendCommand); e != nil {
				return nil, e
			}
			return nil, status.Error(codes.ResourceExhausted, "declared atomic rollback barrier")
		})
		return nil, e
	})
	if status.Code(e) != codes.ResourceExhausted || o.Quarantined() {
		t.Fatal(e)
	}
	_, e = o.Run(ctx, func(db *native.Db) ([]byte, error) {
		tx, e := db.Begin(native.IsolationLevelSerializableSnapshot)
		if e != nil {
			return nil, backend(e)
		}
		defer tx.Destroy()
		for _, k := range []string{historyTreeKey(7, tree[:], branch[:]), historyNodeKey(7, tree[:], branch[:], appendCommand.Node), "v1/outcome/aborted-new-branch"} {
			v, e := get(tx, k)
			if e != nil {
				return nil, e
			}
			if v != nil {
				t.Error("partial aborted append", k)
			}
		}
		return nil, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	request := historyRequest("first-append", appendCommand)
	initial, e := server.Execute(ctx, request)
	if e != nil || initial.Error != wire.HistoryResult_NONE {
		t.Fatal(initial, e)
	}
	later := proto.Clone(appendCommand).(*wire.HistoryCommand)
	later.Node.Events.Data = []byte{9}
	later.TreeInfo.Data = []byte{8}
	if _, e = server.Execute(ctx, historyRequest("upsert", later)); e != nil {
		t.Fatal(e)
	}
	closeOwner(t, o)
	o = owner(t, engine(t, objects, path, false))
	defer closeOwner(t, o)
	server = &HistoryServer{Owner: o}
	replay, e := server.Execute(ctx, request)
	if e != nil || !proto.Equal(replay, initial) {
		t.Fatal(replay, e)
	}
	nodes, e := server.Execute(ctx, historyRequest("read", &wire.HistoryCommand{Kind: wire.HistoryCommand_READ, ShardId: 7, TreeId: tree[:], BranchId: branch[:], MinNodeId: 1, MaxNodeId: 2, PageSize: 10}))
	if e != nil || len(nodes.Nodes) != 1 || nodes.Nodes[0].Events.Data[0] != 9 {
		t.Fatal("replay changed later state", nodes, e)
	}
	trees, e := server.Execute(ctx, historyRequest("tree", &wire.HistoryCommand{Kind: wire.HistoryCommand_GET_TREE, ShardId: 7, TreeId: tree[:], BranchId: branch[:]}))
	if e != nil || len(trees.Trees) != 1 || trees.Trees[0].Info.Data[0] != 8 {
		t.Fatal(trees, e)
	}
	competitor := owner(t, engine(t, objects, path, false))
	defer closeOwner(t, competitor)
	if _, e = server.Execute(ctx, request); status.Code(e) != codes.Unavailable || !o.Quarantined() {
		t.Fatal("fenced history replay", e)
	}
}
