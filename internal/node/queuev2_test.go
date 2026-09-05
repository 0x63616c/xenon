package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/adapter"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	upstream "go.temporal.io/server/common/persistence/tests"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
	"net"
	"os"
	native "slatedb.io/slatedb-go/uniffi"
	"sync/atomic"
	"testing"
)

type queueV2Fixture struct {
	Schema       int    `json:"schema"`
	QueueType    int64  `json:"queue_type"`
	QueueName    string `json:"queue_name"`
	BlobBytes    int    `json:"blob_bytes"`
	MessageCount int    `json:"message_count"`
	Budget       int    `json:"response_budget_bytes"`
}

func queueV2Case(t *testing.T) queueV2Fixture {
	t.Helper()
	b, e := os.ReadFile("../../proof/queuev2/case.json")
	if e != nil {
		t.Fatal(e)
	}
	var f queueV2Fixture
	if e = json.Unmarshal(b, &f); e != nil || f.Schema != 1 || f.MessageCount != 3 || f.BlobBytes != 1048576 || f.Budget != 3145728 {
		t.Fatal("invalid fixture", e)
	}
	return f
}
func TestGoOwnerQueueV2Upstream(t *testing.T) {
	o := owner(t, engine(t, objects(t), cfg(t).Prefix+"-queuev2-upstream", false))
	t.Cleanup(func() { closeOwner(t, o) })
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	var dropped atomic.Bool
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		r, e := handler(ctx, req)
		if q, ok := req.(*wire.QueueV2Request); ok && q.Command.Kind == wire.QueueV2Command_ENQUEUE && e == nil && r.(*wire.QueueV2Result).Error == wire.QueueV2Result_NONE && dropped.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "declared completed QueueV2 response loss")
		}
		return r, e
	}))
	wire.RegisterQueueV2PersistenceServer(server, &QueueV2Server{Owner: o})
	go server.Serve(l)
	t.Cleanup(server.Stop)
	q, e := adapter.NewQueueV2(l.Addr().String(), "p")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(q.Close)
	t.Cleanup(func() {
		if !dropped.Load() {
			t.Error("response loss did not execute")
		}
	})
	upstream.RunQueueV2TestSuite(t, q)
}
func queueV2Request(id string, c *wire.QueueV2Command) *wire.QueueV2Request {
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	h := sha256.Sum256(b)
	return &wire.QueueV2Request{ProtocolVersion: 1, Partition: "p", OperationId: id, CommandSha256: h[:], Command: c}
}
func TestGoOwnerQueueV2Recovery(t *testing.T) {
	f := queueV2Case(t)
	obj := objects(t)
	path := cfg(t).Prefix + "-queuev2-recovery"
	o := owner(t, engine(t, obj, path, false))
	s := &QueueV2Server{Owner: o}
	ctx := context.Background()
	counter := 0
	call := func(c *wire.QueueV2Command) *wire.QueueV2Result {
		t.Helper()
		counter++
		c.QueueType = f.QueueType
		c.QueueName = f.QueueName
		r, e := s.Execute(ctx, queueV2Request(fmt.Sprintf("step-%d", counter), c))
		if e != nil || r.Error != wire.QueueV2Result_NONE {
			t.Fatal(r, e)
		}
		return r
	}
	call(&wire.QueueV2Command{Kind: wire.QueueV2Command_CREATE})
	var initial *wire.QueueV2Result
	var saved *wire.QueueV2Request
	for i := 0; i < f.MessageCount; i++ {
		c := &wire.QueueV2Command{Kind: wire.QueueV2Command_ENQUEUE, QueueType: f.QueueType, QueueName: f.QueueName, HasBlob: true, Data: bytes.Repeat([]byte{byte(i)}, f.BlobBytes), Encoding: 2}
		if i == 0 {
			saved = queueV2Request("enqueue-original", c)
			var e error
			initial, e = s.Execute(ctx, saved)
			if e != nil {
				t.Fatal(e)
			}
		} else {
			call(c)
		}
	}
	page := call(&wire.QueueV2Command{Kind: wire.QueueV2Command_READ, PageSize: 1000})
	if len(page.Messages) != 2 || proto.Size(page) > f.Budget {
		t.Fatal("byte page", len(page.Messages))
	}
	next := call(&wire.QueueV2Command{Kind: wire.QueueV2Command_READ, PageSize: 1000, NextPageToken: page.NextPageToken})
	if len(next.Messages) != 1 || next.Messages[0].Id != 2 {
		t.Fatal(next)
	}
	deleteRequest := queueV2Request("delete-original", &wire.QueueV2Command{Kind: wire.QueueV2Command_DELETE_RANGE, QueueType: f.QueueType, QueueName: f.QueueName, InclusiveMaxId: math.MaxInt64})
	deleted, deleteErr := s.Execute(ctx, deleteRequest)
	if deleteErr != nil {
		t.Fatal(deleteErr)
	}
	if deleted.Deleted != 3 {
		t.Fatal(deleted)
	}
	stale := call(&wire.QueueV2Command{Kind: wire.QueueV2Command_READ, PageSize: 100, NextPageToken: page.NextPageToken})
	if len(stale.Messages) != 0 {
		t.Fatal("stale token resurrected retained sentinel")
	}
	closeOwner(t, o)
	o = owner(t, engine(t, obj, path, false))
	defer closeOwner(t, o)
	s.Owner = o
	replay, e := s.Execute(ctx, saved)
	if e != nil || !proto.Equal(initial, replay) {
		t.Fatal(replay, e)
	}
	newRow := call(&wire.QueueV2Command{Kind: wire.QueueV2Command_ENQUEUE, HasBlob: true, Data: []byte("next"), Encoding: 2})
	if newRow.MessageId != 3 {
		t.Fatal("reused ID", newRow)
	}
	deleteReplay, e := s.Execute(ctx, deleteRequest)
	if e != nil || !proto.Equal(deleted, deleteReplay) {
		t.Fatal(deleteReplay, e)
	}
	current := call(&wire.QueueV2Command{Kind: wire.QueueV2Command_READ, PageSize: 100})
	if len(current.Messages) != 1 || current.Messages[0].Id != 3 {
		t.Fatal(current)
	}
	terminal, _ := (&persistencespb.ReadQueueMessagesNextPageToken{LastReadMessageId: math.MaxInt64}).Marshal()
	done := call(&wire.QueueV2Command{Kind: wire.QueueV2Command_READ, PageSize: 100, NextPageToken: append([]byte{0}, terminal...)})
	if len(done.Messages) != 0 {
		t.Fatal("terminal token overflow")
	}
	// A callback failure after staging range metadata and deletes cannot publish either.
	_, e = o.Run(ctx, func(*native.Db) ([]byte, error) {
		_, e := o.journal("abort-delete", []byte("digest"), queuev2Family, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
			_, e := applyQueueV2(tx, &wire.QueueV2Command{Kind: wire.QueueV2Command_DELETE_RANGE, QueueType: f.QueueType, QueueName: f.QueueName, InclusiveMaxId: 3})
			if e != nil {
				return nil, e
			}
			return nil, status.Error(codes.ResourceExhausted, "declared abort")
		})
		return nil, e
	})
	if status.Code(e) != codes.ResourceExhausted {
		t.Fatal(e)
	}
	current = call(&wire.QueueV2Command{Kind: wire.QueueV2Command_READ, PageSize: 100})
	if len(current.Messages) != 1 {
		t.Fatal("aborted logical deletion escaped")
	}
	competitor := owner(t, engine(t, obj, path, false))
	defer closeOwner(t, competitor)
	if _, e = s.Execute(ctx, saved); status.Code(e) != codes.Unavailable || !o.Quarantined() {
		t.Fatal("fenced replay", e)
	}
}

func TestGoOwnerQueueV2Terminal(t *testing.T) {
	f := queueV2Case(t)
	o := owner(t, engine(t, objects(t), cfg(t).Prefix+"-queuev2-terminal", false))
	defer closeOwner(t, o)
	s := &QueueV2Server{Owner: o}
	ctx := context.Background()
	_, err := o.Run(ctx, func(db *native.Db) ([]byte, error) {
		tx, e := db.Begin(native.IsolationLevelSerializableSnapshot)
		if e != nil {
			return nil, backend(e)
		}
		defer tx.Destroy()
		e = saveCluster(tx, qv2StateKey(f.QueueType, f.QueueName), &wire.QueueV2State{Name: f.QueueName, MinId: math.MaxInt64 - 1, LastId: math.MaxInt64 - 1})
		if e != nil {
			return nil, e
		}
		e = saveCluster(tx, qv2MessageKey(f.QueueType, f.QueueName, math.MaxInt64-1), &wire.QueueV2Entry{Id: math.MaxInt64 - 1, Encoding: 2})
		if e != nil {
			return nil, e
		}
		return nil, commit(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	enqueue := queueV2Request("terminal-enqueue", &wire.QueueV2Command{Kind: wire.QueueV2Command_ENQUEUE, QueueType: f.QueueType, QueueName: f.QueueName, HasBlob: true})
	if _, err = s.Execute(ctx, enqueue); status.Code(err) != codes.ResourceExhausted || o.Quarantined() {
		t.Fatal("terminal allocation", err)
	}
	r, err := s.Execute(ctx, queueV2Request("terminal-delete", &wire.QueueV2Command{Kind: wire.QueueV2Command_DELETE_RANGE, QueueType: f.QueueType, QueueName: f.QueueName, InclusiveMaxId: math.MaxInt64}))
	if err != nil || r.Deleted != 1 {
		t.Fatal(r, err)
	}
	r, err = s.Execute(ctx, queueV2Request("terminal-read", &wire.QueueV2Command{Kind: wire.QueueV2Command_READ, QueueType: f.QueueType, QueueName: f.QueueName, PageSize: 1}))
	if err != nil || len(r.Messages) != 0 {
		t.Fatal("unrepresentable min", r, err)
	}
}
