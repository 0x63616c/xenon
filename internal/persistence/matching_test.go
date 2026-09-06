package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"math"
	"testing"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestMatchingReplayRetainsFairBoundsAndAtomicUserData(t *testing.T) {
	w := &clusterWriter{testWriter: &testWriter{durable: map[string][]byte{}}}
	var service *MatchingService
	bind := func() {
		base, err := NewService(w, "prt_0000000000000000000001", 100, func(context.Context) error { return nil }, func(error) {})
		if err != nil {
			t.Fatal(err)
		}
		service, err = NewMatchingService(base)
		if err != nil {
			t.Fatal(err)
		}
	}
	bind()
	request := func(id int, c *wire.MatchingCommand) *wire.MatchingRequest {
		c = proto.Clone(c).(*wire.MatchingCommand)
		c.NamespaceId = bytes.Repeat([]byte{1}, 16)
		if c.Queue == "" {
			c.Queue = "tasks"
		}
		raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		d := sha256.Sum256(raw)
		return &wire.MatchingRequest{ProtocolVersion: MatchingProtocol(c), Partition: "prt_0000000000000000000001", OperationId: fmt.Sprintf("op_%022d", id), Command: c, CommandSha256: d[:]}
	}
	call := func(id int, c *wire.MatchingCommand) *wire.MatchingResult {
		r, err := service.Execute(context.Background(), request(id, c))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if r := call(1, &wire.MatchingCommand{Kind: wire.MatchingCommand_CREATE_QUEUE, Fair: true, RangeId: 7}); r.Error != wire.MatchingResult_NONE {
		t.Fatal(r)
	}
	tasks := &wire.MatchingCommand{Kind: wire.MatchingCommand_CREATE_TASKS, Fair: true, RangeId: 7, Tasks: []*wire.MatchingTask{{Id: 1, Pass: 1}, {Id: 2, Pass: 1}, {Id: 3, Pass: 2}}}
	created := call(2, tasks)
	if created.Error != wire.MatchingResult_NONE {
		t.Fatal(created)
	}
	read := &wire.MatchingCommand{Kind: wire.MatchingCommand_GET_TASKS, Fair: true, MinPass: 1, MaxId: math.MaxInt64, PageSize: 1}
	page := call(3, read)
	if len(page.Tasks) != 1 || page.Tasks[0].Id != 1 || len(page.Token) != 49 {
		t.Fatal("first fair page", page)
	}
	if r := call(4, &wire.MatchingCommand{Kind: wire.MatchingCommand_COMPLETE_TASKS, Fair: true, MaxPass: 1, MaxId: 2, PageSize: 10}); r.Completed != 1 {
		t.Fatal("exclusive completion bound", r)
	}
	w = &clusterWriter{testWriter: &testWriter{durable: maps.Clone(w.durable)}}
	bind()
	if r := call(2, tasks); !proto.Equal(created, r) {
		t.Fatal("replay changed task batch", r)
	}
	fresh := call(5, read)
	if len(fresh.Tasks) != 1 || fresh.Tasks[0].Id != 2 {
		t.Fatal("deleted task resurrected", fresh)
	}
	read.Token = page.Token
	read.PageSize = 10
	page = call(6, read)
	if len(page.Tasks) != 2 || page.Tasks[0].Id != 2 || page.Tasks[1].Pass != 2 || len(page.Token) != 0 {
		t.Fatal("saved cursor did not resume across deletion", page)
	}
	read.Queue = "other"
	if _, err := service.Execute(context.Background(), request(7, read)); status.Code(err) != codes.InvalidArgument {
		t.Fatal("cross-queue cursor accepted", err)
	}
	init := &wire.MatchingCommand{Kind: wire.MatchingCommand_UPDATE_USER_DATA, Updates: []*wire.MatchingUserUpdate{{Queue: "alpha", Data: []byte("a"), BuildIdsAdded: []string{"old"}}, {Queue: "beta", Data: []byte("b")}}}
	if r := call(8, init); !r.Applied {
		t.Fatal(r)
	}
	bad := &wire.MatchingCommand{Kind: wire.MatchingCommand_UPDATE_USER_DATA, Updates: []*wire.MatchingUserUpdate{{Queue: "alpha", Version: 1, Data: []byte("must-not-apply"), BuildIdsAdded: []string{"bad"}}, {Queue: "beta", Version: 8}}}
	rejected := call(9, bad)
	if rejected.Error != wire.MatchingResult_CONDITION_FAILED || len(rejected.Conflicting) != 1 || rejected.Conflicting[0] != "beta" {
		t.Fatal(rejected)
	}
	if r := call(10, &wire.MatchingCommand{Kind: wire.MatchingCommand_GET_USER_DATA, Queue: "alpha"}); len(r.UserData) != 1 || r.UserData[0].Version != 1 || string(r.UserData[0].Data) != "a" {
		t.Fatal("failed batch partially applied", r)
	}
	if r := call(11, &wire.MatchingCommand{Kind: wire.MatchingCommand_COUNT_BY_BUILD, BuildId: "bad"}); r.Count != 0 {
		t.Fatal("failed batch leaked index", r)
	}
	if r := call(12, &wire.MatchingCommand{Kind: wire.MatchingCommand_UPDATE_USER_DATA, Updates: []*wire.MatchingUserUpdate{{Queue: "alpha", Version: 1, Data: []byte("new"), BuildIdsRemoved: []string{"old"}, BuildIdsAdded: []string{"new"}}}}); !r.Applied {
		t.Fatal(r)
	}
	if r := call(9, bad); !proto.Equal(rejected, r) {
		t.Fatal("logical failure replay changed", r)
	}
	if r := call(13, &wire.MatchingCommand{Kind: wire.MatchingCommand_GET_BY_BUILD, BuildId: "new"}); r.Count != 1 || len(r.QueueNames) != 1 || r.QueueNames[0] != "alpha" {
		t.Fatal("new index missing", r)
	}
	if r := call(14, &wire.MatchingCommand{Kind: wire.MatchingCommand_COUNT_BY_BUILD, BuildId: "old"}); r.Count != 0 {
		t.Fatal("removed index retained", r)
	}
}
