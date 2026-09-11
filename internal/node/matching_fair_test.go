package node

import (
	"bytes"
	"context"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
	suites "go.temporal.io/server/common/persistence/tests"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
	"testing"
)

func TestGoOwnerFairUpstreamTasks(t *testing.T) {
	suite.Run(t, suites.NewTaskQueueFairTaskSuite(t, matchingStoreMode(t, true), log.NewNoopLogger()))
}
func TestGoOwnerFairUpstreamQueues(t *testing.T) {
	suite.Run(t, suites.NewTaskQueueSuite(t, matchingStoreMode(t, true), log.NewNoopLogger()))
}
func TestGoOwnerFairRecovery(t *testing.T) {
	objects := memory.New()
	path := "memory" + "-fair"
	o := memoryOwner(t, objects, path, "p")
	s := &MatchingServer{Owner: o}
	ctx := context.Background()
	ns := bytes.Repeat([]byte{1}, 16)
	call := func(c *wire.MatchingCommand) *wire.MatchingResult {
		t.Helper()
		c.NamespaceId = ns
		c.Queue = "same"
		r, e := s.Execute(ctx, matchingReq(uuid.NewString(), c))
		if e != nil || r.Error != wire.MatchingResult_NONE {
			t.Fatal(r, e)
		}
		return r
	}
	for _, fair := range []bool{false, true} {
		call(&wire.MatchingCommand{Kind: wire.MatchingCommand_CREATE_QUEUE, Fair: fair, RangeId: 1})
	}
	tasks := []*wire.MatchingTask{{Pass: 1, Id: 7, Data: []byte("first")}, {Pass: 2, Id: 7, Data: []byte("retain")}, {Pass: 2, Id: math.MaxInt64, Data: []byte("max")}, {Pass: 3, Id: 0, Data: []byte("next")}}
	create := &wire.MatchingCommand{Kind: wire.MatchingCommand_CREATE_TASKS, Fair: true, NamespaceId: ns, Queue: "same", RangeId: 1, Tasks: tasks}
	q := matchingReq("fair-create", create)
	downgraded := proto.Clone(q).(*wire.MatchingRequest)
	downgraded.ProtocolVersion = 1
	if _, e := s.Execute(ctx, downgraded); status.Code(e) != codes.InvalidArgument {
		t.Fatalf("fair downgrade accepted: %v", e)
	}
	first, e := s.Execute(ctx, q)
	if e != nil || first.Error != wire.MatchingResult_NONE {
		t.Fatal(first, e)
	}
	legacy := call(&wire.MatchingCommand{Kind: wire.MatchingCommand_GET_TASKS, MaxId: math.MaxInt64, PageSize: 10})
	if len(legacy.Tasks) != 0 {
		t.Fatal("fair leaked to legacy")
	}
	var token []byte
	count := 0
	for {
		r := call(&wire.MatchingCommand{Kind: wire.MatchingCommand_GET_TASKS, Fair: true, MinPass: 1, MaxId: math.MaxInt64, PageSize: 1, Token: token})
		for _, task := range r.Tasks {
			if count >= len(tasks) || !proto.Equal(task, tasks[count]) {
				t.Fatal("tuple order", count, task)
			}
			count++
		}
		token = r.Token
		if len(token) == 0 {
			break
		}
		if count > len(tasks) {
			t.Fatal("cursor loop")
		}
	}
	if count != len(tasks) {
		t.Fatal(count)
	}
	page := call(&wire.MatchingCommand{Kind: wire.MatchingCommand_GET_TASKS, Fair: true, MinPass: 1, MaxId: math.MaxInt64, PageSize: 1})
	changed := &wire.MatchingCommand{Kind: wire.MatchingCommand_GET_TASKS, Fair: true, NamespaceId: ns, Queue: "same", MinPass: 2, MaxId: math.MaxInt64, PageSize: 1, Token: page.Token}
	if _, e := s.Execute(ctx, matchingReq("changed-fair-range", changed)); status.Code(e) != codes.InvalidArgument {
		t.Fatalf("cursor changed range: %v", e)
	}
	deleted := call(&wire.MatchingCommand{Kind: wire.MatchingCommand_COMPLETE_TASKS, Fair: true, MaxPass: 2, MaxId: 0, PageSize: 1})
	if deleted.Completed != 1 {
		t.Fatal(deleted)
	}
	closeMemoryOwner(t, o)
	o = memoryOwner(t, objects, path, "p")
	defer closeMemoryOwner(t, o)
	s.Owner = o
	replay, e := s.Execute(ctx, q)
	if e != nil || !proto.Equal(first, replay) {
		t.Fatal(replay, e)
	}
	r := call(&wire.MatchingCommand{Kind: wire.MatchingCommand_GET_TASKS, Fair: true, MinPass: 1, MaxId: math.MaxInt64, PageSize: 10})
	if len(r.Tasks) != 3 || r.Tasks[0].Pass != 2 || r.Tasks[0].Id != 7 {
		t.Fatal("delete leaked acrosspass", r)
	}
	rival := memoryOwner(t, objects, path, "p")
	defer closeMemoryOwner(t, rival)
	if _, e = s.Execute(ctx, q); status.Code(e) != codes.Unavailable || !o.Quarantined() {
		t.Fatal("fenced fair replay", e)
	}
}
