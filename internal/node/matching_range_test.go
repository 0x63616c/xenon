package node

import (
	"bytes"
	"context"
	"encoding/binary"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"math"
	"testing"
)

func TestGoOwnerMatchingRangeSeek(t *testing.T) {
	o := owner(t, engine(t, objects(t), cfg(t).Prefix+"-matching-seek", false))
	defer closeOwner(t, o)
	s := &MatchingServer{Owner: o}
	ctx := context.Background()
	execute := func(id string, c *wire.MatchingCommand) *wire.MatchingResult {
		t.Helper()
		c.NamespaceId = bytes.Repeat([]byte{1}, 16)
		c.Queue = "seek"
		r, e := s.Execute(ctx, matchingReq(id, c))
		if e != nil || r.Error != wire.MatchingResult_NONE {
			t.Fatal(r, e)
		}
		return r
	}
	execute("queue", &wire.MatchingCommand{Kind: wire.MatchingCommand_CREATE_QUEUE, RangeId: 1})
	tasks := []*wire.MatchingTask{}
	for _, id := range []int64{math.MinInt64, -1, 0, math.MaxInt64 - 1} {
		tasks = append(tasks, &wire.MatchingTask{Id: id, Data: []byte{7}})
	}
	execute("tasks", &wire.MatchingCommand{Kind: wire.MatchingCommand_CREATE_TASKS, RangeId: 1, Tasks: tasks})
	first := execute("first", &wire.MatchingCommand{Kind: wire.MatchingCommand_GET_TASKS, MinId: -1, MaxId: 1, PageSize: 1})
	if len(first.Tasks) != 1 || first.Tasks[0].Id != -1 || len(first.Token) != 8 {
		t.Fatal(first)
	}
	second := execute("second", &wire.MatchingCommand{Kind: wire.MatchingCommand_GET_TASKS, MinId: -1, MaxId: 1, PageSize: 1, Token: first.Token})
	if len(second.Tasks) != 1 || second.Tasks[0].Id != 0 || len(second.Token) != 0 {
		t.Fatal(second)
	}
	cursor := make([]byte, 8)
	binary.BigEndian.PutUint64(cursor, uint64(math.MaxInt64)^(1<<63))
	empty := execute("beyond", &wire.MatchingCommand{Kind: wire.MatchingCommand_GET_TASKS, MinId: 0, MaxId: math.MaxInt64, PageSize: 1, Token: cursor})
	if len(empty.Tasks) != 0 || len(empty.Token) != 0 {
		t.Fatal(empty)
	}
	last := execute("last", &wire.MatchingCommand{Kind: wire.MatchingCommand_GET_TASKS, MinId: math.MaxInt64 - 1, MaxId: math.MaxInt64, PageSize: 1})
	if len(last.Tasks) != 1 || last.Tasks[0].Id != math.MaxInt64-1 {
		t.Fatal(last)
	}
}
