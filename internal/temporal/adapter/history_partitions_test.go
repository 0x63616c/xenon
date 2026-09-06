package adapter

import (
	"context"
	"errors"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"testing"
	"time"
)

type emptyHistoryPartitions struct {
	deadlines  []time.Time
	partitions []string
}

func (c *emptyHistoryPartitions) Execute(ctx context.Context, q *wire.HistoryRequest, _ ...grpc.CallOption) (*wire.HistoryResult, error) {
	d, _ := ctx.Deadline()
	c.deadlines = append(c.deadlines, d)
	c.partitions = append(c.partitions, q.Partition)
	if len(c.partitions) == 1 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
			return &wire.HistoryResult{}, nil
		}
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func TestHistoryPartitionDeadline(t *testing.T) {
	c := new(emptyHistoryPartitions)
	s := &HistoryStore{client: c, historyPartitions: []string{"first", "second", "third"}, invocationTimeout: 50 * time.Millisecond}
	start := time.Now()
	r, e := s.GetAllHistoryTreeBranches(context.Background(), &p.GetAllHistoryTreeBranchesRequest{PageSize: 1})
	if !errors.Is(e, context.DeadlineExceeded) || r != nil {
		t.Fatal(r, e)
	}
	if len(c.deadlines) != 2 || !c.deadlines[0].Equal(c.deadlines[1]) {
		t.Fatal("fanout renewed deadline", c.deadlines)
	}
	if time.Since(start) > time.Second {
		t.Fatal("fanout did not stop")
	}
}

func TestHistoryPartitionInvalidCursor(t *testing.T) {
	s := &HistoryStore{historyPartitions: []string{"first"}, invocationTimeout: time.Second}
	for _, token := range []string{"{}", "null", `{"Version":1}`, `{"Version":1,"Partition":0,"Local":null}`, `{"ListHash":null,"Partition":0,"Local":null}`} {
		if r, e := s.GetAllHistoryTreeBranches(context.Background(), &p.GetAllHistoryTreeBranchesRequest{PageSize: 1, NextPageToken: []byte(token)}); e == nil || r != nil {
			t.Fatal("incomplete token accepted", token, r, e)
		}
	}
}
