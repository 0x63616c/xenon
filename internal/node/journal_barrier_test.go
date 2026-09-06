package node

import (
	"context"
	"fmt"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	native "slatedb.io/slatedb-go/uniffi"
	"testing"
	"time"
)

func TestJournalResultBarrier(t *testing.T) {
	t.Run("matching_fresh_replay_and_fence", func(t *testing.T) {
		store := objects(t)
		o := owner(t, engine(t, store, "matching-barrier", false))
		o.config.Authority = func(context.Context) error { return nil }
		s := &MatchingServer{Owner: o}
		f := matchingCase(t)
		id := uuid.MustParse(f.Namespace)
		q := matchingReq("create", &wire.MatchingCommand{Kind: wire.MatchingCommand_CREATE_QUEUE, NamespaceId: id[:], Queue: f.Queue, RangeId: 1})
		for i := 0; i < 2; i++ {
			if r, e := s.Execute(context.Background(), q); e != nil || r.Error != wire.MatchingResult_NONE {
				t.Fatal(r, e)
			}
		}
		// Inspect through test-only native access while no RPC is running. The fresh
		// outcome and replay nonce committed, but no redundant post-result key exists.
		if value, e := o.db.Get([]byte("v1/ownership/read-barrier")); e != nil || value != nil {
			t.Fatal("redundant barrier remained", e)
		}
		replacement := owner(t, engine(t, store, "matching-barrier", false))
		defer closeOwner(t, replacement)
		if _, e := s.Execute(context.Background(), q); status.Code(e) != codes.Unavailable || !o.Quarantined() {
			t.Fatal("fenced replay served", e)
		}
		closeOwner(t, o)
	})
	t.Run("arbitrary_callback_keeps_barrier", func(t *testing.T) {
		o := owner(t, engine(t, objects(t), "arbitrary-barrier", false))
		defer closeOwner(t, o)
		o.config.Authority = func(context.Context) error { return nil }
		_, e := o.Run(context.Background(), func(*native.Db) ([]byte, error) {
			_, e := o.journal("inside", make([]byte, 32), shardFamily, func(*native.DbTransaction) (*wire.StoredOutcome, error) {
				return &wire.StoredOutcome{Result: &wire.StoredOutcome_ShardResult{ShardResult: &wire.ShardResult{}}}, nil
			})
			return nil, e
		})
		if e != nil {
			t.Fatal(e)
		}
		if value, e := o.db.Get([]byte("v1/ownership/read-barrier")); e != nil || value == nil {
			t.Fatal("arbitrary callback lost trailing barrier", e)
		}
	})
	t.Run("captured_read_after_journal_is_fenced", func(t *testing.T) {
		store := objects(t)
		o := owner(t, engine(t, store, "captured-after-journal", false))
		o.config.Authority = func(context.Context) error { return nil }
		captured := make(chan struct{})
		resume := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			_, e := o.Run(context.Background(), func(db *native.Db) ([]byte, error) {
				_, e := o.journal("inside", make([]byte, 32), shardFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
					if e := put(tx, "captured", []byte("acknowledged")); e != nil {
						return nil, e
					}
					return &wire.StoredOutcome{Result: &wire.StoredOutcome_ShardResult{ShardResult: &wire.ShardResult{}}}, nil
				})
				if e != nil {
					return nil, e
				}
				value, e := db.Get([]byte("captured"))
				if e != nil {
					return nil, e
				}
				close(captured)
				<-resume
				return *value, nil
			})
			done <- e
		}()
		<-captured
		replacement := owner(t, engine(t, store, "captured-after-journal", false))
		defer closeOwner(t, replacement)
		close(resume)
		if e := <-done; status.Code(e) != codes.Unavailable || !o.Quarantined() {
			t.Fatal("arbitrary captured read escaped fence", e)
		}
		closeOwner(t, o)
	})
}

// Forty simultaneous queue lookups use the unchanged Temporal five-second
// deadline and default 100ms WAL flush. A redundant second barrier serializes
// roughly eight seconds of durable waits into that window.
func TestMatchingManagedBurst(t *testing.T) {
	o := owner(t, engine(t, objects(t), "matching-burst", false))
	defer closeOwner(t, o)
	o.config.Authority = func(context.Context) error { return nil }
	s := &MatchingServer{Owner: o}
	f := matchingCase(t)
	id := uuid.MustParse(f.Namespace)
	create := matchingReq("create", &wire.MatchingCommand{Kind: wire.MatchingCommand_CREATE_QUEUE, NamespaceId: id[:], Queue: f.Queue, RangeId: 1})
	if r, e := s.Execute(context.Background(), create); e != nil || r.Error != wire.MatchingResult_NONE {
		t.Fatal(r, e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	done := make(chan error, 40)
	for i := 0; i < 40; i++ {
		go func(i int) {
			<-start
			q := matchingReq(fmt.Sprintf("read-%d", i), &wire.MatchingCommand{Kind: wire.MatchingCommand_GET_QUEUE, NamespaceId: id[:], Queue: f.Queue})
			r, e := s.Execute(ctx, q)
			if e == nil && r.Error != wire.MatchingResult_NONE {
				e = fmt.Errorf("logical matching error %v", r.Error)
			}
			done <- e
		}(i)
	}
	close(start)
	for i := 0; i < 40; i++ {
		if e := <-done; e != nil {
			t.Error(e)
		}
	}
	if o.Quarantined() {
		t.Error("healthy managed burst quarantined owner")
	}
}

func TestExecutionResultBarrier(t *testing.T) {
	store := objects(t)
	o := owner(t, engine(t, store, "execution-barrier", false))
	o.config.Authority = func(context.Context) error { return nil }
	s := &ExecutionServer{Owner: o}
	q := executionRequest("missing-execution", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_GET, NamespaceId: "11111111-1111-4111-8111-111111111111", WorkflowId: "missing", RunId: "22222222-2222-4222-8222-222222222222"})
	for i := 0; i < 2; i++ {
		r, e := s.Execute(context.Background(), q)
		if e != nil || r.Error != wire.ExecutionResult_NOT_FOUND {
			t.Fatal(r, e)
		}
		if value, e := o.db.Get([]byte("v1/ownership/read-barrier")); e != nil || value != nil {
			t.Fatal("redundant execution barrier", e)
		}
	}
	replacement := owner(t, engine(t, store, "execution-barrier", false))
	defer closeOwner(t, replacement)
	if _, e := s.Execute(context.Background(), q); status.Code(e) != codes.Unavailable || !o.Quarantined() {
		t.Fatal("fenced execution replay served", e)
	}
	closeOwner(t, o)
}
