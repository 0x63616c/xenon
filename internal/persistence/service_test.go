package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"maps"
	"testing"
	"testing/synctest"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Controlled transaction/durability seam. Only AwaitDurable publishes staged
// data into recovered storage; request semantics are the real Service above.
type testWriter struct {
	durable   map[string][]byte
	staged    map[string][]byte
	allow     <-chan struct{}
	submitted chan struct{}
	failure   error
	commits   int
}
type testReceipt struct{}

func (testReceipt) MutationID() uint64 { return 1 }

type testTx struct {
	writer *testWriter
	data   map[string][]byte
}

func (w *testWriter) Begin(context.Context) (partitions.Transaction, error) {
	return &testTx{w, maps.Clone(w.durable)}, nil
}
func (w *testWriter) AwaitDurable(ctx context.Context, _ partitions.CommitReceipt) error {
	if w.allow != nil {
		select {
		case <-w.allow:
		case <-ctx.Done():
			return &partitions.UnknownOutcome{Cause: ctx.Err()}
		}
	}
	w.durable = maps.Clone(w.staged)
	return nil
}
func (*testWriter) ReadDurable(context.Context, partitions.ReadRequest) (partitions.ReadResult, error) {
	return partitions.ReadResult{}, partitions.ErrInvalid
}
func (*testWriter) Close(context.Context) error { return nil }
func (tx *testTx) Get(_ context.Context, k []byte) ([]byte, error) {
	return bytes.Clone(tx.data[string(k)]), nil
}
func (*testTx) Scan(context.Context, partitions.ScanRequest) (partitions.ReadResult, error) {
	return partitions.ReadResult{}, partitions.ErrInvalid
}
func (tx *testTx) Put(k, v []byte) error { tx.data[string(k)] = bytes.Clone(v); return nil }
func (tx *testTx) Delete(k []byte) error { delete(tx.data, string(k)); return nil }
func (tx *testTx) Commit(context.Context) (partitions.CommitReceipt, error) {
	w := tx.writer
	w.commits++
	w.staged = maps.Clone(tx.data)
	if w.submitted != nil {
		close(w.submitted)
		w.submitted = nil
	}
	return testReceipt{}, w.failure
}
func (*testTx) Abort() error { return nil }

func shardRequest() *wire.ShardRequest {
	c := &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: 7, RangeId: 11, Data: []byte("original")}
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	hash := sha256.Sum256(raw)
	return &wire.ShardRequest{ProtocolVersion: 1, Partition: "prt_0000000000000000000001", OperationId: "op_0000000000000000000001", Command: c, CommandSha256: hash[:]}
}

func TestShardReplyWaitsForDurabilityAndReplaySurvivesRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		allow := make(chan struct{})
		submitted := make(chan struct{})
		w := &testWriter{durable: map[string][]byte{}, allow: allow, submitted: submitted}
		s, err := NewService(w, "prt_0000000000000000000001", 10, func(context.Context) error { return nil }, func(error) {})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { _, err := s.Execute(context.Background(), shardRequest()); done <- err }()
		<-submitted
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("reply before durability: %v", err)
		default:
		}
		if len(w.durable) != 0 {
			t.Fatal("volatile effects recovered")
		}
		close(allow)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		count := binary.BigEndian.Uint64(w.durable["v1/outcome_count"])
		if count != 1 {
			t.Fatal(count)
		}
		recovered := &testWriter{durable: maps.Clone(w.durable)}
		s, err = NewService(recovered, s.partition, 10, func(context.Context) error { return nil }, func(error) {})
		if err != nil {
			t.Fatal(err)
		}
		result, err := s.Execute(context.Background(), shardRequest())
		if err != nil || result.RangeId != 11 {
			t.Fatalf("replay: %v %v", result, err)
		}
		if binary.BigEndian.Uint64(recovered.durable["v1/outcome_count"]) != 1 || recovered.commits != 1 {
			t.Fatal("replay duplicated mutation or skipped durable barrier")
		}
		r := shardRequest()
		r.Command.RangeId = 12
		raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(r.Command)
		digest := sha256.Sum256(raw)
		r.CommandSha256 = digest[:]
		if _, err := s.Execute(context.Background(), r); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("digest reuse: %v", err)
		}
		if recovered.commits != 1 {
			t.Fatal("changed digest committed")
		}
	})
}

func TestShardChecksAuthorityAndPreservesNativeFailure(t *testing.T) {
	w := &testWriter{durable: map[string][]byte{}}
	denied := errors.New("obsolete reservation")
	s, err := NewService(w, "prt_0000000000000000000001", 10, func(context.Context) error { return denied }, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Execute(context.Background(), shardRequest()); !errors.Is(err, denied) || w.commits != 0 {
		t.Fatalf("authority bypass: %v", err)
	}
	s.authority = func(context.Context) error { return nil }
	w.failure = &partitions.UnknownOutcome{Cause: partitions.ErrFenced}
	var observed error
	s.failure = func(err error) { observed = err }
	_, err = s.Execute(context.Background(), shardRequest())
	var unknown *partitions.UnknownOutcome
	if !errors.As(err, &unknown) || !errors.Is(observed, partitions.ErrFenced) {
		t.Fatalf("flattened lifecycle error: returned %v observed %v", err, observed)
	}
	if len(w.durable) != 0 {
		t.Fatal("uncertain failure acknowledged")
	}
}
