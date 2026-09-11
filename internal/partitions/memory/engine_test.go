package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/0x63616c/xenon/internal/partitions"
)

func request(path string, generation uint64) partitions.OpenRequest {
	return partitions.OpenRequest{
		Path: path, Partition: "prt_0000000000000000000000", AssignmentRevision: generation,
		Reservation: "trn_0000000000000000000000", Incarnation: "inc_0000000000000000000000", Generation: generation,
	}
}

func open(t *testing.T, engine *Engine, path string, generation uint64) partitions.Writer {
	t.Helper()
	w, err := engine.Open(context.Background(), request(path, generation))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func commit(t *testing.T, w partitions.Writer, values map[string]string) partitions.CommitReceipt {
	t.Helper()
	tx, err := w.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range values {
		if err := tx.Put([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	r, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AwaitDurable(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCommitAbortReopenAndReceiptOwnership(t *testing.T) {
	engine := New()
	w := open(t, engine, "partitions/a", 1)
	receipt := commit(t, w, map[string]string{"state": "one", "outcome": "saved"})

	tx, err := w.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put([]byte("state"), []byte("aborted")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Delete([]byte("outcome")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	reopened := open(t, engine, "partitions/a", 2)
	result, err := reopened.ReadDurable(context.Background(), partitions.ReadRequest{Keys: [][]byte{[]byte("state"), []byte("outcome")}})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Entries[0].Value) != "one" || string(result.Entries[1].Value) != "saved" {
		t.Fatalf("reopen lost atomic state: %+v", result)
	}
	if err := reopened.AwaitDurable(context.Background(), receipt); !errors.Is(err, partitions.ErrInvalid) {
		t.Fatalf("foreign receipt accepted: %v", err)
	}
}

func TestOpenFencesOldWriterWithoutLosingDurableState(t *testing.T) {
	engine := New()
	old := open(t, engine, "partitions/a", 1)
	commit(t, old, map[string]string{"acknowledged": "yes"})
	stale, err := old.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stale.Put([]byte("stale"), []byte("no")); err != nil {
		t.Fatal(err)
	}

	current := open(t, engine, "partitions/a", 2)
	if _, err := stale.Commit(context.Background()); !errors.Is(err, partitions.ErrFenced) {
		t.Fatalf("stale commit not fenced: %v", err)
	}
	if _, err := old.Begin(context.Background()); !errors.Is(err, partitions.ErrFenced) {
		t.Fatalf("old writer remained usable: %v", err)
	}
	result, err := current.ReadDurable(context.Background(), partitions.ReadRequest{Keys: [][]byte{[]byte("acknowledged"), []byte("stale")}})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Entries[0].Value) != "yes" || result.Entries[1].Value != nil {
		t.Fatalf("fence changed durable state: %+v", result)
	}
}

func TestCommitConflictLeavesDurableStateUnchanged(t *testing.T) {
	engine := New()
	w := open(t, engine, "partitions/a", 1).(*writer)
	commit(t, w, map[string]string{"state": "original"})
	tx, err := w.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put([]byte("state"), []byte("stale")); err != nil {
		t.Fatal(err)
	}
	// Model an independently committed database revision after this snapshot.
	w.db.mu.Lock()
	w.db.version++
	w.db.mu.Unlock()
	if _, err := tx.Commit(context.Background()); !errors.Is(err, partitions.ErrConflict) {
		t.Fatalf("snapshot conflict not reported: %v", err)
	}
	result, err := w.ReadDurable(context.Background(), partitions.ReadRequest{Keys: [][]byte{[]byte("state")}})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Entries[0].Value) != "original" {
		t.Fatalf("conflicting commit changed state: %+v", result)
	}
}

func TestOperationAdmissionAndRelease(t *testing.T) {
	w := open(t, New(), "partitions/a", 1)
	op, err := w.BeginOperation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Begin(context.Background()); !errors.Is(err, partitions.ErrBusy) {
		t.Fatalf("writer admitted competing operation: %v", err)
	}
	tx, err := op.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := op.Begin(context.Background()); !errors.Is(err, partitions.ErrBusy) {
		t.Fatalf("operation admitted competing transaction: %v", err)
	}
	if err := tx.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := op.Begin(context.Background()); err != nil {
		t.Fatalf("operation did not admit sequential transaction: %v", err)
	}
	op.Release()
	if _, err := op.Begin(context.Background()); !errors.Is(err, partitions.ErrOperationDone) {
		t.Fatalf("released operation admitted work: %v", err)
	}
	if _, err := w.Begin(context.Background()); err != nil {
		t.Fatalf("writer admission not released: %v", err)
	}
}

func TestTransactionViewAndScanPagination(t *testing.T) {
	w := open(t, New(), "partitions/a", 1)
	commit(t, w, map[string]string{"a": "1", "b": "2", "c": "3"})
	tx, err := w.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put([]byte("d"), []byte("4")); err != nil {
		t.Fatal(err)
	}
	result, err := tx.Scan(context.Background(), partitions.ScanRequest{Start: []byte("a"), End: []byte("d"), EndInclusive: true, Reverse: true, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !result.More || len(result.Entries) != 2 || string(result.Entries[0].Key) != "d" || string(result.Entries[1].Key) != "c" {
		t.Fatalf("unexpected transaction scan: %+v", result)
	}
	if err := tx.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidOpenAndCanceledContext(t *testing.T) {
	engine := New()
	bad := request("../escape", 1)
	if _, err := engine.Open(context.Background(), bad); !errors.Is(err, partitions.ErrInvalid) {
		t.Fatalf("invalid path accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := engine.Open(ctx, request("valid", 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled open accepted: %v", err)
	}
}
