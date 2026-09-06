package slatedb

import (
	"context"
	"errors"
	"testing"
	"time"

	p "github.com/0x63616c/xenon/internal/partitions"
	native "slatedb.io/slatedb-go/uniffi"
)

func operationBegin(t *testing.T, op p.Operation) p.Transaction {
	t.Helper()
	tx, err := op.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

// Queued external reads and writes must respect the same gate, including the
// gap after abort and the gap after a successful durable child transaction.
func assertAdmissionHeld(t *testing.T, w *writer) {
	t.Helper()
	for _, read := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		var err error
		if read {
			_, err = w.ReadDurable(ctx, p.ReadRequest{})
		} else {
			_, err = w.Begin(ctx)
		}
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("admission escaped (read=%v): %v", read, err)
		}
	}
	if w.retired.Load() {
		t.Fatal("queued cancellation retired writer")
	}
}

func TestNativeOperationSpansAbortAndDurability(t *testing.T) {
	_, open := setup(t)
	w := open("db", false)
	op, err := w.BeginOperation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer op.Release()
	tx := operationBegin(t, op)
	stage(t, tx, "discarded", "value")
	if _, err = op.Begin(context.Background()); !errors.Is(err, p.ErrBusy) {
		t.Fatal("overlapping child", err)
	}
	if err = tx.Abort(); err != nil {
		t.Fatal(err)
	}
	assertAdmissionHeld(t, w)
	tx = operationBegin(t, op)
	stage(t, tx, "child", "durable")
	finish(t, w, tx)
	assertAdmissionHeld(t, w)
	tx = operationBegin(t, op)
	value, err := tx.Get(context.Background(), []byte("child"))
	if err != nil || string(value) != "durable" {
		t.Fatal("child visibility", string(value), err)
	}
	op.Release() // abort idle root, retaining the independently durable child
	op.Release()
	if _, err = op.Begin(context.Background()); !errors.Is(err, p.ErrOperationDone) {
		t.Fatal(err)
	}
	if read(t, w, "discarded") != "" || read(t, w, "child") != "durable" {
		t.Fatal("operation changed transaction atomicity")
	}
}

func TestNativeOperationReleaseRetainsBusyCall(t *testing.T) {
	_, open := setup(t)
	w := open("db", false)
	op, err := w.BeginOperation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tx := operationBegin(t, op).(*transaction)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := invoke(context.Background(), tx, func() (any, error) { close(entered); <-release; _, err := tx.tx.Get([]byte("key")); return nil, err })
		done <- err
	}()
	<-entered
	op.Release()
	op.Release()
	if _, err = op.Begin(context.Background()); !errors.Is(err, p.ErrOperationDone) {
		t.Fatal(err)
	}
	if err = tx.Abort(); !errors.Is(err, p.ErrBusy) {
		t.Fatal("destroyed busy transaction", err)
	}
	assertAdmissionHeld(t, w)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	if err = w.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("close bypassed operation", err)
	}
	cancel()
	if w.closed {
		t.Fatal("destroyed native handle")
	}
	close(release)
	if err = <-done; !errors.Is(err, p.ErrOperationDone) {
		t.Fatal(err)
	}
	if err = w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNativeOperationReleaseRetainsReceipt(t *testing.T) {
	_, open := setup(t)
	w := open("db", true)
	op, err := w.BeginOperation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tx := operationBegin(t, op)
	stage(t, tx, "child", "pending")
	receipt, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = op.Begin(context.Background()); !errors.Is(err, p.ErrBusy) {
		t.Fatal("child before durability", err)
	}
	op.Release()
	op.Release()
	assertAdmissionHeld(t, w)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var unknown *p.UnknownOutcome
	if err = w.AwaitDurable(ctx, receipt); !errors.As(err, &unknown) {
		t.Fatal(err)
	}
	if !w.retired.Load() || len(w.gate) != 1 {
		t.Fatal("unknown released native ownership")
	}
	if _, err = op.Begin(context.Background()); !errors.Is(err, p.ErrOperationDone) {
		t.Fatal(err)
	}
	closeCtx, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	if err = w.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("closed pending receipt", err)
	}
	stop()
	if err = w.db.FlushWithOptions(native.FlushOptions{FlushType: native.FlushTypeWal}); err != nil {
		t.Fatal(err)
	}
	if err = w.AwaitDurable(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered := open("db", false)
	if read(t, recovered, "child") != "pending" {
		t.Fatal("release rolled back submitted child")
	}
}

func TestNativeOperationBeginRacesRelease(t *testing.T) {
	_, open := setup(t)
	w := open("db", false)
	for range 100 {
		op, err := w.BeginOperation(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ready := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			<-ready
			tx, err := op.Begin(context.Background())
			if err == nil {
				_ = tx.Abort()
			}
			done <- err
		}()
		close(ready)
		op.Release()
		err = <-done
		if err != nil && !errors.Is(err, p.ErrOperationDone) && !errors.Is(err, p.ErrTransactionDone) {
			t.Fatal(err)
		}
		op.Release()
		// Acquiring the next scope proves no lost gate or uncounted native call.
		next, err := w.BeginOperation(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		next.Release()
	}
}

func TestNativeOperationUnknownRetiresUnreleasedScope(t *testing.T) {
	_, open := setup(t)
	w := open("db", true)
	op, err := w.BeginOperation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer op.Release()
	tx := operationBegin(t, op)
	receipt, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var unknown *p.UnknownOutcome
	if err = w.AwaitDurable(ctx, receipt); !errors.As(err, &unknown) {
		t.Fatal(err)
	}
	if _, err = op.Begin(context.Background()); !errors.Is(err, p.ErrRetired) {
		t.Fatal("unknown admitted child", err)
	}
	if err = w.db.FlushWithOptions(native.FlushOptions{FlushType: native.FlushTypeWal}); err != nil {
		t.Fatal(err)
	}
	if err = w.AwaitDurable(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	if _, err = op.Begin(context.Background()); !errors.Is(err, p.ErrRetired) {
		t.Fatal("late success reversed retirement", err)
	}
	if len(w.gate) != 1 {
		t.Fatal("native completion released explicit operation")
	}
	op.Release()
	if err = w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
