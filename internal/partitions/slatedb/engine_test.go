package slatedb

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"os"
	"strings"
	"testing"
	"time"

	p "github.com/0x63616c/xenon/internal/partitions"
	native "slatedb.io/slatedb-go/uniffi"
)

// Native Rust/object-store timers are intentionally real in this qualification
// suite. Logical driver clocks and deterministic schedules are separate tests.
func setup(t *testing.T) (*native.ObjectStore, func(string, bool) *writer) {
	t.Helper()
	backend := os.Getenv("XENON_ENGINE_STORE")
	if backend == "" {
		backend = "memory://"
	}
	store, err := native.ObjectStoreResolve(backend)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Destroy)
	return store, func(path string, manual bool) *writer {
		b := native.NewDbBuilder(t.Name()+"/"+path, store)
		defer b.Destroy()
		if manual {
			settings := native.SettingsDefault()
			defer settings.Destroy()
			if err := settings.Set("flush_interval", "null"); err != nil {
				t.Fatal(err)
			}
			if err := b.WithSettings(settings); err != nil {
				t.Fatal(err)
			}
		}
		db, err := b.Build()
		if err != nil {
			t.Fatal(err)
		}
		w := newWriter(db)
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := w.Close(ctx); err != nil && !errors.Is(err, p.ErrFenced) {
				t.Error(err)
			}
		})
		return w
	}
}
func begin(t *testing.T, w p.Writer) p.Transaction {
	t.Helper()
	tx, err := w.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return tx
}
func stage(t *testing.T, tx p.Transaction, key, value string) {
	t.Helper()
	if err := tx.Put([]byte(key), []byte(value)); err != nil {
		t.Fatal(err)
	}
}
func finish(t *testing.T, w p.Writer, tx p.Transaction) p.CommitReceipt {
	t.Helper()
	r, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = w.AwaitDurable(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	return r
}
func read(t *testing.T, w p.Writer, key string) string {
	t.Helper()
	r, err := w.ReadDurable(context.Background(), p.ReadRequest{Keys: [][]byte{[]byte(key)}})
	if err != nil {
		t.Fatal(err)
	}
	return string(r.Entries[0].Value)
}

func TestNativeEngineAtomicRecovery(t *testing.T) {
	_, open := setup(t)
	w := open("db", false)
	tx := begin(t, w)
	stage(t, tx, "state", "one")
	stage(t, tx, "outcome", "digest/result")
	r := finish(t, w, tx)
	tx = begin(t, w)
	value, err := tx.Get(context.Background(), []byte("state"))
	if err != nil || string(value) != "one" {
		t.Fatal("condition read", err)
	}
	scan, err := tx.Scan(context.Background(), p.ScanRequest{Limit: 1})
	if err != nil || len(scan.Entries) != 1 || !scan.More || string(scan.Entries[0].Key) != "outcome" {
		t.Fatal("bounded scan", scan, err)
	}
	stage(t, tx, "state", "aborted")
	if err = tx.Delete([]byte("outcome")); err != nil {
		t.Fatal(err)
	}
	if err = tx.Abort(); err != nil {
		t.Fatal(err)
	}
	if read(t, w, "state") != "one" || read(t, w, "outcome") != "digest/result" {
		t.Fatal("abort leaked")
	}
	other := open("other", false)
	if err = other.AwaitDurable(context.Background(), r); !errors.Is(err, p.ErrInvalid) {
		t.Fatal("foreign receipt accepted", err)
	}
	if err = w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered := open("db", false)
	if read(t, recovered, "state") != "one" || read(t, recovered, "outcome") != "digest/result" {
		t.Fatal("atomic recovery failed")
	}
	tx = begin(t, recovered)
	if err = tx.Delete([]byte("state")); err != nil {
		t.Fatal(err)
	}
	next := finish(t, recovered, tx)
	if next.MutationID() == 0 || read(t, recovered, "state") != "" {
		t.Fatal("delete not durable")
	}
}

func TestNativeEngineFence(t *testing.T) {
	_, open := setup(t)
	old := open("db", false)
	tx := begin(t, old)
	stage(t, tx, "state", "acknowledged")
	finish(t, old, tx)
	replacement := open("db", false)
	_, err := old.ReadDurable(context.Background(), p.ReadRequest{Keys: [][]byte{[]byte("state")}})
	if !errors.Is(err, p.ErrFenced) {
		t.Fatal("read/replay barrier missed native fence", err)
	}
	if _, err = old.Begin(context.Background()); !errors.Is(err, p.ErrRetired) {
		t.Fatal("fence not terminal", err)
	}
	if read(t, replacement, "state") != "acknowledged" {
		t.Fatal("pre-fence durable data lost")
	}
	tx = begin(t, replacement)
	stage(t, tx, "new", "owner")
	finish(t, replacement, tx)
	// Delayed stale open can displace a current writer; registry alone cannot
	// prevent it. Once it completes, a fresh current open must recover progress.
	stale := open("db", false)
	_, err = replacement.ReadDurable(context.Background(), p.ReadRequest{})
	if !errors.Is(err, p.ErrFenced) {
		t.Fatal("late open did not fence", err)
	}
	if err = stale.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	current := open("db", false)
	if read(t, current, "new") != "owner" {
		t.Fatal("late-open recovery lost acknowledged value")
	}
}

func TestNativeEnginePendingLifecycle(t *testing.T) {
	_, open := setup(t)
	w := open("db", true)
	tx := begin(t, w)
	stage(t, tx, "state", "pending")
	stage(t, tx, "outcome", "pending-result")
	r, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Abort(); !errors.Is(err, p.ErrTransactionDone) {
		t.Fatal("dispatched commit aborted", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err = w.Begin(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("admitted before durability", err)
	}
	if w.retired.Load() {
		t.Fatal("canceled queued caller retired owner")
	}
	raw, err := w.db.GetWithOptions([]byte("state"), native.ReadOptions{DurabilityFilter: native.DurabilityLevelRemote})
	if err != nil || raw != nil {
		t.Fatal("pending write prematurely durable", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel2()
	err = w.AwaitDurable(ctx2, r)
	var unknown *p.UnknownOutcome
	if !errors.As(err, &unknown) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("missing unknown outcome", err)
	}
	if !w.retired.Load() || len(w.gate) != 1 {
		t.Fatal("lost retained gate")
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer closeCancel()
	if err = w.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("closed active native handle", err)
	}
	if w.closed {
		t.Fatal("destroyed active database")
	}
	if err = w.db.FlushWithOptions(native.FlushOptions{FlushType: native.FlushTypeWal}); err != nil {
		t.Fatal(err)
	}
	drain, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err = w.AwaitDurable(drain, r); err != nil {
		t.Fatal("late native completion unavailable", err)
	}
	if _, err = w.Begin(context.Background()); !errors.Is(err, p.ErrRetired) {
		t.Fatal("timeout retirement reversed")
	}
	if err = w.Close(drain); err != nil {
		t.Fatal(err)
	}
	recovered := open("db", false)
	if read(t, recovered, "state") != "pending" || read(t, recovered, "outcome") != "pending-result" {
		t.Fatal("caller cancellation rolled back commit")
	}
}

func TestNativeEngineReadBarrier(t *testing.T) {
	_, open := setup(t)
	w := open("db", true)
	// A read-only operation must issue a nonempty WAL write and await its handle.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := w.ReadDurable(ctx, p.ReadRequest{Keys: [][]byte{[]byte("missing")}})
	var unknown *p.UnknownOutcome
	if !errors.As(err, &unknown) {
		t.Fatal("empty read bypassed durability", err)
	}
	if len(w.gate) != 1 {
		t.Fatal("read barrier dropped native ownership")
	}
	if err = w.db.FlushWithOptions(native.FlushOptions{FlushType: native.FlushTypeWal}); err != nil {
		t.Fatal(err)
	}
	drain, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err = w.Close(drain); err != nil {
		t.Fatal(err)
	}
}

func TestNativeEngineCanceledBeforeDispatch(t *testing.T) {
	_, open := setup(t)
	w := open("db", false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := w.Begin(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	tx := begin(t, w)
	stage(t, tx, "state", "not-committed")
	if r, err := tx.Commit(ctx); r != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled commit dispatched", r, err)
	}
	if err := tx.Abort(); err != nil {
		t.Fatal(err)
	}
	if read(t, w, "state") != "" {
		t.Fatal("canceled commit leaked")
	}
}

func TestMain(m *testing.M) {
	if store := os.Getenv("XENON_ENGINE_STORE"); strings.HasPrefix(store, "s3://") {
		client := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("xenon-local", "xenon-local-test-only", ""), RetryMaxAttempts: 1}, func(o *s3.Options) { o.BaseEndpoint = aws.String(os.Getenv("AWS_ENDPOINT")); o.UsePathStyle = true })
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(strings.TrimPrefix(store, "s3://"))})
		cancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

func TestNativeEngineOpen(t *testing.T) {
	backend := os.Getenv("XENON_ENGINE_STORE")
	if backend == "" {
		backend = "memory://"
	}
	e := &Engine{backend: backend}
	if strings.HasPrefix(backend, "s3://") {
		var err error
		e, err = New(backend)
		if err != nil {
			t.Fatal(err)
		}
	}
	req := p.OpenRequest{Path: "public-open/stable", Partition: "prt_0000000000000000000000", AssignmentRevision: 1, Reservation: "trn_0000000000000000000000", Incarnation: "inc_0000000000000000000000", Generation: 1}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if w, err := e.Open(ctx, req); w != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("pre-dispatch open", err)
	}
	bad := req
	bad.Generation = 0
	if _, err := e.Open(context.Background(), bad); !errors.Is(err, p.ErrInvalid) {
		t.Fatal("missing reservation metadata admitted", err)
	}
	w, err := e.Open(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	tx := begin(t, w)
	stage(t, tx, "state", "public engine")
	finish(t, w, tx)
	if read(t, w, "state") != "public engine" {
		t.Fatal("public Open failed")
	}
	if err = w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNativeEngineCanceledReadRetains(t *testing.T) {
	_, open := setup(t)
	w := open("db", false)
	tx := begin(t, w).(*transaction)
	entered, release := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := invoke(ctx, tx, func() (any, error) { close(entered); <-release; _, err := tx.tx.Get([]byte("key")); return nil, err })
		done <- err
	}()
	<-entered
	cancel()
	var unknown *p.UnknownOutcome
	if err := <-done; !errors.As(err, &unknown) {
		t.Fatal(err)
	}
	if err := tx.Abort(); !errors.Is(err, p.ErrBusy) {
		t.Fatal("aborted pending native read", err)
	}
	if len(w.gate) != 1 {
		t.Fatal("dropped native gate")
	}
	close(release)
	drain, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := w.Close(drain); err != nil {
		t.Fatal(err)
	}
}
