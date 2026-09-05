//go:build integration_s3

package directory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fixture struct {
	Schema                               int
	Backend, Endpoint, Bucket, Partition string
	DataPrefix                           string `json:"data_prefix"`
	Incarnation                          string
	DeadlineMS                           int `json:"deadline_ms"`
	Schedule                             []string
}
type faults struct {
	base http.RoundTripper
	mode atomic.Int32
}

func (f *faults) RoundTrip(r *http.Request) (*http.Response, error) {
	mode := f.mode.Load()
	if r.Method == "GET" && mode == 3 {
		<-r.Context().Done()
		return nil, r.Context().Err()
	}
	if r.Method == "PUT" && (mode == 1 || mode == 2) && f.mode.CompareAndSwap(mode, 0) {
		if mode == 2 {
			return nil, io.ErrUnexpectedEOF
		}
		result, e := f.base.RoundTrip(r)
		if e != nil {
			return result, e
		}
		io.Copy(io.Discard, result.Body)
		result.Body.Close()
		return nil, io.ErrUnexpectedEOF
	}
	return f.base.RoundTrip(r)
}
func TestS3Directory(t *testing.T) {
	raw, e := os.ReadFile("../../proof/directory/case.json")
	if e != nil {
		t.Fatal(e)
	}
	var f fixture
	if e = json.Unmarshal(raw, &f); e != nil || f.Schema != 1 || f.Backend != "s3-emulator" || strings.Join(f.Schedule, ",") != "create_only,competing_claims,stale_ready,lost_put_response,lost_put_before_write,same_owner_no_aba,oversized_read,deadline" {
		t.Fatal("invalid fixture", e)
	}
	transport := &faults{base: http.DefaultTransport}
	client := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("xenon-local", "xenon-local-test-only", ""), HTTPClient: &http.Client{Transport: transport}, RetryMaxAttempts: 1}, func(o *s3.Options) { o.BaseEndpoint = aws.String(f.Endpoint); o.UsePathStyle = true })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, e = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(f.Bucket)}); e != nil {
		t.Fatal(e)
	}
	for _, pair := range [][2]string{{"/same/", "same"}, {"data/meta", "/data/"}, {"data", "data/db"}, {"///", "data"}, {"meta//nested", "data"}, {"meta", "data/../meta"}, {strings.Repeat("m", 1024), "data"}, {"meta", string([]byte{255})}} {
		if _, e = New(client, f.Bucket, pair[0], f.Partition, pair[1]); !errors.Is(e, ErrInvalid) {
			t.Fatal("overlapping/ambiguous prefixes", pair, e)
		}
	}
	if _, e = New(client, f.Bucket, "/data-meta/", f.Partition, "/data/"); e != nil {
		t.Fatal("prefix sibling rejected", e)
	}
	d, e := New(client, f.Bucket, "directory", f.Partition, f.DataPrefix)
	if e != nil {
		t.Fatal(e)
	}
	identity := Identity{Node: "node-a", Incarnation: f.Incarnation, Address: "127.0.0.1:7235"}
	a, e := d.Reserve(ctx, nil, identity)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = d.Reserve(ctx, nil, identity); !errors.Is(e, ErrConflict) {
		t.Fatal("create-only overwrite", e)
	}
	snapshot, e := d.Read(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if snapshot.Record() != a.Record() {
		t.Fatal("record mismatch")
	}
	var wg sync.WaitGroup
	reservations := make(chan *Reservation, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r, e := d.Reserve(ctx, &snapshot, identity); reservations <- r; errs <- e }()
	}
	wg.Wait()
	close(reservations)
	close(errs)
	success, conflict := 0, 0
	for e := range errs {
		if e == nil {
			success++
		} else if errors.Is(e, ErrConflict) {
			conflict++
		} else {
			t.Fatal(e)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatal("competing CAS", success, conflict)
	}
	var b *Reservation
	for r := range reservations {
		if r != nil {
			b = r
		}
	}
	copyA := *a
	if _, e = d.Ready(ctx, a); !errors.Is(e, ErrConflict) {
		t.Fatal("stale ready", e)
	}
	if _, e = d.Ready(ctx, &copyA); !errors.Is(e, ErrConsumed) {
		t.Fatal("one-shot", e)
	}
	transport.mode.Store(1)
	ready, e := d.Ready(ctx, b)
	if e != nil || ready.Record().State != "ready" {
		t.Fatal("lost PUT response not reconciled", ready, e)
	}
	transport.mode.Store(2)
	if _, e = d.Reserve(ctx, &ready, identity); !errors.Is(e, ErrUnknown) {
		t.Fatal("uncommitted response must remain unknown", e)
	}
	unchanged, e := d.Read(ctx)
	if e != nil || unchanged.record != ready.record {
		t.Fatal("before-write fault changed record", e)
	}
	c, e := d.Reserve(ctx, &ready, identity)
	if e != nil {
		t.Fatal(e)
	}
	if c.Record().Generation != ready.Record().Generation+1 || c.Record().Transition == ready.Record().Transition {
		t.Fatal("ABA identity reused")
	}
	if _, e = d.Reserve(ctx, &ready, identity); !errors.Is(e, ErrConflict) {
		t.Fatal("old etag reused", e)
	}
	other, _ := New(client, f.Bucket, "other", f.Partition, f.DataPrefix)
	if _, e = other.Ready(ctx, c); !errors.Is(e, ErrInvalid) {
		t.Fatal("cross-directory ready", e)
	}
	if _, e = other.Reserve(ctx, &ready, identity); !errors.Is(e, ErrInvalid) {
		t.Fatal("cross-directory snapshot", e)
	}
	if _, e = d.Ready(ctx, c); e != nil {
		t.Fatal(e)
	}
	bad, _ := New(client, f.Bucket, "oversized", f.Partition, f.DataPrefix)
	if _, e = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(f.Bucket), Key: aws.String(bad.key), Body: bytes.NewReader(bytes.Repeat([]byte("x"), MaxRecordBytes+1))}); e != nil {
		t.Fatal(e)
	}
	if _, e = bad.Read(ctx); !errors.Is(e, ErrInvalid) {
		t.Fatal("oversized read", e)
	}

	invalid, _ := New(client, f.Bucket, "validation", f.Partition, f.DataPrefix)
	for _, state := range []Record{{}, {Format: 1, Partition: f.Partition, DataPrefix: f.DataPrefix, Generation: 1, Transition: "bad", Node: identity.Node, Incarnation: identity.Incarnation, Address: identity.Address, State: "opening"}} {
		body, _ := json.Marshal(state)
		if _, e = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(f.Bucket), Key: aws.String(invalid.key), Body: bytes.NewReader(body)}); e != nil {
			t.Fatal(e)
		}
		if _, e = invalid.Read(ctx); !errors.Is(e, ErrInvalid) {
			t.Fatal("invalid record admitted", e)
		}
	}
	terminal := ready.Record()
	terminal.Generation = math.MaxUint64
	body, _ := json.Marshal(terminal)
	if _, e = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(f.Bucket), Key: aws.String(invalid.key), Body: bytes.NewReader(body)}); e != nil {
		t.Fatal(e)
	}
	atLimit, e := invalid.Read(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = invalid.Reserve(ctx, &atLimit, identity); !errors.Is(e, ErrInvalid) {
		t.Fatal("generation overflow", e)
	}
	transport.mode.Store(3)
	short, stop := context.WithTimeout(ctx, time.Duration(f.DeadlineMS)*time.Millisecond)
	started := time.Now()
	_, e = d.Read(short)
	stop()
	if !errors.Is(e, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatal("unbounded read", e)
	}
	transport.mode.Store(0)
	wrong, _ := New(client, f.Bucket, "directory", f.Partition, "wrong-prefix")
	if _, e = wrong.Read(ctx); !errors.Is(e, ErrInvalid) {
		t.Fatal("prefix mismatch admitted", e)
	}
}
