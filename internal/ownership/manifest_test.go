package ownership

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/0x63616c/xenon/internal/directory"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type manifestS3 struct {
	mu     sync.Mutex
	data   []byte
	lose   bool
	cancel context.CancelFunc
	puts   int
}

func (m *manifestS3) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.data == nil {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey"}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(bytes.Clone(m.data)))}, nil
}
func (m *manifestS3) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if aws.ToString(in.IfNoneMatch) != "*" {
		return nil, errors.New("unconditional write")
	}
	if m.data != nil {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed"}
	}
	m.puts++
	m.data, _ = io.ReadAll(in.Body)
	if m.cancel != nil {
		m.cancel()
	}
	if m.lose {
		return nil, errors.New("response lost")
	}
	return &s3.PutObjectOutput{}, nil
}
func manifestFixture(t *testing.T) (*TopologyStore, *manifestS3, ClusterManifest) {
	t.Helper()
	m := &manifestS3{}
	s, e := NewTopologyStore(m, "bucket", "metadata")
	if e != nil {
		t.Fatal(e)
	}
	return s, m, ClusterManifest{Format: 1, Cluster: "xenon", HistoryShards: 4, LayoutVersion: 1, WireVersion: 1}
}

func TestManifestBootstrapAndConflict(t *testing.T) {
	s, m, want := manifestFixture(t)
	if e := EnsureCluster(context.Background(), s, want, false); !errors.Is(e, directory.ErrMissing) {
		t.Fatal(e)
	}
	if e := EnsureCluster(context.Background(), s, want, true); e != nil {
		t.Fatal(e)
	}
	if e := EnsureCluster(context.Background(), s, want, false); e != nil {
		t.Fatal(e)
	}
	for _, change := range []func(*ClusterManifest){func(m *ClusterManifest) { m.Cluster = "other" }, func(m *ClusterManifest) { m.HistoryShards = 8 }} {
		other := want
		change(&other)
		if e := EnsureCluster(context.Background(), s, other, true); !errors.Is(e, directory.ErrConflict) {
			t.Fatal(e)
		}
	}
	if m.puts != 1 {
		t.Fatal(m.puts)
	}
}
func TestManifestConcurrentBootstrap(t *testing.T) {
	s, m, want := manifestFixture(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := EnsureCluster(context.Background(), s, want, true); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if m.puts != 1 {
		t.Fatal(m.puts)
	}
}
func TestManifestLostResponseAndTimeout(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		s, m, want := manifestFixture(t)
		m.lose = true
		ctx, cancel := context.WithCancel(context.Background())
		if cancelled {
			m.cancel = cancel
		}
		e := EnsureCluster(ctx, s, want, true)
		cancel()
		if cancelled {
			if !errors.Is(e, directory.ErrUnknown) {
				t.Fatal(e)
			}
		} else if e != nil {
			t.Fatal(e)
		}
	}
}
func TestManifestRejectsCorruption(t *testing.T) {
	for _, raw := range []string{`{}`, `{"format":2}`, `{"format":1,"cluster":"xenon","history_shards":4,"layout_version":1,"wire_version":1,"future":true}`, `{"format":1,"cluster":"xenon","history_shards":4,"layout_version":1,"wire_version":1} {}`, string(bytes.Repeat([]byte(" "), 4097))} {
		s, m, want := manifestFixture(t)
		m.data = []byte(raw)
		if e := EnsureCluster(context.Background(), s, want, true); !errors.Is(e, directory.ErrInvalid) {
			t.Fatal(e)
		}
		if m.puts != 0 {
			t.Fatal("rewrote corrupt manifest")
		}
	}
}

func TestManifestCompetingIdentities(t *testing.T) {
	s, m, want := manifestFixture(t)
	results := make(chan error, 2)
	for _, name := range []string{"one", "two"} {
		candidate := want
		candidate.Cluster = name
		go func() { results <- EnsureCluster(context.Background(), s, candidate, true) }()
	}
	a, b := <-results, <-results
	if !((a == nil && errors.Is(b, directory.ErrConflict)) || (b == nil && errors.Is(a, directory.ErrConflict))) {
		t.Fatal(a, b)
	}
	if m.puts != 1 {
		t.Fatal(m.puts)
	}
}
