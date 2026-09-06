package ownership

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/0x63616c/xenon/internal/directory"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// joinS3 models conditional single-object publication, including response loss.
// Existing directory tests use real S3; this fake exercises only Join decisions.
type joinS3 struct {
	data      []byte
	version   int
	lose      bool
	beforePut func()
}

func (s *joinS3) GetObject(ctx context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.data == nil {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey"}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(s.data)), ETag: aws.String(fmt.Sprint(s.version))}, nil
}
func (s *joinS3) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.beforePut != nil {
		f := s.beforePut
		s.beforePut = nil
		f()
	}
	if (in.IfNoneMatch != nil && s.data != nil) || (in.IfMatch != nil && *in.IfMatch != fmt.Sprint(s.version)) {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed"}
	}
	s.data, _ = io.ReadAll(in.Body)
	s.version++
	if s.lose {
		s.lose = false
		return nil, io.ErrUnexpectedEOF
	}
	return &s3.PutObjectOutput{ETag: aws.String(fmt.Sprint(s.version))}, nil
}
func joinID(node string, n int) directory.Identity {
	return directory.Identity{Node: node, Address: node + ":7235", Incarnation: fmt.Sprintf("00000000-0000-4000-8000-%012d", n)}
}
func TestJoinBootstrapBalanceAndResponseLoss(t *testing.T) {
	ctx := context.Background()
	fake := &joinS3{}
	store, _ := NewTopologyStore(fake, "bucket", "metadata")
	if err := Join(ctx, store, joinID("a", 1), "data", false); !errors.Is(err, directory.ErrMissing) {
		t.Fatal(err)
	}
	fake.lose = true
	if err := Join(ctx, store, joinID("a", 1), "data", true); err != nil {
		t.Fatal(err)
	}
	before, _ := store.Read(ctx)
	if err := Join(ctx, store, joinID("b", 2), "data", false); err != nil {
		t.Fatal(err)
	}
	after, _ := store.Read(ctx)
	counts := map[string]int{}
	for id, a := range after.data.Partitions {
		counts[a.Node]++
		if a.DataPrefix != before.data.Partitions[id].DataPrefix {
			t.Fatal("moved durable identity")
		}
	}
	if counts["a"] != 5 || counts["b"] != 5 {
		t.Fatal(counts)
	}
	v := fake.version
	if err := Join(ctx, store, joinID("b", 2), "data", false); err != nil || fake.version != v {
		t.Fatal("idempotent join wrote", err)
	}
	if err := Join(ctx, store, joinID("c", 3), "other-data", false); !errors.Is(err, directory.ErrInvalid) {
		t.Fatal(err)
	}
}
func TestJoinAbortsCompetingIncarnation(t *testing.T) {
	ctx := context.Background()
	fake := &joinS3{}
	store, _ := NewTopologyStore(fake, "bucket", "metadata")
	if err := Join(ctx, store, joinID("a", 1), "data", true); err != nil {
		t.Fatal(err)
	}
	fake.beforePut = func() {
		if err := Join(ctx, store, joinID("a", 3), "data", false); err != nil {
			t.Fatal(err)
		}
	}
	if err := Join(ctx, store, joinID("a", 2), "data", false); !errors.Is(err, directory.ErrConflict) {
		t.Fatal("stale join stole membership", err)
	}
	snap, _ := store.Read(ctx)
	if snap.data.Members["a"].Incarnation != joinID("a", 3).Incarnation {
		t.Fatal("new incarnation lost")
	}
}

func TestJoinRetriesIndependentMemberCAS(t *testing.T) {
	ctx := context.Background()
	fake := &joinS3{}
	store, _ := NewTopologyStore(fake, "bucket", "metadata")
	if err := Join(ctx, store, joinID("a", 1), "data", true); err != nil {
		t.Fatal(err)
	}
	fake.beforePut = func() {
		if err := Join(ctx, store, joinID("c", 3), "data", false); err != nil {
			t.Fatal(err)
		}
	}
	if err := Join(ctx, store, joinID("b", 2), "data", false); err != nil {
		t.Fatal(err)
	}
	snap, _ := store.Read(ctx)
	counts := map[string]int{}
	for _, a := range snap.data.Partitions {
		counts[a.Node]++
	}
	if len(snap.data.Members) != 3 || counts["a"] != 4 || counts["b"] != 3 || counts["c"] != 3 {
		t.Fatal(snap.data)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := Join(cancelled, store, joinID("d", 4), "data", false); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
