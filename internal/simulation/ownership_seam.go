package simulation

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/0x63616c/xenon/internal/directory"
	"github.com/0x63616c/xenon/internal/ownership"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// seamTopologyS3 exposes the same conditional Put boundary as object storage.
// Its callback deterministically overlaps two production ownership.Join calls.
type seamTopologyS3 struct {
	data      []byte
	version   int
	beforePut func()
}

func (s *seamTopologyS3) GetObject(ctx context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.data == nil {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey"}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(s.data)), ETag: aws.String(fmt.Sprint(s.version))}, nil
}

func (s *seamTopologyS3) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.beforePut != nil {
		callback := s.beforePut
		s.beforePut = nil
		callback()
	}
	if in.IfNoneMatch != nil && s.data != nil || in.IfMatch != nil && *in.IfMatch != fmt.Sprint(s.version) {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed"}
	}
	s.data, _ = io.ReadAll(in.Body)
	s.version++
	return &s3.PutObjectOutput{ETag: aws.String(fmt.Sprint(s.version))}, nil
}

func seamJoinIdentity(node string, incarnation int) directory.Identity {
	return directory.Identity{Node: node, Address: node + ":7235", Incarnation: fmt.Sprintf("00000000-0000-4000-8000-%012d", incarnation)}
}

func runOverlappingJoinSeam() error {
	ctx := context.Background()
	backend := new(seamTopologyS3)
	store, err := ownership.NewTopologyStore(backend, "bucket", "metadata")
	if err != nil {
		return err
	}
	next := 0
	if err := store.SetTransitionSource(func() string {
		next++
		return fmt.Sprintf("00000000-0000-4000-8001-%012d", next)
	}); err != nil {
		return err
	}
	if err := ownership.Join(ctx, store, seamJoinIdentity("a", 1), "data", true); err != nil {
		return err
	}
	var nestedErr error
	backend.beforePut = func() {
		nestedErr = ownership.Join(ctx, store, seamJoinIdentity("c", 3), "data", false)
	}
	if err := ownership.Join(ctx, store, seamJoinIdentity("b", 2), "data", false); err != nil {
		return err
	}
	if nestedErr != nil {
		return nestedErr
	}
	snapshot, err := store.Read(ctx)
	if err != nil {
		return err
	}
	record := snapshot.Record()
	counts := map[string]int{}
	for _, assignment := range record.Partitions {
		counts[assignment.Node]++
	}
	if len(record.Members) != 3 || counts["a"] != 4 || counts["b"] != 3 || counts["c"] != 3 {
		return fmt.Errorf("overlapping joins did not converge: members=%d counts=%v", len(record.Members), counts)
	}
	return nil
}
