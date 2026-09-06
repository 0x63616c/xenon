package directory

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// A saved format-1 record proves the unchanged legacy ingress still preserves
// UUIDs, human partition/node names, and its exact physical database reference.
const legacyIdentityRecord = `{"format":1,"partition":"history-0","data_prefix":"existing/data/history-0","generation":7,"transition":"00000000-0000-4000-8000-000000000001","node":"node-a","incarnation":"00000000-0000-4000-8000-000000000002","address":"127.0.0.1:7235","state":"ready"}`

type legacyReader struct{ t *testing.T }

func (f legacyReader) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if aws.ToString(in.Key) != "existing/directory/686973746f72792d30.json" {
		f.t.Fatalf("legacy key changed: %q", aws.ToString(in.Key))
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(legacyIdentityRecord)), ETag: aws.String("legacy-version")}, nil
}
func (f legacyReader) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.t.Fatal("read compatibility must not rewrite the record")
	return nil, nil
}
func TestLegacyIdentityReadCompatibility(t *testing.T) {
	d, err := New(legacyReader{t}, "bucket", "existing/directory", "history-0", "existing/data/history-0")
	if err != nil {
		t.Fatal(err)
	}
	got, err := d.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := Record{Format: 1, Partition: "history-0", DataPrefix: "existing/data/history-0", Generation: 7, Transition: "00000000-0000-4000-8000-000000000001", Node: "node-a", Incarnation: "00000000-0000-4000-8000-000000000002", Address: "127.0.0.1:7235", State: "ready"}
	if got.Record() != want || got.etag != "legacy-version" {
		t.Fatalf("legacy identity/reference changed: %+v", got.Record())
	}
}
