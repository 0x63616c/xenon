//go:build integration_s3

package s3meter

import (
	"context"
	"encoding/json"
	"github.com/0x63616c/xenon/internal/directory"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCASLossSignedS3(t *testing.T) {
	var f struct{ Endpoint, Bucket string }
	raw, err := os.ReadFile("../../../proof/s3-meter/case.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	p, err := New(f.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err = p.EnableCASLoss(filepath.Join(t.TempDir(), "cas.jsonl")); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(p)
	defer server.Close()
	// Exactly the production ownership.Environment defaults: no checksum/retry overrides.
	client := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("xenon-local", "xenon-local-test-only", "")}, func(o *s3.Options) { o.BaseEndpoint = aws.String(server.URL); o.UsePathStyle = true })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(f.Bucket)}); err != nil {
		t.Fatal(err)
	}
	d, err := directory.New(client, f.Bucket, "metadata/owners", "history-0", "data/history-0")
	if err != nil {
		t.Fatal(err)
	}
	initial, err := d.Reserve(ctx, nil, directory.Identity{Node: "a", Incarnation: "00000000-0000-0000-0000-000000000001", Address: "127.0.0.1:17351"})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := d.Ready(ctx, initial)
	if err != nil {
		t.Fatal(err)
	}
	identity := directory.Identity{Node: "c", Incarnation: "00000000-0000-0000-0000-000000000003", Address: "127.0.0.1:17353"}
	if err = p.ArmCASLoss(CASSelector{Path: "/" + f.Bucket + "/metadata/owners/686973746f72792d30.json", Partition: "history-0", Node: identity.Node, Incarnation: identity.Incarnation}); err != nil {
		t.Fatal(err)
	}
	moved, err := d.Reserve(ctx, &ready, identity)
	if err != nil {
		t.Fatalf("real lost response not reconciled: %v receipt=%+v", err, p.CASLossSnapshot())
	}
	if moved.Record().Node != "c" || !p.CASLossSnapshot().ReconciledGET {
		t.Fatalf("no exact GET reconciliation; receipt=%+v", p.CASLossSnapshot())
	}
	final, err := d.Ready(ctx, moved)
	if err != nil {
		t.Fatal(err)
	}
	read, err := d.Read(ctx)
	if err != nil || read.Record() != final.Record() {
		t.Fatal("READY did not survive readback", err)
	}
	receipt := p.CASLossSnapshot()
	if receipt.Error != "" || receipt.State != "successful_response_dropped" || !receipt.ReconciledGET || !receipt.ReadyGET || receipt.UpstreamStatus != 200 {
		t.Fatalf("incomplete actual CAS receipt %+v", receipt)
	}
	deadline := time.Now().Add(time.Second)
	for p.Snapshot().Inflight != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.Snapshot().Aborted != 1 || p.Snapshot().Inflight != 0 {
		t.Fatal("actual response abort/drain missing")
	}
	b, _ := json.Marshal(receipt)
	t.Log(string(b))
}
