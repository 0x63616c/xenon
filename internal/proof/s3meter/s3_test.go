//go:build integration_s3

package s3meter

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestMeterSignedS3(t *testing.T) {
	var f struct {
		Schema                int
		Endpoint, Bucket, Key string
		PayloadBytes          int `json:"payload_bytes"`
		DeadlineSeconds       int `json:"deadline_seconds"`
	}
	raw, e := os.ReadFile("../../../proof/s3-meter/case.json")
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw, &f); e != nil || f.Schema != 1 || f.PayloadBytes != 4097 || f.DeadlineSeconds != 30 {
		t.Fatal("invalid fixture", e)
	}
	p, e := New(f.Endpoint)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	server := httptest.NewServer(p)
	defer server.Close()
	client := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("xenon-local", "xenon-local-test-only", ""), RetryMaxAttempts: 1, RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired, ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired}, func(o *s3.Options) { o.BaseEndpoint = aws.String(server.URL); o.UsePathStyle = true })
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(f.DeadlineSeconds)*time.Second)
	defer cancel()
	if _, e = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(f.Bucket)}); e != nil {
		t.Fatal(e)
	}
	snapshot := func() Report {
		deadline := time.Now().Add(5 * time.Second)
		for {
			r := p.Snapshot()
			if r.Inflight == 0 {
				return r
			}
			if time.Now().After(deadline) {
				t.Fatal("meter attempts failed to drain")
			}
			time.Sleep(time.Millisecond)
		}
	}
	before := snapshot()
	payload := bytes.Repeat([]byte("x"), f.PayloadBytes)
	if _, e = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(f.Bucket), Key: aws.String(f.Key), Body: bytes.NewReader(payload)}); e != nil {
		t.Fatal(e)
	}
	response, e := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(f.Bucket), Key: aws.String(f.Key)})
	if e != nil {
		t.Fatal(e)
	}
	got, e := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if e != nil || !bytes.Equal(got, payload) {
		t.Fatal("signed GET changed payload", e)
	}
	after := snapshot()
	if after.Attempts-before.Attempts != 2 || after.Completed-before.Completed != 2 || after.RequestBodyBytes-before.RequestBodyBytes != uint64(f.PayloadBytes) || after.ResponseBodyBytes-before.ResponseBodyBytes != uint64(f.PayloadBytes) {
		t.Fatalf("known body accounting before=%+v after=%+v", before, after)
	}
	if _, e = client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(f.Bucket), Key: aws.String("missing")}); e == nil {
		t.Fatal("missing object succeeded")
	}
	final := snapshot()
	if final.Attempts != 4 || final.Status[200] != 3 || final.Status[404] != 1 || final.TransportErrors != 0 || final.Inflight != 0 || final.ResponseBodyBytes <= uint64(f.PayloadBytes) {
		t.Fatalf("signed operation accounting %+v", final)
	}
	report, _ := json.Marshal(final)
	t.Log(string(report))
}
