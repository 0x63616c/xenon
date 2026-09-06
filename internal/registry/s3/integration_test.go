//go:build integration_s3

package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/registry"
	"github.com/0x63616c/xenon/internal/registry/contracttest"
	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
)

type faultTransport struct {
	base   http.RoundTripper
	mu     sync.Mutex
	after  func(*http.Response) error
	before bool
	puts   atomic.Int32
}

func (f *faultTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != "PUT" {
		return f.base.RoundTrip(r)
	}
	f.puts.Add(1)
	f.mu.Lock()
	after, before := f.after, f.before
	f.after = nil
	f.before = false
	f.mu.Unlock()
	if before {
		return nil, io.ErrUnexpectedEOF
	}
	response, err := f.base.RoundTrip(r)
	if err != nil || after == nil {
		return response, err
	}
	if response.StatusCode != 200 {
		return response, nil
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	return nil, after(response)
}
func TestS3Registry(t *testing.T) {
	var cfg struct {
		Schema           int
		Endpoint, Bucket string
		DeadlineSeconds  int `json:"deadline_seconds"`
		Schedule         []string
	}
	raw, err := os.ReadFile("../../../test/scenarios/registry/s3.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &cfg); err != nil || cfg.Schema != 1 || cfg.DeadlineSeconds != 90 {
		t.Fatal("fixture", err)
	}
	want := []string{"shared_contract", "lost_commit", "lost_commit_later_record", "lost_before_commit", "cancel_after_dispatch", "corrupt_read", "oversized_read", "reopen"}
	if len(cfg.Schedule) != len(want) {
		t.Fatal("schedule")
	}
	for i := range want {
		if cfg.Schedule[i] != want[i] {
			t.Fatal("schedule")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.DeadlineSeconds)*time.Second)
	defer cancel()
	rawClient := testClient(cfg.Endpoint, http.DefaultTransport)
	if _, err = rawClient.CreateBucket(ctx, &sdk.CreateBucketInput{Bucket: aws.String(cfg.Bucket)}); err != nil {
		t.Fatal(err)
	}
	transport := &faultTransport{base: http.DefaultTransport}
	client := testClient(cfg.Endpoint, transport)
	s, err := New(client, cfg.Bucket, "registry-v1")
	if err != nil {
		t.Fatal(err)
	}
	t.Run("shared_contract", func(t *testing.T) { contracttest.Run(t, ctx, s) })
	for _, mode := range cfg.Schedule[1:5] {
		t.Run(mode, func(t *testing.T) {
			key := registry.Key("fault/" + mode)
			w := contracttest.Write(t, key, "", 80, "body")
			callCtx, stop := context.WithCancel(ctx)
			defer stop()
			transport.mu.Lock()
			if mode == "lost_before_commit" {
				transport.before = true
			} else {
				transport.after = func(response *http.Response) error {
					if mode == "cancel_after_dispatch" {
						stop()
					}
					if mode == "lost_commit_later_record" {
						next := contracttest.Write(t, key, registry.Version(response.Header.Get("ETag")), 81, "later")
						data, e := registry.Encode(key, registry.Version(response.Header.Get("ETag")), next)
						if e != nil {
							return e
						}
						_, e = rawClient.PutObject(ctx, &sdk.PutObjectInput{Bucket: aws.String(cfg.Bucket), Key: aws.String("registry-v1/" + string(key)), IfMatch: aws.String(response.Header.Get("ETag")), Body: bytes.NewReader(data)})
						if e != nil {
							t.Error(e)
							return e
						}
					}
					return io.ErrUnexpectedEOF
				}
			}
			transport.mu.Unlock()
			before := transport.puts.Load()
			_, e := s.Create(callCtx, key, w)
			if mode == "lost_commit" {
				if e != nil {
					t.Fatal(e)
				}
			} else {
				var unknown *registry.UnknownOutcome
				if !errors.As(e, &unknown) {
					t.Fatalf("ambiguity erased: %v", e)
				}
			}
			if transport.puts.Load() != before+1 {
				t.Fatal("SDK retried mutation")
			}
			observed, e := s.Read(ctx, key)
			if mode == "lost_before_commit" {
				var missing *registry.NotFound
				if !errors.As(e, &missing) {
					t.Fatal(e)
				}
			} else {
				if e != nil {
					t.Fatal(e)
				}
				envelope, e := registry.Decode(key, observed)
				if e != nil {
					t.Fatal(e)
				}
				expected := "body"
				if mode == "lost_commit_later_record" {
					expected = "later"
				}
				if string(envelope.Body) != expected {
					t.Fatal("wrong durable state")
				}
			}
		})
	}
	for _, mode := range cfg.Schedule[5:7] {
		t.Run(mode, func(t *testing.T) {
			body := []byte("{}")
			if mode == "oversized_read" {
				body = bytes.Repeat([]byte("x"), MaxRecordBytes+1)
			}
			key := registry.Key(mode)
			if _, err = rawClient.PutObject(ctx, &sdk.PutObjectInput{Bucket: aws.String(cfg.Bucket), Key: aws.String("registry-v1/" + string(key)), Body: bytes.NewReader(body)}); err != nil {
				t.Fatal(err)
			}
			var corrupt *registry.Corrupt
			if _, err = s.Read(ctx, key); !errors.As(err, &corrupt) {
				t.Fatal(err)
			}
		})
	}
	t.Run("reopen", func(t *testing.T) {
		fresh, _ := New(testClient(cfg.Endpoint, http.DefaultTransport), cfg.Bucket, "registry-v1")
		if r, e := fresh.Read(ctx, "contract/replace"); e != nil || r.Version == "" {
			t.Fatal("fresh client recovery", e)
		}
	})
}
