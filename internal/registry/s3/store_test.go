package s3

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/registry"
	"github.com/0x63616c/xenon/internal/registry/contracttest"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
)

func testClient(endpoint string, transport http.RoundTripper) *sdk.Client {
	return sdk.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("xenon-local", "xenon-local-test-only", ""), HTTPClient: &http.Client{Transport: transport}, RetryMaxAttempts: 4}, func(o *sdk.Options) { o.BaseEndpoint = aws.String(endpoint); o.UsePathStyle = true })
}
func TestSingleMutationAttempt(t *testing.T) {
	for _, status := range []int{409, 412, 500, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var puts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				if r.Method == "GET" {
					w.WriteHeader(404)
					io.WriteString(w, "<Error><Code>NoSuchKey</Code></Error>")
					return
				}
				puts.Add(1)
				if r.Header.Get("If-None-Match") != "*" || r.Header.Get("If-Match") != "" {
					t.Error("missing conditional header")
				}
				w.WriteHeader(status)
				code := "InternalError"
				if status == 412 {
					code = "PreconditionFailed"
				}
				if status == 409 {
					code = "ConditionalRequestConflict"
				}
				io.WriteString(w, "<Error><Code>"+code+"</Code></Error>")
			}))
			defer server.Close()
			s, _ := New(testClient(server.URL, http.DefaultTransport), "bucket", "registry")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := s.Create(ctx, "control", contracttest.Write(t, "control", "", 1, "body"))
			var unknown *registry.UnknownOutcome
			var conflict *registry.Conflict
			if status == 412 {
				if !errors.As(err, &conflict) {
					t.Fatal(err)
				}
			} else if !errors.As(err, &unknown) {
				t.Fatal(err)
			}
			if puts.Load() != 1 {
				t.Fatalf("hidden retries: %d", puts.Load())
			}
		})
	}
}
func TestBoundedStrictRead(t *testing.T) {
	for _, body := range []string{"{}", strings.Repeat("x", MaxRecordBytes+1)} {
		t.Run(string(rune(len(body)%26+'a')), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"etag"`)
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				io.WriteString(w, body)
			}))
			defer server.Close()
			s, _ := New(testClient(server.URL, http.DefaultTransport), "bucket", "registry")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var corrupt *registry.Corrupt
			if _, err := s.Read(ctx, "control"); !errors.As(err, &corrupt) {
				t.Fatal(err)
			}
		})
	}
}
func TestConfigAndWriteBounds(t *testing.T) {
	client := testClient("http://unused.invalid", http.DefaultTransport)
	for _, prefix := range []string{"", "/escape", "a/../b", "a/", strings.Repeat("x", 1024)} {
		if _, err := New(client, "bucket", prefix); err == nil {
			t.Fatal(prefix)
		}
	}
	s, _ := New(client, "bucket", "registry")
	var invalid *registry.Invalid
	if _, err := s.Create(context.Background(), "control", contracttest.Write(t, "control", "", 1, strings.Repeat("x", MaxRecordBytes))); !errors.As(err, &invalid) {
		t.Fatal(err)
	}
	if _, err := s.Read(context.Background(), registry.Key(strings.Repeat("x", 1020))); !errors.As(err, &invalid) {
		t.Fatal(err)
	}
}

func TestReadErrorClassification(t *testing.T) {
	for _, code := range []string{"NoSuchKey", "NoSuchBucket", "AccessDenied"} {
		t.Run(code, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(404)
				io.WriteString(w, "<Error><Code>"+code+"</Code></Error>")
			}))
			defer server.Close()
			s, _ := New(testClient(server.URL, http.DefaultTransport), "bucket", "registry")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := s.Read(ctx, "control")
			var missing *registry.NotFound
			var unavailable *registry.Unavailable
			if code == "NoSuchKey" {
				if !errors.As(err, &missing) {
					t.Fatal(err)
				}
			} else if !errors.As(err, &unavailable) {
				t.Fatal("non-key 404 misclassified", err)
			}
		})
	}
}
