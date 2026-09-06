package s3meter

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func serve(t *testing.T, p *Proxy, r *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	p.ServeHTTP(w, r)
	return w
}
func TestMeterControls(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 8193)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "signed.example:123" || r.URL.RequestURI() != "/bucket/a%2Fb?x=2&x=1" || r.Header.Get("Authorization") != "test-signature" {
			t.Error("signed request changed")
		}
		switch r.Method {
		case "PUT":
			b, _ := io.ReadAll(r.Body)
			if !bytes.Equal(b, payload) {
				t.Error("body changed")
			}
			w.WriteHeader(200)
		case "GET":
			_, _ = w.Write(payload)
		default:
			w.WriteHeader(403)
			_, _ = w.Write([]byte("denied"))
		}
	}))
	defer upstream.Close()
	p, e := New(upstream.URL)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	for _, m := range []string{"PUT", "GET", "DELETE"} {
		var b io.Reader
		if m == "PUT" {
			b = bytes.NewReader(payload)
		}
		r := httptest.NewRequest(m, "http://signed.example:123/bucket/a%2Fb?x=2&x=1", b)
		r.Header.Set("Authorization", "test-signature")
		serve(t, p, r)
	}
	got := p.Snapshot()
	if got.Attempts != 3 || got.Finished != 3 || got.Completed != 3 || got.RequestBodyBytes != 8193 || got.ResponseBodyBytes != 8199 || got.Status[200] != 2 || got.Status[403] != 1 || got.Inflight != 0 {
		t.Fatalf("bad counters: %+v", got)
	}
	for _, u := range []string{"https://127.0.0.1:1", "http://example.com:1", "http://0.0.0.0:1", "http://127.0.0.1:1/path", "http://user@127.0.0.1:1"} {
		if x, e := New(u); e == nil {
			x.Close()
			t.Fatal("unsafe target admitted", u)
		}
	}
}

type brokenWriter struct{ header http.Header }

func (w brokenWriter) Header() http.Header         { return w.header }
func (w brokenWriter) WriteHeader(int)             {}
func (w brokenWriter) Write(b []byte) (int, error) { return 2, errors.New("injected partial write") }
func TestMeterPartialAndCancel(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/truncated" {
			w.Header().Set("Content-Length", "10")
			_, _ = w.Write([]byte("abc"))
			return
		}
		_, _ = w.Write([]byte("abcdefgh"))
	}))
	defer upstream.Close()
	p, e := New(upstream.URL)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	// ReverseProxy aborts partial responses; the accounting finalizer still runs.
	func() {
		defer func() {
			if x := recover(); x != http.ErrAbortHandler {
				t.Errorf("unexpected abort %v", x)
			}
		}()
		r := httptest.NewRequest("GET", "http://local/truncated", nil)
		r = r.WithContext(context.WithValue(r.Context(), http.ServerContextKey, &http.Server{}))
		p.ServeHTTP(httptest.NewRecorder(), r)
	}()
	func() {
		defer func() {
			if x := recover(); x != http.ErrAbortHandler {
				t.Errorf("unexpected write abort %v", x)
			}
		}()
		r := httptest.NewRequest("GET", "http://local/write", nil)
		r = r.WithContext(context.WithValue(r.Context(), http.ServerContextKey, &http.Server{}))
		p.ServeHTTP(brokenWriter{http.Header{}}, r)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	serve(t, p, httptest.NewRequest("GET", "http://local/cancel", nil).WithContext(ctx))
	got := p.Snapshot()
	if got.Attempts != 3 || got.Finished != 3 || got.Completed != 0 || got.Aborted != 2 || got.Canceled != 1 || got.TransportErrors != 1 || got.ResponseReadErrors != 1 || got.ResponseWriteErrors != 1 || got.ResponseBodyBytes != 5 {
		t.Fatalf("partial accounting: %+v", got)
	}
}
func TestMeterCancellationDrains(t *testing.T) {
	entered := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() }))
	defer upstream.Close()
	p, e := New(upstream.URL)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		serve(t, p, httptest.NewRequest("GET", "http://local/wait", nil).WithContext(ctx))
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream not entered")
	}
	if p.Snapshot().Inflight != 1 {
		t.Fatal("missing in-flight attempt")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not drain")
	}
	got := p.Snapshot()
	if got.Inflight != 0 || got.Canceled != 1 || got.Completed != 0 || got.TransportErrors != 1 {
		t.Fatalf("cancel accounting %+v", got)
	}
}

type failedBody struct{}

func (failedBody) Read(b []byte) (int, error) { return copy(b, []byte("abc")), io.ErrUnexpectedEOF }
func (failedBody) Close() error               { return nil }
func TestMeterRequestFailure(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(io.Discard, r.Body) }))
	defer upstream.Close()
	p, e := New(upstream.URL)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	r := httptest.NewRequest("PUT", "http://local/partial", nil)
	r.Body = failedBody{}
	r.ContentLength = 10
	serve(t, p, r)
	got := p.Snapshot()
	if got.Attempts != 1 || got.Completed != 0 || got.Finished != 1 || got.RequestBodyBytes != 3 || got.RequestReadErrors != 1 || got.TransportErrors != 1 {
		t.Fatalf("request failure accounting %+v", got)
	}
}
