package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/routing"
	"google.golang.org/grpc/codes"
)

func TestBoundedMetrics(t *testing.T) {
	m := New()
	m.Events <- routing.Event{Kind: "forward", Attempt: 1, Code: codes.Unavailable}
	m.Events <- routing.Event{Kind: "workflow-secret", Attempt: -999, Code: codes.Code(999)}
	close(m.Events)
	m.Run(context.Background())
	w := httptest.NewRecorder()
	m.Handler(nil, nil).ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	body := w.Body.String()
	for _, want := range []string{
		`xenon_routing_observed_events_total{kind="forward",attempt="1",code="Unavailable"} 1`,
		`xenon_routing_observed_events_total{kind="other",attempt="other",code="other"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s", want)
		}
	}
	if strings.Contains(body, "secret") || strings.Count(body, "\n") != 218 {
		t.Fatal("unbounded labels or series")
	}
}

func TestDiagnostics(t *testing.T) {
	m := New()
	ready := false
	handler := m.Handler(func() bool { return ready }, func() any { return map[string]string{"xenon": "test"} })
	for _, tc := range []struct {
		path   string
		ready  bool
		status int
		body   string
	}{
		{"/readyz", false, 503, "not ready\n"},
		{"/readyz", true, 200, "ready\n"},
		{"/version", true, 200, "{\"xenon\":\"test\"}\n"},
		{"/missing", true, 404, "404 page not found\n"},
	} {
		ready = tc.ready
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		if w.Code != tc.status || w.Body.String() != tc.body {
			t.Fatalf("%s: %d %q", tc.path, w.Code, w.Body.String())
		}
	}
	w := httptest.NewRecorder()
	m.Handler(nil, func() any { return make(chan int) }).ServeHTTP(w, httptest.NewRequest("GET", "/version", nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatal("invalid version must fail")
	}
}

func TestConcurrentCollectionAndCancellation(t *testing.T) {
	m := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	handler := m.Handler(nil, nil)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			select {
			case m.Events <- routing.Event{Kind: "local"}:
			default:
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/metrics", nil))
		}
	}()
	wg.Wait()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("collector did not stop")
	}
}
