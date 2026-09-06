// Package observability collects passive, lossy routing diagnostics. It observes
// attempts, not unique operations or durable commits. A saturated Events buffer
// drops routing events before collection, so counters can undercount and must
// never be used for billing or correctness assertions. No drop count is available.
// Exporting happens outside routing decisions. This package does not install
// authentication: its caller must protect the diagnostics listener.
package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/0x63616c/xenon/internal/routing"
	"google.golang.org/grpc/codes"
)

var kinds = [...]string{"resolve", "local", "forward", "other"}
var attempts = [...]string{"0", "1", "other"}

// Metrics uses fixed-size counters; even unexpected input cannot create new labels.
// Construct with New and do not copy after use.
type Metrics struct {
	// Events is attached to Router.Events before serving. Keep it open until all
	// producers stop. The router sends nonblocking; do not replace this channel.
	Events chan routing.Event
	counts [4][3][18]atomic.Uint64
}

func New() *Metrics { return &Metrics{Events: make(chan routing.Event, 1024)} }

// Run consumes events until cancellation or channel closure. On cancellation,
// queued events may be discarded. The agent owns this goroutine's lifecycle.
func (m *Metrics) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case event, ok := <-m.Events:
			if !ok {
				return
			}
			kind := 3
			for i := 0; i < 3; i++ {
				if event.Kind == kinds[i] {
					kind = i
					break
				}
			}
			attempt := event.Attempt
			if attempt < 0 || attempt > 1 {
				attempt = 2
			}
			code := int(event.Code)
			if event.Code > codes.Unauthenticated {
				code = 17
			}
			m.counts[kind][attempt][code].Add(1)
		}
	}
}

// Handler exposes /metrics, /readyz and /version. Callbacks must be concurrency
// safe and fast. Nil readiness reports unavailable; nil version reports null.
func (m *Metrics) Handler(ready func() bool, version func() any) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		fmt.Fprintln(w, "# HELP xenon_routing_observed_events_total Observed routing attempts; lossy and unsuitable for billing or durability assertions.")
		fmt.Fprintln(w, "# TYPE xenon_routing_observed_events_total counter")
		for k, kind := range kinds {
			for a, attempt := range attempts {
				for c := 0; c < 18; c++ {
					code := "other"
					if c < 17 {
						code = codes.Code(c).String()
					}
					fmt.Fprintf(w, "xenon_routing_observed_events_total{kind=%q,attempt=%q,code=%q} %d\n", kind, attempt, code, m.counts[k][a][c].Load())
				}
			}
		}
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if ready == nil || !ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintln(w, "ready")
	})
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		var v any
		if version != nil {
			v = version()
		}
		data, err := json.Marshal(v)
		if err != nil {
			http.Error(w, "version unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(append(data, '\n'))
	})
	return mux
}
