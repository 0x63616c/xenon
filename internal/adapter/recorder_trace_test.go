package adapter

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/proof/recorder"
	"github.com/0x63616c/xenon/internal/rpctrace"
	"google.golang.org/grpc"
)

func TestRecorderAdapterTimingAndLoss(t *testing.T) {
	for _, mode := range []string{"delayed-lost-ack", "recorder-death"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "journal")
			journal, err := recorder.New(path, 100)
			if err != nil {
				t.Fatal(err)
			}
			if err = journal.Accept(recorder.Event{Kind: "open", Phase: "proof", Status: "steady"}); err != nil {
				t.Fatal(err)
			}
			var dropped atomic.Bool
			var endpoint *httptest.Server
			endpoint = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var e recorder.Event
				if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
					t.Error(err)
					return
				}
				if err := journal.Accept(e); err != nil {
					http.Error(w, "rejected", 409)
					return
				}
				if mode == "recorder-death" && e.Kind == "register" && e.Measurement == "rpc_invocation" {
					w.Header().Set("Connection", "close")
					_ = endpoint.Listener.Close()
					w.WriteHeader(204)
					return
				}
				if e.Kind == "register" {
					time.Sleep(20 * time.Millisecond)
				}
				if mode == "delayed-lost-ack" && e.Kind == "register" && !dropped.Swap(true) {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
					return
				}
				w.WriteHeader(204)
			}))
			defer endpoint.Close()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			backend := &delayedTraceShard{}
			wire.RegisterShardPersistenceServer(server, backend)
			go server.Serve(listener)
			defer server.Stop()
			store, err := NewShardStore(listener.Addr().String(), "trace-partition", "test")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			observer, err := rpctrace.NewObserver(endpoint.URL, "producer", "proof")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			started := time.Now()
			_, err = store.invoke(rpctrace.WithObserver(ctx, observer), &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: 1})
			if mode == "recorder-death" {
				if err == nil || backend.calls != 0 || observer.Failure() == nil || time.Since(started) > 3*time.Second {
					t.Fatal("observer loss admitted work or failed open", err, backend.calls)
				}
				// No footer: the real open registration cannot be reconciled as successful.
				if _, err = recorder.Validate(path); err == nil {
					t.Fatal("dead observer evidence passed")
				}
				_ = journal.Close()
				return
			}
			if err != nil || backend.calls != 3 || observer.Failure() != nil {
				t.Fatal(err, backend.calls, observer.Failure())
			}
			if err = journal.Accept(recorder.Event{Kind: "close", Phase: "proof"}); err != nil {
				t.Fatal(err)
			}
			if err = journal.Close(); err != nil {
				t.Fatal(err)
			}
			summary, err := recorder.Validate(path)
			if err != nil || summary.Registered != 4 || summary.Completed != 4 {
				t.Fatal(summary, err)
			}
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			decoder := json.NewDecoder(file)
			registrations := map[string]recorder.Event{}
			var invocation int64
			var attempts int64
			for decoder.More() {
				var e recorder.Event
				if err = decoder.Decode(&e); err != nil {
					t.Fatal(err)
				}
				if e.Kind == "register" {
					registrations[e.ID] = e
				}
				if e.Kind == "terminal" {
					if e.HandshakeNS < int64(15*time.Millisecond) {
						t.Fatal("missing registration overhead", e)
					}
					if registrations[e.ID].Measurement == "rpc_invocation" {
						invocation = e.DurationNS
					} else {
						attempts += e.DurationNS
					}
				}
			}
			if invocation-attempts < int64(90*time.Millisecond) {
				t.Fatal("helper omitted ACK retry or backoff", invocation, attempts)
			}
		})
	}
}
