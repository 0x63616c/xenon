package rpctrace

import (
	"bytes"
	"context"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/0x63616c/xenon/internal/proof/recorder"
)

func TestObserverConcurrentRegistration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	r, err := recorder.New(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Accept(recorder.Event{Kind: "open", Phase: "steady", Status: "steady"}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(r)
	defer server.Close()
	o, err := NewObserver(server.URL, "producer", "steady")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, finish, err := BeginObserved(WithObserver(context.Background(), o), "shard")
			if err != nil {
				t.Error(err)
				return
			}
			if err = finish(nil); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err = o.Failure(); err != nil {
		t.Fatal(err)
	}
	if err = r.Accept(recorder.Event{Kind: "close", Phase: "steady"}); err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	summary, err := recorder.Validate(path)
	if err != nil || summary.Registered != 20 || summary.Completed != 20 {
		t.Fatal(summary, err)
	}
}
func TestObserverConfiguration(t *testing.T) {
	for _, endpoint := range []string{"https://127.0.0.1:1", "http://localhost:1", "http://127.0.0.1:1/path", "http://user@127.0.0.1:1"} {
		if _, err := NewObserver(endpoint, "producer", "steady"); err == nil {
			t.Fatal("invalid endpoint passed", endpoint)
		}
	}
	o, err := NewObserver("http://127.0.0.1:1", "producer", "steady")
	if err != nil {
		t.Fatal(err)
	}
	var buffer bytes.Buffer
	sink := NewSink(&buffer, 1)
	ctx := WithObserver(WithSink(context.Background(), sink), o)
	if _, _, err = BeginObserved(ctx, "shard"); err == nil {
		t.Fatal("conflicting observers passed")
	}
	if err = sink.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
