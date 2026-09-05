package rpctrace

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type blockedWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	bytes.Buffer
}

func (w *blockedWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started); <-w.release })
	return w.Buffer.Write(p)
}
func TestTraceOverflowAndDrain(t *testing.T) {
	w := &blockedWriter{started: make(chan struct{}), release: make(chan struct{})}
	s := NewSink(w, 1)
	s.emit(Event{Kind: "first"})
	<-w.started
	s.emit(Event{Kind: "second"})
	s.emit(Event{Kind: "overflow"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	close(w.release)
	if s.Close(ctx) == nil {
		t.Fatal("overflow passed")
	}
	if !bytes.Contains(w.Bytes(), []byte(`"kind":"trace_error"`)) || !bytes.Contains(w.Bytes(), []byte(`"status":"false"`)) {
		t.Fatal(w.String())
	}
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, errors.New("sensitive disk error") }
func TestTraceWriteFailure(t *testing.T) {
	s := NewSink(errorWriter{}, 1)
	s.emit(Event{Kind: "test"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if s.Close(ctx) == nil {
		t.Fatal("write failure passed")
	}
}

type closeErrorWriter struct{ bytes.Buffer }

func (*closeErrorWriter) Close() error { return errors.New("close failure") }
func TestTraceFinalizationFailures(t *testing.T) {
	for _, w := range []interface{ Write([]byte) (int, error) }{errorWriter{}, &closeErrorWriter{}} {
		s := NewSink(w, 1)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if s.Close(ctx) == nil {
			t.Fatal("zero-event finalization failure passed")
		}
		cancel()
	}
	var buffer bytes.Buffer
	s := NewSink(&buffer, 1)
	_, finish := begin(context.Background(), "active", s)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if s.Close(ctx) == nil {
		t.Fatal("active invocation closed successfully")
	}
	finish(nil)
}
