//go:build slatedb

package node

import (
	"context"
	"errors"
	native "slatedb.io/slatedb-go/uniffi"
	"sync/atomic"
	"testing"
	"time"
)

func TestCanceledAdmissionDoesNotRetireOwner(t *testing.T) {
	for attempt := 0; attempt < 100; attempt++ {
		var entered atomic.Bool
		config := DefaultConfig("matching")
		config.Authority = func(ctx context.Context) error { entered.Store(true); return ctx.Err() }
		// No native object is needed: cancellation must be rejected before authority
		// or the native callback is entered, even when the gate is simultaneously ready.
		o := &Owner{config: config, gate: make(chan struct{}, 1), admitted: make(chan struct{}, 64)}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := o.Run(ctx, func(*native.Db) ([]byte, error) { entered.Store(true); return nil, nil })
		deadline := time.Now().Add(time.Second)
		for o.ActiveNativeOperations() != 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if !errors.Is(err, context.Canceled) || entered.Load() || o.Quarantined() || o.ActiveNativeOperations() != 0 {
			t.Fatalf("canceled admission entered/retired owner: %v entered=%v retired=%v", err, entered.Load(), o.Quarantined())
		}
	}
}
