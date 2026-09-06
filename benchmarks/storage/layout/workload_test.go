package main

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestWorkerFailureCancelsPeersAndPreservesPrimary(t *testing.T) {
	primary := errors.New("conditional state mismatch")
	var ready sync.WaitGroup
	ready.Add(4)
	var emitted error
	rows, err := runWorkers(context.Background(), 4, func(ctx context.Context, shard int) ([]observation, error) {
		ready.Done()
		ready.Wait()
		if shard == 0 {
			return nil, primary
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}, func(err error) { emitted = err })
	if !errors.Is(err, primary) || !errors.Is(emitted, primary) || len(rows) != 0 {
		t.Fatalf("lost first cause: %v, emitted %v", err, emitted)
	}
}
