package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSeedPartitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	groups := map[string][]int{"a": {0, 4}, "b": {1, 5}, "c": {2, 6}, "d": {3, 7}}
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	go func() {
		for range 4 {
			select {
			case <-entered:
			case <-ctx.Done():
				return
			}
		}
		close(release)
	}()
	var mu sync.Mutex
	seen := map[int]int{}
	counts, err := seedPartitions(ctx, groups, func(ctx context.Context, i int) error {
		if i < 4 {
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		mu.Lock()
		defer mu.Unlock()
		seen[i]++
		return nil
	})
	if err != nil || len(seen) != 8 {
		t.Fatal(counts, seen, err)
	}
	for _, n := range counts {
		if n != 2 {
			t.Fatal(counts)
		}
	}
	for _, n := range seen {
		if n != 1 {
			t.Fatal(seen)
		}
	}
}
func TestSeedPartitionsFailureJoinsPeers(t *testing.T) {
	failure := errors.New("write failed")
	counts, err := seedPartitions(context.Background(), map[string][]int{"a": {0, 1}, "b": {2, 3}}, func(_ context.Context, i int) error {
		if i == 1 {
			return failure
		}
		return nil
	})
	if !errors.Is(err, failure) || counts["a"] != 1 || counts["b"] != 2 {
		t.Fatal(counts, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	counts, err = seedPartitions(ctx, map[string][]int{"a": {0}}, func(context.Context, int) error { t.Fatal("canceled write admitted"); return nil })
	if !errors.Is(err, context.Canceled) || counts["a"] != 0 {
		t.Fatal(counts, err)
	}
}
