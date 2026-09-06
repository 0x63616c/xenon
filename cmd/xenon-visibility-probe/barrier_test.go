package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestVisibilityMovementBarrier(t *testing.T) {
	t.Run("page_sizes", func(t *testing.T) {
		for _, size := range []int{1, 7, 100} {
			got, err := movementPageSizes("check", size)
			if err != nil || len(got) != 1 || got[0] != size {
				t.Fatalf("size %d: %v %v", size, got, err)
			}
		}
		for _, size := range []int{-1, 2, 101} {
			if _, err := movementPageSizes("check", size); err == nil {
				t.Fatalf("accepted %d", size)
			}
		}
		for _, mode := range []string{"seed", "mutate"} {
			if _, err := movementPageSizes(mode, 7); err == nil {
				t.Fatalf("accepted override for %s", mode)
			}
		}
		got, err := movementPageSizes("seed", 0)
		if err != nil || len(got) != 3 || got[0] != 1 || got[1] != 7 || got[2] != 100 {
			t.Fatal(got, err)
		}
	})

	t.Run("release", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "release")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- pageBarrier(ctx, path, "exact") }()
		select {
		case err := <-result:
			t.Fatalf("released without observed controller file: %v", err)
		case <-time.After(30 * time.Millisecond):
		}
		if err := os.WriteFile(path, []byte("exact"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	})
	t.Run("wrong_token", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "release")
		if err := os.WriteFile(path, []byte("wrong"), 0600); err != nil {
			t.Fatal(err)
		}
		if pageBarrier(context.Background(), path, "exact") == nil {
			t.Fatal("accepted wrong controller token")
		}
	})
	t.Run("late_release", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "release")
		if err := os.WriteFile(path, []byte("exact"), 0600); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := pageBarrier(ctx, path, "exact"); !errors.Is(err, context.Canceled) {
			t.Fatalf("late release accepted: %v", err)
		}
	})

	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		if err := pageBarrier(ctx, filepath.Join(t.TempDir(), "absent"), "exact"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	})
}
