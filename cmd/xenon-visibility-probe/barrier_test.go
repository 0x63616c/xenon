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
	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		if err := pageBarrier(ctx, filepath.Join(t.TempDir(), "absent"), "exact"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	})
}
