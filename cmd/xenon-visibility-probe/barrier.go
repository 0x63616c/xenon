package main

import (
	"context"
	"fmt"
	"os"
	"time"
)

// The external controller releases an actual cursor only after topology publication.
// This pause has its own120s bound; it does not alter any30s public API deadline.
func pageBarrier(ctx context.Context, path, token string) error {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			if string(raw) != token {
				return fmt.Errorf("movement release token mismatch")
			}
			return nil
		}
		if !os.IsNotExist(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
