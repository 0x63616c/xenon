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
		if err := ctx.Err(); err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err == nil {
			if err := ctx.Err(); err != nil {
				return err
			}
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

// Restrict the override to the frozen population's declared traversal sizes.
// Seeding and mutation continue to validate every declared size.
func movementPageSizes(mode string, override int) ([]int, error) {
	if override == 0 {
		return []int{1, 7, 100}, nil
	}
	if mode != "check" {
		return nil, fmt.Errorf("page-size override requires check mode")
	}
	switch override {
	case 1, 7, 100:
		return []int{override}, nil
	default:
		return nil, fmt.Errorf("page-size override must be 1, 7 or 100")
	}
}
