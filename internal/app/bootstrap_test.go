package app

import (
	"context"
	"errors"
	"testing"
)

func TestServiceConfigCannotFallBackToLegacyManager(t *testing.T) {
	// No bucket/credentials/network setup: selection must fail before any legacy
	// namespace mutation, listener, membership registration or native open.
	runtime := NewLegacyStorageRuntime(Config{ServiceStorage: &ServiceStorageConfig{Format: 2}})
	if err := runtime.Start(context.Background()); !errors.Is(err, ErrServiceRuntimeUnavailable) {
		t.Fatal(err)
	}
	if runtime.manager != nil || runtime.server != nil || runtime.cancel != nil {
		t.Fatal("legacy runtime partially started")
	}
}
