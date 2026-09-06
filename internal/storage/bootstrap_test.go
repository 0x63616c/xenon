package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/0x63616c/xenon/internal/agent"
)

func TestServiceConfigCannotFallBackToLegacyManager(t *testing.T) {
	// No bucket/credentials/network setup: selection must fail before any legacy
	// namespace mutation, listener, membership registration or native open.
	runtime := New(agent.Config{ServiceStorage: &agent.ServiceStorageConfig{Format: 2}})
	if err := runtime.Start(context.Background()); !errors.Is(err, ErrServiceRuntimeUnavailable) {
		t.Fatal(err)
	}
	if runtime.manager != nil || runtime.server != nil || runtime.cancel != nil {
		t.Fatal("legacy runtime partially started")
	}
}
