package main

import (
	"context"
	"fmt"
	"go.temporal.io/api/serviceerror"
	"testing"
)

func TestStorageReadRetryDisposition(t *testing.T) {
	for _, err := range []error{serviceerror.NewUnavailable("EOF"), fmt.Errorf("wrapped: %w", context.DeadlineExceeded)} {
		if !retryStorageRead(err) {
			t.Fatalf("transient not retried: %v", err)
		}
	}
	for _, err := range []error{nil, context.Canceled, serviceerror.NewNotFound("cluster"), serviceerror.NewInternal("corrupt metadata"), serviceerror.NewInvalidArgument("config")} {
		if retryStorageRead(err) {
			t.Fatalf("nontransient retried: %v", err)
		}
	}
}
