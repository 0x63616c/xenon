//go:build integration

package integration

import (
	"context"
	"os"
	"testing"
)

// This explicit integration-tag entry point is retained for CI and package
// development. The normal developer interface is `xenon test integration`.
func TestSlateDBMinIO(t *testing.T) {
	result, err := RunSlateDBMinIO(context.Background(), os.Stderr)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Assertions) != 4 {
		t.Fatalf("incomplete assertion census: %+v", result)
	}
	t.Logf("%s passed in %s: %v", result.Name, result.Duration, result.Assertions)
}

func TestMultiNodeOwnership(t *testing.T) {
	binary := os.Getenv("XENON_INTEGRATION_XENON_BINARY")
	if binary == "" {
		t.Skip("set XENON_INTEGRATION_XENON_BINARY to the built xenon CLI")
	}
	result, err := RunMultiNodeOwnership(context.Background(), os.Stderr, MultiNodeOptions{XenonBinary: binary})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Assertions) != 8 {
		t.Fatalf("incomplete assertion census: %+v", result)
	}
	t.Logf("%s passed in %s: %v", result.Name, result.Duration, result.Assertions)
}

func TestTemporalCompatibility(t *testing.T) {
	binary := os.Getenv("XENON_INTEGRATION_XENON_BINARY")
	if binary == "" {
		t.Skip("set XENON_INTEGRATION_XENON_BINARY to the built xenon CLI")
	}
	result, err := RunTemporalCompatibility(context.Background(), os.Stderr, TemporalOptions{XenonBinary: binary})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Assertions) != 12 {
		t.Fatalf("incomplete assertion census: %+v", result)
	}
	t.Logf("%s passed in %s: %v", result.Name, result.Duration, result.Assertions)
}
