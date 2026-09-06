package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/0x63616c/xenon/internal/temporal/adapter"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

func retryStorageRead(err error) bool {
	var unavailable *serviceerror.Unavailable
	return errors.As(err, &unavailable) || errors.Is(err, context.DeadlineExceeded)
}

// partitionReady proves a request sent directly to one agent is accepted by
// the current owner (locally or through production forwarding). The caller
// chooses a partition known from the immutable cluster layout.
func partitionReady(address, partition string, budget time.Duration) error {
	if partition == "" || budget <= 0 || budget > time.Minute {
		return fmt.Errorf("partition and readiness budget in (0,60s] required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	store, err := adapter.NewClusterStore(address, partition)
	if err != nil {
		return err
	}
	defer store.Close()
	for {
		attempt, done := context.WithTimeout(ctx, 5*time.Second)
		_, err = store.ListClusterMetadata(attempt, &p.InternalListClusterMetadataRequest{PageSize: 1})
		done()
		if err == nil {
			return json.NewEncoder(os.Stdout).Encode(map[string]string{"storage": "ready", "partition": partition})
		}
		if !retryStorageRead(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("partition readiness deadline: %w", ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// Read the existing durable cluster record through the actual stable ingress.
// A listening TCP proxy is insufficient while all its backends are restarting.
// Missing/corrupt metadata and other logical errors fail immediately.
func storageReady(address string, budget time.Duration) error {
	if budget <= 0 || budget > time.Minute {
		return fmt.Errorf("storage readiness budget must be in (0,60s]")
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	store, err := adapter.NewClusterStore(address, "global")
	if err != nil {
		return err
	}
	defer store.Close()
	manager := p.NewClusterMetadataManagerImpl(store, serialization.NewSerializer(), "active", log.NewNoopLogger())
	var last error
	for {
		attempt, done := context.WithTimeout(ctx, 5*time.Second)
		current, err := manager.GetClusterMetadata(attempt, &p.GetClusterMetadataRequest{ClusterName: "active"})
		done()
		if err == nil {
			if current.ClusterName != "active" {
				return fmt.Errorf("cold cluster identity differs")
			}
			return json.NewEncoder(os.Stdout).Encode(map[string]any{"storage": "ready", "cluster": current.ClusterName, "version": current.Version})
		}
		if !retryStorageRead(err) {
			return err
		}
		last = err
		fmt.Fprintln(os.Stderr, "storage readiness transient:", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("storage readiness deadline: %w; last: %v", ctx.Err(), last)
		case <-time.After(250 * time.Millisecond):
		}
	}
}
