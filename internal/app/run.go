// Package app composes Xenon process services and owns their lifetimes.
package app

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/0x63616c/xenon/internal/agent"
	"github.com/0x63616c/xenon/internal/buildinfo"
	"github.com/0x63616c/xenon/internal/observability"
	"github.com/0x63616c/xenon/internal/storage"
	"github.com/0x63616c/xenon/internal/temporal"
)

// Run starts one foreground Xenon instance under the caller's cancellation.
// Service storage is explicit; legacy namespaces require offline cutover.
func Run(ctx context.Context, c agent.Config) error {
	if err := ValidateServiceLayout(c); err != nil {
		return err
	}
	t, err := temporal.New(c)
	if err != nil {
		return err
	}
	metrics := observability.New()
	s := storage.NewServiceRuntime(c, metrics.Events)
	r := &agent.Runtime{Storage: s, Temporal: t, StartupTimeout: 120 * time.Second, ShutdownTimeout: 30 * time.Second}
	go metrics.Run(ctx)
	var diagnostics *http.Server
	if c.DiagnosticsAddress != "" {
		listener, listenErr := net.Listen("tcp", c.DiagnosticsAddress)
		if listenErr != nil {
			return listenErr
		}
		diagnostics = &http.Server{Handler: metrics.Handler(r.Ready, func() any { return map[string]any{"build": buildinfo.Read(), "storage": s.Diagnostics()} }), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second}
		go func() { _ = diagnostics.Serve(listener) }()
	}
	err = r.Run(ctx)
	if diagnostics != nil {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = diagnostics.Shutdown(shutdown)
		cancel()
	}
	return err
}

// ValidateServiceLayout checks the existing Temporal adapter domain contract
// before any namespace claim. It never invents paths, IDs, ordering or a count.
func ValidateServiceLayout(c agent.Config) error {
	if c.ServiceStorage == nil {
		return fmt.Errorf("%w: explicit service_storage format2 required", storage.ErrLegacyPrefix)
	}
	if err := c.Validate(); err != nil {
		return err
	}
	for _, name := range []string{"global", "matching", "history-0", "history-1", "history-2", "history-3", "vis-v1-0", "vis-v1-1", "vis-v1-2", "vis-v1-3"} {
		if _, ok := c.ServiceStorage.Layout.Resolve(name); !ok {
			return fmt.Errorf("service layout lacks required Temporal logical partition %q", name)
		}
	}
	return nil
}
