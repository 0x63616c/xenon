// Package app composes Xenon process services and owns their lifetimes.
package app

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/0x63616c/xenon/internal/agent"
	"github.com/0x63616c/xenon/internal/buildinfo"
	"github.com/0x63616c/xenon/internal/observability"
	"github.com/0x63616c/xenon/internal/storage"
	"github.com/0x63616c/xenon/internal/temporalruntime"
)

// Run starts one foreground Xenon instance under the caller's cancellation.
// The storage runtime still uses the legacy manager until persisted-layout and
// populated-state cutover gates permit new service activation.
func Run(ctx context.Context, c agent.Config) error {
	t, err := temporalruntime.New(c)
	if err != nil {
		return err
	}
	metrics := observability.New()
	s := storage.New(c, metrics.Events)
	r := &agent.Runtime{Storage: s, Temporal: t, StartupTimeout: 120 * time.Second, ShutdownTimeout: 30 * time.Second}
	go metrics.Run(ctx)
	var diagnostics *http.Server
	if c.DiagnosticsAddress != "" {
		listener, listenErr := net.Listen("tcp", c.DiagnosticsAddress)
		if listenErr != nil {
			return listenErr
		}
		diagnostics = &http.Server{Handler: metrics.Handler(r.Ready, func() any { return buildinfo.Read() }), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second}
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
