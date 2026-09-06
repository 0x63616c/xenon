// xenon runs one complete agent: upstream Temporal and Xenon persistence.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/0x63616c/xenon/internal/agent"
	"github.com/0x63616c/xenon/internal/buildinfo"
	"github.com/0x63616c/xenon/internal/observability"
	"github.com/0x63616c/xenon/internal/storage"
	"github.com/0x63616c/xenon/internal/temporalruntime"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: xenon version | check-config --config FILE | start --config FILE")
	}
	if args[0] == "version" {
		if len(args) != 1 {
			return fmt.Errorf("version takes no arguments")
		}
		return json.NewEncoder(os.Stdout).Encode(buildinfo.Read())
	}
	if args[0] != "start" && args[0] != "check-config" {
		return fmt.Errorf("unknown command %q", args[0])
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	path := flags.String("config", "", "Xenon JSON configuration")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if *path == "" || flags.NArg() != 0 {
		return fmt.Errorf("--config FILE required; no positional arguments")
	}
	c, err := agent.Load(*path)
	if err != nil {
		return err
	}
	if _, err = temporalruntime.Configuration(c); err != nil {
		return err
	}
	if args[0] == "check-config" {
		fmt.Println("XENON_CONFIG_VALID")
		return nil
	}
	// Version metadata carries no credentials and is emitted before network work.
	if err = json.NewEncoder(os.Stdout).Encode(buildinfo.Read()); err != nil {
		return err
	}
	t, err := temporalruntime.New(c)
	if err != nil {
		return err
	}
	metrics := observability.New()
	s := storage.New(c, metrics.Events)
	r := &agent.Runtime{Storage: s, Temporal: t, StartupTimeout: 120 * time.Second, ShutdownTimeout: 30 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
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
