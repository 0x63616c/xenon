// xenon runs one complete agent: upstream Temporal and Xenon persistence.
package main

import (
	"context"
	"github.com/0x63616c/xenon/internal/app"
	"os"
	"os/signal"
	"syscall"
)

func main() { os.Exit(mainExit()) }

func mainExit() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return execute(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, app.Run)
}
