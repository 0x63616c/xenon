//go:build ministack

// xenon-temporal retains the pinned Temporal server and installs real Xenon stores.
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/0x63616c/xenon/internal/ministack"
	"github.com/0x63616c/xenon/internal/rpctrace"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/0x63616c/xenon/internal/temporalstore"
	"go.temporal.io/server/temporal"
)

func main() {
	path := flag.String("config", "", "exact Temporal config path")
	check := flag.Bool("check-config", false, "validate pinned configuration and Nexus HTTP routing without starting services")
	flag.Parse()
	if *path == "" {
		log.Fatal("config required")
	}
	if *check {
		if err := ministack.CheckNexusConfig(*path); err != nil {
			log.Fatal(err)
		}
		fmt.Println("TEMPORAL_CONFIG_VALID")
		return
	}
	server, e := temporal.NewServer(temporal.WithServerConfigFilePath(*path), temporal.WithCustomDataStoreFactory(temporalstore.AbstractFactory{}), temporal.WithCustomVisibilityStoreFactory(temporalstore.VisibilityFactory{}), temporal.ForServices(temporal.DefaultServices))
	if e != nil {
		log.Fatal(e)
	}
	if e = server.Start(); e != nil {
		log.Fatal(e)
	}
	fmt.Println("TEMPORAL_STARTED") // supervisor still requires successful API readiness
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	// External supervisor bounds shutdown and owns process-group termination.
	if e = server.Stop(); e != nil {
		log.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if e = rpctrace.Close(ctx); e != nil {
		log.Fatal(e)
	}
	if os.Getenv("XENON_RPC_TRACE_PATH") != "" {
		fmt.Println("TEMPORAL_TRACE_CLOSED")
	}
}
