package main

import (
	"context"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/node"
	"github.com/0x63616c/xenon/internal/ownership"
	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"
	"os"
	native "slatedb.io/slatedb-go/uniffi"
	"strconv"
	"time"
)

func main() {
	if os.Getenv("XENON_TOPOLOGY_PREFIX") != "" {
		managedMain()
		return
	}
	backend := os.Getenv("XENON_BACKEND")
	if backend == "" {
		backend = "s3"
	}
	url := "memory:///"
	switch backend {
	case "memory":
	case "s3":
		bucket := os.Getenv("XENON_BUCKET")
		if bucket == "" {
			log.Fatal("XENON_BUCKET required")
		}
		url = "s3://" + bucket
	default:
		log.Fatal("invalid XENON_BACKEND")
	}
	prefix, partition := os.Getenv("XENON_PREFIX"), os.Getenv("XENON_PARTITION")
	if prefix == "" || partition == "" {
		log.Fatal("XENON_PREFIX and XENON_PARTITION required")
	}
	type opened struct {
		store *native.ObjectStore
		db    *native.Db
		err   error
	}
	ready := make(chan opened, 1)
	go func() {
		store, err := native.ObjectStoreResolve(url)
		if err != nil {
			ready <- opened{err: err}
			return
		}
		builder := native.NewDbBuilder(prefix, store)
		defer builder.Destroy()
		database, err := builder.Build()
		ready <- opened{store, database, err}
	}()
	var result opened
	select {
	case result = <-ready:
	case <-time.After(30 * time.Second):
		log.Fatal("native startup deadline expired; terminating owner process")
	}
	if result.err != nil {
		log.Fatal(result.err)
	}
	config := node.DefaultConfig(partition)
	config.MaxOutcomes = outcomeLimit()
	owner, err := node.NewOwner(result.db, config)
	if err != nil {
		log.Fatal(err)
	}
	address := os.Getenv("XENON_LISTEN")
	if address == "" {
		address = "127.0.0.1:7235"
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(2 * 1024 * 1024))
	wire.RegisterShardPersistenceServer(server, owner)
	wire.RegisterQueuePersistenceServer(server, &node.QueueServer{Owner: owner})
	wire.RegisterQueueV2PersistenceServer(server, &node.QueueV2Server{Owner: owner})
	wire.RegisterHistoryPersistenceServer(server, &node.HistoryServer{Owner: owner})
	wire.RegisterExecutionPersistenceServer(server, &node.ExecutionServer{Owner: owner})
	wire.RegisterExecutionTasksPersistenceServer(server, &node.ExecutionTasksServer{Owner: owner})
	wire.RegisterHistoryTasksPersistenceServer(server, &node.HistoryTasksServer{Owner: owner})
	wire.RegisterMetadataPersistenceServer(server, &node.MetadataServer{Owner: owner})
	wire.RegisterMatchingPersistenceServer(server, &node.MatchingServer{Owner: owner})
	wire.RegisterClusterPersistenceServer(server, &node.ClusterServer{Owner: owner})
	wire.RegisterNexusPersistenceServer(server, &node.NexusServer{Owner: owner})
	wire.RegisterVisibilityPersistenceServer(server, &node.VisibilityServer{Owner: owner})
	fmt.Printf("READY %s\n", listener.Addr())
	// Process replacement, including SIGTERM, ends the embedded runtime together.
	// Never call Destroy on a database while a timed-out native call remains active.
	if err = server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}

func managedMain() {
	topology, err := ownership.Environment()
	if err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", os.Getenv("XENON_LISTEN"))
	if err != nil {
		log.Fatal(err)
	}
	address := os.Getenv("XENON_ADVERTISE")
	if address == "" {
		address = listener.Addr().String()
	}
	manager, err := ownership.NewManager(topology, os.Getenv("XENON_NODE"), address, "s3://"+os.Getenv("XENON_BUCKET"), outcomeLimit())
	if err != nil {
		log.Fatal(err)
	}

	if address := os.Getenv("XENON_METRICS_LISTEN"); address != "" {
		l, e := net.Listen("tcp", address)
		if e != nil {
			log.Fatal(e)
		}
		metrics := &http.Server{Handler: manager.OutcomesHandler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second}
		go func() {
			if e := metrics.Serve(l); e != nil && e != http.ErrServerClosed {
				log.Fatal(e)
			}
		}()
	}
	server, router := manager.Server()
	defer router.Close()
	go manager.Run(context.Background())
	identity := manager.Identity()
	fmt.Printf("INGRESS %s %s %s\n", identity.Node, identity.Incarnation, identity.Address)
	if err = server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}

func outcomeLimit() uint64 {
	limit := node.DefaultConfig("").MaxOutcomes
	if text := os.Getenv("XENON_MAX_OUTCOMES"); text != "" {
		value, e := strconv.ParseUint(text, 10, 64)
		if e != nil || value == 0 {
			log.Fatal("XENON_MAX_OUTCOMES must be a positive uint64")
		}
		limit = value
	}
	return limit
}
