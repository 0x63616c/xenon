package main

import (
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/node"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	native "slatedb.io/slatedb-go/uniffi"
	"strconv"
	"time"
)

func main() {
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
	if value := os.Getenv("XENON_MAX_OUTCOMES"); value != "" {
		limit, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		config.MaxOutcomes = limit
	}
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
	wire.RegisterMetadataPersistenceServer(server, &node.MetadataServer{Owner: owner})
	wire.RegisterClusterPersistenceServer(server, &node.ClusterServer{Owner: owner})
	wire.RegisterNexusPersistenceServer(server, &node.NexusServer{Owner: owner})
	fmt.Printf("READY %s\n", listener.Addr())
	// Process replacement, including SIGTERM, ends the embedded runtime together.
	// Never call Destroy on a database while a timed-out native call remains active.
	if err = server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
