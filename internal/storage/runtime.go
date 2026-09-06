// Package storage owns the embedded storage runtime. Native engine resources do
// not escape this boundary. Closing admission never frees an active native call.
package storage

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/0x63616c/xenon/internal/adapter"
	"github.com/0x63616c/xenon/internal/agent"
	"github.com/0x63616c/xenon/internal/node"
	"github.com/0x63616c/xenon/internal/ownership"
	"github.com/0x63616c/xenon/internal/routing"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
)

type Runtime struct {
	config   agent.Config
	manager  *ownership.Manager
	server   *grpc.Server
	router   *routing.Router
	cancel   context.CancelFunc
	serveErr chan error
	stop     sync.Once
	events   chan<- routing.Event
}

func New(c agent.Config, events ...chan<- routing.Event) *Runtime {
	r := &Runtime{config: c, serveErr: make(chan error, 1)}
	if len(events) > 0 {
		r.events = events[0]
	}
	return r
}

func (r *Runtime) Start(parent context.Context) error {
	c := r.config
	topology, err := ownership.FromEnvironment(c.Bucket, c.Prefix+"/metadata")
	if err != nil {
		return err
	}
	manifestCtx, cancelManifest := context.WithTimeout(parent, 30*time.Second)
	err = ownership.EnsureCluster(manifestCtx, topology, ownership.ClusterManifest{Format: 1, Cluster: c.Cluster, HistoryShards: c.HistoryShards, LayoutVersion: 1, WireVersion: 1}, c.Bootstrap)
	cancelManifest()
	if err != nil {
		return fmt.Errorf("cluster manifest: %w", err)
	}
	listener, err := net.Listen("tcp", c.Listen(8))
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if !success {
			_ = listener.Close()
		}
	}()
	m, err := ownership.NewManager(topology, c.Node, c.Address(8), "s3://"+c.Bucket, node.DefaultConfig("").MaxOutcomes)
	if err != nil {
		return err
	}
	// Startup is one admission attempt. Never re-register an obsolete incarnation.
	join, cancelJoin := context.WithTimeout(parent, 30*time.Second)
	err = ownership.Join(join, topology, m.Identity(), c.Prefix+"/data", c.Bootstrap)
	cancelJoin()
	if err != nil {
		return fmt.Errorf("cluster join: %w", err)
	}
	r.manager = m
	r.server, r.router = m.Server()
	r.router.Events = r.events
	ctx, cancel := context.WithCancel(parent)
	r.cancel = cancel
	go m.Run(ctx)
	go func() { r.serveErr <- r.server.Serve(listener) }()
	success = true
	return nil
}

func (r *Runtime) Ready(ctx context.Context) error {
	select {
	case err := <-r.serveErr:
		return fmt.Errorf("storage listener stopped: %w", err)
	default:
	}
	if r.manager == nil {
		return errors.New("storage not started")
	}
	// This executes a real routed read and durable admission barrier on the global
	// partition. A forwarding-only node may be ready without owning that partition.
	store, err := adapter.NewClusterStore(r.config.Address(8), "global")
	if err != nil {
		return err
	}
	defer store.Close()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err = store.ListClusterMetadata(ctx, &p.InternalListClusterMetadataRequest{PageSize: 1})
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("routed persistence not ready: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (r *Runtime) Stop(ctx context.Context) error {
	r.stop.Do(func() {
		if r.server != nil {
			r.server.Stop()
		}
		if r.router != nil {
			_ = r.router.Close()
		}
		if r.cancel != nil {
			r.cancel()
		}
	})
	// Manager cancellation retires admission. Whole-process exit releases native
	// resources; no concurrent Destroy or fictitious FFI cancellation is attempted.
	return nil
}
