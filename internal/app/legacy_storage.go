// LegacyStorageRuntime retains the historical ownership-manager composition. Native engine resources do
// not escape this boundary. Closing admission never frees an active native call.
package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/0x63616c/xenon/internal/directory"
	"github.com/0x63616c/xenon/internal/node"
	"github.com/0x63616c/xenon/internal/ownership"
	"github.com/0x63616c/xenon/internal/routing"
	"github.com/0x63616c/xenon/internal/temporal/adapter"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
)

type LegacyStorageRuntime struct {
	config   Config
	manager  *ownership.Manager
	server   *grpc.Server
	router   *routing.Router
	cancel   context.CancelFunc
	serveErr chan error
	stop     sync.Once
	events   chan<- routing.Event
	errMu    sync.Mutex
	fatalErr error
}

func NewLegacyStorageRuntime(c Config, events ...chan<- routing.Event) *LegacyStorageRuntime {
	r := &LegacyStorageRuntime{config: c, serveErr: make(chan error, 1)}
	if len(events) > 0 {
		r.events = events[0]
	}
	return r
}

func (r *LegacyStorageRuntime) Start(parent context.Context) error {
	c := r.config
	if c.ServiceStorage != nil {
		return ErrServiceRuntimeUnavailable
	}
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
	membership, err := ownership.NewMembership(topology, m.Identity(), ownership.FailureTimeout)
	if err != nil {
		return err
	}
	r.server, r.router = m.Server()
	r.router.Events = r.events
	ctx, cancel := context.WithCancel(parent)
	r.cancel = cancel
	go m.Run(ctx)
	go r.runMembership(ctx, membership)
	go func() { r.serveErr <- r.server.Serve(listener) }()
	success = true
	return nil
}

func (r *LegacyStorageRuntime) Ready(ctx context.Context) error {
	r.errMu.Lock()
	fatal := r.fatalErr
	r.errMu.Unlock()
	if fatal != nil {
		return Permanent(fatal)
	}
	select {
	case err := <-r.serveErr:
		return Permanent(fmt.Errorf("storage listener stopped: %w", err))
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

func (r *LegacyStorageRuntime) runMembership(ctx context.Context, membership *ownership.Membership) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if err := membership.Step(ctx, time.Now()); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			if errors.Is(err, directory.ErrConflict) {
				r.errMu.Lock()
				r.fatalErr = fmt.Errorf("membership authority lost: %w", err)
				r.errMu.Unlock()
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *LegacyStorageRuntime) Stop(ctx context.Context) error {
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
