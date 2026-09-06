package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/partitions/slatedb"
	"github.com/0x63616c/xenon/internal/registry"
	"github.com/0x63616c/xenon/internal/routing"
	"github.com/0x63616c/xenon/internal/temporal/adapter"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/api/serviceerror"
	persistence "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// ServiceRuntime composes the production drivers. It owns their native/effect
// lifetimes, but delegates all routing, borrowing and authority checks to routing.
// Start does not claim readiness: Ready executes a real routed durable operation.
type ServiceRuntime struct {
	config                                   Config
	events                                   chan<- routing.Event
	mu                                       sync.Mutex
	startAttempt, started, stopping, stopped bool
	startDone, loopsDone                     chan struct{}
	cancel                                   context.CancelFunc
	prepared                                 *Prepared
	coordinator                              *cluster.Service
	membership                               *cluster.Membership
	partitions                               map[identity.PartitionID]*partitions.Service
	server                                   *grpc.Server
	router                                   *routing.Router
	probe                                    *adapter.ClusterStore
	view                                     cluster.MembershipView
	registrationErr, serveErr                error
	lastHeartbeat                            time.Time
	stopSignals                              sync.Once
	// client is configured before Start only by real S3 integration tests.
	client *s3.Client
}

func NewServiceRuntime(c Config, events ...chan<- routing.Event) *ServiceRuntime {
	if c.ServiceStorage != nil {
		copy := c.ServiceStorage.Clone()
		c.ServiceStorage = &copy
	}
	r := &ServiceRuntime{config: c, startDone: make(chan struct{}), loopsDone: make(chan struct{}), registrationErr: errRegistrationPending}
	if len(events) > 0 {
		r.events = events[0]
	}
	return r
}
func serviceS3Client() (*s3.Client, error) {
	region := os.Getenv("AWS_DEFAULT_REGION")
	if region == "" {
		region = os.Getenv("AWS_REGION")
	}
	key, secret := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	if region == "" || key == "" || secret == "" {
		return nil, errors.New("AWS region and external credentials required")
	}
	config := aws.Config{Region: region, Credentials: credentials.NewStaticCredentialsProvider(key, secret, os.Getenv("AWS_SESSION_TOKEN"))}
	return s3.NewFromConfig(config, func(o *s3.Options) {
		if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	}), nil
}
func (r *ServiceRuntime) Start(parent context.Context) (result error) {
	if parent == nil {
		return errors.New("runtime context required")
	}
	r.mu.Lock()
	if r.startAttempt || r.stopping {
		r.mu.Unlock()
		return errors.New("service runtime already started or stopped")
	}
	r.startAttempt = true
	ctx, cancel := context.WithCancel(parent)
	r.cancel = cancel
	r.mu.Unlock()
	defer close(r.startDone)
	success := false
	defer func() {
		if !success {
			cancel()
		}
	}()
	c := r.config
	if c.ServiceStorage == nil {
		return fmt.Errorf("%w: explicit service_storage configuration required", ErrLegacyPrefix)
	}
	if err := c.Validate(); err != nil {
		return err
	}
	client := r.client
	if client == nil {
		var err error
		client, err = serviceS3Client()
		if err != nil {
			return err
		}
	}
	prepared, err := PrepareServiceStorage(ctx, c, client, identity.Generator{})
	if err != nil {
		return err
	}
	store := boundedRegistry{Store: prepared.Registry(), timeout: duration(c.ServiceStorage.RegistryTimeout)}
	coordinator, err := cluster.NewService(ctx, prepared.ClusterConfig(), store, identity.Generator{})
	if err != nil {
		return err
	}
	membership, err := cluster.NewMembership(prepared.MembershipConfig(), store, identity.Generator{})
	if err != nil {
		return err
	}
	engine, err := slatedb.NewWithWALFlushInterval("s3://"+c.Bucket, c.ServiceStorage.EffectiveWALFlushIntervalMS())
	if err != nil {
		return err
	}
	drivers := make(map[identity.PartitionID]*partitions.Service, len(c.ServiceStorage.Layout.Partitions))
	for _, config := range prepared.PartitionConfigs() {
		service, err := partitions.NewService(ctx, config, store, engine, identity.Generator{})
		if err != nil {
			return err
		}
		drivers[config.Partition] = service
	}
	cc := prepared.ClusterConfig()
	server, router, err := routing.NewServer(routing.ServerConfig{Owner: prepared.Owner(), Cluster: prepared.Control().Cluster, Layout: c.ServiceStorage.Layout, ExpectedLayoutDigest: cc.ExpectedLayoutDigest, Store: store, ControlKey: cc.Key, MaxControlBytes: cc.MaxControlBytes, MaxOutcomes: c.ServiceStorage.MaxOutcomes, Now: time.Now, Partitions: drivers, Events: r.events})
	if err != nil {
		return err
	}
	defer func() {
		if !success {
			server.Stop()
			_ = router.Close()
		}
	}()
	listener, err := net.Listen("tcp", c.Listen(8))
	if err != nil {
		return err
	}
	defer func() {
		if !success {
			_ = listener.Close()
		}
	}()
	probe, err := adapter.NewClusterStore(c.Address(8), "global")
	if err != nil {
		return err
	}
	defer func() {
		if !success {
			probe.Close()
		}
	}()
	r.mu.Lock()
	if r.stopping {
		r.mu.Unlock()
		return context.Canceled
	}
	r.prepared, r.coordinator, r.membership, r.partitions = prepared, coordinator, membership, drivers
	r.server, r.router, r.probe = server, router, probe
	r.started = true
	r.mu.Unlock()
	origin := time.Now()
	var loops sync.WaitGroup
	loops.Add(3)
	go func() { defer loops.Done(); r.poll(ctx, origin) }()
	go func() { defer loops.Done(); r.observeMembership(ctx, origin) }()
	go func() {
		defer loops.Done()
		err := server.Serve(listener)
		r.mu.Lock()
		if !r.stopping {
			r.serveErr = fmt.Errorf("storage listener stopped: %w", err)
		}
		r.mu.Unlock()
	}()
	go func() { loops.Wait(); close(r.loopsDone) }()
	success = true
	return nil
}
func (r *ServiceRuntime) poll(ctx context.Context, origin time.Time) {
	tick := time.NewTicker(duration(r.config.ServiceStorage.PollInterval))
	defer tick.Stop()
	for {
		r.mu.Lock()
		view := r.view
		r.mu.Unlock()
		r.coordinator.Poll(cluster.Tick(time.Since(origin)), view)
		for _, physical := range r.config.ServiceStorage.Layout.Partitions {
			r.partitions[physical.ID].Poll()
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
func (r *ServiceRuntime) observeMembership(ctx context.Context, origin time.Time) {
	c := r.config.ServiceStorage
	heartbeats := time.NewTicker(duration(c.HeartbeatInterval))
	defer heartbeats.Stop()
	discovery := time.NewTicker(duration(c.DiscoveryInterval))
	defer discovery.Stop()
	beat := func() {
		budget, cancel := context.WithTimeout(ctx, duration(c.RegistryTimeout))
		defer cancel()
		err := r.membership.Heartbeat(budget, cluster.Tick(time.Since(origin)))
		r.mu.Lock()
		r.registrationErr = err
		if err == nil {
			r.lastHeartbeat = time.Now()
		}
		r.mu.Unlock()
	}
	beat()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeats.C:
			beat()
		case <-discovery.C:
			control, known := r.coordinator.Snapshot().Control()
			view := cluster.MembershipView{}
			if known && control.Coordinator.Incarnation == r.prepared.Owner().Incarnation {
				budget, cancel := context.WithTimeout(ctx, duration(c.RegistryTimeout))
				observed, err := r.membership.Discover(budget, cluster.Tick(time.Since(origin)), control.Coordinator)
				cancel()
				if err == nil {
					view = observed
				}
			}
			r.mu.Lock()
			r.view = view
			r.mu.Unlock()
		}
	}
}

// Ready is an honest admission probe. Membership admission and a running listener
// are necessary, but only the routed ListClusterMetadata barrier proves work can
// currently complete. Later movement can still interrupt Temporal initialization.
var errRegistrationPending = errors.New("membership registration pending")
var errHeartbeatOverdue = errors.New("membership heartbeat is not current")

func (r *ServiceRuntime) Ready(ctx context.Context) error {
	if ctx == nil {
		return errors.New("readiness context required")
	}
	if r.config.ServiceStorage == nil {
		return ErrLegacyPrefix
	}
	if err := r.config.Validate(); err != nil {
		return err
	}
	return waitForReadiness(ctx, duration(r.config.ServiceStorage.PollInterval), r.readyOnce)
}

// A child RPC budget may expire before the caller's readiness budget. Retry a
// completed transient observation without extending either deadline.
func waitForReadiness(ctx context.Context, interval time.Duration, observe func(context.Context) error) error {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	var last error
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(err, last)
		}
		err := observe(ctx)
		if ctx.Err() != nil {
			return errors.Join(ctx.Err(), err, last)
		}
		if err == nil {
			return nil
		}
		last = err
		var permanent *PermanentError
		if errors.As(err, &permanent) {
			return err
		}
		var unavailable *registry.Unavailable
		var unknown *registry.UnknownOutcome
		var conflict *registry.Conflict
		code := serviceerror.ToStatus(err).Code()
		retry := errors.Is(err, errRegistrationPending) || errors.Is(err, errHeartbeatOverdue) || errors.As(err, &unavailable) || errors.As(err, &unknown) || errors.As(err, &conflict) || errors.Is(err, context.DeadlineExceeded) || code == codes.Unavailable || code == codes.DeadlineExceeded
		if !retry {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), err)
		case <-tick.C:
		}
	}
}

func (r *ServiceRuntime) readyOnce(ctx context.Context) error {
	if ctx == nil {
		return errors.New("readiness context required")
	}
	r.mu.Lock()
	if !r.started || r.stopping {
		r.mu.Unlock()
		return errors.New("service runtime not running")
	}
	err := r.registrationErr
	serveErr := r.serveErr
	last := r.lastHeartbeat
	probe := r.probe
	r.mu.Unlock()
	if serveErr != nil {
		return Permanent(serveErr)
	}
	if err != nil {
		return fmt.Errorf("membership registration: %w", err)
	}
	if time.Since(last) >= duration(r.config.ServiceStorage.MembershipFailureAfter) {
		return errHeartbeatOverdue
	}
	_, err = probe.ListClusterMetadata(ctx, &persistence.InternalListClusterMetadataRequest{PageSize: 1})
	return err
}

// Diagnostics reads process state only. It performs no storage/native I/O and is
// deliberately not a readiness assertion. Never include operation payloads.
type ServiceDiagnostics struct {
	WALFlushIntervalMS int                 `json:"wal_flush_interval_ms"`
	Started            bool                `json:"started"`
	Stopping           bool                `json:"stopping"`
	Owner              cluster.Owner       `json:"owner"`
	Coordinator        cluster.Coordinator `json:"coordinator"`
	MembershipReady    bool                `json:"membership_ready"`
	Eligible           int                 `json:"eligible"`
	ReadyPartitions    int                 `json:"ready_partitions"`
	PendingEffects     int                 `json:"pending_effects"`
	RegistrationError  string              `json:"registration_error,omitempty"`
}

func (r *ServiceRuntime) Diagnostics() ServiceDiagnostics {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := ServiceDiagnostics{Started: r.started, Stopping: r.stopping, MembershipReady: r.view.Ready, Eligible: len(r.view.Members)}
	if r.config.ServiceStorage != nil {
		out.WALFlushIntervalMS = r.config.ServiceStorage.EffectiveWALFlushIntervalMS()
	}
	if r.registrationErr != nil {
		out.RegistrationError = r.registrationErr.Error()
	}
	if r.prepared != nil {
		out.Owner = r.prepared.Owner()
	}
	if r.coordinator != nil {
		control, _ := r.coordinator.Snapshot().Control()
		out.Coordinator = control.Coordinator
		out.PendingEffects += len(r.coordinator.Snapshot().Pending())
	}
	for _, p := range r.partitions {
		_, _, _, ready := p.Writer()
		if ready {
			out.ReadyPartitions++
		}
		out.PendingEffects += len(p.Snapshot().Pending())
	}
	return out
}
func (r *ServiceRuntime) Stop(ctx context.Context) error {
	if ctx == nil {
		return errors.New("stop context required")
	}
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return nil
	}
	r.stopping = true
	if r.cancel != nil {
		r.cancel()
	}
	attempted := r.startAttempt
	r.mu.Unlock()
	if !attempted {
		r.mu.Lock()
		r.stopped = true
		r.mu.Unlock()
		return nil
	}
	incomplete := func(err error) error { return errors.Join(ErrProcessExitRequired, err) }
	select {
	case <-r.startDone:
	case <-ctx.Done():
		return incomplete(ctx.Err())
	}
	r.stopSignals.Do(func() {
		if r.server != nil {
			r.server.Stop()
		}
		if r.router != nil {
			_ = r.router.Close()
		}
		if r.probe != nil {
			r.probe.Close()
		}
		if r.coordinator != nil {
			r.coordinator.Stop()
		}
		for _, p := range r.partitions {
			p.Stop()
		}
	})
	r.mu.Lock()
	started := r.started
	r.mu.Unlock()
	if !started {
		r.mu.Lock()
		r.stopped = true
		r.mu.Unlock()
		return nil
	}
	select {
	case <-r.loopsDone:
	case <-ctx.Done():
		return incomplete(ctx.Err())
	}
	if err := r.coordinator.Drain(ctx); err != nil {
		return incomplete(err)
	}
	for _, physical := range r.config.ServiceStorage.Layout.Partitions {
		if err := r.partitions[physical.ID].Drain(ctx); err != nil {
			return incomplete(err)
		}
	}
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()
	return nil
}

// Per-call registry budgets live at the host effect boundary. Cancellation stays
// within the registry contract; a dispatched write can return UnknownOutcome.
type boundedRegistry struct {
	registry.Store
	timeout time.Duration
}

func (s boundedRegistry) Read(ctx context.Context, key registry.Key) (registry.Record, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.Store.Read(ctx, key)
}
func (s boundedRegistry) Create(ctx context.Context, key registry.Key, w registry.Write) (registry.Record, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.Store.Create(ctx, key, w)
}
func (s boundedRegistry) Replace(ctx context.Context, key registry.Key, v registry.Version, w registry.Write) (registry.Record, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.Store.Replace(ctx, key, v, w)
}
