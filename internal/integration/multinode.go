package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/app"
	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
	registrys3 "github.com/0x63616c/xenon/internal/registry/s3"
	"github.com/0x63616c/xenon/internal/routing"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// MultiNodeOptions contains the one process dependency that differs when this
// journey is invoked from a Go test binary instead of the xenon CLI itself.
type MultiNodeOptions struct{ XenonBinary string }

// RunMultiNodeOwnership proves the production process, S3 authority and gRPC
// routing boundaries. Detailed interleavings stay in DST; this journey is one
// bounded correspondence check.
func RunMultiNodeOwnership(ctx context.Context, diagnostics io.Writer, options MultiNodeOptions) (result JourneyResult, err error) {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	evidence, err := beginJourney(ctx, &result, "multi-node-ownership", diagnostics)
	defer evidence.finish(ctx, &err)
	if err != nil {
		return result, err
	}
	diagnostics = evidence.writer()
	runtime, err := startMinIO(ctx, diagnostics)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, runtime.close(diagnostics)) }()
	restore := setEnvironment(map[string]string{"AWS_ACCESS_KEY_ID": "xenon-local", "AWS_SECRET_ACCESS_KEY": "xenon-local-test-only", "AWS_DEFAULT_REGION": "us-east-1", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": runtime.endpoint, "AWS_ALLOW_HTTP": "true", "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false", "AWS_EC2_METADATA_DISABLED": "true", "AWS_CONFIG_FILE": os.DevNull, "AWS_SHARED_CREDENTIALS_FILE": os.DevNull})
	defer restore()

	binary := options.XenonBinary
	if binary == "" {
		binary, err = os.Executable()
	}
	if err != nil {
		return result, err
	}
	if err = evidence.file("xenon-binary", binary); err != nil {
		return result, err
	}
	bucket, prefix := "xenon-integration-"+randomHex(8), "multi-node"
	client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(runtime.endpoint), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider("xenon-local", "xenon-local-test-only", "")})
	if _, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return result, err
	}
	store, err := registrys3.New(client, bucket, prefix+"/metadata/registry")
	if err != nil {
		return result, err
	}

	bases, err := reservePortBlocks(3)
	if err != nil {
		return result, err
	}
	configs := make([]app.Config, 3)
	for i := range configs {
		configs[i] = integrationNodeConfig(bucket, prefix, i+1, bases[i], i == 0)
	}
	for i := range configs {
		configs[i].PublicAddress = fmt.Sprintf("127.0.0.1:%d", bases[0])
		configs[i].PublicHTTPAddress = fmt.Sprintf("127.0.0.1:%d", bases[0]+9)
	}
	if err = evidence.config("nodes", configs); err != nil {
		return result, err
	}
	processes := make([]*nodeProcess, 3)
	defer func() {
		for _, process := range processes {
			if process != nil {
				err = errors.Join(err, process.kill())
			}
		}
	}()
	start := func(index int) error {
		process, startErr := startNode(ctx, binary, configs[index], evidence)
		if startErr != nil {
			return startErr
		}
		processes[index] = process
		if readyErr := waitHTTP(ctx, fmt.Sprintf("http://127.0.0.1:%d/readyz", configs[index].BasePort+10)); readyErr != nil {
			return fmt.Errorf("node %d readiness: %w\n%s", index+1, readyErr, process.output.String())
		}
		return nil
	}
	if err = start(0); err != nil {
		return result, err
	}
	initial, err := waitControl(ctx, store, func(c cluster.Control) bool { return readyOwner(c, "global") == configs[0].ServiceStorage.NodeID })
	if err != nil {
		return result, err
	}
	acknowledged := []*wire.QueueRequest{queueWrite(1)}
	if err = enqueueRecord(ctx, configs[0].Address(8), acknowledged[0]); err != nil {
		return result, fmt.Errorf("initial write: %w", err)
	}
	traffic, stopTraffic := context.WithCancel(ctx)
	trafficDone := make(chan struct{})
	var trafficErr error
	var movedWrite bool
	go func() {
		defer close(trafficDone)
		// Retry an uncertain write with the identical identity and digest. Bound
		// the corpus so final reads remain a small correspondence check.
		for ordinal := 100; traffic.Err() == nil && ordinal < 108; {
			// Observe the production assignment before sending so an acknowledgement
			// from the initial single-node phase alone cannot satisfy movement.
			revision := uint64(0)
			if record, readErr := store.Read(traffic, "cluster/control"); readErr == nil {
				if snapshot, decodeErr := cluster.DecodeControl("cluster/control", record, 1<<20); decodeErr == nil {
					revision = snapshot.Control().AssignmentRevision
				}
			}
			request := queueWrite(ordinal)
			if writeErr := enqueueRecord(traffic, configs[0].Address(8), request); writeErr == nil {
				acknowledged = append(acknowledged, request)
				movedWrite = movedWrite || revision > initial.AssignmentRevision
				ordinal++
			} else if traffic.Err() == nil && status.Code(writeErr) != codes.Unavailable && status.Code(writeErr) != codes.DeadlineExceeded {
				trafficErr = writeErr
				return
			}
			select {
			case <-traffic.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	defer func() {
		stopTraffic()
		select {
		case <-trafficDone:
		case <-time.After(10 * time.Second):
			err = errors.Join(err, errors.New("traffic cleanup timeout"))
		}
	}()
	if err = start(1); err != nil {
		stopTraffic()
		return result, err
	}
	if err = start(2); err != nil {
		stopTraffic()
		return result, err
	}
	nodes := make([]identity.NodeID, len(configs))
	for i := range configs {
		nodes[i] = configs[i].ServiceStorage.NodeID
	}
	slots := make([]identity.PartitionID, len(configs[0].ServiceStorage.Layout.Partitions))
	for i, partition := range configs[0].ServiceStorage.Layout.Partitions {
		slots[i] = partition.ID
	}
	planned, err := cluster.PlanPlacement(configs[0].ServiceStorage.Layout.Placement, slots, nodes)
	if err != nil {
		stopTraffic()
		return result, fmt.Errorf("final placement plan: %w", err)
	}
	moved, err := waitControl(ctx, store, func(c cluster.Control) bool {
		if c.ActiveMove != "" || c.AssignmentRevision <= initial.AssignmentRevision {
			return false
		}
		for partition, owner := range planned {
			state, ok := c.Partitions[partition]
			if !ok || !state.Ready || state.Desired.Node != owner {
				return false
			}
		}
		return true
	})
	stopTraffic()
	select {
	case <-trafficDone:
	case <-time.After(10 * time.Second):
		return result, errors.New("traffic cleanup timeout")
	}
	if err != nil {
		return result, fmt.Errorf("ownership movement: %w", err)
	}
	if trafficErr != nil {
		return result, fmt.Errorf("write traffic failed: %w", trafficErr)
	}
	if len(acknowledged) <= 1 || !movedWrite {
		return result, errors.New("no write acknowledged while nodes joined and ownership moved")
	}
	// Exercise every ingress with a durable write after the final placement.
	for i := range configs {
		request := queueWrite(10 + i)
		if err = enqueueRecord(ctx, configs[i].Address(8), request); err != nil {
			return result, fmt.Errorf("write through node %d: %w", i+1, err)
		}
		acknowledged = append(acknowledged, request)
	}
	physical, _ := moved.Layout.Resolve("global")
	old := moved.Partitions[physical.ID]
	ownerIndex, ingressIndex := -1, 0
	for i := range configs {
		if configs[i].ServiceStorage.NodeID == old.Desired.Node {
			ownerIndex = i
		}
	}
	if ownerIndex < 0 || old.Desired.Node == initial.Partitions[physical.ID].Desired.Node {
		return result, errors.New("global write partition did not move to a known node")
	}
	if ownerIndex == ingressIndex {
		ingressIndex = 1
	}
	ingress := configs[ingressIndex].Address(8)
	if err = waitMetric(ctx, configs[ingressIndex].DiagnosticsAddress, `kind="forward"`); err != nil {
		return result, err
	}
	if err = processes[ownerIndex].kill(); err != nil {
		return result, err
	}
	processes[ownerIndex] = nil
	// The failed in-flight attempt has an uncertain outcome. Keep its exact
	// envelope for reconciliation once takeover has completed.
	pending := queueWrite(20)
	staleCall, stopStaleCall := context.WithTimeout(ctx, 2*time.Second)
	pendingErr := enqueueRecord(staleCall, ingress, pending)
	stopStaleCall()
	if pendingErr == nil {
		acknowledged = append(acknowledged, pending)
	} else if code := status.Code(pendingErr); code != codes.Unavailable && code != codes.DeadlineExceeded && !errors.Is(pendingErr, context.DeadlineExceeded) {
		return result, fmt.Errorf("unexpected failed-owner write result: %w", pendingErr)
	}
	if err = waitMetric(ctx, configs[ingressIndex].DiagnosticsAddress, `kind="resolve",attempt="1"`); err != nil {
		return result, fmt.Errorf("stale-route refresh was not observed: %w", err)
	}
	taken, err := waitControl(ctx, store, func(c cluster.Control) bool {
		owner := readyOwner(c, "global")
		return owner != "" && owner != old.Desired.Node
	})
	if err != nil {
		return result, fmt.Errorf("owner takeover: %w", err)
	}
	if err = enqueueRecord(ctx, ingress, pending); err != nil {
		return result, fmt.Errorf("reconcile failed-owner write: %w", err)
	}
	if pendingErr != nil {
		acknowledged = append(acknowledged, pending)
	}
	if err = verifyQueueRecords(ctx, ingress, acknowledged, 1000); err != nil {
		return result, fmt.Errorf("acknowledged data after takeover: %w", err)
	}
	configs[ownerIndex].Bootstrap, configs[ownerIndex].ServiceStorage.FreshNamespace = false, false
	if err = start(ownerIndex); err != nil {
		return result, fmt.Errorf("same-address replacement: %w", err)
	}
	replaced, err := waitControl(ctx, store, func(c cluster.Control) bool {
		part := c.Partitions[physical.ID]
		return part.Ready && part.Desired.Node == old.Desired.Node && part.Desired.Incarnation != old.Desired.Incarnation && c.AssignmentRevision > taken.AssignmentRevision
	})
	if err != nil {
		return result, fmt.Errorf("replacement recovery: %w", err)
	}
	if replaced.Partitions[physical.ID].Desired.Address != old.Desired.Address {
		return result, errors.New("replacement did not retain same address")
	}
	// A frozen pre-crash route sends deliberately stale work to the same socket.
	// The production router encodes the full authority tuple and bounded hops.
	stale := queueWrite(30)
	if err = rejectStaleQueueWrite(ctx, moved, stale); err != nil {
		return result, err
	}
	missing, err := queueExecute(ctx, ingress, queueRequest(300, &wire.QueueCommand{Kind: wire.QueueCommand_READ, QueueType: stale.Command.QueueType, FirstId: -1, PageSize: 2}))
	if err != nil || missing.GetError() != wire.QueueResult_NONE || len(missing.GetMessages()) != 0 {
		return result, fmt.Errorf("stale mutation was not absent: result=%v error=%v", missing, err)
	}
	fresh := queueWrite(40)
	if err = enqueueRecord(ctx, ingress, fresh); err != nil {
		return result, fmt.Errorf("new write through replacement: %w", err)
	}
	acknowledged = append(acknowledged, fresh)
	// Replay a pre-crash acknowledged enqueue. The journal must return message
	// zero; executing it again would append message one.
	if err = enqueueRecord(ctx, ingress, acknowledged[0]); err != nil {
		return result, fmt.Errorf("acknowledged replay after restart: %w", err)
	}
	if err = verifyQueueRecords(ctx, ingress, acknowledged, 2000); err != nil {
		return result, fmt.Errorf("acknowledged data after restart: %w", err)
	}
	fmt.Fprintf(diagnostics, "multi-node: verified %d acknowledged records after takeover and restart\n", len(acknowledged))
	result.Assertions = []string{"three nodes accepted durable writes", "ownership moved while writes were acknowledged", "failed-owner write reconciled with its original identity and digest", "killed current owner was taken over with acknowledged data intact", "same-address process acquired a new incarnation", "stale incarnation write rejected with typed STALE_OWNER and absent from storage", "acknowledged write replayed after restart without applying twice", "new durable write and all acknowledged data verified after restart"}
	return result, nil
}

func integrationNodeConfig(bucket, prefix string, ordinal, base int, bootstrap bool) app.Config {
	names := []string{"global", "matching", "history-0", "history-1", "history-2", "history-3", "vis-v1-0", "vis-v1-1", "vis-v1-2", "vis-v1-3"}
	parts := make([]cluster.PhysicalPartition, len(names))
	for i, name := range names {
		parts[i] = cluster.PhysicalPartition{LogicalName: name, ID: identity.PartitionID(fmt.Sprintf("prt_%022d", i+1)), Path: prefix + "/data/" + name}
	}
	return app.Config{Cluster: "integration", Node: fmt.Sprintf("node-%d", ordinal), Bucket: bucket, Prefix: prefix, BindIP: "127.0.0.1", AdvertiseIP: "127.0.0.1", BasePort: base, PublicAddress: fmt.Sprintf("127.0.0.1:%d", base), PublicHTTPAddress: fmt.Sprintf("127.0.0.1:%d", base+9), DiagnosticsAddress: fmt.Sprintf("127.0.0.1:%d", base+10), HistoryShards: 4, Bootstrap: bootstrap, ServiceStorage: &app.ServiceStorageConfig{Format: 2, ClusterID: "clu_0000000000000000000001", NodeID: identity.NodeID(fmt.Sprintf("nod_%022d", ordinal)), Layout: cluster.Layout{Version: 1, Placement: cluster.DefaultPlacementConfig(), Partitions: parts}, FreshNamespace: bootstrap, PollInterval: "50ms", HeartbeatInterval: "100ms", DiscoveryInterval: "50ms", RegistryTimeout: "5s", RenewalInterval: "250ms", SuspectAfter: "1s", MembershipFailureAfter: "500ms", MaxControlBytes: 1 << 20, MaxMembershipBytes: 1 << 20, MaxMembershipEntries: 16, MembershipReadBatch: 4, MaxOutcomes: 1000}}
}

const diagnosticLimit = 1 << 20

type synchronizedBuffer struct {
	sync.Mutex
	bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	n := len(p)
	if len(p) >= diagnosticLimit {
		b.Reset()
		p = p[len(p)-diagnosticLimit:]
	}
	if overflow := b.Len() + len(p) - diagnosticLimit; overflow > 0 {
		b.Next(overflow)
	}
	_, _ = b.Buffer.Write(p)
	return n, nil
}
func (b *synchronizedBuffer) String() string { b.Lock(); defer b.Unlock(); return b.Buffer.String() }

type processLifecycle struct {
	command *exec.Cmd
	done    chan struct{}
	waitErr error
	stopErr error
	once    sync.Once
}

func trackProcess(command *exec.Cmd) *processLifecycle {
	p := &processLifecycle{command: command, done: make(chan struct{})}
	go func() { p.waitErr = command.Wait(); close(p.done) }()
	return p
}
func (p *processLifecycle) stop(kill bool) error {
	p.once.Do(func() { p.stopErr = p.terminate(kill, 10*time.Second) })
	return p.stopErr
}
func (p *processLifecycle) terminate(kill bool, grace time.Duration) error {
	select {
	case <-p.done:
		// The command may have exited on cancellation while children still hold
		// the process group. Retire that group even when the leader is gone.
		killErr := syscall.Kill(-p.command.Process.Pid, syscall.SIGKILL)
		if errors.Is(killErr, syscall.ESRCH) {
			killErr = nil
		}
		return fmt.Errorf("process %d exited before requested shutdown: %w", p.command.Process.Pid, errors.Join(errors.New("unexpected exit"), p.waitErr, killErr))
	default:
	}
	signal := syscall.SIGINT
	if kill {
		signal = syscall.SIGKILL
	}
	signalErr := syscall.Kill(-p.command.Process.Pid, signal)
	if errors.Is(signalErr, syscall.ESRCH) {
		signalErr = nil
	}
	select {
	case <-p.done:
		// Xenon deliberately returns ErrProcessExitRequired after a coordinated
		// shutdown, which maps to a non-zero process status. The process was live
		// when we requested SIGINT and exited within the grace period, so cleanup
		// completed. Unexpected exits are rejected by the pre-signal check above.
		if !kill && signalErr == nil {
			return nil
		}
		var exit *exec.ExitError
		if errors.As(p.waitErr, &exit) {
			if status, ok := exit.Sys().(syscall.WaitStatus); ok && status.Signaled() && status.Signal() == signal {
				return signalErr
			}
		}
		return errors.Join(signalErr, p.waitErr)
	case <-time.After(grace):
		timeoutErr := fmt.Errorf("process %d shutdown timeout after %s", p.command.Process.Pid, grace)
		killErr := syscall.Kill(-p.command.Process.Pid, syscall.SIGKILL)
		if errors.Is(killErr, syscall.ESRCH) {
			killErr = nil
		}
		select {
		case <-p.done:
			return errors.Join(timeoutErr, signalErr, killErr)
		case <-time.After(grace):
			return errors.Join(timeoutErr, signalErr, killErr, errors.New("process did not exit after SIGKILL"))
		}
	}
}

type nodeProcess struct {
	lifecycle   *processLifecycle
	output      *synchronizedBuffer
	directory   string
	evidence    *journeyEvidence
	cleanupOnce sync.Once
	cleanupErr  error
}

func startNode(ctx context.Context, binary string, config app.Config, evidence *journeyEvidence) (*nodeProcess, error) {
	directory, err := os.MkdirTemp("", "xenon-node-")
	if err != nil {
		return nil, err
	}
	path := filepath.Join(directory, "config.json")
	raw, err := json.Marshal(config)
	if err == nil {
		err = os.WriteFile(path, raw, 0600)
	}
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(directory))
	}
	if err = evidence.config(filepath.Base(directory)+"-config", config); err != nil {
		return nil, errors.Join(err, os.RemoveAll(directory))
	}
	buffer := new(synchronizedBuffer)
	command := exec.CommandContext(ctx, binary, "start", "--config", path)
	command.Dir, command.Env, command.Stdout, command.Stderr = directory, os.Environ(), buffer, buffer
	command.SysProcAttr, command.WaitDelay = &syscall.SysProcAttr{Setpgid: true}, time.Second
	if err = command.Start(); err != nil {
		return nil, errors.Join(err, os.RemoveAll(directory))
	}
	return &nodeProcess{lifecycle: trackProcess(command), output: buffer, directory: directory, evidence: evidence}, nil
}
func (p *nodeProcess) kill() error { return p.cleanup(true) }
func (p *nodeProcess) stop() error { return p.cleanup(false) }
func (p *nodeProcess) cleanup(kill bool) error {
	p.cleanupOnce.Do(func() {
		p.cleanupErr = p.lifecycle.stop(kill)
		p.cleanupErr = errors.Join(p.cleanupErr, p.evidence.save(filepath.Base(p.directory)+".log", []byte(p.output.String())))
		// Removing the local state makes subsequent starts true cold starts. The
		// bounded log and exact generated config live in the journey bundle.
		p.cleanupErr = errors.Join(p.cleanupErr, os.RemoveAll(p.directory))
	})
	return p.cleanupErr
}

func reservePortBlocks(count int) ([]int, error) {
	const block = 16
	for base := 20000; base < 60000-count*block; base += 53 {
		listeners := make([]net.Listener, 0, count*11)
		ok := true
		for process := 0; process < count && ok; process++ {
			for offset := 0; offset <= 10; offset++ {
				listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(base+process*block+offset))
				if err != nil {
					ok = false
					break
				}
				listeners = append(listeners, listener)
			}
		}
		for _, listener := range listeners {
			_ = listener.Close()
		}
		if ok {
			bases := make([]int, count)
			for i := range bases {
				bases[i] = base + i*block
			}
			return bases, nil
		}
	}
	return nil, errors.New("no free local port blocks")
}

func waitHTTP(ctx context.Context, url string) error {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	return wait(ctx, func() bool {
		response, err := client.Get(url)
		if err != nil {
			return false
		}
		_ = response.Body.Close()
		return response.StatusCode == http.StatusOK
	})
}
func waitMetric(ctx context.Context, address, fragment string) error {
	client := &http.Client{Timeout: time.Second}
	return wait(ctx, func() bool {
		response, err := client.Get("http://" + address + "/metrics")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		raw, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.Contains(line, fragment) && !strings.HasSuffix(line, " 0") {
				return true
			}
		}
		return false
	})
}
func wait(ctx context.Context, condition func() bool) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if condition() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}
func waitControl(ctx context.Context, store registry.Store, condition func(cluster.Control) bool) (cluster.Control, error) {
	var latest cluster.Control
	err := wait(ctx, func() bool {
		record, err := store.Read(ctx, "cluster/control")
		if err != nil {
			return false
		}
		snapshot, err := cluster.DecodeControl("cluster/control", record, 1<<20)
		if err != nil {
			return false
		}
		latest = snapshot.Control()
		return condition(latest)
	})
	return latest, err
}
func readyOwner(control cluster.Control, logical string) identity.NodeID {
	if control.Layout == nil {
		return ""
	}
	physical, ok := control.Layout.Resolve(logical)
	if !ok {
		return ""
	}
	part := control.Partitions[physical.ID]
	if !part.Ready {
		return ""
	}
	return part.Desired.Node
}
func queueRequest(ordinal int, command *wire.QueueCommand) *wire.QueueRequest {
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	digest := sha256.Sum256(raw)
	return &wire.QueueRequest{ProtocolVersion: 1, Partition: "global", OperationId: fmt.Sprintf("op_%022d", ordinal), CommandSha256: digest[:], Command: command}
}

func queueWrite(ordinal int) *wire.QueueRequest {
	// Private queue types are unused by the embedded Temporal runtime.
	return queueRequest(ordinal, &wire.QueueCommand{Kind: wire.QueueCommand_ENQUEUE, QueueType: int32(1000 + ordinal), Data: []byte(fmt.Sprintf("acknowledged-payload-%d", ordinal)), Encoding: 1})
}

func queueExecute(ctx context.Context, address string, request *wire.QueueRequest) (*wire.QueueResult, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	call, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	return wire.NewQueuePersistenceClient(connection).Execute(call, request)
}

func enqueueRecord(ctx context.Context, address string, request *wire.QueueRequest) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	result, err := queueExecute(ctx, address, request)
	if err != nil {
		return err
	}
	if result.Error != wire.QueueResult_NONE || result.MessageId != 0 {
		return fmt.Errorf("write %s was not applied: %v", request.OperationId, result)
	}
	return nil
}

func verifyQueueRecords(ctx context.Context, address string, acknowledged []*wire.QueueRequest, readBase int) error {
	// Reads also have replay identities: use a fresh range at each checkpoint
	// so restart verification cannot return a journaled pre-restart read.
	for i, request := range acknowledged {
		result, err := queueExecute(ctx, address, queueRequest(readBase+i, &wire.QueueCommand{Kind: wire.QueueCommand_READ, QueueType: request.Command.QueueType, FirstId: -1, PageSize: 2}))
		if err != nil {
			return err
		}
		if err = checkQueueRecord(result, request.Command); err != nil {
			return fmt.Errorf("queue %d: %w", request.Command.QueueType, err)
		}
	}
	return nil
}

func checkQueueRecord(result *wire.QueueResult, command *wire.QueueCommand) error {
	if result.GetError() != wire.QueueResult_NONE || len(result.GetMessages()) != 1 {
		return fmt.Errorf("expected exactly one queued message, got %v", result)
	}
	message := result.Messages[0]
	if message.GetId() != 0 || message.GetQueueType() != command.QueueType || !bytes.Equal(message.GetData(), command.Data) || message.GetEncoding() != "Proto3" {
		return fmt.Errorf("expected exact payload, queue type, encoding and message ID 0, got %v", result)
	}
	return nil
}

// frozenRoute is fault input, not another routing implementation.
type frozenRoute struct{ routing.Route }

func (r frozenRoute) Resolve(context.Context, string, bool) (routing.Route, error) {
	return r.Route, nil
}

func rejectStaleQueueWrite(ctx context.Context, control cluster.Control, request *wire.QueueRequest) error {
	physical, _ := control.Layout.Resolve("global")
	part := control.Partitions[physical.ID]
	digest, err := control.Layout.Digest()
	if err != nil {
		return err
	}
	hint := &routing.ExpectedOwner{Cluster: control.Cluster, LayoutDigest: digest, Partition: physical.ID, Node: part.Desired.Node, Incarnation: part.Desired.Incarnation, AssignmentRevision: part.AssignmentRevision, Reservation: part.Reservation, Generation: part.Generation}
	router := &routing.Router{RequireAuthority: true, Node: "journey-stale-ingress", Directory: frozenRoute{routing.Route{Node: string(part.Desired.Node), Address: part.Desired.Address, Expected: hint}}, Local: func(context.Context, string, proto.Message) (proto.Message, error) {
		return nil, errors.New("unexpected local stale dispatch")
	}}
	defer router.Close()
	call, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	_, err = router.Interceptor(func(string) proto.Message { return new(wire.QueueResult) })(call, request, &grpc.UnaryServerInfo{FullMethod: wire.QueuePersistence_Execute_FullMethodName}, nil)
	return checkStaleOwnerRejection(err)
}

func checkStaleOwnerRejection(err error) error {
	if status.Code(err) == codes.Unavailable {
		for _, detail := range status.Convert(err).Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Domain == "xenon.routing.v1" && info.Reason == "STALE_OWNER" {
				return nil
			}
		}
	}
	return fmt.Errorf("stale incarnation write did not return typed STALE_OWNER: %v", err)
}

func clusterList(ctx context.Context, address string, ordinal int) error {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer connection.Close()
	command := &wire.ClusterCommand{Kind: wire.ClusterCommand_LIST, PageSize: 1}
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	digest := sha256.Sum256(raw)
	call, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	result, err := wire.NewClusterPersistenceClient(connection).Execute(call, &wire.ClusterRequest{ProtocolVersion: 1, Partition: "global", OperationId: fmt.Sprintf("op_%022d", ordinal), CommandSha256: digest[:], Command: command})
	if err != nil {
		return err
	}
	if result.Error != wire.ClusterResult_NONE {
		return fmt.Errorf("cluster operation result %s: %s", result.Error, result.Message)
	}
	return nil
}
