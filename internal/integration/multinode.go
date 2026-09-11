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
	"sync/atomic"
	"time"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/app"
	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
	registrys3 "github.com/0x63616c/xenon/internal/registry/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// MultiNodeOptions contains the one process dependency that differs when this
// journey is invoked from a Go test binary instead of the xenon CLI itself.
type MultiNodeOptions struct{ XenonBinary string }

// RunMultiNodeOwnership proves the production process, S3 authority and gRPC
// routing boundaries. Detailed interleavings stay in DST; this journey is one
// bounded correspondence check.
func RunMultiNodeOwnership(ctx context.Context, diagnostics io.Writer, options MultiNodeOptions) (result JourneyResult, err error) {
	started := time.Now()
	result.Name = "multi-node-ownership"
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
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
	processes := make([]*nodeProcess, 3)
	defer func() {
		for _, process := range processes {
			if process != nil {
				process.stop()
			}
		}
	}()
	start := func(index int) error {
		process, startErr := startNode(ctx, binary, configs[index])
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
	if err = clusterList(ctx, configs[0].Address(8), 1); err != nil {
		return result, fmt.Errorf("initial operation: %w", err)
	}
	traffic, stopTraffic := context.WithCancel(ctx)
	var progress atomic.Uint64
	trafficDone := make(chan struct{})
	go func() {
		defer close(trafficDone)
		for ordinal := 100; traffic.Err() == nil; ordinal++ {
			if clusterList(traffic, configs[0].Address(8), ordinal) == nil {
				progress.Add(1)
			}
			select {
			case <-traffic.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
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
	<-trafficDone
	if err != nil {
		return result, fmt.Errorf("ownership movement: %w", err)
	}
	if progress.Load() == 0 {
		return result, errors.New("no operation completed while nodes joined and ownership moved")
	}
	// Node 1 is deliberately stale ingress after movement. A successful operation
	// and observed forward prove bounded refresh/forwarding through production gRPC.
	if err = clusterList(ctx, configs[0].Address(8), 2); err != nil {
		return result, fmt.Errorf("forward after movement: %w", err)
	}
	if err = waitMetric(ctx, configs[0].DiagnosticsAddress, `kind="forward"`); err != nil {
		return result, err
	}

	oldIncarnation := moved.Partitions[moved.Layout.Partitions[0].ID].Desired.Incarnation
	processes[1].kill()
	processes[1] = nil
	// Before suspicion can publish takeover, ingress still resolves the killed
	// owner. The failed request must exercise the bounded refresh attempt; it is
	// not itself allowed to wait indefinitely for a topology change.
	staleCall, stopStaleCall := context.WithTimeout(ctx, 2*time.Second)
	_ = clusterList(staleCall, configs[0].Address(8), 3)
	stopStaleCall()
	if err = waitMetric(ctx, configs[0].DiagnosticsAddress, `kind="resolve",attempt="1"`); err != nil {
		return result, fmt.Errorf("stale-route refresh was not observed: %w", err)
	}
	taken, err := waitControl(ctx, store, func(c cluster.Control) bool {
		owner := readyOwner(c, "global")
		return owner != "" && owner != configs[1].ServiceStorage.NodeID
	})
	if err != nil {
		return result, fmt.Errorf("owner takeover: %w", err)
	}
	if err = clusterList(ctx, configs[0].Address(8), 4); err != nil {
		return result, fmt.Errorf("progress after kill: %w", err)
	}
	configs[1].Bootstrap, configs[1].ServiceStorage.FreshNamespace = false, false
	if err = start(1); err != nil {
		return result, fmt.Errorf("same-address replacement: %w", err)
	}
	replaced, err := waitControl(ctx, store, func(c cluster.Control) bool {
		id := c.Layout.Partitions[0].ID
		part := c.Partitions[id]
		return part.Ready && part.Desired.Node == configs[1].ServiceStorage.NodeID && part.Desired.Incarnation != oldIncarnation && c.AssignmentRevision > taken.AssignmentRevision
	})
	if err != nil {
		return result, fmt.Errorf("replacement recovery: %w", err)
	}
	if replaced.Partitions[replaced.Layout.Partitions[0].ID].Desired.Address != configs[1].Address(8) {
		return result, errors.New("replacement did not retain same address")
	}
	if err = clusterList(ctx, configs[0].Address(8), 5); err != nil {
		return result, fmt.Errorf("progress through replacement: %w", err)
	}
	result.Assertions = []string{"three nodes joined", "ownership moved while operations progressed", "stale ingress forwarded and refreshed within bounded routing", "killed owner was taken over", "same-address process acquired a new incarnation", "new progress succeeded"}
	result.Duration = time.Since(started)
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

type synchronizedBuffer struct {
	sync.Mutex
	bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	if b.Len() > 1<<20 {
		return len(p), nil
	}
	return b.Buffer.Write(p)
}
func (b *synchronizedBuffer) String() string { b.Lock(); defer b.Unlock(); return b.Buffer.String() }

type nodeProcess struct {
	command *exec.Cmd
	output  *synchronizedBuffer
	done    chan struct{}
	once    sync.Once
}

func startNode(ctx context.Context, binary string, config app.Config) (*nodeProcess, error) {
	directory, err := os.MkdirTemp("", "xenon-node-")
	if err != nil {
		return nil, err
	}
	path := filepath.Join(directory, "config.json")
	raw, _ := json.Marshal(config)
	if err = os.WriteFile(path, raw, 0600); err != nil {
		_ = os.RemoveAll(directory)
		return nil, err
	}
	buffer := new(synchronizedBuffer)
	command := exec.CommandContext(ctx, binary, "start", "--config", path)
	command.Dir, command.Env, command.Stdout, command.Stderr = directory, os.Environ(), buffer, buffer
	if err = command.Start(); err != nil {
		_ = os.RemoveAll(directory)
		return nil, err
	}
	process := &nodeProcess{command: command, output: buffer, done: make(chan struct{})}
	go func() {
		_ = command.Wait()
		_ = os.RemoveAll(directory)
		close(process.done)
	}()
	return process, nil
}
func (p *nodeProcess) kill() {
	p.once.Do(func() {
		if p.command.Process != nil {
			_ = p.command.Process.Kill()
		}
		p.wait()
	})
}
func (p *nodeProcess) stop() {
	p.once.Do(func() {
		if p.command.Process != nil {
			_ = p.command.Process.Signal(os.Interrupt)
			time.Sleep(200 * time.Millisecond)
			_ = p.command.Process.Kill()
		}
		p.wait()
	})
}

func (p *nodeProcess) wait() {
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
	}
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
