//go:build integration_s3

package ownership

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	native "slatedb.io/slatedb-go/uniffi"
	"strings"
	"testing"
	"time"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/directory"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func TestS3OwnerManager(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	store, e := Environment()
	if e != nil {
		t.Fatal(e)
	}
	client := store.client.(*s3.Client)
	if _, e = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(store.bucket)}); e != nil {
		t.Fatal(e)
	}
	topologyAssertions(t, ctx, store)
	binary := os.Getenv("XENON_GO_NODE_BINARY")
	if binary == "" {
		t.Fatal("built binary required")
	}
	launch := func(id string) (directory.Identity, func()) {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary)
		cmd.Env = append(os.Environ(), "XENON_NODE="+id, "XENON_LISTEN=127.0.0.1:0")
		stdout, e := cmd.StdoutPipe()
		if e != nil {
			t.Fatal(e)
		}
		cmd.Stderr = os.Stderr
		if e = cmd.Start(); e != nil {
			t.Fatal(e)
		}
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		stop := func() { _ = cmd.Process.Kill(); <-done }
		t.Cleanup(stop)
		lines := make(chan string, 1)
		go func() {
			scan := bufio.NewScanner(stdout)
			if scan.Scan() {
				lines <- scan.Text()
			}
		}()
		select {
		case line := <-lines:
			fields := strings.Fields(line)
			if len(fields) != 4 || fields[0] != "INGRESS" {
				t.Fatal(line)
			}
			return directory.Identity{Node: fields[1], Incarnation: fields[2], Address: fields[3]}, stop
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		return directory.Identity{}, stop
	}
	a, killA := launch("a")
	b, _ := launch("b")
	members := map[string]Member{"a": {Address: a.Address, Incarnation: a.Incarnation}, "b": {Address: b.Address, Incarnation: b.Incarnation}}
	desired := Topology{Members: members, Partitions: map[string]Assignment{"p": {"a", "data/p"}, "q": {"b", "data/q"}}}
	snap, e := store.Publish(ctx, nil, desired)
	if e != nil {
		t.Fatal(e)
	}
	invoke := func(address, partition, id string, kind wire.ShardCommand_Kind, data []byte) (*wire.ShardResult, error) {
		conn, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if e != nil {
			return nil, e
		}
		defer conn.Close()
		c := &wire.ShardCommand{Kind: kind, ShardId: 1, RangeId: 7, Data: data, Encoding: 1}
		encoded, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		digest := sha256.Sum256(encoded)
		req := &wire.ShardRequest{ProtocolVersion: 1, Partition: partition, OperationId: id, CommandSha256: digest[:], Command: c}
		deadline, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		return wire.NewShardPersistenceClient(conn).Execute(deadline, req)
	}
	eventually := func(f func() bool) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for !f() {
			if time.Now().After(deadline) || ctx.Err() != nil {
				t.Fatal("progress deadline")
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	payload := []byte{0, 255, 1, 128}
	eventually(func() bool {
		r, e := invoke(b.Address, "p", "create-p", wire.ShardCommand_CREATE_OR_GET, payload)
		return e == nil && string(r.Data) == string(payload)
	})
	eventually(func() bool {
		r, e := invoke(a.Address, "q", "create-q", wire.ShardCommand_CREATE_OR_GET, payload)
		return e == nil && string(r.Data) == string(payload)
	})
	// Same member ID replacement has a new incarnation and cannot acquire until
	// explicit activation. Old ingress must forward to that new incarnation.
	replacement, killReplacement := launch("a")
	if replacement.Incarnation == a.Incarnation {
		t.Fatal("reused incarnation")
	}
	time.Sleep(400 * time.Millisecond)
	pdir, _ := directory.New(store.client, store.bucket, store.prefix+"/owners", "p", "data/p")
	observed, e := pdir.Read(ctx)
	if e != nil || observed.Record().Incarnation != a.Incarnation {
		t.Fatal("unactivated same-member process acquired", e)
	}
	desired.Members["a"] = Member{Address: replacement.Address, Incarnation: replacement.Incarnation}
	desired.Partitions["p"] = Assignment{"b", "data/p"}
	snap, e = store.Publish(ctx, &snap, desired)
	if e != nil {
		t.Fatal(e)
	}
	eventually(func() bool {
		r, e := invoke(a.Address, "p", "create-p", wire.ShardCommand_CREATE_OR_GET, payload)
		return e == nil && string(r.Data) == string(payload)
	})
	desired.Partitions["p"] = Assignment{"a", "data/p"}
	snap, e = store.Publish(ctx, &snap, desired)
	if e != nil {
		t.Fatal(e)
	}
	eventually(func() bool {
		r, e := invoke(a.Address, "p", "create-p", wire.ShardCommand_CREATE_OR_GET, payload)
		return e == nil && string(r.Data) == string(payload)
	})
	killA() // killed process was the withdrawn incarnation; replacement stays live.
	// Move q to replacement, kill current b only in later crash schedule. This
	// asserts one process can simultaneously own p and q after a topology move.
	desired.Partitions["q"] = Assignment{"a", "data/q"}
	snap, e = store.Publish(ctx, &snap, desired)
	if e != nil {
		t.Fatal(e)
	}
	eventually(func() bool {
		r, e := invoke(b.Address, "q", "create-q", wire.ShardCommand_CREATE_OR_GET, payload)
		return e == nil && string(r.Data) == string(payload)
	})
	// Kill the active owner of both partitions, explicitly activate a fresh process,
	// and replay acknowledged operations through the other ingress.
	killReplacement()
	restarted, _ := launch("a")
	desired.Members["a"] = Member{Address: restarted.Address, Incarnation: restarted.Incarnation}
	snap, e = store.Publish(ctx, &snap, desired)
	if e != nil {
		t.Fatal(e)
	}
	for _, partition := range []string{"p", "q"} {
		eventually(func() bool {
			r, e := invoke(b.Address, partition, "create-"+partition, wire.ShardCommand_CREATE_OR_GET, payload)
			return e == nil && string(r.Data) == string(payload)
		})
	}
	// Native delayed contenders use the same S3 prefix and real Build, with exact
	// local fault hooks. Automatic reconcilers are intentionally absent here.
	makeManager := func(id string) *Manager {
		m, e := NewManager(store, id, "127.0.0.1:1", "s3://"+store.bucket, 200000)
		if e != nil {
			t.Fatal(e)
		}
		return m
	}
	d := makeManager("d")
	old := makeManager("old")
	late := makeManager("late")
	activate := func(m *Manager) {
		t.Helper()
		identity := m.Identity()
		desired.Members[identity.Node] = Member{Address: identity.Address, Incarnation: identity.Incarnation}
		desired.Partitions["r"] = Assignment{identity.Node, "data/r"}
		var e error
		snap, e = store.Publish(ctx, &snap, desired)
		if e != nil {
			t.Fatal(e)
		}
	}
	activate(old)
	if e = old.reconcile(ctx, "r"); e != nil {
		t.Fatal(e)
	}
	oldOwner, e := old.Owner("r")
	if e != nil {
		t.Fatal(e)
	}
	persist := func(m *Manager) {
		t.Helper()
		owner, e := m.Owner("r")
		if e != nil {
			t.Fatal(e)
		}
		command := &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: 9, RangeId: 3, Data: payload, Encoding: 1}
		raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(command)
		digest := sha256.Sum256(raw)
		r, e := owner.Execute(ctx, &wire.ShardRequest{ProtocolVersion: 1, Partition: "r", OperationId: "delayed-ack", CommandSha256: digest[:], Command: command})
		if e != nil || string(r.Data) != string(payload) {
			t.Fatal("acknowledged state/replay lost", e, r)
		}
	}
	persist(old)
	// Old result is captured after authority check but before durable barrier.
	captured := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, e := oldOwner.Run(ctx, func(db *native.Db) ([]byte, error) {
			raw, err := db.Get([]byte("v1/shard/0000000009"))
			close(captured)
			<-release
			if err != nil {
				return nil, err
			}
			if raw == nil {
				return nil, fmt.Errorf("missing captured acknowledged shard")
			}
			return *raw, nil
		})
		result <- e
	}()
	<-captured
	activate(d)
	if e = d.reconcile(ctx, "r"); e != nil {
		t.Fatal(e)
	}
	close(release)
	if e = <-result; e == nil {
		t.Fatal("stale read barrier succeeded")
	}

	persist(d)
	for i := 0; i < 2; i++ {
		late = makeManager(fmt.Sprintf("late-%d", i))
		activate(late)
		paused, resume := make(chan struct{}), make(chan struct{})
		late.beforeOpen = func(directory.Record) { close(paused); <-resume }
		done := make(chan error, 1)
		go func() { done <- late.reconcile(ctx, "r") }()
		<-paused
		activate(d)
		if e = d.reconcile(ctx, "r"); e != nil {
			t.Fatal(e)
		}
		close(resume)
		if e = <-done; !errors.Is(e, directory.ErrConflict) {
			t.Fatalf("late attempt %d: %v", i, e)
		}
		late.beforeOpen = nil
		owner, e := d.Owner("r")
		if e != nil {
			t.Fatal(e)
		}
		if _, e = owner.Run(ctx, func(_ *native.Db) ([]byte, error) { return []byte("captured"), nil }); e == nil {
			t.Fatal("delayed opener did not fence durable read")
		}
		if e = d.reconcile(ctx, "r"); e != nil {
			t.Fatal(e)
		}
		owner, e = d.Owner("r")
		if e != nil {
			t.Fatal(e)
		}
		if _, e = owner.Run(ctx, func(_ *native.Db) ([]byte, error) { return []byte("fresh"), nil }); e != nil {
			t.Fatal(e)
		}
		persist(d)
	}
	// A topology-only change after the check can transiently publish READY; the
	// actual shared admission still rejects it. Then the designated owner recovers.
	activate(late)
	late.beforeReady = func(directory.Record) { activate(d) }
	if e = late.reconcile(ctx, "r"); e != nil {
		t.Fatal(e)
	}
	late.beforeReady = nil
	stale, e := late.Owner("r")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = stale.Run(ctx, func(_ *native.Db) ([]byte, error) { t.Error("obsolete topology admitted callback"); return nil, nil }); e == nil {
		t.Fatal("obsolete topology served")
	}
	eventually(func() bool { return d.reconcile(ctx, "r") == nil })
	persist(d)
	for _, m := range []*Manager{old, late, d} {
		m.retireAll()
		for _, o := range m.owners {
			_ = o.owner.Close(ctx)
		}
	}
	fmt.Println("owner manager move, crash, topology seam and finite delayed opener assertions passed")
}

// loseTopologyResponse injects failure only after the actual S3 mutation commits.
type loseTopologyResponse struct {
	directory.S3
	lose bool
}

func (s *loseTopologyResponse) PutObject(ctx context.Context, in *s3.PutObjectInput, options ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	out, e := s.S3.PutObject(ctx, in, options...)
	if e == nil && s.lose {
		s.lose = false
		return nil, fmt.Errorf("declared postcommit response loss")
	}
	return out, e
}
func topologyAssertions(t *testing.T, ctx context.Context, base *TopologyStore) {
	t.Helper()
	loss := &loseTopologyResponse{S3: base.client, lose: true}
	store, e := NewTopologyStore(loss, base.bucket, "topology-faults")
	if e != nil {
		t.Fatal(e)
	}
	cfg := Topology{Members: map[string]Member{"a": {Address: "127.0.0.1:1", Incarnation: "00000000-0000-4000-8000-000000000001"}}, Partitions: map[string]Assignment{"p": {"a", "fault-data/p"}}}
	first, e := store.Publish(ctx, nil, cfg)
	if e != nil || first.data.Revision != 1 {
		t.Fatal("lost response not reconciled", e)
	}
	if _, e = store.Publish(ctx, nil, cfg); !errors.Is(e, directory.ErrConflict) {
		t.Fatal("create-only overwritten", e)
	}
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, e := store.Publish(ctx, &first, cfg); results <- e }()
	}
	success, conflict := 0, 0
	for i := 0; i < 2; i++ {
		e := <-results
		if e == nil {
			success++
		} else if errors.Is(e, directory.ErrConflict) {
			conflict++
		} else {
			t.Fatal(e)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatal("conditional topology race", success, conflict)
	}
	latest, e := store.Read(ctx)
	if e != nil || latest.data.Revision != 2 || latest.data.Transition == first.data.Transition {
		t.Fatal("topology ABA", e)
	}
	bad := latest.Record()
	bad.Partitions["p"] = Assignment{"a", "different-data"}
	if _, e = store.Publish(ctx, &latest, bad); !errors.Is(e, directory.ErrInvalid) {
		t.Fatal("data prefix rebound", e)
	}
	bad = latest.Record()
	bad.Partitions["q"] = Assignment{"a", "fault-data/p/nested"}
	if _, e = store.Publish(ctx, &latest, bad); !errors.Is(e, directory.ErrInvalid) {
		t.Fatal("overlapping partitions accepted", e)
	}
	// Record returns copies: a caller cannot mutate a private observed CAS value.
	if latest.data.Partitions["p"].DataPrefix != "fault-data/p" || len(latest.data.Partitions) != 1 {
		t.Fatal("mutable snapshot escaped")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e = store.Read(canceled); e == nil {
		t.Fatal("canceled metadata read succeeded")
	}
}
