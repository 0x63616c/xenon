package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type devFake struct {
	calls      int
	containers map[string]devFakeContainer
	volume     *devFakeVolume
	sequence   int
}
type devFakeContainer struct {
	ID     string `json:"Id"`
	Name   string
	Config struct {
		Image  string
		Labels map[string]string
	}
	Running bool
}
type devFakeVolume struct {
	Name, CreatedAt string
	Labels          map[string]string
}

func newDevFake() *devFake { return &devFake{containers: map[string]devFakeContainer{}} }
func (f *devFake) call(ctx context.Context, args ...string) ([]byte, error) {
	f.calls++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	encode := func(v any) ([]byte, error) { return json.Marshal(v) }
	labels := func() map[string]string {
		out := map[string]string{}
		for i, a := range args {
			if a == "--label" {
				pair := strings.SplitN(args[i+1], "=", 2)
				out[pair[0]] = pair[1]
			}
		}
		return out
	}
	switch args[0] {
	case "ps":
		var ids []string
		filter := strings.TrimPrefix(args[len(args)-1], "label=io.xenon.dev.run=")
		for id, c := range f.containers {
			if c.Config.Labels["io.xenon.dev.run"] == filter {
				ids = append(ids, id)
			}
		}
		return []byte(strings.Join(ids, "\n")), nil
	case "inspect":
		c, ok := f.containers[args[1]]
		if !ok {
			return nil, fmt.Errorf("not found")
		}
		return encode([]devFakeContainer{c})
	case "create":
		f.sequence++
		id := fmt.Sprintf("%064x", f.sequence)
		c := devFakeContainer{ID: id}
		c.Config.Labels = labels()
		for i, a := range args {
			if a == "--name" {
				c.Name = "/" + args[i+1]
			}
			if devImage.MatchString(a) {
				c.Config.Image = a
				break
			}
		}
		f.containers[id] = c
		return []byte(id), nil
	case "start", "stop":
		c := f.containers[args[len(args)-1]]
		c.Running = args[0] == "start"
		f.containers[c.ID] = c
		return nil, nil
	case "logs":
		return []byte("retained shutdown log\n"), nil
	case "rm":
		delete(f.containers, args[1])
		return nil, nil
	case "volume":
		switch args[1] {
		case "ls":
			if f.volume == nil {
				return nil, nil
			}
			return []byte(f.volume.Name), nil
		case "create":
			f.volume = &devFakeVolume{Name: args[len(args)-1], CreatedAt: "2026-09-06T00:00:00Z", Labels: labels()}
			return []byte(f.volume.Name), nil
		case "inspect":
			if f.volume == nil {
				return nil, fmt.Errorf("not found")
			}
			return encode([]devFakeVolume{*f.volume})
		case "logs":
			return []byte("retained shutdown log\n"), nil
		case "rm":
			f.volume = nil
			return nil, nil
		}
	}
	return nil, fmt.Errorf("unexpected command %v", args)
}
func devTestFixture(t *testing.T) (string, string, devPlan) {
	t.Helper()
	dir := t.TempDir()
	fixture := devPlan{Schema: 1, Containers: []devContainer{{Role: "s3", Image: "sha256:" + strings.Repeat("a", 64), Args: []string{"server", "/data"}, NetworkHolder: true, Objects: true, Ports: []string{"127.0.0.1:30006:9000"}}, {Role: "agent-a", Image: "sha256:" + strings.Repeat("b", 64), Args: []string{"start"}}}}
	raw, _ := json.Marshal(fixture)
	path := filepath.Join(dir, "fixture.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path, filepath.Join(dir, "state"), fixture
}
func devTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestDevOwnedLifecyclePreservesVolumeAndSentinel(t *testing.T) {
	path, state, _ := devTestFixture(t)
	fake := newDevFake()
	sentinel := devFakeContainer{ID: strings.Repeat("f", 64), Name: "/sentinel"}
	sentinel.Config.Labels = map[string]string{"io.xenon.dev.run": "unrelated"}
	fake.containers[sentinel.ID] = sentinel
	call := func(action string, ephemeral bool) DevResult {
		t.Helper()
		result, err := runDev(devTestContext(t), action, path, state, ephemeral, fake.call)
		if err != nil {
			t.Fatal(result, err)
		}
		return result
	}
	first := call("up", false)
	if !strings.HasPrefix(first.Run, "dev_") {
		t.Fatal("new fixture lacks typed ID", first.Run)
	}
	if first.Status != "started" || len(fake.containers) != 3 || fake.volume == nil {
		t.Fatal(first, fake)
	}
	volume := *fake.volume
	stopped := call("down", false)
	if !stopped.CleanupVerified || len(fake.containers) != 1 || fake.volume == nil {
		t.Fatal(stopped, fake)
	}
	logs, err := filepath.Glob(filepath.Join(state, "*.log"))
	if err != nil || len(logs) != 2 {
		t.Fatal("shutdown evidence missing", logs, err)
	}
	call("down", false)
	second := call("up", false)
	if second.Run != first.Run || fake.volume.Name != volume.Name || fake.volume.CreatedAt != volume.CreatedAt {
		t.Fatal("lost preserved identity")
	}
	call("down", true)
	if fake.volume != nil || len(fake.containers) != 1 || fake.containers[sentinel.ID].Name != "/sentinel" {
		t.Fatal("sentinel/data cleanup mismatch")
	}
	call("down", false)
	if _, err := runDev(devTestContext(t), "up", path, state, false, fake.call); err == nil {
		t.Fatal("retired fixture reused")
	}
}
func TestDevValidationBeforeDocker(t *testing.T) {
	path, state, fixture := devTestFixture(t)
	fixture.Containers[0].Image = "latest"
	raw, _ := json.Marshal(fixture)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	fake := newDevFake()
	if _, err := runDev(devTestContext(t), "up", path, state, false, fake.call); err == nil || fake.calls != 0 {
		t.Fatal(err, fake.calls)
	}
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatal("invalid fixture created state")
	}
}
func TestDevRefusesConflictingContainerAndMissingPreservedVolume(t *testing.T) {
	for _, mode := range []string{"labels", "replacement", "missing volume"} {
		t.Run(mode, func(t *testing.T) {
			path, state, _ := devTestFixture(t)
			fake := newDevFake()
			if _, err := runDev(devTestContext(t), "up", path, state, false, fake.call); err != nil {
				t.Fatal(err)
			}
			count := len(fake.containers)
			for id, c := range fake.containers {
				if mode == "labels" {
					c.Config.Labels["io.xenon.dev.fixture"] = "other"
					fake.containers[id] = c
					break
				}
				if mode == "replacement" {
					delete(fake.containers, id)
					c.ID = strings.Repeat("e", 64)
					fake.containers[c.ID] = c
					break
				}
			}
			action := "down"
			if mode == "missing volume" {
				fake.volume = nil
				action = "up"
			}
			result, err := runDev(devTestContext(t), action, path, state, false, fake.call)
			if err == nil || result.CleanupVerified || len(fake.containers) != count {
				t.Fatal(result, err)
			}
		})
	}
}
func TestDevAdoptsCreateBeforeRecordCrash(t *testing.T) {
	path, state, _ := devTestFixture(t)
	fake := newDevFake()
	if _, err := runDev(devTestContext(t), "up", path, state, false, fake.call); err != nil {
		t.Fatal(err)
	}
	var saved devState
	if err := readDevJSON(filepath.Join(state, "state.json"), &saved); err != nil {
		t.Fatal(err)
	}
	saved.Resources = nil
	if err := saveDevState(state, saved); err != nil {
		t.Fatal(err)
	}
	result, err := runDev(devTestContext(t), "down", path, state, false, fake.call)
	if err != nil || !result.CleanupVerified || len(fake.containers) != 0 {
		t.Fatal(result, err)
	}
}

func runDev(ctx context.Context, action, fixturePath, dir string, ephemeral bool, docker devDocker) (DevResult, error) {
	var fixture devPlan
	if action == "up" {
		if err := readDevJSON(fixturePath, &fixture); err != nil {
			return DevResult{Schema: 1, Status: "invalid"}, err
		}
	}
	return runDevPlan(ctx, action, fixture, dir, ephemeral, docker, devHooks{})
}

func TestDevConstrainedThreeNodePlan(t *testing.T) {
	fixture := DevFixture{Schema: 1, Image: "sha256:" + strings.Repeat("b", 64), S3Port: 30006, TemporalPort: 30233, HTTPPort: 30243, StoragePort: 30935, DiagnosticsPorts: [3]int{30250, 31250, 32250}}
	plan, err := fixture.plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Containers) != 9 || plan.Containers[0].Image != devMinioImage {
		t.Fatal("lost pinned topology")
	}
	nodes := 0
	for _, container := range plan.Containers {
		if container.Role == "omes-worker" && (len(container.Args) < 2 || container.Args[0] != "worker" || container.Args[1] != "--err-on-unimplemented") {
			t.Fatal("pinned Omes worker subcommand or strict capability validation missing")
		}
		if container.Config != nil {
			nodes++
			if err := ValidateServiceLayout(*container.Config); err != nil {
				t.Fatal(err)
			}
			if container.Config.Prefix != "agent" {
				t.Fatal("changed preserved prefix")
			}
		}
	}
	if nodes != 3 {
		t.Fatal(nodes)
	}
	fixture.DiagnosticsPorts[2] = fixture.S3Port
	if _, err := fixture.plan(); err == nil {
		t.Fatal("duplicate host port accepted")
	}
}

func TestDevCleanupDeadlineRetainsPendingOwnership(t *testing.T) {
	path, state, _ := devTestFixture(t)
	fake := newDevFake()
	if _, err := runDev(devTestContext(t), "up", path, state, false, fake.call); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := runDev(ctx, "down", path, state, false, func(actual context.Context, args ...string) ([]byte, error) {
		if actual != ctx {
			t.Fatal("cleanup renewed its context")
		}
		if args[0] == "stop" {
			cancel()
			<-actual.Done()
			return nil, actual.Err()
		}
		return fake.call(actual, args...)
	})
	if err == nil || result.CleanupVerified || len(result.Pending) != 2 || len(fake.containers) != 2 {
		t.Fatal(result, err)
	}
}

func TestDevInitializationRecordedOnlyAfterReadyAndDisablesBootstrap(t *testing.T) {
	_, dir, plan := devTestFixture(t)
	config := serviceConfig()
	plan.Containers[1].Config = &config
	fake := newDevFake()
	ready := false
	sawInitialized := false
	hooks := devHooks{
		Started: func(_ context.Context, role, _ string, state devState) error {
			if role == "agent-a" {
				sawInitialized = state.Initialized
			}
			return nil
		},
		Identity: func(context.Context) (string, error) { return "trn_0000000000000000000001", nil },
		Ready: func(context.Context) error {
			if !ready {
				return fmt.Errorf("worker not ready")
			}
			return nil
		},
	}
	if _, err := runDevPlan(devTestContext(t), "up", plan, dir, false, fake.call, hooks); err == nil {
		t.Fatal("first startup unexpectedly ready")
	}
	var state devState
	if err := readDevJSON(filepath.Join(dir, "state.json"), &state); err != nil || state.Initialized || state.Initialization != "" {
		t.Fatal(state, err)
	}
	ready = true
	if _, err := runDevPlan(devTestContext(t), "up", plan, dir, false, fake.call, hooks); err != nil || sawInitialized {
		t.Fatal("first failed startup not resumable", err)
	}
	if err := readDevJSON(filepath.Join(dir, "state.json"), &state); err != nil || !state.Initialized || state.Initialization == "" {
		t.Fatal(state, err)
	}
	// Existing containers are reused, so their mounted configuration must also be rewritten.
	if _, err := runDevPlan(devTestContext(t), "up", plan, dir, false, fake.call, hooks); err != nil || !sawInitialized {
		t.Fatal("initialization forgotten", err)
	}
	var saved Config
	if err := readDevJSON(filepath.Join(dir, "agent-a.json"), &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Bootstrap || saved.ServiceStorage.FreshNamespace {
		t.Fatal("preserved restart can bootstrap empty storage")
	}
	if !plan.Containers[1].Config.Bootstrap || !plan.Containers[1].Config.ServiceStorage.FreshNamespace {
		t.Fatal("fixture identity mutated")
	}
}

func TestDevLegacyRunOwnershipRemainsReadable(t *testing.T) {
	path, dir, plan := devTestFixture(t)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	legacy := strings.Repeat("a", 32)
	if err := saveDevState(dir, devState{Schema: 1, Run: legacy, Hash: fixtureHash(plan), Fixture: plan}); err != nil {
		t.Fatal(err)
	}
	fake := newDevFake()
	for _, action := range []string{"up", "down"} {
		got, err := runDev(devTestContext(t), action, path, dir, false, fake.call)
		if err != nil || got.Run != legacy {
			t.Fatal("legacy identity rewritten or rejected", got, err)
		}
	}
}
