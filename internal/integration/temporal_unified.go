package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/0x63616c/xenon/internal/app"
	"github.com/0x63616c/xenon/internal/cluster"
	registrys3 "github.com/0x63616c/xenon/internal/registry/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type TemporalOptions struct {
	XenonBinary string
}

func RunTemporalCompatibility(ctx context.Context, diagnostics io.Writer, options TemporalOptions) (result JourneyResult, err error) {
	ctx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	evidence, err := beginJourney(ctx, &result, "temporal-compatibility", diagnostics)
	defer evidence.finish(ctx, &err)
	if err != nil {
		return result, err
	}
	diagnostics = evidence.writer()
	root, err := repositoryRoot(ctx)
	if err != nil {
		return result, err
	}
	var fixture temporalCase
	if err = decodeFile(filepath.Join(root, "test/scenarios/ministack/case.json"), &fixture); err != nil {
		return result, err
	}
	if fixture.Schema != 1 || fixture.HistoryShards != 4 || len(fixture.Partitions) != 10 {
		return result, errors.New("unsupported ministack fixture")
	}
	if options.XenonBinary == "" {
		return result, errors.New("Xenon binary is required")
	}
	binary := options.XenonBinary
	journeyDirectory := evidence.directory
	if err = evidence.config("fixture", fixture); err != nil {
		return result, err
	}
	if err = evidence.file("xenon-binary", binary); err != nil {
		return result, err
	}
	composeFile, err := prepareUnifiedIngress(root, journeyDirectory)
	if err != nil {
		return result, err
	}
	if err = evidence.file("generated-compose", composeFile); err != nil {
		return result, err
	}
	if err = evidence.file("generated-haproxy", filepath.Join(journeyDirectory, "haproxy.cfg")); err != nil {
		return result, err
	}
	bin := filepath.Join(root, ".local/bin")
	if err = os.MkdirAll(bin, 0700); err != nil {
		return result, err
	}
	fmt.Fprintln(diagnostics, "Preparing pinned Temporal SDK workload")
	if _, err = runOutput(ctx, root, os.Environ(), "go", "build", "-o", filepath.Join(bin, "xenon-sdk-probe"), "./cmd/xenon-sdk-probe"); err != nil {
		return result, err
	}
	if err = evidence.file("sdk-probe-binary", filepath.Join(bin, "xenon-sdk-probe")); err != nil {
		return result, err
	}
	project := "xenon-temporal-" + randomHex(6)
	compose := []string{"compose", "--project-name", project, "-f", composeFile}
	defer func() {
		c, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		logCtx, stopLogs := context.WithTimeout(context.Background(), 5*time.Second)
		logs, captureErr := dockerOutput(logCtx, append(compose, "logs", "--tail", "200")...)
		stopLogs()
		fmt.Fprintf(diagnostics, "Compose logs (capture error: %v):\n%s\n", captureErr, logs)
		_, e := dockerOutput(c, append(compose, "down", "--volumes", "--remove-orphans")...)
		err = errors.Join(err, e)
	}()
	if _, err = dockerOutput(ctx, append(compose, "up", "-d", "s3", "ingress")...); err != nil {
		return result, err
	}
	if err = waitHTTP(ctx, "http://127.0.0.1:19006/minio/health/live"); err != nil {
		return result, err
	}
	restore := setEnvironment(map[string]string{"AWS_ACCESS_KEY_ID": "xenon-local", "AWS_SECRET_ACCESS_KEY": "xenon-local-test-only", "AWS_DEFAULT_REGION": "us-east-1", "AWS_REGION": "us-east-1", "AWS_ENDPOINT": "http://127.0.0.1:19006", "AWS_ALLOW_HTTP": "true", "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false", "AWS_EC2_METADATA_DISABLED": "true"})
	defer restore()
	bucket, prefix := "xenon-temporal-"+randomHex(8), "compatibility"
	client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String("http://127.0.0.1:19006"), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider("xenon-local", "xenon-local-test-only", "")})
	if _, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return result, err
	}
	store, err := registrys3.New(client, bucket, prefix+"/metadata/registry")
	if err != nil {
		return result, err
	}
	configs := []app.Config{integrationNodeConfig(bucket, prefix, 1, 18233, true), integrationNodeConfig(bucket, prefix, 2, 19233, false)}
	for i := range configs {
		configs[i].PublicAddress = "127.0.0.1:17233"
		configs[i].PublicHTTPAddress = "127.0.0.1:17243"
	}
	if err = evidence.config("nodes", configs); err != nil {
		return result, err
	}
	processes := make([]*nodeProcess, 2)
	defer func() {
		for _, p := range processes {
			if p != nil {
				err = errors.Join(err, p.kill())
			}
		}
	}()
	start := func(i int) error {
		p, e := startNode(ctx, binary, configs[i], evidence)
		if e != nil {
			return e
		}
		processes[i] = p
		readyCtx, cancelReady := context.WithTimeout(ctx, 90*time.Second)
		defer cancelReady()
		if e = waitHTTP(readyCtx, "http://"+configs[i].DiagnosticsAddress+"/readyz"); e != nil {
			return fmt.Errorf("node %d readiness: %w\n%s", i+1, e, p.output.String())
		}
		return nil
	}
	if err = start(0); err != nil {
		return result, err
	}
	if err = start(1); err != nil {
		return result, err
	}
	fmt.Fprintln(diagnostics, "Two unified Xenon processes are ready behind ingress")
	env := os.Environ()
	fmt.Fprintln(diagnostics, "Checking Temporal ingress and bootstrapping the namespace")
	if err = probeUntil(ctx, root, env, bin, fixture, "health"); err != nil {
		return result, fmt.Errorf("Temporal health: %w", err)
	}
	raw, err := probe(ctx, root, env, bin, fixture, "bootstrap")
	if err != nil {
		return result, fmt.Errorf("Temporal bootstrap: %w", err)
	}
	var bootstrap struct {
		HistoryPartition string `json:"history_partition"`
	}
	if json.Unmarshal(raw, &bootstrap) != nil {
		return result, errors.New("invalid bootstrap")
	}
	if _, err = probe(ctx, root, env, bin, fixture, "fuzz-endpoint"); err != nil {
		return result, fmt.Errorf("Nexus endpoint registration: %w", err)
	}
	if _, err = probe(ctx, root, env, bin, fixture, "fuzz-endpoint-ready"); err != nil {
		return result, fmt.Errorf("Nexus endpoint readiness: %w", err)
	}
	fmt.Fprintln(diagnostics, "Temporal namespace and Nexus endpoint are ready")
	worker, err := startOwned(ctx, root, filepath.Join(journeyDirectory, "worker.log"), env, filepath.Join(bin, "xenon-sdk-probe"), "--mode", "worker", "--address", "127.0.0.1:17233", "--namespace", fixture.Namespace, "--workflow-id", fixture.WorkflowID, "--task-queue", fixture.TaskQueue)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, worker.stop(false)) }()
	if _, err = worker.waitLine(ctx, "{"); err != nil {
		return result, err
	}
	startedRaw, err := probe(ctx, root, env, bin, fixture, "start")
	if err != nil {
		return result, fmt.Errorf("workflow start: %w", err)
	}
	var execution struct {
		RunID string `json:"run_id"`
	}
	if json.Unmarshal(startedRaw, &execution) != nil || execution.RunID == "" {
		return result, errors.New("invalid start")
	}
	if err = probePhase(ctx, root, env, bin, fixture, "await-control"); err != nil {
		return result, fmt.Errorf("workflow control barrier: %w", err)
	}
	before, err := waitControl(ctx, store, func(c cluster.Control) bool { return readyOwner(c, bootstrap.HistoryPartition) != "" })
	if err != nil {
		return result, err
	}
	owner := readyOwner(before, bootstrap.HistoryPartition)
	physical, ok := before.Layout.Resolve(bootstrap.HistoryPartition)
	if !ok {
		return result, fmt.Errorf("history partition %q is absent from layout", bootstrap.HistoryPartition)
	}
	oldIncarnation := before.Partitions[physical.ID].Desired.Incarnation
	index := -1
	for i := range configs {
		if configs[i].ServiceStorage.NodeID == owner {
			index = i
		}
	}
	if index < 0 {
		return result, fmt.Errorf("history owner %s not a process", owner)
	}
	if err = processes[index].kill(); err != nil {
		return result, err
	}
	processes[index] = nil
	if _, err = waitControl(ctx, store, func(c cluster.Control) bool {
		next := readyOwner(c, bootstrap.HistoryPartition)
		return next != "" && next != owner
	}); err != nil {
		return result, err
	}
	fmt.Fprintf(diagnostics, "Killed history owner %s; takeover completed\n", owner)
	if _, err = probe(ctx, root, env, bin, fixture, "control"); err != nil {
		return result, err
	}
	configs[index].Bootstrap = false
	configs[index].ServiceStorage.FreshNamespace = false
	if err = start(index); err != nil {
		return result, err
	}
	if _, err = waitControl(ctx, store, func(c cluster.Control) bool {
		for _, partition := range c.Partitions {
			if partition.Ready && partition.Desired.Node == owner && partition.Desired.Incarnation != oldIncarnation {
				return true
			}
		}
		return false
	}); err != nil {
		return result, fmt.Errorf("replacement incarnation did not become ready: %w", err)
	}
	fmt.Fprintf(diagnostics, "Restarted %s with a fresh incarnation\n", owner)
	for i, p := range processes {
		if err = p.kill(); err != nil {
			return result, err
		}
		processes[i] = nil
	}
	for i := range configs {
		configs[i].Bootstrap = false
		configs[i].ServiceStorage.FreshNamespace = false
		if err = start(i); err != nil {
			return result, err
		}
	}
	fmt.Fprintln(diagnostics, "Cold restart completed from disposable local directories")
	history := filepath.Join(journeyDirectory, "history")
	if err = os.Mkdir(history, 0700); err != nil {
		return result, err
	}
	if _, err = probe(ctx, root, env, bin, fixture, "verify", "--run-id", execution.RunID, "--output", history); err != nil {
		return result, err
	}
	if _, err = probe(ctx, root, env, bin, fixture, "visibility", "--query", "WorkflowId = '"+fixture.WorkflowID+"'", "--expected-count", "2"); err != nil {
		return result, err
	}
	result.Assertions = []string{
		"two unified Xenon instances served one ingress",
		"activity completed",
		"timer fired",
		"signal and update progressed the workflow",
		"child workflow completed",
		"Continue-As-New completed",
		"Nexus endpoint executed",
		"history matched the SDK workload",
		"visibility returned both workflow runs",
		"history-owner loss recovered",
		"same node restarted with a new incarnation",
		"cold local restart preserved acknowledged state",
	}
	return result, nil
}

func prepareUnifiedIngress(root, destination string) (string, error) {
	source := filepath.Join(root, "test/scenarios/ministack/config")
	compose, err := os.ReadFile(filepath.Join(source, "compose.json"))
	if err != nil {
		return "", err
	}
	haproxy, err := os.ReadFile(filepath.Join(source, "haproxy.cfg"))
	if err != nil {
		return "", err
	}
	configured := strings.ReplaceAll(string(haproxy), "host.docker.internal:17351", "host.docker.internal:18241")
	configured = strings.ReplaceAll(configured, "host.docker.internal:17352", "host.docker.internal:19241")
	configured = strings.ReplaceAll(configured, "    server node_c host.docker.internal:17353 check\n", "")
	configured = strings.ReplaceAll(configured, "host.docker.internal:18243", "host.docker.internal:18242")
	configured = strings.ReplaceAll(configured, "host.docker.internal:19243", "host.docker.internal:19242")
	if configured == string(haproxy) {
		return "", errors.New("ministack HAProxy storage backends did not match the pinned legacy topology")
	}
	if err = os.WriteFile(filepath.Join(destination, "haproxy.cfg"), []byte(configured), 0600); err != nil {
		return "", err
	}
	path := filepath.Join(destination, "compose.json")
	if err = os.WriteFile(path, compose, 0600); err != nil {
		return "", err
	}
	return path, nil
}
