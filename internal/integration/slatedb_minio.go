// Package integration contains the small, explicitly selected real-system
// journeys. It is intentionally callable from the Go CLI rather than go test so
// the ordinary test suite never starts Docker.
package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/partitions/slatedb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const minioImage = "minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e"

// RunSlateDBMinIO proves the native seam that deterministic simulation cannot:
// remote durability, reopen after losing the response, idempotent reconciliation,
// and fencing by a replacement native writer.
func RunSlateDBMinIO(ctx context.Context, diagnostics io.Writer) (result JourneyResult, err error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	evidence, err := beginJourney(ctx, &result, "slatedb-minio", diagnostics)
	defer evidence.finish(ctx, &err)
	if err != nil {
		return result, err
	}
	diagnostics = evidence.writer()
	if err = evidence.config("native-journey", map[string]any{"image": minioImage, "timeout": "2m", "wal_flush_interval_ms": 10, "operation": "op_0000000000000000000001", "digest": "digest-1", "faults": []string{"close-after-commit-before-response", "replacement-fences-stale-transaction"}}); err != nil {
		return result, err
	}
	runtime, err := startMinIO(ctx, diagnostics)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, runtime.close(diagnostics)) }()

	restore := setEnvironment(map[string]string{
		"AWS_ACCESS_KEY_ID": "xenon-local", "AWS_SECRET_ACCESS_KEY": "xenon-local-test-only",
		"AWS_DEFAULT_REGION": "us-east-1", "AWS_REGION": "us-east-1",
		"AWS_ENDPOINT": runtime.endpoint, "AWS_ALLOW_HTTP": "true",
		"AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false", "AWS_EC2_METADATA_DISABLED": "true",
		"AWS_CONFIG_FILE": os.DevNull, "AWS_SHARED_CREDENTIALS_FILE": os.DevNull,
	})
	defer restore()

	bucket := "xenon-integration-" + randomHex(8)
	client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(runtime.endpoint), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider("xenon-local", "xenon-local-test-only", "")})
	if _, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return result, fmt.Errorf("create MinIO bucket: %w", err)
	}
	engine, err := slatedb.NewWithWALFlushInterval("s3://"+bucket, 10)
	if err != nil {
		return result, err
	}
	request := partitions.OpenRequest{Path: "integration/slatedb-minio", Partition: identity.PartitionID("prt_0000000000000000000001"), AssignmentRevision: 1, Reservation: identity.TransitionID("trn_0000000000000000000001"), Incarnation: identity.IncarnationID("inc_0000000000000000000001"), Generation: 1}
	if err = evidence.config("native-open", map[string]any{"endpoint": runtime.endpoint, "bucket": bucket, "request": request}); err != nil {
		return result, err
	}
	writer, err := engine.Open(ctx, request)
	if err != nil {
		return result, fmt.Errorf("open first writer: %w", err)
	}

	defer func() { err = errors.Join(err, closeWriter(writer, false)) }()

	// The atomic operation record is the reconciliation point. The simulated
	// lost response is deliberate: no receipt/result is retained by the caller.
	if err = applyOnce(ctx, writer, "op_0000000000000000000001", "digest-1"); err != nil {
		return result, fmt.Errorf("commit and await remote durability: %w", err)
	}
	if err = writer.Close(ctx); err != nil {
		return result, fmt.Errorf("discard first writer: %w", err)
	}
	request.AssignmentRevision, request.Generation = 2, 2
	request.Reservation = identity.TransitionID("trn_0000000000000000000002")
	request.Incarnation = identity.IncarnationID("inc_0000000000000000000002")
	reopened, err := engine.Open(ctx, request)
	if err != nil {
		return result, fmt.Errorf("reopen from S3: %w", err)
	}
	// Shutdown of this deliberately displaced writer may itself report fencing.
	defer func() { err = errors.Join(err, closeWriter(reopened, true)) }()
	if err = applyOnce(ctx, reopened, "op_0000000000000000000001", "digest-1"); err != nil {
		return result, fmt.Errorf("reconcile lost response: %w", err)
	}
	if got, err := read(ctx, reopened, "application/count"); err != nil || got != "1" {
		return result, fmt.Errorf("exactly-once state after reopen: got %q: %w", got, err)
	}

	stale, err := reopened.Begin(ctx)
	if err != nil {
		return result, err
	}
	if err = stale.Put([]byte("application/stale"), []byte("must-not-commit")); err != nil {
		return result, err
	}
	request.AssignmentRevision, request.Generation = 3, 3
	request.Reservation = identity.TransitionID("trn_0000000000000000000003")
	request.Incarnation = identity.IncarnationID("inc_0000000000000000000003")
	replacement, err := engine.Open(ctx, request)
	if err != nil {
		return result, fmt.Errorf("open replacement writer: %w", err)
	}
	defer func() { err = errors.Join(err, closeWriter(replacement, false)) }()
	receipt, staleErr := stale.Commit(ctx)
	if staleErr == nil {
		staleErr = reopened.AwaitDurable(ctx, receipt)
	}
	if !errors.Is(staleErr, partitions.ErrFenced) {
		return result, fmt.Errorf("displaced writer was not fenced: %w", staleErr)
	}
	if got, readErr := read(ctx, replacement, "application/stale"); readErr != nil || got != "" {
		return result, fmt.Errorf("stale mutation visible: got %q: %w", got, readErr)
	}
	if got, readErr := read(ctx, replacement, "application/count"); readErr != nil || got != "1" {
		return result, fmt.Errorf("acknowledged state lost after fencing: got %q: %w", got, readErr)
	}

	result.Assertions = []string{"remote durability survived reopen", "lost response reconciled exactly once", "displaced writer fenced", "stale mutation absent"}
	return result, nil
}

func applyOnce(ctx context.Context, writer partitions.Writer, operation, digest string) error {
	tx, err := writer.Begin(ctx)
	if err != nil {
		return err
	}
	key := []byte("operations/" + operation)
	seen, err := tx.Get(ctx, key)
	if err != nil {
		_ = tx.Abort()
		return err
	}
	if seen != nil {
		_ = tx.Abort()
		if string(seen) != digest {
			return fmt.Errorf("operation digest changed")
		}
		return nil
	}
	count, err := tx.Get(ctx, []byte("application/count"))
	if err != nil {
		_ = tx.Abort()
		return err
	}
	if count != nil && string(count) != "0" {
		_ = tx.Abort()
		return fmt.Errorf("unexpected application count %q", count)
	}
	if err = tx.Put([]byte("application/count"), []byte("1")); err == nil {
		err = tx.Put(key, []byte(digest))
	}
	if err != nil {
		_ = tx.Abort()
		return err
	}
	receipt, err := tx.Commit(ctx)
	if err != nil {
		return err
	}
	return writer.AwaitDurable(ctx, receipt)
}

func read(ctx context.Context, writer partitions.Writer, key string) (string, error) {
	result, err := writer.ReadDurable(ctx, partitions.ReadRequest{Keys: [][]byte{[]byte(key)}})
	if err != nil || len(result.Entries) == 0 {
		return "", err
	}
	return string(result.Entries[0].Value), nil
}

func closeWriter(writer partitions.Writer, allowFenced bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := writer.Close(ctx); err != nil && !(allowFenced && errors.Is(err, partitions.ErrFenced)) {
		return fmt.Errorf("close native writer: %w", err)
	}
	return nil
}

type minioRuntime struct{ name, endpoint string }

func startMinIO(ctx context.Context, diagnostics io.Writer) (_ *minioRuntime, err error) {
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf("Docker CLI required for integration tests: %w", err)
	}
	name := "xenon-integration-" + randomHex(8)
	runtime := &minioRuntime{name: name}
	cleanup := true
	defer func() {
		if cleanup {
			err = errors.Join(err, runtime.close(diagnostics))
		}
	}()
	cmd := exec.CommandContext(ctx, "docker", "run", "--detach", "--name", name, "--publish", "127.0.0.1::9000", "--env", "MINIO_ROOT_USER=xenon-local", "--env", "MINIO_ROOT_PASSWORD=xenon-local-test-only", minioImage, "server", "/data")
	if output, err := outputCommand(cmd); err != nil {
		return nil, fmt.Errorf("start MinIO: %w: %s", err, strings.TrimSpace(string(output)))
	}
	port, err := dockerOutput(ctx, "port", name, "9000/tcp")
	if err != nil {
		return nil, err
	}
	port = strings.TrimSpace(port)
	if i := strings.LastIndex(port, ":"); i >= 0 {
		port = port[i+1:]
	}
	runtime.endpoint = "http://127.0.0.1:" + port
	deadline := time.Now().Add(30 * time.Second)
	client := &http.Client{Timeout: time.Second}
	for time.Now().Before(deadline) {
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, runtime.endpoint+"/minio/health/live", nil)
		response, requestErr := client.Do(request)
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				cleanup = false
				return runtime, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	logsCtx, cancelLogs := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelLogs()
	logs, _ := dockerOutput(logsCtx, "logs", "--tail", "200", name)
	return nil, fmt.Errorf("MinIO readiness timeout: %s", strings.TrimSpace(logs))
}

func (m *minioRuntime) close(diagnostics io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	logCtx, stopLogs := context.WithTimeout(context.Background(), 5*time.Second)
	logs, logErr := dockerOutput(logCtx, "logs", "--tail", "200", m.name)
	stopLogs()
	if diagnostics != nil {
		fmt.Fprintf(diagnostics, "MinIO %s logs (capture error: %v):\n%s\n", m.name, logErr, logs)
	}
	output, err := outputCommand(exec.CommandContext(ctx, "docker", "rm", "--force", m.name))
	if err != nil && strings.Contains(string(output), "No such container") {
		return nil
	}
	if err != nil && diagnostics != nil {
		fmt.Fprintf(diagnostics, "MinIO cleanup: %s\n", output)
	}
	if err != nil {
		return fmt.Errorf("remove MinIO container %s: %w", m.name, err)
	}
	return nil
}

func dockerOutput(ctx context.Context, args ...string) (string, error) {
	output, err := outputCommand(exec.CommandContext(ctx, "docker", args...))
	if err != nil {
		return "", fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

func randomHex(bytes int) string {
	value := make([]byte, bytes)
	if _, err := rand.Read(value); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(value)
}

func setEnvironment(values map[string]string) func() {
	prior := make(map[string]*string, len(values))
	for key, value := range values {
		if old, ok := os.LookupEnv(key); ok {
			copy := old
			prior[key] = &copy
		} else {
			prior[key] = nil
		}
		_ = os.Setenv(key, value)
	}
	return func() {
		for key, value := range prior {
			if value == nil {
				_ = os.Unsetenv(key)
			} else {
				_ = os.Setenv(key, *value)
			}
		}
	}
}
