package app

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

//go:embed dev_template.json
var devTemplate []byte

const devMinioImage = "minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e"

// DevFixture deliberately describes only the pinned local three-node profile.
// Image is the immutable result of Dockerfile.unified-agent's dev-runtime target.
type DevFixture struct {
	Schema           int    `json:"schema"`
	Image            string `json:"image"`
	S3Port           int    `json:"s3_port"`
	TemporalPort     int    `json:"temporal_port"`
	HTTPPort         int    `json:"http_port"`
	StoragePort      int    `json:"storage_port"`
	DiagnosticsPorts [3]int `json:"diagnostics_ports"`
}

func (f DevFixture) plan() (devPlan, error) {
	if f.Schema != 1 || !devImage.MatchString(f.Image) {
		return devPlan{}, errors.New("schema 1 and immutable dev-runtime image required")
	}
	ports := append([]int{f.S3Port, f.TemporalPort, f.HTTPPort, f.StoragePort}, f.DiagnosticsPorts[:]...)
	seen := map[int]bool{}
	for _, p := range ports {
		if p < 1024 || p > 65535 || seen[p] {
			return devPlan{}, errors.New("seven distinct explicit unprivileged host ports required")
		}
		seen[p] = true
	}
	internal := []int{9000, 17233, 17243, 17935, 18250, 19250, 21250}
	mappings := []string{}
	for i, p := range ports {
		mappings = append(mappings, fmt.Sprintf("127.0.0.1:%d:%d", p, internal[i]))
	}
	plan := devPlan{Schema: 1, Containers: []devContainer{{Role: "s3", Image: devMinioImage, Args: []string{"server", "/data"}, Environment: []string{"MINIO_ROOT_USER=xenon-local", "MINIO_ROOT_PASSWORD=xenon-local-test-only"}, NetworkHolder: true, Objects: true, Ports: mappings}}}
	for _, v := range []struct{ role, listen, backends string }{{"storage-ingress", "0.0.0.0:17935", "127.0.0.1:18241,127.0.0.1:19241,127.0.0.1:21241"}, {"temporal-ingress", "0.0.0.0:17233", "127.0.0.1:18233,127.0.0.1:19233,127.0.0.1:21233"}, {"http-ingress", "0.0.0.0:17243", "127.0.0.1:18242,127.0.0.1:19242,127.0.0.1:21242"}} {
		plan.Containers = append(plan.Containers, devContainer{Role: v.role, Image: f.Image, Entrypoint: "/usr/local/bin/xenon-ingress", Args: []string{"--listen", v.listen, "--backends", v.backends}})
	}
	for i, port := range []int{18233, 19233, 21233} {
		var c Config
		if err := json.Unmarshal(devTemplate, &c); err != nil {
			return devPlan{}, err
		}
		c.Node = fmt.Sprintf("dev-%d", i+1)
		c.BasePort = port
		c.DiagnosticsAddress = fmt.Sprintf("0.0.0.0:%d", internal[i+4])
		c.Bootstrap = i == 0
		c.ServiceStorage.FreshNamespace = i == 0
		c.ServiceStorage.NodeID = identity.NodeID(fmt.Sprintf("nod_%022d", i+1))
		if err := ValidateServiceLayout(c); err != nil {
			return devPlan{}, err
		}
		plan.Containers = append(plan.Containers, devContainer{Role: fmt.Sprintf("agent-%d", i+1), Image: f.Image, Args: []string{"start", "--config", "/etc/xenon/dev.json"}, Config: &c, Environment: []string{"AWS_DEFAULT_REGION=us-east-1", "AWS_ACCESS_KEY_ID=xenon-local", "AWS_SECRET_ACCESS_KEY=xenon-local-test-only", "AWS_ENDPOINT=http://127.0.0.1:9000", "AWS_ALLOW_HTTP=true", "AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false", "SLATEDB_UNIFFI_RUNTIME_THREADS=2"}})
	}
	plan.Containers = append(plan.Containers, devContainer{Role: "worker", Image: f.Image, Entrypoint: "/usr/local/bin/xenon-sdk-probe", Args: []string{"--mode", "worker", "--namespace", "xenon-ministack", "--address", "127.0.0.1:17233"}})
	plan.Containers = append(plan.Containers, devContainer{Role: "omes-worker", Image: f.Image, Entrypoint: "/usr/local/bin/omes-worker", Args: []string{"--server-address", "127.0.0.1:17233", "--namespace", "xenon-ministack", "--task-queue", "omes-xenon-dev"}})
	return plan, plan.validate()
}

// RunDev starts the declared local fixture or tears down its recorded ownership.
// Shared network namespace qualifies this as local process, not multi-host proof.
func RunDev(ctx context.Context, action, fixturePath, dir string, ephemeral bool) (DevResult, error) {
	var f DevFixture
	var plan devPlan
	if action == "up" {
		if err := readDevJSON(fixturePath, &f); err != nil {
			return DevResult{Schema: 1, Status: "invalid"}, err
		}
		var err error
		plan, err = f.plan()
		if err != nil {
			return DevResult{Schema: 1, Status: "invalid"}, err
		}
	}
	hooks := devHooks{}
	if action == "up" {
		hooks.Started = func(ctx context.Context, role, id string, state devState) error {
			if role == "s3" {
				return devStorage(ctx, f.S3Port, *plan.Containers[4].Config, state.Initialized, state.Initialization)
			}
			if role == "agent-3" {
				// All nodes must reach real application readiness before namespace setup.
				if err := devReady(ctx, f.DiagnosticsPorts); err != nil {
					return err
				}
				_, err := dockerCommand(ctx, "exec", id, "/usr/local/bin/xenon-sdk-probe", "--mode", "bootstrap", "--cluster", "xenon", "--namespace", "xenon-ministack", "--address", "127.0.0.1:17233")
				if err != nil {
					return err
				}
				_, err = dockerCommand(ctx, "exec", id, "/usr/local/bin/xenon-sdk-probe", "--mode", "schema-ready", "--namespace", "xenon-ministack", "--address", "127.0.0.1:17233")
				return err
			}
			return nil
		}
		hooks.Identity = func(ctx context.Context) (string, error) {
			return devAuthority(ctx, devS3Client(f.S3Port), *plan.Containers[4].Config, "")
		}
		hooks.Ready = func(ctx context.Context) error {
			if err := devReady(ctx, f.DiagnosticsPorts); err != nil {
				return err
			}
			c, err := client.DialContext(ctx, client.Options{HostPort: fmt.Sprintf("127.0.0.1:%d", f.TemporalPort), Namespace: "xenon-ministack"})
			if err != nil {
				return err
			}
			defer c.Close()
			for _, queue := range []string{"xenon-ministack-worker", "omes-xenon-dev"} {
				if err := devWait(ctx, func() error {
					response, err := c.WorkflowService().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{Namespace: "xenon-ministack", TaskQueue: &taskqueue.TaskQueue{Name: queue}, TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW})
					if err != nil {
						return err
					}
					if len(response.GetPollers()) == 0 {
						return errors.New("worker has no observed poller")
					}
					return nil
				}); err != nil {
					return err
				}
			}
			return nil
		}
	}
	result, err := runDevPlan(ctx, action, plan, dir, ephemeral, dockerCommand, hooks)
	if err == nil && action == "up" {
		// Saved host inspection configuration refers to the same cluster/layout.
		inspect := *plan.Containers[4].Config
		raw, _ := json.MarshalIndent(inspect, "", "  ")
		err = os.WriteFile(filepath.Join(dir, "inspect.json"), append(raw, '\n'), 0600)
	}
	return result, err
}
func devReady(ctx context.Context, ports [3]int) error {
	for _, port := range ports {
		if err := devWait(ctx, func() error {
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/readyz", port), nil)
			if err != nil {
				return err
			}
			client := http.Client{Timeout: time.Second}
			response, err := client.Do(request)
			if err != nil {
				return err
			}
			defer response.Body.Close()
			if response.StatusCode != 200 {
				return errors.New("node not ready")
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
func devWait(ctx context.Context, check func() error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := check(); err == nil {
			return nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func devS3Client(port int) *s3.Client {
	return s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("xenon-local", "xenon-local-test-only", "")}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://127.0.0.1:%d", port))
		o.UsePathStyle = true
	})
}

// devStorage verifies the preserved authority before starting any agent. A
// successful previous initialization permanently removes permission to create
// a missing bucket or bootstrap a missing cluster. Failed first starts remain
// resumable, and readiness is recorded only after the complete fixture is ready.
func devStorage(ctx context.Context, port int, config Config, initialized bool, identity string) error {
	client := devS3Client(port)
	if initialized {
		if identity == "" {
			return errors.New("preserved cluster has no recorded initialization identity")
		}
		err := devWait(ctx, func() error { _, err := devAuthority(ctx, client, config, identity); return err })
		if err != nil {
			return fmt.Errorf("preserved cluster unavailable; refusing bootstrap: %w", err)
		}
		return nil
	}
	return devWait(ctx, func() error {
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("xenon-agent-proof")})
		var api smithy.APIError
		if errors.As(err, &api) && (api.ErrorCode() == "BucketAlreadyOwnedByYou" || api.ErrorCode() == "BucketAlreadyExists") {
			return nil
		}
		return err
	})
}

func devAuthority(ctx context.Context, client *s3.Client, config Config, expected string) (string, error) {
	if _, err := inspectService(ctx, config, client); err != nil {
		return "", err
	}
	manifest, err := readServiceManifest(ctx, client, config.Bucket, config.Prefix+"/metadata/cluster.json")
	if err != nil {
		return "", err
	}
	id := string(manifest.Initialization)
	if id == "" || expected != "" && expected != id {
		return "", errors.New("preserved cluster initialization identity changed")
	}
	return id, nil
}
