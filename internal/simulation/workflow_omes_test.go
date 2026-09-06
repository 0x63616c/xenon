//go:build darwin || linux

package simulation

import (
	"context"
	"encoding/json"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPreparedResidentToolsPinnedWithoutRuntime(t *testing.T) {
	bundle := os.Getenv("XENON_WORKFLOW_GENERATOR_BUNDLE")
	if bundle == "" {
		t.Skip("prepared tools opt-in; no services are started")
	}
	dir := t.TempDir()
	fixture := filepath.Join(dir, "fixture.json")
	topology := residentTestTopology()
	topology.RunID = ""
	topology.FixtureSHA256 = ""
	raw, _ := json.Marshal(topology)
	if e := os.WriteFile(fixture, raw, 0600); e != nil {
		t.Fatal(e)
	}
	oracle := "/usr/bin/true"
	h, e := hashWorkflowFile(oracle)
	if e != nil {
		t.Fatal(e)
	}
	runtime, e := NewOmesRuntime(filepath.Join(bundle, "worker/build.json"), fixture, oracle, h)
	if e != nil {
		t.Fatal(e)
	}
	if e = runtime.checkTools(); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(fixture, append(raw, ' '), 0600); e != nil {
		t.Fatal(e)
	}
	if e = runtime.checkTools(); e == nil {
		t.Fatal("changed fixture accepted")
	}
}

// A prior case's audit cannot certify a later case that never reached Execute.
type emptyResidentServer struct {
	workflowservice.UnimplementedWorkflowServiceServer
}

func (emptyResidentServer) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{}, nil
}
func (emptyResidentServer) ListWorkflowExecutions(context.Context, *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	return &workflowservice.ListWorkflowExecutionsResponse{}, nil
}
func (emptyResidentServer) CountWorkflowExecutions(context.Context, *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	return &workflowservice.CountWorkflowExecutionsResponse{}, nil
}
func TestResidentCleanupRejectsPreviousCaseAudit(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(server, emptyResidentServer{})
	go server.Serve(listener)
	defer server.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runtime := &OmesRuntime{auditedRun: "previous"}
	topology := residentTestTopology()
	topology.Address = listener.Addr().String()
	topology.RunID = "next"
	err = runtime.Cleanup(ctx, topology, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "remote cleanup unverified after failed case") {
		t.Fatalf("prior audit certified next case: %v", err)
	}
	// Empty is the pre-admission boundary, including a later run using same name.
	runtime.auditedRun = topology.RunID
	if err = runtime.Empty(ctx, topology); err != nil {
		t.Fatal(err)
	}
	if runtime.auditedRun != "" {
		t.Fatal("pre-admission retained old audit")
	}
}
