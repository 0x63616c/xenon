package persistence

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"testing"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/cluster"
	vmodel "github.com/0x63616c/xenon/internal/visibility"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestVisibilityReplayPreservesTombstoneIndexesAndLayoutRoute(t *testing.T) {
	const namespace = "00000000-0000-0000-0000-000000000001"
	const run = "00000000-0000-0000-0000-000000000002"
	logical, err := vmodel.Partition(namespace, run)
	if err != nil {
		t.Fatal(err)
	}
	layout := cluster.Layout{Version: 1, Placement: cluster.DefaultPlacementConfig(), Partitions: []cluster.PhysicalPartition{{LogicalName: logical, ID: "prt_0000000000000000000001", Path: "data/visibility"}}}
	w := &clusterWriter{testWriter: &testWriter{durable: map[string][]byte{}}}
	var s *VisibilityService
	bind := func() {
		base, err := NewService(w, layout.Partitions[0].ID, 100, func(context.Context) error { return nil }, func(error) {})
		if err != nil {
			t.Fatal(err)
		}
		s, err = NewVisibilityService(base, layout)
		if err != nil {
			t.Fatal(err)
		}
	}
	bind()
	request := func(id int, c *wire.VisibilityCommand) *wire.VisibilityRequest {
		raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		d := sha256.Sum256(raw)
		return &wire.VisibilityRequest{ProtocolVersion: 1, Partition: string(layout.Partitions[0].ID), OperationId: fmt.Sprintf("op_%022d", id), Command: c, CommandSha256: d[:]}
	}
	call := func(id int, c *wire.VisibilityCommand) *wire.VisibilityResult {
		r, err := s.Execute(context.Background(), request(id, c))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	doc := &wire.VisibilityDocument{NamespaceId: namespace, RunId: run, WorkflowId: "workflow", StartTime: &wire.VisibilityTime{Seconds: 100}, ExecutionTime: &wire.VisibilityTime{Seconds: 100}, TaskId: 1, Status: 1}
	start := &wire.VisibilityCommand{Kind: wire.VisibilityCommand_START, Document: doc}
	before := call(1, start)
	if before.Error != wire.VisibilityResult_NONE {
		t.Fatal(before)
	}
	query := &wire.VisibilityCommand{Kind: wire.VisibilityCommand_LIST, PageSize: 10, Query: &wire.VisibilityQuery{FormatVersion: 1, PartitionFormat: 1, NamespaceId: namespace}}
	listed := call(2, query)
	if len(listed.Documents) != 1 || listed.Documents[0].RunId != run {
		t.Fatal("index missing", listed)
	}
	call(3, &wire.VisibilityCommand{Kind: wire.VisibilityCommand_DELETE, NamespaceId: namespace, RunId: run})
	w = &clusterWriter{testWriter: &testWriter{durable: maps.Clone(w.durable)}}
	bind()
	if r := call(1, start); !proto.Equal(r, before) {
		t.Fatal("replay changed", r)
	}
	doc.TaskId = 99
	call(4, &wire.VisibilityCommand{Kind: wire.VisibilityCommand_UPSERT, Document: doc})
	if r := call(5, &wire.VisibilityCommand{Kind: wire.VisibilityCommand_GET, NamespaceId: namespace, RunId: run}); r.Error != wire.VisibilityResult_NOT_FOUND {
		t.Fatal("tombstone resurrected", r)
	}
	if r := call(6, query); len(r.Documents) != 0 {
		t.Fatal("deleted index survived", r)
	}
	// Capturing the route by value prevents a caller's later slice mutation from
	// changing which document partition this borrowed writer accepts.
	layout.Partitions[0].LogicalName = "global"
	if r := call(7, &wire.VisibilityCommand{Kind: wire.VisibilityCommand_GET, NamespaceId: namespace, RunId: run}); r.Error != wire.VisibilityResult_NOT_FOUND {
		t.Fatal(r)
	}
	wrong, err := NewVisibilityService(s.service, layout)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrong.Execute(context.Background(), request(8, start)); status.Code(err) != codes.InvalidArgument {
		t.Fatal("logical route mismatch accepted", err)
	}
	if _, err := NewVisibilityService(s.service, cluster.Layout{}); status.Code(err) != codes.InvalidArgument {
		t.Fatal("invalid layout accepted", err)
	}
	layout.Partitions[0].ID = "prt_0000000000000000000002"
	if _, err := NewVisibilityService(s.service, layout); status.Code(err) != codes.InvalidArgument {
		t.Fatal("missing physical mapping accepted", err)
	}
}
