package node

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	vmodel "github.com/0x63616c/xenon/internal/visibility"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
	"os"
	"testing"
	"time"
)

func TestGoOwnerVisibilityRecovery(t *testing.T) {
	var fixture struct {
		Schema    int
		Namespace string `json:"namespace_id"`
		Run       string `json:"run_id"`
		Start     string
	}
	raw, e := os.ReadFile("../../proof/visibility/case.json")
	if e != nil {
		t.Fatal(e)
	}
	if json.Unmarshal(raw, &fixture) != nil || fixture.Schema != 1 {
		t.Fatal("invalid fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	objects := objects(t)
	partition, e := vmodel.Partition(fixture.Namespace, fixture.Run)
	if e != nil {
		t.Fatal(e)
	}
	open := func() *Owner {
		o, e := NewOwner(engine(t, objects, "visibility-recovery", false), DefaultConfig(partition))
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = o.Close(context.Background()) })
		return o
	}
	o := open()
	s := &VisibilityServer{Owner: o}
	request := func(id string, c *wire.VisibilityCommand) *wire.VisibilityRequest {
		b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		h := sha256.Sum256(b)
		return &wire.VisibilityRequest{ProtocolVersion: 1, Partition: partition, OperationId: id, CommandSha256: h[:], Command: c}
	}
	call := func(id string, c *wire.VisibilityCommand) *wire.VisibilityResult {
		t.Helper()
		r, e := s.Execute(ctx, request(id, c))
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	start, _ := time.Parse(time.RFC3339, fixture.Start)
	d := &wire.VisibilityDocument{NamespaceId: fixture.Namespace, RunId: fixture.Run, WorkflowId: "workflow", StartTime: vmodel.Time(start), ExecutionTime: vmodel.Time(start), TaskId: 1, Status: 1}
	d.Attributes = map[string]*wire.VisibilityAttribute{"Double01": {ValueType: int32(enumspb.INDEXED_VALUE_TYPE_DOUBLE), Values: []*wire.QueryValue{{Scalar: &wire.QueryValue_DoubleValue{DoubleValue: 1.234565}}}}}
	equivalent := proto.Clone(d).(*wire.VisibilityDocument)
	equivalent.Attributes["Double01"].Values[0].Scalar = &wire.QueryValue_DoubleValue{DoubleValue: 1.23457}
	firstKeys, err := visibilityIndices(d)
	if err != nil {
		t.Fatal(err)
	}
	secondKeys, err := visibilityIndices(equivalent)
	if err != nil || len(firstKeys) != 2 || len(secondKeys) != 2 || firstKeys[1] != secondKeys[1] {
		t.Fatal("equivalent decimal equality indexes differ", firstKeys, secondKeys, err)
	}
	put := &wire.VisibilityCommand{Kind: wire.VisibilityCommand_START, Document: d}
	saved := call("start", put)
	if saved.Error != wire.VisibilityResult_NONE {
		t.Fatal(saved)
	}
	newer := proto.Clone(d).(*wire.VisibilityDocument)
	newer.TaskId = 2
	newer.WorkflowId = "newer"
	call("upsert", &wire.VisibilityCommand{Kind: wire.VisibilityCommand_UPSERT, Document: newer})
	call("stale", put)
	get := &wire.VisibilityCommand{Kind: wire.VisibilityCommand_GET, NamespaceId: d.NamespaceId, RunId: d.RunId}
	if r := call("get-newer", get); len(r.Documents) != 1 || r.Documents[0].WorkflowId != "newer" {
		t.Fatal(r)
	}
	deletion := &wire.VisibilityCommand{Kind: wire.VisibilityCommand_DELETE, NamespaceId: d.NamespaceId, RunId: d.RunId}
	call("delete", deletion)
	call("delete-again", deletion)
	newer.TaskId = math.MaxInt64
	call("delayed-newer", &wire.VisibilityCommand{Kind: wire.VisibilityCommand_UPSERT, Document: newer})
	newer.CloseTime = vmodel.Time(start.Add(time.Hour))
	call("delayed-close", &wire.VisibilityCommand{Kind: wire.VisibilityCommand_CLOSE, Document: newer})
	if r := call("deleted", get); r.Error != wire.VisibilityResult_NOT_FOUND {
		t.Fatal("resurrection", r)
	}
	if e = o.Close(ctx); e != nil {
		t.Fatal(e)
	}
	o = open()
	s.Owner = o
	if r := call("start", put); !proto.Equal(r, saved) {
		t.Fatal("changed replay")
	}
	if r := call("after-reopen", get); r.Error != wire.VisibilityResult_NOT_FOUND {
		t.Fatal("reopen resurrected", r)
	}
	// A query verifies deletion removed the transactional order index too.
	q := &wire.VisibilityQuery{FormatVersion: 1, PartitionFormat: 1, NamespaceId: d.NamespaceId}
	if r := call("count", &wire.VisibilityCommand{Kind: wire.VisibilityCommand_COUNT, Query: q}); r.Count != 0 {
		t.Fatal("stale index", r)
	}
	rival := open()
	_ = rival
	if _, e = s.Execute(ctx, request("start", put)); status.Code(e) != codes.Unavailable || !o.Quarantined() {
		t.Fatal("fenced replay published", e)
	}
}

func TestGoOwnerVisibilityEmptyIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	objects := objects(t)
	partition, _ := vmodel.Partition("", "")
	zero := "00000000-0000-0000-0000-000000000000"
	same, _ := vmodel.Partition(zero, zero)
	if same != partition {
		t.Fatal("declared routing input differs")
	}
	open := func() *Owner {
		o, e := NewOwner(engine(t, objects, "visibility-empty", false), DefaultConfig(partition))
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = o.Close(context.Background()) })
		return o
	}
	o := open()
	s := &VisibilityServer{Owner: o}
	call := func(id string, c *wire.VisibilityCommand) *wire.VisibilityResult {
		t.Helper()
		b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		h := sha256.Sum256(b)
		r, e := s.Execute(ctx, &wire.VisibilityRequest{ProtocolVersion: 1, Partition: partition, OperationId: id, CommandSha256: h[:], Command: c})
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	for i, identity := range []string{"", zero} {
		d := &wire.VisibilityDocument{NamespaceId: identity, RunId: identity, WorkflowId: identity, StartTime: vmodel.Time(time.Time{}), ExecutionTime: vmodel.Time(time.Time{}), Status: 1}
		if r := call([]string{"empty", "zero"}[i], &wire.VisibilityCommand{Kind: wire.VisibilityCommand_UPSERT, Document: d}); r.Error != wire.VisibilityResult_NONE {
			t.Fatal(r)
		}
	}
	if e := o.Close(ctx); e != nil {
		t.Fatal(e)
	}
	s.Owner = open()
	for i, identity := range []string{"", zero} {
		r := call([]string{"read-empty", "read-zero"}[i], &wire.VisibilityCommand{Kind: wire.VisibilityCommand_GET, NamespaceId: identity, RunId: identity})
		if len(r.Documents) != 1 || r.Documents[0].WorkflowId != identity {
			t.Fatal("empty and zero conflated", r)
		}
	}
	call("delete-empty", &wire.VisibilityCommand{Kind: wire.VisibilityCommand_DELETE})
	if r := call("zero-survives", &wire.VisibilityCommand{Kind: wire.VisibilityCommand_GET, NamespaceId: zero, RunId: zero}); len(r.Documents) != 1 {
		t.Fatal(r)
	}
	// A deleted absent run cannot be created later, even at the largest task ID.
	unknown := &wire.VisibilityCommand{Kind: wire.VisibilityCommand_DELETE, NamespaceId: zero, RunId: ""}
	call("delete-before", unknown)
	late := &wire.VisibilityDocument{NamespaceId: zero, RunId: "", StartTime: vmodel.Time(time.Time{}), ExecutionTime: vmodel.Time(time.Time{}), TaskId: math.MaxInt64}
	call("late-start", &wire.VisibilityCommand{Kind: wire.VisibilityCommand_START, Document: late})
	if r := call("still-deleted", &wire.VisibilityCommand{Kind: wire.VisibilityCommand_GET, NamespaceId: zero, RunId: ""}); r.Error != wire.VisibilityResult_NOT_FOUND {
		t.Fatal(r)
	}
}
