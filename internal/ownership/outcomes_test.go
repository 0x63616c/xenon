//go:build slatedb

package ownership

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http/httptest"
	"testing"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/directory"
	"github.com/0x63616c/xenon/internal/node"
	partitiondb "github.com/0x63616c/xenon/internal/partitions/slatedb"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
)

func TestOutcomeMetrics(t *testing.T) {
	ctx := context.Background()
	m, e := NewManager(&TopologyStore{bucket: "test"}, "n", "127.0.0.1:1", "s3://test", 3)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = NewManager(&TopologyStore{bucket: "test"}, "n", "a", "s3://test", 0); e == nil {
		t.Fatal("zero cap accepted")
	}
	objects, e := native.ObjectStoreResolve("memory:///")
	if e != nil {
		t.Fatal(e)
	}
	defer objects.Destroy()
	builder := native.NewDbBuilder("metrics", objects)
	db, e := builder.Build()
	builder.Destroy()
	if e != nil {
		t.Fatal(e)
	}
	writer, e := partitiondb.AdoptNative(db)
	if e != nil {
		t.Fatal(e)
	}
	o, e := node.NewOwner(writer, m.ownerConfig("p"))
	if e != nil {
		t.Fatal(e)
	}
	defer o.Close(ctx)
	m.owners["p"] = &managed{owner: o, record: directory.Record{Partition: "p"}}
	command := &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: 1, RangeId: 3}
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	d := sha256.Sum256(b)
	q := &wire.ShardRequest{ProtocolVersion: 1, Partition: "p", OperationId: "actual", CommandSha256: d[:], Command: command}
	if _, e = m.dispatch(ctx, wire.ShardPersistence_Execute_FullMethodName, q); e != nil {
		t.Fatal(e)
	}
	handler := m.OutcomesHandler()
	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest("GET", "/outcomes", nil))
		if rec.Code != 200 {
			t.Fatal(rec.Code, rec.Body)
		}
		var report OutcomesReport
		if e = json.Unmarshal(rec.Body.Bytes(), &report); e != nil {
			t.Fatal(e)
		}
		p := report.Partitions["p"]
		if report.Incarnation != m.Identity().Incarnation || p.Usage.Limit != 3 || p.Usage.Entries != 1 || !p.Usage.AccountingComplete || p.LocalDispatch.SuccessfulOperations != 1 || p.LocalDispatch.Attempts != 1 {
			t.Fatal(report)
		}
	}
	// A local logical failure is a served result but not a successful operation.
	command = &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: 2}
	b, _ = proto.MarshalOptions{Deterministic: true}.Marshal(command)
	d = sha256.Sum256(b)
	q = &wire.ShardRequest{ProtocolVersion: 1, Partition: "p", OperationId: "logical", CommandSha256: d[:], Command: command}
	if _, e = m.dispatch(ctx, wire.ShardPersistence_Execute_FullMethodName, q); e != nil {
		t.Fatal(e)
	}
	report := m.Outcomes(ctx)
	p := report.Partitions["p"]
	if p.Usage.Entries != 2 || p.LocalDispatch.Attempts != 2 || p.LocalDispatch.RPCResults != 2 || p.LocalDispatch.SuccessfulOperations != 1 {
		t.Fatal(report)
	}
}
