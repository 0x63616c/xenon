//go:build integration_s3 && slatedb

package ownership

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/node"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
)

func TestS3Maintenance(t *testing.T) {
	var fixture struct {
		Schema, Updates int
		Payload         int `json:"payload_bytes"`
		Timeout         int `json:"timeout_seconds"`
		Settings        map[string]any
	}
	raw, e := os.ReadFile("../../proof/maintenance/case.json")
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw, &fixture); e != nil || fixture.Schema != 1 || fixture.Updates != 32 || fixture.Payload != 8192 || fixture.Timeout != 1200 {
		t.Fatal("invalid maintenance fixture", e)
	}
	if fixture.Settings["garbage_collector_options.boundary_files_enabled"] != true || fixture.Settings["garbage_collector_options.wal_fence_options.dry_run"] != true {
		t.Fatal("unsafe maintenance configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(fixture.Timeout)*time.Second)
	defer cancel()
	store, e := Environment()
	if e != nil {
		t.Fatal(e)
	}
	client := store.client.(*s3.Client)
	if _, e = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(store.bucket)}); e != nil {
		t.Fatal(e)
	}
	objects, e := native.ObjectStoreResolve("s3://" + store.bucket)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(objects.Destroy)
	path := "data/maintenance"
	list := func() map[string]int64 {
		t.Helper()
		out := map[string]int64{}
		pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: aws.String(store.bucket), Prefix: aws.String(path + "/")})
		for pager.HasMorePages() {
			page, e := pager.NextPage(ctx)
			if e != nil {
				t.Fatal(e)
			}
			for _, v := range page.Contents {
				out[aws.ToString(v.Key)] = aws.ToInt64(v.Size)
			}
		}
		return out
	}
	seen := map[string]int64{}
	remember := func() {
		for k, v := range list() {
			seen[k] = v
		}
	}
	recorder := native.NewDefaultMetricsRecorder()
	t.Cleanup(recorder.Destroy)
	open := func() (*native.Db, *node.Owner) {
		t.Helper()
		b := native.NewDbBuilder(path, objects)
		defer b.Destroy()
		settings := native.SettingsDefault()
		defer settings.Destroy()
		for key, value := range fixture.Settings {
			raw, e := json.Marshal(value)
			if e != nil {
				t.Fatal(e)
			}
			if e = settings.Set(key, string(raw)); e != nil {
				t.Fatalf("setting %s: %v", key, e)
			}
		}
		if e = b.WithSettings(settings); e != nil {
			t.Fatal(e)
		}
		if e = b.WithMetricsRecorder(recorder); e != nil {
			t.Fatal(e)
		}
		db, e := b.Build()
		if e != nil {
			t.Fatal(e)
		}
		o, e := node.NewOwner(db, node.DefaultConfig("p"))
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() {
			c, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_ = o.Close(c)
		})
		return db, o
	}
	db, o := open()
	for _, remove := range []bool{false, true} {
		var h *native.WriteHandle
		if remove {
			h, e = db.Delete([]byte("maintenance/deleted"))
		} else {
			h, e = db.Put([]byte("maintenance/deleted"), bytes.Repeat([]byte{42}, fixture.Payload))
		}
		if e != nil {
			t.Fatal(e)
		}
		if e = h.AwaitDurable(); e != nil {
			t.Fatal(e)
		}
		h.Destroy()
	}
	request := func(id string, kind wire.ShardCommand_Kind, previous, next int64, data []byte) *wire.ShardRequest {
		c := &wire.ShardCommand{Kind: kind, ShardId: 17, PreviousRangeId: previous, RangeId: next, Data: data, Encoding: 1}
		raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
		h := sha256.Sum256(raw)
		return &wire.ShardRequest{ProtocolVersion: 1, Partition: "p", OperationId: id, CommandSha256: h[:], Command: c}
	}
	first := request("initial", wire.ShardCommand_CREATE_OR_GET, 0, 1, bytes.Repeat([]byte{1}, fixture.Payload))
	if r, e := o.Execute(ctx, first); e != nil || r.Error != wire.ShardResult_NONE {
		t.Fatal(r, e)
	}
	if e = db.FlushWithOptions(native.FlushOptions{FlushType: native.FlushTypeMemTable}); e != nil {
		t.Fatal(e)
	}
	remember()
	snapshot, e := db.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	protected, e := snapshot.Get([]byte("v1/shard/0000000017"))
	if e != nil || protected == nil {
		t.Fatal("snapshot missing", e)
	}
	snapshotAlive := true
	defer func() {
		if snapshotAlive {
			snapshot.Destroy()
		}
	}()
	var last *wire.ShardRequest
	for i := 1; i <= fixture.Updates; i++ {
		last = request(fmt.Sprintf("update-%d", i), wire.ShardCommand_UPDATE, int64(i), int64(i+1), bytes.Repeat([]byte{byte(i + 1)}, fixture.Payload))
		if r, e := o.Execute(ctx, last); e != nil || r.Error != wire.ShardResult_NONE {
			t.Fatal(i, r, e)
		}
		if e = db.FlushWithOptions(native.FlushOptions{FlushType: native.FlushTypeMemTable}); e != nil {
			t.Fatal(e)
		}
		remember()
	}
	remember()
	adminBuilder := native.NewAdminBuilder(path, objects)
	defer adminBuilder.Destroy()
	admin, e := adminBuilder.Build()
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Destroy()
	observed := map[string]uint64{}
	for {
		for _, resource := range []string{"wal", "manifest", "compacted"} {
			m := recorder.MetricByNameAndLabels("slatedb.gc.deleted_count", []native.MetricLabel{{Key: "resource", Value: resource}})
			if m != nil {
				if v, ok := m.Value.(native.MetricValueCounter); ok {
					observed[resource] = v.Field0
				}
			}
		}
		manifest, e := admin.ReadManifest(nil)
		if e != nil {
			t.Fatal(e)
		}
		if observed["wal"] > 0 && observed["manifest"] > 0 && manifest != nil && manifest.LastCompactedL0SstViewId != nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("maintenance not observed: %v %v", observed, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	got, e := snapshot.Get([]byte("v1/shard/0000000017"))
	if e != nil || got == nil || !bytes.Equal(*got, *protected) {
		t.Fatal("protected snapshot changed after GC", e)
	}
	// A live snapshot pins old SSTs. Release only after proving its stable view;
	// obsolete SST collection must then actually make progress.
	snapshot.Destroy()
	snapshotAlive = false
	for {
		m := recorder.MetricByNameAndLabels("slatedb.gc.deleted_count", []native.MetricLabel{{Key: "resource", Value: "compacted"}})
		if m != nil {
			if v, ok := m.Value.(native.MetricValueCounter); ok {
				observed["compacted"] = v.Field0
			}
		}
		if observed["compacted"] > 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("SST collection did not resume after snapshot release: %v", observed)
		case <-time.After(100 * time.Millisecond):
		}
	}
	remaining := list()
	deleted := map[string]int{}
	for key := range seen {
		if _, ok := remaining[key]; !ok {
			for _, class := range []string{"wal", "manifest", "compacted"} {
				if strings.Contains(key, "/"+class+"/") {
					deleted[class]++
				}
			}
		}
	}
	for _, class := range []string{"wal", "manifest", "compacted"} {
		if deleted[class] == 0 {
			t.Fatalf("no observed S3 deletion for %s: %v", class, deleted)
		}
	}
	// This acknowledged update remains in WAL rather than an explicit memtable flush.
	beforeWAL, e := admin.ReadManifest(nil)
	if e != nil || beforeWAL == nil {
		t.Fatal(e)
	}
	last = request("wal-only-final", wire.ShardCommand_UPDATE, int64(fixture.Updates+1), int64(fixture.Updates+2), []byte{99})
	if r, e := o.Execute(ctx, last); e != nil || r.Error != wire.ShardResult_NONE {
		t.Fatal(r, e)
	}
	afterWAL, e := admin.ReadManifest(nil)
	if e != nil || afterWAL == nil || afterWAL.LastL0Seq != beforeWAL.LastL0Seq {
		t.Fatal("final acknowledgement was not isolated to WAL", e)
	}
	t.Logf("observed S3 deletions: %v", deleted)
	t.Logf("observed actual GC deletions and committed compaction: %v", observed)
	_, rival := open()
	if _, e = o.Execute(ctx, last); status.Code(e) != codes.Unavailable {
		t.Fatal("old owner survived fence", e)
	}
	replay, e := rival.Execute(ctx, last)
	if e != nil || replay.Error != wire.ShardResult_NONE {
		t.Fatal("acknowledged replay lost", replay, e)
	}
	fences := map[string]bool{}
	for key, size := range list() {
		if size == 0 && strings.Contains(key, "/wal/") {
			fences[key] = true
		}
	}
	if len(fences) == 0 {
		t.Fatal("no native WAL fence object observed")
	}
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(2200 * time.Millisecond):
	}
	afterFenceGC := list()
	for key := range fences {
		if _, ok := afterFenceGC[key]; !ok {
			t.Fatalf("fence deleted by maintenance: %s", key)
		}
	}
	t.Logf("retained %d observed WAL fences past accelerated minimum age", len(fences))
	c, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if e = rival.Close(c); e != nil {
		t.Fatal(e)
	}
	recoveredDB, reopened := open()
	deletedValue, e := recoveredDB.Get([]byte("maintenance/deleted"))
	if e != nil || deletedValue != nil {
		t.Fatal("deleted record resurrected after maintenance", e)
	}
	r, e := reopened.Execute(ctx, request("final-read", wire.ShardCommand_GET, 0, 0, nil))
	if e != nil || r.RangeId != int64(fixture.Updates+2) || !bytes.Equal(r.Data, last.Command.Data) {
		t.Fatal("acknowledged state lost after GC/reopen", r, e)
	}
}
