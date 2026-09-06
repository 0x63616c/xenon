package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type clusterWriter struct {
	*testWriter
	getErr error
}
type clusterTx struct {
	*testTx
	getErr error
}

func (w *clusterWriter) Begin(context.Context) (partitions.Transaction, error) {
	return &clusterTx{testTx: &testTx{writer: w.testWriter, data: maps.Clone(w.durable)}, getErr: w.getErr}, nil
}
func (tx *clusterTx) Get(ctx context.Context, key []byte) ([]byte, error) {
	if tx.getErr != nil {
		return nil, tx.getErr
	}
	return tx.testTx.Get(ctx, key)
}
func (tx *clusterTx) Scan(_ context.Context, r partitions.ScanRequest) (partitions.ReadResult, error) {
	result := partitions.ReadResult{}
	for _, key := range slices.Sorted(maps.Keys(tx.data)) {
		b := []byte(key)
		if r.Start != nil && (bytes.Compare(b, r.Start) < 0 || r.StartExclusive && bytes.Equal(b, r.Start)) {
			continue
		}
		if r.End != nil && (bytes.Compare(b, r.End) > 0 || !r.EndInclusive && bytes.Equal(b, r.End)) {
			continue
		}
		if len(result.Entries) == r.Limit {
			result.More = true
			break
		}
		result.Entries = append(result.Entries, partitions.Entry{Key: bytes.Clone(b), Value: bytes.Clone(tx.data[key])})
	}
	return result, nil
}
func clusterCommand(id int, c *wire.ClusterCommand) *wire.ClusterRequest {
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	digest := sha256.Sum256(raw)
	return &wire.ClusterRequest{ProtocolVersion: 1, Partition: "prt_0000000000000000000001", OperationId: fmt.Sprintf("op_%022d", id), Command: c, CommandSha256: digest[:]}
}
func clusterService(t *testing.T, w *clusterWriter, now func() time.Time) *ClusterService {
	t.Helper()
	base, err := NewService(w, "prt_0000000000000000000001", 1000, func(context.Context) error { return nil }, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewClusterService(base, now)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func clusterExecute(t *testing.T, s *ClusterService, id int, c *wire.ClusterCommand) *wire.ClusterResult {
	t.Helper()
	r, err := s.Execute(context.Background(), clusterCommand(id, c))
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestClusterDurableReplayRetainsCASResultAndAccounting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		allow, submitted := make(chan struct{}), make(chan struct{})
		w := &clusterWriter{testWriter: &testWriter{durable: map[string][]byte{}, allow: allow, submitted: submitted}}
		s := clusterService(t, w, func() time.Time { return time.Unix(100, 0) })
		save := &wire.ClusterCommand{Kind: wire.ClusterCommand_SAVE, ClusterName: "alpha", Blob: &wire.ClusterBlob{Data: []byte("original")}}
		done := make(chan error, 1)
		go func() { _, err := s.Execute(context.Background(), clusterCommand(1, save)); done <- err }()
		<-submitted
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatal("reply before durability", err)
		default:
		}
		if len(w.durable) != 0 {
			t.Fatal("volatile effects recovered")
		}
		close(allow)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if binary.BigEndian.Uint64(w.durable["v1/outcome_count"]) != 1 {
			t.Fatal("missing atomic accounting")
		}
		rejected := clusterExecute(t, s, 2, save)
		if rejected.Error != wire.ClusterResult_UNAVAILABLE {
			t.Fatal("CAS failure changed", rejected)
		}
		updated := proto.Clone(save).(*wire.ClusterCommand)
		updated.Version = 1
		updated.Blob.Data = []byte("updated")
		if !clusterExecute(t, s, 3, updated).Applied {
			t.Fatal("versioned update failed")
		}
		recovered := &clusterWriter{testWriter: &testWriter{durable: maps.Clone(w.durable)}}
		s = clusterService(t, recovered, func() time.Time { return time.Unix(10000, 0) })
		replayed := clusterExecute(t, s, 2, save)
		if !proto.Equal(rejected, replayed) || recovered.commits != 1 || len(recovered.durable["v1/barrier"]) != 1 || binary.BigEndian.Uint64(recovered.durable["v1/outcome_count"]) != 3 {
			t.Fatal("logical replay changed or skipped nonempty barrier")
		}
		if _, err := s.Execute(context.Background(), clusterCommand(2, updated)); status.Code(err) != codes.InvalidArgument {
			t.Fatal("changed digest accepted", err)
		}
		got := clusterExecute(t, s, 4, &wire.ClusterCommand{Kind: wire.ClusterCommand_GET, ClusterName: "alpha"})
		if got.Record.Version != 2 || string(got.Record.Blob.Data) != "updated" {
			t.Fatal("replay reapplied mutation", got)
		}
	})
}
func TestClusterPaginationPreservesBinarySuffixesAndMemberExpiry(t *testing.T) {
	now := time.Unix(1000, 123).UTC()
	w := &clusterWriter{testWriter: &testWriter{durable: map[string][]byte{}}}
	s := clusterService(t, w, func() time.Time { return now })
	names := []string{"", "a", "a\x00", "z"}
	for i, name := range names {
		clusterExecute(t, s, i+1, &wire.ClusterCommand{Kind: wire.ClusterCommand_SAVE, ClusterName: name, Blob: &wire.ClusterBlob{Data: []byte(name)}})
	}
	var token []byte
	var seen []string
	for page := 0; page < 10; page++ {
		r := clusterExecute(t, s, 10+page, &wire.ClusterCommand{Kind: wire.ClusterCommand_LIST, PageSize: 1, NextPageToken: token})
		for _, record := range r.Records {
			seen = append(seen, string(record.Blob.Data))
		}
		token = r.NextPageToken
		if token == nil {
			break
		}
	}
	if !slices.Equal(seen, names) {
		t.Fatal("page token skipped or duplicated suffix", seen)
	}
	upsert := &wire.ClusterCommand{Kind: wire.ClusterCommand_UPSERT_MEMBER, Member: &wire.ClusterMemberRecord{HostId: bytes.Repeat([]byte{255}, 16), Role: 1, RpcAddress: []byte{127, 0, 0, 1}, RpcPort: 7233, SessionStart: clusterStamp(now)}, RecordExpiryNanos: int64(time.Hour)}
	clusterExecute(t, s, 30, upsert)
	now = now.Add(30 * time.Minute)
	clusterExecute(t, s, 30, upsert) // replay must not extend expiry or heartbeat
	members := clusterExecute(t, s, 31, &wire.ClusterCommand{Kind: wire.ClusterCommand_GET_MEMBERS, SessionStartedAfter: clusterStamp(time.Time{}), PageSize: 1})
	if len(members.Members) != 1 || !instant(members.Members[0].RecordExpiry).Equal(now.Add(30*time.Minute)) {
		t.Fatal("replay changed membership time", members)
	}
	now = now.Add(30 * time.Minute)
	if got := clusterExecute(t, s, 32, &wire.ClusterCommand{Kind: wire.ClusterCommand_GET_MEMBERS, SessionStartedAfter: clusterStamp(time.Time{})}); len(got.Members) != 0 {
		t.Fatal("expiry boundary admitted member")
	}
	clusterExecute(t, s, 33, &wire.ClusterCommand{Kind: wire.ClusterCommand_PRUNE_MEMBERS})
	if _, exists := w.durable[memberPrefix+string(upsert.Member.HostId)]; !exists {
		t.Fatal("prune changed strict-before boundary")
	}
	now = now.Add(time.Nanosecond)
	clusterExecute(t, s, 34, &wire.ClusterCommand{Kind: wire.ClusterCommand_PRUNE_MEMBERS})
	if _, exists := w.durable[memberPrefix+string(upsert.Member.HostId)]; exists {
		t.Fatal("expired member retained")
	}
}
func TestClusterTypedFailureAndCrossFamilyIdentity(t *testing.T) {
	w := &clusterWriter{testWriter: &testWriter{durable: map[string][]byte{}}, getErr: partitions.ErrFenced}
	s := clusterService(t, w, func() time.Time { return time.Time{} })
	var notified error
	s.service.failure = func(err error) { notified = err }
	request := clusterCommand(1, &wire.ClusterCommand{Kind: wire.ClusterCommand_GET, ClusterName: "alpha"})
	if _, err := s.Execute(context.Background(), request); !errors.Is(err, partitions.ErrFenced) || !errors.Is(notified, partitions.ErrFenced) || w.commits != 0 {
		t.Fatal("typed fencing error lost", err, notified)
	}
	w.getErr = nil
	foreign := &wire.StoredOutcome{CommandSha256: request.CommandSha256, Result: &wire.StoredOutcome_ShardResult{ShardResult: &wire.ShardResult{ShardId: 1}}}
	raw, _ := proto.Marshal(foreign)
	w.durable["v1/outcome/"+request.OperationId] = raw
	if _, err := s.Execute(context.Background(), request); status.Code(err) != codes.InvalidArgument || w.commits != 0 {
		t.Fatal("foreign family replay accepted", err)
	}
	delete(w.durable, "v1/outcome/"+request.OperationId)
	s.service.authority = func(context.Context) error { return partitions.ErrRetired }
	if _, err := s.Execute(context.Background(), request); !errors.Is(err, partitions.ErrRetired) || w.commits != 0 {
		t.Fatal("authority failure staged mutation", err)
	}
}
