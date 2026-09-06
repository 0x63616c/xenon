package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"maps"
	"testing"
	"testing/synctest"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func metadataCommand(id int, c *wire.MetadataCommand) *wire.MetadataRequest {
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	digest := sha256.Sum256(raw)
	return &wire.MetadataRequest{ProtocolVersion: 1, Partition: "prt_0000000000000000000001", OperationId: fmt.Sprintf("op_%022d", id), Command: c, CommandSha256: digest[:]}
}
func metadataService(t *testing.T, w *clusterWriter) *MetadataService {
	t.Helper()
	base, err := NewService(w, "prt_0000000000000000000001", 1000, func(context.Context) error { return nil }, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewMetadataService(base)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func metadataExecute(t *testing.T, s *MetadataService, id int, c *wire.MetadataCommand) *wire.MetadataResult {
	t.Helper()
	r, err := s.Execute(context.Background(), metadataCommand(id, c))
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestNamespaceAtomicIndexesDurabilityAndReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		allow, submitted := make(chan struct{}), make(chan struct{})
		w := &clusterWriter{testWriter: &testWriter{durable: map[string][]byte{}, allow: allow, submitted: submitted}}
		s := metadataService(t, w)
		id := bytes.Repeat([]byte{1}, 16)
		create := &wire.MetadataCommand{Kind: wire.MetadataCommand_CREATE, Id: id, Name: "alpha", Data: []byte("initial"), IsGlobal: true}
		done := make(chan error, 1)
		go func() { _, err := s.Execute(context.Background(), metadataCommand(1, create)); done <- err }()
		<-submitted
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatal("reply before durability", err)
		default:
		}
		if len(w.durable) != 0 {
			t.Fatal("partial namespace indexes recovered")
		}
		close(allow)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(w.durable[namespaceNameKey("alpha")], id) || w.durable[namespaceIDKey(id)] == nil || binary.BigEndian.Uint64(w.durable["v1/namespace/notification"]) != 2 {
			t.Fatal("indexes and version not committed atomically")
		}
		duplicate := proto.Clone(create).(*wire.MetadataCommand)
		duplicate.Id = bytes.Repeat([]byte{2}, 16)
		if got := metadataExecute(t, s, 2, duplicate); got.Error != wire.MetadataResult_ALREADY_EXISTS {
			t.Fatal("unique name lost", got)
		}
		rename := &wire.MetadataCommand{Kind: wire.MetadataCommand_RENAME, Id: id, Name: "beta", PreviousName: "intentionally-ignored", Data: []byte("changed"), NotificationVersion: 1}
		failed := metadataExecute(t, s, 3, rename)
		if failed.Error != wire.MetadataResult_CONDITION_FAILED {
			t.Fatal("wrong version accepted", failed)
		}
		good := proto.Clone(rename).(*wire.MetadataCommand)
		good.NotificationVersion = 2
		if got := metadataExecute(t, s, 4, good); got.Error != wire.MetadataResult_NONE {
			t.Fatal("PreviousName became an extra condition", got)
		}
		if w.durable[namespaceNameKey("alpha")] != nil || !bytes.Equal(w.durable[namespaceNameKey("beta")], id) {
			t.Fatal("rename failed to replace actual index")
		}
		recovered := &clusterWriter{testWriter: &testWriter{durable: maps.Clone(w.durable)}}
		s = metadataService(t, recovered)
		if got := metadataExecute(t, s, 3, rename); !proto.Equal(got, failed) || recovered.commits != 1 || len(recovered.durable["v1/barrier"]) != 1 || binary.BigEndian.Uint64(recovered.durable["v1/outcome_count"]) != 4 {
			t.Fatal("logical replay changed or skipped barrier", got)
		}
		if _, err := s.Execute(context.Background(), metadataCommand(3, good)); status.Code(err) != codes.InvalidArgument {
			t.Fatal("changed digest accepted", err)
		}
		metadataExecute(t, s, 5, &wire.MetadataCommand{Kind: wire.MetadataCommand_DELETE_BY_NAME, Name: "beta"})
		if recovered.durable[namespaceNameKey("beta")] != nil || recovered.durable[namespaceIDKey(id)] != nil || binary.BigEndian.Uint64(recovered.durable["v1/namespace/notification"]) != 3 {
			t.Fatal("delete index/version semantics changed")
		}
		metadataExecute(t, s, 4, good)
		if recovered.durable[namespaceNameKey("beta")] != nil {
			t.Fatal("replay resurrected deleted namespace")
		}
	})
}
func TestNamespaceEnumerationByteBudgetAndValidation(t *testing.T) {
	w := &clusterWriter{testWriter: &testWriter{durable: map[string][]byte{}}}
	s := metadataService(t, w)
	for i := 0; i < 4; i++ {
		metadataExecute(t, s, i+1, &wire.MetadataCommand{Kind: wire.MetadataCommand_CREATE, Id: bytes.Repeat([]byte{byte(i)}, 16), Name: fmt.Sprintf("namespace-%d", i), Data: bytes.Repeat([]byte{byte(i)}, 1024*1024)})
	}
	first := metadataExecute(t, s, 5, &wire.MetadataCommand{Kind: wire.MetadataCommand_LIST, PageSize: 1000})
	if len(first.Namespaces) != 2 || proto.Size(first) > 3*1024*1024 || !bytes.Equal(first.NextPageToken, bytes.Repeat([]byte{1}, 16)) {
		t.Fatal("response bound or token changed")
	}
	next := metadataExecute(t, s, 6, &wire.MetadataCommand{Kind: wire.MetadataCommand_LIST, PageSize: 1000, NextPageToken: first.NextPageToken})
	if len(next.Namespaces) != 2 || next.Namespaces[0].Id[0] != 2 || next.NextPageToken != nil {
		t.Fatal("exclusive namespace enumeration boundary")
	}
	commits := w.commits
	for _, command := range []*wire.MetadataCommand{
		{Kind: wire.MetadataCommand_LIST, PageSize: 0},
		{Kind: wire.MetadataCommand_LIST, PageSize: 1, NextPageToken: []byte{1}},
		{Kind: wire.MetadataCommand_GET, Id: bytes.Repeat([]byte{1}, 16), Name: "both"},
	} {
		if _, err := s.Execute(context.Background(), metadataCommand(7, command)); status.Code(err) != codes.InvalidArgument {
			t.Fatal("invalid command accepted", err)
		}
	}
	if w.commits != commits {
		t.Fatal("invalid ingress committed outcome")
	}
	if got := metadataExecute(t, s, 8, &wire.MetadataCommand{Kind: wire.MetadataCommand_GET_METADATA}); got.NotificationVersion != 5 {
		t.Fatal("enumeration changed notification version", got)
	}
}
