package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math"
	"testing"
	"testing/synctest"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func nexusCommand(id int, c *wire.NexusCommand) *wire.NexusRequest {
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	digest := sha256.Sum256(raw)
	return &wire.NexusRequest{ProtocolVersion: 1, Partition: "prt_0000000000000000000001", OperationId: fmt.Sprintf("op_%022d", id), Command: c, CommandSha256: digest[:]}
}
func nexusService(t *testing.T, w *clusterWriter) *NexusService {
	t.Helper()
	base, err := NewService(w, "prt_0000000000000000000001", 1000, func(context.Context) error { return nil }, func(error) {})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewNexusService(base)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func nexusExecute(t *testing.T, s *NexusService, id int, c *wire.NexusCommand) *wire.NexusResult {
	t.Helper()
	r, err := s.Execute(context.Background(), nexusCommand(id, c))
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestNexusAtomicVersionsDurabilityAndReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		allow, submitted := make(chan struct{}), make(chan struct{})
		w := &clusterWriter{testWriter: &testWriter{durable: map[string][]byte{}, allow: allow, submitted: submitted}}
		s := nexusService(t, w)
		id := bytes.Repeat([]byte{1}, 16)
		create := &wire.NexusCommand{Endpoint: &wire.NexusEndpoint{Id: id, Data: []byte("original")}}
		done := make(chan error, 1)
		go func() { _, err := s.Execute(context.Background(), nexusCommand(1, create)); done <- err }()
		<-submitted
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatal("reply before durability", err)
		default:
		}
		if len(w.durable) != 0 {
			t.Fatal("volatile catalog recovered")
		}
		close(allow)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		failed := &wire.NexusCommand{TableVersion: 1, Endpoint: &wire.NexusEndpoint{Id: id, Version: 7}}
		prior := nexusExecute(t, s, 2, failed)
		if prior.Error != wire.NexusResult_UNAVAILABLE || prior.TableVersion != 1 || binary.BigEndian.Uint64(w.durable[nexusVersion]) != 1 {
			t.Fatal("endpoint failure advanced catalog", prior)
		}
		update := &wire.NexusCommand{TableVersion: 1, Endpoint: &wire.NexusEndpoint{Id: id, Version: 1, Data: []byte("updated")}}
		if got := nexusExecute(t, s, 3, update); got.TableVersion != 2 || got.Error != wire.NexusResult_NONE {
			t.Fatal(got)
		}
		recovered := &clusterWriter{testWriter: &testWriter{durable: maps.Clone(w.durable)}}
		s = nexusService(t, recovered)
		if got := nexusExecute(t, s, 2, failed); !proto.Equal(prior, got) || recovered.commits != 1 || len(recovered.durable["v1/barrier"]) != 1 || binary.BigEndian.Uint64(recovered.durable["v1/outcome_count"]) != 3 {
			t.Fatal("logical outcome changed or lacked durability barrier", got)
		}
		if _, err := s.Execute(context.Background(), nexusCommand(2, update)); status.Code(err) != codes.InvalidArgument {
			t.Fatal("changed identity digest accepted", err)
		}
		if got := nexusExecute(t, s, 4, &wire.NexusCommand{Kind: wire.NexusCommand_GET, Id: id}); got.Endpoint.Version != 2 || string(got.Endpoint.Data) != "updated" {
			t.Fatal("replay reapplied mutation", got)
		}
	})
}
func TestNexusCatalogPaginationValidationAndOverflow(t *testing.T) {
	w := &clusterWriter{testWriter: &testWriter{durable: map[string][]byte{}}}
	s := nexusService(t, w)
	for i := 0; i < 3; i++ {
		nexusExecute(t, s, i+1, &wire.NexusCommand{TableVersion: int64(i), Endpoint: &wire.NexusEndpoint{Id: bytes.Repeat([]byte{byte(i)}, 16), Data: []byte{byte(i)}}})
	}
	page := nexusExecute(t, s, 4, &wire.NexusCommand{Kind: wire.NexusCommand_LIST, TableVersion: 3, PageSize: 1})
	if len(page.Endpoints) != 1 || len(page.NextPageToken) != 17 || page.Endpoints[0].Id[0] != 0 {
		t.Fatal("first binary ID page", page)
	}
	next := nexusExecute(t, s, 5, &wire.NexusCommand{Kind: wire.NexusCommand_LIST, TableVersion: 3, PageSize: 1, NextPageToken: page.NextPageToken})
	if len(next.Endpoints) != 1 || next.Endpoints[0].Id[0] != 1 {
		t.Fatal("exclusive page boundary", next)
	}
	if got := nexusExecute(t, s, 6, &wire.NexusCommand{Kind: wire.NexusCommand_LIST, TableVersion: 2, PageSize: 1, NextPageToken: []byte{1}}); got.Error != wire.NexusResult_INTERNAL {
		t.Fatal("validation order changed", got)
	}
	if got := nexusExecute(t, s, 7, &wire.NexusCommand{Kind: wire.NexusCommand_LIST, TableVersion: 2, PageSize: 1}); got.Error != wire.NexusResult_UNAVAILABLE {
		t.Fatal("stale enumeration accepted", got)
	}
	if got := nexusExecute(t, s, 8, &wire.NexusCommand{Kind: wire.NexusCommand_DELETE, TableVersion: 3, Id: bytes.Repeat([]byte{1}, 16)}); got.TableVersion != 4 {
		t.Fatal(got)
	}
	if _, exists := w.durable[nexusPrefix+string(bytes.Repeat([]byte{1}, 16))]; exists {
		t.Fatal("delete retained endpoint")
	}
	version := make([]byte, 8)
	binary.BigEndian.PutUint64(version, math.MaxInt64)
	w.durable[nexusVersion] = version
	if got := nexusExecute(t, s, 9, &wire.NexusCommand{TableVersion: math.MaxInt64, Endpoint: &wire.NexusEndpoint{Id: bytes.Repeat([]byte{5}, 16)}}); got.Error != wire.NexusResult_RESOURCE_EXHAUSTED {
		t.Fatal("catalog version overflow", got)
	}
}
func TestNexusTypedFailureAndFamilyIsolation(t *testing.T) {
	w := &clusterWriter{testWriter: &testWriter{durable: map[string][]byte{}}, getErr: partitions.ErrFenced}
	s := nexusService(t, w)
	var notified error
	s.service.failure = func(err error) { notified = err }
	q := nexusCommand(1, &wire.NexusCommand{Kind: wire.NexusCommand_LIST, PageSize: 1})
	if _, err := s.Execute(context.Background(), q); !errors.Is(err, partitions.ErrFenced) || !errors.Is(notified, partitions.ErrFenced) {
		t.Fatal("typed native failure lost", err)
	}
	w.getErr = nil
	raw, _ := proto.Marshal(&wire.StoredOutcome{CommandSha256: q.CommandSha256, Result: &wire.StoredOutcome_ClusterResult{ClusterResult: &wire.ClusterResult{}}})
	w.durable["v1/outcome/"+q.OperationId] = raw
	if _, err := s.Execute(context.Background(), q); status.Code(err) != codes.InvalidArgument || w.commits != 0 {
		t.Fatal("cross-family outcome accepted", err)
	}
}
