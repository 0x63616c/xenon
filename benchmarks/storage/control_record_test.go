// Research fixture, not a production record or migration format.
package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

type Owner struct {
	Node        identity.NodeID        `json:"node"`
	Incarnation identity.IncarnationID `json:"incarnation"`
}
type Coordinator struct {
	Owner           Owner  `json:"owner"`
	Generation      uint64 `json:"generation"`
	RenewalSequence uint64 `json:"renewal_sequence"`
}
type Reservation struct {
	Transition         identity.TransitionID `json:"transition"`
	Desired            Owner                 `json:"desired_owner"`
	AssignmentRevision uint64                `json:"assignment_revision"`
	Generation         uint64                `json:"generation"`
}
type Ready struct {
	Owner              Owner  `json:"owner"`
	AssignmentRevision uint64 `json:"assignment_revision"`
	Generation         uint64 `json:"generation"`
}
type Partition struct {
	DataPrefix  string      `json:"data_prefix"`
	Desired     Owner       `json:"desired_owner"`
	Reservation Reservation `json:"reservation"`
	Ready       Ready       `json:"ready"`
}

// Exact concepts from dst-service-contracts.md, with explicit synthetic JSON
// field names/representation. Heartbeats are separate advisory records. Desired,
// reservation and ready are all populated (including old ready during movement).
// This is NOT the final production format and excludes retained historical receipts.
type Control struct {
	Format             uint32                             `json:"format"`
	Cluster            identity.ClusterID                 `json:"cluster"`
	Coordinator        Coordinator                        `json:"coordinator"`
	AssignmentRevision uint64                             `json:"assignment_revision"`
	Partitions         map[identity.PartitionID]Partition `json:"partitions"`
}
type config struct {
	Version    int              `json:"version"`
	Partitions []int            `json:"partition_counts"`
	Instances  int              `json:"instances"`
	Counter    uint64           `json:"counter_value"`
	Expected   registry.Version `json:"expected_version"`
	Budget     int              `json:"registry_record_budget_bytes"`
	Shape      string           `json:"shape"`
}

func input(t testing.TB) config {
	t.Helper()
	b, err := os.ReadFile("control_record.json")
	if err != nil {
		t.Fatal(err)
	}
	var c config
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		t.Fatal(err)
	}
	if c.Version != 1 || c.Instances <= 0 || c.Counter < 2 {
		t.Fatal("invalid config")
	}
	return c
}
func fixture(count int, c config) Control {
	owner := func(i int) Owner {
		return Owner{identity.NodeID(fmt.Sprintf("nod_%022d", i+1)), identity.IncarnationID(fmt.Sprintf("inc_%022d", i+1))}
	}
	out := Control{Format: 1, Cluster: "clu_0000000000000000000001", Coordinator: Coordinator{owner(0), c.Counter, c.Counter}, AssignmentRevision: c.Counter, Partitions: make(map[identity.PartitionID]Partition, count)}
	for i := 0; i < count; i++ {
		id := identity.PartitionID(fmt.Sprintf("prt_%022d", i+1))
		desired := owner(i % c.Instances)
		prior := owner((i + 1) % c.Instances)
		out.Partitions[id] = Partition{DataPrefix: fmt.Sprintf("v2/%s/%s", out.Cluster, id), Desired: desired, Reservation: Reservation{identity.TransitionID(fmt.Sprintf("trn_%022d", i+1)), desired, c.Counter, c.Counter}, Ready: Ready{prior, c.Counter - 1, c.Counter - 1}}
	}
	return out
}
func publication(t testing.TB, c config, payload []byte) []byte {
	t.Helper()
	w, err := registry.NewWrite("control", c.Expected, "trn_0000000000000000999999", payload)
	if err != nil {
		t.Fatal(err)
	}
	b, err := registry.Encode("control", c.Expected, w)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type sizeResult struct {
	Partitions     int    `json:"partitions"`
	Instances      int    `json:"instances"`
	PayloadBytes   int    `json:"payload_bytes"`
	EnvelopeBytes  int    `json:"registry_envelope_bytes"`
	WithinBudget   bool   `json:"within_configured_1mib_budget"`
	PayloadSHA256  string `json:"payload_sha256"`
	EnvelopeSHA256 string `json:"envelope_sha256"`
}

func TestControlRecordMeasurements(t *testing.T) {
	c := input(t)
	var results []sizeResult
	for _, count := range c.Partitions {
		record := fixture(count, c)
		for id, p := range record.Partitions {
			if err := id.Validate(); err != nil {
				t.Fatal(err)
			}
			if err := p.Desired.Node.Validate(); err != nil {
				t.Fatal(err)
			}
			if err := p.Desired.Incarnation.Validate(); err != nil {
				t.Fatal(err)
			}
			if err := p.Reservation.Transition.Validate(); err != nil {
				t.Fatal(err)
			}
		}
		payload, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		again, err := json.Marshal(fixture(count, c))
		if err != nil || !bytes.Equal(payload, again) {
			t.Fatal("nondeterministic serialization", err)
		}
		envelope := publication(t, c, payload)
		decoded, err := registry.Decode("control", registry.Record{Body: envelope, Version: "opaque-next-version"})
		if err != nil || !bytes.Equal(decoded.Body, payload) {
			t.Fatal("envelope roundtrip", err)
		}
		a, b := sha256.Sum256(payload), sha256.Sum256(envelope)
		results = append(results, sizeResult{count, c.Instances, len(payload), len(envelope), len(envelope) <= c.Budget, hex.EncodeToString(a[:]), hex.EncodeToString(b[:])})
	}
	if output := os.Getenv("CONTROL_RECORD_RESULTS"); output != "" {
		b, _ := json.MarshalIndent(results, "", "  ")
		if err := os.WriteFile(output, append(b, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range results {
		t.Logf("partitions=%d instances=%d payload_bytes=%d envelope_bytes=%d within_1MiB=%v", r.Partitions, r.Instances, r.PayloadBytes, r.EnvelopeBytes, r.WithinBudget)
	}
}
func BenchmarkControlRecord(b *testing.B) {
	c := input(b)
	for _, count := range c.Partitions {
		record := fixture(count, c)
		payload, err := json.Marshal(record)
		if err != nil {
			b.Fatal(err)
		}
		envelope := publication(b, c, payload)
		b.Run(fmt.Sprintf("marshal/partitions=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for b.Loop() {
				if _, err := json.Marshal(record); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("publication/partitions=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(envelope)))
			for b.Loop() {
				publication(b, c, payload)
			}
		})
		b.Run(fmt.Sprintf("decode/partitions=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(envelope)))
			for b.Loop() {
				if _, err := registry.Decode("control", registry.Record{Body: envelope, Version: "next"}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
