// Standalone research module: these candidates do not enter production go.mod.
package placement

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"testing"

	consistent "github.com/buraksezer/consistent"
	rendezvous "github.com/dgryski/go-rendezvous"
)

type config struct {
	Version        int        `json:"version"`
	Partitions     []int      `json:"partitions"`
	BenchmarkNodes []int      `json:"benchmark_nodes"`
	Replication    int        `json:"replication_factor"`
	Load           float64    `json:"load_factor"`
	Hash           string     `json:"hash"`
	Weights        []string   `json:"weights"`
	Scenarios      []scenario `json:"scenarios"`
}
type scenario struct {
	Name    string   `json:"name"`
	Initial int      `json:"initial_nodes"`
	Changes []change `json:"changes"`
}
type change struct {
	Add   bool `json:"add"`
	First int  `json:"first"`
	Count int  `json:"count"`
}

func inputs(t testing.TB) config {
	t.Helper()
	b, err := os.ReadFile("scenarios.json")
	if err != nil {
		t.Fatal(err)
	}
	var c config
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		t.Fatal(err)
	}
	if c.Version != 1 || c.Hash != "SHA256 first 8 bytes big endian" || c.Replication <= 0 || c.Load < 1 {
		t.Fatal("invalid configuration")
	}
	return c
}

type hasher struct{}

func (hasher) Sum64(b []byte) uint64 { h := sha256.Sum256(b); return binary.BigEndian.Uint64(h[:8]) }
func hashString(s string) uint64     { return (hasher{}).Sum64([]byte(s)) }

type member string

func (m member) String() string { return string(m) }
func nodes(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("node-%06d", i)
	}
	return out
}
func partitions(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("partition-%06d", i)
	}
	return out
}

var candidates = []string{"sorted_round_robin", "bounded_consistent", "rendezvous"}

// plan reconstructs from a canonical membership snapshot. It never relies on a
// prior process's library object/history. Index i is a fixed logical slot; using
// LocateKey(partitionName) would hash slots again and lose the count-load bound.
func plan(candidate string, keys, members []string, c config) []string {
	members = slices.Clone(members)
	slices.Sort(members)
	out := make([]string, len(keys))
	switch candidate {
	case "sorted_round_robin":
		// Exact sorted assignment rule in internal/ownership/join.go:rebalance.
		ordered := slices.Clone(keys)
		slices.Sort(ordered)
		byKey := make(map[string]string, len(keys))
		for i, k := range ordered {
			byKey[k] = members[i%len(members)]
		}
		for i, k := range keys {
			out[i] = byKey[k]
		}
	case "bounded_consistent":
		ms := make([]consistent.Member, len(members))
		for i, m := range members {
			ms[i] = member(m)
		}
		ring := consistent.New(ms, consistent.Config{PartitionCount: len(keys), ReplicationFactor: c.Replication, Load: c.Load, Hasher: hasher{}, ReplicaKey: consistent.DefaultReplicaKey})
		for i := range keys {
			out[i] = ring.GetPartitionOwner(i).String()
		}
	case "rendezvous":
		ring := rendezvous.New(members, hashString)
		for i, k := range keys {
			out[i] = ring.Lookup(k)
		}
	default:
		panic("unknown candidate")
	}
	return out
}
func apply(t testing.TB, members []string, c change) []string {
	t.Helper()
	set := map[string]bool{}
	for _, m := range members {
		set[m] = true
	}
	for i := c.First; i < c.First+c.Count; i++ {
		name := fmt.Sprintf("node-%06d", i)
		if set[name] == c.Add {
			t.Fatal("invalid membership change", c, name)
		}
		if c.Add {
			set[name] = true
		} else {
			delete(set, name)
		}
	}
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	slices.Sort(out)
	if len(out) == 0 {
		t.Fatal("empty membership")
	}
	return out
}
func assignmentHash(a []string) string {
	b, _ := json.Marshal(a)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

type metrics struct {
	Candidate             string             `json:"candidate"`
	Scenario              string             `json:"scenario"`
	Step                  int                `json:"step"`
	Partitions            int                `json:"partitions"`
	Nodes                 int                `json:"nodes"`
	AssignmentSHA256      string             `json:"assignment_sha256"`
	Moved                 int                `json:"moved"`
	MovedFraction         float64            `json:"moved_fraction"`
	MinOwned              int                `json:"min_owned"`
	MaxOwned              int                `json:"max_owned"`
	MeanOwned             float64            `json:"mean_owned"`
	MaxMean               float64            `json:"max_mean_owned"`
	WeightedMaxMean       map[string]float64 `json:"weighted_max_mean"`
	WeightedMovedFraction map[string]float64 `json:"weighted_moved_fraction"`
}

func weight(profile string, index, count int) float64 {
	switch profile {
	case "uniform":
		return 1
	case "single_hot_1000":
		if index == 0 {
			return 1000
		}
		return 1
	case "one_percent_hot_100":
		if index < (count+99)/100 {
			return 100
		}
		return 1
	default:
		panic("unknown weights")
	}
}
func measure(t testing.TB, c config, candidate string, sc scenario, step int, members, got, prior []string) metrics {
	t.Helper()
	counts := map[string]int{}
	for _, m := range members {
		counts[m] = 0
	}
	m := metrics{Candidate: candidate, Scenario: sc.Name, Step: step, Partitions: len(got), Nodes: len(members), AssignmentSHA256: assignmentHash(got), MinOwned: len(got), MeanOwned: float64(len(got)) / float64(len(members)), WeightedMaxMean: map[string]float64{}, WeightedMovedFraction: map[string]float64{}}
	for i, owner := range got {
		if _, ok := counts[owner]; !ok {
			t.Fatal("unknown owner", owner)
		}
		counts[owner]++
		if prior != nil && prior[i] != owner {
			m.Moved++
		}
	}
	for _, count := range counts {
		m.MinOwned = min(m.MinOwned, count)
		m.MaxOwned = max(m.MaxOwned, count)
	}
	m.MaxMean = float64(m.MaxOwned) / m.MeanOwned
	m.MovedFraction = float64(m.Moved) / float64(len(got))
	if candidate == "bounded_consistent" && m.MaxOwned > int(math.Ceil(m.MeanOwned*c.Load)) {
		t.Fatal("count bound exceeded", m)
	}
	for _, profile := range c.Weights {
		loads := map[string]float64{}
		total, moved := 0.0, 0.0
		for i, owner := range got {
			w := weight(profile, i, len(got))
			loads[owner] += w
			total += w
			if prior != nil && prior[i] != owner {
				moved += w
			}
		}
		maxLoad := 0.0
		for _, v := range loads {
			maxLoad = max(maxLoad, v)
		}
		m.WeightedMaxMean[profile] = maxLoad / (total / float64(len(members)))
		m.WeightedMovedFraction[profile] = moved / total
	}
	return m
}
func TestMeasurements(t *testing.T) {
	c := inputs(t)
	var results []metrics
	for _, count := range c.Partitions {
		keys := partitions(count)
		for _, candidate := range candidates {
			for _, sc := range c.Scenarios {
				members := nodes(sc.Initial)
				var prior []string
				seen := map[string]string{}
				for step := 0; step <= len(sc.Changes); step++ {
					if step > 0 {
						members = apply(t, members, sc.Changes[step-1])
					}
					got := plan(candidate, keys, members, c)
					reversed := slices.Clone(members)
					slices.Reverse(reversed)
					if !slices.Equal(got, plan(candidate, keys, reversed, c)) {
						t.Fatal("input-order nondeterminism", candidate)
					}
					repeat := plan(candidate, keys, members, c)
					if !slices.Equal(got, repeat) {
						t.Fatal("repeat nondeterminism", candidate)
					}
					membership := strings.Join(members, ",")
					hash := assignmentHash(got)
					if old, ok := seen[membership]; ok && old != hash {
						t.Fatal("same membership changed after churn", candidate)
					}
					seen[membership] = hash
					results = append(results, measure(t, c, candidate, sc, step, members, got, prior))
					prior = got
				}
			}
		}
	}
	if output := os.Getenv("PLACEMENT_RESULTS"); output != "" {
		f, err := os.Create(output)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		for _, row := range results {
			if err := enc.Encode(row); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Logf("%d deterministic snapshots checked across repeated joins/departures and workload skew", len(results))
}
func TestRendezvousRemoveProbe(t *testing.T) {
	// Keep the actual candidate defect visible. This is an observed limitation,
	// not acceptance of Remove. The measured planner always reconstructs snapshots.
	r := rendezvous.New(nodes(3), hashString)
	var recovered any
	func() { defer func() { recovered = recover() }(); r.Remove("node-000001") }()
	if recovered == nil {
		t.Fatal("pinned Remove behavior changed; revisit documented limitation")
	}
	t.Logf("pinned mutable Remove is unusable: %v", recovered)
}
func BenchmarkPlan(b *testing.B) {
	c := inputs(b)
	for _, count := range c.Partitions {
		keys := partitions(count)
		for _, n := range c.BenchmarkNodes {
			members := nodes(n)
			for _, candidate := range candidates {
				b.Run(fmt.Sprintf("%s/partitions=%d/nodes=%d", candidate, count, n), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if got := plan(candidate, keys, members, c); len(got) != count {
							b.Fatal("missing assignments")
						}
					}
				})
			}
		}
	}
}
