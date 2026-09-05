package ownership

import (
	"context"
	"encoding/json"
	"github.com/0x63616c/xenon/internal/directory"
	"github.com/0x63616c/xenon/internal/node"
	"net/http"
	"time"
)

type LocalDispatch struct {
	Attempts             uint64 `json:"attempts"`
	RPCResults           uint64 `json:"rpc_results"`
	SuccessfulOperations uint64 `json:"successful_operations"`
}
type PartitionOutcomes struct {
	OwnerRecord   *directory.Record  `json:"owner_record,omitempty"`
	Usage         *node.OutcomeUsage `json:"usage,omitempty"`
	LocalDispatch LocalDispatch      `json:"local_dispatch"`
	Error         string             `json:"error,omitempty"`
}
type OutcomesReport struct {
	SchemaVersion int                          `json:"schema_version"`
	Node          string                       `json:"node"`
	Incarnation   string                       `json:"incarnation"`
	Address       string                       `json:"address"`
	Partitions    map[string]PartitionOutcomes `json:"partitions"`
}

func (m *Manager) Outcomes(ctx context.Context) OutcomesReport {
	report := OutcomesReport{SchemaVersion: 1, Node: m.identity.Node, Incarnation: m.identity.Incarnation, Address: m.identity.Address, Partitions: map[string]PartitionOutcomes{}}
	m.mu.Lock()
	owners := map[string]*managed{}
	for id, o := range m.owners {
		owners[id] = o
	}
	for id, count := range m.dispatchCounts {
		report.Partitions[id] = PartitionOutcomes{LocalDispatch: count}
	}
	m.mu.Unlock()
	for id, o := range owners {
		entry := report.Partitions[id]
		record := o.record
		entry.OwnerRecord = &record
		usage, e := o.owner.OutcomeUsage(ctx)
		entry.Usage = usage
		if e != nil {
			entry.Error = e.Error()
		}
		report.Partitions[id] = entry
	}
	return report
}

// OutcomesHandler exposes bounded aggregate reads, never a routing/admission grant.
// Metrics scrapes do not count as locally served persistence operations.
func (m *Manager) OutcomesHandler() http.Handler {
	slots := make(chan struct{}, 4)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/outcomes" {
			http.NotFound(w, r)
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			http.Error(w, "metrics admission full", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		report := m.Outcomes(ctx)
		w.Header().Set("Content-Type", "application/json")
		for _, p := range report.Partitions {
			if p.Error != "" {
				w.WriteHeader(http.StatusServiceUnavailable)
				break
			}
		}
		_ = json.NewEncoder(w).Encode(report)
	})
}
