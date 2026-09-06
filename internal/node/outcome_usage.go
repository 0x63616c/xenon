package node

import (
	"context"
	"encoding/binary"
	"encoding/json"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/persistence"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math"
	native "slatedb.io/slatedb-go/uniffi"
)

type OutcomeFamilyUsage struct {
	Entries             uint64 `json:"entries"`
	EncodedOutcomeBytes uint64 `json:"encoded_outcome_bytes"`
}
type OutcomeUsage struct {
	Partition                string                        `json:"partition"`
	Limit                    uint64                        `json:"limit"`
	Entries                  uint64                        `json:"entries"`
	Remaining                uint64                        `json:"remaining"`
	EncodedOutcomeBytes      uint64                        `json:"encoded_outcome_bytes"`
	UnaccountedLegacyEntries uint64                        `json:"unaccounted_legacy_entries"`
	AccountingComplete       bool                          `json:"accounting_complete"`
	Families                 map[string]OutcomeFamilyUsage `json:"families"`
}

func readOutcomeAccounting(tx *native.DbTransaction) (map[string]OutcomeFamilyUsage, error) {
	b, e := get(tx, "v1/outcome_usage")
	if e != nil {
		return nil, e
	}
	m := map[string]OutcomeFamilyUsage{}
	if b != nil {
		if len(b) > 65536 {
			return nil, status.Error(codes.Unavailable, "outcome accounting too large")
		}
		if e = json.Unmarshal(b, &m); e != nil || m == nil {
			return nil, status.Error(codes.Unavailable, "corrupt outcome accounting")
		}
	}
	return m, nil
}
func accountOutcome(tx *native.DbTransaction, outcome *wire.StoredOutcome, size int) error {
	return persistence.AccountOutcome(context.Background(), legacyShardTransaction{tx}, outcome, size)
}

// OutcomeUsage reads bounded aggregate metadata under normal owner admission and
// a durable fencing barrier. It does not create an outcome or grant authority.
// Byte totals cover encoded StoredOutcome values, excluding keys/native overhead.
func (o *Owner) OutcomeUsage(ctx context.Context) (*OutcomeUsage, error) {
	raw, e := o.Run(ctx, func(db *native.Db) ([]byte, error) {
		tx, e := db.Begin(native.IsolationLevelSerializableSnapshot)
		if e != nil {
			return nil, backend(e)
		}
		defer tx.Destroy()
		b, e := get(tx, "v1/outcome_count")
		if e != nil {
			return nil, e
		}
		var count uint64
		if b != nil {
			if len(b) != 8 {
				return nil, status.Error(codes.Unavailable, "corrupt outcome count")
			}
			count = binary.BigEndian.Uint64(b)
		}
		families, e := readOutcomeAccounting(tx)
		if e != nil {
			return nil, e
		}
		u := &OutcomeUsage{Partition: o.config.Partition, Limit: o.config.MaxOutcomes, Entries: count, Families: families}
		if count < u.Limit {
			u.Remaining = u.Limit - count
		}
		var known uint64
		for _, f := range families {
			if math.MaxUint64-known < f.Entries || math.MaxUint64-u.EncodedOutcomeBytes < f.EncodedOutcomeBytes {
				return nil, status.Error(codes.Unavailable, "corrupt outcome accounting overflow")
			}
			known += f.Entries
			u.EncodedOutcomeBytes += f.EncodedOutcomeBytes
		}
		if known > count {
			return nil, status.Error(codes.Unavailable, "outcome accounting exceeds journal count")
		}
		u.UnaccountedLegacyEntries = count - known
		u.AccountingComplete = known == count
		if e = put(tx, "v1/barrier", []byte(uuid.NewString())); e != nil {
			return nil, e
		}
		if e = commit(tx); e != nil {
			return nil, e
		}
		return json.Marshal(u)
	})
	if e != nil {
		return nil, e
	}
	u := new(OutcomeUsage)
	if e = json.Unmarshal(raw, u); e != nil {
		return nil, backend(e)
	}
	return u, nil
}
