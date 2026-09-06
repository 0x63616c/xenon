package persistence

import (
	"context"
	"encoding/binary"
	"encoding/json"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math"
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

func readOutcomeAccounting(ctx context.Context, tx ShardTransaction) (map[string]OutcomeFamilyUsage, error) {
	b, e := tx.Get(ctx, []byte("v1/outcome_usage"))
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

// ReadOutcomeUsage interprets bounded journal accounting in the caller's admitted
// transaction. The caller must complete its durable fencing barrier before reply.
func ReadOutcomeUsage(ctx context.Context, tx ShardTransaction, partition string, limit uint64) (*OutcomeUsage, error) {
	b, e := tx.Get(ctx, []byte("v1/outcome_count"))
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
	families, e := readOutcomeAccounting(ctx, tx)
	if e != nil {
		return nil, e
	}
	u := &OutcomeUsage{Partition: partition, Limit: limit, Entries: count, Families: families}
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
	return u, nil
}

type familyUsage = OutcomeFamilyUsage

// AccountOutcome preserves legacy bounded aggregate accounting in the SAME
// transaction as operation effects, outcome and count. Never expire replay here.
func AccountOutcome(ctx context.Context, tx ShardTransaction, outcome *wire.StoredOutcome, size int) error {
	usage, err := readOutcomeAccounting(ctx, tx)
	if err != nil {
		return err
	}
	if outcome == nil || size < 0 {
		return status.Error(codes.InvalidArgument, "invalid outcome accounting input")
	}
	ref := outcome.ProtoReflect()
	field := ref.WhichOneof(ref.Descriptor().Oneofs().ByName("result"))
	if field == nil {
		return status.Error(codes.Unavailable, "outcome has no result family")
	}
	name := string(field.Name())
	value := usage[name]
	if value.Entries == math.MaxUint64 || math.MaxUint64-value.EncodedOutcomeBytes < uint64(size) {
		return status.Error(codes.ResourceExhausted, "outcome accounting overflow")
	}
	value.Entries++
	value.EncodedOutcomeBytes += uint64(size)
	usage[name] = value
	raw, err := json.Marshal(usage)
	if err != nil {
		return err
	}
	return tx.Put([]byte("v1/outcome_usage"), raw)
}
