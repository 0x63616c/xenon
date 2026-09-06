package node

import (
	"context"
	"encoding/json"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/persistence"
	"github.com/google/uuid"
	native "slatedb.io/slatedb-go/uniffi"
)

// Compatibility names for the retiring native owner API.
type OutcomeFamilyUsage = persistence.OutcomeFamilyUsage
type OutcomeUsage = persistence.OutcomeUsage

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
		u, e := persistence.ReadOutcomeUsage(ctx, legacyShardTransaction{tx}, o.config.Partition, o.config.MaxOutcomes)
		if e != nil {
			return nil, e
		}

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
