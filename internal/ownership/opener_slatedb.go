//go:build slatedb

package ownership

import (
	"context"

	"github.com/0x63616c/xenon/internal/directory"
	"github.com/0x63616c/xenon/internal/partitions"
	partitiondb "github.com/0x63616c/xenon/internal/partitions/slatedb"
	native "slatedb.io/slatedb-go/uniffi"
)

func newPartitionOpener(objectURL string) func(context.Context, directory.Record) (partitions.Writer, error) {
	return func(_ context.Context, record directory.Record) (partitions.Writer, error) {
		store, err := native.ObjectStoreResolve(objectURL)
		if err != nil {
			return nil, err
		}
		defer store.Destroy()
		builder := native.NewDbBuilder(record.DataPrefix, store)
		defer builder.Destroy()
		db, err := builder.Build()
		if err != nil {
			return nil, err
		}
		return partitiondb.AdoptNative(db)
	}
}
