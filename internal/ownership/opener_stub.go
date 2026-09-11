//go:build !slatedb

package ownership

import (
	"context"

	"github.com/0x63616c/xenon/internal/directory"
	"github.com/0x63616c/xenon/internal/partitions"
	partitiondb "github.com/0x63616c/xenon/internal/partitions/slatedb"
)

func newPartitionOpener(string) func(context.Context, directory.Record) (partitions.Writer, error) {
	return func(context.Context, directory.Record) (partitions.Writer, error) {
		return nil, partitiondb.ErrNotBuilt
	}
}
