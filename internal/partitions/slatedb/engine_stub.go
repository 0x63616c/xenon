//go:build !slatedb

package slatedb

import (
	"context"
	"errors"
	"strings"

	"github.com/0x63616c/xenon/internal/partitions"
)

// ErrNotBuilt keeps native SlateDB out of the ordinary Go test graph. Real
// binaries and the three selected integration journeys build with -tags=slatedb.
var ErrNotBuilt = errors.New("SlateDB support not built; use -tags=slatedb")

type Engine struct{}

func New(objectStoreURL string) (*Engine, error) {
	return NewWithWALFlushInterval(objectStoreURL, 100)
}

func NewWithWALFlushInterval(objectStoreURL string, milliseconds int) (*Engine, error) {
	if milliseconds < 1 || milliseconds > 1000 || !strings.HasPrefix(objectStoreURL, "s3://") || len(objectStoreURL) == 5 {
		return nil, partitions.ErrInvalid
	}
	return &Engine{}, nil
}

func (*Engine) Open(context.Context, partitions.OpenRequest) (partitions.Writer, error) {
	return nil, ErrNotBuilt
}
