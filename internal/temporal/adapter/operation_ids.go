package adapter

import (
	"github.com/0x63616c/xenon/internal/identity"
	"sync"
)

// StoreOption configures a store before it serves requests.
type StoreOption func(*operationIDs)

// WithOperationIDSource injects operation entropy independently of clocks,
// routing and fault scheduling. A source shared across separately constructed
// stores must support concurrent calls; each store serializes its own source.
// Nil selects the production cryptographic generator.
func WithOperationIDSource(source identity.Source) StoreOption {
	if source == nil {
		source = identity.Generator{}
	}
	return func(ids *operationIDs) { ids.source = source }
}

type operationIDs struct {
	mu     sync.Mutex
	source identity.Source
}

func newOperationIDs(options ...StoreOption) *operationIDs {
	ids := &operationIDs{source: identity.Generator{}}
	for _, option := range options {
		if option != nil {
			option(ids)
		}
	}
	return ids
}
func (ids *operationIDs) next() (string, error) {
	// Zero-value test/legacy store literals retain secure production generation.
	if ids == nil {
		id, err := identity.NewOperationID(identity.Generator{})
		return string(id), err
	}
	ids.mu.Lock()
	defer ids.mu.Unlock()
	id, err := identity.NewOperationID(ids.source)
	return string(id), err
}
