package temporalstore

import (
	"fmt"
	"github.com/0x63616c/xenon/internal/adapter"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

// VisibilityFactory supplies the pinned Temporal custom-visibility extension.
// Namespace aliases and preallocated attribute definitions remain in Temporal's
// real metadata stores; Xenon stores visibility documents in four fixed domains.
type VisibilityFactory struct{}

func (VisibilityFactory) NewVisibilityStore(c config.CustomDatastoreConfig, provider searchattribute.Provider, mappers searchattribute.MapperProvider, _ namespace.Registry, registry *chasm.Registry, _ resolver.ServiceResolver, _ log.Logger, _ metrics.Handler) (store.VisibilityStore, error) {

	if c.Name != "xenon" {
		return nil, fmt.Errorf("visibility custom datastore name must be xenon")
	}
	for key, value := range c.Options {
		if key != "address" && key != "index" && key != "schema_partition" {
			return nil, fmt.Errorf("unknown visibility option %q", key)
		}
		if _, ok := value.(string); !ok {
			return nil, fmt.Errorf("visibility option %q must be a string", key)
		}
	}
	address, _ := c.Options["address"].(string)
	index, _ := c.Options["index"].(string)
	schema, _ := c.Options["schema_partition"].(string)
	if index == "" {
		index = "xenon-visibility"
	}
	if address == "" || schema == "" {
		return nil, fmt.Errorf("visibility custom datastore requires address and schema_partition")
	}
	if c.IndexName != "" && c.IndexName != index {
		return nil, fmt.Errorf("visibility indexName must match index option")
	}

	return adapter.NewVisibilityStore(address, index, schema, provider, mappers, registry)
}
