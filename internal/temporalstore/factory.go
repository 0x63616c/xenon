// Package temporalstore connects the pinned Temporal persistence factory to Xenon.
package temporalstore

import (
	"fmt"
	"sync"

	"github.com/0x63616c/xenon/internal/adapter"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"
)

// AbstractFactory is installed with temporal.WithCustomDataStoreFactory.
// A partition is a stable logical name, never a node address.
type AbstractFactory struct{}

var _ client.AbstractDataStoreFactory = AbstractFactory{}

type Factory struct {
	mu                                          sync.Mutex
	closed                                      bool
	err                                         error
	address, cluster, history, matching, global string
	stores                                      []p.Closeable
}

var _ p.DataStoreFactory = (*Factory)(nil)

func (AbstractFactory) NewFactory(c config.CustomDatastoreConfig, _ resolver.ServiceResolver, cluster string, _ log.Logger, _ metrics.Handler, _ serialization.Serializer) p.DataStoreFactory {
	f := &Factory{cluster: cluster}
	if c.Name != "xenon" {
		f.err = fmt.Errorf("custom datastore name must be xenon")
		return f
	}
	fields := map[string]*string{"address": &f.address, "historyPartition": &f.history, "matchingPartition": &f.matching, "globalPartition": &f.global}
	for key, target := range fields {
		value, ok := c.Options[key].(string)
		if !ok || value == "" {
			f.err = fmt.Errorf("xenon datastore requires string option %s", key)
			return f
		}
		*target = value
	}
	for key := range c.Options {
		if _, ok := fields[key]; !ok {
			f.err = fmt.Errorf("unknown xenon datastore option %s", key)
			return f
		}
	}
	return f
}

func create[T p.Closeable](f *Factory, constructor func() (T, error)) (T, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var empty T
	if f.err != nil {
		return empty, f.err
	}
	if f.closed {
		return empty, fmt.Errorf("xenon datastore factory is closed")
	}
	s, e := constructor()
	if e != nil {
		return empty, e
	}
	f.stores = append(f.stores, s)
	return s, nil
}
func (f *Factory) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	for _, s := range f.stores {
		s.Close()
	}
	f.stores = nil
}
func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	return create(f, func() (*adapter.MatchingStore, error) { return adapter.NewMatchingStore(f.address, f.matching) })
}
func (f *Factory) NewFairTaskStore() (p.TaskStore, error) {
	return create(f, func() (*adapter.MatchingStore, error) { return adapter.NewFairMatchingStore(f.address, f.matching) })
}
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	return create(f, func() (*adapter.ShardStore, error) { return adapter.NewShardStore(f.address, f.history, f.cluster) })
}
func (f *Factory) NewMetadataStore() (p.MetadataStore, error) {
	return create(f, func() (*adapter.MetadataStore, error) { return adapter.NewMetadataStore(f.address, f.global) })
}
func (f *Factory) NewExecutionStore() (p.ExecutionStore, error) {
	return create(f, func() (*adapter.ExecutionStore, error) { return adapter.NewExecutionStore(f.address, f.history) })
}
func (f *Factory) NewQueue(t p.QueueType) (p.Queue, error) {
	return create(f, func() (*adapter.Queue, error) { return adapter.NewQueue(f.address, f.global, t) })
}
func (f *Factory) NewQueueV2() (p.QueueV2, error) {
	return create(f, func() (*adapter.QueueV2, error) { return adapter.NewQueueV2(f.address, f.global) })
}
func (f *Factory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	return create(f, func() (*adapter.ClusterStore, error) { return adapter.NewClusterStore(f.address, f.global) })
}
func (f *Factory) NewNexusEndpointStore() (p.NexusEndpointStore, error) {
	return create(f, func() (*adapter.NexusStore, error) { return adapter.NewNexusStore(f.address, f.global) })
}
