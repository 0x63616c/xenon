package main

import (
	"context"
	"fmt"

	"github.com/0x63616c/xenon/internal/adapter"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

// Seed standard SQL-style custom slots through the real guarded metadata manager.
// Preserve existing index maps and compare the exact read version on publication.
func seedSearchAttributes(ctx context.Context, address string) error {
	store, e := adapter.NewClusterStore(address, "global")
	if e != nil {
		return e
	}
	defer store.Close()
	manager := p.NewClusterMetadataManagerImpl(store, serialization.NewSerializer(), "active", log.NewNoopLogger())
	current, e := manager.GetClusterMetadata(ctx, &p.GetClusterMetadataRequest{ClusterName: "active"})
	if e != nil {
		return e
	}
	if current.IndexSearchAttributes == nil {
		current.IndexSearchAttributes = map[string]*persistencespb.IndexSearchAttributes{}
	}
	expected := sadefs.GetDBIndexSearchAttributes(nil)
	index := current.IndexSearchAttributes["xenon-visibility"]
	if index == nil {
		index = &persistencespb.IndexSearchAttributes{CustomSearchAttributes: map[string]enumspb.IndexedValueType{}}
		current.IndexSearchAttributes["xenon-visibility"] = index
	}
	if index.CustomSearchAttributes == nil {
		index.CustomSearchAttributes = map[string]enumspb.IndexedValueType{}
	}
	for name, kind := range expected.CustomSearchAttributes {
		if old, ok := index.CustomSearchAttributes[name]; ok && old != kind {
			return fmt.Errorf("existing search slot type differs: %s", name)
		}
		index.CustomSearchAttributes[name] = kind
	}
	saved, e := manager.SaveClusterMetadata(ctx, &p.SaveClusterMetadataRequest{ClusterMetadata: current.ClusterMetadata, Version: current.Version})
	if e != nil {
		return e
	}
	if !saved {
		return fmt.Errorf("search slot metadata CAS lost; retry bootstrap from fresh read")
	}
	return nil
}
