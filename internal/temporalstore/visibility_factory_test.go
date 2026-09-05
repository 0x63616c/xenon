package temporalstore

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/searchattribute"
	"testing"
)

func TestVisibilityFactoryConfiguration(t *testing.T) {
	cases := []config.CustomDatastoreConfig{
		{Name: "typo", Options: map[string]any{"address": "127.0.0.1:1", "schema_partition": "global"}},
		{Name: "xenon", Options: map[string]any{"address": "127.0.0.1:1", "schema_partition": "global", "typo": "bad"}},
		{Name: "xenon", Options: map[string]any{"address": "127.0.0.1:1", "schema_partition": "global", "index": 1}},
		{Name: "xenon", Options: map[string]any{"address": "127.0.0.1:1", "schema_partition": false}},
		{Name: "xenon", IndexName: "different", Options: map[string]any{"address": "127.0.0.1:1", "schema_partition": "global"}},
	}
	for i, c := range cases {
		if store, e := (VisibilityFactory{}).NewVisibilityStore(c, searchattribute.NewTestProvider(), nil, nil, nil, nil, nil, nil); e == nil || store != nil {
			t.Fatalf("invalid case%d accepted", i)
		}
	}
	valid := config.CustomDatastoreConfig{Name: "xenon", IndexName: "xenon-visibility", Options: map[string]any{"address": "127.0.0.1:1", "schema_partition": "global"}}
	store, e := (VisibilityFactory{}).NewVisibilityStore(valid, searchattribute.NewTestProvider(), nil, nil, nil, nil, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	if store.GetIndexName() != "xenon-visibility" {
		t.Fatal(store.GetIndexName())
	}
}
