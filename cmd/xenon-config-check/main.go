// xenon-config-check loads configuration with the pinned Temporal parser only.
package main

import (
	"fmt"
	"go.temporal.io/server/common/config"
	"log"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("at least one Temporal config path required")
	}
	for _, path := range os.Args[1:] {
		cfg, e := config.Load(config.WithConfigFile(path))
		if e != nil {
			log.Fatal(e)
		}
		if e = cfg.Validate(); e != nil {
			log.Fatal(e)
		}
		if cfg.Persistence.DefaultStore != "xenon-default" || cfg.Persistence.VisibilityStore != "xenon-visibility" {
			log.Fatal("unexpected datastore selection")
		}
		for _, store := range cfg.Persistence.DataStores {
			if store.CustomDataStoreConfig == nil || store.SQL != nil || store.Cassandra != nil || store.Elasticsearch != nil {
				log.Fatal("non-Xenon durable datastore configured")
			}
		}
		fmt.Println("CONFIG_VALID", path)
	}
}
