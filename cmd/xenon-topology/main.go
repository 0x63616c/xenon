// xenon-topology conditionally publishes an explicit operator assignment.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/0x63616c/xenon/internal/directory"
	"github.com/0x63616c/xenon/internal/ownership"
	"io"
	"log"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: xenon-topology topology.json")
	}
	f, e := os.Open(os.Args[1])
	if e != nil {
		log.Fatal(e)
	}
	defer f.Close()
	dec := json.NewDecoder(io.LimitReader(f, 65537))
	dec.DisallowUnknownFields()
	var next ownership.Topology
	if e = dec.Decode(&next); e != nil {
		log.Fatal(e)
	}
	if dec.Decode(new(any)) != io.EOF {
		log.Fatal("trailing topology input")
	}
	store, e := ownership.Environment()
	if e != nil {
		log.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	observed, e := store.Read(ctx)
	var prior *ownership.TopologySnapshot
	if e == nil {
		prior = &observed
	} else if !errors.Is(e, directory.ErrMissing) {
		log.Fatal(e)
	}
	result, e := store.Publish(ctx, prior, next)
	if e != nil {
		log.Fatal(e)
	}
	fmt.Printf("PUBLISHED %d %s\n", result.Record().Revision, result.Record().Transition)
}
