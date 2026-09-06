package ownership

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/0x63616c/xenon/internal/directory"
)

// Join registers an agent once at startup. Never invoke it from reconciliation:
// a new invocation cannot distinguish an intentional restart from stale reentry.
// It publishes placement intentions; Manager still fences actual writer admission.
func Join(ctx context.Context, store *TopologyStore, identity directory.Identity, dataPrefix string, bootstrap bool) error {
	if store == nil || !pathOK(dataPrefix) {
		return directory.ErrInvalid
	}
	expected := map[string]Assignment{}
	for _, name := range []string{"global", "matching", "history-0", "history-1", "history-2", "history-3", "vis-v1-0", "vis-v1-1", "vis-v1-2", "vis-v1-3"} {
		expected[name] = Assignment{Node: identity.Node, DataPrefix: dataPrefix + "/" + name}
	}
	want := Member{Address: identity.Address, Incarnation: identity.Incarnation}
	initial := Topology{Format: 1, Revision: 1, Transition: identity.Incarnation, Members: map[string]Member{identity.Node: want}, Partitions: expected}
	if !store.valid(initial) {
		return directory.ErrInvalid
	}
	var predecessor Member
	var predecessorExists, observed bool
	for attempt := 0; attempt < 8; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		snap, err := store.Read(ctx)
		var prior *TopologySnapshot
		next := cloneTopology(initial)
		if err == nil {
			prior = &snap
			next = snap.Record()
			if len(next.Partitions) != len(expected) {
				return fmt.Errorf("%w: agent partition layout mismatch", directory.ErrInvalid)
			}
			for id, a := range expected {
				if got, ok := next.Partitions[id]; !ok || got.DataPrefix != a.DataPrefix {
					return fmt.Errorf("%w: agent partition prefix mismatch", directory.ErrInvalid)
				}
			}
		} else if !errors.Is(err, directory.ErrMissing) {
			return err
		} else if !bootstrap {
			return directory.ErrMissing
		}
		current, exists := next.Members[identity.Node]
		if prior == nil {
			current = Member{}
			exists = false
		}
		if exists && current == want {
			return nil
		}
		if !observed {
			predecessor, predecessorExists, observed = current, exists, true
		} else if current != predecessor || exists != predecessorExists {
			return directory.ErrConflict
		}
		next.Members[identity.Node] = want
		nodes := make([]string, 0, len(next.Members))
		for id := range next.Members {
			nodes = append(nodes, id)
		}
		sort.Strings(nodes)
		partitions := make([]string, 0, len(next.Partitions))
		for id := range next.Partitions {
			partitions = append(partitions, id)
		}
		sort.Strings(partitions)
		for index, id := range partitions {
			a := next.Partitions[id]
			a.Node = nodes[index%len(nodes)]
			next.Partitions[id] = a
		}
		if _, err = store.Publish(ctx, prior, next); err == nil {
			return nil
		} else if !errors.Is(err, directory.ErrConflict) && !errors.Is(err, directory.ErrUnknown) {
			return err
		}
	}
	return fmt.Errorf("%w: join attempts exhausted; inspect topology before restarting", directory.ErrUnknown)
}
