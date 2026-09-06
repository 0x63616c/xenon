// Package replay retains the historical test harness import during layout migration.
// Production services call persistence.RunReplay directly.
package replay

import (
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/persistence"
)

type Effects = persistence.ReplayEffects

// Run forwards to the sole production durable replay implementation.
func Run(effects Effects, id string, digest []byte, limit uint64) (*wire.StoredOutcome, error) {
	return persistence.RunReplay(effects, id, digest, limit)
}
