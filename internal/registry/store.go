// Package registry defines durable, linearizable per-key conditional publication.
// No implementation or production persisted-format migration is supplied here.
package registry

import (
	"bytes"
	"context"
	"github.com/0x63616c/xenon/internal/identity"
)

type Key string

// Version is opaque and scoped to a key. Only equality is meaningful. Backends
// must prevent ABA, including replacements with identical application bytes.
type Version string

// Record.Body is the complete encoded publication envelope, not just its payload.
// Every backend return must own its bytes; Clone is also useful at cache edges.
type Record struct {
	Body    []byte
	Version Version
}

func (r Record) Clone() Record { return Record{Body: bytes.Clone(r.Body), Version: r.Version} }

type Write struct {
	Transition identity.TransitionID
	Digest     [32]byte
	Body       []byte
}

// Store has no unconditional overwrite. Create requires absence; Replace requires
// a nonempty exact observed version. Success means whole-record durable publication
// and a nonempty version. Reads return coherent records at a linearization point.
// Mutations validate writes before dispatch. Disable hidden mutation retries or
// reconcile aggregate attempts: an earlier possibly committed attempt dominates
// later Conflict/Unavailable/cancellation until matching readback proves success.
// Cancellation after dispatch is UnknownOutcome, never assumed rollback.
// Unavailable and Conflict guarantee the aggregate call did not publish.
// Returned Body slices belong to callers; implementations must copy on ingress
// and egress, and include fresh transition identities in every accepted change.
type Store interface {
	Read(context.Context, Key) (Record, error)
	Create(context.Context, Key, Write) (Record, error)
	Replace(context.Context, Key, Version, Write) (Record, error)
}

type NotFound struct{ Key Key }

func (e *NotFound) Error() string { return "registry record not found: " + string(e.Key) }

type Conflict struct{ Key Key }

func (e *Conflict) Error() string { return "registry condition conflict: " + string(e.Key) }

type UnknownOutcome struct {
	Key        Key
	Transition identity.TransitionID
	Cause      error
}

func (e *UnknownOutcome) Error() string {
	return "registry publication outcome unknown: " + string(e.Key) + " transition " + string(e.Transition)
}
func (e *UnknownOutcome) Unwrap() error { return e.Cause }

type Unavailable struct {
	Key   Key
	Cause error
}

func (e *Unavailable) Error() string { return "registry unavailable: " + string(e.Key) }
func (e *Unavailable) Unwrap() error { return e.Cause }

type Invalid struct {
	Key    Key
	Reason string
}

func (e *Invalid) Error() string {
	return "invalid registry request: " + string(e.Key) + ": " + e.Reason
}

type Corrupt struct {
	Key   Key
	Cause error
}

func (e *Corrupt) Error() string { return "corrupt registry record: " + string(e.Key) }
func (e *Corrupt) Unwrap() error { return e.Cause }
