// Package partitions owns local partition writer lifecycles. Native database
// handles stay behind Engine; registry authority is validated by its controller.
package partitions

import (
	"context"
	"errors"
	"github.com/0x63616c/xenon/internal/identity"
)

type OpenRequest struct {
	Path               string // Stable database path; never an owner address.
	Partition          identity.PartitionID
	AssignmentRevision uint64
	Reservation        identity.TransitionID
	Incarnation        identity.IncarnationID
	Generation         uint64
}

type Engine interface {
	Open(context.Context, OpenRequest) (Writer, error)
}
type Writer interface {
	Begin(context.Context) (Transaction, error)
	AwaitDurable(context.Context, CommitReceipt) error
	ReadDurable(context.Context, ReadRequest) (ReadResult, error)
	Close(context.Context) error
}
type Transaction interface {
	Get(context.Context, []byte) ([]byte, error) // nil means absent.
	Scan(context.Context, ScanRequest) (ReadResult, error)
	Put([]byte, []byte) error
	Delete([]byte) error
	Commit(context.Context) (CommitReceipt, error)
	Abort() error
}

// CommitReceipt is an opaque writer/mutation capability. Implementations must
// reject foreign receipts. Commit success is submission, not durable success.
// Even when Commit returns UnknownOutcome it can return a receipt to reconcile.
type CommitReceipt interface{ MutationID() uint64 }

// Scan bounds are inclusive Start and exclusive End; nil means unbounded.
// Limit must be positive. More indicates additional entries in this snapshot.
type ScanRequest struct {
	Start, End []byte
	Limit      int
}
type ReadRequest struct {
	Keys [][]byte
	Scan *ScanRequest
}
type Entry struct{ Key, Value []byte }
type ReadResult struct {
	Entries []Entry
	More    bool
}

var (
	ErrBusy            = errors.New("transaction native call active")
	ErrInvalid         = errors.New("invalid engine request")
	ErrRetired         = errors.New("writer retired")
	ErrFenced          = errors.New("writer fenced")
	ErrConflict        = errors.New("transaction conflict")
	ErrTransactionDone = errors.New("transaction already finished or commit dispatched")
)

// UnknownOutcome means native work may complete after the caller stops waiting.
// It never authorizes a new operation identity or rollback of submitted writes.
type UnknownOutcome struct{ Cause error }

func (e *UnknownOutcome) Error() string { return "engine outcome unknown: " + e.Cause.Error() }
func (e *UnknownOutcome) Unwrap() error { return e.Cause }
