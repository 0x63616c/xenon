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

// Open retains ownership of a dispatched native open through completion and any
// cancellation cleanup. Drivers must keep its effect pending until the call
// returns; canceling its context does not guarantee prompt return.
type Engine interface {
	Open(context.Context, OpenRequest) (Writer, error)
}
type Writer interface {
	BeginOperation(context.Context) (Operation, error)
	Begin(context.Context) (Transaction, error)
	AwaitDurable(context.Context, CommitReceipt) error
	ReadDurable(context.Context, ReadRequest) (ReadResult, error)
	Close(context.Context) error
}

// Operation retains whole-operation exclusion across multiple transactions.
// Begin returns ErrBusy while a transaction or native durability receipt remains
// active. Successful Abort/AwaitDurable permits another Begin without reacquiring
// writer admission. Release is idempotent, rejects future Begin immediately and
// aborts an idle transaction; busy native work retains admission until completion.
// Ordinary Writer.Begin is an implicit operation released with its transaction.
type Operation interface {
	Begin(context.Context) (Transaction, error)
	Release()
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

// Scan bounds are bytewise lower Start and upper End, independent of Reverse.
// Defaults are [Start, End); nil means unbounded. Equal bounds are empty unless
// both endpoints are inclusive; Start greater than End is invalid.
// Reverse returns descending keys without changing the bounds. Resume an
// ascending page with Start=last key, StartExclusive=true; resume a descending
// page with End=last key, EndInclusive=false, retaining the other bound.
// Limit is 1..100000. More means another application key exists in this same
// transaction view and range after the returned page, in the requested order.
type ScanRequest struct {
	Start, End     []byte
	StartExclusive bool
	EndInclusive   bool
	Reverse        bool
	Limit          int
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
	ErrOperationDone   = errors.New("operation admission released")
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
