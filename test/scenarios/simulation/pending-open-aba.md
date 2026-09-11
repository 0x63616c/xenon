# Assignment ABA while native Open is pending

`TestCoupledPendingOpenAssignmentABA` extends the Go-authored assignment recovery
schedule using the same real cluster and partition Steps. It executes two variants:

1. Reservation publication emits Open. Before the native action executes, two
   coordinator publications change the owner A → B → A. A fresh partition read
   sees the newer assignment revision but retains the in-flight Open exclusion.
   Its eventual Open completion produces Close for that exact obsolete handle,
   followed by a fresh reservation/Open/Ready/commit sequence.
2. Native Open executes but its completion remains pending. During the A → B → A
   cycle, B reserves and opens the same database, displacing A's native epoch.
   A's attempted commit through the still-pending handle is rejected as fenced.
   The late completion is retired as above; B's displaced handle is also closed.

Both variants require exactly two observed desired-owner changes between the Open
request and its completion; the fresh read must show A again with a greater
assignment revision. No effects may release Open exclusion on that read, and the
late completion must emit exactly one Close for the original handle. The standard
independent checker additionally requires final Ready ownership, a current native
commit, no obsolete live handles, and empty pending queues.

Each complete trace is replayed exactly. A negative control enables the modeled
engine's faulty acceptance of A's displaced pending handle; it must fail
`post_fence_commit` at that specific handle. Earlier mutation labels are removed
so a failure at an earlier fixture boundary cannot satisfy the control.

```sh
go test -race ./internal/simulation \
  -run 'TestCoupled(PendingOpen|AssignmentABA|RenewalUnknown)' -count=1 -v
```

The combined selection passed in 2.113 seconds; the strengthened pending-Open
coverage-count selection passed in 1.841 seconds (Go 1.27.1 darwin/arm64).
Native epochs remain modeled, not qualified SlateDB behavior. This adds explicit
pending-Open ABA and modeled displacement coverage, not physical crash, real
network loss or the full FAULT-01 acceptance matrix.
