# Shared production replay scenario

Run from a clean checkout using the pinned Go toolchain:

```sh
GOTOOLCHAIN=go1.27.1 go test -count=1 -v ./internal/replay ./internal/simulation
```

`replay_test.go` contains the exact fixed schedule and operation bytes: increment
with ID `stable-operation`, durably commit state and outcome, drop the response,
discard volatile staging, reopen durable state, retry, then reject changed input.
There is no random seed or wall-clock scheduling in this scenario. The independent
oracle expects value one, one outcome, and a nonempty committed replay barrier.
The negative control deliberately loses the outcome while persisting effects;
the same oracle detects the resulting double application.

Both the simulator and `node.Owner.journal` invoke `replay.Run`. Production wraps
its existing native transaction, accounting, family check and process-cut commit
hooks; the scenario supplies an atomic in-memory durable store and controlled
crash. Native handles and Owner admission are unchanged.

`coordination_test.go` adds the first coupled deterministic proof. It loads the
committed schedule at
`test/scenarios/simulation/lost-response-crash-move.json` and drives production
`Join`, `TopologyStore`, `Membership.Step`, `Router.Interceptor`, and
`replay.Run`. The schedule loses both bounded forwarding responses, crashes the
owner, advances logical time to eviction, and retries through the same endpoint.
The independent checks require one mutation, the original durable result,
movement to the surviving owner, changed-input rejection, stale-owner rejection,
preserved forwarding fields, and an exact event trace. Negative controls prove
the checkers reject a missing atomic outcome and a stale-owner acknowledgement.

Run it with retained provenance from a clean checkout:

```sh
python3 scripts/prove.py simulation
```

The coupled proof uses an in-memory conditional-S3 implementation and the same
atomic durability model as the replay unit proof. It does not replace native
SlateDB lifecycle or real-S3 tests. The fixed schedule has no random decisions;
seed zero records that fact. Workload generation, schedule minimization, native
durability-completion ordering, and broader schedule coverage remain open.

For retained evidence, save the exact source revision, hash this schedule and
`internal/replay/replay.go`, record `go version`, and retain verbose test output.
The integrated declarative proof runner must attach those identities before this
slice can contribute a reproducible release gate. A passing ad hoc test alone
must not be labelled full simulation acceptance.
