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

This is a **replay decision unit proof**, not the full deterministic coordination
acceptance gate. It does not yet simulate production ownership movement, routing,
deadlines, native durability completion scheduling or stale-owner admission. It
also does not replace native lifecycle, real-stack or real-S3 tests. Accounting
is a no-op test effect; production accounting remains in its existing transaction.

For retained evidence, save the exact source revision, hash this schedule and
`internal/replay/replay.go`, record `go version`, and retain verbose test output.
The integrated declarative proof runner must attach those identities before this
slice can contribute a reproducible release gate. A passing ad hoc test alone
must not be labelled full simulation acceptance.
