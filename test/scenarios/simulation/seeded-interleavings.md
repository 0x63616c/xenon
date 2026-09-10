# Seeded coupled delivery exploration

This is partial DST-01 evidence, not the complete FAULT-01 capability matrix.
`NewCoupledInterleavings` takes the committed coordinator-move input and expands
fault seed/index into a topological ordering. Actor-local order and all external
registry/native linearizations are preserved; poll/completion delivery across
actors varies. No execution result is consulted when choosing an ordering, and
failed seeds are never retried or filtered. Workload bytes remain fixed. This is
not random generation of Temporal workflow semantics.

Production calls remain `cluster.NewState` / `cluster.Step` and
`partitions.NewState` / `partitions.Step` inside `runCoupled`. The registry CAS and
native epoch behavior are explicitly modeled in that executor. No native engine,
network, SDK, OS scheduling or storage durability is qualified by these tests.
The 24-hour case advances the new coordinator's local tick from zero to 24 hours,
then observes an accepted takeover publication and final healthy recovery.

Run from a checkout with the pinned native bindings available to the linker:

```sh
go test -race ./internal/simulation -run 'TestCoupled(Seeded|Virtual)' -count=1 -v
```

The repeatability check runs seeds 1..100 and indices 0..9, twice each, asserting
byte-identical expanded order and decision trace. Each schedule has 84 actions.
It requires multiple distinct expanded orders and exactly 1,000 observations of
each of these existing safety cuts: stale coordinator plan CAS rejected, stale
Ready CAS rejected, and obsolete writer commit rejected after a displaced Open.
Three separately seeded negative-control scenarios force each of those violations
and must fail its named independent checker invariant.

The test logs distinct-order and executed-cut counts and elapsed time. On base
revision `9c38f16` with these generator/test additions, Go 1.27.1 darwin/arm64,
non-race measurement was 3.591 seconds for generation and 2,000 executions,
approximately 1.796 ms per execution including generation/checking overhead.
This bounds measurement scope; it is not a throughput benchmark. The committed
three-minute external watchdog also allows the race build: an initial one-minute
race run reached seed 65/index 6 before the watchdog fired, implying about 92
seconds for the full selection on this host. Three minutes provides approximately
2x that observed race cost;
it only fails a stuck run and never advances logical time or permits success.

Not yet covered by this generator: physical owner crash or lost acknowledged
application state, lost registry response, storage error, duplicate/drop message,
renewal response ambiguity, assignment ABA, same-address route incarnation,
persistence operation identity/replay, and arbitrary conflicting linearization
orders. Those require additional explicit seams and independent oracles. Delaying
a completion here must not be reported as any of those faults. There is also no
instrumented zero-I/O adapter receipt yet; the no-I/O boundary is source-visible,
not a complete DST-01 acceptance claim. CLI generated-search wiring is separate.

The final race run passed in 82.830 seconds (80.901 seconds for the 2,000-run
exploration itself). `seeded-interleavings-receipt.json` records the exact source
file hashes and bounded executed counts; this source-addition receipt is not a
clean-checkout integrated acceptance result.
