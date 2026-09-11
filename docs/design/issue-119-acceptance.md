# Issue #119: fast Go failure-search developer loop

Owner: [#119](https://github.com/0x63616c/xenon/issues/119).

## Goal

Xenon has a conventional Go testing pyramid. Most correctness work runs in
milliseconds or seconds without Docker, Temporal, MinIO, native SlateDB, Python
or Make. A small integration tier proves that the modeled seams match the real
engine, process and Temporal boundaries.

The primary developer interface is:

```sh
go test ./...
just xenon
xenon test dst
xenon test dst --seed 42 --cases 1000
xenon replay failure.json
xenon minimize failure.json
xenon test integration
```

`just xenon` is the normal developer entrypoint. It fingerprints the current
working tree's executable Go and embedded source inputs, rebuilds the cached
`.local/bin/xenon` when any relevant input changes, and then runs that binary.
An unchanged tree reuses the verified binary and native-library caches. Additional
arguments are forwarded to the same binary, such as `just xenon test dst`.

Commands choose sensible defaults and create temporary artifacts themselves.
Users do not provide internal oracle binaries, hashes, fixture files or runtime
bundle paths.

## Testing pyramid

| Tier | Purpose | Default dependency | Expected use |
| --- | --- | --- | --- |
| Go unit tests | Local invariants and error behavior | None | Every edit |
| Go DST | Ownership, routing, retries and failure schedules | Virtual clock and in-memory adapters | Every edit |
| Go component tests | One production seam at a time | In-process adapter where possible | Every PR |
| Go integration tests | Native/process/Temporal behavior DST cannot prove | MinIO, real binaries and Temporal | Relevant PRs and CI |
| Release qualification | AWS S3 and longer compatibility coverage | Explicit external environment | Release work |

Race detection is a separate CI job. It must not make the ordinary DST loop slow.

## Go DST interface

Scenarios are Go code. There is no required YAML or JSON scenario language.
Tests use production `cluster.Step`, `partitions.Step` and
`persistence.RunReplay` decisions through a small simulation interface:

```go
func TestOwnerDiesAfterReady(t *testing.T) {
	sim := dst.New(t, dst.WithSeed(42))
	cluster := xenontest.NewCluster(sim, 3)
	partition := cluster.AssignPartition()

	cluster.WaitUntil(partition.Ready())
	cluster.Kill(partition.Owner())
	cluster.RunUntilIdle()

	cluster.RequireSingleWriter(partition)
	cluster.RequireAcknowledgedWrites()
}
```

The exact exported names may change while deepening the module. The interface
must keep clock control, scheduling, faults, assertions and artifacts behind a
small Go surface.

The default `xenon test dst` run finishes in under ten seconds on the documented
development machine after compilation. The 1,000-case command finishes in under
30 seconds without `-race`. Receipts record the measured environment; a timing
regression fails the dedicated performance check rather than making correctness
depend on a flaky per-test stopwatch.

Required generated schedules cover:

- crash before commit and after durable commit before response;
- lost conditional-write response and retry;
- renewal versus coordinator takeover;
- ownership movement and assignment ABA;
- stale owner resume and post-fence commit;
- overlapping joins;
- delayed, dropped and duplicated messages;
- stale routing, including the same address with a new incarnation;
- failure before and after reservation, Open and Ready transitions;
- storage delay/error and cleanup ordering;
- competing failure causes and admission cutoff.

Virtual time advances long intervals without sleeping. DST performs no network
calls, process launches, Docker operations or native engine opens.

## Failure artifacts

The first failure prints its seed and artifact path. The artifact contains only
the information required to reproduce it: schema version, source revision,
workload and fault seeds, expanded schedule, normalized trace and invariant
fingerprint.

`xenon replay` does not require the original generator. `xenon minimize` uses
deterministic candidate ordering, preserves dependencies and accepts a reduction
only when it reproduces the same invariant fingerprint. Originals remain intact.

Independent negative controls must prove detection of acknowledged-write loss,
duplicate application, changed operation digest, stale-owner acknowledgement and
failed healthy settle.

## Small integration tier

DST cannot establish native durability, OS process behavior or Temporal
compatibility. Keep exactly these three bounded Go-driven journeys:

1. **SlateDB + MinIO:** commit, await durability, lose the response, discard local
   state, reopen, reconcile exactly once and prove a displaced writer is fenced.
2. **Three Xenon processes:** write while ownership moves, kill the current owner,
   take over, restart the same address with a new incarnation, reject stale work
   and make new progress.
3. **Temporal compatibility:** two Temporal instances use Xenon's endpoint; a
   compact SDK workflow covers activity, timer, signal/update, child,
   Continue-As-New, Nexus, history and visibility across one owner loss and cold
   restart.

These run through `xenon test integration`. The Go runner owns lifecycle,
readiness, observed fault barriers, assertions and cleanup. It may invoke Docker
as an external tool; it is not implemented by Python.

AWS S3 qualification and long soaks belong to release work in #94/#107. MinIO
and DST do not claim to prove AWS behavior.

## Definition of done

- [ ] `just xenon` runs Xenon from the current working-tree source. A relevant
  source edit invalidates and rebuilds the executable; a second unchanged run
  verifies and reuses the cached executable and native library.
- [ ] `go test ./...` has a fast default path that does not require Python,
  Make, Docker, MinIO or Temporal. Native/integration tests are explicitly
  selected when needed.
- [ ] `xenon test dst` is implemented in Go, code-driven, virtual-time and uses
  production decision functions.
- [ ] The required DST fault families execute across at least 1,000 schedules
  and 100 seeds inside the measured budget.
- [ ] A failing seed can be replayed and minimized through the Go CLI with the
  same stable fingerprint.
- [ ] The five independent negative controls fail for their named invariant.
- [ ] `xenon test integration` owns and runs the three bounded journeys above.
- [ ] User-facing build, test and run documentation uses Go and `xenon`; Python
  and Make are not required entry points.
- [ ] One batched independent review finds no duplicate production algorithm in
  the simulator and no integration-only claim represented as DST coverage.

## Migration

Existing Python proof controllers and manifests are historical migration inputs.
Do not extend them. Replace one retained path at a time with the Go command, prove
the replacement, then remove the unreachable Python and Make entry points in a
cleanup batch. Preserve prior real-run evidence as historical evidence.

Performance benchmarking remains #116. Broad metrics, logs and tracing remain
#92. Packaging, real AWS and release soak remain #94/#107.
