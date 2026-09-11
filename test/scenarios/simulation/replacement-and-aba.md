# Coordinator replacement and assignment ABA

The Go-authored schedules in `coupled_replacement_test.go` extend the complete
coordinator-move schedule through real `cluster.Step` and `partitions.Step`
publications. They do not inject fabricated authoritative control records.

- **Ambiguous renewal after replacement:** the coordinator renews successfully,
  loses its response, and a contender observes that authority twice across the
  suspicion interval and publishes a replacement. The previous coordinator reads
  the replacement. It emits zero new effects while retaining the original
  unresolved publication condition, transition and bytes. Its historical
  uncertainty does not become authority to act as coordinator.
- **Assignment A → B → A:** the coordinator publishes both desired-owner changes
  before A's writer polls. A reads the same owner identity with a newer assignment
  revision, closes its old native handle, reserves a new generation, opens and
  publishes Ready again, then commits successfully. This covers an existing live
  writer encountering ABA, not a native Open held pending across ABA.

The optional `unresolved_publication` trace field copies the production
controller's retained unknown effect; its write bytes are detached from state.
It appears only while the controller retains historical uncertainty. The existing
no-fault golden trace remains byte-identical.

Both schedules execute twice with exact complete-trace comparison and the normal
independent final-owner/commit/handle/pending-effect checks. Additional observation
oracles reject erased ambiguity history, an omitted close, and reused assignment
revision using named invariant failures. A fourth negative control attempts a
commit through the actual closed pre-ABA handle and deliberately makes the modeled
engine accept it. Earlier mutation labels are removed so this control must fail
at the new ABA commit boundary, not an earlier scenario fault.

```sh
go test -race ./internal/simulation \
  -run 'TestCoupled(AssignmentABA|RenewalUnknown|CoordinatorMove|LostPublication)' \
  -count=1 -v
```

These are two bounded fault schedules with four deliberate negative controls, not
all FAULT-01 coverage. Native engine fencing remains modeled; real adapter/native
contracts, pending-Open ABA, crash cuts and network behavior require separate
execution evidence. The last successful focused race run covered the two new
schedules in 1.870 seconds on Go 1.27.1 darwin/arm64. The earlier broader selection
also passed the existing exact golden and lost-publication scenarios (2.161s).
