# Ambiguous publication and renewal/takeover cuts

`coupled_ambiguity_test.go` authors three bounded schedules in Go using the
committed coordinator-move fixture. It does not mutate authoritative records
outside production Step publications:

| Schedule | Required executed observation | Healthy settle |
| --- | --- | --- |
| Lost renewal response | One accepted CAS followed by one `unknown_publication` completion with no returned record | Final current owner Ready, current-epoch commit, no obsolete handles/effects |
| Lost assignment response | One accepted assignment CAS followed by the same missing-receipt observation; a later fresh read reconciles changed state | Same independent settle checker |
| Renewal while contender suspects old coordinator | Contender observes before real renewal; its tick-20 read emits no publication; tick-40 read permits exactly one accepted takeover | Same independent settle checker |

The lost-response seam executes publication first, then withholds the receipt and
delivers `registry.UnknownOutcome` to `cluster.Step` (also available for a partition
publication). It cannot silently turn a conflict into lost success. Unknown fault
names, wrong actions, and a read completion mislabeled as a lost publication
response are rejected before the first trace event. The real adapter/native
contract is not qualified by this modeled completion.

Every successful schedule is executed twice and its complete trace compared.
The two lost-response schedules each run the existing three independent safety
mutants (`stale_plan`, `old_ready`, `post_fence_commit`); all six must fail their
specific typed invariant, not an unrelated error. These mutants qualify the
existing safety boundaries under the new fault, not arbitrary renewal mutants.

Reproduce with pinned native bindings configured for the test link:

```sh
go test -race ./internal/simulation -run 'TestCoupled(Lost|RenewalRestarts|Unsupported|Coordinator|Negative|Replay|Settle|Checker)' -count=1 -v
```

This remains partial FAULT-01 evidence. In particular, no new schedule here proves
renewal ambiguity after coordinator replacement, assignment-revision ABA,
publication requests dropped before commit, physical crashes, or actual transport
loss. Existing controller unit tests cover related local decisions; translating
those into this complete coupled recovery schedule still remains work.
