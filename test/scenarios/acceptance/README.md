# Issue 119 gate registration (partial DELIVER-01)

The five required names are now registered with `scripts/prove.py`:

```sh
python3 scripts/prove.py cli-contracts
python3 scripts/prove.py workflow-search-dst
python3 scripts/prove.py workflow-search-real
python3 scripts/prove.py workflow-replay-minimize
python3 scripts/prove.py issue-119-acceptance
```

**Every command currently exits 1.** Each creates a schema-2 receipt under
`.local/evidence/` with `result: incomplete`, `executed_tests: 0`,
`proof_pass: false` and `acceptance_pass: false`. Invalid or missing registration
inputs produce `result: failed`, also exit 1. `--allow-dirty` cannot promote any
of these results. Existing component experiments remain separate.

The committed manifests map every criterion required by DELIVER-01 to an
explicit unverified obligation. Their source inputs are context pointers, not
claims that a linked test proves a criterion. The runner validates all five
registrations, hashes their inputs and records candidate revision and dirty
status. An aggregate receipt lists its four children as unverified. A missing
child registration, omitted criterion, missing input, claimed completion, added
command or handwritten pass flag fails validation.

This is deliberately registration-only: it does not execute component tests,
consume child proof receipts, validate negative-control fingerprints, or satisfy
any full criterion. None of the audited full gates has sufficient coverage at
this revision. CLI still needs complete side-effect and configuration-source
instrumentation; the deterministic fault/mutant matrix remains partial; real
search and controlled real reduction need their full gate proof. The missing
obligations are recorded per criterion rather than hidden behind an empty
successful experiment.

To make a gate executable, add an allowlisted verifier together with its exact
expected test and negative-control inventory, fixture/tool/input hashes, budgets
and setup/run/teardown. Its receipt verifier must inspect actual assertions and
hashes, including child receipts from the same clean candidate. Merely changing
this registration schema or marking criteria passed is rejected. The aggregate
must stay non-pass until all four executable gates plus source/layout and
evidence validation are implemented and proven.

Regression checks:

```sh
python3 -m unittest discover -s scripts -p test_prove.py
```

Controls cover all five refusal paths, missing/forged child registrations,
incomplete mappings, self-certification, missing source inputs, and exact Go
result validation rejecting zero expected tests, missing tests and skipped or
failed negative controls. Synthetic event streams test the result parser only;
they are not workflow or distributed-systems evidence.
