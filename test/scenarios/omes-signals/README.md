# Pinned Omes signal compatibility overlay

This candidate repairs a generator/Go-worker metadata mismatch in Omes
`c6978ba39aa03551ce28974117e8d7ecf983d2b3`. It is explicitly modified upstream
code. The original corpus under `proof/omes-corpus/` and its failed 900-second
first-input receipt remain unchanged. No full-stack replay pass is claimed here.

The pinned generator resets IDs in each client action set and leaves expected
IDs empty, including absent workflow inputs. The Go worker ignores nonzero IDs
outside its expected list. Thus the original first input's numbered with-start
and return signals are skipped while its unnumbered upsert/timer runs.

The delegated correction adds protobuf field 5, `optional_signal_ids`, as a
strict consume-once whitelist. Required IDs still delay completion; optional
IDs do not. IDs outside both lists remain ignored. The existing worker's
cancellation and handler dispatch remain unchanged. An optional action that was
received need not finish before the workflow returns.

The typed normalizer gives every signal occurrence a unique positive ID across
the entire input, including Continue-As-New successors. Direct client signals
are required; nested workflow/client-activity sends are optional because they
may be abandoned or cancelled. Child workflow payloads are separate execution
contexts. Only the generator's explicit single-signal top-level CAN boundary
with a wait-for-current-run flag is supported; ambiguous shapes fail visibly.
All twenty saved inputs fit this declared shape. Their action trees, order,
concurrency, awaits, cancellation options and payloads compare equal after
removing signal metadata and decoding embedded WorkflowInput payloads.

`corrected-v1/` is a separate prospective corpus. `contract.json` binds old/new
hashes, source pins and the exact patch. No existing runtime profile consumes
these bytes yet. Integrating this profile requires explicitly building the
patched Omes CLI and worker and recording their overlay provenance.

From a clean Xenon checkout, provide clean local source checkouts at the exact
pins in the contract and the pinned protoc 36.0 executable:

```sh
python3 scripts/omes-signal-proof.py \
  --source /absolute/path/to/pinned-omes \
  --api-source /absolute/path/to/pinned-api \
  --protoc /absolute/path/to/protoc
```

The runner copies upstream with `git archive`, reproduces a failing regression
on the original worker, applies the patch, regenerates Go protobuf with pinned
protoc-gen-go v1.31.0, and executes the real worker in the Go SDK test environment
using Omes' actual payload converter. It then regenerates all twenty corrected
inputs twice and compares committed bytes. This is a deterministic workflow
unit environment, not a real Temporal service or an S3 proof. Evidence and
sandbox sources are retained under `.local/evidence/`.

## Explicit corrected runtime mode

`python3 scripts/ministack-runtime.py --corrected-fuzz-soak` selects the new
profile. Original `--fuzz-soak` remains unchanged and still uses the original
corpus. The corrected mode builds both CLI and Go worker from a fresh retained
upstream archive with the exact overlay and regenerated protobuf. Its build
manifest records the source tree, patch, schema generator versions, generated
schema, prepared files, effective SDK and both executable hashes. It does not
claim the CLI has an unmodified upstream VCS revision.

The corrected profile retains all twenty inputs, at least two full rounds,
at least3600 seconds, the900-second per-input deadline and the36000-second
controller bound. Commands differ only in the corpus input paths. Every input
rechecks prepared/source artifacts; the final result rechecks tracked Xenon
inputs and the overlay bundle. The source tree and logs stay in the run's
ignored evidence directory. A corrected success would be new evidence, never a
rewrite of the original failed gate. No corrected full-stack run has yet passed.

`python3 scripts/prove.py corrected-fuzz-controls` checks the mode, unchanged
budgets and artifact-tamper rejection without compiling native code or launching
a stack. The overlay is scoped to this validated generated corpus; it is not a
general validator for arbitrary malformed required/optional ID lists.
