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
