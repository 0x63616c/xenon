# Generated CLI component journey

From a clean checkout with the pinned native build from `scripts/build-go-node.py`:

```sh
python3 scripts/cli-generated-search-proof.py \
  --native-root /absolute/path/to/checkout-with-native-build \
  --evidence /absolute/path/outside-checkout/new-component-evidence
```

The optional native root defaults to the current checkout. The script verifies
its build receipt, library checksum, binding checksum, clean pinned native source
and Cargo lock. It builds the actual CLI from the current clean revision with
pinned Go and checks the binary's actual SlateDB/Temporal module paths, versions,
checksums and absence of replacements, plus its exact native build attestation.
It records the binary hash and embedded build identity. Reused native
artifacts remain build attestations, not independent runtime loader proof.

Four finite seeded searches must complete with fixed workflow bytes and at least
two distinct expanded orders. Replay uses the last saved artifact, an empty
working directory and empty tool PATH after moving the harness's original input
away. Expanded scenario and decision trace must match and the original artifact
must remain unchanged. A separate continuous search must return budget exit 2.
There is no minimum continuous throughput requirement: budget exhaustion never
means full correctness. Commands and output hashes are retained on failure.

This is a narrow component journey. It is **not** `workflow-search-dst` or any
#119 acceptance gate. It neither covers the missing fault cuts nor instruments
all I/O, and the empty replay PATH is not a filesystem sandbox. The native engine
is linked but the coupled execution uses modeled external effects.

Run assertion negative controls without building or launching services:

```sh
python3 -m unittest discover -s scripts -p test_cli_generated_search_proof.py
```
