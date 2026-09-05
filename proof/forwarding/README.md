# Internal forwarding transport proof

Run `python3 scripts/prove.py forwarding` from a clean checkout with pinned Go1.27.1. The committed manifest verifies the exact tests and records source, input and tool hashes. Tests start loopback gRPC servers and close them automatically; no external services or credentials are needed.

`internal/routing` accepts complete protobuf operations carrying a stable partition ID. It requires a caller deadline, resolves a ready owner, invokes the owner's local dispatch or forwards the same request. One refresh retry per node and at most two forwarding hops bound cyclic routes. Local dispatch must still enforce the ownership manager's admission and SlateDB fencing; a routing decision grants no storage authority. Closing rejects new admission and closes outbound connections; already-admitted local work retains its deadline and ownership lifecycle.

The test resolver and recorded response are memory fixtures. These tests prove transport behavior and invocation identity, not durable replay or ownership handover. The native persistence proofs cover journal durability separately. Wiring the router into the node process requires the S3-backed ownership directory and per-partition dispatch manager, followed by combined multi-node fault tests. No static map has been installed as production authority.
