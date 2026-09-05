# Go shard owner proof

From a clean checkout with Git, Python 3, Go 1.27.1, Rust 1.94.0 and a C toolchain:

```sh
python3 scripts/prove.py go-shard
```

The committed manifest `experiments/go-shard.json` builds the official native library from the exact revision in `tools/slatedb-native.json`, verifies the published Go binding against that source, builds the Go server, and executes seven registered commands. Downloads and build products remain under ignored `.local`. A JSON report records the clean source revision, input hashes, tool versions, native library and server binary hashes, exact test events and command outcomes. `--allow-dirty` is development-only and never produces `proof_pass: true`.

The RPC fixtures use only the freshly built Go server. Native tests exercise read/write outcome replay after reopening, fencing rejection on replay, stalled `AwaitDurable`, bounded caller return, rejection after quarantine, retained resources, flush-driven drain, and admission paused across quarantine. This is a memory-object-store proof; S3, process-kill recovery, directory admission, complete Temporal boot and distributed orchestration remain separate gates.

`internal/node.Owner.Run` holds one partition gate across the complete callback, including all native calls and durability. The callback must retain and release its own native transaction/write handles and must not return a handle. It may continue after the caller times out. Native/unknown outcomes return gRPC Unavailable and quarantine the owner; persisted logical failures are encoded in result bytes. Timeout quarantine is permanent for that owner, even if the native call later finishes. Completion cannot release the gate until the caller has resolved its deadline decision. Close waits for the gate and never destroys an active native handle. A hung native future requires process replacement.

The runnable server defaults to S3. Set `XENON_BUCKET`, `XENON_PREFIX`, `XENON_PARTITION`, optionally `XENON_LISTEN`; the pinned SlateDB URL resolver forwards environment options to its S3 object-store implementation (including endpoint and credential configuration). `XENON_BACKEND=memory` is explicit test configuration. No durable local store is used. Run with the built native library directory on `LD_LIBRARY_PATH` (Linux) or `DYLD_LIBRARY_PATH` (macOS). Startup has a 30-second process-exit watchdog. S3 configuration support is source-implemented here, not an executed S3 proof.
