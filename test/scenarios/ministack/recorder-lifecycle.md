# Optional external registration census

Run `python3 scripts/prove.py recorder-lifecycle` from a clean checkout for the committed lightweight subprocess and adapter controls. Runtime opt-in is `scripts/ministack-runtime.py --external-recorder`; it is mutually exclusive with the older asynchronous `--measurements` trace mode. Default smoke behavior is unchanged.

The controller opens one prospectively declared fault phase for the entire run. Every Temporal process receives a fresh incarnation. A separate recorder process group survives producer termination and cold restart. Registration is synchronized to its proof journal before acknowledgment; this is proof instrumentation, not an application durability dependency. HTTP protocol rejection poisons recorder finalization.

Only an independently observed, previously declared SIGKILL is expected. Graceful producers must exit successfully and emit exactly one close marker after stopping the server and observer. Missing markers, unexpected exits, recorder loss, invalid journals or resource/S3 report failures make measurement status fail while retaining the functional result separately. The final journal is validated offline and hashed.

`registration_census_complete` describes registered events and explicit terminal-unobserved entries. Lost acknowledgment can mean execution never started; missing terminal can mean execution completed. No durations are fabricated. `measurement_complete` is limited to this declared registration census plus sampled resource and S3 HTTP aggregates. `steady_gate_executed`, `full_acceptance`, and `fault_latency_distribution_complete` remain false. There is no dynamic phase switching or steady p99 claim in this slice.

The controls launch actual recorder and producer subprocesses, kill the producer after registration, preserve the recorder, and complete a replacement incarnation. They also reject recorder loss, a missing graceful marker, changed configuration and malformed HTTP metadata. These controls do not execute the complete ministack.
