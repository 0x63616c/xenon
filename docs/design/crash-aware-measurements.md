# Crash-aware proof measurements

Delegated decision, 2026-09-05, linked to [measurement issue73](https://github.com/0x63616c/xenon/issues/73). The coordinator adjudicated the independent advocate/reviewer exchange. This document selects a proof design; no external recorder has been implemented or executed, and the frozen full profile is unchanged.

The current asynchronous completion-only trace correctly fails completeness after SIGKILL. Its queue can lose an unknown number of starts and terminals; phase markers cannot reconstruct those events. Retain that verdict for existing evidence. Missing telemetry is not a successful measurement.

## Selected minimum protocol

Use one external recorder in the existing proof controller's failure domain, surviving all declared Temporal/Xenon producer kills. It stores proof telemetry only, never application state, routing authority or a persistence outcome. It adds no application durability dependency. Its own death, capacity exhaustion, malformed events or unavailable output fails the measurement run. The application remains unchanged when instrumentation is disabled.

Each helper invocation and each generated Execute attempt registers a unique producer-incarnation/sequence/ID before its measured work is allowed to proceed. Registration is idempotent: identical retries/readback return the same acknowledgment, conflicting content fails. The producer waits for acknowledgment with a bounded deadline. Recorder loss fails admission in proof mode and the overall measurement proof; it must never silently fall back to unobserved calls. Capture family/method and existing operation/partition identity when available, with no payload or error text.

Registration is **not proof that execution started**. The recorder may accept BEGIN and lose its acknowledgment before the producer starts work. Every registered ID ultimately has either an observed terminal (completed or explicitly not admitted) or a terminal-unobserved classification. Terminal-unobserved means execution may never have started, may have finished before its END was lost, or may have been interrupted. Do not infer which case occurred from the absence of END. The set is a conservative registration census covering permitted measured work, not an exact census of executed operations.

Completed terminals carry the producer's monotonic duration, status and attempt linkage. A separate registration-handshake duration records observer overhead. Start the instrumented helper clock before registration, so acknowledgment latency is included in its measured work duration. Capture the terminal timestamp after the actual helper result is determined; report terminal-delivery/phase-flush overhead separately rather than pretending it was measured before it occurred. The observer changes timings: retain the same5s/30s thresholds and label these instrumented durations. Do not subtract estimated observer cost to make a failing run pass.

A bounded phase flush/readback confirms all records accepted so far. The controller also records actual producer death and incarnation, recorder closure, fault trigger and recovery boundaries. Sequence continuity and terminal pairing are checked offline; overflow, gaps, conflicts or missing recorder closure fail census integrity. Producer death can leave known registrations terminal-unobserved even when the recorder's own census is intact.

## Measurement eligibility

Keep these distinct report fields: registration-census integrity; registration count; actually completed count; explicitly not-admitted count; terminal-unobserved IDs/count; and latency eligibility for each predeclared window/family. Never collapse them into an unqualified `all traces complete` result.

A steady window is eligible only after its phase barrier reconciles **every** registration, including admission failures, and there are at least100 completed observations for each required family. Unknown terminals or missing boundaries invalidate that window; do not remove uncertain/slow calls after inspecting results. Classify cross-boundary calls prospectively. If the controller needs quiescence to close a steady phase before arming a kill, declare that boundary protocol before execution and fail if it cannot settle within its bound. Do not retrospectively relabel an uncertain steady call as fault traffic.

A fault window retains every observed raw completion duration plus the exact terminal-unobserved registration set. Unknowns have null duration/status, not zero, timeout, success or `kill_time - begin_time`. That subtraction is not even a justified lower bound when END may have been lost after completion. Do not include unknowns in a percentile or claim a complete fault-latency distribution. The existing full contract imposes steady p99/max and separate fault recovery/drain budgets; fault unknowns must additionally reconcile with independent application-state/acknowledgment oracles. They cannot excuse acknowledged loss or false condition success.

All existing thresholds remain: steady p99≤5s/max≤30s, visibility≤60s, mixed completion≤900s, fault resume≤120s/drain≤300s. The raw helper/Execute distinction remains: these are helper RPC invocations and generated Execute attempts, not whole public persistence APIs or underlying transparent gRPC retries.

## Bounded implementation and falsifiers

First implement a loopback-only proof recorder with bounded registrations/events, idempotent request handling, explicit producer registration and an offline validator. Add optional adapter registration/terminal hooks behind a separate proof configuration, preserving the disabled path. Keep it separate from ownership admission and S3 application data.

Commit controls for ACK lost after accepted BEGIN; identical/conflicting retry; kill before work after registration; END lost after actual completion; producer kill during an invocation; phase flush with unresolved entries; sequence gap/overflow; recorder death/timeout; restart with a fresh incarnation; and closed steady windows with insufficient samples. Prove the recorder survives the exact producer process-group kill used by the harness. Validate deliberate completed-call delay plus registration delay against monotonic durations; validate that unknown terminals never receive synthetic durations or enter histograms. Only after those pass integrate the unchanged workloads and fault schedules. No current smoke or measurement failure is reclassified by this decision.
