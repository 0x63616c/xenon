# Per-instance workload schema precondition

The #94 agent proof now observes the required schema on each addressed frontend
before recording its schema readiness. `xenon-sdk-probe --mode schema-ready`
uses the explicit `--address` and workload namespace. It performs:

1. `OperatorService.ListSearchAttributes`, requiring `XenonProof`,
   `OmesExecutionID` and `KS_Keyword` as Keyword and `KS_Int` as Int. On pinned
   Temporal this forces that frontend's cluster schema refresh and reads the
   namespace's persisted aliases.
2. `WorkflowService.CountWorkflowExecutions` with a conjunction containing all
   four aliases and correctly typed literals. Xenon's actual visibility compiler
   reads its schema provider and namespace mapper before executing the count.
   A successful operator list alone is not sufficient.

This creates no canary workflow. These are normal routed persistence reads, which
retain the existing durable replay/barrier semantics. The returned count need not
be zero; the query's purpose is typed schema validation, not an absence assertion.
The receipt records the addressed instance, returned attributes, exact query and
count. Source and binary hashes retain the precondition's implementation identity.

The initial A/B checks share the original 120-second bootstrap setup budget with
bootstrap itself. Joining C and restarted B must pass the check within their
existing 120-second startup observation, and all three cold instances use the
same combined app-health/schema requirement within their existing startup waits.
The ten-minute profile uses the same start callback. Each probe receives only
the remaining setup time and refuses success after its deadline. Missing schema
or alias propagation is retried only as a setup precondition; it never suppresses
an Omes/SDK failure or lengthens a counted workload's deadline. Background process
failures still escape the wait immediately and retain their original cause.

This is a harness precondition, not a production traffic-admission mechanism.
A newly started frontend may accept traffic before the harness observes it. The
queries exercise the addressed frontend's current visibility provider/mapper;
they do not flush or prove the independent history/worker caches, nor promise
future availability during a move. The earlier hosted missing-attribute failure
remains separate evidence until an appropriate composed run succeeds.

Tests pin the exact four-name/type contract, reject each missing/wrong field,
require count validation despite an apparently complete operator response,
exercise pending propagation and cancellation, and verify direct instance
addressing with the unchanged remaining deadline. They use injected clients and
no live proof endpoints.
