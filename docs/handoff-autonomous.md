# Xenon: autonomous delivery handoff

## Start here

You are the coordinating agent for [0x63616c/xenon](https://github.com/0x63616c/xenon), currently private. Your goal is to finish the Wayfinder effort and deliver the working end-to-end proof, with professional repository packaging and reproducible evidence. Do not stop at another plan, a scaffold, a successful build, or a single happy-path workflow.

**Latest user authorization supersedes the earlier human-in-the-loop workflow.** Calum explicitly requested full-auto operation, coordinated subagents, and agent debate to resolve decisions formerly labelled grilling. You may make project architecture, implementation, test, packaging and integration decisions on his behalf within the requirements below. Do not repeatedly ask him to choose or approve routine work. Record choices as **delegated agent decisions**, never as statements Calum actually made.

The word “Wavefront” in the latest request is understood in this conversation as **Wayfinder**, Matt Pocock's issue-map process. Do not introduce another tracking product.

Read this handoff, AGENTS.md, CONTEXT.md, the live map, current ticket comments and the three research reports. Check live state before mutating; this document is a checkpoint, not authority to overwrite subsequent changes. Start working, not by asking whether to proceed.

## What we are building and why

Xenon is the working codename for an object-storage-backed persistence system for Temporal. The inspiration is WarpStream: keep durable customer data in their own object storage, separate compute from durable storage, and reduce the burden of operating traditional databases.

Retain Temporal Server's workflow engine and public frontend. Implement its persistence extension interfaces for execution and visibility. The leading architecture is a gRPC persistence adapter inside Temporal that sends complete persistence operations to partitioned Xenon storage nodes. Each node can own several storage partitions. S3 carries durable data; local caches are disposable.

SlateDB is the leading candidate, not a proven or irrevocable choice. It is embedded and permits one active writer per database. Multiple databases can be owned by different nodes. The agent must establish transaction-safe placements and safe ownership, rather than assume all servers can open the same prefix for writing.

Calum is a Staff infrastructure engineer working with Kafka and Temporal, previously on a workflow-engine team. He understands platform operations and wants technically credible evidence, not enthusiasm dressed as certainty. Be concise in progress reports; do not give time estimates.

## Non-negotiable requirements and priorities

1. S3/object storage is the only durable application-storage dependency, including Xenon ownership/placement metadata. Do not quietly add PostgreSQL, DynamoDB, etcd or a persistent-disk replication tier. Hosting infrastructure may have its own control plane, but Xenon must not rely on it as a hidden durability authority.
2. Existing Temporal SDKs and Temporal UI must work. Both execution persistence and visibility are part of the proof.
3. Dynamic storage-node scale-out is required: add a node while work runs, actually redistribute partitions, and show the added node serving traffic.
4. Multiple Temporal instances must be supportable independently of Xenon storage-node ownership. A single process can be an early smoke test, never the delivered architectural restriction.
5. Acknowledged persistence mutations must survive serving-process failure and loss of local cache/disks. Ownership transfer must not corrupt state.
6. Slower operation is acceptable. Do not trade away durability, correctness or necessary functionality merely to chase latency. Bound retry/timeouts thoughtfully; slower does not mean silently accepting permanent lack of progress.
7. Retain Temporal's public behavior as the compatibility goal. Build a pinned, explicit feature matrix. Stage implementation, but do not declare “all features” from passing a small subset. Do not silently remove hard features.
8. Use **Omes**, Temporal's load generator at temporalio/omes. Earlier “OEMS” was a spelling misunderstanding; this is not a finance workflow.
9. Produce a professional OSS-ready codebase: understandable design, reproducible setup, CI, tests, observability, examples and accurate limitations.
10. Customer-owned data and future BYOC SaaS should remain possible. Do not build an unnecessary proprietary control-plane dependency.

The naming discussion is over for now: use Xenon. Avoid reopening branding while implementing.

## Autonomy and two-agent decision protocol

Adapt Wayfinder explicitly; do not pretend an agent is literally Calum.

For each former grilling ticket:
- Claim the ticket and load its question, blockers and evidence.
- Spawn two independent agents with the same evidence and constraints.
- **User-priority advocate:** argue for the choice best matching Calum's expressed priorities: S3-only, scalable, simple to operate, minimal divergence from Temporal, compatible, slower allowed, polished delivery. This is a proxy role, not personal impersonation.
- **Adversarial systems reviewer:** challenge correctness, transaction boundaries, stale owners, failure interleavings, compatibility, maintenance costs and untested assumptions. Offer an alternative, not merely objections.
- Have them exchange concrete objections and revise their positions. They may agree when evidence warrants it; do not manufacture conflict.
- The coordinator decides using evidence and the priority ordering. A debate cannot substitute for a test: create and execute a bounded experiment when a factual unknown controls the choice.
- Record options, evidence, objections, chosen trade-off, remaining risks, falsifying tests and revisit trigger in the issue resolution. Label it “Delegated agent decision under Calum's autonomous-delivery authorization.”
- Close the decision ticket, add a short linked pointer to the map, and create the next specific tickets. Continue automatically.

User-requested adaptations override the upstream skill's planning-only default, live-human grilling requirement, stop-after-charting rule, and one-nonresearch-ticket-per-session limit for this effort. Work through as many tickets and sessions as needed. Keep the map as an index; details live in tickets and linked artifacts.

Use independent implementation, review and validation agents where useful. Bound their tasks, give each clear files/branches and acceptance criteria, and avoid concurrent edits to the same files. The coordinator integrates and verifies. Agents do not get to weaken requirements by voting.

Commit and push completed work. Use branches/worktrees and PRs for review; merge changes after meaningful review and passing gates, subject to actual branch protections. Do not bypass platform permissions or fake approvals.

## Definition of shipped

Deliver a reproducibly runnable Xenon proof, not a production certification:
- Pinned Temporal integration, functioning execution and visibility stores, existing UI and SDKs.
- Omes scenarios covering basic execution then meaningful combinations of activities, timers, signals, retries, child workflows, updates and Continue-As-New as applicable to the chosen coverage.
- Explicit visibility API assertions and UI exercises, including search attributes, filtering, counts and pagination.
- At least two Xenon nodes serving assigned partitions; demonstrate addition of a node during workload and ownership redistribution. Multiple Temporal instances exercised.
- Fault-injection proof around commits, acknowledgements, lost RPC responses, stale routes, competing owners and recovery from S3 without local state.
- Evidence that acknowledged mutations remain recoverable, condition checks remain correct, and affected work resumes after faults stop.
- Automated tests, CI and a documented single-entrypoint local setup; pin containers/tools and provide teardown.
- Repeatable real-S3 validation where credentials and authorized resources are available. An emulator alone does not prove real S3 semantics. Record unavailable real-S3 validation as a blocker, not a pass.
- Reproducible benchmark/reporting for completed work, latency distributions, visibility lag, object requests, CPU/memory and recovery. Agree concrete proof targets through delegated debate before using them as gates. Do not invent results.
- README quickstart, architecture and decision docs, operations/recovery guidance, compatibility matrix, troubleshooting, examples and a release checklist. A clean end-to-end demonstration is included.
- The Wayfinder map reflects actual completion. Close the final map only after its destination is met; unresolved external prerequisites remain visible.

Prepare a simple professional landing-page draft after the core proof is working if it advances OSS readiness. Show only implemented capabilities and measured results. A production SaaS, billing and custom control dashboard remain future scope; existing Temporal UI is required now. Keep the repository private until explicit public-release authorization; “ship” here authorizes delivery/integration of the proof, not disclosure of the private repo. Prepare publication assets without making that a reason to stop engineering work.

Use already-authorized test environments and credentials within their scope. If a missing secret, access right, unavoidable external spend or destructive operation truly blocks completion, finish all independent work, preserve a runnable checkpoint, and state the exact blocker. Do not ask routine design questions or claim an external capability exists.

## Current repository and tracker state

At handoff, main has README.md, AGENTS.md, CONTEXT.md, docs/agents/issue-tracker.md and this handoff. No server implementation exists. No runtime tests have run.

Canonical map: [Prove S3-backed Temporal with dynamically scalable Xenon storage](https://github.com/0x63616c/xenon/issues/1).

| Ticket | Checkpoint state | Dependency |
| --- | --- | --- |
| [Identify transaction-safe persistence partitions](https://github.com/0x63616c/xenon/issues/2) | Closed: bounded source research | None |
| [Establish durable writes and safe SlateDB ownership transfer](https://github.com/0x63616c/xenon/issues/3) | Closed: bounded source research | None |
| [Define the visibility compatibility and indexing options](https://github.com/0x63616c/xenon/issues/4) | Closed: bounded source research | None |
| [Choose the storage partition layout and engine](https://github.com/0x63616c/xenon/issues/5) | Open; next decision | Partition and durability research |
| [Choose routing and ownership handover](https://github.com/0x63616c/xenon/issues/6) | Open | Layout/engine and durability research |
| [Choose visibility layout and supported proof coverage](https://github.com/0x63616c/xenon/issues/7) | Open | Layout/engine and visibility research |
| [Agree the Omes and failure-test acceptance criteria](https://github.com/0x63616c/xenon/issues/8) | Open | Routing/ownership and visibility decisions |
| [Agree the implementation sequence and local harness](https://github.com/0x63616c/xenon/issues/9) | Open | Acceptance criteria |

The closed research tickets are not runtime correctness proofs. Their reports expose unresolved contracts and tests.

Native GitHub parent/blocking relationships are **not wired**. The previous connector supported issues, labels and file writes but lacked relationship mutations; no authenticated gh CLI was present. Links in bodies are temporary navigation, not a substitute for native edges. The intended numeric graph is in docs/agents/issue-tracker.md. Use authenticated gh/API capabilities if available in your environment to repair native relationships and verify them, without duplicating issues. Keep progressing through the recorded graph if capabilities remain unavailable; report the exact gap. Do not require Calum to do manual tracker administration while useful work remains.

Labels already used: wayfinder:map, wayfinder:research, wayfinder:grilling. Create additional labels as needed. Issues were assigned to 0x63616c to claim research on behalf of agents; assignment does not mean Calum is expected to act.

## Research assets: fetch these branches

These notes are committed on research branches, not in main's directory tree:
- [Partition boundaries](https://github.com/0x63616c/xenon/blob/research/partitions/docs/research/partition-boundaries.md), branch research/partitions, commit 089a18f8b564d9a7412e8bcf4fb1c165452425aa.
- [Durability and ownership](https://github.com/0x63616c/xenon/blob/research/durability/docs/research/durability-ownership.md), branch research/durability, commit 25207d01b2697639fd65567a552e0045d58f1e9b.
- [Visibility](https://github.com/0x63616c/xenon/blob/research/visibility/docs/research/visibility.md), branch research/visibility, commit 39bcfe4649b3bf55733f120671acd05f311860ea.

Integrate the validated notes into main through the normal review flow so future checkouts contain the evidence. Do not delete branches until references remain valid.

Temporal source baseline in partition/visibility reports: 891d1b648b7252925142cc36f13e40a0e2ed4244.
Durability report cross-checks Temporal v1.31.2: 19a774302c613da9adc4436ab14278ccdca8e0a5.
SlateDB v0.16.0: 3fb9e8abab0c9f5833f0c154140ceef009fea02a.
Choose coherent build pins and rerun the contract audit for the selected version; do not blend main and release APIs unknowingly.

The earlier standalone temporal-object-storage-assessment.md had broken /blob/undefined/ links and invalid snapshot cells. It is not checked verified evidence and was deliberately not imported into main. Use the three replacement reports and recheck primary sources.

Earlier subjective probabilities were about 80% for a narrow prototype and 60% for a broader functional system. They were not measured, do not include proven dynamic scaling, and must never become acceptance evidence or a reason to stop checking.

## Technical findings that must survive handoff

### Temporal integration and partitioning

Temporal exposes custom execution and visibility store factories. These are internal/experimental integration seams, not a stable plugin compatibility promise. Retaining the existing frontend/engine reduces the amount of behavioral reimplementation but does not eliminate the storage work.

Execution writes have a strong same-History-ShardID boundary: current execution, mutable state, related runs, maps, buffered events, CHASM nodes and generated tasks must remain colocated with the shard guard. History appends precede some state transactions; don't falsely describe the SQL behavior as one atomic transaction for all history and state.

Candidate domains from inspected code:
- Whole history shards, potentially grouped in one SlateDB database.
- Matching task-queue domains with **all subqueues together**. Both current V1 and V2 CreateTasks can write across subqueues while checking subqueue zero. range_hash includes subqueue; blindly using it as independent placement breaks this transaction.
- Namespace-wide task-queue user data and build-ID mappings: a request can update several queues atomically.
- Global namespace catalog/notification version domain.
- Global Nexus catalog/table-version domain.
- Named QueueV2 domains containing metadata and messages; range deletion updates both atomically.
- Other metadata and enumeration paths need their own placement rules.

List operations across domains require query/pagination machinery, not automatically distributed write transactions. Keep operations intact over RPC rather than attempting client-side remote Get/Put transactions.

Preserve typed condition errors and response fields. DBRecordVersion checks include requested-version-minus-one behavior, legacy NextEventID handling and current-run details. Task user-data Conflicting/Applied flags must survive RPC translation. Generic gRPC Internal for all errors is insufficient.

The [PlanetScale 2022 production example](https://planetscale.com/blog/temporal-workflows-at-scale-sharding-in-production) is real, using shard_id and range_hash with some tables unsharded. It is supporting precedent, not proof that all transactions stayed in one database or that current Temporal features are compatible. Current TaskScanPartitions explicitly mentions Vitess, but is not a complete routing layer.

### SlateDB durability, ownership and scaling

Commit returns before object-store durability. AwaitDurable is required before successful persistence acknowledgement. Default Memory reads can expose committed-but-unflushed state; an independent queue reader can dispatch work based on state later lost in a crash. Waiting only in the writer is insufficient.

A serialized per-partition admission gate through durable acknowledgement is a possible test baseline. It is not a restriction to one server. More concurrent durable-read policies must prove interaction with transaction snapshots/conflicts.

Three distinct mechanisms:
- Temporal RangeID guards History ownership.
- SlateDB writer epoch fences writers of a database prefix.
- Xenon directory generation routes clients/coordinates intended ownership.
None substitutes for the others.

S3 conditional writes can support ownership-directory experiments, but directory CAS plus engine fencing is not a complete proved protocol. A stale contender paused after reserving ownership may resume opening SlateDB after its reservation is superseded and fence the legitimate writer. Post-open rechecks prevent publication, not that disruption. Test liveness after faults stop as well as data safety.

A lost RPC response after commit is an unknown outcome, not rollback. Durable deduplication/result records or operation-specific reconciliation need deliberate design and retention. Never report a conflict as success merely because an earlier request might have committed.

GC, compaction, WAL fences and reader checkpoints are correctness participants. Do not “prove” recovery only with cleanup disabled. Durable does not mean current; stale-reader handling matters.

Scale-out should first move whole logical partitions across nodes with stable S3 prefixes. Storage ownership and Temporal History ownership are independent. Increasing nodes does not require changing Temporal history-shard count. A hot indivisible transaction domain retains a single-writer ceiling; future splitting is a separate problem.

### Visibility and Omes

Visibility is asynchronously updated from durable execution tasks; execution and visibility documents need not share a distributed transaction. It still requires durable acknowledgements, idempotent/version-aware document/index updates and correct late-delete handling.

Use Temporal's generic query converter where possible. SlateDB does not supply a query engine. SQL and ES differ in custom ORDER BY, text analysis and missing/null semantics; choose and document a behavioral baseline without claiming universal equivalence. CHASM list/count, divisions, search-attribute administration and complete returned records must not disappear.

Fixed logical visibility partitions with canonical documents and transactional indexes are a candidate. Query fan-out, count aggregation and merge pagination must handle ownership changes. Durable tokens identify logical partitions/query position, not node addresses. Stable routing does not create a cross-page snapshot. Test frozen-data completeness and concurrent-mutation behavior separately.

TaskID-based version guards help with duplicate/reordered upserts. Deletes may need tombstones plus a safe retention policy; preserve upstream close-before-delete ordering.

Omes has simple scenarios, throughput_stress, kitchen-sink workflows and fuzzing. Pin versions and reproducible inputs. Point Omes at Xenon-backed Temporal rather than accidentally using its embedded default server. Existing UI also needs explicit testing; a successful Omes scenario does not test every visibility or frontend API.

## Immediate execution plan

1. Inspect current repository state and capabilities; read this handoff and research assets.
2. Repair native tracker links where possible and reconcile old human-only text with this authorization. Preserve historical comments; annotate supersession rather than falsifying history.
3. Claim Choose the storage partition layout and engine. Run the two-agent decision protocol. Use concrete code/tests to resolve disputed facts and record the delegated outcome.
4. Resolve routing/ownership and visibility choices, parallelizing only independent work. Specify failure interleavings before implementation.
5. Set measurable acceptance criteria, pin versions and choose a minimal local harness through delegated decisions.
6. Add implementation/prototype/test tickets with dependencies as design clears. Build vertical slices from persistence conformance to a real workflow, visibility, multi-node behavior, ownership faults and Omes. Avoid a giant untested rewrite.
7. Review and integrate completed branches, run meaningful tests, update evidence and map, and keep going through packaging and final proof.
8. Deliver links to the runnable code, exact commands, validation reports and remaining limitations. If blocked, give exact evidence and a resumable state; never declare the project shipped when required gates failed.

## Skills and workflow references

Fetch Matt Pocock's current upstream skills from mattpocock/skills:
- skills/engineering/wayfinder/SKILL.md
- skills/productivity/grilling/SKILL.md
- skills/engineering/domain-modeling/SKILL.md
- skills/engineering/research/SKILL.md

The user's old dotfiles Wayfinder copy was behind upstream. Use upstream with the explicit Xenon autonomy adaptations above. Keep these adaptations in project instructions instead of misrepresenting upstream's human-in-the-loop process.

Use available GitHub tools/CLI for tracking and commits. Discover supported operations rather than inventing tool arguments. Tool limitations from the previous session are not proof your next environment has the same limits. Do not rely on scratch paths or old agent processes surviving: the repo, issue comments and committed branch artifacts are the handoff.

## Suggested kickoff instruction

Read docs/handoff-autonomous.md in 0x63616c/xenon and continue autonomously. Coordinate independent advocate/reviewer and implementation/test agents, resolve the Wayfinder decisions under the recorded delegation, then build and validate the complete S3-backed, dynamically scalable Temporal proof. Keep the tracker and evidence current. Do not stop for routine approvals or after planning; continue until the documented shipping gates pass or a concrete external blocker prevents further progress.
