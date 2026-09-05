# Live visibility oracle candidate

The committed live-oracle.json commands target the existing ministack. No native build or live workload was executed during implementation. The parent controller supplies a clean built binary, scoped evidence directories, namespace readiness and actual ownership movements; this program never changes topology. Reports explicitly set full_acceptance and ownership_movement_executed false.

Seed uses the supported Xenon VisibilityStore adapter against stable storage ingress. Queries use the genuine Temporal WorkflowService ListWorkflowExecutions and CountWorkflowExecutions APIs. The actual namespace UUID comes from DescribeNamespace, is bound into each report, and drives the existing partition hash; the frozen2000UUID/data/status/memo recipe is otherwise retained. Synthetic visibility records do not represent real workflow histories. TaskQueue isolates the dataset from SDK/Omes traffic.

Every checkpoint verifies exact2000RunIDs, workflow IDs, statuses, count, and four500-record ExecutionStatus groups with page sizes1/7/100. The final three1.1MB memos exercise global byte limits. Compare set/group hashes and namespace identity across caller-controlled movement checkpoints; the command does not infer a movement from repeated reads.

The mutation mode uses20separateUUIDs/taskqueue records. A writer changes WorkflowID with monotonically larger TaskID while public scans proceed after per-write release barriers. During this phase only the known original/updated name is allowed, with exact immutable RunID/status membership; after the writer finishes every updated name is required. This is controlled live mutation, not snapshot-isolation or exhaustive concurrent scheduling proof. Rerun in a fresh scoped environment; reusing prior mutated data is not an equivalent initial state.

All Temporal API calls have30-second contexts and adapter calls retain their existing30-second ceiling. The entire command is bounded at15minutes. Failure exits nonzero and produces no success report. Output paths must be new; source/binary/environment provenance and process cleanup are the enclosing controller's responsibility.

`go test -race ./cmd/xenon-visibility-probe` runs lightweight recipe/set/group controls with a fake public API, including duplicate rejection. It is not real Temporal execution. Real checkpoint execution is pending a safely available ministack.
