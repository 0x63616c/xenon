# Xenon

Experimental S3-backed persistence for Temporal.

The goal is an end-to-end proof with existing Temporal SDKs, Omes, visibility and Temporal UI, including dynamic storage-node scale-out and crash recovery with disposable local disks.

We are retaining Temporal Server and investigating a gRPC persistence adapter with partitioned storage nodes. SlateDB is a candidate, not a final choice. No working server or production-readiness claim exists yet.

Architecture decisions are made with Calum through the Wayfinder issue map. See the repository issues and `docs/agents/issue-tracker.md` for progress and tracker limitations.

Future direction: a professional open-source release; possible BYOC service. This repository is private during investigation.
