# Xenon

Experimental S3-backed persistence for Temporal.

The goal is an end-to-end proof with existing Temporal SDKs, Omes, visibility and Temporal UI, including dynamic storage-node scale-out and crash recovery with disposable local disks.

We are retaining Temporal Server and investigating a gRPC persistence adapter with partitioned storage nodes. SlateDB is a candidate, not a final choice. No working server or production-readiness claim exists yet.

Start with [the autonomous delivery handoff](docs/handoff-autonomous.md). Calum has delegated architecture and implementation decisions to coordinated agents using an advocate/reviewer debate and evidence-based validation. Progress is tracked through the [Wayfinder map](https://github.com/0x63616c/xenon/issues/1); see `docs/agents/issue-tracker.md` for tracker limitations.

Future direction: a professional open-source release; possible BYOC service. This repository is private during investigation.
