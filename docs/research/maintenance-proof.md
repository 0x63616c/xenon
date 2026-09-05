# Native maintenance proof

Run `python3 scripts/prove.py maintenance` from a clean checkout. The manifest pins the native source/library, Go binary, MinIO image and all fixture settings. The controller owns one scoped Compose project on port19005 and removes only its containers/volume.

The fixture accelerates compaction and normal manifest/WAL/obsolete-SST GC, retaining metadata boundary protection and WAL-fence dry-run. These are test settings only. Production defaults are unchanged. A protected native snapshot is retained through observed maintenance, then released before requiring obsolete-SST collection to advance. Native deletion counters alone are insufficient: the test also records actual S3 object IDs and verifies disappearance by class, and checks committed compaction metadata. No objects are manually deleted to manufacture maintenance progress.

An acknowledged update without an explicit memtable flush tests WAL recovery by a competing owner. The old writer must be fenced, replay must preserve the result, retained zero-byte WAL fences must survive the accelerated minimum age, and a fresh handle must read the final bytes. Local disk caches are not configured. This is not process-kill composition, real S3, production certification or a complete Temporal maintenance proof.

The pinned engine writes a hard-coded 900-second checkpoint before retiring compaction inputs (`compactor_state_protocols.rs::write_manifest`). The test allows1200seconds for normal expiry and collection; it never removes checkpoints or changes the clock. This wait is part of the reproducible setup. The current manifest compaction marker is LastCompactedL0SstViewId, not the legacy optional SST ID field.
