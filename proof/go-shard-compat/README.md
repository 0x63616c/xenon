# Stored shard compatibility

Run `python3 scripts/prove.py go-shard-compat` from a clean checkout. Requires the pinned Rust and Go toolchains, Docker Compose, AWS CLI, and local port 19002. The runner builds both nodes and the pinned native Go library, starts a dedicated pinned MinIO project, and executes the committed fault schedule. It records source/config hashes, binary hashes, tool versions, exact assertions, and cleanup in `.local/evidence/`.

The test kills Rust after acknowledged writes, opens the same object prefix with Go, verifies exact shard bytes and old recorded outcomes, writes with Go, kills it, and verifies recovery and replay with Rust. Only the pinned shard schema and local emulator are proved; this is not a general migration, real AWS, or full Temporal proof.
