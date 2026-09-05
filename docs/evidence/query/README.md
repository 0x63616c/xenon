# Typed query contract checkpoint

Developer validation on 2026-09-05: pinned Temporal 1.31.2 generic converter emits versioned protobuf expressions. `go test ./internal/query ./gen/xenon/v1`, `go test -race ./internal/query ./gen/xenon/v1` and `go vet ./...` passed. Independent reviewer reran the converter tests uncached and identified missing empty-Text validation, now fixed with regression fixtures. Additional fixtures cover alias/group preservation and namespace binding.

This tests type-preserving conversion, int64 precision, default/archetype division, errors and digest binding. It does not evaluate records, implement indexes, prove CHASM mapper remapping or establish PostgreSQL query equivalence. Those remain required downstream tests.

Generation: install protoc 36.0 and run `make generate`. The script pins protoc-gen-go 1.36.10; generated sources are committed. Go test runtime is pinned to 1.27.1. Containerized clean proof and provenance-runner integration remain separate gates; these developer runs are not release evidence.
