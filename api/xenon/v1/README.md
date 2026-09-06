# Xenon v1 wire contract

Canonical schema and Go bindings live together in `api/xenon/v1`. Run
`scripts/generate-proto.sh` to reproduce bindings using pinned protoc 36.0,
protoc-gen-go 1.36.10 and protoc-gen-go-grpc 1.5.1. The virtual proto source path
remains `xenon/v1/*.proto` through `-I api`; protobuf package and gRPC service
names remain `xenon.v1`.

Migration from base `3a83534` moved `proto/xenon/v1/*.proto` and
`gen/xenon/v1/*.pb.go` here. The sole schema-byte change is the `go_package`
option from `github.com/0x63616c/xenon/gen/xenon/v1;xenonv1` to
`github.com/0x63616c/xenon/api/xenon/v1;xenonv1`. This is an explicitly delegated
coordinator clarification permitting generated-language metadata changes;
no messages, fields, tags, dependencies, services or persisted keys changed.
`migration-source-hashes.json` records every old/new schema hash. Historical
receipts retain their original source paths and hashes.

Before/after descriptor sets produced with pinned protoc were equal after
sorting by filename and clearing only `FileOptions.go_package`. Their normalized
SHA-256 is `b25f1bb1ec15a3d522fbc12994e352ec5b13a05912ae48e2eb3f296cd37ba09a`.
`TestCanonicalLayoutPreservesWireDescriptors` enforces that golden against all
13 compiled descriptors and separately requires the canonical Go option. This
covers wire/service metadata identity; runtime behavior remains subject to the
existing persistence/routing/native and full scenario suites.
