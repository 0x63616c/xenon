.PHONY: help smoke fuzz-soak test-harness check-layout check build test test-rust test-go proof probe-local stop generate

test: test-go

check: check-layout test-harness test-go test-rust

.NOTPARALLEL: check

test-rust:
	cargo test --manifest-path test/compatibility/rust/Cargo.toml --target-dir target --locked --workspace

# Build the pinned native library before linking Go tests on either supported host.
test-go:
	python3 scripts/build-go-node.py
	env GOENV=off GOWORK=off GOFLAGS=-mod=readonly GOTOOLCHAIN=go1.27.1 \
		CGO_ENABLED=1 CGO_LDFLAGS="-L$(CURDIR)/.local/slatedb-native-target/debug" \
		LD_LIBRARY_PATH="$(CURDIR)/.local/slatedb-native-target/debug" \
		DYLD_LIBRARY_PATH="$(CURDIR)/.local/slatedb-native-target/debug" \
		SLATEDB_UNIFFI_RUNTIME_THREADS=2 XENON_NODE_BINARY="$(CURDIR)/.local/bin/xenon-go-node" \
		go test -race ./...

CASE ?= go-runtime-stores
proof:
	python3 scripts/prove.py "$(CASE)"

probe-local:
	./scripts/probe-local.sh

# Stop local services while retaining the emulator object-store volume.
stop:
	docker compose -f deploy/compose.yaml down

generate:
	./scripts/generate-proto.sh

build:
	python3 scripts/build-go-node.py

help:
	@echo "make build          Build Go server and pinned native library"
	@echo "make test           Run Go race tests"
	@echo "make test-harness   Run Python controller regression tests"
	@echo "make proof CASE=... Run one registered component proof"
	@echo "make smoke          Run real Temporal/MinIO smoke and cold recovery"
	@echo "make fuzz-soak      Run saved Omes corpus for the declared soak"
	@echo "make check          Run local layout, harness, Go and Rust checks"

check-layout:
	python3 scripts/check-layout.py

test-harness:
	PYTHONPATH=scripts python3 -m unittest discover -s scripts -p 'test_*.py'

smoke:
	python3 scripts/ministack-runtime.py

fuzz-soak:
	python3 scripts/ministack-runtime.py --fuzz-soak
