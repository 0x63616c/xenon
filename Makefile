.PHONY: test probe-local stop generate

test:
	cargo test --locked --workspace
	go test ./...

probe-local:
	./scripts/probe-local.sh

# Stop local services while retaining the emulator object-store volume.
stop:
	docker compose -f deploy/compose.yaml down

generate:
	./scripts/generate-proto.sh
