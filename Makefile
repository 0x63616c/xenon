.PHONY: test probe-local stop

test:
	cargo test --locked --workspace

probe-local:
	./scripts/probe-local.sh

# Stop local services while retaining the emulator object-store volume.
stop:
	docker compose -f deploy/compose.yaml down
