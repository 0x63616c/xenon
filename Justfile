set dotenv-load := false

# Build the pinned native SlateDB library and Xenon CLI, reusing verified caches.
xenon:
    go run ./cmd/xenon-build

