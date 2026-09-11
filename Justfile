set dotenv-load := false
set positional-arguments := true

# Build the pinned SlateDB-backed Xenon CLI if needed, then run it.
xenon *args:
    @go run ./cmd/xenon-build
    @exec ./.local/bin/xenon "$@"
