set dotenv-load := false

# Build the latest Xenon source when it changes, then run the cached binary.
xenon *args:
    #!/usr/bin/env bash
    set -euo pipefail

    readonly root="$(pwd)"
    readonly cache_dir="$root/.local/bin"
    readonly binary="$cache_dir/xenon"
    readonly fingerprint_file="$cache_dir/xenon.source.sha256"
    readonly native_dir="$root/.local/slatedb-native-target/debug"

    mkdir -p "$cache_dir"
    fingerprint="$({ git ls-files 'cmd/**' 'internal/**' go.mod go.sum; git ls-files --others --exclude-standard -- 'cmd/**' 'internal/**'; } | grep -v '_test.go$' | sort -u | xargs shasum -a 256 | shasum -a 256 | cut -d ' ' -f 1)"

    if [[ ! -x "$binary" || ! -f "$fingerprint_file" || "$(cat "$fingerprint_file")" != "$fingerprint" ]]; then
        echo "building xenon..." >&2
        CGO_ENABLED=1 \
        CGO_LDFLAGS="-L$native_dir" \
        go build -o "$binary" ./cmd/xenon
        printf '%s\n' "$fingerprint" > "$fingerprint_file"
    fi

    export DYLD_LIBRARY_PATH="$native_dir${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
    export LD_LIBRARY_PATH="$native_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    exec "$binary" {{ args }}
