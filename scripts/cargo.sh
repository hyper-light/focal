#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# protobuf-build 0.15.1 builds a native host protoc from its pinned source on
# Unix when needed. Honor an explicit PROTOC without requiring old x86 binaries.
exec cargo "$@"
