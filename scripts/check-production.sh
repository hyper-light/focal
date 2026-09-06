#!/usr/bin/env bash
# Keep test-only panic allowances out of the production verification build.
set -euo pipefail
task_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$task_root"
bash scripts/cargo.sh clippy --workspace --lib --bins --locked -- \
  -D warnings \
  -D clippy::panic \
  -D clippy::unwrap_used \
  -D clippy::expect_used \
  -D clippy::unreachable \
  -D clippy::indexing_slicing \
  -D clippy::arithmetic_side_effects \
  -D clippy::disallowed_macros \
  -D clippy::todo \
  -D clippy::unimplemented \
  -D clippy::dbg_macro
