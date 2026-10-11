#!/usr/bin/env bash
# The production no-panic policy (CLAUDE.md §1) on every library and binary
# target of the workspace, the measurement tools included, in two parts:
# Clippy denies the policy's lints on the compiled targets, and
# `check_production_policy.py` refuses any production source that carries an
# `allow` of one of them — the opt-out a `deny` would honour (a `forbid`
# level is not usable: derive macros emit allowances of their own). Test-only
# allowances stay scoped to test builds (`cfg_attr(test, allow(..))`, an
# attribute inside a `#[cfg(test)]` item), which `--lib --bins` never
# compiles. The policy check proves itself on its fixtures first.
#
# No runtime recursion (CLAUDE.md §1a): `check_recursion.py` reads the
# unoptimised LLVM IR of every library and binary target, the vendored
# `hyper-*` crates included, and refuses any call-graph cycle through the
# workspace's code, and the source for a future-returning function that calls
# itself; it proves itself on its fixtures first.
set -euo pipefail
task_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$task_root"
python3 scripts/check_production_policy.py
bash scripts/cargo.sh clippy --workspace --lib --bins --locked -- \
  -D warnings \
  -D clippy::panic \
  -D clippy::unwrap_used \
  -D clippy::expect_used \
  -D clippy::unreachable \
  -D clippy::indexing_slicing \
  -D clippy::arithmetic_side_effects \
  -D clippy::disallowed_macros \
  -D clippy::disallowed_methods \
  -D clippy::todo \
  -D clippy::unimplemented \
  -D clippy::dbg_macro
python3 scripts/check_recursion.py
