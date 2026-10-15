// A production source with a local allowance of a lint the policy forbids
// (CLAUDE.md §1): `scripts/check_production_policy.py` must refuse it.
#![allow(clippy::unwrap_used)]
fn main() {
    let value: Option<u8> = std::env::args().nth(1).and_then(|arg| arg.parse().ok());
    let _ = value.unwrap();
}
