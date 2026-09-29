# Vendored crates

The crates in this directory are built into every focal binary in place of their
crates.io copies (`[patch.crates-io]` in the workspace `Cargo.toml`). Each is the
crates.io archive of the named version, unpacked byte for byte and unchanged;
the archive's SHA-256 was checked against the checksum crates.io publishes for
that version before it was unpacked. Keeping the sources here means a release is
built from what was reviewed, can be built without the registry, and can carry
an upstream fix before a release of the crate does, with the difference visible
in this tree.

| Crate | Version | crates.io archive SHA-256 | Taken | Why it is here |
|---|---|---|---|---|
| `aws-lc-rs` | 1.18.1 | `b281d307588d634de920874890732659e2e7672f72b5e10e81badc1a8a83621e` | 2026-09-29 | The cryptographic provider behind rustls, quinn and rcgen, and the ECDSA verification of enrollment statements ([07](../docs/archictecutre/07-decisions-and-traceability.md)) |
| `aws-lc-sys` | 0.45.0 | `9bff6c3b54fad79a2e60b8102caf565819711497c1f5f092f49508e2f5c31b27` | 2026-09-29 | AWS-LC 5.7.0 itself (C and assembly, with the pre-generated bindings for every target focal ships), built by `aws-lc-rs` |

These directories hold third-party code under their own licenses (`LICENSE`
in each); the release notices and SBOM list them from `Cargo.lock` like any
other dependency, with the archive checksum above as their provenance
(`scripts/release/notices.py`). They are not workspace members: focal's lints
and the production panic policy apply to `crates/` and `tools/`, and these
crates keep their own.

## Refreshing a vendored crate

1. Download the new version's archive and verify it:
   `curl -sL https://static.crates.io/crates/<name>/<name>-<version>.crate -o <name>-<version>.crate`,
   then compare `shasum -a 256` with the `checksum` field of
   `https://crates.io/api/v1/crates/<name>/<version>`.
2. Replace the directory with the unpacked archive (`tar xzf`, then move
   `<name>-<version>` to `vendor/<name>`; drop `.cargo-ok` if present). Do not
   edit the sources; a change focal needs before upstream releases it is a
   separate, documented patch commit on top of the verbatim import.
3. Update the table above, `Cargo.lock`, `docs/dependencies/inventory.tsv`
   (`python3 scripts/release/notices.py --check`), and record the reason in
   `docs/archictecutre/09-implementation-status.md`.
4. Run the six gates; the dependency audit (`cargo deny`) covers the vendored
   crates' licenses through `Cargo.lock`.
