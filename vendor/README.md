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

## Shared crates

The `hyper-*` crates are not crates.io archives. They come from the repository focal shares with
mantle and slates (<https://github.com/hyper-light/hyper-raft>), taken from it whole at one
revision, which `SNAPSHOT` in each names:
- focal's consensus core and its election law, which moved there
  ([27](../docs/archictecutre/27-consensus-roadmap-and-slates-port.md) §14);
- the durable shell around the core, with the log, the block layer and the liveness stream it
  builds on (27 §15). Each is that revision's `crates/<name>/src`, `ORIGIN.md` (and `README.md`) and the
repository's `LICENSE`, unchanged, with one file of focal's: a `Cargo.toml` of its own that states
what the shared repository's workspace gave it (version, edition, dependencies) and makes it a
workspace root of its own, outside focal's, and that sets `rust-version` to focal's toolchain, which
the snapshot builds on. Its suites, benchmarks and lint wall run in the shared repository on all six
targets; a change to it is made there first and taken here by a new snapshot, never edited here.

| Crate | Version | Revision | Taken | Why it is here |
|---|---|---|---|---|
| `hyper-block` | 0.1.0 | `f311ab65979e8442777b0b69ff6bd7b5dfe7d64b` | 2026-10-08 | Block I/O for the log: aligned direct I/O, each platform's full flush, group commit's wait |
| `hyper-durable` | 0.1.0 | `f311ab65979e8442777b0b69ff6bd7b5dfe7d64b` | 2026-10-08 | The durable shell a group's replica becomes (27 §15) |
| `hyper-liveness` | 0.1.0 | `f311ab65979e8442777b0b69ff6bd7b5dfe7d64b` | 2026-10-08 | The node-pair liveness stream, which the shell's owner wires once focal elects by suspicion |
| `hyper-log` | 0.1.0 | `f311ab65979e8442777b0b69ff6bd7b5dfe7d64b` | 2026-10-08 | The log every group of a data directory writes, one flush for all of them (27 §15.3) |
| `hyper-raft` | 0.1.0 | `f311ab65979e8442777b0b69ff6bd7b5dfe7d64b` | 2026-10-08 | The consensus core every group runs |
| `hyper-timing` | 0.1.0 | `f311ab65979e8442777b0b69ff6bd7b5dfe7d64b` | 2026-10-08 | The election law's draw the core takes its delays from |
| `hyper-seal` | 0.1.0 | `f311ab65979e8442777b0b69ff6bd7b5dfe7d64b` | 2026-10-08 | Sealing at rest: the log's per-session keys, MACs and tags (hyper-log's sealed log), whole files sealed by STREAM (`sealed_file`), keys wrapped by AES-256-KW and sent to another machine by ML-KEM-1024 |

A new snapshot: `git archive <revision> crates/<name>/src crates/<name>/ORIGIN.md
crates/<name>/README.md LICENSE` from the shared repository into `vendor/<name>`, `SNAPSHOT` set to
`<name> <revision> crates/<name>`, the manifest kept, and the table above, `Cargo.lock` and
`docs/dependencies/inventory.tsv` (the revision is the source it lists) brought up to date.

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
   (`python3 scripts/release/notices.py <scratch directory>`, which refuses a stale roster), and record the reason in
   `docs/archictecutre/09-implementation-status.md`.
4. Run the six gates; the dependency audit (`cargo deny`) covers the vendored
   crates' licenses through `Cargo.lock`.
