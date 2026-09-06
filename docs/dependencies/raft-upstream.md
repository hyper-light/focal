# Raft upstream revision

Focal pins `raft` and `raft-proto` to commit [`8e4cef172421bf77b2ae1c26628a9531b0be41f0`](https://github.com/tikv/raft-rs/commit/8e4cef172421bf77b2ae1c26628a9531b0be41f0), fetched through the official `tikv/raft-rs` repository. This is the reviewed head of upstream [PR #578](https://github.com/tikv/raft-rs/pull/578), not a published release or merged upstream commitment. It is immutable in the manifest and lockfile; source policy requires an explicit revision and allows only this Git repository.

The PR was open and unmerged when reviewed on 2026-09-05. Its contributor branch originates in `ggirol-rc/raft-rs`. The [complete diff against base `aafb07c7bab439c6139926a77dfafc5b10e9bc84`](https://github.com/tikv/raft-rs/compare/aafb07c7bab439c6139926a77dfafc5b10e9bc84...8e4cef172421bf77b2ae1c26628a9531b0be41f0) changes six files, adding 30 lines and deleting 22. Production changes are the dependency replacement `fxhash` to `rustc-hash` and the corresponding `DefaultHashBuilder` alias. Other changes update the harness dependency and make tests/configuration test formatting independent of hash iteration order. No Raft transition, log, quorum, or wire schema code changes in that PR diff.

The PR base already includes upstream native `protocompat` support and optional protobuf features. Focal selects only `prost-codec`; the vulnerable Rust `protobuf` runtime is absent from the resolved graph. The adapter uses upstream `PbMessage`/`PbMessageExt`, with bounded Prost decoding for peer messages and persisted records. Existing protobuf-format disk bytes remain readable.

Latest published release at review was v0.7.0 (2023-03-07). Latest merged master was `ad13f3d90780f53aea2488c6a4b76c0d334bf136`. The chosen PR is two merged changes behind that master: the `rand` 0.9.3 update (#586) and unstable-entry buffer shrinking (#589). Neither was present in Focal's previous published v0.7.0 baseline. These absent improvements and the unmerged status are reasons to reassess the pin when a compatible merged revision or release removes both dependencies. Do not change this to branch tracking. Review the full replacement diff, repeat compatibility tests, and refresh the audit before updating.

`protobuf-build` 0.15.1 uses `protobuf-src` as a build-only C++ compiler source dependency on Unix. This is distinct from the removed Rust parser. Its native host compiler fallback replaces the previous architecture-specific bundled compiler workaround; explicit `PROTOC` is still honored. Compiler sources and their licenses are included in the inventory.

Validation on 2026-09-05:

- `cargo test -p focal-consensus -p focal-log --offline`: 12 consensus and 4 WAL tests passed, including fixed old protobuf bytes, actual old snapshot/HardState WAL recovery, malformed unknown groups, partitions, learner membership, and durability failures.
- Exact upstream source unit tests with `--no-default-features --features prost-codec --lib -p raft -p raft-proto`: 47 Raft tests passed. The isolated test copy changed only build profiles; its independent development lockfile was not copied into Focal.
- `cargo test -p focal-node --test fleet_quic --offline`: real authenticated three-replica QUIC quorum, leader loss, exact retry, and disk restart passed.
- `cargo clippy -p focal-consensus -p focal-log -p focal-node --all-targets --offline -- -D warnings`: passed.
- `cargo tree -i protobuf --offline` and `cargo tree -i fxhash --offline`: neither package exists in the workspace graph.
- `cargo-deny` 0.20.2 advisories, bans, licenses, and sources: passed with ten duplicate-version warnings, no advisory exclusions.
