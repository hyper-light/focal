# Building and checking Focal

The workspace pins Rust 1.94.1 and edition 2024. `Cargo.lock` records the resolved dependency graph. Install Rust through rustup and a native protobuf compiler (`brew install protobuf` on macOS; `apt-get install protobuf-compiler` on Debian/Ubuntu), then run:

```sh
bash scripts/cargo.sh test --workspace --locked
bash scripts/cargo.sh clippy --workspace --all-targets --locked -- -D warnings
bash scripts/check-production.sh
cargo fmt --all --check
python3 scripts/check-contracts.py
```

`scripts/cargo.sh` invokes the pinned Cargo workspace directly. The selected `protobuf-build` 0.15.1 respects explicit `PROTOC`, then a usable compiler on `PATH`; on Unix its fallback is a native host compiler built from the locked `protobuf-src` C++ sources. There is no Rosetta or architecture-specific executable fallback in Focal's wrapper. A native compiler remains a build convenience, not a deployed Focal configuration concept. See the [reviewed Raft revision and compiler provenance](dependencies/raft-upstream.md).

The CI matrix requests Linux x86_64 and macOS arm64 checks; configured jobs do not prove those environments have passed. Local verification is on macOS arm64. `cargo-deny` 0.20.2 passed advisories, bans, licenses, and sources on 2026-09-05, with ten duplicate-version warnings and no advisory suppressions; [dependency review](dependencies/README.md) records the scope. Windows support, release qualification, Meta-scale performance, and multi-region deployment remain unqualified. Track qualification in [implementation status](archictecutre/09-implementation-status.md).

Deterministic tests keep fixed seeds beside the tested history. The pure domain reducer does not read wall time, randomness, the filesystem, or the network. The disk simulator distinguishes data synchronization from directory synchronization; actual WAL tests additionally exercise real files and fail-stop persistence points.
