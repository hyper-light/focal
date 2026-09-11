# Building and checking Focal

Normal installation uses one prebuilt `focal` executable for the server, CLI and
MCP server. Follow [binary installation](../README.md#install); running that binary
requires no Rust, protobuf compiler, Python or source checkout. The
[release workflow](../.github/workflows/release.yml) builds raw binaries for macOS
ARM64/x64 and Linux ARM64/x64 with GNU or static musl. A manual workflow run
produces downloadable CI artifacts; an intentional version tag publishes only
after every required platform and release check succeeds. No first tagged release
has been published by this work. See [release procedures and checksums](../scripts/release/README.md).

Building from source is optional, for contributors or a custom build. The
workspace pins Rust 1.94.1 and edition 2024. `Cargo.lock` records the resolved
dependency graph. Install Rust through rustup and a native protobuf compiler
(`brew install protobuf` on macOS; `apt-get install protobuf-compiler` on
Debian/Ubuntu), then run from the checkout:

```sh
bash scripts/cargo.sh build --release -p focal-node --bin focal --locked
./target/release/focal --version
./target/release/focal --help
```

That executable supplies the same server and client commands as a downloaded
binary. A container image is built from the same sources with
`docker build -f deploy/container/Dockerfile -t focal:0.1.0 .`: the release's pinned
Rust Alpine image compiles the static musl binary, which is copied alone into an
empty image (`FROM scratch`) with an unprivileged user; the image needs no shell, libc
or system certificates. The build was not executed where this was written (it needs
a container runtime and the crate registry). To check contributor changes:

```sh
bash scripts/cargo.sh test --workspace --locked
bash scripts/cargo.sh clippy --workspace --all-targets --locked -- -D warnings
bash scripts/check-production.sh
cargo fmt --all --check
python3 scripts/check-contracts.py
```

`scripts/cargo.sh` invokes the pinned Cargo workspace directly. The selected `protobuf-build` 0.15.1 respects explicit `PROTOC`, then a usable compiler on `PATH`; on Unix its fallback is a native host compiler built from the locked `protobuf-src` C++ sources. There is no Rosetta or architecture-specific executable fallback in Focal's wrapper. A native compiler remains a build convenience, not a deployed Focal configuration concept. See the [reviewed Raft revision and compiler provenance](dependencies/raft-upstream.md).

The general CI matrix requests Linux x86_64 and macOS arm64 checks; the release
workflow additionally requires all six native binary lanes. Configured jobs do
not prove those environments have passed. Local verification is on macOS arm64.
`cargo-deny` 0.20.2 passed advisories, bans, licenses, and sources on 2026-09-05,
with ten duplicate-version warnings and no advisory suppressions;
[dependency review](dependencies/README.md) records the scope. Windows requires a
real native transport and durable filesystem port. Complete release-matrix
qualification, Meta-scale performance, and multi-region deployment remain
unqualified. Track qualification in
[implementation status](archictecutre/09-implementation-status.md).

Deterministic tests keep fixed seeds beside the tested history. The pure domain reducer does not read wall time, randomness, the filesystem, or the network. The disk simulator distinguishes data synchronization from directory synchronization; actual WAL tests additionally exercise real files and fail-stop persistence points.
