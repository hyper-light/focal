# Dependency review

The [inventory](inventory.tsv) records all 203 external packages from `cargo metadata --format-version 1 --locked --offline` on 2026-09-05, including build, development, and target-conditional dependencies. Every package declares license metadata. `Cargo.lock` is the reproducibility authority; refresh this inventory when dependencies change.

The full `cargo-deny` 0.20.2 check passed on 2026-09-05 against the fetched RustSec database and [repository policy](../../deny.toml), targeting arm64 macOS and x86-64 Linux: zero advisory, ban, license, or source errors. Ten duplicate-version warnings remain. No advisories are suppressed. Repeat with `cargo deny check advisories bans licenses sources`; this remains a blocking CI gate.

The Raft dependency is pinned to an explicitly reviewed upstream PR revision. It removes both the affected `protobuf` Rust runtime ([RUSTSEC-2024-0437](https://rustsec.org/advisories/RUSTSEC-2024-0437.html)) and unmaintained `fxhash` ([RUSTSEC-2025-0057](https://rustsec.org/advisories/RUSTSEC-2025-0057.html)) from the dependency graph. This is dependency removal, not an advisory exception or version relabeling. See [exact provenance, tradeoffs, and test evidence](raft-upstream.md) and [the codec boundary](../../crates/focal-consensus/SECURITY.md).

The initial deprecated `serde_yaml` adapter was replaced with deserializer-only `serde-saphyr` 1.2. Deployment configuration enforces input, depth, node/event, and scalar budgets and disables anchors/aliases. Tests exercise the parser used by the binary; [primary documentation](https://docs.rs/serde-saphyr/1.2.0/serde_saphyr/) describes its supported Serde surface.

The license policy checks declared permissive expressions and detected license text. The inventory is not a distribution notice bundle or a legal review. Release-specific notices and binary/source distribution qualification remain unfinished; no release certification is implied by passing this audit.
