#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! What in focal's dependencies can decompress, and what reaches it (the adversarial battery's
//! DB-2), read from the workspace's resolved graph (`cargo metadata`, offline and locked). focal's
//! own codecs expand nothing: they refuse a declared length before allocating (the size bombs). The
//! one decompressor focal's binary links is the TLS stack's: `hyper-tls` decompresses certificate
//! chains (RFC 8879) with brotli and zlib, and every configuration focal's endpoints hold takes no
//! certificate compression (`no_tls_configuration_focal_holds_decompresses_a_certificate`), so no
//! peer's bytes reach either. zstd is locked for the comparison harness (`focal-compare`) and as a
//! dev-dependency of the bus's codec benchmarks, never in what focal ships. A decompressor that
//! comes in any other way fails this test until the threat model says what reaches it.
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Crates that decompress: the codecs a Rust dependency can pull in.
const CODECS: &[&str] = &[
    "async-compression",
    "brotli",
    "brotli-decompressor",
    "bzip2",
    "bzip2-sys",
    "deflate",
    "fdeflate",
    "flate2",
    "inflate",
    "libz-ng-sys",
    "libz-rs-sys",
    "libz-sys",
    "lz4",
    "lz4-sys",
    "lz4_flex",
    "lzma-rs",
    "lzma-sys",
    "miniz_oxide",
    "ruzstd",
    "snap",
    "weezl",
    "xz2",
    "zip",
    "zlib-rs",
    "zstd",
    "zstd-safe",
    "zstd-sys",
    "zune-inflate",
];

/// The workspace's resolved graph: package id -> (name, the ids it depends on as a library or a
/// build script, never as a dev-dependency), and its members.
struct Graph {
    names: BTreeMap<String, String>,
    shipped: BTreeMap<String, Vec<String>>,
    members: Vec<String>,
}
fn graph() -> Graph {
    let output = std::process::Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--format-version",
            "1",
            "--locked",
            "--offline",
            "--manifest-path",
        ])
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.toml"))
        .output()
        .expect("cargo metadata runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: Value = serde_json::from_slice(&output.stdout).unwrap();
    let names = metadata["packages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|package| {
            (
                package["id"].as_str().unwrap().to_owned(),
                package["name"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    let shipped = metadata["resolve"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|node| {
            let deps = node["deps"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|dep| {
                    dep["dep_kinds"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|kind| kind["kind"].is_null() || kind["kind"] == "build")
                })
                .map(|dep| dep["pkg"].as_str().unwrap().to_owned())
                .collect();
            (node["id"].as_str().unwrap().to_owned(), deps)
        })
        .collect();
    let members = metadata["workspace_members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap().to_owned())
        .collect();
    Graph {
        names,
        shipped,
        members,
    }
}
impl Graph {
    fn id(&self, name: &str) -> String {
        self.names
            .iter()
            .find(|(_, of)| of.as_str() == name)
            .map(|(id, _)| id.clone())
            .unwrap_or_else(|| panic!("no package {name}"))
    }
    /// The decompressors `root` builds into what it ships, not passing through `gate`.
    fn codecs_of(&self, root: &str, gate: Option<&str>) -> BTreeSet<String> {
        let mut seen = BTreeSet::new();
        let mut stack = vec![root.to_owned()];
        while let Some(id) = stack.pop() {
            if gate == Some(self.names[&id].as_str()) || !seen.insert(id.clone()) {
                continue;
            }
            stack.extend(self.shipped[&id].iter().cloned());
        }
        seen.iter()
            .map(|id| self.names[id].clone())
            .filter(|name| CODECS.contains(&name.as_str()))
            .collect()
    }
}

#[test]
fn only_the_tls_stack_reaches_a_decompressor_in_what_focal_ships() {
    let graph = graph();
    let locked: BTreeSet<&str> = graph
        .names
        .values()
        .map(String::as_str)
        .filter(|name| CODECS.contains(name))
        .collect();
    assert_eq!(
        locked,
        BTreeSet::from([
            "brotli",
            "brotli-decompressor",
            "zlib-rs",
            "zstd",
            "zstd-safe",
            "zstd-sys"
        ]),
        "the decompressors locked are the ones the threat model states"
    );
    let node = graph.id("focal-node");
    let tls = BTreeSet::from(["brotli", "brotli-decompressor", "zlib-rs"].map(str::to_owned));
    assert_eq!(
        graph.codecs_of(&node, None),
        tls,
        "the binary links the TLS stack's"
    );
    assert_eq!(
        graph.codecs_of(&node, Some("hyper-tls")),
        BTreeSet::new(),
        "and none but through it"
    );
    for member in &graph.members {
        if graph.names[member] == "focal-compare" {
            continue;
        }
        assert_eq!(
            graph.codecs_of(member, Some("hyper-tls")),
            BTreeSet::new(),
            "{} ships a decompressor outside the TLS stack",
            graph.names[member]
        );
    }
}
