//! Packaging as rendered assets (doc 08 §3, §5; 24 §24): a systemd unit for
//! a supervised host and Kubernetes manifests for a namespace, both computed
//! from the requested configuration and a few infrastructure facts. The
//! renderer never solves policy and never touches a cluster: it names every
//! fact it lacks (`MissingInput`) instead of guessing it, and the files it
//! writes are the same bytes for the same inputs.
pub mod kubernetes;
pub mod systemd;
#[cfg(test)]
mod tests;

use crate::config::Settings;
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;

/// One rendered file, relative to the output directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenderedFile {
    pub name: String,
    pub content: String,
}
/// An infrastructure fact the renderer needs and was not given. The assets
/// are rendered with a stated placeholder where one is safe; the operator
/// supplies the fact and renders again, or edits the placeholder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "input", rename_all = "snake_case")]
pub enum MissingInput {
    /// No `--image`; the manifests name the local tag `focal:<version>`.
    Image { placeholder: String },
    /// No `--storage-class`; the claims take the cluster's default class.
    StorageClass,
    /// No `--secret`; the hosts mount `focal-invitations`, and the
    /// invitations still have to be issued by the founder and installed.
    InvitationSecret {
        secret: String,
        invitations: Vec<String>,
        script: String,
    },
    /// Zone survival needs `2f+1` zones named with `--zone`.
    Zones { needed: usize, given: usize },
    /// A systemd host advertises an address or name its peers reach.
    Advertise,
}
/// What a render produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenderedAssets {
    pub files: Vec<RenderedFile>,
    pub missing: Vec<MissingInput>,
    /// Facts the operator should know before applying: what is unqualified,
    /// what the probes mean, where the invitations come from.
    pub notes: Vec<String>,
}
impl RenderedAssets {
    fn push(&mut self, name: &str, content: String) {
        self.files.push(RenderedFile {
            name: name.to_owned(),
            content,
        });
    }
}

/// The node configuration a rendered asset ships: the requested policy and
/// the facts the package fixes (topology per set, addresses per host). The
/// text is the schema the node reads (`config/schema/deployment-v1.json`).
pub fn config_yaml(settings: &Settings, node: &ConfigNode<'_>) -> String {
    let mut out = String::from("version: 1\n");
    if node.listen.is_some()
        || node.advertise.is_some()
        || node.metrics_listen.is_some()
        || settings.node.max_tenants.is_some()
    {
        out.push_str("node:\n");
        if let Some(listen) = node.listen {
            let _ = writeln!(out, "  listen: {}", yaml_string(listen));
        }
        if let Some(advertise) = node.advertise {
            let _ = writeln!(out, "  advertise: {}", yaml_string(advertise));
        }
        if let Some(metrics) = node.metrics_listen {
            let _ = writeln!(out, "  metrics_listen: {}", yaml_string(metrics));
        }
        if let Some(max) = settings.node.max_tenants {
            let _ = writeln!(out, "  max_tenants: {max}");
        }
    }
    if node.region.is_some() || node.zone.is_some() {
        out.push_str("topology:\n");
        if let Some(region) = node.region {
            let _ = writeln!(out, "  region: {}", yaml_string(region));
        }
        if let Some(zone) = node.zone {
            let _ = writeln!(out, "  zone: {}", yaml_string(zone));
        }
    }
    let _ = writeln!(
        out,
        "durability:\n  survive: {}\n  max_failures: {}",
        super::survive_name(settings.durability.survive),
        settings.durability.max_failures
    );
    if !settings.placement.home_regions.is_empty() || !settings.placement.residency.is_empty() {
        out.push_str("placement:\n");
        if !settings.placement.home_regions.is_empty() {
            let _ = writeln!(
                out,
                "  home_regions: [{}]",
                list(&settings.placement.home_regions)
            );
        }
        if !settings.placement.residency.is_empty() {
            let _ = writeln!(
                out,
                "  residency: [{}]",
                list(&settings.placement.residency)
            );
        }
    }
    out
}
/// The per-node facts a configuration file fixes.
#[derive(Debug, Clone, Copy, Default)]
pub struct ConfigNode<'a> {
    pub listen: Option<&'a str>,
    pub advertise: Option<&'a str>,
    pub metrics_listen: Option<&'a str>,
    pub region: Option<&'a str>,
    pub zone: Option<&'a str>,
}
fn list(values: &[String]) -> String {
    let mut out = String::new();
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        out.push_str(&yaml_string(value));
    }
    out
}
/// A double-quoted YAML scalar: every value a package carries is quoted, so
/// a zone named `no` or a port-like `1e3` stays a string.
pub(crate) fn yaml_string(value: &str) -> String {
    let mut out = String::from("\"");
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
/// A DNS label for a Kubernetes object or a systemd unit: lowercase
/// alphanumerics and hyphens, at most 63 bytes, starting and ending with an
/// alphanumeric.
pub(crate) fn valid_label(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-')
}
