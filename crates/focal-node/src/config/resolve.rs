//! Configuration precedence, made explicit (doc 08 §2): command-line
//! overrides apply only to eligible node-local startup fields, then the
//! supplied file, then creation defaults; committed policy is never a
//! last-writer-wins startup option. Every value names its source.
use super::{CommittedPolicy, ConfigError, Settings};
use serde::Serialize;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

/// Where a resolved value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigSource {
    CommandLine,
    File,
    CreationDefault,
    /// The store's committed policy at this revision.
    Committed(u64),
}
/// The source of every resolved field, by its configuration path.
pub type FieldSources = BTreeMap<&'static str, ConfigSource>;

/// The startup overrides the command line may supply: node-local fields only.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CliOverrides {
    pub data_dir: Option<PathBuf>,
    pub advertise: Option<String>,
    pub listen: Option<SocketAddr>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSettings {
    pub settings: Settings,
    pub sources: FieldSources,
    /// The committed policy the policy fields were checked against, if any.
    pub committed: Option<CommittedPolicy>,
}

/// Which top-level and nested keys the file set, so an omitted field keeps
/// its committed value instead of resetting to a creation default.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilePresence {
    keys: Vec<String>,
}
impl FilePresence {
    /// The dotted paths present in `yaml`, at most two levels deep.
    pub fn of(yaml: &str) -> Result<Self, ConfigError> {
        let options = serde_saphyr::options! {
            budget: serde_saphyr::budget! {max_depth:16,max_events:8192,max_nodes:4096,max_total_scalar_bytes:64*1024,max_aliases:0,max_anchors:0,max_documents:1},
        };
        let value: serde_json::Value = serde_saphyr::from_str_with_options(yaml, options)?;
        let mut keys = Vec::new();
        if let Some(object) = value.as_object() {
            for (key, inner) in object {
                keys.push(key.clone());
                if let Some(nested) = inner.as_object() {
                    for name in nested.keys() {
                        keys.push(format!("{key}.{name}"));
                    }
                }
            }
        }
        Ok(Self { keys })
    }
    pub fn has(&self, path: &str) -> bool {
        self.keys.iter().any(|key| key == path)
    }
}

const POLICY_FIELDS: [&str; 4] = [
    "durability.survive",
    "durability.max_failures",
    "placement.home_regions",
    "placement.residency",
];
const LOCAL_FIELDS: [&str; 8] = [
    "node.data_dir",
    "node.listen",
    "node.advertise",
    "node.seeds",
    "node.max_tenants",
    "node.metrics_listen",
    "topology.zone",
    "topology.region",
];

/// Resolve the settings a startup runs with. `file` is the parsed
/// configuration file with `presence` naming the keys it set; `committed`
/// is the store's policy when the store exists. A policy field the file
/// sets to a value other than the committed one is refused by name; one
/// the file omits takes the committed value.
pub fn resolve(
    cli: &CliOverrides,
    file: Option<(&Settings, &FilePresence)>,
    committed: Option<&CommittedPolicy>,
) -> Result<ResolvedSettings, ConfigError> {
    resolve_with(cli, file, committed, true)
}
/// Resolve a policy request (`deployment plan`, `explain`): the file's
/// policy fields are what the operator asks for, so a value other than the
/// committed one is the request, not a refusal; omitted fields still take
/// the committed values.
pub fn resolve_request(
    cli: &CliOverrides,
    file: Option<(&Settings, &FilePresence)>,
    committed: Option<&CommittedPolicy>,
) -> Result<ResolvedSettings, ConfigError> {
    resolve_with(cli, file, committed, false)
}
fn resolve_with(
    cli: &CliOverrides,
    file: Option<(&Settings, &FilePresence)>,
    committed: Option<&CommittedPolicy>,
    enforce: bool,
) -> Result<ResolvedSettings, ConfigError> {
    let mut settings = file.map_or_else(Settings::default, |(settings, _)| settings.clone());
    let mut sources = FieldSources::new();
    for field in LOCAL_FIELDS.iter().chain(POLICY_FIELDS.iter()) {
        let set = file.is_some_and(|(_, presence)| presence.has(field));
        sources.insert(
            field,
            if set {
                ConfigSource::File
            } else {
                ConfigSource::CreationDefault
            },
        );
    }
    if let Some(committed) = committed {
        let requested = settings.policy_intent();
        let intent = &committed.intent;
        for (field, set) in POLICY_FIELDS
            .map(|field| (field, file.is_some_and(|(_, presence)| presence.has(field))))
        {
            let differs = match field {
                "durability.survive" => requested.durability.survive != intent.durability.survive,
                "durability.max_failures" => {
                    requested.durability.max_failures != intent.durability.max_failures
                }
                "placement.home_regions" => {
                    requested.placement.home_regions != intent.placement.home_regions
                }
                _ => requested.placement.residency != intent.placement.residency,
            };
            if set && differs && enforce {
                return Err(ConfigError::CommittedPolicyChange { field });
            }
            if !set {
                sources.insert(field, ConfigSource::Committed(committed.revision.0));
                match field {
                    "durability.survive" => settings.durability.survive = intent.durability.survive,
                    "durability.max_failures" => {
                        settings.durability.max_failures = intent.durability.max_failures;
                    }
                    "placement.home_regions" => {
                        settings.placement.home_regions = intent.placement.home_regions.clone();
                    }
                    _ => settings.placement.residency = intent.placement.residency.clone(),
                }
            }
        }
    }
    if let Some(data_dir) = &cli.data_dir {
        settings.node.data_dir = Some(data_dir.clone());
        sources.insert("node.data_dir", ConfigSource::CommandLine);
    }
    if let Some(advertise) = &cli.advertise {
        settings.node.advertise = Some(advertise.clone());
        sources.insert("node.advertise", ConfigSource::CommandLine);
    }
    if let Some(listen) = cli.listen {
        settings.node.listen = Some(listen);
        sources.insert("node.listen", ConfigSource::CommandLine);
    }
    settings.validate()?;
    Ok(ResolvedSettings {
        settings,
        sources,
        committed: committed.cloned(),
    })
}
