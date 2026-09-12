//! Deployment explanation, plans and their application (doc 08 §9).
use clap::Subcommand;
use focal_node::{
    cluster_admin::{ClusterAdmin, ClusterAdminError},
    config::Settings,
    deployment::{self, DeploymentError, plan::DeploymentPlan},
    placement::{self, NodeFacts},
};
use std::{
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Subcommand)]
pub(crate) enum DeploymentCommand {
    /// Check the desired guarantee against an explicit inventory of verified node facts.
    Explain {
        #[arg(long)]
        inventory: Option<PathBuf>,
    },
    /// Emit the machine-readable deployment schema.
    Schema,
    /// Compute a plan from the requested configuration and what this node
    /// observes; read-only, nothing is journaled or created.
    Plan {
        /// Write the plan artifact here (a new file; never overwritten).
        #[arg(long, required_unless_present = "dry_run", conflicts_with = "dry_run")]
        output: Option<PathBuf>,
        /// Print the plan without writing it.
        #[arg(long)]
        dry_run: bool,
    },
    /// Apply a plan on this node, resuming its journal; a stale plan is
    /// refused before any side effect.
    Apply {
        #[arg(long)]
        plan_file: PathBuf,
        /// Wait up to this many seconds for placement changes to complete.
        #[arg(long, default_value_t = 0)]
        wait: u64,
    },
    /// Progress of applied plans, re-checked against the directory.
    Status {
        /// One plan by its identity; every journaled plan otherwise.
        #[arg(long)]
        plan: Option<String>,
    },
    /// Render packaging for the requested configuration (--config FILE):
    /// files written under --output, facts the render lacks named in the
    /// result. Rendering touches no cluster and no node.
    Render {
        #[command(subcommand)]
        target: RenderTarget,
    },
}
#[derive(Subcommand)]
pub(crate) enum RenderTarget {
    /// A headless Service, a founder StatefulSet and a host StatefulSet per
    /// failure domain, disruption budgets, a ConfigMap per set and the
    /// invitation script (08 §5).
    Kubernetes {
        #[arg(long)]
        namespace: String,
        /// The directory the files are written into (created; existing
        /// files are never overwritten).
        #[arg(long)]
        output: PathBuf,
        /// The image every pod runs.
        #[arg(long)]
        image: Option<String>,
        #[arg(long)]
        storage_class: Option<String>,
        /// The secret holding one invitation per host pod.
        #[arg(long)]
        secret: Option<String>,
        /// A zone for zone survival; repeat for each, the founder takes the first.
        #[arg(long = "zone")]
        zones: Vec<String>,
        /// Nodes in total; default the fewest the durability needs.
        #[arg(long)]
        nodes: Option<u32>,
        /// Each pod's volume request.
        #[arg(long, default_value = "20Gi")]
        volume: String,
        /// The QUIC port every pod listens and advertises on.
        #[arg(long, default_value_t = 7443)]
        port: u16,
    },
    /// A hardened unit for one supervised host and its configuration (08 §3).
    Systemd {
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value = "/usr/local/bin/focal")]
        binary: String,
        #[arg(long, default_value = "focal")]
        user: String,
        /// The node's data directory on the host (`--data-dir` names this
        /// process's own).
        #[arg(long, default_value = "/var/lib/focal")]
        state_dir: String,
        #[arg(long, default_value = "/etc/focal/focal.yaml")]
        config_path: String,
        /// A host's invitation, redeemed at its first start.
        #[arg(long)]
        invite_file: Option<String>,
    },
}
/// What the top-level arguments contributed.
pub(crate) struct Inputs<'a> {
    pub config: Option<&'a Path>,
    pub data_dir_override: Option<PathBuf>,
}

pub(crate) fn run(
    runtime: &tokio::runtime::Runtime,
    settings: &Settings,
    inputs: Inputs<'_>,
    command: DeploymentCommand,
) -> crate::Result<()> {
    match command {
        DeploymentCommand::Schema => {
            writeln!(
                std::io::stdout().lock(),
                "{}",
                include_str!("../../../../config/schema/deployment-v1.json")
            )?;
            Ok(())
        }
        DeploymentCommand::Explain { inventory } => explain(runtime, settings, &inputs, inventory),
        DeploymentCommand::Plan { output, dry_run } => {
            if inputs.config.is_none() {
                return Err(DeploymentError::NoConfig.into());
            }
            plan(runtime, settings, output.as_deref(), dry_run)
        }
        DeploymentCommand::Apply { plan_file, wait } => apply(runtime, settings, &plan_file, wait),
        DeploymentCommand::Status { plan } => status(runtime, settings, plan.as_deref()),
        DeploymentCommand::Render { target } => {
            if inputs.config.is_none() {
                return Err(DeploymentError::NoConfig.into());
            }
            render(settings, target)
        }
    }
}

fn render(settings: &Settings, target: RenderTarget) -> crate::Result<()> {
    use focal_node::deployment::render::{kubernetes, systemd};
    let (kind, output, assets) = match target {
        RenderTarget::Kubernetes {
            namespace,
            output,
            image,
            storage_class,
            secret,
            zones,
            nodes,
            volume,
            port,
        } => (
            "kubernetes",
            output,
            kubernetes::render(
                settings,
                &kubernetes::KubernetesRequest {
                    namespace,
                    image,
                    storage_class,
                    secret,
                    zones,
                    nodes,
                    volume,
                    port,
                },
                env!("CARGO_PKG_VERSION"),
            )?,
        ),
        RenderTarget::Systemd {
            output,
            binary,
            user,
            state_dir,
            config_path,
            invite_file,
        } => (
            "systemd",
            output,
            systemd::render(
                settings,
                &systemd::SystemdRequest {
                    binary,
                    user,
                    data_dir: state_dir,
                    config_path,
                    invite_file,
                },
            )?,
        ),
    };
    std::fs::create_dir_all(&output)?;
    let mut written = Vec::new();
    for file in &assets.files {
        let path = output.join(&file.name);
        write_new(&path, file.content.as_bytes())?;
        written.push(path.display().to_string());
    }
    focal_platform::sync_dir(&output)?;
    crate::print_json(&serde_json::json!({
        "schema_version": 1,
        "result": {
            "kind": "deployment_render",
            "target": kind,
            "output": output.display().to_string(),
            "files": written,
            "missing": assets.missing,
            "notes": assets.notes,
        }
    }))
}

/// What a running node's directory reports, for `explain` (08 §9): the
/// nodes with their announced domains and standing, and every session with
/// the level it wants, the level it has and what blocks it.
fn observed_view(observation: &deployment::plan::Observation) -> serde_json::Value {
    let hex = |id: &[u8; 16]| {
        id.iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    serde_json::json!({
        "observed_at": observation.observed_at,
        "nodes": observation.nodes.iter().map(|node| serde_json::json!({
            "node": node.node,
            "alive": node.alive,
            "eligible": node.eligible,
            "region": node.region,
            "zone": node.zone,
        })).collect::<Vec<_>>(),
        "sessions": observation.sessions.iter().map(|session| serde_json::json!({
            "tenant": hex(&session.tenant),
            "session": hex(&session.session),
            "desired": deployment::plan::LevelView::of(session.desired),
            "achieved": session.achieved.map(deployment::plan::LevelView::of),
            "pending": session.pending.is_some(),
            "blocked_by": session.blocked_by,
        })).collect::<Vec<_>>(),
    })
}

fn explain(
    runtime: &tokio::runtime::Runtime,
    settings: &Settings,
    inputs: &Inputs<'_>,
    inventory: Option<PathBuf>,
) -> crate::Result<()> {
    // Without an explicit inventory, a running node that runs a directory
    // is explained against what the directory observes: every node it
    // knows, with its announced domains, and every session's achieved
    // level. A node that is not running, or runs no directory, is explained
    // against itself alone.
    let observation = match inventory {
        None if network(settings).unwrap_or(false) => {
            ClusterAdmin::open(settings).ok().and_then(|admin| {
                runtime
                    .block_on(deployment::observe::observe(&admin, true))
                    .ok()
            })
        }
        _ => None,
    };
    let nodes: Vec<NodeFacts> = match (&inventory, &observation) {
        (Some(path), _) => serde_json::from_slice(&crate::read_file(path, 1024 * 1024)?)?,
        (None, Some(observation)) => observation
            .nodes
            .iter()
            .map(|node| NodeFacts {
                id: node.node,
                topology: focal_node::config::Topology {
                    region: node.region.clone(),
                    zone: node.zone.clone(),
                },
                verified: true,
                eligible: node.eligible && node.alive,
            })
            .collect(),
        (None, None) => vec![NodeFacts {
            id: 1,
            topology: settings.topology.clone(),
            verified: true,
            eligible: true,
        }],
    };
    // Requested is the file; effective is the store's committed policy
    // when one exists; every value names its source (doc 08 §9).
    let (committed, sources) = {
        let root = settings.data_dir()?;
        let committed = focal_node::config::policy::read_committed(&root)
            .ok()
            .flatten();
        let file = inputs.config.and_then(|path| {
            let text = crate::read_file(path, 64 * 1024).ok()?;
            let text = std::str::from_utf8(&text).ok()?;
            Some((
                Settings::from_yaml(text).ok()?,
                focal_node::config::resolve::FilePresence::of(text).ok()?,
            ))
        });
        let sources = focal_node::config::resolve(
            &focal_node::config::CliOverrides {
                data_dir: inputs.data_dir_override.clone(),
                advertise: None,
                listen: None,
            },
            file.as_ref()
                .map(|(settings, presence)| (settings, presence)),
            committed.as_ref(),
        )
        .map(|resolved| resolved.sources)
        .unwrap_or_default();
        (committed, sources)
    };
    let effective = committed
        .as_ref()
        .map(|policy| policy.intent.clone())
        .unwrap_or_else(|| settings.policy_intent());
    let committed_revision = committed.as_ref().map(|policy| policy.revision.0);
    // The guarantee is active when every observed session has the
    // effective level; without an observation nothing is active.
    let effective_level = deployment::plan::level(&effective);
    let activated = observation.as_ref().is_some_and(|observation| {
        !observation.sessions.is_empty()
            && observation.sessions.iter().all(|session| {
                session
                    .achieved
                    .is_some_and(|achieved| achieved.covers(effective_level))
            })
    });
    let observed = observation.as_ref().map(observed_view);
    // The inventory is explicit, observed, or this node alone: a committed
    // policy that needs more hosts than the inventory holds is reported
    // unsatisfied against that inventory, still naming what is requested
    // and effective.
    match placement::plan(&nodes, &settings.durability, &settings.placement) {
        Ok(plan) => crate::print_json(
            &serde_json::json!({"condition":"PlanValid","activated":activated,"plan":plan,"requested":settings.policy_intent(),"effective":effective,"committed_revision":committed_revision,"sources":sources,"observed":observed}),
        ),
        Err(error) => {
            crate::print_json(
                &serde_json::json!({"condition":"GuaranteeUnsatisfied","activated":activated,"reason":error.to_string(),"requested":settings.policy_intent(),"effective":effective,"committed_revision":committed_revision,"sources":sources,"observed":observed}),
            )?;
            Err(error.into())
        }
    }
}

fn network(settings: &Settings) -> crate::Result<bool> {
    Ok(focal_node::network_state::network_requested(
        &settings.data_dir()?,
        settings,
    ))
}

fn plan(
    runtime: &tokio::runtime::Runtime,
    settings: &Settings,
    output: Option<&Path>,
    dry_run: bool,
) -> crate::Result<()> {
    let network = network(settings)?;
    let admin = ClusterAdmin::open(settings)?;
    let requested = settings.policy_intent();
    // Without a directory the node's own facts must satisfy the request:
    // the same rule the first start applies before pinning a policy.
    if !network {
        placement::plan(
            &[NodeFacts {
                id: admin.identity().node,
                topology: settings.topology.clone(),
                verified: true,
                eligible: true,
            }],
            &requested.durability,
            &requested.placement,
        )
        .map_err(DeploymentError::Unsatisfiable)?;
    }
    let plan = runtime.block_on(async {
        let observation = deployment::observe::observe(&admin, network).await?;
        let mut proposals = Vec::new();
        proposals
            .try_reserve_exact(observation.sessions.len())
            .map_err(|_| DeploymentError::Capacity)?;
        let survive = deployment::survive_code(requested.durability.survive);
        for session in &observation.sessions {
            // Dry runs: the agent proposes and journals nothing.
            let proposal = match admin
                .plan_session_reply(
                    session.tenant,
                    session.session,
                    survive,
                    requested.durability.max_failures,
                    true,
                )
                .await
            {
                Ok(reply) => match reply.state {
                    0 => deployment::plan::Proposal::Planned {
                        operation: reply.operation,
                        voters: reply.voters,
                    },
                    1 => deployment::plan::Proposal::Pending {
                        operation: reply.operation,
                        voters: reply.voters,
                    },
                    _ => deployment::plan::Proposal::Satisfied,
                },
                Err(ClusterAdminError::Access(focal_wire::AccessError::InvalidRequest)) => {
                    deployment::plan::Proposal::Refused(
                        "no placement satisfies the requested durability with the enrolled, live nodes (or another request for this session is in flight)".into(),
                    )
                }
                Err(error) => return Err(DeploymentError::Admin(error)),
            };
            proposals.push(proposal);
        }
        deployment::plan::compose(&observation, &requested, &proposals, deployment::now_ms()?)
    })?;
    if let Some(output) = output {
        write_new(output, &plan.encode()?)?;
    }
    crate::print_json(&serde_json::json!({
        "schema_version": 1,
        "result": {
            "plan": plan.view(),
            "dry_run": dry_run,
            "output": output.map(|path| path.display().to_string()),
            "empty": plan.is_empty(),
        }
    }))
}
/// Plans are immutable: the output is a new file, synced before return.
fn write_new(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}
fn read_plan(path: &Path) -> crate::Result<DeploymentPlan> {
    let bytes = crate::read_file(path, deployment::plan::MAX_PLAN_BYTES)?;
    Ok(DeploymentPlan::decode(&bytes)?)
}

fn apply(
    runtime: &tokio::runtime::Runtime,
    settings: &Settings,
    plan_file: &Path,
    wait: u64,
) -> crate::Result<()> {
    let plan = read_plan(plan_file)?;
    let network = network(settings)?;
    let admin = ClusterAdmin::open(settings)?;
    let report = runtime.block_on(deployment::apply::apply(
        &admin,
        network,
        &plan,
        Duration::from_secs(wait),
    ))?;
    crate::print_json(&serde_json::json!({"schema_version": 1, "result": report}))
}

fn status(
    runtime: &tokio::runtime::Runtime,
    settings: &Settings,
    plan: Option<&str>,
) -> crate::Result<()> {
    let plan_id = match plan {
        Some(text) => Some(
            focal_client::input::parse_id(text)
                .map_err(|_| DeploymentError::Corrupt("plan identity is not 32 hex digits"))?,
        ),
        None => None,
    };
    let network = network(settings)?;
    let admin = ClusterAdmin::open(settings)?;
    let reports = runtime.block_on(deployment::apply::status(&admin, network, plan_id))?;
    crate::print_json(&serde_json::json!({
        "schema_version": 1,
        "result": {"kind": "deployment_status", "plans": reports}
    }))
}
