//! Local root and installed-application administration through the physical owner.
use clap::Subcommand;
use focal_control::MembershipChange;
use focal_node::{
    cluster_admin::{ClusterAdmin, ClusterAdminError},
    network_admin::AdminRead,
};
use std::path::PathBuf;

#[derive(Subcommand)]
pub(crate) enum ClusterCommand {
    /// Read this physical owner's identity, local health or listener configuration.
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
    /// Administer already installed application replicas; this does not assign placement.
    Replicas {
        #[command(subcommand)]
        command: ReplicaCommand,
    },
    /// Write a private one-node invitation; retrying the same name is exact.
    Invite {
        #[arg(long)]
        node: String,
        #[arg(long)]
        output: PathBuf,
    },
    /// Quorum-read this node's root-group leader and voting membership.
    Status,
    /// Every directory partition this node acts on: nodes, sessions, their
    /// desired and achieved guarantee and what blocks it.
    Placement,
    /// The bounded next actions the placement controller would take.
    Plan,
    /// Admit or list the tenants the cluster serves.
    Tenants {
        #[command(subcommand)]
        command: TenantCommand,
    },
    /// Create application sessions on this node.
    Sessions {
        #[command(subcommand)]
        command: SessionCommand,
    },
    /// Inspect committed contact announcements, not placement or credential grants.
    Nodes {
        #[command(subcommand)]
        command: NodesCommand,
    },
    /// Inspect redacted committed invitation metadata or revoke admission.
    Invitations {
        #[command(subcommand)]
        command: InvitationCommand,
    },
    /// Inspect or revoke the credential issued by an invitation.
    Credentials {
        #[command(subcommand)]
        command: CredentialCommand,
    },
    /// Invite an authenticated client principal, without any physical node role.
    Client {
        #[command(subcommand)]
        command: ClientCommand,
    },
    /// Inspect or change the local root metadata group's configuration.
    Membership {
        #[command(subcommand)]
        command: MembershipCommand,
    },
    /// Initiate transfer; success does not mean the target has become leader.
    Leader {
        #[command(subcommand)]
        command: LeaderCommand,
    },
    /// Inspect or retry the exact latest durable local admin intent.
    Request {
        #[command(subcommand)]
        command: RequestCommand,
    },
    /// A native session's retention floor and archive counts.
    Retention {
        #[command(subcommand)]
        command: RetentionCommand,
    },
    /// A retired claim's archive bundle as this node holds and verifies it.
    Archive {
        #[command(subcommand)]
        command: ArchiveCommand,
    },
    /// The collector that reclaims unreferenced bytes on this node.
    Gc {
        #[command(subcommand)]
        command: GcCommand,
    },
    /// Coherent backups of a hosted session at a declared prefix.
    Backup {
        #[command(subcommand)]
        command: BackupCommand,
    },
    /// The storage view of this node: the volume envelope, what is staged,
    /// the agents, and every hosted session's oldest retained prefix.
    Storage {
        #[command(subcommand)]
        command: StorageCommand,
    },
    /// Restore a session from a verified backup onto this node. The old
    /// incarnation continues only when its source is fenced; otherwise
    /// `--new-incarnation` acknowledges a recovery under a new log group.
    Restore {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        new_incarnation: bool,
    },
    /// Repair a hosted session's custody on this node: re-verify every
    /// object its committed prefix names, recopy what is missing from
    /// another required copy, and complete the other required copies. An
    /// object no copy can supply is reported as unrecoverable; the session
    /// then needs a restore.
    Repair {
        /// The session's tenant; this node's own tenant when omitted.
        #[arg(long, requires = "session")]
        tenant: Option<String>,
        /// The session; this node's original session when omitted.
        #[arg(long)]
        session: Option<String>,
        /// Resume a bounded walk after this artifact (`next_after` of the
        /// previous report).
        #[arg(long)]
        after: Option<String>,
        /// The objects one call examines at most (1–4096).
        #[arg(long, default_value_t = 256)]
        limit: u32,
    },
    /// The upgrade fence: the capability level the cluster is held to, this
    /// binary's level and every node's reported level; raise it once every
    /// node runs a binary at the new level.
    Upgrade {
        #[command(subcommand)]
        command: UpgradeCommand,
    },
}
#[derive(Subcommand)]
pub(crate) enum UpgradeCommand {
    /// The committed fence, this binary's level and every node's reported
    /// capability.
    Status,
    /// Raise the fence to a level (founder only): refused by name
    /// (`members_behind`) while any node reports a lower capability or
    /// none; a fence already at the level reads as done; a fence never
    /// lowers. Afterwards a binary announcing less refuses to start.
    Activate {
        #[arg(long)]
        fence: u32,
    },
}
#[derive(Subcommand)]
pub(crate) enum StorageCommand {
    /// Disk pressure by kind, staged uploads, the archive agent's and the
    /// collector's settings and progress, and each hosted session's
    /// retention floor; no reclamation is initiated.
    Show,
}
#[derive(Subcommand)]
pub(crate) enum BackupCommand {
    /// Write a backup of a hosted native session into a new directory on
    /// this node: its durable envelope, seed chunks, every object its prefix
    /// names, and a manifest written last.
    Create {
        /// The session's tenant; this node's own tenant when omitted.
        #[arg(long, requires = "session")]
        tenant: Option<String>,
        /// The session; this node's original session when omitted.
        #[arg(long)]
        session: Option<String>,
        #[arg(long)]
        output: PathBuf,
    },
    /// Verify a backup directory without writing; needs no running node.
    Verify {
        #[arg(long)]
        input: PathBuf,
    },
}
#[derive(Subcommand)]
pub(crate) enum GcCommand {
    /// The collector's settings, whether a pass is in progress, how many
    /// completed, and the last pass's report.
    Show,
    /// Bring a quarantined content object back by its domain and root, while
    /// its quarantine round has not expired.
    Restore {
        #[arg(long)]
        domain: String,
        #[arg(long)]
        root: String,
    },
}
#[derive(Subcommand)]
pub(crate) enum RetentionCommand {
    /// The published prefix, what registered consumers still need, what the
    /// archive reports holding, the floor and what holds it there, and the
    /// families retired; no reclamation is initiated.
    Show {
        #[arg(long)]
        session: Option<String>,
    },
}
#[derive(Subcommand)]
pub(crate) enum ArchiveCommand {
    /// The continuation a retired claim left in the core and its bundle as
    /// this node holds it: verified structurally, with the copies holding a
    /// receipt.
    Show {
        #[arg(long)]
        session: Option<String>,
        #[arg(long)]
        claim: String,
    },
}
#[derive(Subcommand)]
pub(crate) enum NodeCommand {
    Identity,
    Health,
    Config,
    /// The four readiness probes with the facts they derive from.
    Readiness,
    /// The node's metrics as Prometheus text: memory and volume envelopes,
    /// WAL, replicas and their lags, peers, liveness, credential expiry,
    /// placement and the upgrade fence, sampled every five seconds.
    Metrics,
    /// One readiness probe for a supervisor: exit 0 when it holds, 1
    /// (`probe_failed`) otherwise.
    Probe {
        #[arg(long, value_parser = ["alive", "catching-up", "authoritative", "policy"])]
        check: String,
    },
}
#[derive(Subcommand)]
pub(crate) enum NodesCommand {
    List,
    /// Stop placing sessions on a node: its grant is re-issued ineligible,
    /// the controller heals every placement that named it and retires its
    /// copies. The founder is never drained.
    Drain {
        #[arg(long)]
        node: u64,
    },
    /// Consider a drained node for placement again.
    Undrain {
        #[arg(long)]
        node: u64,
    },
    /// Remove a drained node that no session names any more: its root-group
    /// membership, then the credential its invitation issued.
    Remove {
        #[arg(long)]
        node: u64,
    },
    /// Drain a node once its replacement is enrolled, alive, eligible and
    /// reporting; the planner chooses among every eligible node.
    Replace {
        #[arg(long)]
        node: u64,
        #[arg(long = "with")]
        replacement: u64,
    },
}
#[derive(Subcommand)]
pub(crate) enum TenantCommand {
    /// Admit a tenant through the founder's enrollment authority; retrying an
    /// admitted tenant reads as done.
    Admit {
        #[arg(long)]
        tenant: String,
    },
    /// The founder's tenant and every admitted one.
    List,
}
#[derive(Subcommand)]
pub(crate) enum SessionCommand {
    /// Create a session for a served tenant on this node; the same name is
    /// the same session.
    Create {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        name: String,
    },
    /// Plan a session's placement under a durability (survive node, zone or
    /// region with up to N failures); the controller executes it unattended.
    Plan {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        session: String,
        #[arg(long, default_value = "node")]
        survive: String,
        #[arg(long)]
        max_failures: u16,
        /// Report the plan the request denotes without journaling it.
        #[arg(long)]
        dry_run: bool,
    },
}
#[derive(Subcommand)]
pub(crate) enum ClientCommand {
    Invite {
        #[arg(long)]
        name: String,
        #[arg(long)]
        output: PathBuf,
    },
}
#[derive(Subcommand)]
pub(crate) enum InvitationCommand {
    List {
        #[arg(long)]
        after: Option<String>,
        #[arg(long, default_value_t = 32)]
        limit: u16,
        #[arg(long)]
        expected_revision: Option<u64>,
    },
    Get {
        id: String,
    },
    /// Also revokes any credential issued through this invitation; does not drain placement.
    Revoke {
        id: String,
        #[arg(long)]
        expected_revision: Option<u64>,
    },
}
#[derive(Subcommand)]
pub(crate) enum CredentialCommand {
    Get {
        #[arg(long)]
        invitation: String,
    },
    /// Disables the issuing invitation and credential; consensus membership is unchanged.
    Revoke {
        #[arg(long)]
        invitation: String,
        #[arg(long)]
        expected_revision: Option<u64>,
    },
    /// Renew this node's own credential now: the same key under a fresh
    /// certificate and lifetime, presented on every path at once.
    Renew,
    /// Rotate this node's own credential to a fresh key under the same
    /// identity; the previous certificate authorizes through the grace.
    Rotate,
}
#[derive(Subcommand)]
pub(crate) enum MembershipCommand {
    Show,
    AddLearner {
        #[arg(long)]
        node: u64,
        #[arg(long)]
        expected_configuration_index: Option<u64>,
    },
    Promote {
        #[arg(long)]
        node: u64,
        #[arg(long)]
        expected_configuration_index: Option<u64>,
    },
    Remove {
        #[arg(long)]
        node: u64,
        #[arg(long)]
        expected_configuration_index: Option<u64>,
    },
    LeaveJoint {
        #[arg(long)]
        expected_configuration_index: Option<u64>,
    },
}
#[derive(Subcommand)]
pub(crate) enum LeaderCommand {
    Transfer {
        #[arg(long)]
        node: u64,
        #[arg(long)]
        expected_configuration_index: Option<u64>,
    },
}
#[derive(Subcommand)]
pub(crate) enum RequestCommand {
    Inspect,
    Retry { operation_id: String },
    Reconcile { operation_id: String },
}
#[derive(Subcommand)]
pub(crate) enum ReplicaCommand {
    /// Observe local persisted prefix and decoder floor without starting an upgrade.
    Diagnostics {
        #[arg(long)]
        session: Option<String>,
    },
    /// Initiate transfer under an exact application configuration fence.
    Transfer {
        #[arg(long)]
        session: Option<String>,
        #[arg(long)]
        node: u64,
        #[arg(long)]
        expected_configuration_index: Option<u64>,
    },
    /// Propose committed activation of native history on an empty ledger;
    /// every voter must already promise the native decoder.
    ActivateNative {
        #[arg(long)]
        session: Option<String>,
    },
    /// Checkpoint one installed replica's applied prefix now and compact its
    /// log; a native Core root beyond the inline bound is sealed as seeds.
    Checkpoint {
        #[arg(long)]
        session: Option<String>,
    },
    /// Observe a bounded live page of this physical node's installed application replicas.
    List {
        #[arg(long)]
        after: Option<String>,
        #[arg(long, default_value_t = 32)]
        limit: u16,
    },
    /// Quorum-read one installed application's exact committed configuration.
    Show {
        #[arg(long)]
        session: Option<String>,
    },
    Membership {
        #[arg(long)]
        session: Option<String>,
        #[command(subcommand)]
        command: MembershipCommand,
    },
    /// The members of a native session's range group and their holders, or
    /// a move of one member to a node (doc 25 §6).
    Ranges {
        #[arg(long)]
        session: Option<String>,
        #[command(subcommand)]
        command: RangesCommand,
    },
    Request {
        #[command(subcommand)]
        command: RequestCommand,
    },
}
#[derive(Subcommand)]
pub(crate) enum RangesCommand {
    /// Every member: identity, span, generation, holder and readers, with the
    /// transfer in progress and the retired maps awaiting cleanup.
    List,
    /// Begin moving one member to a node; the controller carries the transfer
    /// through readiness, activation and cleanup.
    Move {
        #[arg(long)]
        member: String,
        #[arg(long)]
        node: u64,
    },
}

pub(crate) fn run(
    runtime: &tokio::runtime::Runtime,
    settings: &focal_node::config::Settings,
    command: ClusterCommand,
) -> crate::Result<()> {
    if let ClusterCommand::Invite { node, output } = command {
        return runtime.block_on(crate::invite(settings, &node, &output));
    }
    // Verification reads only the backup: it runs anywhere the binary does.
    if let ClusterCommand::Backup {
        command: BackupCommand::Verify { input },
    } = &command
    {
        let result = ClusterAdmin::backup_verify(input)?;
        return crate::print_json(&serde_json::json!({"schema_version":1,"result":result}));
    }
    // A laptop node has no admin socket: its activation runs offline against
    // the exclusive data directory and returns once the record is applied.
    if let ClusterCommand::Replicas {
        command: ReplicaCommand::ActivateNative { session: None },
    } = &command
        && !focal_node::network_state::network_requested(&settings.data_dir()?, settings)
    {
        let activation = focal_node::native_activation::activate_local(
            settings,
            focal_ledger::NativeContentProfile::AuthoredV1,
        )?;
        let group: String = activation
            .group
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        return crate::print_json(&serde_json::json!({
            "schema_version": 1,
            "result": focal_client::admin::AdminResult::ReplicaNativeActivationProposed {
                session: activation.ledger.session.to_string(),
                group,
            },
            "activated": activation.activation.is_native(),
            "proposed": activation.proposed,
        }));
    }
    let admin = ClusterAdmin::open(settings)?;
    if let ClusterCommand::Node {
        command: NodeCommand::Metrics,
    } = &command
    {
        // Exposition text goes out as it is: a scraper reads it, not a
        // JSON consumer.
        let result =
            runtime.block_on(admin.operator(focal_node::network_admin::OperatorRead::Metrics))?;
        let focal_client::admin::AdminResult::Metrics { text } = result else {
            return Err(ClusterAdminError::Invalid.into());
        };
        return crate::print_text(&text);
    }
    let result = match command {
        ClusterCommand::Node {
            command: NodeCommand::Probe { check },
        } => runtime.block_on(admin.probe(&check)),
        ClusterCommand::Node { command } => runtime.block_on(admin.operator(match command {
            NodeCommand::Identity => focal_node::network_admin::OperatorRead::Identity,
            NodeCommand::Health => focal_node::network_admin::OperatorRead::Health,
            NodeCommand::Config => focal_node::network_admin::OperatorRead::Configuration,
            NodeCommand::Readiness => focal_node::network_admin::OperatorRead::Readiness,
            NodeCommand::Metrics | NodeCommand::Probe { .. } => {
                return Err(ClusterAdminError::Invalid.into());
            }
        })),
        ClusterCommand::Replicas { command } => replicas(runtime, &admin, command),
        ClusterCommand::Retention {
            command: RetentionCommand::Show { session },
        } => runtime.block_on(admin.retention(session_of(&admin, session)?)),
        ClusterCommand::Archive {
            command: ArchiveCommand::Show { session, claim },
        } => runtime.block_on(admin.archive(
            session_of(&admin, session)?,
            focal_model::ClaimId(focal_client::input::parse_id(&claim)?),
        )),
        ClusterCommand::Gc {
            command: GcCommand::Show,
        } => runtime.block_on(admin.gc()),
        ClusterCommand::Gc {
            command: GcCommand::Restore { domain, root },
        } => runtime.block_on(admin.gc_restore(
            focal_model::ContentDomainId(focal_client::input::parse_id(&domain)?),
            focal_client::input::parse_hash(&root)?,
        )),
        ClusterCommand::Backup {
            command:
                BackupCommand::Create {
                    tenant,
                    session,
                    output,
                },
        } => {
            let ledger = focal_model::LedgerId {
                tenant: tenant.map_or(Ok(admin.tenant()), |tenant| {
                    focal_client::input::parse_id(&tenant).map(focal_model::TenantId)
                })?,
                session: session_of(&admin, session)?,
            };
            runtime.block_on(admin.backup_create(ledger, &output))
        }
        ClusterCommand::Backup {
            command: BackupCommand::Verify { .. },
        } => return Err("invalid backup dispatch".into()),
        ClusterCommand::Restore {
            input,
            new_incarnation,
        } => runtime.block_on(admin.restore(&input, new_incarnation)),
        ClusterCommand::Repair {
            tenant,
            session,
            after,
            limit,
        } => {
            let ledger = focal_model::LedgerId {
                tenant: tenant.map_or(Ok(admin.tenant()), |tenant| {
                    focal_client::input::parse_id(&tenant).map(focal_model::TenantId)
                })?,
                session: session_of(&admin, session)?,
            };
            let after = after
                .map(|after| focal_client::input::parse_id(&after))
                .transpose()?;
            runtime.block_on(admin.repair(ledger, after, limit))
        }
        ClusterCommand::Upgrade {
            command: UpgradeCommand::Status,
        } => runtime.block_on(admin.upgrade_status()),
        ClusterCommand::Upgrade {
            command: UpgradeCommand::Activate { fence },
        } => runtime.block_on(admin.activate_fence(fence)),
        ClusterCommand::Storage {
            command: StorageCommand::Show,
        } => runtime.block_on(admin.storage()),
        ClusterCommand::Status => runtime.block_on(admin.read(AdminRead::Membership)),
        ClusterCommand::Placement => runtime.block_on(admin.placement()),
        ClusterCommand::Plan => runtime.block_on(admin.plan()),
        ClusterCommand::Tenants { command } => match command {
            TenantCommand::Admit { tenant } => {
                runtime.block_on(admin.admit_tenant(focal_client::input::parse_id(&tenant)?))
            }
            TenantCommand::List => runtime.block_on(admin.tenants()),
        },
        ClusterCommand::Sessions { command } => match command {
            SessionCommand::Create { tenant, name } => runtime
                .block_on(admin.create_session(focal_client::input::parse_id(&tenant)?, &name)),
            SessionCommand::Plan {
                tenant,
                session,
                survive,
                max_failures,
                dry_run,
            } => runtime.block_on(admin.plan_session(
                focal_client::input::parse_id(&tenant)?,
                focal_client::input::parse_id(&session)?,
                &survive,
                max_failures,
                dry_run,
            )),
        },
        ClusterCommand::Nodes { command } => match command {
            NodesCommand::List => runtime.block_on(admin.read(AdminRead::Contacts)),
            NodesCommand::Drain { node } => runtime.block_on(admin.node_eligibility(node, false)),
            NodesCommand::Undrain { node } => runtime.block_on(admin.node_eligibility(node, true)),
            NodesCommand::Remove { node } => runtime.block_on(admin.remove_node(node)),
            NodesCommand::Replace { node, replacement } => {
                runtime.block_on(admin.replace_node(node, replacement))
            }
        },
        ClusterCommand::Client {
            command: ClientCommand::Invite { name, output },
        } => runtime.block_on(admin.invite_client(&name, &output)),
        ClusterCommand::Invitations { command } => match command {
            InvitationCommand::List {
                after,
                limit,
                expected_revision,
            } => runtime.block_on(
                admin.read(AdminRead::Invitations {
                    after: after
                        .as_deref()
                        .map(focal_client::input::parse_id)
                        .transpose()?,
                    limit,
                    expected_revision,
                }),
            ),
            InvitationCommand::Get { id } => runtime.block_on(admin.read(AdminRead::Invitation {
                id: focal_client::input::parse_id(&id)?,
            })),
            InvitationCommand::Revoke {
                id,
                expected_revision,
            } => runtime
                .block_on(admin.revoke(focal_client::input::parse_id(&id)?, expected_revision)),
        },
        ClusterCommand::Credentials { command } => match command {
            CredentialCommand::Get { invitation } => {
                runtime.block_on(admin.read(AdminRead::Invitation {
                    id: focal_client::input::parse_id(&invitation)?,
                }))
            }
            CredentialCommand::Revoke {
                invitation,
                expected_revision,
            } => runtime.block_on(admin.revoke(
                focal_client::input::parse_id(&invitation)?,
                expected_revision,
            )),
            CredentialCommand::Renew => runtime.block_on(admin.renew_credential()),
            CredentialCommand::Rotate => runtime.block_on(admin.rotate_credential()),
        },
        ClusterCommand::Membership { command } => match command {
            MembershipCommand::Show => runtime.block_on(admin.read(AdminRead::Configuration)),
            MembershipCommand::AddLearner {
                node,
                expected_configuration_index,
            } => runtime.block_on(admin.membership(
                MembershipChange::AddLearner { node },
                expected_configuration_index,
            )),
            MembershipCommand::Promote {
                node,
                expected_configuration_index,
            } => runtime.block_on(admin.membership(
                MembershipChange::Promote { node },
                expected_configuration_index,
            )),
            MembershipCommand::Remove {
                node,
                expected_configuration_index,
            } => runtime.block_on(admin.membership(
                MembershipChange::Remove { node },
                expected_configuration_index,
            )),
            MembershipCommand::LeaveJoint {
                expected_configuration_index,
            } => runtime.block_on(
                admin.membership(MembershipChange::LeaveJoint, expected_configuration_index),
            ),
        },
        ClusterCommand::Leader {
            command:
                LeaderCommand::Transfer {
                    node,
                    expected_configuration_index,
                },
        } => runtime.block_on(admin.transfer(node, expected_configuration_index)),
        ClusterCommand::Request {
            command: RequestCommand::Inspect,
        } => admin.inspect(),
        ClusterCommand::Request {
            command: RequestCommand::Retry { operation_id },
        } => runtime.block_on(admin.retry(&operation_id)),
        ClusterCommand::Request {
            command: RequestCommand::Reconcile { operation_id },
        } => runtime.block_on(admin.reconcile(&operation_id)),
        ClusterCommand::Invite { .. } => return Err("invalid invitation dispatch".into()),
    }?;
    crate::print_json(&serde_json::json!({"schema_version":1,"result":result}))
}

/// The session an operator named, else the node identity's original one.
fn session_of(
    admin: &ClusterAdmin,
    value: Option<String>,
) -> Result<focal_model::SessionId, focal_node::cluster_admin::ClusterAdminError> {
    value
        .map(|value| {
            focal_client::input::parse_id(&value)
                .map(focal_model::SessionId)
                .map_err(|_| focal_node::cluster_admin::ClusterAdminError::Invalid)
        })
        .transpose()
        .map(|session| session.unwrap_or(admin.identity().ledger.session))
}
fn replicas(
    runtime: &tokio::runtime::Runtime,
    admin: &ClusterAdmin,
    command: ReplicaCommand,
) -> Result<focal_client::admin::AdminResult, focal_node::cluster_admin::ClusterAdminError> {
    use focal_node::cluster_admin::ClusterAdminError;
    let session = |value: Option<String>| {
        value
            .map(|value| {
                focal_client::input::parse_id(&value)
                    .map(focal_model::SessionId)
                    .map_err(|_| ClusterAdminError::Invalid)
            })
            .transpose()
            .map(|s| s.unwrap_or(admin.identity().ledger.session))
    };
    match command {
        ReplicaCommand::Diagnostics { session: value } => runtime.block_on(admin.operator(
            focal_node::network_admin::OperatorRead::Replica {
                session: session(value)?,
            },
        )),
        ReplicaCommand::Transfer {
            session: value,
            node,
            expected_configuration_index,
        } => runtime.block_on(admin.replica_transfer(
            session(value)?,
            node,
            expected_configuration_index,
        )),
        ReplicaCommand::ActivateNative { session: value } => {
            runtime.block_on(admin.replica_activate_native(session(value)?))
        }
        ReplicaCommand::Checkpoint { session: value } => {
            runtime.block_on(admin.replica_checkpoint(session(value)?))
        }
        ReplicaCommand::List { after, limit } => runtime.block_on(
            admin.replica_list(
                after
                    .map(|value| {
                        focal_client::input::parse_id(&value)
                            .map(focal_model::SessionId)
                            .map_err(|_| ClusterAdminError::Invalid)
                    })
                    .transpose()?,
                limit,
            ),
        ),
        ReplicaCommand::Show { session: value } => {
            runtime.block_on(admin.replica_show(session(value)?))
        }
        ReplicaCommand::Membership {
            session: value,
            command,
        } => {
            let session = session(value)?;
            let (change, index) = match command {
                MembershipCommand::Show => return runtime.block_on(admin.replica_show(session)),
                MembershipCommand::AddLearner {
                    node,
                    expected_configuration_index,
                } => (
                    MembershipChange::AddLearner { node },
                    expected_configuration_index,
                ),
                MembershipCommand::Promote {
                    node,
                    expected_configuration_index,
                } => (
                    MembershipChange::Promote { node },
                    expected_configuration_index,
                ),
                MembershipCommand::Remove {
                    node,
                    expected_configuration_index,
                } => (
                    MembershipChange::Remove { node },
                    expected_configuration_index,
                ),
                MembershipCommand::LeaveJoint {
                    expected_configuration_index,
                } => (MembershipChange::LeaveJoint, expected_configuration_index),
            };
            runtime.block_on(admin.replica_change(session, change, index))
        }
        ReplicaCommand::Ranges {
            session: value,
            command,
        } => {
            let session = session(value)?;
            match command {
                RangesCommand::List => runtime.block_on(admin.replica_ranges(session)),
                RangesCommand::Move { member, node } => runtime.block_on(
                    admin.move_range(
                        admin.tenant().0,
                        session.0,
                        focal_client::input::parse_id(&member)
                            .map_err(|_| ClusterAdminError::Invalid)?,
                        node,
                    ),
                ),
            }
        }
        ReplicaCommand::Request { command } => match command {
            RequestCommand::Inspect => admin.replica_inspect(),
            RequestCommand::Retry { operation_id } => {
                runtime.block_on(admin.replica_retry(&operation_id))
            }
            RequestCommand::Reconcile { operation_id } => {
                runtime.block_on(admin.replica_reconcile(&operation_id))
            }
        },
    }
}
