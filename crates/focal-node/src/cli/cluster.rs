//! Local root and installed-application administration through the physical owner.
use clap::Subcommand;
use focal_control::MembershipChange;
use focal_node::{cluster_admin::ClusterAdmin, network_admin::AdminRead};
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
}
#[derive(Subcommand)]
pub(crate) enum NodeCommand {
    Identity,
    Health,
    Config,
}
#[derive(Subcommand)]
pub(crate) enum NodesCommand {
    List,
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
    Request {
        #[command(subcommand)]
        command: RequestCommand,
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
    let admin = ClusterAdmin::open(settings)?;
    let result = match command {
        ClusterCommand::Node { command } => runtime.block_on(admin.operator(match command {
            NodeCommand::Identity => focal_node::network_admin::OperatorRead::Identity,
            NodeCommand::Health => focal_node::network_admin::OperatorRead::Health,
            NodeCommand::Config => focal_node::network_admin::OperatorRead::Configuration,
        })),
        ClusterCommand::Replicas { command } => replicas(runtime, &admin, command),
        ClusterCommand::Status => runtime.block_on(admin.read(AdminRead::Membership)),
        ClusterCommand::Nodes {
            command: NodesCommand::List,
        } => runtime.block_on(admin.read(AdminRead::Contacts)),
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
