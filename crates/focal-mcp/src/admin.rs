//! Optional physical-node administration. The installing process supplies an
//! authenticated local backend; this hook cannot carry arbitrary wire commands.
use focal_client::{admin::AdminResult, input::InputError};
use serde::Deserialize;
use tokio::{runtime::Runtime, sync::oneshot};

#[derive(Debug, Clone, Copy)]
pub enum AdminChange {
    AddLearner(u64),
    Promote(u64),
    Remove(u64),
    LeaveJoint,
}
#[derive(Debug)]
pub enum AdminAction {
    NodeIdentity,
    NodeHealth,
    NodeConfiguration,
    ReplicaDiagnostics {
        session: Option<[u8; 16]>,
    },
    ReplicaTransfer {
        session: Option<[u8; 16]>,
        node: u64,
        expected_configuration_index: Option<u64>,
    },
    InviteNode {
        name: String,
        output: String,
    },
    ReplicaList {
        after: Option<[u8; 16]>,
        limit: u16,
    },
    ReplicaShow {
        session: Option<[u8; 16]>,
    },
    ReplicaChange {
        session: Option<[u8; 16]>,
        change: AdminChange,
        expected_configuration_index: Option<u64>,
    },
    ReplicaInspect,
    ReplicaRetry {
        operation_id: String,
        reconcile: bool,
    },
    Status,
    Configuration,
    Contacts,
    Membership {
        change: AdminChange,
        expected_configuration_index: Option<u64>,
    },
    Transfer {
        node: u64,
        expected_configuration_index: Option<u64>,
    },
    Inspect,
    Retry {
        operation_id: String,
    },
    Reconcile {
        operation_id: String,
    },
    Invitations {
        after: Option<[u8; 16]>,
        limit: u16,
        expected_revision: Option<u64>,
    },
    Invitation {
        id: [u8; 16],
    },
    Revoke {
        id: [u8; 16],
        expected_revision: Option<u64>,
    },
    InviteClient {
        name: String,
        output: String,
    },
}
#[derive(Debug, thiserror::Error)]
#[error("{detail}")]
pub struct AdminError {
    pub code: &'static str,
    pub condition: &'static str,
    pub detail: String,
}
pub trait AdminBackend: Send {
    fn execute(
        &mut self,
        runtime: &Runtime,
        action: AdminAction,
        cancel: &mut oneshot::Receiver<()>,
    ) -> Result<AdminResult, AdminError>;
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    node: u64,
    expected_configuration_index: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fence {
    expected_configuration_index: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Retry {
    operation_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Invitation {
    id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Revoke {
    id: String,
    expected_revision: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Invitations {
    after: Option<String>,
    limit: Option<u16>,
    expected_revision: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InviteClient {
    name: String,
    output: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InviteNode {
    node: String,
    output: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplicaList {
    after: Option<String>,
    limit: Option<u16>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplicaSession {
    session: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplicaChange {
    session: Option<String>,
    node: Option<u64>,
    expected_configuration_index: Option<u64>,
}
pub(crate) fn parse(
    name: &str,
    arguments: serde_json::Map<String, serde_json::Value>,
) -> Result<AdminAction, InputError> {
    let value = serde_json::Value::Object(arguments);
    let action = match name {
        "cluster.node.identity" | "cluster.node.health" | "cluster.node.config" => {
            let _: Empty = serde_json::from_value(value)
                .map_err(|_| InputError::Invalid("node inspection"))?;
            match name {
                "cluster.node.identity" => AdminAction::NodeIdentity,
                "cluster.node.health" => AdminAction::NodeHealth,
                _ => AdminAction::NodeConfiguration,
            }
        }
        "cluster.replicas.diagnostics" => {
            let args: ReplicaSession = serde_json::from_value(value)
                .map_err(|_| InputError::Invalid("replica diagnostics"))?;
            AdminAction::ReplicaDiagnostics {
                session: parse_session(args.session)?,
            }
        }
        "cluster.replicas.transfer" => {
            let args: ReplicaChange = serde_json::from_value(value)
                .map_err(|_| InputError::Invalid("replica transfer"))?;
            let node = args
                .node
                .filter(|node| *node != 0)
                .ok_or(InputError::Invalid("replica node"))?;
            AdminAction::ReplicaTransfer {
                session: parse_session(args.session)?,
                node,
                expected_configuration_index: args.expected_configuration_index,
            }
        }
        "cluster.invite" => {
            let args: InviteNode = serde_json::from_value(value)
                .map_err(|_| InputError::Invalid("node invitation name/output"))?;
            valid_invite(&args.node, &args.output)?;
            AdminAction::InviteNode {
                name: args.node,
                output: args.output,
            }
        }
        "cluster.replicas.list" => {
            let args: ReplicaList =
                serde_json::from_value(value).map_err(|_| InputError::Invalid("replica page"))?;
            let limit = args.limit.unwrap_or(32);
            if limit == 0 || limit > 64 {
                return Err(InputError::Invalid("replica limit"));
            }
            AdminAction::ReplicaList {
                after: parse_session(args.after)?,
                limit,
            }
        }
        "cluster.replicas.show" => {
            let args: ReplicaSession = serde_json::from_value(value)
                .map_err(|_| InputError::Invalid("replica session"))?;
            AdminAction::ReplicaShow {
                session: parse_session(args.session)?,
            }
        }
        "cluster.replicas.request.inspect" => {
            let _: Empty = serde_json::from_value(value)
                .map_err(|_| InputError::Invalid("replica inspect"))?;
            AdminAction::ReplicaInspect
        }
        "cluster.replicas.request.retry" | "cluster.replicas.request.reconcile" => {
            let args: Retry = serde_json::from_value(value)
                .map_err(|_| InputError::Invalid("replica reference"))?;
            valid_reference(&args.operation_id, "r1")?;
            AdminAction::ReplicaRetry {
                operation_id: args.operation_id,
                reconcile: name.ends_with("reconcile"),
            }
        }
        "cluster.replicas.add_learner"
        | "cluster.replicas.promote"
        | "cluster.replicas.remove"
        | "cluster.replicas.leave_joint" => {
            let args: ReplicaChange = serde_json::from_value(value)
                .map_err(|_| InputError::Invalid("replica membership"))?;
            let change = if name.ends_with("leave_joint") {
                if args.node.is_some() {
                    return Err(InputError::Invalid("leave joint has no node"));
                }
                AdminChange::LeaveJoint
            } else {
                let node = args
                    .node
                    .filter(|node| *node != 0)
                    .ok_or(InputError::Invalid("replica node"))?;
                if name.ends_with("add_learner") {
                    AdminChange::AddLearner(node)
                } else if name.ends_with("promote") {
                    AdminChange::Promote(node)
                } else {
                    AdminChange::Remove(node)
                }
            };
            AdminAction::ReplicaChange {
                session: parse_session(args.session)?,
                change,
                expected_configuration_index: args.expected_configuration_index,
            }
        }
        "cluster.client.invite" => {
            let args: InviteClient = serde_json::from_value(value)
                .map_err(|_| InputError::Invalid("client invitation name/output"))?;
            valid_invite(&args.name, &args.output)?;
            AdminAction::InviteClient {
                name: args.name,
                output: args.output,
            }
        }
        "cluster.invitations.list" => {
            let args: Invitations = serde_json::from_value(value)
                .map_err(|_| InputError::Invalid("invitation page"))?;
            let limit = args.limit.unwrap_or(32);
            if limit == 0 || limit > 64 {
                return Err(InputError::Invalid("invitation limit"));
            }
            AdminAction::Invitations {
                after: args
                    .after
                    .as_deref()
                    .map(focal_client::input::parse_id)
                    .transpose()?,
                limit,
                expected_revision: args.expected_revision,
            }
        }
        "cluster.invitations.get" | "cluster.credentials.get" => {
            let args: Invitation =
                serde_json::from_value(value).map_err(|_| InputError::Invalid("invitation ID"))?;
            AdminAction::Invitation {
                id: focal_client::input::parse_id(&args.id)?,
            }
        }
        "cluster.invitations.revoke" | "cluster.credentials.revoke" => {
            let args: Revoke = serde_json::from_value(value)
                .map_err(|_| InputError::Invalid("invitation revocation"))?;
            AdminAction::Revoke {
                id: focal_client::input::parse_id(&args.id)?,
                expected_revision: args.expected_revision,
            }
        }
        "cluster.status"
        | "cluster.membership.show"
        | "cluster.nodes.list"
        | "cluster.request.inspect" => {
            let _: Empty = serde_json::from_value(value)
                .map_err(|_| InputError::Invalid("unexpected cluster input"))?;
            match name {
                "cluster.status" => AdminAction::Status,
                "cluster.membership.show" => AdminAction::Configuration,
                "cluster.nodes.list" => AdminAction::Contacts,
                _ => AdminAction::Inspect,
            }
        }
        "cluster.membership.add_learner"
        | "cluster.membership.promote"
        | "cluster.membership.remove"
        | "cluster.leader.transfer" => {
            let args: Member = serde_json::from_value(value)
                .map_err(|_| InputError::Invalid("cluster node and configuration fence"))?;
            if args.node == 0 {
                return Err(InputError::Invalid("zero cluster node"));
            }
            if name == "cluster.leader.transfer" {
                AdminAction::Transfer {
                    node: args.node,
                    expected_configuration_index: args.expected_configuration_index,
                }
            } else {
                AdminAction::Membership {
                    change: match name {
                        "cluster.membership.add_learner" => AdminChange::AddLearner(args.node),
                        "cluster.membership.promote" => AdminChange::Promote(args.node),
                        _ => AdminChange::Remove(args.node),
                    },
                    expected_configuration_index: args.expected_configuration_index,
                }
            }
        }
        "cluster.membership.leave_joint" => {
            let args: Fence = serde_json::from_value(value)
                .map_err(|_| InputError::Invalid("cluster configuration fence"))?;
            AdminAction::Membership {
                change: AdminChange::LeaveJoint,
                expected_configuration_index: args.expected_configuration_index,
            }
        }
        "cluster.request.retry" | "cluster.request.reconcile" => {
            let args: Retry = serde_json::from_value(value)
                .map_err(|_| InputError::Invalid("cluster admin operation reference"))?;
            let mut fields = args.operation_id.split(':');
            if fields.next() != Some("a1")
                || !fields.next().is_some_and(hex16)
                || !fields.next().is_some_and(hex16)
                || fields.next().is_some()
            {
                return Err(InputError::Invalid("cluster admin operation reference"));
            }
            if name == "cluster.request.retry" {
                AdminAction::Retry {
                    operation_id: args.operation_id,
                }
            } else {
                AdminAction::Reconcile {
                    operation_id: args.operation_id,
                }
            }
        }
        _ => return Err(InputError::Invalid("unsupported cluster operation")),
    };
    Ok(action)
}
fn hex16(value: &str) -> bool {
    value.len() == 16
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && value != "0000000000000000"
}
fn valid_invite(name: &str, output: &str) -> Result<(), InputError> {
    if name.is_empty()
        || name.len() > 63
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        || output.is_empty()
        || output.len() > 4096
    {
        return Err(InputError::Invalid("invitation name/output"));
    }
    Ok(())
}
fn parse_session(value: Option<String>) -> Result<Option<[u8; 16]>, InputError> {
    value
        .as_deref()
        .map(focal_client::input::parse_id)
        .transpose()
}
fn valid_reference(value: &str, prefix: &str) -> Result<(), InputError> {
    let mut parts = value.split(':');
    if parts.next() != Some(prefix)
        || !parts.next().is_some_and(hex16)
        || parts
            .next()
            .is_none_or(|part| focal_client::input::parse_id(part).is_err())
        || parts.next().is_some()
    {
        Err(InputError::Invalid("admin reference"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn args(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        value.as_object().unwrap().clone()
    }
    #[test]
    fn admin_catalog_and_typed_inputs_agree_without_raw_control_authority() {
        let mut tools = Vec::new();
        crate::catalog_admin::append(&mut tools).unwrap();
        assert_eq!(tools.len(), crate::catalog_admin::TOOL_COUNT);
        let mut names = std::collections::BTreeSet::new();
        for tool in tools {
            assert!(names.insert(tool.name.clone()));
            assert_eq!(tool.input_schema["additionalProperties"], false);
            let value = match tool.name.as_str() {
                "cluster.replicas.transfer"
                | "cluster.replicas.add_learner"
                | "cluster.replicas.promote"
                | "cluster.replicas.remove" => json!({"node":7,"expected_configuration_index":3}),
                "cluster.replicas.request.retry" | "cluster.replicas.request.reconcile" => {
                    json!({"operation_id":"r1:0000000000000001:00000000000000000000000000000002"})
                }
                "cluster.invite" => json!({"node":"worker-2","output":"/tmp/worker.invite"}),
                "cluster.membership.add_learner"
                | "cluster.membership.promote"
                | "cluster.membership.remove"
                | "cluster.leader.transfer" => json!({"node":7,"expected_configuration_index":3}),
                "cluster.request.retry" | "cluster.request.reconcile" => {
                    json!({"operation_id":"a1:0000000000000001:0000000000000002"})
                }
                "cluster.invitations.get"
                | "cluster.credentials.get"
                | "cluster.invitations.revoke"
                | "cluster.credentials.revoke" => json!({"id":"01010101010101010101010101010101"}),
                "cluster.client.invite" => json!({"name":"alice","output":"/tmp/alice.invite"}),
                _ => json!({}),
            };
            assert!(
                parse(&tool.name, args(value.clone())).is_ok(),
                "{}",
                tool.name
            );
            let mut forged = args(value);
            forged.insert("principal".into(), json!("another-owner"));
            assert!(parse(&tool.name, forged).is_err(), "{}", tool.name);
        }
        assert!(parse("cluster.control", args(json!({"request":[]}))).is_err());
        assert!(parse("cluster.membership.promote", args(json!({"node":0}))).is_err());
        assert!(parse("cluster.invitations.list", args(json!({"limit":65}))).is_err());
        assert!(
            parse(
                "cluster.request.retry",
                args(json!({"operation_id":"a1:0000000000000000:0000000000000002"}))
            )
            .is_err()
        );
    }
}
