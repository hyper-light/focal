//! What planning and applying observe: the committed policy and the
//! directory's sessions as the placement agent reports them.
use super::plan::{DeploymentIdentity, Observation, ObservedNode, ObservedSession, SessionEpochs};
use super::{DeploymentError, GuaranteeLevel, parse_survive};
use crate::{cluster_admin::ClusterAdmin, config::policy::read_committed};
use focal_client::admin::AdminPlacement;
use std::collections::BTreeSet;

/// Observe the node: its committed policy and, when it runs a directory,
/// every session the placement view names. A truncated view is refused
/// because a plan must name every session it covers.
pub async fn observe(admin: &ClusterAdmin, network: bool) -> Result<Observation, DeploymentError> {
    let committed =
        read_committed(admin.root())?.ok_or(crate::config::ConfigError::PolicyMissing)?;
    let identity = admin.identity();
    let deployment = DeploymentIdentity {
        cluster: identity.cluster,
        node: identity.node,
    };
    if !network {
        return Ok(Observation {
            deployment,
            committed,
            observed_at: 0,
            sessions: Vec::new(),
            nodes: Vec::new(),
        });
    }
    let view = admin.placement_view().await?;
    let (sessions, nodes) = convert(&view)?;
    Ok(Observation {
        deployment,
        committed,
        observed_at: view.observed_at,
        sessions,
        nodes,
    })
}
fn id(text: &str) -> Result<[u8; 16], DeploymentError> {
    focal_client::input::parse_id(text)
        .map_err(|_| DeploymentError::Corrupt("directory view names an invalid identity"))
}
fn level(survive: &str, max_failures: u16) -> Result<GuaranteeLevel, DeploymentError> {
    Ok(GuaranteeLevel {
        survive: parse_survive(survive).ok_or(DeploymentError::Corrupt(
            "directory view names an unknown failure class",
        ))?,
        max_failures,
    })
}
/// Sessions in partition order, each once, and every node the partitions
/// know, each once.
pub fn convert(
    view: &AdminPlacement,
) -> Result<(Vec<ObservedSession>, Vec<ObservedNode>), DeploymentError> {
    let mut sessions = Vec::new();
    let mut nodes = Vec::new();
    let mut seen_sessions = BTreeSet::new();
    let mut seen_nodes = BTreeSet::new();
    for partition in &view.partitions {
        if partition.truncated {
            return Err(DeploymentError::Truncated);
        }
        for node in &partition.nodes {
            if seen_nodes.insert((node.node, node.generation)) {
                nodes
                    .try_reserve_exact(1)
                    .map_err(|_| DeploymentError::Capacity)?;
                nodes.push(ObservedNode {
                    node: node.node,
                    generation: node.generation,
                    alive: node.alive,
                    eligible: node.eligible,
                    disk_available: node.disk_available,
                });
            }
        }
        for session in &partition.sessions {
            let tenant = id(&session.tenant)?;
            let ledger = id(&session.session)?;
            if !seen_sessions.insert((tenant, ledger)) {
                continue;
            }
            if sessions.len() >= super::plan::MAX_SESSIONS {
                return Err(DeploymentError::Capacity);
            }
            sessions
                .try_reserve_exact(1)
                .map_err(|_| DeploymentError::Capacity)?;
            let achieved = match (&session.achieved_survive, session.achieved_max_failures) {
                (Some(survive), Some(max_failures)) => Some(level(survive, max_failures)?),
                _ => None,
            };
            let pending = match &session.pending {
                Some(pending) => Some(id(&pending.operation)?),
                None => None,
            };
            sessions.push(ObservedSession {
                tenant,
                session: ledger,
                epochs: SessionEpochs {
                    route: session.route_epoch,
                    membership: session.membership_epoch,
                    placement: session.placement_epoch,
                },
                voters: session.voters.clone(),
                desired: level(&session.survive, session.max_failures)?,
                achieved,
                pending,
                blocked_by: session.blocked_by.clone(),
            });
        }
    }
    Ok((sessions, nodes))
}
