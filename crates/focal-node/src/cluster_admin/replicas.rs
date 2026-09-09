use super::*;
use crate::network_admin::{ReplicaAdminCommand, ReplicaAdminReply};
use focal_client::admin::{AdminReplicaMembership, AdminReplicaStatus};
use focal_ledger::{MembershipView, SessionMembershipReceipt, SessionMembershipRequest};
use focal_model::SessionId;

const DIRECTORY: &str = "REPLICA.admin";
const MARKER: &str = "REPLICA.admin.initialized";
#[derive(Serialize, Deserialize)]
struct State {
    schema: u16,
    identity: NodeIdentity,
    next: u64,
    latest: Option<Intent>,
}
#[derive(Serialize, Deserialize)]
struct Intent {
    sequence: u64,
    session: SessionId,
    group: [u8; 16],
    request: SessionMembershipRequest,
    receipt: Option<SessionMembershipReceipt>,
    fenced: bool,
}
impl ClusterAdmin {
    pub async fn replica_transfer(
        &self,
        session: SessionId,
        target: u64,
        expected_index: Option<u64>,
    ) -> Result<AdminResult> {
        let (group, view) = self.replica_configuration(session, None).await?;
        if expected_index.is_some_and(|expected| expected != view.configuration_index) {
            return Err(ControlFailure::CompareFailed.into());
        }
        let request = ControlTransfer {
            target,
            expected_configuration_index: view.configuration_index,
            expected: view.configuration,
        };
        match self
            .replica_exchange(ReplicaAdminCommand::Transfer {
                session,
                group,
                request,
            })
            .await?
        {
            ReplicaAdminReply::TransferInitiated {
                session: actual,
                group: actual_group,
                target: actual_target,
            } if actual == session && actual_group == group && actual_target == target => {
                Ok(AdminResult::ReplicaTransferInitiated {
                    session: session.to_string(),
                    group: hex(&group),
                    target,
                })
            }
            _ => Err(ClusterAdminError::Invalid),
        }
    }
    /// Propose native activation on the ledger's authority. The record commits
    /// under the current configuration; diagnostics show when it applied.
    pub async fn replica_activate_native(&self, session: SessionId) -> Result<AdminResult> {
        let (group, _) = self.replica_configuration(session, None).await?;
        match self
            .replica_exchange(ReplicaAdminCommand::ActivateNative { session, group })
            .await?
        {
            ReplicaAdminReply::NativeActivationProposed {
                session: actual,
                group: actual_group,
            } if actual == session && actual_group == group => {
                Ok(AdminResult::ReplicaNativeActivationProposed {
                    session: session.to_string(),
                    group: hex(&group),
                })
            }
            _ => Err(ClusterAdminError::Invalid),
        }
    }
    pub async fn replica_list(&self, after: Option<SessionId>, limit: u16) -> Result<AdminResult> {
        let ReplicaAdminReply::Inventory {
            management_sequence,
            replicas,
            next,
        } = self
            .replica_exchange(ReplicaAdminCommand::List { after, limit })
            .await?
        else {
            return Err(ClusterAdminError::Invalid);
        };
        if replicas.len() > usize::from(limit)
            || !replicas
                .windows(2)
                .all(|rows| matches!(rows,[left,right] if left.session<right.session))
            || replicas
                .first()
                .is_some_and(|r| after.is_some_and(|a| r.session <= a))
            || next.is_some_and(|next| replicas.last().is_none_or(|r| r.session != next))
        {
            return Err(ClusterAdminError::Invalid);
        }
        let mut rows = Vec::new();
        rows.try_reserve_exact(replicas.len())
            .map_err(|_| ClusterAdminError::Capacity)?;
        for row in replicas {
            if row.node != self.identity.node || row.group == [0; 16] || row.session.is_zero() {
                return Err(ClusterAdminError::Invalid);
            }
            rows.push(AdminReplicaStatus {
                session: row.session.to_string(),
                group: hex(&row.group),
                node: row.node,
                leader: row.leader,
                term: row.term,
                sequence: row.sequence.0,
                stopped: row.stopped,
            });
        }
        Ok(AdminResult::Replicas {
            node: self.identity.node,
            management_sequence,
            replicas: rows,
            next: next.map(|s| s.to_string()),
        })
    }
    pub async fn replica_show(&self, session: SessionId) -> Result<AdminResult> {
        let (group, view) = self.replica_configuration(session, None).await?;
        Ok(AdminResult::ReplicaMembership {
            membership: membership(
                &self.identity,
                session,
                group,
                view.configuration_index,
                view.configuration,
            ),
        })
    }
    pub async fn replica_change(
        &self,
        session: SessionId,
        change: MembershipChange,
        expected_index: Option<u64>,
    ) -> Result<AdminResult> {
        let (mut journal, mut state) = self.replica_journal(true)?;
        if state
            .latest
            .as_ref()
            .is_some_and(|latest| latest.receipt.is_none() && !latest.fenced)
        {
            return Err(ClusterAdminError::Pending);
        }
        let (group, view) = self.replica_configuration(session, None).await?;
        if expected_index.is_some_and(|expected| expected != view.configuration_index) {
            return Err(ControlFailure::CompareFailed.into());
        }
        let next_configuration = change
            .apply_to(&view.configuration)
            .map_err(|_| ClusterAdminError::Invalid)?;
        let sequence = state.next;
        let mut hash = blake3::Hasher::new_derive_key("focal.node.replica-admin.intent.v1");
        hash.update(&self.identity.cluster);
        hash.update(&self.identity.node.to_be_bytes());
        hash.update(&self.identity.ledger.tenant.0);
        hash.update(&session.0);
        hash.update(&group);
        hash.update(&sequence.to_be_bytes());
        hash.update(
            &postcard::to_stdvec(&(view.configuration_index, change))
                .map_err(|_| ClusterAdminError::Capacity)?,
        );
        let mut id = [0; 16];
        for (out, byte) in id.iter_mut().zip(hash.finalize().as_bytes()) {
            *out = *byte;
        }
        let request = SessionMembershipRequest {
            id,
            expected_index: view.configuration_index,
            expected: view.configuration,
            change,
        };
        request.validate().map_err(|_| ClusterAdminError::Invalid)?;
        let bytes = postcard::experimental::serialized_size(&request)
            .map_err(|_| ClusterAdminError::Capacity)?
            .checked_add(
                postcard::experimental::serialized_size(&next_configuration)
                    .map_err(|_| ClusterAdminError::Capacity)?,
            )
            .and_then(|v| v.checked_add(2048))
            .ok_or(ClusterAdminError::Capacity)?;
        if bytes > LIMIT {
            return Err(ClusterAdminError::Capacity);
        }
        state.next = sequence.checked_add(1).ok_or(ClusterAdminError::Capacity)?;
        state.latest = Some(Intent {
            sequence,
            session,
            group,
            request,
            receipt: None,
            fenced: false,
        });
        save_value(&mut journal, &state)?;
        self.replica_drive(&mut journal, &mut state).await
    }
    pub fn replica_inspect(&self) -> Result<AdminResult> {
        let (_journal, state) = self.replica_journal(false)?;
        result(&self.identity, &state)
    }
    pub async fn replica_retry(&self, reference: &str) -> Result<AdminResult> {
        let (mut journal, mut state) = self.replica_journal(false)?;
        check_reference(self.identity.node, &state, reference)?;
        self.replica_drive(&mut journal, &mut state).await
    }
    /// A newer configuration fences a request but cannot recover a receipt
    /// overwritten by another membership operation. Preserve that uncertainty.
    pub async fn replica_reconcile(&self, reference: &str) -> Result<AdminResult> {
        let (mut journal, mut state) = self.replica_journal(false)?;
        check_reference(self.identity.node, &state, reference)?;
        let latest = state.latest.as_ref().ok_or(ClusterAdminError::Expired)?;
        if latest.receipt.is_some() || latest.fenced {
            return result(&self.identity, &state);
        }
        let (_, view) = self
            .replica_configuration(latest.session, Some(latest.group))
            .await?;
        if reconcile_view(&mut state, &view)? {
            save_value(&mut journal, &state)?;
        }
        result(&self.identity, &state)
    }
    async fn replica_drive(
        &self,
        journal: &mut PrivateJournal,
        state: &mut State,
    ) -> Result<AdminResult> {
        let latest = state.latest.as_ref().ok_or(ClusterAdminError::Expired)?;
        if latest.receipt.is_some() {
            return result(&self.identity, state);
        }
        if latest.fenced {
            return Err(ClusterAdminError::Expired);
        }
        let ReplicaAdminReply::Configuration {
            session,
            group,
            view,
        } = self
            .replica_exchange(ReplicaAdminCommand::Change {
                session: latest.session,
                group: latest.group,
                request: latest.request.clone(),
            })
            .await?
        else {
            return Err(ClusterAdminError::Invalid);
        };
        if session != latest.session || group != latest.group {
            return Err(ClusterAdminError::Invalid);
        }
        validate_view(&view)?;
        let receipt =
            matching_receipt(&latest.request, &view)?.ok_or(ClusterAdminError::Pending)?;
        state
            .latest
            .as_mut()
            .ok_or(ClusterAdminError::Corrupt)?
            .receipt = Some(receipt);
        save_value(journal, state)?;
        result(&self.identity, state)
    }
    async fn replica_configuration(
        &self,
        session: SessionId,
        expected: Option<[u8; 16]>,
    ) -> Result<([u8; 16], MembershipView)> {
        let ReplicaAdminReply::Configuration {
            session: actual,
            group,
            view,
        } = self
            .replica_exchange(ReplicaAdminCommand::Configuration {
                session,
                group: expected,
            })
            .await?
        else {
            return Err(ClusterAdminError::Invalid);
        };
        if actual != session
            || group == [0; 16]
            || expected.is_some_and(|expected| expected != group)
        {
            return Err(ClusterAdminError::Invalid);
        }
        validate_view(&view)?;
        Ok((group, *view))
    }
    async fn replica_exchange(&self, command: ReplicaAdminCommand) -> Result<ReplicaAdminReply> {
        let bytes = self
            .exchange_bytes(AdminCommand::Replica(Box::new(command)))
            .await?;
        let (reply, tail): (ReplicaAdminReply, _) =
            postcard::take_from_bytes(&bytes).map_err(|_| ClusterAdminError::Invalid)?;
        if !tail.is_empty() {
            return Err(ClusterAdminError::Invalid);
        }
        match reply {
            ReplicaAdminReply::Rejected(error) => Err(error.into()),
            reply => Ok(reply),
        }
    }
    fn replica_journal(&self, create: bool) -> Result<(PrivateJournal, State)> {
        let fresh = initialize_named(&self.root, &self.identity, create, DIRECTORY, MARKER)?;
        let mut journal = PrivateJournal::open(self.root.join(DIRECTORY))?;
        let state = match journal.read()? {
            Some(bytes) => {
                let (state, tail): (State, _) =
                    postcard::take_from_bytes(&bytes).map_err(|_| ClusterAdminError::Corrupt)?;
                if !tail.is_empty() {
                    return Err(ClusterAdminError::Corrupt);
                }
                state
            }
            None if fresh => {
                let state = State {
                    schema: 1,
                    identity: self.identity.clone(),
                    next: 1,
                    latest: None,
                };
                save_value(&mut journal, &state)?;
                state
            }
            None => return Err(ClusterAdminError::Corrupt),
        };
        if state.schema != 1 || state.identity != self.identity || state.next == 0 {
            return Err(ClusterAdminError::Corrupt);
        }
        if let Some(latest) = &state.latest {
            if latest.sequence == 0
                || latest.sequence.checked_add(1) != Some(state.next)
                || latest.session.is_zero()
                || latest.group == [0; 16]
                || (latest.receipt.is_some() && latest.fenced)
            {
                return Err(ClusterAdminError::Corrupt);
            }
            latest
                .request
                .validate()
                .map_err(|_| ClusterAdminError::Corrupt)?;
            if let Some(receipt) = &latest.receipt {
                validate_receipt(&latest.request, receipt)?;
            }
        } else if state.next != 1 {
            return Err(ClusterAdminError::Corrupt);
        }
        Ok((journal, state))
    }
}
fn reconcile_view(state: &mut State, view: &MembershipView) -> Result<bool> {
    validate_view(view)?;
    let latest = state.latest.as_mut().ok_or(ClusterAdminError::Corrupt)?;
    if latest.receipt.is_some() || latest.fenced {
        return Ok(false);
    }
    let receipt = matching_receipt(&latest.request, view)?;
    let fenced = receipt.is_none() && view.configuration_index > latest.request.expected_index;
    let changed = receipt.is_some() || fenced;
    if changed {
        latest.receipt = receipt;
        latest.fenced = fenced;
    }
    Ok(changed)
}
fn validate_view(view: &MembershipView) -> Result<()> {
    view.configuration
        .validate()
        .map_err(|_| ClusterAdminError::Invalid)?;
    if view.latest.as_ref().is_some_and(|receipt| {
        receipt.id == [0; 16]
            || receipt.index == 0
            || receipt.term == 0
            || receipt.index != view.configuration_index
            || receipt.configuration != view.configuration
    }) {
        return Err(ClusterAdminError::Invalid);
    }
    Ok(())
}
fn validate_receipt(
    request: &SessionMembershipRequest,
    receipt: &SessionMembershipReceipt,
) -> Result<()> {
    let bytes = postcard::to_stdvec(request).map_err(|_| ClusterAdminError::Capacity)?;
    if receipt.id != request.id
        || receipt.request_hash != *blake3::hash(&bytes).as_bytes()
        || receipt.index <= request.expected_index
        || receipt.term == 0
        || receipt.configuration
            != request
                .change
                .apply_to(&request.expected)
                .map_err(|_| ClusterAdminError::Invalid)?
    {
        return Err(ClusterAdminError::Invalid);
    }
    Ok(())
}
fn matching_receipt(
    request: &SessionMembershipRequest,
    view: &MembershipView,
) -> Result<Option<SessionMembershipReceipt>> {
    match &view.latest {
        Some(receipt) if receipt.id == request.id => {
            validate_receipt(request, receipt)?;
            Ok(Some(receipt.clone()))
        }
        _ => Ok(None),
    }
}
fn reference(node: u64, id: [u8; 16]) -> String {
    format!("r1:{node:016x}:{}", hex(&id))
}
fn check_reference(node: u64, state: &State, value: &str) -> Result<()> {
    if state
        .latest
        .as_ref()
        .is_none_or(|latest| reference(node, latest.request.id) != value)
    {
        Err(ClusterAdminError::Expired)
    } else {
        Ok(())
    }
}
fn result(identity: &NodeIdentity, state: &State) -> Result<AdminResult> {
    let latest = state.latest.as_ref().ok_or(ClusterAdminError::Expired)?;
    let operation_id = reference(identity.node, latest.request.id);
    match &latest.receipt {
        Some(receipt) => Ok(AdminResult::ReplicaCommitted {
            operation_id,
            request_id: hex(&receipt.id),
            request_hash: hex(&receipt.request_hash),
            committed_index: receipt.index,
            committed_term: receipt.term,
            membership: membership(
                identity,
                latest.session,
                latest.group,
                receipt.index,
                receipt.configuration.clone(),
            ),
        }),
        None => Ok(AdminResult::ReplicaRequest {
            operation_id,
            session: latest.session.to_string(),
            group: hex(&latest.group),
            state: if latest.fenced {
                "FencedOutcomeUnknown"
            } else {
                "Pending"
            }
            .into(),
        }),
    }
}
fn membership(
    identity: &NodeIdentity,
    session: SessionId,
    group: [u8; 16],
    index: u64,
    configuration: MembershipConfiguration,
) -> AdminReplicaMembership {
    AdminReplicaMembership {
        cluster: hex(&identity.cluster),
        tenant: identity.ledger.tenant.to_string(),
        session: session.to_string(),
        group: hex(&group),
        configuration_index: index,
        voters: configuration.voters,
        learners: configuration.learners,
        voters_outgoing: configuration.voters_outgoing,
        learners_next: configuration.learners_next,
        auto_leave: configuration.auto_leave,
    }
}

#[cfg(test)]
#[path = "replica_tests.rs"]
mod tests;
