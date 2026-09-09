//! The placement controller: on the node that leads the partition owner and
//! the session's log, drive a committed plan from preparation to activation
//! and retire the copies it leaves behind. Every step is derived from the
//! committed partition state, the session's applied membership and the root
//! authority, so a restarted controller resumes at the same step, and every
//! command goes through the agent's exact-retry journals.
use super::*;
use crate::{
    fleet::SessionMembershipRequest,
    placement_control::{CollectRequest, SessionFact},
    placement_proof::MembershipRecord,
};
use focal_consensus::MembershipChange;
use focal_directory::{
    AuthorityCommand, AuthorityOperation, GroupAuthorityGrant, PartitionCheckpoint,
    PendingPlacement, Refusal, RefusalCode, SessionFence, SessionFenceKind, required_phase,
};
use focal_ledger::SessionPlacementRequest;
use focal_model::{RaftIndex, RaftTerm};

/// A deterministic identity for one session-log configuration change, so a
/// lost reply is answered by the retained receipt of the identical change.
fn membership_id(ledger: LedgerId, operation: OperationId, node: u64, kind: u8) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new_derive_key("focal.placement.membership-change.v1");
    hasher.update(&ledger.tenant.0);
    hasher.update(&ledger.session.0);
    hasher.update(&operation.0);
    hasher.update(&node.to_be_bytes());
    hasher.update(&[kind]);
    let mut id = [0; 16];
    for (target, source) in id.iter_mut().zip(hasher.finalize().as_bytes()) {
        *target = *source;
    }
    id
}
const CHANGE_ADD_LEARNER: u8 = 1;
const CHANGE_PROMOTE: u8 = 2;
const CHANGE_REMOVE: u8 = 3;

fn same_members(applied: &[u64], granted: &BTreeMap<u64, u64>) -> bool {
    applied.iter().copied().eq(granted.keys().copied())
}

impl PlacementAgent {
    /// One controller step for one session, or nothing to do.
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed observations; no state is retained"
    )]
    pub(super) async fn control(
        &mut self,
        handles: &NetworkHandles,
        pool: &PeerConnectionPool,
        descriptor: &SessionDescriptor,
        directory: &PartitionCheckpoint,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        root: &RootObservation,
        now: i64,
    ) -> Result<Option<AgentStep>, AgentError> {
        let node = self.state.node;
        let Ok(host) = handles.fleet.current_host(descriptor.ledger) else {
            return Ok(None);
        };
        if host.progress().leader != node {
            return Ok(None);
        }
        let Some(authority) = root.authority() else {
            return Ok(None);
        };
        let Some(grant) = authority.groups.get(&descriptor.log_group) else {
            return Ok(None);
        };
        let facts = host.registration_facts().await?.value().clone();
        let membership = &facts.membership;
        let configuration = &membership.configuration;
        // A configuration the log committed but the root has not installed
        // fences every fact under it; install it first.
        if !same_members(&configuration.voters, &grant.voters)
            || !same_members(&configuration.voters_outgoing, &grant.outgoing_voters)
            || !same_members(&configuration.learners, &grant.learners)
        {
            let Some(receipt) = membership.latest.as_ref() else {
                return Ok(None);
            };
            if receipt.configuration != *configuration {
                return Ok(None);
            }
            let voters_changed = !same_members(&configuration.voters, &grant.voters)
                || !same_members(&configuration.voters_outgoing, &grant.outgoing_voters);
            let members = |ids: &[u64]| -> Result<BTreeMap<u64, u64>, AgentError> {
                ids.iter()
                    .map(|id| {
                        authority
                            .nodes
                            .get(id)
                            .map(|grant| (*id, grant.enrollment.generation))
                            .ok_or(AgentError::Identity)
                    })
                    .collect()
            };
            let next = GroupAuthorityGrant {
                group: grant.group,
                genesis: grant.genesis,
                scope: grant.scope.clone(),
                membership_epoch: grant
                    .membership_epoch
                    .checked_add(u64::from(voters_changed))
                    .ok_or(AgentError::Capacity)?,
                voters: members(&configuration.voters)?,
                outgoing_voters: members(&configuration.voters_outgoing)?,
                learners: members(&configuration.learners)?,
                expires_at: configuration
                    .voters
                    .iter()
                    .chain(&configuration.voters_outgoing)
                    .chain(&configuration.learners)
                    .filter_map(|id| authority.nodes.get(id).map(|grant| grant.expires_at))
                    .min()
                    .unwrap_or(grant.expires_at)
                    .min(grant.expires_at),
            };
            let record = MembershipRecord {
                index: RaftIndex(receipt.index),
                term: RaftTerm(receipt.term),
                record_hash: ContentHash(receipt.request_hash),
            };
            let window = self.window(now)?;
            let request = CollectRequest {
                ledger: descriptor.ledger,
                group: descriptor.log_group.0,
                voters: grant.voters.keys().copied().collect(),
                fact: SessionFact::Membership { next, record },
                window,
            };
            let proof = self.collect(handles, pool, request).await?;
            let enrollment_revision = self.root_enrollment_revision(root)?;
            let command = ControlCommand::Authority(AuthorityCommand {
                expected_revision: authority.revision,
                enrollment_revision,
                decided_at: now,
                operation: AuthorityOperation::ChangeGroup { proof },
            });
            let journals = self.journals.as_mut().ok_or(AgentError::Identity)?;
            journals.root.intend(&handles.control, command).await?;
            return Ok(Some(AgentStep::Advanced));
        }
        match &descriptor.pending {
            Some(plan) => {
                self.drive_plan(
                    handles, pool, descriptor, plan, snapshot, installed, grant, &facts, &host, now,
                )
                .await
            }
            None => {
                self.retire_and_heal(
                    handles, descriptor, directory, snapshot, installed, &facts, &host, now,
                )
                .await
            }
        }
    }
    pub(super) fn window(&self, now: i64) -> Result<ProofWindow, AgentError> {
        Ok(ProofWindow {
            issued_at: now,
            expires_at: now.checked_add(PROOF_WINDOW).ok_or(AgentError::Capacity)?,
        })
    }
    fn root_enrollment_revision(&self, root: &RootObservation) -> Result<u64, AgentError> {
        let ControlBootstrap::Root { enrollment, .. } = &root.snapshot().state else {
            return Err(AgentError::Identity);
        };
        Ok(focal_enrollment::EnrollmentRegistry::restore(
            enrollment,
            self.state.genesis.founder.cluster,
            focal_enrollment::EnrollmentLimits::default(),
        )
        .map_err(|_| AgentError::Identity)?
        .revision())
    }
    /// Apply one session-log configuration change exactly once.
    async fn change_membership(
        &self,
        host: &ReplicaHost,
        facts: &HostedSessionFacts,
        id: [u8; 16],
        change: MembershipChange,
    ) -> Result<Option<AgentStep>, AgentError> {
        if facts
            .membership
            .latest
            .as_ref()
            .is_some_and(|receipt| receipt.id == id)
        {
            return Ok(None);
        }
        match host
            .change_membership(SessionMembershipRequest {
                id,
                expected_index: facts.membership.configuration_index,
                expected: facts.membership.configuration.clone(),
                change,
            })
            .await
        {
            Ok(_) => Ok(Some(AgentStep::Advanced)),
            Err(LedgerError::Consensus(ConsensusError::LearnerBehind)) => Err(AgentError::Behind),
            Err(error) => Err(error.into()),
        }
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed observations; no state is retained"
    )]
    async fn drive_plan(
        &mut self,
        handles: &NetworkHandles,
        pool: &PeerConnectionPool,
        descriptor: &SessionDescriptor,
        plan: &PendingPlacement,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        grant: &GroupAuthorityGrant,
        facts: &HostedSessionFacts,
        host: &ReplicaHost,
        now: i64,
    ) -> Result<Option<AgentStep>, AgentError> {
        let configuration = &facts.membership.configuration;
        match plan.phase {
            PlacementPhase::Planned => {
                let command = self.session_command(
                    descriptor,
                    snapshot,
                    installed,
                    now,
                    SessionChange::BeginPreparation {
                        operation: plan.operation,
                    },
                    None,
                )?;
                return self.intend_partition(handles, command).await.map(Some);
            }
            PlacementPhase::Failed => return Ok(None),
            PlacementPhase::Cutover => {
                return self
                    .activate(
                        handles, pool, descriptor, plan, snapshot, installed, grant, facts, host,
                        now,
                    )
                    .await;
            }
            _ => {}
        }
        // New voters join the log as learners once installed and are promoted
        // once caught up; the root grant follows each committed change above.
        for (voter, progress) in plan
            .desired
            .placement
            .voters
            .keys()
            .filter_map(|voter| plan.progress.get(voter).map(|progress| (*voter, progress)))
        {
            if progress.phase == AssignmentPhase::Failed {
                continue;
            }
            let member = configuration.contains(voter);
            let is_voter = configuration.voters.contains(&voter);
            if !member && progress.phase.at_least(AssignmentPhase::Installed) {
                return self
                    .change_membership(
                        host,
                        facts,
                        membership_id(descriptor.ledger, plan.operation, voter, CHANGE_ADD_LEARNER),
                        MembershipChange::AddLearner { node: voter },
                    )
                    .await;
            }
            if member && !is_voter && progress.phase.at_least(AssignmentPhase::CaughtUp) {
                return self
                    .change_membership(
                        host,
                        facts,
                        membership_id(descriptor.ledger, plan.operation, voter, CHANGE_PROMOTE),
                        MembershipChange::Promote { node: voter },
                    )
                    .await;
            }
        }
        if !same_members(&configuration.voters, &plan.desired.placement.voters) {
            return Ok(None);
        }
        // The log now has the desired voters and the root grant names them:
        // commit the cutover record so every copy verifies custody under the
        // new route and signs readiness.
        let Some((active, _)) = &facts.active else {
            return Ok(None);
        };
        let request = SessionPlacementRequest {
            expected_index: active.index.0,
            expected_configuration_index: facts.membership.configuration_index,
            operation: plan.operation,
            kind: SessionFenceKind::Cutover,
            from_route: active.to_route,
            to_route: plan.next_route,
            membership_epoch: grant.membership_epoch,
            placement_epoch: plan.next_placement,
            placement: plan.desired.clone(),
        };
        let committed = facts.placement.as_ref().is_some_and(|fence| {
            fence.kind == SessionFenceKind::Cutover && fence.operation == plan.operation
        });
        if !committed {
            host.propose_placement(request).await?;
            return Ok(Some(AgentStep::Advanced));
        }
        // A voter whose custody is verified under the new route is promoted in
        // the directory's eyes once the grant names it as a voter.
        for (voter, progress) in plan
            .desired
            .placement
            .voters
            .keys()
            .filter_map(|voter| plan.progress.get(voter).map(|progress| (*voter, progress)))
        {
            if grant.voters.contains_key(&voter)
                && progress.phase.at_least(AssignmentPhase::CustodyVerified)
                && !progress.phase.at_least(AssignmentPhase::Promoted)
            {
                let mut promoted = progress.clone();
                promoted.phase = AssignmentPhase::Promoted;
                let command = self.session_command(
                    descriptor,
                    snapshot,
                    installed,
                    now,
                    SessionChange::Progress {
                        operation: plan.operation,
                        progress: promoted,
                    },
                    None,
                )?;
                return self.intend_partition(handles, command).await.map(Some);
            }
        }
        let all_promoted = plan.desired.placement.voters.keys().all(|voter| {
            plan.progress
                .get(voter)
                .is_some_and(|progress| progress.phase.at_least(AssignmentPhase::Promoted))
        });
        if !all_promoted {
            return Ok(None);
        }
        let fence = facts.placement.clone().ok_or(AgentError::Identity)?;
        self.record_fence(
            handles, pool, descriptor, plan, snapshot, installed, grant, request, fence, now,
        )
        .await
    }
    /// Collect a voter-majority proof of a committed placement fence and
    /// record it in the directory as the cutover barrier or the activation.
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed observations; no state is retained"
    )]
    async fn record_fence(
        &mut self,
        handles: &NetworkHandles,
        pool: &PeerConnectionPool,
        descriptor: &SessionDescriptor,
        plan: &PendingPlacement,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        grant: &GroupAuthorityGrant,
        request: SessionPlacementRequest,
        fence: SessionFence,
        now: i64,
    ) -> Result<Option<AgentStep>, AgentError> {
        let window = self.window(now)?;
        let request = CollectRequest {
            ledger: descriptor.ledger,
            group: descriptor.log_group.0,
            voters: grant.voters.keys().copied().collect(),
            fact: SessionFact::Placement(request),
            window,
        };
        let proof = self.collect(handles, pool, request).await?;
        let change = match fence.kind {
            SessionFenceKind::Cutover => SessionChange::Cutover {
                operation: plan.operation,
                authority: fence,
            },
            SessionFenceKind::Activated => SessionChange::Activate {
                operation: plan.operation,
                authority: fence,
            },
            SessionFenceKind::Created => return Err(AgentError::Identity),
        };
        let command =
            self.session_command(descriptor, snapshot, installed, now, change, Some(proof))?;
        self.intend_partition(handles, command).await.map(Some)
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed observations; no state is retained"
    )]
    async fn activate(
        &mut self,
        handles: &NetworkHandles,
        pool: &PeerConnectionPool,
        descriptor: &SessionDescriptor,
        plan: &PendingPlacement,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        grant: &GroupAuthorityGrant,
        facts: &HostedSessionFacts,
        host: &ReplicaHost,
        now: i64,
    ) -> Result<Option<AgentStep>, AgentError> {
        let Some(barrier) = &plan.barrier else {
            return Ok(None);
        };
        // Every copy must have signed readiness at or beyond the barrier.
        let ready = plan.desired.placement.nodes().into_iter().all(|node| {
            plan.ready
                .get(&node)
                .is_some_and(|ready| ready.through >= barrier.sequence)
                && plan.progress.get(&node).is_some_and(|progress| {
                    progress.phase.at_least(required_phase(&progress.roles))
                })
        });
        if !ready {
            return Ok(None);
        }
        let cutover = facts
            .placement
            .as_ref()
            .filter(|fence| fence.operation == plan.operation)
            .ok_or(AgentError::Identity)?;
        let request = SessionPlacementRequest {
            expected_index: barrier.index.0,
            expected_configuration_index: facts.membership.configuration_index,
            operation: plan.operation,
            kind: SessionFenceKind::Activated,
            from_route: barrier.from_route,
            to_route: barrier.to_route,
            membership_epoch: barrier.membership_epoch,
            placement_epoch: barrier.placement_epoch,
            placement: plan.desired.clone(),
        };
        if cutover.kind != SessionFenceKind::Activated {
            host.propose_placement(request).await?;
            return Ok(Some(AgentStep::Advanced));
        }
        let fence = cutover.clone();
        self.record_fence(
            handles, pool, descriptor, plan, snapshot, installed, grant, request, fence, now,
        )
        .await
    }
    /// After activation: drain and retire the copies the placement dropped,
    /// and re-plan under the active policy when the live nodes no longer
    /// carry it.
    #[allow(
        clippy::too_many_arguments,
        reason = "one bounded pass over borrowed observations; no state is retained"
    )]
    async fn retire_and_heal(
        &mut self,
        handles: &NetworkHandles,
        descriptor: &SessionDescriptor,
        directory: &PartitionCheckpoint,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        facts: &HostedSessionFacts,
        host: &ReplicaHost,
        now: i64,
    ) -> Result<Option<AgentStep>, AgentError> {
        let configuration = &facts.membership.configuration;
        for (node, copy) in &descriptor.retiring {
            match copy.phase {
                AssignmentPhase::Active => {
                    let command = self.session_command(
                        descriptor,
                        snapshot,
                        installed,
                        now,
                        SessionChange::Drain {
                            operation: descriptor.authority.operation,
                            node: *node,
                        },
                        None,
                    )?;
                    return self.intend_partition(handles, command).await.map(Some);
                }
                AssignmentPhase::Draining => {
                    if configuration.contains(*node) {
                        return self
                            .change_membership(
                                host,
                                facts,
                                membership_id(
                                    descriptor.ledger,
                                    descriptor.authority.operation,
                                    *node,
                                    CHANGE_REMOVE,
                                ),
                                MembershipChange::Remove { node: *node },
                            )
                            .await;
                    }
                    let command = self.session_command(
                        descriptor,
                        snapshot,
                        installed,
                        now,
                        SessionChange::Retire {
                            operation: descriptor.authority.operation,
                            node: *node,
                        },
                        None,
                    )?;
                    return self.intend_partition(handles, command).await.map(Some);
                }
                _ => {}
            }
        }
        // Self-healing: the active placement must still verify against the
        // live registry; otherwise plan again under the same policy.
        let config = self.partition_config();
        if focal_directory::verify_placement(
            &descriptor.active,
            &directory.nodes,
            config.max_members,
        )
        .is_ok()
        {
            return Ok(None);
        }
        let operation = OperationId({
            let mut hasher = blake3::Hasher::new_derive_key("focal.placement.heal.v1");
            hasher.update(&descriptor.ledger.tenant.0);
            hasher.update(&descriptor.ledger.session.0);
            hasher.update(&descriptor.authority.record_hash.0);
            let mut id = [0; 16];
            for (target, source) in id.iter_mut().zip(hasher.finalize().as_bytes()) {
                *target = *source;
            }
            id
        });
        match focal_directory::propose_placement(
            &directory.nodes,
            &descriptor.active.policy,
            config.max_members,
            config.min_disk_available,
        ) {
            Ok(proposal) => {
                let command = self.session_command(
                    descriptor,
                    snapshot,
                    installed,
                    now,
                    SessionChange::Plan {
                        operation,
                        desired: proposal.spec,
                        observations: proposal.observations,
                    },
                    None,
                )?;
                self.intend_partition(handles, command).await.map(Some)
            }
            Err(_) => {
                let refusal = Refusal {
                    operation,
                    code: RefusalCode::NoPlacement,
                    node: None,
                    attempt: 1,
                    at: 0,
                };
                if descriptor.refusals.contains(&refusal) {
                    return Ok(None);
                }
                let command = self.session_command(
                    descriptor,
                    snapshot,
                    installed,
                    now,
                    SessionChange::Refuse { operation, refusal },
                    None,
                )?;
                self.intend_partition(handles, command).await.map(Some)
            }
        }
    }
    /// The bounds the partition applies to every placement it accepts.
    pub(super) fn partition_config(&self) -> focal_directory::PartitionConfig {
        focal_directory::PartitionConfig::default()
    }
}
