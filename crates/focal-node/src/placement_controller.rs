//! The placement controller: on the node that leads the partition owner,
//! drive every session's committed plan from preparation to activation and
//! retire the copies it leaves behind — through this node's own replica when
//! it leads the session's log, and otherwise through the node that does
//! (`SessionDriver`, 24 §9). Every step is derived from the committed
//! partition state, the session's applied membership and the root
//! authority, so a restarted controller resumes at the same step, and every
//! command goes through the agent's exact-retry journals.
use super::*;
use crate::session_control::{
    SESSION_CONTROL_SCHEMA, SessionCall, SessionControlReply, SessionControlRequest,
};
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
const SESSION_CONTROL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// How the controller reaches a session's log: its own replica when this
/// node leads the log, otherwise the node that does, over the authenticated
/// peer connection (`Operation::SessionControl`, 24 §9).
pub(super) enum SessionDriver {
    Local(ReplicaHost),
    Remote { leader: u64 },
}
impl SessionDriver {
    fn local(&self) -> Option<&ReplicaHost> {
        match self {
            Self::Local(host) => Some(host),
            Self::Remote { .. } => None,
        }
    }
}
impl PlacementAgent {
    /// One session control call to the session's leader.
    async fn session_call(
        &mut self,
        pool: &PeerConnectionPool,
        leader: u64,
        descriptor: &SessionDescriptor,
        call: SessionCall,
    ) -> Result<SessionControlReply, AgentError> {
        let body = postcard::to_stdvec(&SessionControlRequest {
            schema: SESSION_CONTROL_SCHEMA,
            call,
        })
        .map_err(|_| AgentError::Capacity)?;
        let request = focal_wire::RequestEnvelope {
            protocol: focal_wire::PROTOCOL_VERSION,
            ledger: descriptor.ledger,
            route_epoch: descriptor.route_epoch,
            request_epoch: focal_model::RequestEpoch(1),
            request_id: self.next_request_id()?,
            operation: focal_wire::Operation::SessionControl {
                group: descriptor.ledger.session.0,
                request: body,
            },
        };
        let bytes = tokio::time::timeout(
            SESSION_CONTROL_TIMEOUT,
            pool.send_placement(leader, &request),
        )
        .await
        .map_err(|_| AgentError::Control(ControlFailure::Unavailable))?
        .map_err(|error| {
            AgentError::Control(match error {
                PeerSendError::Rejected(AccessError::Unauthorized) => ControlFailure::Unauthorized,
                PeerSendError::Rejected(
                    AccessError::InvalidRequest | AccessError::UnsupportedOperation,
                ) => ControlFailure::Invalid,
                PeerSendError::Rejected(AccessError::Capacity) => ControlFailure::Capacity,
                _ => ControlFailure::Unavailable,
            })
        })?;
        let reply: SessionControlReply =
            postcard::from_bytes(&bytes).map_err(|_| AgentError::Identity)?;
        if let SessionControlReply::Refused(failure) = &reply {
            if let ControlFailure::NotLeader { leader } = failure
                && *leader != 0
            {
                // Follow the log to where it leads on the next pass.
                let _ = self.session_leaders.insert(descriptor.ledger, *leader);
            }
            return Err(AgentError::Control(*failure));
        }
        Ok(reply)
    }
    /// The session's registration facts through `driver`.
    async fn session_facts(
        &mut self,
        pool: &PeerConnectionPool,
        driver: &SessionDriver,
        descriptor: &SessionDescriptor,
    ) -> Result<HostedSessionFacts, AgentError> {
        match driver {
            SessionDriver::Local(host) => Ok(host.registration_facts().await?.value().clone()),
            SessionDriver::Remote { leader } => {
                match self
                    .session_call(pool, *leader, descriptor, SessionCall::Facts)
                    .await?
                {
                    SessionControlReply::Facts(facts) => Ok(*facts),
                    _ => Err(AgentError::Identity),
                }
            }
        }
    }
    /// Propose one placement record to the session's log through `driver`.
    async fn session_placement(
        &mut self,
        pool: &PeerConnectionPool,
        driver: &SessionDriver,
        descriptor: &SessionDescriptor,
        request: SessionPlacementRequest,
    ) -> Result<(), AgentError> {
        match driver {
            SessionDriver::Local(host) => {
                host.propose_placement(request).await?;
                Ok(())
            }
            SessionDriver::Remote { leader } => {
                match self
                    .session_call(pool, *leader, descriptor, SessionCall::Placement(request))
                    .await?
                {
                    SessionControlReply::Placed => Ok(()),
                    _ => Err(AgentError::Identity),
                }
            }
        }
    }
    /// Range movement, balancing and holder publication drive the movement
    /// map on the log's own replica (25 §6–§9), so a voter that does not
    /// lead a session with such work asks its leader for leadership — one
    /// raft transfer message the leader answers by timing this voter into a
    /// campaign; a plan needs no claim (it is driven through the leader), and
    /// a node that is not a voter, or a session without range work, asks for
    /// nothing.
    async fn claim_for_ranges(&self, descriptor: &SessionDescriptor, handles: &NetworkHandles) {
        let node = self.state.node;
        let Ok(host) = handles.fleet.current_host(descriptor.ledger) else {
            return;
        };
        if host.progress().leader == 0 {
            return;
        }
        let voter = match host.registration_facts().await {
            Ok(facts) => facts
                .value()
                .membership
                .configuration
                .voters
                .contains(&node),
            Err(_) => false,
        };
        if !voter {
            return;
        }
        let queued = self
            .move_requests
            .iter()
            .any(|job| job.ledger == descriptor.ledger);
        let ranges = match host.range_view(false).await {
            Ok(view) => {
                view.pending.is_some()
                    || !view.history.is_empty()
                    || descriptor
                        .holders
                        .as_ref()
                        .is_none_or(|holders| holders.epoch < view.epoch.0)
            }
            Err(_) => false,
        };
        if queued || ranges {
            let _ = host.transfer_leader(node).await;
        }
    }
    /// Which driver reaches the session's log from here: the hosted replica
    /// when it leads, otherwise the leader it knows, the leader the last
    /// call named, the directory's route, or the placement's preferred
    /// leader.
    async fn session_driver(
        &self,
        handles: &NetworkHandles,
        descriptor: &SessionDescriptor,
    ) -> SessionDriver {
        let node = self.state.node;
        let mut known = 0;
        if let Ok(host) = handles.fleet.current_host(descriptor.ledger) {
            let progress = host.progress();
            if progress.leader == node {
                return SessionDriver::Local(host);
            }
            known = progress.leader;
        }
        if known == 0 {
            known = self
                .session_leaders
                .get(&descriptor.ledger)
                .copied()
                .unwrap_or(0);
        }
        if known == 0 {
            known = handles
                .routes
                .resolve(descriptor.ledger)
                .await
                .map(|resolved| resolved.route.leader)
                .unwrap_or(0);
        }
        if known == 0 || known == node {
            known = descriptor.active.placement.preferred_leader;
        }
        SessionDriver::Remote { leader: known }
    }
}

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
        let Some(authority) = root.authority() else {
            return Ok(None);
        };
        let Some(grant) = authority.groups.get(&descriptor.log_group) else {
            return Ok(None);
        };
        let driver = self.session_driver(handles, descriptor).await;
        if driver.local().is_none() {
            self.claim_for_ranges(descriptor, handles).await;
        }
        let facts = self.session_facts(pool, &driver, descriptor).await?;
        let membership = &facts.membership;
        let configuration = &membership.configuration;
        // A configuration the log committed but the root has not installed
        // fences every fact under it; install it first. The grant also
        // follows a member re-granted at a new generation (a drain or an
        // undrain, 24 §19) so every seat names the generation the node
        // signs at; the epoch moves only when the voting nodes change.
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
        let voters = members(&configuration.voters)?;
        let outgoing_voters = members(&configuration.voters_outgoing)?;
        let learners = members(&configuration.learners)?;
        if voters != grant.voters
            || outgoing_voters != grant.outgoing_voters
            || learners != grant.learners
        {
            let Some(receipt) = membership.latest.as_ref() else {
                return Ok(None);
            };
            if receipt.configuration != *configuration {
                return Ok(None);
            }
            let voters_changed = !same_members(&configuration.voters, &grant.voters)
                || !same_members(&configuration.voters_outgoing, &grant.outgoing_voters);
            let next = GroupAuthorityGrant {
                group: grant.group,
                genesis: grant.genesis,
                scope: grant.scope.clone(),
                membership_epoch: grant
                    .membership_epoch
                    .checked_add(u64::from(voters_changed))
                    .ok_or(AgentError::Capacity)?,
                voters,
                outgoing_voters,
                learners,
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
        // Range movement, balancing and holder publication read and drive
        // the movement map on the log's own replica; they run where this
        // node leads the session's log.
        if let Some(host) = driver.local() {
            if let Some(step) = self
                .drive_movement(pool, descriptor, authority, host)
                .await?
            {
                return Ok(Some(step));
            }
            if let Some(step) = self.drive_balance(descriptor, host).await? {
                return Ok(Some(step));
            }
            if let Some(step) = self
                .publish_holders(handles, descriptor, snapshot, installed, host, now)
                .await?
            {
                return Ok(Some(step));
            }
        }
        match &descriptor.pending {
            Some(plan) => {
                self.drive_plan(
                    handles, pool, descriptor, plan, snapshot, installed, grant, &facts, &driver,
                    now,
                )
                .await
            }
            None => {
                self.retire_and_heal(
                    handles, pool, descriptor, directory, snapshot, installed, &facts, &driver, now,
                )
                .await
            }
        }
    }
    /// Drive a session's range movement (25 §6) on the node that leads it:
    /// answer the operator's move requests with a `Begin`, then carry the
    /// pending transfer through its seed, barrier, the holders' readiness
    /// and seals (each stated over the holder's authenticated connection,
    /// verified against this replica's own digest of the frozen member and
    /// attested), activation, and the cleanup of the retired map. Every step
    /// is one proposal per pass; a refusal is retried on the next.
    async fn drive_movement(
        &mut self,
        pool: &PeerConnectionPool,
        descriptor: &SessionDescriptor,
        authority: &focal_directory::AuthorityCheckpoint,
        host: &ReplicaHost,
    ) -> Result<Option<AgentStep>, AgentError> {
        use crate::fleet::{RangeFact, RangeFactRequest};
        use focal_ranges::{RangeOperation, RecoveryProof, ReplicaId};
        // The operator's pending moves for this session.
        let mut position = 0;
        while let Some(job) = self.move_requests.get(position) {
            if job.ledger != descriptor.ledger {
                position = position.saturating_add(1);
                continue;
            }
            let job = self.move_requests.swap_remove(position);
            let generation = authority
                .nodes
                .get(&job.node)
                .map(|grant| grant.enrollment.generation);
            // A move outside the session's residency is refused before any
            // byte moves (24 §22).
            let residency = &descriptor.active.policy.residency;
            let region = authority
                .nodes
                .get(&job.node)
                .map_or(focal_directory::RegionId::UNKNOWN, |grant| {
                    grant.enrollment.region
                });
            if !residency.is_empty() && !residency.contains(&region) {
                let _ = job.reply.send(Err(AgentError::Residency(
                    crate::placement_executor::ExecutorError::OutsideResidency {
                        node: job.node,
                        region,
                    },
                )));
                continue;
            }
            crate::fault::hit(crate::fault::FaultSite::MovementBegin);
            let result = match generation {
                None => Err(AgentError::Identity),
                Some(generation) => host
                    .move_range(
                        job.member,
                        ReplicaId {
                            node: job.node,
                            generation,
                        },
                    )
                    .await
                    .map_err(AgentError::from),
            };
            let _ = job.reply.send(result);
        }
        let view = match host.range_view(false).await {
            Ok(view) => view,
            // Not a native session, or nothing to move.
            Err(_) => return Ok(None),
        };
        if view.in_flight {
            return Ok(None);
        }
        let Some(pending) = view.pending.as_ref() else {
            // Cleanup of the oldest retired map once nothing pins it.
            let Some(old) = view.history.first() else {
                return Ok(None);
            };
            let verifier = focal_ledger::LedgerRangeVerifier::new(view.genesis);
            let mut recovery = RecoveryProof {
                ledger: descriptor.ledger,
                epoch: view.epoch,
                through: view.prefix,
                manifest: old.proofs,
                attestation: ContentHash([0; 32]),
            };
            verifier
                .attest_recovery(&mut recovery)
                .map_err(|error| AgentError::Ledger(LedgerError::Native(error)))?;
            crate::fault::hit(crate::fault::FaultSite::MovementCleanup);
            return match host
                .propose_range(RangeOperation::Cleanup {
                    operation: old.operation,
                    recovery,
                })
                .await
            {
                Ok(()) => Ok(Some(AgentStep::Advanced)),
                // Pinned or busy: try again later.
                Err(_) => Ok(None),
            };
        };
        let operation = pending.operation;
        let group = descriptor.ledger.session.0;
        // Seeds: for a log-tailing holder the log is the seed; the record
        // names the member's rows digest at this prefix as its identity.
        if let Some(target) = pending
            .replacements
            .iter()
            .find(|target| target.holder.is_some() && !pending.snapshots.contains(&target.id))
        {
            let view = host.range_view(true).await.map_err(AgentError::from)?;
            let hash = view
                .members
                .iter()
                .find(|member| member.start == target.start && member.end == target.end)
                .and_then(|member| member.digest)
                .ok_or(AgentError::Identity)?;
            crate::fault::hit(crate::fault::FaultSite::MovementSeed);
            return match host
                .propose_range(RangeOperation::Snapshot {
                    operation,
                    range: target.id,
                    hash,
                })
                .await
            {
                Ok(()) => Ok(Some(AgentStep::Advanced)),
                Err(_) => Ok(None),
            };
        }
        let Some(barrier) = pending.barrier else {
            crate::fault::hit(crate::fault::FaultSite::MovementBarrier);
            return match host
                .propose_range(RangeOperation::Barrier { operation })
                .await
            {
                Ok(()) => Ok(Some(AgentStep::Advanced)),
                // Candidates pending: the barrier waits for them.
                Err(_) => Ok(None),
            };
        };
        let _ = barrier;
        let verifier = focal_ledger::LedgerRangeVerifier::new(view.genesis);
        let digests = host.range_view(true).await.map_err(AgentError::from)?;
        // Readiness of every replica-held replacement, then the seal of every
        // replica-held source, each from its holder.
        let ask = |node: u64, fact: RangeFactRequest| {
            let request = focal_wire::RequestEnvelope {
                protocol: focal_wire::PROTOCOL_VERSION,
                ledger: descriptor.ledger,
                route_epoch: descriptor.route_epoch,
                request_epoch: focal_model::RequestEpoch(1),
                request_id: focal_model::RequestId::from_u128(u128::from(node) | (1u128 << 64)),
                operation: focal_wire::Operation::RangeControl {
                    group,
                    request: postcard::to_stdvec(&crate::fleet::RangeControlRequest {
                        schema: crate::fleet::RANGE_CONTROL_SCHEMA,
                        fact,
                    })
                    .unwrap_or_default(),
                },
            };
            async move {
                let bytes = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    pool.send_placement(node, &request),
                )
                .await
                .ok()?
                .ok()?;
                match postcard::from_bytes::<crate::fleet::RangeControlReply>(&bytes).ok()? {
                    crate::fleet::RangeControlReply::Fact(fact) => Some(*fact),
                    crate::fleet::RangeControlReply::Refused(_) => None,
                }
            }
        };
        if let Some((target, holder)) = pending
            .replacements
            .iter()
            .filter(|target| !pending.ready.contains(&target.id))
            .find_map(|target| target.holder.map(|holder| (target, holder)))
        {
            let Some(fact) = ask(
                holder.node,
                RangeFactRequest::Ready {
                    operation,
                    range: target.id,
                },
            )
            .await
            else {
                return Ok(None);
            };
            let step = crate::fleet::verify_fact(&verifier, &digests, holder.node, fact)?;
            crate::fault::hit(crate::fault::FaultSite::MovementReady);
            return match host.propose_range(step).await {
                Ok(()) => Ok(Some(AgentStep::Advanced)),
                Err(_) => Ok(None),
            };
        }
        if let Some((source, holder)) = pending
            .sources
            .iter()
            .filter(|source| !pending.seals.contains(source))
            .find_map(|source| {
                digests
                    .members
                    .iter()
                    .find(|member| member.id == *source)
                    .and_then(|member| member.holder)
                    .map(|holder| (source, holder))
            })
        {
            let Some(fact) = ask(
                holder.node,
                RangeFactRequest::Seal {
                    operation,
                    range: *source,
                },
            )
            .await
            else {
                return Ok(None);
            };
            let step = crate::fleet::verify_fact(&verifier, &digests, holder.node, fact)?;
            crate::fault::hit(crate::fault::FaultSite::MovementSeal);
            return match host.propose_range(step).await {
                Ok(()) => Ok(Some(AgentStep::Advanced)),
                Err(_) => Ok(None),
            };
        }
        // Progress of every replica-held member that stays, then activation.
        let mut unchanged = Vec::new();
        unchanged
            .try_reserve_exact(digests.members.len())
            .map_err(|_| AgentError::Capacity)?;
        for member in digests
            .members
            .iter()
            .filter(|member| !pending.sources.contains(&member.id))
        {
            let Some(holder) = member.holder else {
                continue;
            };
            let Some(RangeFact::Progress(progress)) =
                ask(holder.node, RangeFactRequest::Progress { range: member.id }).await
            else {
                return Ok(None);
            };
            unchanged.push(crate::fleet::verify_progress(
                &verifier,
                &digests,
                holder.node,
                progress,
            )?);
        }
        crate::fault::hit(crate::fault::FaultSite::MovementActivate);
        match host.activate_range(unchanged).await {
            Ok(()) => Ok(Some(AgentStep::Advanced)),
            // A proof still missing or a candidate pending: the next pass.
            Err(_) => Ok(None),
        }
    }
    /// Publish the committed map's members and holders to the directory
    /// (25 §9): one change per range epoch once no transfer is pending, so
    /// the directory names every holder the session's log does.
    async fn publish_holders(
        &mut self,
        handles: &NetworkHandles,
        descriptor: &SessionDescriptor,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        host: &ReplicaHost,
        now: i64,
    ) -> Result<Option<AgentStep>, AgentError> {
        let Ok(view) = host.range_view(false).await else {
            return Ok(None);
        };
        if view.pending.is_some() {
            return Ok(None);
        }
        let epoch = view.epoch.0;
        if descriptor
            .holders
            .as_ref()
            .is_some_and(|holders| holders.epoch >= epoch)
        {
            return Ok(None);
        }
        let mut members = Vec::new();
        members
            .try_reserve_exact(view.members.len())
            .map_err(|_| AgentError::Capacity)?;
        for member in &view.members {
            members.push(focal_directory::RangeHolder {
                member: member.id,
                start: member.start,
                node: member.holder.map(|replica| replica.node),
                generation: member.holder.map(|replica| replica.generation),
            });
        }
        let command = self.session_command(
            descriptor,
            snapshot,
            installed,
            now,
            SessionChange::Holders {
                holders: focal_directory::RangeHolders { epoch, members },
            },
            None,
        )?;
        self.intend_partition(handles, command).await.map(Some)
    }
    /// Reshape a session's group from its measured members (25 §8): the
    /// balancer's decision, when it makes one, is a split of a member near
    /// its middle or a merge of two adjacent members, proposed as a layout
    /// record; a refusal (a transfer pending, candidates in flight, no
    /// dividing affinity) is observed again on a later pass.
    async fn drive_balance(
        &mut self,
        descriptor: &SessionDescriptor,
        host: &ReplicaHost,
    ) -> Result<Option<AgentStep>, AgentError> {
        use crate::range_balancer::{Decision, MemberLoad};
        let Ok(view) = host.range_view(false).await else {
            return Ok(None);
        };
        if view.pending.is_some() || view.in_flight {
            return Ok(None);
        }
        let loads: Vec<MemberLoad> = view
            .members
            .iter()
            .map(|member| MemberLoad {
                id: member.id,
                entries: member.entries,
            })
            .collect();
        let Some(decision) = self.balancer.observe(
            descriptor.ledger,
            &loads,
            focal_ranges::RangeLimits::default().max_ranges,
        ) else {
            return Ok(None);
        };
        let operation = match decision {
            Decision::Split { member } => {
                let Some(at) = host.split_point(member).await.unwrap_or(None) else {
                    return Ok(None);
                };
                let mut hasher = blake3::Hasher::new_derive_key("focal.range.split.member.v1");
                hasher.update(&descriptor.ledger.tenant.0);
                hasher.update(&descriptor.ledger.session.0);
                hasher.update(&member.0.to_le_bytes());
                hasher.update(&at);
                hasher.update(&view.epoch.0.to_le_bytes());
                let mut bytes = [0u8; 16];
                for (target, source) in bytes.iter_mut().zip(hasher.finalize().as_bytes()) {
                    *target = *source;
                }
                let id = focal_memory::RangeId(u128::from_le_bytes(bytes));
                if id.0 == 0 || view.members.iter().any(|existing| existing.id == id) {
                    return Ok(None);
                }
                focal_ledger::LayoutOperation::Split { at, id }
            }
            Decision::Merge { left } => focal_ledger::LayoutOperation::Merge { left },
        };
        match host.propose_layout(operation).await {
            Ok(()) => Ok(Some(AgentStep::Advanced)),
            Err(_) => Ok(None),
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
    /// Apply one session-log configuration change exactly once, through
    /// `driver`.
    async fn change_membership(
        &mut self,
        pool: &PeerConnectionPool,
        driver: &SessionDriver,
        descriptor: &SessionDescriptor,
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
        let request = SessionMembershipRequest {
            id,
            expected_index: facts.membership.configuration_index,
            expected: facts.membership.configuration.clone(),
            change,
        };
        match driver {
            SessionDriver::Local(host) => match host.change_membership(request).await {
                Ok(_) => Ok(Some(AgentStep::Advanced)),
                Err(LedgerError::Consensus(ConsensusError::LearnerBehind)) => {
                    Err(AgentError::Behind)
                }
                Err(error) => Err(error.into()),
            },
            SessionDriver::Remote { leader } => {
                match self
                    .session_call(pool, *leader, descriptor, SessionCall::Membership(request))
                    .await?
                {
                    SessionControlReply::Membership(_) => Ok(Some(AgentStep::Advanced)),
                    _ => Err(AgentError::Identity),
                }
            }
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
        driver: &SessionDriver,
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
                        handles, pool, descriptor, plan, snapshot, installed, grant, facts, driver,
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
                        pool,
                        driver,
                        descriptor,
                        facts,
                        membership_id(descriptor.ledger, plan.operation, voter, CHANGE_ADD_LEARNER),
                        MembershipChange::AddLearner { node: voter },
                    )
                    .await;
            }
            if member && !is_voter && progress.phase.at_least(AssignmentPhase::CaughtUp) {
                return self
                    .change_membership(
                        pool,
                        driver,
                        descriptor,
                        facts,
                        membership_id(descriptor.ledger, plan.operation, voter, CHANGE_PROMOTE),
                        MembershipChange::Promote { node: voter },
                    )
                    .await;
            }
        }
        // Every desired voter votes in the log; a current voter the plan
        // drops (a drained or dead host, the founder a larger fleet leaves
        // out) keeps its vote until activation retires it (24 §4, §19), so
        // the old contract holds through the transition.
        if !plan
            .desired
            .placement
            .voters
            .keys()
            .all(|voter| configuration.voters.contains(voter))
        {
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
            self.session_placement(pool, driver, descriptor, request)
                .await?;
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
        driver: &SessionDriver,
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
            self.session_placement(pool, driver, descriptor, request)
                .await?;
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
        pool: &PeerConnectionPool,
        descriptor: &SessionDescriptor,
        directory: &PartitionCheckpoint,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        facts: &HostedSessionFacts,
        driver: &SessionDriver,
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
                                pool,
                                driver,
                                descriptor,
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
        // live registry; otherwise plan again under the same policy. The check
        // is a pure function of the descriptor's active placement and the node
        // set, so skip it while neither has moved since it last passed.
        let config = self.partition_config();
        let fingerprint = (descriptor.authority.record_hash, directory.revision);
        if self.verified_active.get(&descriptor.ledger) == Some(&fingerprint) {
            return Ok(None);
        }
        if focal_directory::verify_placement(
            &descriptor.active,
            &directory.nodes,
            config.max_members,
        )
        .is_ok()
        {
            self.verified_active.insert(descriptor.ledger, fingerprint);
            return Ok(None);
        }
        self.verified_active.remove(&descriptor.ledger);
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
        match focal_directory::propose_placement_keeping(
            &directory.nodes,
            &descriptor.active.policy,
            &descriptor.active.placement.voters,
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

/// What the controller would do next for one session, from the committed
/// directory alone: the operator's `cluster plan`. Never executes anything.
pub(crate) fn planned_actions(
    descriptor: &SessionDescriptor,
    nodes: &std::collections::BTreeMap<u64, focal_directory::NodeRecord>,
) -> Vec<String> {
    let mut actions = Vec::new();
    let mut push = |action: String| {
        if actions.len() < 32 && actions.try_reserve(1).is_ok() {
            actions.push(action);
        }
    };
    match &descriptor.pending {
        None => {
            if let Ok(report) = focal_directory::effective_guarantee(descriptor, nodes)
                && report.achieved != Some(report.desired)
            {
                push(format!(
                    "replan under the active policy: {} of {} promised failures achieved{}",
                    report
                        .achieved
                        .map_or("none".to_owned(), |achieved| achieved
                            .max_failures
                            .to_string()),
                    report.desired.max_failures,
                    report
                        .blocked_by
                        .first()
                        .map(|blocker| format!(" ({:?})", blocker.reason))
                        .unwrap_or_default()
                ));
            }
            for node in descriptor.retiring.keys() {
                push(format!(
                    "drain and retire the abandoned copy on node {node}"
                ));
            }
        }
        Some(plan) => match plan.phase {
            PlacementPhase::Planned => {
                push(format!(
                    "begin preparation of plan {}",
                    hex(&plan.operation.0)
                ));
            }
            PlacementPhase::Cutover => push("activate the cut-over placement".to_owned()),
            PlacementPhase::Failed => push("abort the failed plan and replan".to_owned()),
            PlacementPhase::Preparing
            | PlacementPhase::Catchup
            | PlacementPhase::Custody
            | PlacementPhase::Promoting => {
                let mut all_promoted = !plan.desired.placement.voters.is_empty();
                for progress in plan.progress.values() {
                    let voter = plan.desired.placement.voters.contains_key(&progress.node);
                    match progress.phase {
                        AssignmentPhase::Assigned => {
                            push(format!("install the copy on node {}", progress.node));
                        }
                        AssignmentPhase::Installed if voter => {
                            push(format!("add node {} as a learner", progress.node));
                        }
                        AssignmentPhase::CaughtUp if voter => {
                            push(format!("promote node {} to voter", progress.node));
                        }
                        AssignmentPhase::Failed => push(format!(
                            "node {} refused its assignment{}",
                            progress.node,
                            progress
                                .refusal
                                .map(|code| format!(" ({code:?})"))
                                .unwrap_or_default()
                        )),
                        _ => {}
                    }
                    if voter
                        && !matches!(
                            progress.phase,
                            AssignmentPhase::Promoted | AssignmentPhase::Active
                        )
                    {
                        all_promoted = false;
                    }
                }
                for node in plan.desired.placement.voters.keys() {
                    if !plan.progress.contains_key(node) {
                        all_promoted = false;
                        push(format!("assign node {node} its copy"));
                    }
                }
                if all_promoted {
                    push("cut over the session log to the new placement".to_owned());
                }
            }
        },
    }
    actions
}
fn hex(bytes: &[u8]) -> String {
    let mut text = String::new();
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(text, "{byte:02x}");
    }
    text
}
