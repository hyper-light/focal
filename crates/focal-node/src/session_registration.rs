//! Descriptive first-session registration, derived from the existing session.
//! These helpers do not install authority or submit mutations. The composition
//! owner journals each complete intent before proposal; the destination control
//! owner independently checks its installed authority when applying it.
use crate::{
    config::{Durability, FailureDomain, Placement as ConfigPlacement, Settings},
    control_host::RootObservation,
    directory_bootstrap::FirstDirectoryPlan,
    embedded::NodeIdentity,
    network_state::NetworkGenesis,
    placement_proof::AccountedAuthorityProof,
};
use focal_control::{
    ControlAuthoritySnapshot, ControlBootstrap, ControlCommand, ControlEvidence, ControlIdentity,
    ControlScope, ControlSnapshot, VerifiedPartitionCommand,
};
use focal_directory::{
    AuthorityAnchor, AuthorityCheckpoint, AuthorityCommand, AuthorityConfig, AuthorityFact,
    AuthorityOperation, AuthorityRegistry, AuthorityVerifier, DurabilityIntent, FailureClass,
    GroupAuthorityGrant, GroupScope, LogGroupId, NamespaceRange, NodeRecord, NodeTopologyGrant,
    OperationId, PartitionCheckpoint, PartitionCommand, PartitionOperation, Placement,
    PlacementPolicy, PlacementSpec, RegionId, RootCheckpoint, SessionFenceKind,
};
use focal_enrollment::{EnrollmentLimits, EnrollmentRegistry, EnrollmentRole, server_fingerprint};
use focal_ledger::{CommittedPlacement, MembershipView, Session, SessionPlacementRequest};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{ContentHash, LedgerId, RouteEpoch, SessionSeq};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const MAX_POLICY_BYTES: usize = 64 * 1024;
const MAX_AUTHORITY_BYTES: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum SessionRegistrationError {
    #[error("session registration does not match the original founder or committed authority")]
    Unauthorized,
    #[error("session registration needs current enrollment, topology, or authority metadata")]
    NotReady,
    #[error("the actual founder replica cannot satisfy the inherited deployment policy")]
    PolicyUnsatisfied,
    #[error("session registration conflicts with an existing placement or directory entry")]
    Conflict,
    #[error("session registration exceeds its bounded allowance")]
    Capacity,
    #[error("session owner: {0}")]
    Ledger(#[from] focal_ledger::LedgerError),
    #[error("directory authority: {0}")]
    Directory(#[from] focal_directory::DirectoryError),
}
type Result<T> = std::result::Result<T, SessionRegistrationError>;

/// The facts a registration reads from a session, whether it still owns the
/// `Session` or the fleet hosts it. Read on the owner thread so every field
/// describes one applied prefix; nothing here is accepted from a peer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostedSessionFacts {
    pub cluster: [u8; 16],
    pub ledger: LedgerId,
    pub node: u64,
    pub group: LogGroupId,
    pub genesis: ContentHash,
    pub memory_limit: u64,
    pub membership: MembershipView,
    /// The latest placement record, which may be a pending cutover.
    pub placement: Option<focal_directory::SessionFence>,
    /// The activated placement and its members.
    pub active: Option<(focal_directory::SessionFence, PlacementSpec)>,
    pub sequence: SessionSeq,
    pub authoritative: bool,
}
impl HostedSessionFacts {
    pub fn from_session(session: &Session) -> Result<Self> {
        let status = session.status();
        Ok(Self {
            cluster: session.cluster_id(),
            ledger: session.ledger(),
            node: status.node_id,
            group: LogGroupId(session.group_id()),
            genesis: session.placement_genesis()?,
            memory_limit: u64::try_from(session.memory_stats().limit)
                .map_err(|_| SessionRegistrationError::Capacity)?,
            membership: session.membership()?,
            placement: session.placement(),
            active: session
                .active_fence()
                .cloned()
                .zip(session.active_placement().cloned()),
            sequence: session.sequence(),
            authoritative: session.is_authoritative(),
        })
    }
}

/// Captured before transferring the actual Session into its fleet owner. There
/// is no decoder or constructor accepting claimed Raft coordinates. Application
/// identities and raw deployment constraints survive the local-to-network step.
pub struct FirstSessionPlan {
    founder: NodeIdentity,
    root: ControlIdentity,
    group: LogGroupId,
    genesis: ContentHash,
    configuration_index: u64,
    operation: OperationId,
    client: [u8; 16],
    durability: Durability,
    placement: ConfigPlacement,
    required_memory: u64,
    _allocation: Allocation,
}
impl FirstSessionPlan {
    pub fn capture(
        session: &Session,
        network: &NetworkGenesis,
        settings: &Settings,
        required_memory: u64,
        budget: &MemoryBudget,
    ) -> Result<Self> {
        let facts = HostedSessionFacts::from_session(session)?;
        Self::capture_facts(&facts, network, settings, required_memory, budget)
    }
    /// `capture` over facts exported by a hosted replica of the founder's
    /// own session.
    pub fn capture_facts(
        facts: &HostedSessionFacts,
        network: &NetworkGenesis,
        settings: &Settings,
        required_memory: u64,
        budget: &MemoryBudget,
    ) -> Result<Self> {
        Self::capture_hosted(
            facts,
            network,
            &network.founder,
            settings,
            required_memory,
            budget,
        )
    }
    /// `capture_facts` for any session a node hosts alone: `host` is the
    /// node's identity at the session's ledger (the founder's own identity
    /// for the founder session). The plan registers the session under the
    /// hosting node's committed enrollment exactly as the founder's was
    /// ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md) §16).
    pub fn capture_hosted(
        facts: &HostedSessionFacts,
        network: &NetworkGenesis,
        host: &NodeIdentity,
        settings: &Settings,
        required_memory: u64,
        budget: &MemoryBudget,
    ) -> Result<Self> {
        let bytes = serialized_size(&(&settings.durability, &settings.placement))?;
        if bytes > MAX_POLICY_BYTES {
            return Err(SessionRegistrationError::Capacity);
        }
        let allocation = reserve(budget, bytes)?;
        settings
            .validate()
            .map_err(|_| SessionRegistrationError::PolicyUnsatisfied)?;
        // Validation of the bounded immutable genesis is admitted separately;
        // it must never turn an arbitrary current enrollment into a founder.
        let genesis_bytes = serialized_size(network)?;
        if genesis_bytes > MAX_POLICY_BYTES {
            return Err(SessionRegistrationError::Capacity);
        }
        let _genesis = reserve(budget, genesis_bytes)?;
        network
            .validate(&network.founder)
            .map_err(|_| SessionRegistrationError::Unauthorized)?;
        if host.cluster != network.founder.cluster
            || host.root != network.founder.root
            || host.node == 0
            || host.ledger.tenant.is_zero()
            || host.ledger.session.0 == [0; 16]
        {
            return Err(SessionRegistrationError::Unauthorized);
        }
        let founder = host;
        let membership = &facts.membership;
        if facts.cluster != founder.cluster
            || facts.ledger != founder.ledger
            || facts.node != founder.node
            || membership.configuration.voters != [founder.node]
            || !membership.configuration.learners.is_empty()
            || !membership.configuration.voters_outgoing.is_empty()
            || !membership.configuration.learners_next.is_empty()
            || membership.configuration.auto_leave
            || required_memory < facts.memory_limit
        {
            return Err(SessionRegistrationError::Unauthorized);
        }
        let group = facts.group;
        let genesis = facts.genesis;
        let operation = OperationId(stable_id(
            "focal.session.first-registration-operation.v1",
            founder,
            group,
            genesis,
        )?);
        if facts.placement.as_ref().is_some_and(|fence| {
            fence.kind != SessionFenceKind::Created || fence.operation != operation
        }) {
            return Err(SessionRegistrationError::Conflict);
        }
        Ok(Self {
            founder: founder.clone(),
            root: network.root,
            group,
            genesis,
            configuration_index: membership.configuration_index,
            operation,
            client: stable_id(
                "focal.session.first-registration-client.v1",
                founder,
                group,
                genesis,
            )?,
            durability: settings.durability.clone(),
            placement: settings.placement.clone(),
            required_memory,
            _allocation: allocation,
        })
    }
    pub fn founder(&self) -> &NodeIdentity {
        &self.founder
    }
    pub fn ledger(&self) -> LedgerId {
        self.founder.ledger
    }
    pub fn operation(&self) -> OperationId {
        self.operation
    }
    /// Separate per-owner request streams may use the same stable client ID.
    /// Sequence selection and exact unknown-outcome recovery belong to the
    /// durable controller journal, never to an ambient clock or this helper.
    pub fn client(&self) -> [u8; 16] {
        self.client
    }
    pub fn prepare(
        &self,
        observation: &RootObservation,
        now: i64,
        budget: &MemoryBudget,
    ) -> Result<InitialSessionIntent> {
        let snapshot = observation.snapshot();
        if snapshot.identity != self.root || snapshot.applied_index == 0 {
            return Err(SessionRegistrationError::Unauthorized);
        }
        let ControlBootstrap::Root {
            directory,
            enrollment,
        } = &snapshot.state
        else {
            return Err(SessionRegistrationError::Unauthorized);
        };
        let checkpoint = observation
            .authority()
            .ok_or(SessionRegistrationError::NotReady)?;
        let context = AuthorityContext::load(
            checkpoint,
            enrollment,
            self.root,
            snapshot.applied_index,
            now,
            budget,
        )?;
        let node = context.founder(&self.founder, now)?;
        let mut allocation = reserve(budget, MAX_POLICY_BYTES)?;
        let members = BTreeMap::from([(self.founder.node, node.enrollment.generation)]);
        let spec = PlacementSpec {
            policy: PlacementPolicy {
                durability: DurabilityIntent {
                    survive: match self.durability.survive {
                        FailureDomain::Node => FailureClass::Node,
                        FailureDomain::Zone => FailureClass::Zone,
                        FailureDomain::Region => FailureClass::Region,
                    },
                    max_failures: self.durability.max_failures,
                },
                residency: resolve_regions(&self.placement.residency, directory)?,
                home_regions: resolve_regions(&self.placement.home_regions, directory)?,
                required_memory: self.required_memory,
            },
            placement: Placement {
                voters: members.clone(),
                materializers: members.clone(),
                content_copies: members.clone(),
                preferred_leader: self.founder.node,
            },
        };
        focal_directory::verify_placement(
            &spec,
            &BTreeMap::from([(
                self.founder.node,
                NodeRecord {
                    enrollment: node.enrollment.clone(),
                    load: None,
                    liveness: None,
                },
            )]),
            1,
        )
        .map_err(|_| SessionRegistrationError::PolicyUnsatisfied)?;
        let request = SessionPlacementRequest {
            expected_index: 0,
            expected_configuration_index: self.configuration_index,
            operation: self.operation,
            kind: SessionFenceKind::Created,
            from_route: RouteEpoch(0),
            to_route: RouteEpoch(1),
            membership_epoch: 1,
            placement_epoch: 1,
            placement: spec,
        };
        request.validate()?;
        let grant = GroupAuthorityGrant {
            group: self.group,
            genesis: self.genesis,
            scope: GroupScope::Session(self.ledger()),
            membership_epoch: 1,
            voters: members,
            outgoing_voters: BTreeMap::new(),
            learners: BTreeMap::new(),
            expires_at: node.expires_at,
        };
        let root_command = match checkpoint.groups.get(&self.group) {
            Some(existing) => {
                check_group(existing, &grant, now)?;
                None
            }
            None => Some(ControlCommand::Authority(AuthorityCommand {
                expected_revision: checkpoint.revision,
                enrollment_revision: context.enrollment.revision(),
                decided_at: now,
                operation: AuthorityOperation::BootstrapGroup { grant },
            })),
        };
        allocation
            .shrink_to(charge(serialized_size(&(&request, &root_command))?)?)
            .map_err(|_| SessionRegistrationError::Capacity)?;
        Ok(InitialSessionIntent {
            founder: self.founder.clone(),
            root: self.root,
            group: self.group,
            genesis: self.genesis,
            request,
            root_command,
            _allocation: allocation,
        })
    }
}

pub struct InitialSessionIntent {
    founder: NodeIdentity,
    root: ControlIdentity,
    group: LogGroupId,
    genesis: ContentHash,
    request: SessionPlacementRequest,
    root_command: Option<ControlCommand>,
    _allocation: Allocation,
}
pub struct AccountedRegistrationCommand {
    command: ControlCommand,
    _allocation: Allocation,
}
impl AccountedRegistrationCommand {
    pub fn command(&self) -> &ControlCommand {
        &self.command
    }
    /// A caller moving the command into its journal/queue retains this allowance
    /// until its own admitted representation takes ownership of the bytes.
    pub fn into_parts(self) -> (ControlCommand, Allocation) {
        (self.command, self._allocation)
    }
}
impl InitialSessionIntent {
    pub fn placement_request(&self) -> &SessionPlacementRequest {
        &self.request
    }
    pub fn root_command(&self) -> Option<&ControlCommand> {
        self.root_command.as_ref()
    }
    /// Enroll only the exact founder topology already installed from root.
    /// Application session creation is a separate subsequent partition command.
    pub fn partition_enrollment(
        &self,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        now: i64,
        budget: &MemoryBudget,
    ) -> Result<Option<AccountedRegistrationCommand>> {
        let (partition, context) = self.partition_context(snapshot, installed, now, budget)?;
        let node = context.founder(&self.founder, now)?;
        let expected_generation = match partition.nodes.get(&self.founder.node) {
            Some(existing) if existing.enrollment == node.enrollment => return Ok(None),
            Some(existing) if existing.enrollment.generation < node.enrollment.generation => {
                Some(existing.enrollment.generation)
            }
            Some(_) => return Err(SessionRegistrationError::Conflict),
            None => None,
        };
        if expected_generation.unwrap_or(0).checked_add(1) != Some(node.enrollment.generation) {
            return Err(SessionRegistrationError::NotReady);
        }
        let allocation = reserve(budget, serialized_size(&node.enrollment)?)?;
        Ok(Some(AccountedRegistrationCommand {
            command: ControlCommand::VerifiedPartition(VerifiedPartitionCommand {
                command: PartitionCommand {
                    expected_revision: partition.revision,
                    delegation_epoch: partition.delegation.epoch,
                    operation: PartitionOperation::Enroll {
                        node: node.enrollment.clone(),
                        expected_generation,
                    },
                },
                evidence: evidence(installed, &context.enrollment, now),
            }),
            _allocation: allocation,
        }))
    }
    /// Consume a signed share produced from an actual session-owner witness.
    /// Both metadata versions must be the same directory prefix. There is no
    /// fallback to an unsigned fact or caller-declared aggregate voter list.
    pub fn create_session(
        &self,
        snapshot: &ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        witness: &CommittedPlacement,
        proof: AccountedAuthorityProof,
        now: i64,
        budget: &MemoryBudget,
    ) -> Result<Option<AccountedRegistrationCommand>> {
        let (partition, context) = self.partition_context(snapshot, installed, now, budget)?;
        let node = context.founder(&self.founder, now)?;
        if partition
            .nodes
            .get(&self.founder.node)
            .is_none_or(|existing| existing.enrollment != node.enrollment)
        {
            return Err(SessionRegistrationError::NotReady);
        }
        let fence = witness.fence();
        if witness.cluster() != self.founder.cluster
            || witness.node() != self.founder.node
            || witness.genesis() != self.genesis
            || witness.placement() != &self.request.placement
            || witness.configuration().voters != [self.founder.node]
            || !witness.configuration().learners.is_empty()
            || !witness.configuration().voters_outgoing.is_empty()
            || !witness.configuration().learners_next.is_empty()
            || witness.configuration().auto_leave
            || fence.ledger != self.founder.ledger
            || fence.log_group != self.group
            || fence.operation != self.request.operation
            || fence.kind != SessionFenceKind::Created
            || fence.from_route != RouteEpoch(0)
            || fence.to_route != RouteEpoch(1)
            || fence.membership_epoch != 1
            || fence.placement_epoch != 1
            || proof.proof().statement.fact != AuthorityFact::Session(fence.clone())
        {
            return Err(SessionRegistrationError::Unauthorized);
        }
        context
            .authority
            .verifier(
                &context.enrollment,
                std::slice::from_ref(proof.proof()),
                now,
            )?
            .verify_session_fence(fence)?;
        focal_directory::verify_placement(&self.request.placement, &partition.nodes, 1)?;
        if let Some(existing) = partition.sessions.get(&self.founder.ledger) {
            if existing.log_group == self.group
                && existing.active == self.request.placement
                && existing.authority == *fence
                && existing.route_epoch == RouteEpoch(1)
                && existing.membership_epoch == 1
                && existing.placement_epoch == 1
                && existing.pending.is_none()
            {
                return Ok(None);
            }
            return Err(SessionRegistrationError::Conflict);
        }
        let bytes = serialized_size(&(&self.request.placement, proof.proof()))?;
        let allocation = reserve(budget, bytes)?;
        let mut evidence = evidence(installed, &context.enrollment, now);
        evidence.proofs.push(proof.proof().clone());
        Ok(Some(AccountedRegistrationCommand {
            command: ControlCommand::VerifiedPartition(VerifiedPartitionCommand {
                command: PartitionCommand {
                    expected_revision: partition.revision,
                    delegation_epoch: partition.delegation.epoch,
                    operation: PartitionOperation::CreateSession {
                        ledger: self.founder.ledger,
                        log_group: self.group,
                        placement: self.request.placement.clone(),
                        authority: fence.clone(),
                    },
                },
                evidence,
            }),
            _allocation: allocation,
        }))
    }
    fn partition_context<'a>(
        &self,
        snapshot: &'a ControlSnapshot,
        installed: &ControlAuthoritySnapshot,
        now: i64,
        budget: &MemoryBudget,
    ) -> Result<(&'a PartitionCheckpoint, AuthorityContext)> {
        let plan = FirstDirectoryPlan::derive(self.founder.cluster, self.founder.node)
            .map_err(|_| SessionRegistrationError::Unauthorized)?;
        let ControlBootstrap::Partition { directory } = &snapshot.state else {
            return Err(SessionRegistrationError::Unauthorized);
        };
        if snapshot.identity
            != plan
                .identity()
                .map_err(|_| SessionRegistrationError::Unauthorized)?
            || installed.identity != snapshot.identity
            || installed.applied_index != snapshot.applied_index
            || snapshot.applied_index == 0
            || snapshot.revisions.partition != directory.revision
            || installed.source != self.root
            || installed.decided_at > now
            || directory.delegation != plan.delegation()
            || !directory.delegation.namespace.contains(self.founder.ledger)
            || directory.sealed.is_some()
        {
            return Err(SessionRegistrationError::Unauthorized);
        }
        let context = AuthorityContext::load(
            &installed.authority,
            &installed.enrollment,
            self.root,
            installed.source_index,
            now,
            budget,
        )?;
        let node = context.founder(&self.founder, now)?;
        let expected = GroupAuthorityGrant {
            group: self.group,
            genesis: self.genesis,
            scope: GroupScope::Session(self.founder.ledger),
            membership_epoch: 1,
            voters: self.request.placement.placement.voters.clone(),
            outgoing_voters: BTreeMap::new(),
            learners: BTreeMap::new(),
            expires_at: node.expires_at,
        };
        let group = context
            .authority
            .group(self.group)
            .ok_or(SessionRegistrationError::NotReady)?;
        check_group(group, &expected, now)?;
        if expected.voters.get(&self.founder.node) != Some(&node.enrollment.generation) {
            return Err(SessionRegistrationError::Conflict);
        }
        Ok((directory, context))
    }
}

struct AuthorityContext {
    authority: AuthorityRegistry,
    enrollment: EnrollmentRegistry,
    _allocation: Allocation,
}
impl AuthorityContext {
    fn load(
        checkpoint: &AuthorityCheckpoint,
        enrollment: &[u8],
        root: ControlIdentity,
        source_index: u64,
        now: i64,
        budget: &MemoryBudget,
    ) -> Result<Self> {
        let bytes = serialized_size(checkpoint)?
            .checked_add(enrollment.len())
            .ok_or(SessionRegistrationError::Capacity)?;
        if bytes > MAX_AUTHORITY_BYTES {
            return Err(SessionRegistrationError::Capacity);
        }
        let allocation = reserve(budget, bytes)?;
        let enrollment =
            EnrollmentRegistry::restore(enrollment, root.cluster.0, EnrollmentLimits::default())
                .map_err(|_| SessionRegistrationError::Unauthorized)?;
        let anchor = AuthorityAnchor {
            cluster: root.cluster,
            metadata_group: LogGroupId(root.group),
            genesis: ContentHash(root.genesis),
            namespace: NamespaceRange::all(),
            enrollment_ca: ContentHash(server_fingerprint(enrollment.ca_certificate())),
        };
        if root.scope != ControlScope::Root
            || source_index == 0
            || checkpoint.revision == 0
            || checkpoint.applied_index == 0
            || checkpoint.applied_index > source_index
            || enrollment.applied_index() > source_index
            || now < 0
            || checkpoint.clock > now
        {
            return Err(SessionRegistrationError::Unauthorized);
        }
        let authority = AuthorityRegistry::restore(
            checkpoint.clone(),
            &anchor,
            AuthorityConfig::default(),
            budget.clone(),
        )?;
        Ok(Self {
            authority,
            enrollment,
            _allocation: allocation,
        })
    }
    fn founder(&self, founder: &NodeIdentity, now: i64) -> Result<&NodeTopologyGrant> {
        let node = self
            .authority
            .node(founder.node)
            .ok_or(SessionRegistrationError::NotReady)?;
        if node.principal != founder.issuer.0 || node.expires_at <= now {
            return Err(SessionRegistrationError::Unauthorized);
        }
        self.authority
            .verifier(&self.enrollment, &[], now)?
            .verify_enrollment(&node.enrollment)?;
        let receipt = self
            .enrollment
            .enrollments()
            .find(|receipt| {
                receipt.identity.node_id == Some(founder.node)
                    && receipt.identity.principal == founder.issuer.0
                    && ContentHash(receipt.public_key) == node.enrollment.identity
            })
            .ok_or(SessionRegistrationError::Unauthorized)?;
        let identity = self
            .enrollment
            .authorize_certificate(&receipt.certificate, now)
            .map_err(|_| SessionRegistrationError::Unauthorized)?;
        if identity.role != EnrollmentRole::Node
            || !node.enrollment.eligible
            || receipt.expires_at < node.expires_at
        {
            return Err(SessionRegistrationError::Unauthorized);
        }
        Ok(node)
    }
}
/// Evidence naming the installed authority and enrollment revisions of a
/// partition owner, for commands that carry no proofs of their own or append
/// them afterwards. The enrollment bytes are decoded under `budget`.
pub(crate) fn control_evidence(
    installed: &ControlAuthoritySnapshot,
    cluster: [u8; 16],
    now: i64,
    budget: &MemoryBudget,
) -> Result<ControlEvidence> {
    if installed.enrollment.len() > MAX_AUTHORITY_BYTES {
        return Err(SessionRegistrationError::Capacity);
    }
    let _allocation = reserve(budget, installed.enrollment.len())?;
    let enrollment =
        EnrollmentRegistry::restore(&installed.enrollment, cluster, EnrollmentLimits::default())
            .map_err(|_| SessionRegistrationError::Unauthorized)?;
    Ok(evidence(installed, &enrollment, now))
}
fn evidence(
    installed: &ControlAuthoritySnapshot,
    enrollment: &EnrollmentRegistry,
    now: i64,
) -> ControlEvidence {
    ControlEvidence {
        authority_revision: installed.authority.revision,
        enrollment_revision: enrollment.revision(),
        decided_at: now,
        proofs: Vec::new(),
    }
}
fn check_group(
    existing: &GroupAuthorityGrant,
    expected: &GroupAuthorityGrant,
    now: i64,
) -> Result<()> {
    if existing.group != expected.group
        || existing.genesis != expected.genesis
        || existing.scope != expected.scope
        || existing.membership_epoch != expected.membership_epoch
        || existing.voters != expected.voters
        || !existing.outgoing_voters.is_empty()
        || !existing.learners.is_empty()
        || existing.expires_at <= now
        || existing.expires_at > expected.expires_at
    {
        return Err(SessionRegistrationError::Conflict);
    }
    Ok(())
}
fn resolve_regions(labels: &[String], root: &RootCheckpoint) -> Result<BTreeSet<RegionId>> {
    let mut regions = BTreeSet::new();
    for label in labels {
        let mut matches = root
            .regions
            .values()
            .filter(|region| region.label == *label);
        let region = matches.next().ok_or(SessionRegistrationError::NotReady)?;
        if region.id == RegionId::UNKNOWN || matches.next().is_some() {
            return Err(SessionRegistrationError::PolicyUnsatisfied);
        }
        regions.insert(region.id);
    }
    Ok(regions)
}
fn stable_id(
    domain: &'static str,
    founder: &NodeIdentity,
    group: LogGroupId,
    genesis: ContentHash,
) -> Result<[u8; 16]> {
    let mut hash = blake3::Hasher::new_derive_key(domain);
    for value in [
        founder.cluster,
        founder.ledger.tenant.0,
        founder.ledger.session.0,
        group.0,
        founder.root.0,
        founder.issuer.0,
    ] {
        hash.update(&value);
    }
    hash.update(&founder.node.to_be_bytes());
    hash.update(&genesis.0);
    let mut id = [0; 16];
    for (target, byte) in id.iter_mut().zip(hash.finalize().as_bytes()) {
        *target = *byte;
    }
    if id == [0; 16] {
        return Err(SessionRegistrationError::Unauthorized);
    }
    Ok(id)
}
fn serialized_size(value: &(impl Serialize + ?Sized)) -> Result<usize> {
    postcard::experimental::serialized_size(value).map_err(|_| SessionRegistrationError::Capacity)
}
fn reserve(budget: &MemoryBudget, encoded: usize) -> Result<Allocation> {
    budget
        .reserve(
            BudgetKind::Control,
            BudgetLane::Completion,
            charge(encoded)?,
        )
        .map_err(|_| SessionRegistrationError::Capacity)
        .map(|reservation| reservation.commit())
}
fn charge(encoded: usize) -> Result<usize> {
    encoded
        .checked_mul(64)
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .ok_or(SessionRegistrationError::Capacity)
}

#[cfg(test)]
#[path = "session_registration_tests.rs"]
mod tests;
