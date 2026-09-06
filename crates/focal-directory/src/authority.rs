//! Committed infrastructure assignments. Only the authenticated metadata owner
//! may propose these commands; certificate possession cannot grant topology.
use crate::*;
use focal_enrollment::{EnrollmentRegistry, EnrollmentRole, server_fingerprint};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget, OwnerId};
use focal_model::{ContentHash, LedgerId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityAnchor {
    pub cluster: ClusterId,
    pub metadata_group: LogGroupId,
    /// Immutable public genesis of the metadata authority that installs grants.
    pub genesis: ContentHash,
    pub namespace: NamespaceRange,
    pub enrollment_ca: ContentHash,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeTopologyGrant {
    /// `attestation` is zero in a command and set by prepare to the grant digest.
    /// `identity` is focal_enrollment::server_fingerprint of the enrolled cert.
    pub enrollment: NodeEnrollment,
    pub principal: [u8; 16],
    pub expires_at: i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupScope {
    Session(LedgerId),
    Partition {
        partition: PartitionId,
        namespace: NamespaceRange,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupAuthorityGrant {
    pub group: LogGroupId,
    pub genesis: ContentHash,
    pub scope: GroupScope,
    pub membership_epoch: u64,
    /// Node ID -> exact enrolled generation. Lists are installed metadata, never
    /// taken from an incoming proof's assertion of its own quorum.
    pub voters: BTreeMap<u64, u64>,
    pub outgoing_voters: BTreeMap<u64, u64>,
    pub learners: BTreeMap<u64, u64>,
    pub expires_at: i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityCheckpoint {
    pub schema: u16,
    pub anchor: AuthorityAnchor,
    pub revision: u64,
    pub applied_index: u64,
    pub clock: i64,
    pub nodes: BTreeMap<u64, NodeTopologyGrant>,
    pub groups: BTreeMap<LogGroupId, GroupAuthorityGrant>,
}
#[derive(Debug, Clone, Copy)]
pub struct AuthorityConfig {
    pub max_nodes: usize,
    pub max_groups: usize,
    pub max_members: usize,
    pub max_endpoint_bytes: usize,
    pub max_proofs: usize,
    pub max_proof_bytes: usize,
    pub max_checkpoint_bytes: usize,
    pub max_proof_lifetime_seconds: i64,
}
impl Default for AuthorityConfig {
    fn default() -> Self {
        Self {
            max_nodes: 1024,
            max_groups: 4096,
            max_members: 31,
            max_endpoint_bytes: 512,
            max_proofs: 16,
            max_proof_bytes: 256 * 1024,
            max_checkpoint_bytes: 8 * 1024 * 1024,
            max_proof_lifetime_seconds: 300,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorityCommand {
    pub expected_revision: u64,
    pub enrollment_revision: u64,
    /// Recorded by the authenticated metadata owner, not taken from peer proof.
    pub decided_at: i64,
    pub operation: AuthorityOperation,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(
    clippy::large_enum_variant,
    reason = "bounded owned command avoids another allocation"
)]
pub enum AuthorityOperation {
    AdvanceClock,
    GrantNode {
        grant: NodeTopologyGrant,
        expected_generation: Option<u64>,
    },
    /// Initial genesis assignment only. Does not certify any committed prefix,
    /// promote a live learner, or replace an existing group.
    BootstrapGroup {
        grant: GroupAuthorityGrant,
    },
    /// Old installed quorum must attest the exact committed configuration record.
    ChangeGroup {
        proof: AuthorityProof,
    },
}
struct AuthorityVersion {
    state: AuthorityCheckpoint,
    _allocation: Allocation,
}
pub struct AuthorityRegistry {
    root: AuthorityVersion,
    owner: OwnerId,
    config: AuthorityConfig,
    budget: MemoryBudget,
}
pub struct PreparedAuthorityUpdate {
    owner: OwnerId,
    base_revision: u64,
    next: AuthorityVersion,
}
impl PreparedAuthorityUpdate {
    pub fn checkpoint(&self) -> &AuthorityCheckpoint {
        &self.next.state
    }
}
impl AuthorityRegistry {
    pub fn new(
        anchor: AuthorityAnchor,
        config: AuthorityConfig,
        budget: MemoryBudget,
    ) -> Result<Self, DirectoryError> {
        Self::restore(
            AuthorityCheckpoint {
                schema: 1,
                anchor: anchor.clone(),
                revision: 0,
                applied_index: 0,
                clock: 0,
                nodes: BTreeMap::new(),
                groups: BTreeMap::new(),
            },
            &anchor,
            config,
            budget,
        )
    }
    /// The caller obtains this checkpoint from its own quorum-installed metadata
    /// log. Parsing an arbitrary peer checkpoint is not an authority installation.
    pub fn restore(
        state: AuthorityCheckpoint,
        expected: &AuthorityAnchor,
        config: AuthorityConfig,
        budget: MemoryBudget,
    ) -> Result<Self, DirectoryError> {
        validate_config(config)?;
        if &state.anchor != expected {
            return Err(DirectoryError::WrongCluster);
        }
        if (state.revision == 0
            && (state.applied_index != 0 || !state.nodes.is_empty() || !state.groups.is_empty()))
            || (state.revision > 0 && state.applied_index == 0)
        {
            return Err(DirectoryError::UnverifiedAuthority);
        }
        validate_state(&state, config)?;
        let allocation = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                state_charge(&state)?,
            )?
            .commit();
        Ok(Self {
            root: AuthorityVersion {
                state,
                _allocation: allocation,
            },
            owner: OwnerId::new()?,
            config,
            budget,
        })
    }
    pub fn checkpoint(&self) -> &AuthorityCheckpoint {
        &self.root.state
    }
    pub fn revision(&self) -> u64 {
        self.root.state.revision
    }
    pub fn applied_index(&self) -> u64 {
        self.root.state.applied_index
    }
    pub fn charged_bytes(&self) -> Result<usize, DirectoryError> {
        state_charge(&self.root.state)
    }
    pub fn node(&self, node: u64) -> Option<&NodeTopologyGrant> {
        self.root.state.nodes.get(&node)
    }
    pub fn group(&self, group: LogGroupId) -> Option<&GroupAuthorityGrant> {
        self.root.state.groups.get(&group)
    }
    pub(crate) fn config(&self) -> AuthorityConfig {
        self.config
    }
    pub(crate) fn validate_enrollment(
        &self,
        enrollment: &EnrollmentRegistry,
    ) -> Result<(), DirectoryError> {
        if enrollment.cluster() != self.root.state.anchor.cluster.0
            || ContentHash(server_fingerprint(enrollment.ca_certificate()))
                != self.root.state.anchor.enrollment_ca
        {
            return Err(DirectoryError::WrongCluster);
        }
        Ok(())
    }
    pub fn prepare(
        &self,
        command: &AuthorityCommand,
        enrollment: &EnrollmentRegistry,
    ) -> Result<PreparedAuthorityUpdate, DirectoryError> {
        self.validate_enrollment(enrollment)?;
        if command.expected_revision != self.revision()
            || command.enrollment_revision != enrollment.revision()
        {
            return Err(DirectoryError::CompareFailed);
        }
        if command.decided_at < self.root.state.clock || command.decided_at < 0 {
            return Err(DirectoryError::ClockRegression);
        }
        let extra = match &command.operation {
            AuthorityOperation::AdvanceClock => 0,
            AuthorityOperation::GrantNode { grant, .. } => {
                validate_node_shape(grant, self.config, true)?;
                add(
                    tree_row::<(u64, NodeTopologyGrant)>(),
                    grant.enrollment.endpoint.capacity(),
                )?
            }
            AuthorityOperation::BootstrapGroup { grant } => {
                validate_group_shape(grant, &self.root.state.anchor, self.config)?;
                group_charge(grant)?
            }
            AuthorityOperation::ChangeGroup { proof } => {
                let verifier =
                    self.verifier(enrollment, std::slice::from_ref(proof), command.decided_at)?;
                verifier.verify_membership(proof)?;
                match &proof.statement.fact {
                    AuthorityFact::Membership { next, .. } => group_charge(next)?,
                    _ => return Err(DirectoryError::WrongOperation),
                }
            }
        };
        let allocation = self
            .budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                add(state_charge(&self.root.state)?, extra)?,
            )?
            .commit();
        let mut next = self.root.state.clone();
        match &command.operation {
            AuthorityOperation::AdvanceClock => {}
            AuthorityOperation::GrantNode {
                grant,
                expected_generation,
            } => {
                let old = next.nodes.get(&grant.enrollment.node);
                if old.map(|entry| entry.enrollment.generation) != *expected_generation {
                    return Err(DirectoryError::CompareFailed);
                }
                if expected_generation.map_or(Some(1), |old| old.checked_add(1))
                    != Some(grant.enrollment.generation)
                    || old.is_some_and(|old| {
                        grant.enrollment.authority_epoch < old.enrollment.authority_epoch
                    })
                {
                    return Err(DirectoryError::StaleNode);
                }
                validate_node_identity(grant, enrollment, command.decided_at)?;
                let mut grant = grant.clone();
                grant.enrollment.attestation = node_digest(&next.anchor, &grant)?;
                next.nodes.insert(grant.enrollment.node, grant);
            }
            AuthorityOperation::BootstrapGroup { grant } => {
                if next.groups.contains_key(&grant.group) {
                    return Err(DirectoryError::Duplicate);
                }
                if grant.membership_epoch != 1 || !grant.outgoing_voters.is_empty() {
                    return Err(DirectoryError::StaleEpoch);
                }
                validate_live_group(grant, &next, enrollment, command.decided_at, self.config)?;
                next.groups.insert(grant.group, grant.clone());
            }
            AuthorityOperation::ChangeGroup { proof } => {
                let AuthorityFact::Membership { next: grant, .. } = &proof.statement.fact else {
                    return Err(DirectoryError::WrongOperation);
                };
                validate_live_group(grant, &next, enrollment, command.decided_at, self.config)?;
                next.groups.insert(grant.group, grant.clone());
            }
        }
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or(DirectoryError::CounterExhausted)?;
        next.clock = command.decided_at;
        // applied_index is installed only after the embedding metadata Raft commit.
        validate_state(&next, self.config)?;
        Ok(PreparedAuthorityUpdate {
            owner: self.owner,
            base_revision: self.revision(),
            next: AuthorityVersion {
                state: next,
                _allocation: allocation,
            },
        })
    }
    pub fn publish(
        &mut self,
        mut prepared: PreparedAuthorityUpdate,
        committed_index: u64,
    ) -> Result<(), DirectoryError> {
        if prepared.owner != self.owner || prepared.base_revision != self.revision() {
            return Err(DirectoryError::StalePreparation);
        }
        if committed_index <= self.applied_index() {
            return Err(DirectoryError::StaleEpoch);
        }
        if prepared.next.state.revision > committed_index {
            return Err(DirectoryError::StaleEpoch);
        }
        prepared.next.state.applied_index = committed_index;
        self.root = prepared.next;
        Ok(())
    }
}
fn validate_config(config: AuthorityConfig) -> Result<(), DirectoryError> {
    if config.max_nodes == 0
        || config.max_groups == 0
        || config.max_members == 0
        || config.max_members > 127
        || config.max_endpoint_bytes == 0
        || config.max_endpoint_bytes > 2048
        || config.max_proofs == 0
        || config.max_proofs > 64
        || config.max_proof_bytes == 0
        || config.max_proof_bytes > 2 * 1024 * 1024
        || config.max_checkpoint_bytes == 0
        || config.max_proof_lifetime_seconds <= 0
    {
        return Err(DirectoryError::Invalid("authority bounds"));
    }
    Ok(())
}
fn validate_state(
    state: &AuthorityCheckpoint,
    config: AuthorityConfig,
) -> Result<(), DirectoryError> {
    state.anchor.namespace.validate()?;
    if state.schema != 1
        || state.anchor.cluster.0 == [0; 16]
        || state.anchor.metadata_group.0 == [0; 16]
        || !types::nonzero_hash(state.anchor.genesis)
        || !types::nonzero_hash(state.anchor.enrollment_ca)
        || state.clock < 0
        || state.nodes.len() > config.max_nodes
        || state.groups.len() > config.max_groups
    {
        return Err(DirectoryError::Invalid("authority checkpoint"));
    }
    if postcard::experimental::serialized_size(state).map_err(|_| DirectoryError::Capacity)?
        > config.max_checkpoint_bytes
    {
        return Err(DirectoryError::Capacity);
    }
    for (id, grant) in &state.nodes {
        validate_node_shape(grant, config, false)?;
        if *id != grant.enrollment.node
            || grant.enrollment.attestation != node_digest(&state.anchor, grant)?
        {
            return Err(DirectoryError::UnverifiedAuthority);
        }
    }
    for (id, grant) in &state.groups {
        if *id != grant.group {
            return Err(DirectoryError::UnverifiedAuthority);
        }
        validate_group_shape(grant, &state.anchor, config)?;
    }
    Ok(())
}
fn node_digest(
    anchor: &AuthorityAnchor,
    grant: &NodeTopologyGrant,
) -> Result<ContentHash, DirectoryError> {
    // Serialize the fields excluding attestation; no self-referential hash or clone.
    let n = &grant.enrollment;
    digest(
        b"focal.directory.node-topology.v1",
        &(
            anchor,
            n.node,
            n.generation,
            n.region,
            n.zone,
            &n.endpoint,
            n.identity,
            n.authority_epoch,
            n.eligible,
            grant.principal,
            grant.expires_at,
        ),
    )
}
fn validate_node_shape(
    grant: &NodeTopologyGrant,
    config: AuthorityConfig,
    command: bool,
) -> Result<(), DirectoryError> {
    let n = &grant.enrollment;
    if n.node == 0
        || n.generation == 0
        || n.region.0 == [0; 16]
        || n.zone.0 == [0; 16]
        || n.authority_epoch == 0
        || n.endpoint.is_empty()
        || n.endpoint.len() > config.max_endpoint_bytes
        || !types::nonzero_hash(n.identity)
        || grant.principal == [0; 16]
        || grant.expires_at <= 0
        || (command && types::nonzero_hash(n.attestation))
    {
        return Err(DirectoryError::Invalid("node topology grant"));
    }
    Ok(())
}
pub(crate) fn validate_node_identity(
    grant: &NodeTopologyGrant,
    registry: &EnrollmentRegistry,
    now: i64,
) -> Result<(), DirectoryError> {
    if now >= grant.expires_at {
        return Err(DirectoryError::Expired);
    }
    let receipt = registry
        .enrollments()
        .find(|receipt| {
            ContentHash(server_fingerprint(&receipt.certificate)) == grant.enrollment.identity
        })
        .ok_or(DirectoryError::UnverifiedAuthority)?;
    let identity = registry
        .authorize_certificate(&receipt.certificate, now)
        .map_err(|_| DirectoryError::UnverifiedAuthority)?;
    if identity.role != EnrollmentRole::Node
        || identity.node_id != Some(grant.enrollment.node)
        || identity.principal != grant.principal
        || grant.expires_at > receipt.expires_at
    {
        return Err(DirectoryError::UnverifiedAuthority);
    }
    Ok(())
}
fn validate_group_shape(
    grant: &GroupAuthorityGrant,
    anchor: &AuthorityAnchor,
    config: AuthorityConfig,
) -> Result<(), DirectoryError> {
    if grant.group.0 == [0; 16]
        || !types::nonzero_hash(grant.genesis)
        || grant.membership_epoch == 0
        || grant.expires_at <= 0
        || grant.voters.is_empty()
        || grant.voters.len() > config.max_members
        || grant.outgoing_voters.len() > config.max_members
        || grant.learners.len() > config.max_members
    {
        return Err(DirectoryError::Invalid("group authority grant"));
    }
    for (id, generation) in grant
        .voters
        .iter()
        .chain(&grant.outgoing_voters)
        .chain(&grant.learners)
    {
        if *id == 0 || *generation == 0 {
            return Err(DirectoryError::StaleNode);
        }
        if grant.learners.contains_key(id)
            && (grant.voters.contains_key(id) || grant.outgoing_voters.contains_key(id))
        {
            return Err(DirectoryError::Quorum);
        }
        if grant
            .voters
            .get(id)
            .zip(grant.outgoing_voters.get(id))
            .is_some_and(|(a, b)| a != b)
        {
            return Err(DirectoryError::StaleNode);
        }
    }
    match grant.scope {
        GroupScope::Session(ledger) if anchor.namespace.contains(ledger) => {}
        GroupScope::Partition {
            partition,
            namespace,
        } if partition.0 != [0; 16] && contains_range(anchor.namespace, namespace) => {
            namespace.validate()?
        }
        _ => return Err(DirectoryError::OutsideNamespace),
    }
    Ok(())
}
pub(crate) fn contains_range(outer: NamespaceRange, inner: NamespaceRange) -> bool {
    inner.start >= outer.start
        && outer
            .end
            .is_none_or(|end| inner.end.is_some_and(|inner| inner <= end))
}
fn validate_live_group(
    grant: &GroupAuthorityGrant,
    state: &AuthorityCheckpoint,
    enrollment: &EnrollmentRegistry,
    now: i64,
    config: AuthorityConfig,
) -> Result<(), DirectoryError> {
    validate_group_shape(grant, &state.anchor, config)?;
    if grant.expires_at <= now {
        return Err(DirectoryError::Expired);
    }
    for (node, generation) in grant
        .voters
        .iter()
        .chain(&grant.outgoing_voters)
        .chain(&grant.learners)
    {
        let registered = state.nodes.get(node).ok_or(DirectoryError::Missing)?;
        if registered.enrollment.generation != *generation
            || !registered.enrollment.eligible
            || grant.expires_at > registered.expires_at
        {
            return Err(DirectoryError::StaleNode);
        }
        validate_node_identity(registered, enrollment, now)?;
    }
    Ok(())
}
fn group_charge(grant: &GroupAuthorityGrant) -> Result<usize, DirectoryError> {
    add(
        tree_row::<(LogGroupId, GroupAuthorityGrant)>(),
        mul(
            add(
                add(grant.voters.len(), grant.outgoing_voters.len())?,
                grant.learners.len(),
            )?,
            tree_row::<(u64, u64)>(),
        )?,
    )
}
fn state_charge(state: &AuthorityCheckpoint) -> Result<usize, DirectoryError> {
    let mut bytes = add(size_of::<AuthorityVersion>(), ALLOCATOR_OVERHEAD)?;
    for grant in state.nodes.values() {
        bytes = add(
            bytes,
            add(
                tree_row::<(u64, NodeTopologyGrant)>(),
                grant.enrollment.endpoint.capacity(),
            )?,
        )?;
    }
    for grant in state.groups.values() {
        bytes = add(bytes, group_charge(grant)?)?;
    }
    Ok(bytes)
}
