//! Reconstruct peer transport from durable root metadata and enroll new root
//! learners. Reachability never supplies geography, custody, or voting rights.
use crate::{
    control_host::{ControlHost, RootObservation},
    embedded::atomic_file,
    network_bootstrap::unix_time,
    network_state::NetworkState,
    quorum_enrollment::QuorumEnrollmentHost,
};
use focal_control::*;
use focal_directory::{
    AuthorityCommand, AuthorityOperation, NodeEnrollment, NodeTopologyGrant, RegionId, ZoneId,
};
use focal_enrollment::{
    EnrollmentLimits, EnrollmentReceipt, EnrollmentRegistry, EnrollmentRole, JoinFailure,
    JoinFuture, JoinHandler, JoinRequest, JoinResponse, PrivateJournal,
};
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{ParticipantId, RequestEpoch, RequestId, RouteEpoch};
use focal_wire::*;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    time::Duration,
};

#[derive(Debug, thiserror::Error)]
pub enum ControllerError {
    #[error("root network metadata is inconsistent")]
    Identity,
    #[error("network controller admission exceeded")]
    Capacity,
    #[error("root network owner stopped")]
    Stopped,
    #[error("network controller runtime is unavailable")]
    Runtime,
    #[error("root metadata: {0}")]
    Control(#[from] ControlFailure),
    #[error("enrollment: {0}")]
    Enrollment(#[from] focal_enrollment::EnrollmentError),
    #[error("network startup: {0}")]
    Network(#[from] crate::network_bootstrap::NetworkError),
    #[error("peer transport: {0}")]
    Transport(#[from] PeerSendError),
    #[error("network authorization: {0}")]
    Access(#[from] AccessError),
    #[error("network state encoding: {0}")]
    Encoding(#[from] postcard::Error),
    #[error("network controller journal: {0}")]
    Io(#[from] std::io::Error),
}

/// Receipt release follows committed enrollment and the controller's active
/// grant projection. Only that controller writes the registry; a delayed join
/// completion can never restore a grant removed by a newer revocation.
#[derive(Clone)]
pub struct RegisteredEnrollment {
    host: QuorumEnrollmentHost,
    registry: PeerRegistry,
}
impl RegisteredEnrollment {
    pub fn new(host: QuorumEnrollmentHost, registry: PeerRegistry) -> Self {
        Self { host, registry }
    }
}
impl JoinHandler for RegisteredEnrollment {
    fn handle(&self, request: JoinRequest) -> JoinFuture<'_> {
        Box::pin(async move {
            let response = JoinHandler::handle(&self.host, request).await;
            let JoinResponse::Enrolled(receipt) = response else {
                return response;
            };
            let grant = match self
                .host
                .authorize_certificate(receipt.certificate.clone())
                .await
            {
                Ok(grant) => grant,
                Err(_) => return JoinResponse::Rejected(JoinFailure::OutcomeUnknown),
            };
            if wait_registered_grant(&self.registry, &receipt, &grant, Duration::from_secs(5))
                .await
                .is_err()
            {
                return JoinResponse::Rejected(JoinFailure::OutcomeUnknown);
            }
            JoinResponse::Enrolled(receipt)
        })
    }
}

async fn wait_registered_grant(
    registry: &PeerRegistry,
    receipt: &EnrollmentReceipt,
    grant: &PeerGrant,
    deadline: Duration,
) -> Result<(), JoinFailure> {
    let role = match (receipt.identity.role, receipt.identity.node_id) {
        (EnrollmentRole::Node, Some(node_id)) if node_id != 0 => PeerRole::Node { node_id },
        (EnrollmentRole::Client, None) => PeerRole::Actor,
        _ => return Err(JoinFailure::OutcomeUnknown),
    };
    if grant.principal.0 != receipt.identity.principal || grant.role != role {
        return Err(JoinFailure::OutcomeUnknown);
    }
    let fingerprint = certificate_fingerprint(&receipt.certificate);
    std::panic::AssertUnwindSafe(async {
        tokio::time::timeout(deadline, async {
            loop {
                if let Ok(peer) = registry.authenticate(fingerprint)
                    && peer.principal() == grant.principal
                    && peer.role() == grant.role
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| JoinFailure::OutcomeUnknown)
    })
    .catch_unwind()
    .await
    .map_err(|_| JoinFailure::OutcomeUnknown)?
}

/// Keep recovery routes, but never keep ingress grants when the committed
/// authorization prefix cannot be observed. The deadline also covers a stalled
/// physical metadata owner; no permanently suspended read can pin old grants.
async fn observe_projection(
    host: &ControlHost,
    registry: &PeerRegistry,
) -> Result<Option<RootObservation>, ControllerError> {
    let failure = match tokio::time::timeout(Duration::from_secs(5), host.observe_root()).await {
        Ok(Ok(observation)) => return Ok(Some(observation)),
        Ok(Err(failure)) => Some(failure),
        Err(_) => None,
    };
    registry.replace_grants(BTreeMap::new())?;
    match failure {
        Some(ControlFailure::Capacity | ControlFailure::NotReady) | None => Ok(None),
        Some(error) => Err(error.into()),
    }
}

/// Install grants from the recovered durable prefix before opening ingress.
/// Immutable genesis must never resurrect a later revocation during restart.
/// A newly joined node may not yet occur in its local prefix; its sponsor already
/// committed and registered that receipt before releasing it to the joiner.
pub fn seed_peer_registry(
    state: &NetworkState,
    receipt: &EnrollmentReceipt,
    enrollment: &EnrollmentRegistry,
    registry: &PeerRegistry,
    now: i64,
) -> Result<(), ControllerError> {
    let mut physical = state.genesis.founder.clone();
    physical.node = state.node;
    state
        .validate(&physical)
        .map_err(|_| ControllerError::Identity)?;
    if receipt.identity.cluster != state.genesis.founder.cluster
        || receipt.identity.node_id != Some(state.node)
        || receipt.identity.role != EnrollmentRole::Node
        || receipt.expires_at <= now
        || receipt.issued_at > now
    {
        return Err(ControllerError::Identity);
    }
    if enrollment.ca_certificate() != state.sponsor.ca_certificate {
        return Err(ControllerError::Identity);
    }
    let grants = active_grants(enrollment, state, now)?;
    if let Some(known) = enrollment
        .enrollments()
        .find(|known| known.identity.node_id == Some(state.node))
    {
        if known != receipt {
            return Err(ControllerError::Identity);
        }
        enrollment.authorize_certificate(&receipt.certificate, now)?;
    }
    registry.replace_grants(grants)?;
    Ok(())
}

fn genesis_enrollment(state: &NetworkState) -> Result<EnrollmentRegistry, ControllerError> {
    let ControlBootstrap::Root { enrollment, .. } = &state.genesis.bootstrap else {
        return Err(ControllerError::Identity);
    };
    let registry = EnrollmentRegistry::restore(
        enrollment,
        state.genesis.founder.cluster,
        EnrollmentLimits::default(),
    )?;
    if registry.ca_certificate() != state.sponsor.ca_certificate {
        return Err(ControllerError::Identity);
    }
    Ok(registry)
}
fn node_grant(node: u64, principal: [u8; 16], state: &NetworkState) -> PeerGrant {
    PeerGrant {
        principal: ParticipantId(principal),
        tenants: BTreeSet::from([state.genesis.root_namespace.tenant]),
        role: PeerRole::Node { node_id: node },
    }
}
fn active_grants(
    enrollment: &EnrollmentRegistry,
    state: &NetworkState,
    now: i64,
) -> Result<BTreeMap<[u8; 32], PeerGrant>, ControllerError> {
    let mut grants = BTreeMap::new();
    for receipt in enrollment.enrollments() {
        let identity = match enrollment.authorize_certificate(&receipt.certificate, now) {
            Ok(identity) => identity,
            Err(
                focal_enrollment::EnrollmentError::Revoked
                | focal_enrollment::EnrollmentError::Expired,
            ) => continue,
            Err(error) => return Err(error.into()),
        };
        if identity.cluster != state.genesis.founder.cluster {
            return Err(ControllerError::Identity);
        }
        let grant = match (identity.role, identity.node_id) {
            (EnrollmentRole::Node, Some(node)) if node != 0 => {
                node_grant(node, identity.principal, state)
            }
            (EnrollmentRole::Client, None) => PeerGrant {
                principal: ParticipantId(identity.principal),
                tenants: BTreeSet::from([state.genesis.founder.ledger.tenant]),
                role: PeerRole::Actor,
            },
            _ => return Err(ControllerError::Identity),
        };
        grants.insert(certificate_fingerprint(&receipt.certificate), grant);
    }
    Ok(grants)
}

#[derive(Serialize, Deserialize)]
struct RootIntent {
    schema: u16,
    root: ControlIdentity,
    completed: u64,
    pending: Option<ControlRequest>,
}
struct RootAdmission {
    journal: Option<PrivateJournal>,
    intent: RootIntent,
    principal: ParticipantId,
}
impl RootAdmission {
    fn open(state: &NetworkState, root: &std::path::Path) -> Result<Self, ControllerError> {
        let path = root.join("cluster/root-admission");
        let marker = root.join("ROOT-ADMISSION.initialized");
        if marker.exists() && !path.join("journal.bin").is_file() {
            return Err(ControllerError::Identity);
        }
        let mut journal = PrivateJournal::open(path)?;
        let intent = match journal.read()? {
            Some(bytes) => {
                let (value, rest): (RootIntent, _) = postcard::take_from_bytes(&bytes)?;
                if !rest.is_empty() {
                    return Err(ControllerError::Identity);
                }
                value
            }
            None => {
                let value = RootIntent {
                    schema: 1,
                    root: state.genesis.root,
                    completed: 0,
                    pending: None,
                };
                journal.replace(&postcard::to_stdvec(&value)?)?;
                value
            }
        };
        let principal = controller_principal(state.genesis.founder.cluster);
        if intent.schema != 1
            || intent.root != state.genesis.root
            || intent.pending.as_ref().is_some_and(|request| {
                request.id.client != principal.0
                    || Some(request.id.sequence) != intent.completed.checked_add(1)
                    || request.acknowledged_through != intent.completed
                    || !root_admission_command(state, &request.command)
            })
        {
            return Err(ControllerError::Identity);
        }
        if !marker.exists() {
            atomic_file(&marker, b"root learner admission initialized")?;
        }
        Ok(Self {
            journal: Some(journal),
            intent,
            principal,
        })
    }
    #[cfg(test)]
    fn save(&mut self) -> Result<(), ControllerError> {
        self.journal
            .as_mut()
            .ok_or(ControllerError::Stopped)?
            .replace(&postcard::to_stdvec(&self.intent)?)?;
        Ok(())
    }
    async fn save_on(&mut self, host: &ControlHost) -> Result<(), ControllerError> {
        crate::control_host::save_local_intent(
            host,
            &mut self.journal,
            postcard::to_stdvec(&self.intent)?,
        )
        .await
        .map_err(|error| match error {
            crate::control_host::LocalIntentError::Capacity => ControllerError::Capacity,
            crate::control_host::LocalIntentError::Unavailable => ControllerError::Stopped,
            crate::control_host::LocalIntentError::Persistence(error) => {
                ControllerError::Enrollment(error)
            }
        })
    }
    async fn advance(
        &mut self,
        state: &NetworkState,
        observation: &RootObservation,
        eligible: &BTreeSet<u64>,
        host: &ControlHost,
        budget: &MemoryBudget,
    ) -> Result<(), ControllerError> {
        if host.progress().leader != state.node {
            return Ok(());
        }
        if self.intent.pending.is_none() {
            let now = unix_time()?;
            let command = match next_root_command(state, observation, eligible, now)? {
                Some(command) => Some(command),
                None => {
                    use crate::directory_bootstrap::{
                        DirectoryBootstrapError, FirstDirectoryPlan, next_first_directory_command,
                    };
                    let plan = FirstDirectoryPlan::derive(
                        state.genesis.founder.cluster,
                        state.genesis.founder.node,
                    )
                    .map_err(|_| ControllerError::Identity)?;
                    match next_first_directory_command(plan, observation, now, budget) {
                        Ok(command) => command,
                        Err(
                            DirectoryBootstrapError::Unauthorized
                            | DirectoryBootstrapError::Capacity
                            | DirectoryBootstrapError::NotReady,
                        ) => None,
                        Err(_) => return Err(ControllerError::Identity),
                    }
                }
            };
            let Some(command) = command else {
                return Ok(());
            };
            self.intent.pending = Some(ControlRequest {
                id: ControlRequestId {
                    client: self.principal.0,
                    sequence: self
                        .intent
                        .completed
                        .checked_add(1)
                        .ok_or(ControllerError::Capacity)?,
                },
                acknowledged_through: self.intent.completed,
                command,
            });
            self.save_on(host).await?;
        }
        let peer = AuthenticatedPeer::local(PeerGrant {
            principal: self.principal,
            tenants: BTreeSet::from([state.genesis.root_namespace.tenant]),
            role: PeerRole::Runtime,
        })?;
        let request = self
            .intent
            .pending
            .as_ref()
            .ok_or(ControllerError::Identity)?
            .clone();
        match host.submit(peer, request).await {
            Ok(receipt) => {
                let expected = self
                    .intent
                    .pending
                    .as_ref()
                    .ok_or(ControllerError::Identity)?;
                if receipt.request != expected.id
                    || receipt.committed_index == 0
                    || receipt.committed_term == 0
                {
                    return Err(ControllerError::Identity);
                }
                self.intent.completed = receipt.request.sequence;
                self.intent.pending = None;
                self.save_on(host).await?;
            }
            Err(ControlFailure::CompareFailed) => {
                // This is a pre-admission rejection; the sequence remains free.
                self.intent.pending = None;
                self.save_on(host).await?;
            }
            Err(
                ControlFailure::NotLeader { .. }
                | ControlFailure::NotReady
                | ControlFailure::Capacity
                | ControlFailure::Unavailable
                | ControlFailure::OutcomeUnknown,
            ) => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
}
// Keep the existing durable journal schema, path and principal: upgrading a
// controller must preserve an unknown learner admission from its previous run.
fn root_admission_command(state: &NetworkState, command: &ControlCommand) -> bool {
    let Ok(plan) = crate::directory_bootstrap::FirstDirectoryPlan::derive(
        state.genesis.founder.cluster,
        state.genesis.founder.node,
    ) else {
        return false;
    };
    match command {
        ControlCommand::Membership(ControlMembershipCommand {
            change: MembershipChange::AddLearner { .. },
            ..
        })
        | ControlCommand::ActivateAuthority(AuthorityActivation::Root { .. }) => true,
        ControlCommand::Authority(AuthorityCommand {
            operation:
                AuthorityOperation::GrantNode {
                    grant,
                    expected_generation: None,
                },
            ..
        }) => {
            grant.enrollment.region == RegionId([0; 16])
                && grant.enrollment.zone == ZoneId([0; 16])
                && grant.enrollment.authority_epoch == 1
                && grant.enrollment.generation == 1
                && grant.enrollment.eligible
        }
        ControlCommand::VerifiedRoot(VerifiedRootCommand {
            command:
                focal_directory::RootCommand {
                    operation: focal_directory::RootOperation::Delegate { delegation },
                    ..
                },
            evidence,
        }) => *delegation == plan.delegation() && evidence.proofs.is_empty(),
        ControlCommand::Authority(AuthorityCommand {
            operation: AuthorityOperation::BootstrapGroup { grant },
            ..
        }) => {
            grant.group == plan.group()
                && plan.identity().is_ok_and(|identity| {
                    grant.genesis == focal_model::ContentHash(identity.genesis)
                })
                && grant.scope
                    == (focal_directory::GroupScope::Partition {
                        partition: plan.partition(),
                        namespace: focal_directory::NamespaceRange::all(),
                    })
                && grant.membership_epoch == 1
                && grant.voters.len() == 1
                && grant
                    .voters
                    .get(&state.genesis.founder.node)
                    .is_some_and(|generation| *generation != 0)
                && grant.learners.is_empty()
                && grant.outgoing_voters.is_empty()
        }
        _ => false,
    }
}
fn next_root_command(
    state: &NetworkState,
    observation: &RootObservation,
    eligible: &BTreeSet<u64>,
    now: i64,
) -> Result<Option<ControlCommand>, ControllerError> {
    let snapshot = observation.snapshot();
    if snapshot.identity != state.genesis.root {
        return Err(ControllerError::Identity);
    }
    let Some(authority) = observation.authority() else {
        return Ok(Some(ControlCommand::ActivateAuthority(
            AuthorityActivation::Root {
                expected_root_revision: snapshot.revisions.root,
                expected_enrollment_revision: snapshot.revisions.enrollment,
                decided_at: now,
            },
        )));
    };
    let ControlBootstrap::Root { enrollment, .. } = &snapshot.state else {
        return Err(ControllerError::Identity);
    };
    let enrollment = EnrollmentRegistry::restore(
        enrollment,
        state.genesis.founder.cluster,
        EnrollmentLimits::default(),
    )?;
    let current = observation.configuration();
    // Admit already-capable nodes before allocating another capability. A full
    // grant table must not starve learners whose capability is already installed.
    for node in eligible {
        if current.configuration.contains(*node) {
            continue;
        }
        let Some(grant) = authority.nodes.get(node) else {
            continue;
        };
        let live = grant.enrollment.eligible
            && grant.expires_at > now
            && enrollment.enrollments().any(|receipt| {
                receipt.identity.node_id == Some(*node)
                    && receipt.identity.principal == grant.principal
                    && focal_model::ContentHash(focal_enrollment::server_fingerprint(
                        &receipt.certificate,
                    )) == grant.enrollment.identity
            });
        if live {
            return Ok(Some(ControlCommand::Membership(ControlMembershipCommand {
                expected_configuration_index: current.configuration_index,
                expected: current.configuration.clone(),
                change: MembershipChange::AddLearner { node: *node },
            })));
        }
    }
    for node in eligible {
        if authority.nodes.contains_key(node) {
            continue;
        }
        let contact = observation
            .contacts()
            .contacts
            .records
            .iter()
            .find(|contact| contact.node == *node)
            .ok_or(ControllerError::Identity)?;
        let receipt = enrollment
            .enrollments()
            .find(|receipt| receipt.identity.node_id == Some(*node))
            .ok_or(ControllerError::Identity)?;
        let identity = authorize_node_contact(
            &enrollment,
            *node,
            contact.principal,
            contact.certificate_fingerprint,
            now,
        )
        .map_err(ControlFailure::from)?;
        return Ok(Some(ControlCommand::Authority(AuthorityCommand {
            expected_revision: authority.revision,
            enrollment_revision: enrollment.revision(),
            decided_at: now,
            operation: AuthorityOperation::GrantNode {
                expected_generation: None,
                grant: NodeTopologyGrant {
                    enrollment: NodeEnrollment {
                        node: *node,
                        generation: 1,
                        region: RegionId([0; 16]),
                        zone: ZoneId([0; 16]),
                        endpoint: contact.advertise.to_string(),
                        identity: focal_model::ContentHash(focal_enrollment::server_fingerprint(
                            &receipt.certificate,
                        )),
                        authority_epoch: 1,
                        attestation: focal_model::ContentHash([0; 32]),
                        eligible: true,
                    },
                    principal: identity.principal,
                    expires_at: receipt.expires_at,
                },
            },
        })));
    }
    Ok(None)
}
fn controller_principal(cluster: [u8; 16]) -> ParticipantId {
    let hash = blake3::derive_key("focal.root.learner-controller.v1", &cluster);
    let mut value = [0; 16];
    for (target, source) in value.iter_mut().zip(hash) {
        *target = source;
    }
    ParticipantId(value)
}

/// Dropping the controller future withdraws ingress authorization even if its
/// caller keeps the listener and registry alive. A poisoned registry already
/// rejects authentication, so cleanup failure must never unwind from Drop.
struct PeerProjectionGuard<'a>(&'a PeerRegistry);
impl Drop for PeerProjectionGuard<'_> {
    fn drop(&mut self) {
        let _ = self.0.replace_grants(BTreeMap::new());
    }
}

pub struct NetworkController {
    state: NetworkState,
    receipt: EnrollmentReceipt,
    admission: Option<RootAdmission>,
    routes: BTreeMap<u64, PeerEndpoint>,
    route_revision: u64,
    contact_cursor: u64,
    budget: MemoryBudget,
    _allocation: Allocation,
}
impl NetworkController {
    pub fn new(
        state: NetworkState,
        receipt: EnrollmentReceipt,
        root: PathBuf,
        budget: MemoryBudget,
    ) -> Result<Self, ControllerError> {
        // Covers the bounded root checkpoint decode, grants, contact candidates,
        // journal and old/new route maps. No per-peer shared ownership is added.
        let allocation = budget
            .reserve(
                BudgetKind::Control,
                BudgetLane::Completion,
                64 * 1024 * 1024,
            )
            .map_err(|_| ControllerError::Capacity)?
            .commit();
        let admission = if state.node == state.genesis.founder.node {
            Some(RootAdmission::open(&state, &root)?)
        } else {
            None
        };
        Ok(Self {
            state,
            receipt,
            admission,
            routes: BTreeMap::new(),
            route_revision: 0,
            contact_cursor: 0,
            budget,
            _allocation: allocation,
        })
    }
    pub fn run<'a>(
        self,
        pool: &'a PeerConnectionPool,
        host: &'a ControlHost,
        registry: &'a PeerRegistry,
    ) -> impl std::future::Future<Output = Result<(), ControllerError>> + 'a {
        // Construct before the async body: cancellation before its first poll
        // must also withdraw the startup projection.
        let projection = PeerProjectionGuard(registry);
        async move {
            let _projection = projection;
            // A handle can be moved into a runtime without a timer driver. Consume
            // the failed controller at this dependency boundary; never reuse it.
            if tokio::runtime::Handle::try_current().is_err() {
                Err(ControllerError::Runtime)
            } else {
                std::panic::AssertUnwindSafe(self.run_inner(pool, host, registry))
                    .catch_unwind()
                    .await
                    .unwrap_or(Err(ControllerError::Runtime))
            }
        }
    }
    async fn run_inner(
        mut self,
        pool: &PeerConnectionPool,
        host: &ControlHost,
        registry: &PeerRegistry,
    ) -> Result<(), ControllerError> {
        let mut observed: Option<RootObservation> = None;
        let mut eligible = BTreeSet::new();
        let mut next_transition = i64::MAX;
        let mut previous_time = None;
        loop {
            let progress = host.progress();
            if progress.stopped {
                return Err(ControllerError::Stopped);
            }
            let mut now = unix_time()?;
            if observed
                .as_ref()
                .is_none_or(|value| value.snapshot().applied_index != progress.applied_index)
            {
                // Routes/grants own their bounded projection. Release the old
                // export before reserving the next one, otherwise two large
                // snapshots can permanently prevent a refresh under pressure.
                observed = None;
                let next = match observe_projection(host, registry).await? {
                    Some(value) => value,
                    None => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                // Export may wait behind disk IO. Authorization uses the time
                // at projection, never the pre-await timestamp.
                now = unix_time()?;
                (eligible, next_transition) = self.refresh(&next, pool, registry, now)?;
                observed = Some(next);
            } else if now >= next_transition || previous_time.is_some_and(|previous| now < previous)
            {
                let current = observed.as_ref().ok_or(ControllerError::Identity)?;
                (eligible, next_transition) = self.refresh(current, pool, registry, now)?;
            }
            previous_time = Some(now);
            {
                let current = observed.as_ref().ok_or(ControllerError::Identity)?;
                // An unavailable route must not delay projection refresh or
                // expiration for an entire discovery sweep. Cancellation is an
                // unknown outcome; the next round uses this exact contact key.
                if let Ok(result) = tokio::time::timeout(
                    Duration::from_secs(5),
                    self.announce(current, pool, host, registry),
                )
                .await
                {
                    result?;
                }
                if let Some(admission) = &mut self.admission {
                    admission
                        .advance(&self.state, current, &eligible, host, &self.budget)
                        .await?;
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    fn refresh(
        &mut self,
        observed: &RootObservation,
        pool: &PeerConnectionPool,
        registry: &PeerRegistry,
        now: i64,
    ) -> Result<(BTreeSet<u64>, i64), ControllerError> {
        let snapshot = observed.snapshot();
        if snapshot.identity != self.state.genesis.root
            || observed.contacts().identity != snapshot.identity
            || observed.contacts().applied_index != snapshot.applied_index
            || observed.configuration().applied_index != snapshot.applied_index
        {
            return Err(ControllerError::Identity);
        }
        let ControlBootstrap::Root { enrollment, .. } = &snapshot.state else {
            return Err(ControllerError::Identity);
        };
        let enrollment = EnrollmentRegistry::restore(
            enrollment,
            self.state.genesis.founder.cluster,
            EnrollmentLimits::default(),
        )?;
        if enrollment.ca_certificate() != self.state.sponsor.ca_certificate {
            return Err(ControllerError::Identity);
        }
        let grants = active_grants(&enrollment, &self.state, now)?;
        let next_transition = enrollment
            .enrollments()
            .flat_map(|receipt| [receipt.issued_at, receipt.expires_at])
            .filter(|time| *time > now)
            .min()
            .unwrap_or(i64::MAX);
        let mut routes = BTreeMap::new();
        // The immutable sponsor is an authenticated bootstrap route, not a hint
        // supplied by an arbitrary peer. A committed contact can supersede it.
        let initial = genesis_enrollment(&self.state)?;
        let founder = initial
            .enrollments()
            .next()
            .ok_or(ControllerError::Identity)?;
        if self.state.node != self.state.genesis.founder.node
            && grants.contains_key(&certificate_fingerprint(&founder.certificate))
        {
            routes.insert(
                self.state.genesis.founder.node,
                PeerEndpoint {
                    address: self
                        .state
                        .sponsor
                        .endpoint
                        .parse()
                        .map_err(|_| ControllerError::Identity)?,
                    server_name: founder.identity.server_name.clone(),
                },
            );
        }
        let mut eligible = BTreeSet::new();
        for contact in &observed.contacts().contacts.records {
            if !grants.contains_key(&contact.certificate_fingerprint) {
                continue;
            }
            let identity = authorize_node_contact(
                &enrollment,
                contact.node,
                contact.principal,
                contact.certificate_fingerprint,
                now,
            )
            .map_err(ControlFailure::from)?;
            if identity.server_name != contact.server_name {
                return Err(ControllerError::Identity);
            }
            eligible.insert(contact.node);
            if contact.node != self.state.node {
                routes.insert(
                    contact.node,
                    PeerEndpoint {
                        address: contact.advertise,
                        server_name: contact.server_name.clone(),
                    },
                );
            }
        }
        registry.replace_grants(grants)?;
        if routes != self.routes {
            let revision = self
                .route_revision
                .checked_add(1)
                .ok_or(ControllerError::Capacity)?;
            pool.replace_routes(revision, routes.clone())?;
            self.routes = routes;
            self.route_revision = revision;
        }
        Ok((eligible, next_transition))
    }
    async fn announce(
        &mut self,
        observed: &RootObservation,
        pool: &PeerConnectionPool,
        host: &ControlHost,
        registry: &PeerRegistry,
    ) -> Result<(), ControllerError> {
        let fingerprint = certificate_fingerprint(&self.receipt.certificate);
        if let Some(contact) = observed
            .contacts()
            .contacts
            .records
            .iter()
            .find(|contact| contact.node == self.state.node)
        {
            if contact.certificate_fingerprint != fingerprint
                || contact.advertise != self.state.advertise
            {
                return Err(ControllerError::Identity);
            }
            return Ok(());
        }
        let request = RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: self.state.genesis.root_namespace,
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId(self.receipt.request),
            operation: Operation::NodeContact {
                group: self.state.genesis.root.group,
                sequence: 1,
                acknowledged_through: 0,
                expected_generation: 0,
                advertise: self.state.advertise,
            },
        };
        if host.progress().leader == self.state.node {
            let peer = registry.authenticate(fingerprint)?;
            let response = dispatch(host, peer, request, &ControlHost::wire_limits()).await;
            if let Response::Control { response } = response.result {
                let reply = ControlReply::decode(
                    &response,
                    ControlHost::wire_limits().max_frame_bytes as usize,
                )
                .map_err(ControlFailure::from)?;
                check_contact_reply(reply, &self.receipt)?;
            }
        } else {
            self.announce_remote(pool, &request, host.progress().leader)
                .await?;
        }
        Ok(())
    }
    async fn announce_remote(
        &mut self,
        pool: &PeerConnectionPool,
        request: &RequestEnvelope,
        leader: u64,
    ) -> Result<(), ControllerError> {
        // The preferred peer has its own deadline below the round deadline;
        // stale leader progress cannot consume every round before discovery.
        if self.routes.contains_key(&leader)
            && let Ok(Ok(response)) = tokio::time::timeout(
                Duration::from_secs(1),
                pool.send_peer_control(leader, request),
            )
            .await
        {
            let reply = ControlReply::decode(
                &response,
                ControlHost::wire_limits().max_frame_bytes as usize,
            )
            .map_err(ControlFailure::from)?;
            if check_contact_reply(reply, &self.receipt)? {
                return Ok(());
            }
        }
        // A bounded round visits only installed routes and advances before
        // waiting. Subsequent rounds never learn endpoints from redirects.
        for _ in 0..8 {
            let Some(target) = pool.next_route_target(self.contact_cursor)? else {
                self.contact_cursor = 0;
                break;
            };
            self.contact_cursor = target;
            if target == leader {
                continue;
            }
            if let Ok(Ok(response)) = tokio::time::timeout(
                Duration::from_secs(1),
                pool.send_peer_control(target, request),
            )
            .await
            {
                let reply = ControlReply::decode(
                    &response,
                    ControlHost::wire_limits().max_frame_bytes as usize,
                )
                .map_err(ControlFailure::from)?;
                if check_contact_reply(reply, &self.receipt)? {
                    break;
                }
            }
        }
        Ok(())
    }
}
fn check_contact_reply(
    reply: ControlReply,
    expected: &EnrollmentReceipt,
) -> Result<bool, ControllerError> {
    match reply {
        ControlReply::Committed(receipt)
            if receipt.committed_index > 0
                && receipt.committed_term > 0
                && receipt.request.client == expected.identity.principal
                && receipt.request.sequence == 1 =>
        {
            Ok(true)
        }
        ControlReply::Rejected(
            ControlFailure::NotLeader { .. }
            | ControlFailure::NotReady
            | ControlFailure::Capacity
            | ControlFailure::Unavailable
            | ControlFailure::OutcomeUnknown,
        ) => Ok(false),
        ControlReply::Rejected(error) => Err(error.into()),
        _ => Err(ControllerError::Identity),
    }
}

#[cfg(test)]
#[path = "network_controller_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "network_controller_admission_tests.rs"]
mod admission_tests;
