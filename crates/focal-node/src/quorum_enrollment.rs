//! Private signing custody composed with an external replicated root authority.
//! A single bounded owner journals exact public proposals before sending them.
//! No credential or invitation is released until a quorum read confirms it.
use crate::{cluster::InviteIntent, control_host::ControlHost};
use focal_control::*;
use focal_enrollment::*;
use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use focal_model::{ParticipantId, RequestId, TenantId};
use focal_wire::{AuthenticatedPeer, PeerGrant, PeerRole};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot};

pub type ControlFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ControlFailure>> + Send + 'a>>;
/// The router binds this private signer's dedicated sequence principal, using
/// either local Runtime authority or the immutable founder's narrow remote
/// enrollment grant. State reads must use the owner's ReadIndex API.
/// Endpoint discovery never changes this pinned identity or grants membership.
pub trait EnrollmentControl: Send + Sync {
    fn identity(&self) -> ControlIdentity;
    fn principal(&self) -> ParticipantId;
    fn read_state(&self, request: RequestId) -> ControlFuture<'_, ControlSnapshot>;
    fn submit(&self, request: ControlRequest) -> ControlFuture<'_, ControlReceipt>;
}
#[derive(Clone)]
pub struct LocalEnrollmentControl {
    host: ControlHost,
    peer: AuthenticatedPeer,
}
impl LocalEnrollmentControl {
    pub fn new(host: ControlHost, peer: AuthenticatedPeer) -> Result<Self, QuorumEnrollmentError> {
        if peer.role() != PeerRole::Runtime || host.progress().identity.scope != ControlScope::Root
        {
            return Err(QuorumEnrollmentError::Identity);
        }
        Ok(Self { host, peer })
    }
}
impl EnrollmentControl for LocalEnrollmentControl {
    fn identity(&self) -> ControlIdentity {
        self.host.progress().identity
    }
    fn principal(&self) -> ParticipantId {
        self.peer.principal()
    }
    fn read_state(&self, request: RequestId) -> ControlFuture<'_, ControlSnapshot> {
        Box::pin(async move {
            match self
                .host
                .read(self.peer.clone(), request, ControlRead::State)
                .await?
            {
                ControlReadResult::State(state) => Ok(state),
                _ => Err(ControlFailure::Invalid),
            }
        })
    }
    fn submit(&self, request: ControlRequest) -> ControlFuture<'_, ControlReceipt> {
        Box::pin(self.host.submit(self.peer.clone(), request))
    }
}
#[derive(Debug, thiserror::Error)]
pub enum QuorumEnrollmentError {
    #[error("enrollment: {0}")]
    Enrollment(#[from] EnrollmentError),
    #[error("replicated enrollment decision: {0}")]
    Control(#[from] ControlFailure),
    #[error("signer, root authority, or authenticated principal does not match")]
    Identity,
    #[error("invitation request identity conflicts with its saved intent")]
    IntentConflict,
    #[error("enrollment owner has stopped or its disk state is ambiguous")]
    Stopped,
}
#[derive(Clone, Debug)]
pub struct QuorumEnrollmentConfig {
    pub root: ControlIdentity,
    /// Dedicated server-owned signer principal; do not share its sequence stream.
    /// Remote use requires the immutable founder's narrow enrollment grant.
    pub principal: ParticipantId,
    pub server_name: String,
    /// Server-owned certificate grants; no tenant grant comes from a CSR.
    pub tenants: BTreeSet<TenantId>,
    pub limits: EnrollmentLimits,
    pub queue_items: usize,
    pub request_timeout: Duration,
}
impl QuorumEnrollmentConfig {
    pub fn new(
        root: ControlIdentity,
        principal: ParticipantId,
        server_name: String,
        tenants: BTreeSet<TenantId>,
    ) -> Self {
        Self {
            root,
            principal,
            server_name,
            tenants,
            limits: EnrollmentLimits::default(),
            queue_items: 16,
            request_timeout: Duration::from_secs(10),
        }
    }
    fn validate(&self) -> Result<(), QuorumEnrollmentError> {
        if self.root.scope != ControlScope::Root
            || self.principal.is_zero()
            || self.server_name.is_empty()
            || self.server_name.len() > 253
            || self.tenants.is_empty()
            || self.tenants.len() > 1024
            || self.tenants.iter().any(|tenant| tenant.is_zero())
            || !(1..=128).contains(&self.queue_items)
            || self.request_timeout.is_zero()
            || self.request_timeout > Duration::from_secs(60)
            || !(16 * 1024..=8 * 1024 * 1024).contains(&self.limits.max_checkpoint_bytes)
        {
            return Err(EnrollmentError::Invalid.into());
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
struct Journal {
    schema: u16,
    root: ControlIdentity,
    principal: ParticipantId,
    server_name: String,
    next_sequence: u64,
    pending: Option<ControlRequest>,
}
struct Answer<T> {
    result: Result<T, QuorumEnrollmentError>,
    _charge: Allocation,
}
enum Action {
    Invite(RequestId, InviteIntent, oneshot::Sender<Answer<Invitation>>),
    Redeem(JoinRequest, oneshot::Sender<Answer<EnrollmentReceipt>>),
    Renew(RenewRequest, oneshot::Sender<Answer<EnrollmentReceipt>>),
    Revoke(InvitationId, oneshot::Sender<Answer<()>>),
    Authorize(Vec<u8>, oneshot::Sender<Answer<PeerGrant>>),
    AdmitTenant([u8; 16], oneshot::Sender<Answer<()>>),
    ActivateFence(u32, oneshot::Sender<Answer<focal_enrollment::UpgradeFence>>),
    Stop(oneshot::Sender<()>),
}
struct Work {
    action: Action,
    _charge: Allocation,
}
#[derive(Clone)]
pub struct QuorumEnrollmentHost {
    sender: mpsc::Sender<Work>,
    budget: MemoryBudget,
    reply_allowance: usize,
}
pub struct QuorumEnrollmentDriver {
    authority: BootstrapAuthority,
    directory: PathBuf,
    config: QuorumEnrollmentConfig,
    disk: PrivateJournal,
    journal: Journal,
    receiver: mpsc::Receiver<Work>,
    budget: MemoryBudget,
    _state_charge: Allocation,
    failed: bool,
}
impl QuorumEnrollmentHost {
    /// Explicit first initialization. Existing retry state is an error. Reopening
    /// must use `open`, including after a failed/unknown initialization response.
    pub fn create(
        authority: BootstrapAuthority,
        directory: impl AsRef<Path>,
        config: QuorumEnrollmentConfig,
        budget: MemoryBudget,
    ) -> Result<(Self, QuorumEnrollmentDriver), QuorumEnrollmentError> {
        Self::build(authority, directory.as_ref(), config, budget, true)
    }
    /// Missing initialized state is corruption, never a fresh sequence authority.
    pub fn open(
        authority: BootstrapAuthority,
        directory: impl AsRef<Path>,
        config: QuorumEnrollmentConfig,
        budget: MemoryBudget,
    ) -> Result<(Self, QuorumEnrollmentDriver), QuorumEnrollmentError> {
        Self::build(authority, directory.as_ref(), config, budget, false)
    }
    fn build(
        authority: BootstrapAuthority,
        directory: &Path,
        config: QuorumEnrollmentConfig,
        budget: MemoryBudget,
        create: bool,
    ) -> Result<(Self, QuorumEnrollmentDriver), QuorumEnrollmentError> {
        config.validate()?;
        if authority.cluster() != config.root.cluster.0 {
            return Err(QuorumEnrollmentError::Identity);
        }
        let state_charge = reserve(&budget, BudgetKind::Control, 512 * 1024)?;
        let mut disk = PrivateJournal::open(directory)?;
        let saved = disk.read()?;
        let journal = match (create, saved) {
            (true, None) => {
                let journal = Journal {
                    schema: 1,
                    root: config.root,
                    principal: config.principal,
                    server_name: config.server_name.clone(),
                    next_sequence: 1,
                    pending: None,
                };
                save(&mut disk, &journal)?;
                journal
            }
            (false, Some(bytes)) => {
                let (journal, trailing): (Journal, _) =
                    postcard::take_from_bytes(&bytes).map_err(|_| EnrollmentError::Corrupt)?;
                if !trailing.is_empty() {
                    return Err(EnrollmentError::Corrupt.into());
                }
                journal
            }
            (true, Some(_)) => return Err(EnrollmentError::Conflict.into()),
            (false, None) => return Err(EnrollmentError::Corrupt.into()),
        };
        if journal.schema != 1
            || journal.root != config.root
            || journal.principal != config.principal
            || journal.server_name != config.server_name
            || journal.next_sequence == 0
            || journal.pending.as_ref().is_some_and(|request| {
                request.id.client != config.principal.0
                    || request.id.sequence != journal.next_sequence
                    || request.acknowledged_through != journal.next_sequence.saturating_sub(1)
                    || !matches!(request.command, ControlCommand::Enrollment(_))
            })
        {
            return Err(QuorumEnrollmentError::Identity);
        }
        let reply_allowance = config
            .tenants
            .len()
            .checked_mul(128)
            .ok_or(EnrollmentError::Capacity)?
            .max(64 * 1024);
        let (sender, receiver) = mpsc::channel(config.queue_items);
        Ok((
            Self {
                sender,
                budget: budget.clone(),
                reply_allowance,
            },
            QuorumEnrollmentDriver {
                authority,
                directory: directory.to_owned(),
                config,
                disk,
                journal,
                receiver,
                budget,
                _state_charge: state_charge,
                failed: false,
            },
        ))
    }
    fn enqueue(&self, action: Action, bytes: usize) -> Result<(), QuorumEnrollmentError> {
        let charge = bytes
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(self.reply_allowance))
            .ok_or(EnrollmentError::Capacity)?;
        self.sender
            .try_send(Work {
                action,
                _charge: reserve(&self.budget, BudgetKind::Pending, charge)?,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => EnrollmentError::Capacity.into(),
                mpsc::error::TrySendError::Closed(_) => QuorumEnrollmentError::Stopped,
            })
    }
    pub async fn invite(
        &self,
        request: RequestId,
        intent: InviteIntent,
    ) -> Result<Invitation, QuorumEnrollmentError> {
        if request.is_zero() || intent.endpoint.len() > 512 {
            return Err(EnrollmentError::Invalid.into());
        }
        let (send, receive) = oneshot::channel();
        self.enqueue(Action::Invite(request, intent, send), 4096)?;
        receive
            .await
            .map_err(|_| QuorumEnrollmentError::Stopped)?
            .result
    }
    pub async fn redeem(
        &self,
        request: JoinRequest,
    ) -> Result<EnrollmentReceipt, QuorumEnrollmentError> {
        let bytes = request.encode()?.len();
        let (send, receive) = oneshot::channel();
        self.enqueue(Action::Redeem(request, send), bytes)?;
        receive
            .await
            .map_err(|_| QuorumEnrollmentError::Stopped)?
            .result
    }
    /// Renew a credential its holder proves it holds; the same key receives a
    /// fresh certificate and lifetime.
    pub async fn renew(
        &self,
        request: RenewRequest,
    ) -> Result<EnrollmentReceipt, QuorumEnrollmentError> {
        let bytes = request.encode()?.len();
        let (send, receive) = oneshot::channel();
        self.enqueue(Action::Renew(request, send), bytes)?;
        receive
            .await
            .map_err(|_| QuorumEnrollmentError::Stopped)?
            .result
    }
    pub async fn revoke(&self, invitation: InvitationId) -> Result<(), QuorumEnrollmentError> {
        let (send, receive) = oneshot::channel();
        self.enqueue(Action::Revoke(invitation, send), 16)?;
        receive
            .await
            .map_err(|_| QuorumEnrollmentError::Stopped)?
            .result
    }
    /// Admit a tenant the cluster serves ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md)
    /// §16): a committed enrollment fact under the founder authority. A
    /// tenant already admitted is answered as done.
    pub async fn admit_tenant(&self, tenant: [u8; 16]) -> Result<(), QuorumEnrollmentError> {
        if tenant == [0; 16] {
            return Err(EnrollmentError::Invalid.into());
        }
        let (send, receive) = oneshot::channel();
        self.enqueue(Action::AdmitTenant(tenant, send), 16)?;
        receive
            .await
            .map_err(|_| QuorumEnrollmentError::Stopped)?
            .result
    }
    /// Raise the upgrade fence to `level` ([24](../../../docs/archictecutre/24-placement-execution-and-fleet-control.md)
    /// §21): a committed enrollment fact under the founder authority. A
    /// fence at or above `level` is answered as it is.
    pub async fn activate_fence(
        &self,
        level: u32,
    ) -> Result<focal_enrollment::UpgradeFence, QuorumEnrollmentError> {
        if level == 0 {
            return Err(EnrollmentError::Invalid.into());
        }
        let (send, receive) = oneshot::channel();
        self.enqueue(Action::ActivateFence(level, send), 16)?;
        receive
            .await
            .map_err(|_| QuorumEnrollmentError::Stopped)?
            .result
    }
    pub async fn authorize_certificate(
        &self,
        certificate: Vec<u8>,
    ) -> Result<PeerGrant, QuorumEnrollmentError> {
        if certificate.len() > 4096 {
            return Err(EnrollmentError::Capacity.into());
        }
        let bytes = certificate.len();
        let (send, receive) = oneshot::channel();
        self.enqueue(Action::Authorize(certificate, send), bytes)?;
        receive
            .await
            .map_err(|_| QuorumEnrollmentError::Stopped)?
            .result
    }
    pub async fn stop(&self) -> Result<(), QuorumEnrollmentError> {
        let (send, receive) = oneshot::channel();
        self.enqueue(Action::Stop(send), 0)?;
        receive.await.map_err(|_| QuorumEnrollmentError::Stopped)
    }
}
fn join_response(result: Result<EnrollmentReceipt, QuorumEnrollmentError>) -> JoinResponse {
    match result {
        Ok(receipt) => JoinResponse::Enrolled(receipt),
        Err(QuorumEnrollmentError::Enrollment(error)) => {
            JoinResponse::Rejected(JoinFailure::from(&error))
        }
        Err(QuorumEnrollmentError::Control(ControlFailure::Capacity)) => {
            JoinResponse::Rejected(JoinFailure::Capacity)
        }
        Err(QuorumEnrollmentError::Identity) => JoinResponse::Rejected(JoinFailure::WrongCluster),
        Err(_) => JoinResponse::Rejected(JoinFailure::OutcomeUnknown),
    }
}
impl JoinHandler for QuorumEnrollmentHost {
    fn handle(&self, request: JoinRequest) -> JoinFuture<'_> {
        Box::pin(async move { join_response(self.redeem(request).await) })
    }
    fn renew(&self, request: RenewRequest) -> JoinFuture<'_> {
        Box::pin(async move { join_response(self.renew(request).await) })
    }
}
impl QuorumEnrollmentDriver {
    /// One owner task, with borrowed externally managed routing. Filesystem and
    /// signing operations are bounded synchronous work; a dedicated executor
    /// may be used by the process owner. There are no per-request spawned tasks.
    pub async fn run(
        mut self,
        control: &impl EnrollmentControl,
    ) -> Result<(), QuorumEnrollmentError> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(QuorumEnrollmentError::Stopped);
        }
        self.check_router(control)?;
        while let Some(work) = self.receiver.recv().await {
            match work.action {
                Action::Invite(id, intent, send) => {
                    let result = self.invite(control, id, intent).await;
                    let _ = send.send(Answer {
                        result,
                        _charge: work._charge,
                    });
                }
                Action::Redeem(request, send) => {
                    let result = self.redeem(control, &request).await;
                    let _ = send.send(Answer {
                        result,
                        _charge: work._charge,
                    });
                }
                Action::Renew(request, send) => {
                    let result = self.renew(control, &request).await;
                    let _ = send.send(Answer {
                        result,
                        _charge: work._charge,
                    });
                }
                Action::Revoke(id, send) => {
                    let result = self.revoke(control, id).await;
                    let _ = send.send(Answer {
                        result,
                        _charge: work._charge,
                    });
                }
                Action::Authorize(cert, send) => {
                    let result = self.authorize(control, &cert).await;
                    let _ = send.send(Answer {
                        result,
                        _charge: work._charge,
                    });
                }
                Action::AdmitTenant(tenant, send) => {
                    let result = self.admit_tenant(control, tenant).await;
                    let _ = send.send(Answer {
                        result,
                        _charge: work._charge,
                    });
                }
                Action::ActivateFence(level, send) => {
                    let result = self.activate_fence(control, level).await;
                    let _ = send.send(Answer {
                        result,
                        _charge: work._charge,
                    });
                }
                Action::Stop(send) => {
                    let _ = send.send(());
                    return Ok(());
                }
            }
            if self.failed {
                return Err(QuorumEnrollmentError::Stopped);
            }
        }
        Ok(())
    }
    fn check_router(&self, control: &impl EnrollmentControl) -> Result<(), QuorumEnrollmentError> {
        if control.identity() != self.config.root || control.principal() != self.config.principal {
            return Err(QuorumEnrollmentError::Identity);
        }
        Ok(())
    }
    async fn registry(
        &self,
        control: &impl EnrollmentControl,
    ) -> Result<(EnrollmentRegistry, Allocation), QuorumEnrollmentError> {
        self.check_router(control)?;
        let bytes = self
            .config
            .limits
            .max_checkpoint_bytes
            .checked_mul(4)
            .ok_or(EnrollmentError::Capacity)?;
        let charge = reserve(&self.budget, BudgetKind::Control, bytes)?;
        let mut id = [0; 16];
        getrandom::fill(&mut id).map_err(|_| EnrollmentError::Crypto)?;
        let state = tokio::time::timeout(
            self.config.request_timeout,
            control.read_state(RequestId(id)),
        )
        .await
        .map_err(|_| ControlFailure::OutcomeUnknown)??;
        if state.identity != self.config.root {
            return Err(QuorumEnrollmentError::Identity);
        }
        let ControlBootstrap::Root { enrollment, .. } = state.state else {
            return Err(QuorumEnrollmentError::Identity);
        };
        let registry = EnrollmentRegistry::restore(
            &enrollment,
            self.config.root.cluster.0,
            self.config.limits.clone(),
        )?;
        if registry.ca_certificate() != self.authority.ca_certificate() {
            return Err(QuorumEnrollmentError::Identity);
        }
        Ok((registry, charge))
    }
    fn persist(&mut self) -> Result<(), QuorumEnrollmentError> {
        if let Err(error) = save(&mut self.disk, &self.journal) {
            self.failed = true;
            return Err(error);
        }
        Ok(())
    }
    async fn reconcile(
        &mut self,
        control: &impl EnrollmentControl,
    ) -> Result<(), QuorumEnrollmentError> {
        self.check_router(control)?;
        let Some(request) = self.journal.pending.clone() else {
            return Ok(());
        };
        let result =
            tokio::time::timeout(self.config.request_timeout, control.submit(request.clone()))
                .await
                .map_err(|_| ControlFailure::OutcomeUnknown)?;
        match result {
            Ok(receipt) => {
                let encoded =
                    postcard::to_allocvec(&request).map_err(|_| EnrollmentError::Capacity)?;
                if receipt.request_hash != blake3::derive_key("focal.control.request.v1", &encoded)
                    || receipt.request != request.id
                    || receipt.committed_index == 0
                    || receipt.committed_term == 0
                {
                    self.failed = true;
                    return Err(QuorumEnrollmentError::Identity);
                }
                self.journal.next_sequence = self
                    .journal
                    .next_sequence
                    .checked_add(1)
                    .ok_or(EnrollmentError::Capacity)?;
                self.journal.pending = None;
                self.persist()
            }
            // These rejections are issued only after the leader's current-term
            // fence. An exact committed request is returned before comparison.
            Err(ControlFailure::CompareFailed | ControlFailure::Rejected) => {
                self.journal.pending = None;
                self.persist()
            }
            Err(error) => Err(error.into()),
        }
    }
    async fn commit(
        &mut self,
        control: &impl EnrollmentControl,
        command: EnrollmentCommand,
    ) -> Result<(), QuorumEnrollmentError> {
        if self.journal.pending.is_some() {
            return Err(ControlFailure::OutcomeUnknown.into());
        }
        self.journal.pending = Some(ControlRequest {
            id: ControlRequestId {
                client: self.config.principal.0,
                sequence: self.journal.next_sequence,
            },
            acknowledged_through: self.journal.next_sequence.saturating_sub(1),
            command: ControlCommand::Enrollment(command),
        });
        self.persist()?;
        self.reconcile(control).await
    }
    async fn invite(
        &mut self,
        control: &impl EnrollmentControl,
        id: RequestId,
        intent: InviteIntent,
    ) -> Result<Invitation, QuorumEnrollmentError> {
        self.reconcile(control).await?;
        let (registry, charge) = self.registry(control).await?;
        let now = now()?;
        let bytes = postcard::to_allocvec(&intent).map_err(|_| EnrollmentError::Invalid)?;
        let hash = blake3::derive_key("focal.quorum-enrollment.invite-intent.v1", &bytes);
        let slot = blake3::derive_key("focal.quorum-enrollment.invite-request.v1", &id.0);
        let remembered = self.disk.invitation(slot)?;
        let path = self
            .directory
            .join(blake3::Hash::from_bytes(slot).to_hex().as_str());
        if remembered.is_some() && !path.try_exists().map_err(EnrollmentError::Io)? {
            self.failed = true;
            return Err(EnrollmentError::Corrupt.into());
        }
        let pending = if path.try_exists().map_err(EnrollmentError::Io)? {
            PendingInvitation::open(&path, self.config.root.cluster.0)?
        } else {
            let expires_at = now
                .checked_add(
                    i64::try_from(intent.lifetime_seconds).map_err(|_| EnrollmentError::Invalid)?,
                )
                .ok_or(EnrollmentError::Invalid)?;
            registry
                .prepare_invitation(
                    &self.authority,
                    InviteOptions {
                        endpoint: intent.endpoint,
                        server_name: self.config.server_name.clone(),
                        role: intent.role,
                        expires_at,
                    },
                    now,
                )?
                .persist(&path, hash)?
        };
        if pending.intent_hash() != hash {
            return Err(QuorumEnrollmentError::IntentConflict);
        }
        if remembered.is_some_and(|invitation| invitation != pending.id()) {
            self.failed = true;
            return Err(EnrollmentError::Corrupt.into());
        }
        if let Err(error) = self.disk.remember_invitation(slot, pending.id()) {
            self.failed = true;
            return Err(error.into());
        }
        match pending.release(&registry) {
            Ok(invitation) => return Ok(invitation),
            Err(EnrollmentError::NotCommitted) => {}
            Err(error) => return Err(error.into()),
        }
        let command = registry.prepare_pending_invitation(&pending, &self.authority, now)?;
        drop(registry);
        drop(charge);
        self.commit(control, command).await?;
        let (registry, _charge) = self.registry(control).await?;
        Ok(pending.release(&registry)?)
    }
    async fn redeem(
        &mut self,
        control: &impl EnrollmentControl,
        request: &JoinRequest,
    ) -> Result<EnrollmentReceipt, QuorumEnrollmentError> {
        self.reconcile(control).await?;
        let (registry, charge) = self.registry(control).await?;
        let command = match registry.prepare_join(&self.authority, request, now()?)? {
            JoinPreparation::Existing(receipt) => return Ok(receipt),
            JoinPreparation::Commit(command) => command,
        };
        drop(registry);
        drop(charge);
        self.commit(control, command).await?;
        let (registry, _charge) = self.registry(control).await?;
        Ok(registry.release(request, now()?)?)
    }
    async fn renew(
        &mut self,
        control: &impl EnrollmentControl,
        request: &RenewRequest,
    ) -> Result<EnrollmentReceipt, QuorumEnrollmentError> {
        self.reconcile(control).await?;
        let (registry, charge) = self.registry(control).await?;
        let grace = crate::credential_renewal::grace_seconds(registry.limits().credential_lifetime);
        let command = match registry.prepare_renew(&self.authority, request, now()?, grace)? {
            RenewPreparation::Existing(receipt) => return Ok(receipt),
            RenewPreparation::Commit(command) => command,
        };
        drop(registry);
        drop(charge);
        self.commit(control, command).await?;
        let (registry, _charge) = self.registry(control).await?;
        Ok(registry.release_renewal(request, now()?)?)
    }
    async fn revoke(
        &mut self,
        control: &impl EnrollmentControl,
        id: InvitationId,
    ) -> Result<(), QuorumEnrollmentError> {
        self.reconcile(control).await?;
        let (registry, charge) = self.registry(control).await?;
        if registry.invitation_revoked(id)? {
            return Ok(());
        }
        let command = registry.prepare_revoke(id, now()?)?;
        drop(registry);
        drop(charge);
        self.commit(control, command).await?;
        let (registry, _charge) = self.registry(control).await?;
        if !registry.invitation_revoked(id)? {
            return Err(EnrollmentError::NotCommitted.into());
        }
        Ok(())
    }
    async fn admit_tenant(
        &mut self,
        control: &impl EnrollmentControl,
        tenant: [u8; 16],
    ) -> Result<(), QuorumEnrollmentError> {
        self.reconcile(control).await?;
        let (registry, charge) = self.registry(control).await?;
        if registry.admits_tenant(tenant) {
            return Ok(());
        }
        let command = registry.prepare_admit_tenant(&self.authority, tenant, now()?)?;
        drop(registry);
        drop(charge);
        self.commit(control, command).await?;
        let (registry, _charge) = self.registry(control).await?;
        if !registry.admits_tenant(tenant) {
            return Err(EnrollmentError::NotCommitted.into());
        }
        Ok(())
    }
    async fn activate_fence(
        &mut self,
        control: &impl EnrollmentControl,
        level: u32,
    ) -> Result<focal_enrollment::UpgradeFence, QuorumEnrollmentError> {
        self.reconcile(control).await?;
        let (registry, charge) = self.registry(control).await?;
        if registry.fence().level >= level {
            return Ok(registry.fence());
        }
        let command = registry.prepare_activate_fence(&self.authority, level, now()?)?;
        drop(registry);
        drop(charge);
        self.commit(control, command).await?;
        let (registry, _charge) = self.registry(control).await?;
        if registry.fence().level < level {
            return Err(EnrollmentError::NotCommitted.into());
        }
        Ok(registry.fence())
    }
    /// The grant a certificate earns: the configured tenants and every tenant
    /// the committed registry admits, so admission never needs a restart and
    /// a credential never serves a tenant the cluster has not committed.
    async fn authorize(
        &mut self,
        control: &impl EnrollmentControl,
        cert: &[u8],
    ) -> Result<PeerGrant, QuorumEnrollmentError> {
        self.reconcile(control).await?;
        let (registry, _charge) = self.registry(control).await?;
        let identity = registry.authorize_certificate(cert, now()?)?;
        let role = match (identity.role, identity.node_id) {
            (EnrollmentRole::Node, Some(node_id)) if node_id != 0 => PeerRole::Node { node_id },
            (EnrollmentRole::Client, None) => PeerRole::Actor,
            _ => return Err(QuorumEnrollmentError::Identity),
        };
        let mut tenants = self.config.tenants.clone();
        tenants.extend(registry.tenants().map(TenantId));
        Ok(PeerGrant {
            principal: ParticipantId(identity.principal),
            tenants,
            role,
        })
    }
}
fn now() -> Result<i64, QuorumEnrollmentError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| EnrollmentError::Invalid)?
            .as_secs(),
    )
    .map_err(|_| EnrollmentError::Invalid.into())
}
fn reserve(
    budget: &MemoryBudget,
    kind: BudgetKind,
    bytes: usize,
) -> Result<Allocation, QuorumEnrollmentError> {
    budget
        .reserve(kind, BudgetLane::Ordinary, bytes)
        .map(|reservation| reservation.commit())
        .map_err(|_| EnrollmentError::Capacity.into())
}
fn save(disk: &mut PrivateJournal, journal: &Journal) -> Result<(), QuorumEnrollmentError> {
    let size =
        postcard::experimental::serialized_size(journal).map_err(|_| EnrollmentError::Capacity)?;
    if size > 60 * 1024 {
        return Err(EnrollmentError::Capacity.into());
    }
    let bytes = postcard::to_allocvec(journal).map_err(|_| EnrollmentError::Capacity)?;
    Ok(disk.replace(&bytes)?)
}
