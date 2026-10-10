//! One enrollment message per connection — an invitation's redemption or a credential's renewal —
//! carried on focal-wire's endpoint (hyper-transport on hyper-rt) as a message of the enrollment
//! protocol, whose distinct ALPN exposes only the bounded enrollment handler. The server is
//! authenticated before the client holds an issued certificate: TLS verifies its chain to the
//! trust's issuers and its name, and the dial accepts only a leaf the trust pins, so no
//! token-bearing request is made, let alone sent, for any other server.
use crate::*;
use focal_memory::MemoryBudget;
use focal_wire::net::{Inbound, Kind, Reply, Serve, ServeFuture, Served};
use focal_wire::{
    AdmissionLimits, ClientTls, EnrollmentProtocol, PeerRegistry, QuicConnector, QuicServer,
    ServerTls, WireError, WireLimits,
};
use serde::{Deserialize, Serialize};
use std::{
    future::{Future, poll_fn},
    net::SocketAddr,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::{Pin, pin},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};
use zeroize::Zeroizing;

/// The enrollment protocol as a listener serves it: its name in the TLS handshake, and the most
/// one of its messages carries — [`MAX_MESSAGE_BYTES`], what its encoder holds every message to.
pub const ENROLLMENT_PROTOCOL: EnrollmentProtocol = EnrollmentProtocol {
    alpn: ENROLLMENT_ALPN,
    max_message_bytes: 16 * 1024,
};
/// A redemption's kind on the enrollment protocol.
const JOIN: u8 = 1;
/// A renewal's kind on the enrollment protocol.
const RENEW: u8 = 3;

#[derive(Debug, Clone)]
pub struct TransportLimits {
    pub max_connections: usize,
    pub timeout: Duration,
}
impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            max_connections: 32,
            timeout: Duration::from_secs(15),
        }
    }
}
impl TransportLimits {
    fn validate(&self) -> Result<(), EnrollmentError> {
        if self.max_connections == 0
            || self.max_connections > 1024
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(120)
        {
            return Err(EnrollmentError::Capacity);
        }
        Ok(())
    }
    /// An enrollment endpoint's limits: one message at a time on a connection, each at most the
    /// protocol's bound, answered within `timeout`.
    pub(crate) fn wire(&self) -> Result<WireLimits, EnrollmentError> {
        self.validate()?;
        let message = ENROLLMENT_PROTOCOL.max_message_bytes;
        let limits = WireLimits {
            max_frame_bytes: message,
            max_items: 1,
            max_cost: u64::from(message),
            max_connections: self.max_connections,
            streams_per_connection: 1,
            control_streams: 1,
            request_timeout: self.timeout,
        };
        limits.validate().map_err(|_| EnrollmentError::Capacity)?;
        Ok(limits)
    }
    fn admission(&self) -> AdmissionLimits {
        AdmissionLimits::for_connections(self.max_connections)
    }
    /// What an enrollment endpoint holds at most, funded: the transport's own on the ordinary
    /// lane, and its messages — of the control class, as enrollment's are — on the completion
    /// lane kept for them.
    pub(crate) fn budget(&self) -> Result<MemoryBudget, EnrollmentError> {
        let bytes = focal_wire::endpoint_bytes(&self.wire()?, &self.admission())
            .map_err(|_| EnrollmentError::Capacity)?;
        let limit = usize::try_from(bytes.transport.saturating_add(bytes.messages))
            .map_err(|_| EnrollmentError::Capacity)?;
        let reserve = usize::try_from(bytes.messages).map_err(|_| EnrollmentError::Capacity)?;
        MemoryBudget::new(limit, reserve).map_err(|_| EnrollmentError::Capacity)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinFailure {
    Unauthorized,
    WrongCluster,
    Expired,
    Revoked,
    Used,
    Capacity,
    Conflict,
    Unavailable,
    OutcomeUnknown,
}
impl From<&EnrollmentError> for JoinFailure {
    fn from(error: &EnrollmentError) -> Self {
        match error {
            EnrollmentError::WrongCluster => Self::WrongCluster,
            EnrollmentError::Expired => Self::Expired,
            EnrollmentError::Revoked => Self::Revoked,
            EnrollmentError::Used => Self::Used,
            EnrollmentError::Capacity => Self::Capacity,
            EnrollmentError::Conflict => Self::Conflict,
            EnrollmentError::Io(_) | EnrollmentError::NotCommitted => Self::OutcomeUnknown,
            EnrollmentError::Locked => Self::Unavailable,
            _ => Self::Unauthorized,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// Exactly one response per bounded connection; inline receipt storage is part
// of that reservation and avoids an additional unaccounted heap allocation.
#[allow(clippy::large_enum_variant)]
pub enum JoinResponse {
    Enrolled(EnrollmentReceipt),
    Rejected(JoinFailure),
}
pub type JoinFuture<'a> = Pin<Box<dyn Future<Output = JoinResponse> + Send + 'a>>;
/// Authenticate against the registry, commit the prepared command, apply it,
/// then return registry.release(). Returning a prepared certificate is invalid.
/// Handler cancellation must never be interpreted as cancellation of a log entry.
pub trait JoinHandler: Send + Sync + 'static {
    fn handle(&self, request: JoinRequest) -> JoinFuture<'_>;
    /// Renew a credential the requester proves it holds. Handlers that only
    /// enroll refuse renewals.
    fn renew(&self, request: RenewRequest) -> JoinFuture<'_> {
        let _ = request;
        Box::pin(async { JoinResponse::Rejected(JoinFailure::Unauthorized) })
    }
}
impl<F, Fut> JoinHandler for F
where
    F: Fn(JoinRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = JoinResponse> + Send + 'static,
{
    fn handle(&self, request: JoinRequest) -> JoinFuture<'_> {
        Box::pin(self(request))
    }
}

// Explicit compatibility for intentionally shared dynamic handlers. Owned,
// cloneable actor handles do not need an additional reference-counted wrapper.
impl JoinHandler for Arc<dyn JoinHandler> {
    fn handle(&self, request: JoinRequest) -> JoinFuture<'_> {
        self.as_ref().handle(request)
    }
    fn renew(&self, request: RenewRequest) -> JoinFuture<'_> {
        self.as_ref().renew(request)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum JoinTransportError {
    #[error("enrollment authentication or configuration failed")]
    Enrollment(#[from] EnrollmentError),
    #[error("enrollment service rejected the request: {0:?}")]
    Rejected(JoinFailure),
    #[error("enrollment service is unavailable")]
    Unavailable,
    #[error("enrollment outcome is unknown; retry the saved join identity")]
    OutcomeUnknown,
    #[error("invalid enrollment frame")]
    InvalidFrame,
}

/// A listener's service with the enrollment protocol beside `data`: a joiner's message goes to
/// the [`JoinHandler`], every other request to `data`. The enrollment protocol carries no data
/// peer's message, so one from a data peer is refused. At most `max_connections` enrollments are
/// handled at once, a joiner having no identity whose share could bound it; one past them is
/// refused, and its joiner told the sponsor is unavailable.
pub struct EnrollmentService<S, H> {
    data: S,
    handler: H,
    limits: TransportLimits,
    /// The enrollments under way, shared with each one's answer, which gives its place back.
    handling: Arc<AtomicUsize>,
}
impl<S: Serve, H: JoinHandler + Clone> EnrollmentService<S, H> {
    /// `limits.timeout` is the most an enrollment's handling takes, past which its outcome is
    /// unknown; `limits.max_connections` the enrollments handled at once.
    pub fn new(data: S, handler: H, limits: TransportLimits) -> Result<Self, JoinTransportError> {
        limits.validate()?;
        Ok(Self {
            data,
            handler,
            limits,
            handling: Arc::new(AtomicUsize::new(0)),
        })
    }
}
/// One enrollment under way, its place given back as it ends.
struct Handling(Arc<AtomicUsize>);
impl Drop for Handling {
    fn drop(&mut self) {
        let _ = self
            .0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                Some(held.saturating_sub(1))
            });
    }
}
impl<S: Serve, H: JoinHandler + Clone> Serve for EnrollmentService<S, H> {
    fn capabilities(&self) -> [bool; 4] {
        self.data.capabilities()
    }
    fn serve(&self, request: Served) -> ServeFuture {
        match (request.kind, &request.from) {
            (Kind::Enrollment(tag), Inbound::Joiner) => {
                let body = Zeroizing::new(request.body);
                let most = self.limits.max_connections;
                if self
                    .handling
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                        (held < most).then(|| held.saturating_add(1))
                    })
                    .is_err()
                {
                    return Box::pin(async { None });
                }
                let handling = Handling(self.handling.clone());
                let handler = self.handler.clone();
                let timeout = self.limits.timeout;
                Box::pin(async move {
                    let _handling = handling;
                    answer(handler, tag, body, timeout)
                        .await
                        .map(Reply::uncharged)
                })
            }
            (Kind::Enrollment(_), _) => Box::pin(async { None }),
            _ => self.data.serve(request),
        }
    }
}

/// The data protocol of a listener that serves enrollment alone: nothing.
struct NoData;
impl Serve for NoData {
    fn capabilities(&self) -> [bool; 4] {
        [false; 4]
    }
    fn serve(&self, _request: Served) -> ServeFuture {
        Box::pin(async { None })
    }
}

/// The answer to one enrollment message: its request decoded, its handler's response encoded, all
/// of it within `timeout` and behind an unwind boundary — a dependency's panic is an outcome
/// unknown, never a partly processed enrollment resumed. `None` refuses the message: a malformed
/// one, or one whose outcome the handler could not say in time.
async fn answer<H: JoinHandler>(
    handler: H,
    tag: u8,
    body: Zeroizing<Vec<u8>>,
    timeout: Duration,
) -> Option<Vec<u8>> {
    let exchange = hyper_rt::futures::within(nanos(timeout), async {
        let response = match tag {
            JOIN => handler.handle(decode::<JoinRequest>(&body).ok()?).await,
            RENEW => handler.renew(decode::<RenewRequest>(&body).ok()?).await,
            _ => return None,
        };
        // A response past the bound is not sent: its outcome is unknown to the joiner, who retries
        // the saved join identity.
        encode(&response)
            .or_else(|_| encode(&JoinResponse::Rejected(JoinFailure::OutcomeUnknown)))
            .ok()
    });
    let mut exchange = pin!(exchange);
    let polled = poll_fn(|context| {
        match catch_unwind(AssertUnwindSafe(|| exchange.as_mut().poll(context))) {
            Ok(poll) => poll.map(Some),
            Err(_) => Poll::Ready(None),
        }
    })
    .await;
    polled.and_then(Result::ok).flatten().flatten()
}

/// An enrollment endpoint alone, its own socket: a sponsor's bootstrap listener in a test or a
/// tool. A node serves enrollment beside its data protocol on one listener instead
/// ([`EnrollmentService`]).
pub struct EnrollmentServer {
    server: QuicServer,
    limits: TransportLimits,
}
impl EnrollmentServer {
    pub fn bind(
        address: SocketAddr,
        identity: &CredentialMaterial,
        limits: TransportLimits,
    ) -> Result<Self, JoinTransportError> {
        require_runtime()?;
        let wire = limits.wire()?;
        let server = QuicServer::bind_serving(
            address,
            ServerTls::from_config(identity.server_config()?),
            // A listener of the enrollment protocol alone grants no certificate anything.
            PeerRegistry::new(1).map_err(|_| JoinTransportError::Unavailable)?,
            Some(ENROLLMENT_PROTOCOL),
            wire,
            limits.admission(),
            limits.budget()?,
        )
        .map_err(|_| JoinTransportError::Unavailable)?;
        Ok(Self { server, limits })
    }
    pub fn local_addr(&self) -> Result<SocketAddr, JoinTransportError> {
        self.server
            .local_addr()
            .map_err(|_| JoinTransportError::Unavailable)
    }
    /// Closes the endpoint and its connections; `serve` returns. Refused only when the shard has
    /// no task left to queue it.
    pub fn close(&self) -> Result<(), JoinTransportError> {
        self.server
            .close()
            .map_err(|_| JoinTransportError::Unavailable)
    }
    pub async fn serve<H: JoinHandler + Clone>(
        &self,
        handler: H,
    ) -> Result<(), JoinTransportError> {
        require_runtime()?;
        let service = EnrollmentService::new(NoData, handler, self.limits.clone())?;
        self.server
            .serve_with(service)
            .await
            .map_err(|_| JoinTransportError::Unavailable)
    }
}

/// Redeems invitations and renews credentials at a sponsor: each exchange on an endpoint of its
/// own at the client's address (a port of 0 lets exchanges run at once), under the TLS its trust
/// verifies, at most `max_connections` at once.
pub struct EnrollmentClient {
    address: SocketAddr,
    limits: TransportLimits,
    inflight: AtomicUsize,
}
impl EnrollmentClient {
    pub fn bind(address: SocketAddr, limits: TransportLimits) -> Result<Self, JoinTransportError> {
        limits.validate()?;
        require_runtime()?;
        Ok(Self {
            address,
            limits,
            inflight: AtomicUsize::new(0),
        })
    }
    pub async fn redeem(
        &self,
        address: SocketAddr,
        invitation: &Invitation,
        key: &JoinKey,
        now: i64,
    ) -> Result<EnrollmentReceipt, JoinTransportError> {
        require_runtime()?;
        let _permit = self.permit()?;
        if invitation.cluster() != key.cluster() {
            return Err(EnrollmentError::WrongCluster.into());
        }
        let trust = invitation.trust();
        let response = self
            .exchange(address, trust, invitation.client_config()?, JOIN, || {
                // No token-bearing request exists before the server's chain and pinned leaf
                // have both passed: it is made on the connection that will carry it.
                encode(&invitation.request(key, now)?)
            })
            .await?;
        match response {
            JoinResponse::Rejected(JoinFailure::OutcomeUnknown) => {
                Err(JoinTransportError::OutcomeUnknown)
            }
            JoinResponse::Rejected(error) => Err(JoinTransportError::Rejected(error)),
            JoinResponse::Enrolled(receipt) => {
                if receipt.invitation != invitation.id()
                    || receipt.request != key.request_id()
                    || receipt.identity.cluster != invitation.cluster()
                    || receipt.identity.role != invitation.role()
                    || receipt.csr_hash != hash("focal.enrollment.csr.v1", key.csr())
                    || receipt.public_key != pki::csr_key_hash(key.csr())?
                    || receipt.expires_at <= now
                    || pki::verify_issued(
                        &receipt,
                        trust
                            .issuers
                            .iter()
                            .map(|issuer| issuer.certificate.as_slice()),
                    )
                    .is_err()
                {
                    return Err(JoinTransportError::OutcomeUnknown);
                }
                Ok(receipt)
            }
        }
    }
    /// Renew a held credential at the sponsor: the request is signed by the
    /// credential the caller holds and answered with the committed receipt.
    pub async fn renew(
        &self,
        address: SocketAddr,
        trust: &ServerTrust,
        request: RenewRequest,
        now: i64,
    ) -> Result<EnrollmentReceipt, JoinTransportError> {
        require_runtime()?;
        let _permit = self.permit()?;
        if trust.ca_certificate.is_empty() {
            return Err(EnrollmentError::Invalid.into());
        }
        let response = self
            .exchange(address, trust, trust.client_config()?, RENEW, || {
                encode(&request)
            })
            .await?;
        match response {
            JoinResponse::Rejected(JoinFailure::OutcomeUnknown) => {
                Err(JoinTransportError::OutcomeUnknown)
            }
            JoinResponse::Rejected(error) => Err(JoinTransportError::Rejected(error)),
            JoinResponse::Enrolled(receipt) => {
                if receipt.invitation != request.invitation_id()
                    || receipt.request != request.request_id()
                    || receipt.identity.cluster != request.cluster()
                    || receipt.expires_at <= now
                    || receipt.expires_at <= request.holds_until()
                    || pki::verify_issued(
                        &receipt,
                        trust
                            .issuers
                            .iter()
                            .map(|issuer| issuer.certificate.as_slice()),
                    )
                    .is_err()
                {
                    return Err(JoinTransportError::OutcomeUnknown);
                }
                Ok(receipt)
            }
        }
    }
    /// Nothing is held between exchanges: each closes its endpoint as it ends.
    pub fn close(&self) {}

    /// One message of kind `tag` to the sponsor at `address`, made by `message` once the
    /// connection is up and its leaf pinned, and its response. A connection that could not be
    /// made, or whose leaf is not pinned, sent nothing; past that the outcome is unknown unless
    /// the response says it.
    async fn exchange(
        &self,
        address: SocketAddr,
        trust: &ServerTrust,
        tls: rustls::ClientConfig,
        tag: u8,
        message: impl FnOnce() -> Result<Vec<u8>, EnrollmentError>,
    ) -> Result<JoinResponse, JoinTransportError> {
        let connector = QuicConnector::bind(
            self.address,
            ClientTls::from_config(tls),
            self.limits.wire()?,
            self.limits.budget()?,
        )
        .map_err(|_| JoinTransportError::Unavailable)?;
        let timeout = self.limits.timeout;
        let exchanged = hyper_rt::futures::within(nanos(timeout), async {
            let link = connector
                .enrollment(address, &trust.server_name, trust.pins())
                .await
                .map_err(|error| match error {
                    WireError::Unpinned => {
                        JoinTransportError::Enrollment(EnrollmentError::Unpinned)
                    }
                    _ => JoinTransportError::Unavailable,
                })?;
            // The link wipes the message however its exchange ends: it holds an invitation's
            // secret.
            let answer = link
                .ask(tag, message()?, timeout)
                .await
                .map_err(|_| JoinTransportError::OutcomeUnknown)?;
            decode::<JoinResponse>(&answer).map_err(|_| JoinTransportError::OutcomeUnknown)
        })
        .await;
        // The endpoint goes with the exchange, answered or not.
        let _ = connector.close();
        match exchanged {
            Ok(Some(response)) => response,
            // Past its time the exchange may have been carried: its outcome is unknown.
            Ok(None) => Err(JoinTransportError::OutcomeUnknown),
            Err(_) => Err(JoinTransportError::Unavailable),
        }
    }

    /// One of the exchanges the client runs at once, or `Unavailable` past its bound.
    fn permit(&self) -> Result<Permit<'_>, JoinTransportError> {
        let most = self.limits.max_connections;
        self.inflight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                (held < most).then(|| held.saturating_add(1))
            })
            .map_err(|_| JoinTransportError::Unavailable)?;
        Ok(Permit(&self.inflight))
    }
}
/// An exchange in flight, given back as it ends.
struct Permit<'a>(&'a AtomicUsize);
impl Drop for Permit<'_> {
    fn drop(&mut self) {
        let _ = self
            .0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                Some(held.saturating_sub(1))
            });
    }
}

fn nanos(period: Duration) -> u64 {
    u64::try_from(period.as_nanos()).unwrap_or(u64::MAX)
}

/// A network adapter runs on a hyper-rt shard: off one it is refused, typed, before any socket
/// is made.
fn require_runtime() -> Result<(), JoinTransportError> {
    hyper_rt::futures::shard_id()
        .map(|_| ())
        .ok_or(JoinTransportError::Unavailable)
}
