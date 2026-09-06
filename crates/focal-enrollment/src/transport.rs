//! One invitation redemption per TLS 1.3 connection. This endpoint deliberately
//! requires server authentication before the client has a issued certificate.
//! Its distinct ALPN exposes only the bounded enrollment handler.
use crate::*;
use quinn::{
    Endpoint,
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
};
use serde::{Deserialize, Serialize};
use std::{
    future::{Future, poll_fn},
    net::SocketAddr,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::{Pin, pin},
    sync::Arc,
    task::Poll,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::Semaphore,
    task::JoinSet,
};
use zeroize::Zeroizing;

const MAGIC: &[u8; 8] = b"FCLENR01";
const HEADER: usize = 16;
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
    // Quinn requires shared transport/TLS configuration for its internal tasks.
    fn quic(&self) -> Result<Arc<quinn::TransportConfig>, EnrollmentError> {
        self.validate()?;
        let mut transport = quinn::TransportConfig::default();
        transport.max_concurrent_bidi_streams(1u8.into());
        transport.max_concurrent_uni_streams(0u8.into());
        transport.stream_receive_window(((MAX_MESSAGE_BYTES + HEADER) as u32).into());
        transport.receive_window(((MAX_MESSAGE_BYTES + HEADER) as u32).into());
        transport.send_window((MAX_MESSAGE_BYTES + HEADER) as u64);
        transport.max_idle_timeout(Some(
            self.timeout
                .try_into()
                .map_err(|_| EnrollmentError::Invalid)?,
        ));
        Ok(Arc::new(transport))
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

pub struct EnrollmentServer {
    endpoint: Endpoint,
    limits: TransportLimits,
}
impl EnrollmentServer {
    pub fn bind(
        address: SocketAddr,
        identity: &CredentialMaterial,
        limits: TransportLimits,
    ) -> Result<Self, JoinTransportError> {
        require_runtime()?;
        let mut config = quinn::ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(identity.server_config()?)
                .map_err(|_| EnrollmentError::Crypto)?,
        ));
        config.transport_config(limits.quic()?);
        let endpoint =
            Endpoint::server(config, address).map_err(|_| JoinTransportError::Unavailable)?;
        Ok(Self { endpoint, limits })
    }
    pub fn local_addr(&self) -> Result<SocketAddr, JoinTransportError> {
        self.endpoint
            .local_addr()
            .map_err(|_| JoinTransportError::Unavailable)
    }
    pub fn close(&self) {
        self.endpoint.close(0u8.into(), b"shutdown");
    }
    pub async fn serve<H: JoinHandler + Clone>(
        &self,
        handler: H,
    ) -> Result<(), JoinTransportError> {
        require_runtime()?;
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                incoming = self.endpoint.accept() => {
                    let Some(incoming) = incoming else { break };
                    if tasks.len() >= self.limits.max_connections { incoming.refuse(); continue; }
                    let handler = handler.clone();
                    let timeout = self.limits.timeout;
                    tasks.spawn(async move {
                        let _ = tokio::time::timeout(timeout, async move {
                            let connection = incoming.await.map_err(|_| JoinTransportError::Unavailable)?;
                            serve_enrollment_connection(connection, handler, timeout).await
                        }).await;
                    });
                }
                _ = tasks.join_next(), if !tasks.is_empty() => {}
            }
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        Ok(())
    }
}

/// Serve only enrollment on an already negotiated connection. A shared listener
/// must route by the negotiated ALPN before invoking this bounded handler.
pub async fn serve_enrollment_connection<H: JoinHandler>(
    connection: quinn::Connection,
    handler: H,
    timeout: Duration,
) -> Result<(), JoinTransportError> {
    require_runtime()?;
    if timeout.is_zero() || timeout > Duration::from_secs(120) {
        return Err(EnrollmentError::Invalid.into());
    }
    let handshake = connection
        .handshake_data()
        .ok_or(JoinTransportError::InvalidFrame)?
        .downcast::<quinn::crypto::rustls::HandshakeData>()
        .map_err(|_| JoinTransportError::InvalidFrame)?;
    if handshake.protocol.as_deref() != Some(ENROLLMENT_ALPN) {
        connection.close(1u8.into(), b"wrong enrollment protocol");
        return Err(JoinTransportError::InvalidFrame);
    }
    // Handle::try_current cannot detect a disabled time driver. Keep timeout
    // construction inside the polled boundary; a dependency unwind must not
    // escape this public adapter or resume a partly processed enrollment.
    let exchange = async {
        tokio::time::timeout(timeout, async {
            let (mut send, mut receive) = connection
                .accept_bi()
                .await
                .map_err(|_| JoinTransportError::Unavailable)?;
            let request: JoinRequest = read_frame(&mut receive, 1).await?;
            // A FIN, not a partial request or stalled suffix, admits metadata work.
            require_end(&mut receive).await?;
            let response = handler.handle(request).await;
            let response = if encode(&response).is_ok() {
                response
            } else {
                JoinResponse::Rejected(JoinFailure::OutcomeUnknown)
            };
            write_frame(&mut send, 2, &response).await?;
            send.finish()
                .map_err(|_| JoinTransportError::OutcomeUnknown)?;
            send.stopped()
                .await
                .map_err(|_| JoinTransportError::OutcomeUnknown)?;
            Ok(())
        })
        .await
        .map_err(|_| JoinTransportError::OutcomeUnknown)?
    };
    let mut exchange = pin!(exchange);
    let result = poll_fn(|context| {
        match catch_unwind(AssertUnwindSafe(|| exchange.as_mut().poll(context))) {
            Ok(poll) => poll.map(Ok),
            Err(_) => Poll::Ready(Err(JoinTransportError::OutcomeUnknown)),
        }
    })
    .await;
    match result {
        Ok(result) => result,
        Err(error) => {
            connection.close(1u8.into(), b"enrollment unavailable");
            Err(error)
        }
    }
}

pub struct EnrollmentClient {
    endpoint: Endpoint,
    limits: TransportLimits,
    inflight: Semaphore,
}
impl EnrollmentClient {
    pub fn bind(address: SocketAddr, limits: TransportLimits) -> Result<Self, JoinTransportError> {
        limits.validate()?;
        require_runtime()?;
        let endpoint = Endpoint::client(address).map_err(|_| JoinTransportError::Unavailable)?;
        let inflight = Semaphore::new(limits.max_connections);
        Ok(Self {
            endpoint,
            limits,
            inflight,
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
        let _permit = self
            .inflight
            .try_acquire()
            .map_err(|_| JoinTransportError::Unavailable)?;
        if invitation.cluster() != key.cluster() {
            return Err(EnrollmentError::WrongCluster.into());
        }
        let mut tls = quinn::ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(invitation.client_config()?)
                .map_err(|_| EnrollmentError::Crypto)?,
        ));
        tls.transport_config(self.limits.quic()?);
        let connecting = self
            .endpoint
            .connect_with(tls, address, &invitation.trust().server_name)
            .map_err(|_| JoinTransportError::Unavailable)?;
        let connection = tokio::time::timeout(self.limits.timeout, connecting)
            .await
            .map_err(|_| JoinTransportError::Unavailable)?
            .map_err(|_| JoinTransportError::Unavailable)?;
        // No application stream or token-bearing buffer exists before both
        // certificate validation and the operator-delivered leaf pin succeed.
        let request = invitation.request_after_quic(&connection, key, now)?;
        let response = tokio::time::timeout(self.limits.timeout, async {
            let (mut send, mut receive) = connection
                .open_bi()
                .await
                .map_err(|_| JoinTransportError::OutcomeUnknown)?;
            write_frame(&mut send, 1, &request).await?;
            send.finish()
                .map_err(|_| JoinTransportError::OutcomeUnknown)?;
            let response = read_frame(&mut receive, 2).await?;
            require_end(&mut receive).await?;
            Ok::<JoinResponse, JoinTransportError>(response)
        })
        .await
        .map_err(|_| JoinTransportError::OutcomeUnknown)?
        .map_err(|_| JoinTransportError::OutcomeUnknown)?;
        connection.close(0u8.into(), b"complete");
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
                    || pki::verify_issued(&receipt, &invitation.trust().ca_certificate).is_err()
                {
                    return Err(JoinTransportError::OutcomeUnknown);
                }
                Ok(receipt)
            }
        }
    }
    pub fn close(&self) {
        self.endpoint.close(0u8.into(), b"shutdown");
    }
}

async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    kind: u16,
    value: &T,
) -> Result<(), JoinTransportError> {
    let bytes = Zeroizing::new(encode(value)?);
    let mut header = [0; HEADER];
    header[..8].copy_from_slice(MAGIC);
    header[8..10].copy_from_slice(&1u16.to_be_bytes());
    header[10..12].copy_from_slice(&kind.to_be_bytes());
    header[12..].copy_from_slice(&(bytes.len() as u32).to_be_bytes());
    writer
        .write_all(&header)
        .await
        .map_err(|_| JoinTransportError::OutcomeUnknown)?;
    writer
        .write_all(&bytes)
        .await
        .map_err(|_| JoinTransportError::OutcomeUnknown)?;
    Ok(())
}
async fn read_frame<R: AsyncRead + Unpin, T: serde::de::DeserializeOwned>(
    reader: &mut R,
    kind: u16,
) -> Result<T, JoinTransportError> {
    let mut header = [0; HEADER];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|_| JoinTransportError::InvalidFrame)?;
    let length = u32::from_be_bytes(
        header[12..16]
            .try_into()
            .map_err(|_| JoinTransportError::InvalidFrame)?,
    ) as usize;
    if &header[..8] != MAGIC
        || header[8..10] != 1u16.to_be_bytes()
        || header[10..12] != kind.to_be_bytes()
        || length > MAX_MESSAGE_BYTES
    {
        return Err(JoinTransportError::InvalidFrame);
    }
    let mut bytes = Zeroizing::new(vec![0; length]);
    reader
        .read_exact(&mut bytes)
        .await
        .map_err(|_| JoinTransportError::InvalidFrame)?;
    decode(&bytes).map_err(|_| JoinTransportError::InvalidFrame)
}
async fn require_end<R: AsyncRead + Unpin>(reader: &mut R) -> Result<(), JoinTransportError> {
    let mut byte = [0];
    match reader.read(&mut byte).await {
        Ok(0) => Ok(()),
        _ => Err(JoinTransportError::InvalidFrame),
    }
}

fn require_runtime() -> Result<(), JoinTransportError> {
    tokio::runtime::Handle::try_current()
        .map(|_| ())
        .map_err(|_| JoinTransportError::Unavailable)
}
