//! Mutually authenticated TLS 1.3 over QUIC: hyper-transport's endpoint over hyper-quic and
//! hyper-tls, driven on hyper-rt by one owner task per socket ([`crate::net`], 27 §15). One
//! request is one exchange, its kind the operation's registered tag and its body the envelope;
//! slow work on one exchange does not hold up another, and the connection's credit is taken in the
//! order of the classes' urgency.
use crate::net::{
    Answer, Asked, Command, Driver, Failed, FocalClasses, Inbound, Kind, LeafPins, Linked,
    NetBudget, NetDirectory, Network, Reply, Sender, Serve, ServeFuture, Served,
};
use crate::*;
use focal_memory::MemoryBudget;
use hyper_transport::congestion::{Congestion, CubicConfig};
use hyper_transport::tls::Tls;
use hyper_transport::{Classes, Config, Endpoint, Limits, PathFacts, PeerId};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::{net::SocketAddr, time::Duration};

/// DER credentials supplied by the deployment's existing PKI. Private keys are
/// never Debug/Serialize and are never accepted in a request envelope.
pub struct TlsIdentity {
    pub certificate_chain: Vec<CertificateDer<'static>>,
    pub private_key: PrivateKeyDer<'static>,
}
impl TlsIdentity {
    pub fn from_pkcs8(certificate_chain: Vec<Vec<u8>>, private_key: Vec<u8>) -> Self {
        Self {
            certificate_chain: certificate_chain
                .into_iter()
                .map(CertificateDer::from)
                .collect(),
            private_key: PrivatePkcs8KeyDer::from(private_key).into(),
        }
    }
}
/// Longest silence before a QUIC connection is considered dead: what the
/// transport reclaims a vanished peer's connection by, with a keep-alive every
/// quarter of it, so a live peer is heard from several times in every window
/// however late its scheduler runs it. It is never a request's deadline. The
/// connection's liveness is its own: a request ends by its own progress
/// deadline and the connection carrying it stays; a restarted peer is told
/// sooner by its stateless reset (RFC 9000 §10.3), and node death is the
/// membership's to decide (SWIM with Lifeguard), not the transport's.
/// Bounding the idle period by the request timeout made a short deadline a
/// short liveness window: a process stalled for half a second under load lost
/// every connection it held (a busy machine's focal-wire run, 2026-10-07).
const IDLE_TIMEOUT: Duration = Duration::from_secs(10);
/// The most a stream is given to send ahead of what its reader has taken: a
/// megabyte, hyper-transport's stream window bound is the QUIC assembler's
/// span limit made explicit (`hyper_transport::stream_window_ceiling`), and
/// focal's content lane is sized in megabyte streams ([`content_streams`]).
pub const STREAM_WINDOW_CEILING: u32 = 1024 * 1024;
/// The path the content lane is sized for: what one gigabit per second
/// holds in flight over a hundred milliseconds (27 §7), a wide-area link
/// between data centres; a nearer path fills fewer of its streams and a
/// further one is not carried faster by more of them than the window
/// the law opens.
pub const REFERENCE_PATH_BITS_PER_SECOND: u64 = 1_000_000_000;
pub const REFERENCE_PATH_ROUND_TRIP: Duration = Duration::from_millis(100);
/// The streams a transfer to one peer may hold at once: what the
/// reference path holds in flight, a stream's window at a time, and one
/// ([`bulk_width`]): thirteen.
pub fn content_streams() -> u32 {
    let in_flight = REFERENCE_PATH_BITS_PER_SECOND
        .saturating_mul(u64::try_from(REFERENCE_PATH_ROUND_TRIP.as_millis()).unwrap_or(u64::MAX))
        .checked_div(8_000)
        .unwrap_or(0);
    let streams = in_flight
        .div_ceil(u64::from(STREAM_WINDOW_CEILING))
        .saturating_add(1);
    u32::try_from(streams).unwrap_or(u32::MAX)
}

/// By how many streams a transfer goes on a connection whose law holds
/// `window` bytes in flight, `most` at most: one for every megabyte the
/// window holds, and one. A stream carries no more than a megabyte in a
/// round trip ([`STREAM_WINDOW_CEILING`]), so a window of less is filled by
/// one; and the law opens its window no further than what is sent fills
/// it, so the stream that is one more is what lets it find that the path
/// holds more.
pub fn bulk_width(window: u64, most: usize) -> usize {
    usize::try_from(
        window
            .checked_div(u64::from(STREAM_WINDOW_CEILING))
            .unwrap_or(0),
    )
    .unwrap_or(usize::MAX)
    .saturating_add(1)
    .min(most.max(1))
}

/// The most exchanges one endpoint holds at once, both ways: its exchange table is allocated to
/// its bound, so the bound is stated rather than taken from the product of the connection and
/// stream bounds, which reaches 2^26. Sixty-four thousand is the most a node's listener carries:
/// `WireLimits::max_connections` at its largest default, a hundred and twenty-eight, each with the
/// consensus window's five hundred and twelve exchanges in flight.
pub const MAX_ENDPOINT_EXCHANGES: usize = 1 << 16;

/// The commands a handle may queue for its owner: one for each exchange it may have in flight,
/// so a handle never waits for the queue while the owner is moving.
fn command_queue(limits: &WireLimits) -> usize {
    endpoint_exchanges(limits).max(1)
}

fn endpoint_exchanges(limits: &WireLimits) -> usize {
    limits
        .max_connections
        .saturating_mul(limits.streams_per_connection as usize)
        .clamp(1, MAX_ENDPOINT_EXCHANGES)
}

/// The bounds of an endpoint carrying focal's traffic under `limits`, admitting by `admission`.
pub fn endpoint_limits(
    limits: &WireLimits,
    admission: &AdmissionLimits,
) -> Result<Limits, WireError> {
    limits.validate()?;
    let streams = limits.streams_per_connection;
    // A connection's receive window grows to what its streams hold at once, a stream's window
    // each, and never past the assembler's bound for a stream.
    let stream = u64::from(limits.max_frame_bytes)
        .min(hyper_transport::stream_window_ceiling())
        .min(u64::from(STREAM_WINDOW_CEILING));
    let window_ceiling =
        stream
            .saturating_mul(u64::from(streams))
            .max(hyper_transport::initial_window(
                hyper_transport::MIN_DATAGRAM,
            ));
    let receive_chunks = usize::try_from(window_ceiling)
        .unwrap_or(usize::MAX)
        .checked_div(hyper_transport::RECEIVE_CHUNK)
        .unwrap_or(0)
        .saturating_add(1);
    Ok(Limits {
        admission: hyper_transport::AdmissionLimits {
            pending: admission.pending.max(1),
            identities: admission.identities.max(1),
            connections: admission.connections.max(1),
            // An identity alone may hold the endpoint's connections; as others come each is left
            // a fair share (hyper-transport's max-min admission), and a connection is replaced
            // only when idle.
            per_identity: admission.connections.max(1),
        },
        streams_per_connection: streams,
        exchanges: endpoint_exchanges(limits),
        // focal's traffic is exchanges; no lane is opened.
        lanes_per_peer: 1,
        lane_window: 1,
        // focal's messages are their bodies: a head is refused from its prefix.
        max_head: 0,
        window_ceiling,
        serve_period: limits.request_timeout,
        idle_timeout: IDLE_TIMEOUT,
        keep_alive: IDLE_TIMEOUT.checked_div(4),
        max_responses: admission.pending.max(1),
        receive_chunks,
    })
}

/// What an endpoint under `limits` and `admission` holds of its budget at most, bytes, in two
/// parts: the transport's own — its socket's buffers, its receive pool and each connection's
/// receive window, charged to the ordinary lane — and its messages', a body each way on each
/// stream of each connection at the frame bound, charged to the lane its class is funded from. A budget of both funds the endpoint
/// so that the transport's own bounds refuse before the budget does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EndpointBytes {
    pub transport: u64,
    pub messages: u64,
}
pub fn endpoint_bytes(
    limits: &WireLimits,
    admission: &AdmissionLimits,
) -> Result<EndpointBytes, WireError> {
    let endpoint = endpoint_limits(limits, admission)?;
    let connections = u64::try_from(endpoint.admission.connections).unwrap_or(u64::MAX);
    let pool = u64::try_from(endpoint.receive_chunks)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(hyper_transport::RECEIVE_CHUNK).unwrap_or(u64::MAX));
    let messages = u64::from(limits.max_frame_bytes)
        .saturating_mul(2)
        .saturating_mul(u64::from(endpoint.streams_per_connection))
        .saturating_mul(connections);
    let socket = u64::try_from(endpoint_socket_bytes()).unwrap_or(u64::MAX);
    Ok(EndpointBytes {
        transport: endpoint
            .window_ceiling
            .saturating_mul(connections)
            .saturating_add(pool)
            .saturating_add(socket),
        messages,
    })
}

/// What an endpoint's socket is charged for its life, bytes: the most it holds, its receive
/// buffers and its outbox (`hyper_rt::udp::Io::held_bound`).
pub fn endpoint_socket_bytes() -> usize {
    hyper_rt::udp::Io { batch: IO_BATCH }.held_bound()
}

/// What one endpoint of `limits` may hold, as an allowance: [`endpoint_bytes`] at the admission
/// a connector of these limits keeps (`AdmissionLimits::for_connections`), its messages on the
/// completion lane kept for them. A child of `parent` where the endpoint is charged to its owner's
/// budget (a node's), a budget of its own where the endpoint is its own owner (a client's, a
/// join's); either way the transport's bounds refuse before the allowance does.
pub fn endpoint_budget(
    limits: &WireLimits,
    parent: Option<&MemoryBudget>,
) -> Result<MemoryBudget, WireError> {
    let admission = crate::AdmissionLimits::for_connections(limits.max_connections);
    let bytes = endpoint_bytes(limits, &admission)?;
    let limit = usize::try_from(bytes.transport.saturating_add(bytes.messages))
        .map_err(|_| WireError::Limit)?;
    let reserve = usize::try_from(bytes.messages).map_err(|_| WireError::Limit)?;
    match parent {
        Some(parent) => parent.child(limit, reserve),
        None => MemoryBudget::new(limit, reserve),
    }
    .map_err(|_| WireError::Limit)
}

/// The server side of focal's TLS: what a listener presents and verifies its clients by.
pub struct ServerTls(rustls::ServerConfig);
/// The client side of focal's TLS: what a dial presents and verifies its server by, and the role
/// its identity is granted as. A message's class is its kind's for its sender's role, decided alike
/// by the server, which knows the role from the grant, and by the dialer, which sends it in that
/// class (`net::classes`): a connector of a node's identity is a node's, any other a participant's,
/// a join's a joiner's. A connector told a role its identity is not granted has its exchanges
/// refused for their classes.
pub struct ClientTls {
    config: rustls::ClientConfig,
    role: Sender,
}
impl ServerTls {
    /// A configuration the caller built: its certificate resolver, client verifier and protocols
    /// (a listener that serves enrollment beside the data protocol). The endpoint holds it to the
    /// transport's rules: no early data, a protocol named.
    pub fn from_config(config: rustls::ServerConfig) -> Self {
        Self(config)
    }
    /// The configuration, for an endpoint a test drives itself.
    #[cfg(test)]
    pub(crate) fn into_config(self) -> rustls::ServerConfig {
        self.0
    }
}
impl ClientTls {
    /// A configuration the caller built (an enrollment's pinned verifier, without client auth),
    /// for an identity granted as `role`.
    pub fn from_config(config: rustls::ClientConfig, role: Sender) -> Self {
        Self { config, role }
    }
    /// The role the identity is granted as.
    pub fn role(&self) -> Sender {
        self.role
    }
    /// The configuration, for an endpoint a test drives itself.
    #[cfg(test)]
    pub(crate) fn into_config(self) -> rustls::ClientConfig {
        self.config
    }
}

pub fn server_tls(
    identity: TlsIdentity,
    client_roots: Vec<Vec<u8>>,
    limits: &WireLimits,
) -> Result<ServerTls, WireError> {
    limits.validate()?;
    let provider = crate::crypto_provider();
    // A client's chain is verified against the roots this server knows;
    // a successor issuer's endorsement by one of them is an ordinary
    // intermediate to it (`trust`).
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        crate::TrustRoots::new(client_roots)?.store()?,
        provider,
    )
    .build()
    .map_err(|_| WireError::Authentication)?;
    let mut tls = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| WireError::Authentication)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(identity.certificate_chain, identity.private_key)
        .map_err(|_| WireError::Authentication)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    Ok(ServerTls(tls))
}
pub fn client_tls(
    identity: TlsIdentity,
    server_roots: Vec<Vec<u8>>,
    limits: &WireLimits,
    role: Sender,
) -> Result<ClientTls, WireError> {
    limits.validate()?;
    let provider = crate::crypto_provider();
    // The server's chain is verified against the roots this client knows;
    // a successor issuer's endorsement by one of them is an ordinary
    // intermediate to it (`trust`).
    let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
        crate::TrustRoots::new(server_roots)?.store()?,
        provider,
    )
    .build()
    .map_err(|_| WireError::Authentication)?;
    client_tls_with(identity, verifier, limits, role)
}
/// [`client_tls`] for a client that adopts an issuer a verified chain
/// carried endorsed and it did not hold (24 §11): the configuration, and
/// where the adoption is read.
pub fn client_tls_adopting(
    identity: TlsIdentity,
    server_roots: Vec<Vec<u8>>,
    limits: &WireLimits,
    role: Sender,
) -> Result<(ClientTls, crate::AdoptedRoots), WireError> {
    limits.validate()?;
    let (verifier, adopted) = crate::AdoptingServerVerifier::new(
        crate::TrustRoots::new(server_roots)?,
        crate::crypto_provider(),
    )?;
    Ok((client_tls_with(identity, verifier, limits, role)?, adopted))
}
fn client_tls_with(
    identity: TlsIdentity,
    verifier: Box<dyn rustls::client::danger::ServerCertVerifier>,
    limits: &WireLimits,
    role: Sender,
) -> Result<ClientTls, WireError> {
    limits.validate()?;
    let mut tls = rustls::ClientConfig::builder_with_provider(crate::crypto_provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| WireError::Authentication)?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(identity.certificate_chain, identity.private_key)
        .map_err(|_| WireError::Authentication)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    Ok(ClientTls { config: tls, role })
}
/// The client side of a listener, which never dials: no root, so no server verifies, and the
/// data protocol named so the endpoint takes it.
pub(crate) fn listener_client() -> Result<rustls::ClientConfig, WireError> {
    let mut tls = rustls::ClientConfig::builder_with_provider(crate::crypto_provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| WireError::Authentication)?
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];
    Ok(tls)
}

/// The congestion law focal's connections run: Copa, chosen by measurement over focal's grids (27
/// §3.1 P10, F39): it stalled on no path and kept what is asked beside a transfer from waiting for
/// a full queue (hyper-raft docs/transport.md §4d).
fn congestion() -> Congestion {
    Congestion::Cubic(CubicConfig::default())
}

/// The datagrams one system call carries each way: hyper-rt's batch bound, at most the 64
/// segments of a Linux GSO/GRO send or receive.
const IO_BATCH: usize = 32;

/// An endpoint on `address`, its owner started on the current shard, woken at every withdrawal of
/// a grant by `revocations` when it serves under a registry.
#[allow(clippy::too_many_arguments)]
fn start(
    bind: Bind,
    (tls, role): (Tls, Sender),
    directory: NetDirectory,
    limits: &WireLimits,
    admission: &AdmissionLimits,
    budget: MemoryBudget,
    revocations: Option<hyper_rt::sync::WatchReceiver>,
    enrollment: Option<u32>,
) -> Result<(Network, SocketAddr), WireError> {
    let driver = endpoint_driver(bind, (tls, role), directory, limits, admission, budget)?;
    let local = driver.local_addr().map_err(|_| WireError::Connection)?;
    let network = Network::start(
        (driver, role),
        limits.clone(),
        command_queue(limits),
        revocations,
        enrollment,
    )?;
    Ok((network, local))
}

/// The enrollment protocol a listener serves beside the data protocol: its name in the TLS
/// handshake and the most one of its messages carries — what its own frames were held to, never
/// the data protocol's frame bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnrollmentProtocol {
    pub alpn: &'static [u8],
    pub max_message_bytes: u32,
}

/// Where an endpoint's socket comes from: an address bound for it on the current shard, or a socket
/// its caller bound and held from the moment it learnt the port (`hyper_rt::udp::UdpSocket::adopt`:
/// binding by number again would give the port up between the check and the use).
#[derive(Debug)]
pub enum Bind {
    Address(SocketAddr),
    Socket(std::net::UdpSocket),
}
impl From<SocketAddr> for Bind {
    fn from(address: SocketAddr) -> Self {
        Self::Address(address)
    }
}
impl From<std::net::UdpSocket> for Bind {
    fn from(socket: std::net::UdpSocket) -> Self {
        Self::Socket(socket)
    }
}

/// An endpoint carrying focal's traffic on `bind`, its socket the current shard's, and not yet
/// driven: what [`start`] gives an owner. `role` is what the endpoint's identity is granted as,
/// which with a message's kind gives the class it sends it in.
pub(crate) fn endpoint_driver(
    bind: impl Into<Bind>,
    (tls, role): (Tls, Sender),
    directory: NetDirectory,
    limits: &WireLimits,
    admission: &AdmissionLimits,
    budget: MemoryBudget,
) -> Result<Driver, WireError> {
    let config = Config {
        tls,
        role,
        limits: endpoint_limits(limits, admission)?,
        congestion: congestion(),
    };
    let io = hyper_rt::udp::Io { batch: IO_BATCH };
    // The socket's buffers are the endpoint's from the start: charged their bound now.
    let budget = NetBudget::new(budget, endpoint_socket_bytes()).map_err(|_| WireError::Limit)?;
    // Made the instant before the driver anchors its clock to the shard's: every later instant
    // the driver gives it is that anchor's or after.
    let endpoint: Endpoint<FocalClasses, NetBudget, NetDirectory> =
        Endpoint::new(config, budget, directory, std::time::Instant::now())
            .map_err(|_| WireError::Authentication)?;
    match bind.into() {
        Bind::Address(address) => Driver::bind(endpoint, address, io),
        Bind::Socket(socket) => hyper_rt::udp::UdpSocket::adopt(socket.into())
            .and_then(|socket| hyper_rt::udp::Batched::new(socket, io, false))
            .and_then(|socket| Driver::new(endpoint, socket)),
    }
    .map_err(|_| WireError::Connection)
}

pub struct QuicServer {
    network: Network,
    address: SocketAddr,
    registry: PeerRegistry,
    limits: WireLimits,
}
impl QuicServer {
    /// `budget` funds what the server reads (the audit's F03): every body is reserved from it
    /// before any of it is read, against its sender's share.
    pub fn bind(
        address: impl Into<Bind>,
        tls: ServerTls,
        registry: PeerRegistry,
        limits: WireLimits,
        budget: MemoryBudget,
    ) -> Result<Self, WireError> {
        let admission = crate::AdmissionLimits::for_connections(limits.max_connections)
            .replacing_after(limits.request_timeout);
        Self::bind_admitting(address, tls, registry, limits, admission, budget)
    }
    /// [`Self::bind`] with the admission bounds stated.
    pub fn bind_admitting(
        address: impl Into<Bind>,
        tls: ServerTls,
        registry: PeerRegistry,
        limits: WireLimits,
        admission: crate::AdmissionLimits,
        budget: MemoryBudget,
    ) -> Result<Self, WireError> {
        Self::bind_serving(address, tls, registry, None, limits, admission, budget)
    }
    /// [`Self::bind_admitting`] for a listener that also serves the `enrollment` protocol (its
    /// `tls` names it beside the data protocol): a joiner on it is a peer of its own, whose role
    /// lets it send the enrollment kinds and nothing else, each message held to the protocol's
    /// bound.
    pub fn bind_serving(
        address: impl Into<Bind>,
        tls: ServerTls,
        registry: PeerRegistry,
        enrollment: Option<EnrollmentProtocol>,
        limits: WireLimits,
        admission: crate::AdmissionLimits,
        budget: MemoryBudget,
    ) -> Result<Self, WireError> {
        limits.validate()?;
        // The directory names every identity the admission holds and every handshake in flight,
        // so the admission is what refuses an identity past its bound — typed, retryable — and
        // never the directory, which would tell a granted certificate it is refused.
        let directory = NetDirectory::serving(
            registry.clone(),
            enrollment.map(|protocol| protocol.alpn),
            admission.identities.saturating_add(admission.pending),
        );
        let tls = Tls {
            server: Some(tls.0),
            client: listener_client()?,
        };
        // A listener is a node's: what it sends, it sends as one.
        let (network, address) = start(
            address.into(),
            (tls, Sender::Node),
            directory,
            &limits,
            &admission,
            budget,
            registry.watch(),
            enrollment.map(|protocol| protocol.max_message_bytes),
        )?;
        Ok(Self {
            network,
            address,
            registry,
            limits,
        })
    }
    /// The server's owner, for a test that fills its queue.
    #[cfg(test)]
    pub(crate) fn network(&self) -> &Network {
        &self.network
    }
    /// Who holds this server's connections, and what it refused.
    pub async fn admission(&self) -> AdmissionStats {
        let stats = self
            .network
            .call(|reply| Command::Stats { reply })
            .await
            .unwrap_or_default();
        AdmissionStats::of(&stats)
    }
    pub fn local_addr(&self) -> Result<SocketAddr, WireError> {
        Ok(self.address)
    }
    /// Present `tls` to every handshake from now on: a renewed identity, roots that changed.
    pub async fn replace_tls(&self, tls: ServerTls) -> Result<(), WireError> {
        self.identity().replace(tls).await
    }
    /// A handle that presents another identity to this server's next handshakes, held apart from
    /// the server (a credential's renewal swaps it while the server serves).
    pub fn identity(&self) -> ServerIdentity {
        ServerIdentity {
            network: self.network.clone(),
        }
    }
    /// Waits until the server has ended — closed, its endpoint and socket gone, so an address it
    /// held is free again.
    pub async fn ended(&self) {
        let Ok((done, ended)) = hyper_rt::sync::oneshot() else {
            return;
        };
        if self.network.send(Command::Ended { done }).await.is_ok() {
            // Told as the owner ends, or dropped by an owner that ended first.
            let _ = ended.await;
        }
    }
    /// Closes the server: its connections close and `serve` returns. Refused only when the shard
    /// has no task left to queue it (`Limit`); a server already closed is closed.
    pub fn close(&self) -> Result<(), WireError> {
        match self.network.send_now(Command::Close) {
            Err(WireError::Connection) => Ok(()),
            closed => closed,
        }
    }
    /// Serves requests with `handler` until the server is closed.
    pub async fn serve<H: RequestHandler + Clone>(&self, handler: H) -> Result<(), WireError> {
        self.serve_with(DataService::new(
            handler,
            self.registry.clone(),
            self.limits.clone(),
        ))
        .await
    }
    /// Serves with `serve`, which answers the data protocol and whatever else the listener
    /// carries, until the server is closed.
    pub async fn serve_with<S: Serve>(&self, serve: S) -> Result<(), WireError> {
        // Off a shard nothing could be served: refused, before an ended owner is told apart.
        crate::require_runtime()?;
        let (done, ended) = hyper_rt::sync::oneshot().map_err(|_| WireError::Connection)?;
        match self
            .network
            .send(Command::Serve {
                serve: Box::new(serve),
                done,
            })
            .await
        {
            // A server closed before it served has ended all the same.
            Err(WireError::Connection) => return Ok(()),
            sent => sent?,
        }
        // The owner ending without a word is an end all the same.
        let _ = ended.await;
        Ok(())
    }
}

/// focal's data protocol served by a [`RequestHandler`]: a request is decoded, held to the kind
/// and the profile it came under and to the grant current now, dispatched, and its answer
/// encoded under the profile.
#[derive(Clone)]
pub struct DataService<H> {
    handler: H,
    registry: PeerRegistry,
    limits: WireLimits,
}
impl<H: RequestHandler + Clone> DataService<H> {
    pub fn new(handler: H, registry: PeerRegistry, limits: WireLimits) -> Self {
        Self {
            handler,
            registry,
            limits,
        }
    }
    /// The answer to `served`, a request of the data protocol: `None` resets the exchange.
    pub fn answer(&self, served: Served) -> ServeFuture {
        let this = self.clone();
        Box::pin(async move { this.answer_now(served).await })
    }
    async fn answer_now(self, served: Served) -> Option<Reply> {
        let Inbound::Data {
            fingerprint,
            peer: authenticated,
        } = served.from
        else {
            return None;
        };
        let Kind::Operation(tag) = served.kind else {
            return None;
        };
        let negotiated = served.negotiated?;
        let request: RequestEnvelope = decode_payload(&served.body).ok()?;
        // The operation is the one its kind named, of the class its sender's role carries it in.
        let carried = FocalClasses::class_of(
            Kind::Operation(request.operation.registered_tag()),
            Sender::of(authenticated.role()),
        );
        if request.operation.registered_tag() != tag || carried != Some(served.class) {
            return None;
        }
        let mut limits = self.limits.clone();
        limits.max_frame_bytes = negotiated.max_frame_bytes;
        limits.max_items = negotiated.max_items;
        let response = if negotiated.accepts_protocol(request.protocol) {
            // What is dispatched is authorized by the grant current now, never by one captured
            // before its body arrived (the audit's F35).
            let peer = self.registry.authenticate(fingerprint).ok()?;
            dispatch_accounted_by(
                &self.handler,
                peer,
                request,
                &limits,
                (served.path, served.arrived_ns),
            )
            .await
        } else {
            OwnedResponse::new(request.reply(Response::Error(AccessError::UnsupportedProtocol)))
        };
        let body = encode_payload(response.envelope(), negotiated.max_frame_bytes).ok()?;
        // The envelope goes; what its handler charged for it stays with the encoding until the
        // endpoint has taken its last byte.
        Some(Reply {
            body,
            charge: Some(response.into_charge()),
        })
    }
}
impl<H: RequestHandler + Clone> Serve for DataService<H> {
    fn capabilities(&self) -> [bool; 4] {
        [
            self.handler.supports_managed_requests(),
            self.handler.supports_participant_requests(),
            self.handler.supports_native_requests(),
            self.handler.supports_ordered_replication(),
        ]
    }
    fn serve(&self, request: Served) -> ServeFuture {
        self.answer(request)
    }
}

pub struct QuicConnector {
    network: Network,
    limits: WireLimits,
    /// What the connector's identity is granted as: a replacement identity must be the same.
    role: Sender,
    /// The profiles offered in every Hello, the newest first: all this
    /// binary speaks, unless [`Self::offering`] made it an older one.
    offers: Vec<u16>,
}
/// Every profile this binary speaks, the newest first: what a connector
/// offers in its Hello.
pub const OFFERED_PROTOCOLS: [u16; 5] = [
    crate::ORDERED_PROTOCOL_VERSION,
    crate::NATIVE_PROTOCOL_VERSION,
    PEER_PROTOCOL_VERSION,
    MANAGED_PROTOCOL_VERSION,
    PROTOCOL_VERSION,
];
impl QuicConnector {
    pub fn limits(&self) -> &WireLimits {
        &self.limits
    }
    /// A connector on `address`, its endpoint's owner on the current shard. `budget` funds the
    /// answers it reads; a connector dials at most `limits.max_connections` routes at once.
    pub fn bind(
        address: SocketAddr,
        tls: ClientTls,
        limits: WireLimits,
        budget: MemoryBudget,
    ) -> Result<Self, WireError> {
        limits.validate()?;
        let admission = crate::AdmissionLimits::for_connections(limits.max_connections);
        let directory = NetDirectory::dialing(limits.max_connections);
        let role = tls.role;
        let tls = Tls {
            server: None,
            client: tls.config,
        };
        let (network, _) = start(
            Bind::Address(address),
            (tls, role),
            directory,
            &limits,
            &admission,
            budget,
            None,
            None,
        )?;
        Ok(Self {
            network,
            limits,
            role,
            offers: OFFERED_PROTOCOLS.to_vec(),
        })
    }
    /// A connector that is its own owner — a client's, a join's — its endpoint funded by a budget
    /// of its own sized to `limits` ([`endpoint_budget`]).
    pub fn bind_standalone(
        address: SocketAddr,
        tls: ClientTls,
        limits: WireLimits,
    ) -> Result<Self, WireError> {
        let budget = endpoint_budget(&limits, None)?;
        Self::bind(address, tls, limits, budget)
    }
    /// A connector that offers only `versions` in its Hello: what a
    /// connector of an older binary offers, for a fleet's tests of a mixed
    /// window (27 §12). The base profile is always among them.
    pub fn offering(mut self, versions: &[u16]) -> Result<Self, WireError> {
        if versions.is_empty()
            || versions.len() > OFFERED_PROTOCOLS.len()
            || !versions.contains(&PROTOCOL_VERSION)
            || versions
                .iter()
                .any(|version| !OFFERED_PROTOCOLS.contains(version))
        {
            return Err(WireError::Limit);
        }
        self.offers = versions.to_vec();
        Ok(self)
    }
    /// Present another client identity on every connection opened from now
    /// on; connections already open keep the identity they were opened with.
    /// The identity is granted as the one it replaces (a renewal), or it is
    /// refused: the connector's messages are sent in its role's classes.
    pub async fn replace_tls(&self, tls: ClientTls) -> Result<(), WireError> {
        if tls.role != self.role {
            return Err(WireError::Limit);
        }
        let tls = Box::new(Tls {
            server: None,
            client: tls.config,
        });
        self.network
            .call(|reply| Command::SetTls { tls, reply })
            .await?
    }
    /// A clonable handle that opens connections on this connector's endpoint, with the identity
    /// it presents when each is opened.
    pub fn dialer(&self) -> Result<QuicDialer, WireError> {
        Ok(QuicDialer {
            network: self.network.clone(),
            limits: self.limits.clone(),
            offers: self.offers.clone(),
        })
    }
    pub async fn connect(
        &self,
        address: SocketAddr,
        server_name: &str,
    ) -> Result<QuicRemote, WireError> {
        open_remote(
            &self.network,
            &self.limits,
            &self.offers,
            address,
            server_name,
            false,
        )
        .await
    }
    /// A connection to `address` of the caller's alone: never the route's to `address` that
    /// [`Self::connect`] shares, never found by another call, and forgotten once it closes — a
    /// probe of exactly an address that closes its connection after (24 §24) leaves the route's
    /// alone.
    pub async fn connect_alone(
        &self,
        address: SocketAddr,
        server_name: &str,
    ) -> Result<QuicRemote, WireError> {
        open_remote(
            &self.network,
            &self.limits,
            &self.offers,
            address,
            server_name,
            true,
        )
        .await
    }
    /// A connection of the enrollment protocol to the server at `address` named `server_name`:
    /// the caller's alone, and ungreeted, its joiner sending the enrollment kinds alone. The
    /// server must present a leaf `pins` accepts beside the chain and name the connector's TLS
    /// verified: one that chains but is not pinned is refused [`WireError::Unpinned`], and no
    /// message is made for it.
    pub async fn enrollment(
        &self,
        address: SocketAddr,
        server_name: &str,
        pins: LeafPins,
    ) -> Result<EnrollmentLink, WireError> {
        if server_name.len() > 253 {
            return Err(WireError::Limit);
        }
        let name = server_name.to_owned();
        let linked: Linked = self
            .network
            .call(|reply| Command::Connect {
                address,
                name,
                pin: Some(pins),
                offers: Vec::new(),
                alone: true,
                greet: false,
                reply,
            })
            .await??;
        Ok(EnrollmentLink {
            network: self.network.clone(),
            peer: linked.peer,
            epoch: linked.epoch,
        })
    }
    /// What the connector's endpoint holds and refused.
    pub async fn stats(&self) -> crate::net::NetStats {
        self.network
            .call(|reply| Command::Stats { reply })
            .await
            .unwrap_or_default()
    }
    /// Closes the connector's endpoint, its connections with it. Refused only when the shard has
    /// no task left to queue it (`Limit`); an endpoint already closed is closed.
    pub fn close(&self) -> Result<(), WireError> {
        match self.network.send_now(Command::Close) {
            Err(WireError::Connection) => Ok(()),
            closed => closed,
        }
    }
}
/// A connection of the enrollment protocol ([`QuicConnector::enrollment`]), closed as it is
/// dropped. A close its shard cannot queue (its tasks all taken) leaves the connection to its idle
/// timeout, as an abandoned connection is.
pub struct EnrollmentLink {
    network: Network,
    peer: PeerId,
    epoch: hyper_transport::Epoch,
}
impl EnrollmentLink {
    /// One message of the enrollment protocol, `body` of kind `tag`, and its answer's body, held
    /// to the connector's `max_frame_bytes`; `period` is the server's time to answer once it has
    /// the message. The message is wiped however the exchange ends.
    pub async fn ask(
        &self,
        tag: u8,
        body: Vec<u8>,
        period: Duration,
    ) -> Result<Vec<u8>, WireError> {
        let kind = Kind::Enrollment(tag);
        let body = Asked::of(kind, body);
        let answered: Result<Answer, Failed> = self
            .network
            .call(|reply| Command::Ask {
                peer: self.peer,
                epoch: self.epoch,
                kind,
                body,
                period,
                reply,
            })
            .await?;
        answered
            .map(|answer| answer.body)
            .map_err(|failed| failed.error)
    }
}
impl Drop for EnrollmentLink {
    fn drop(&mut self) {
        let _ = self
            .network
            .send_now(Command::Disconnect { peer: self.peer });
    }
}
/// A handle that presents another TLS identity to a server's next handshakes; clones share it.
#[derive(Clone)]
pub struct ServerIdentity {
    network: Network,
}
impl ServerIdentity {
    /// Present `tls` to every handshake from now on: a renewed identity, roots that changed.
    pub async fn replace(&self, tls: ServerTls) -> Result<(), WireError> {
        let tls = Box::new(Tls {
            server: Some(tls.0),
            client: listener_client()?,
        });
        self.network
            .call(|reply| Command::SetTls { tls, reply })
            .await?
    }
}
/// The connector's endpoint, as a handle: connections it opens are the connector's own (same
/// endpoint, same certificate check by `server_name`).
#[derive(Clone)]
pub struct QuicDialer {
    network: Network,
    limits: WireLimits,
    offers: Vec<u16>,
}
impl QuicDialer {
    pub async fn connect(
        &self,
        address: SocketAddr,
        server_name: &str,
    ) -> Result<QuicRemote, WireError> {
        open_remote(
            &self.network,
            &self.limits,
            &self.offers,
            address,
            server_name,
            false,
        )
        .await
    }
}
async fn open_remote(
    network: &Network,
    limits: &WireLimits,
    offers: &[u16],
    address: SocketAddr,
    server_name: &str,
    alone: bool,
) -> Result<QuicRemote, WireError> {
    if server_name.len() > 253 {
        return Err(WireError::Limit);
    }
    let name = server_name.to_owned();
    let offers = offers.to_vec();
    let linked: Linked = network
        .call(|reply| Command::Connect {
            address,
            name,
            pin: None,
            offers,
            alone,
            greet: true,
            reply,
        })
        .await??;
    let negotiated = linked.negotiated.ok_or(WireError::InvalidFrame)?;
    if !matches!(
        negotiated.protocol,
        PROTOCOL_VERSION
            | MANAGED_PROTOCOL_VERSION
            | PEER_PROTOCOL_VERSION
            | crate::NATIVE_PROTOCOL_VERSION
            | crate::ORDERED_PROTOCOL_VERSION
    ) {
        return Err(WireError::InvalidFrame);
    }
    Ok(QuicRemote {
        network: network.clone(),
        peer: linked.peer,
        epoch: linked.epoch,
        negotiated,
        limits: limits.clone(),
    })
}

/// An exchange's answer, and what the path to its peer measured when it came.
#[derive(Debug)]
pub struct Exchanged {
    pub response: ResponseEnvelope,
    pub path: Option<PathFacts>,
}
/// Why an exchange ended unanswered, and whether its connection is gone with it.
#[derive(Debug)]
pub struct Unanswered {
    pub error: WireError,
    pub closed: bool,
}

/// A greeted connection to a peer, as a handle: clones share it.
#[derive(Clone)]
pub struct QuicRemote {
    network: Network,
    peer: PeerId,
    epoch: hyper_transport::Epoch,
    negotiated: Negotiated,
    limits: WireLimits,
}
impl QuicRemote {
    pub fn negotiated(&self) -> Negotiated {
        self.negotiated
    }
    /// Closes the connection; exchanges under way on it are refused. Refused only when the shard
    /// has no task left to queue it (`Limit`); a connection already gone is closed.
    pub fn close(&self) -> Result<(), WireError> {
        match self
            .network
            .send_now(Command::Disconnect { peer: self.peer })
        {
            Err(WireError::Connection) => Ok(()),
            closed => closed,
        }
    }
    /// What the path to the peer measures now, while the connection is up.
    pub async fn path(&self) -> Option<PathFacts> {
        self.network
            .call(|reply| Command::Path {
                peer: self.peer,
                reply,
            })
            .await
            .ok()
            .flatten()
    }
    pub async fn request(&self, request: &RequestEnvelope) -> Result<ResponseEnvelope, WireError> {
        self.request_within(request, self.limits.request_timeout)
            .await
    }
    /// An exchange whose peer is given `period` to answer what it has been asked once it has it:
    /// judged by its progress (hyper-transport's `Progress`), never by a wall-clock bound on the
    /// whole.
    pub async fn request_within(
        &self,
        request: &RequestEnvelope,
        period: Duration,
    ) -> Result<ResponseEnvelope, WireError> {
        self.exchange_within(request, period)
            .await
            .map(|exchanged| exchanged.response)
            .map_err(|unanswered| unanswered.error)
    }
    /// [`Self::request_within`], with what the path measured when the answer came, or whether
    /// the connection went with the exchange.
    pub async fn exchange_within(
        &self,
        request: &RequestEnvelope,
        period: Duration,
    ) -> Result<Exchanged, Unanswered> {
        let unanswered = |error| Unanswered {
            error,
            closed: false,
        };
        if !self.negotiated.accepts_protocol(request.protocol) {
            return Err(unanswered(WireError::Access(
                AccessError::UnsupportedProtocol,
            )));
        }
        let body = encode_payload(request, self.negotiated.max_frame_bytes).map_err(unanswered)?;
        let kind = Kind::Operation(request.operation.registered_tag());
        let answered: Result<Answer, Failed> = self
            .network
            .call(|reply| Command::Ask {
                peer: self.peer,
                epoch: self.epoch,
                kind,
                body: Asked::of(kind, body),
                period,
                reply,
            })
            .await
            .map_err(|error| Unanswered {
                error,
                closed: true,
            })?;
        let answer = answered.map_err(|failed| Unanswered {
            error: failed.error,
            closed: failed.closed,
        })?;
        let response: ResponseEnvelope = decode_payload(&answer.body).map_err(unanswered)?;
        validate_response(request, &response, None, &self.limits).map_err(unanswered)?;
        Ok(Exchanged {
            response,
            path: answer.path,
        })
    }
}

/// The connections a participant keeps to the routes it reaches — its initial endpoint and the
/// hints its requests are redirected to — one per route, dialed once (the audit's F60): its
/// connector's endpoint keeps them, and concurrent cold calls to one route wait on the one dial
/// in flight. A route's name is resolved afresh on each dial. The connection last handed out for
/// each route is remembered, so a failure its caller reports closes that connection and no later
/// one.
pub struct RouteConnections {
    connector: QuicConnector,
    max_routes: usize,
    /// The connection each route was last handed, by its endpoint and server name; at most
    /// `max_routes`, the one handed longest ago going first.
    handed: std::sync::Mutex<std::collections::VecDeque<((String, String), QuicRemote)>>,
    /// Connections it closed whose close their endpoint could not queue (the shard's tasks all
    /// taken): each idles out at its connection's idle timeout instead.
    closes_refused: std::sync::atomic::AtomicU64,
}
/// A connection the cache handed out.
pub struct Connected {
    pub remote: QuicRemote,
    pub generation: u64,
}
/// The addresses a name resolves to that a dial tries: the resolver's first.
const RESOLVED: usize = 1;
impl RouteConnections {
    pub fn new(connector: QuicConnector, max_routes: usize) -> Result<Self, WireError> {
        if max_routes == 0 || max_routes > 1024 {
            return Err(WireError::Limit);
        }
        Ok(Self {
            connector,
            max_routes,
            handed: std::sync::Mutex::new(std::collections::VecDeque::new()),
            closes_refused: std::sync::atomic::AtomicU64::new(0),
        })
    }
    pub fn limits(&self) -> &WireLimits {
        self.connector.limits()
    }
    /// The dials the routes' endpoint started: a route's first, and each after its connection
    /// went.
    pub async fn dials(&self) -> u64 {
        self.connector.stats().await.dials
    }
    /// Connections it closed whose close their endpoint could not queue.
    pub fn closes_refused(&self) -> u64 {
        self.closes_refused
            .load(std::sync::atomic::Ordering::Relaxed)
    }
    /// Closes `remote`, counting a close its endpoint could not queue.
    fn close(&self, remote: &QuicRemote) {
        if remote.close().is_err() {
            let _ = self.closes_refused.fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |count| Some(count.saturating_add(1)),
            );
        }
    }
    /// The routes this cache may hold.
    pub fn max_routes(&self) -> usize {
        self.max_routes
    }
    /// The connection to `endpoint` under `server_name`.
    pub async fn connect(&self, endpoint: &str, server_name: &str) -> Result<Connected, WireError> {
        if endpoint.len() > 512 || server_name.len() > 253 {
            return Err(WireError::Limit);
        }
        let address = resolve(endpoint, self.limits().request_timeout).await?;
        let remote = self.connector.connect(address, server_name).await?;
        let route = (endpoint.to_owned(), server_name.to_owned());
        if let Ok(mut handed) = self.handed.lock() {
            handed.retain(|(held, _)| *held != route);
            if handed.len() >= self.max_routes {
                handed.pop_front();
            }
            handed.push_back((route, remote.clone()));
        }
        Ok(Connected {
            generation: remote.epoch,
            remote,
        })
    }
    /// `request` on the route's connection, dialed if none is up. The connection is closed only
    /// when it is what failed — closed, or not speaking the protocol
    /// ([`crate::connection_failed`], the peer pool's rule); a request that timed out or was
    /// refused on a connection that carries the caller's other requests failed alone.
    pub async fn request(
        &self,
        endpoint: &str,
        server_name: &str,
        request: &RequestEnvelope,
    ) -> Result<ResponseEnvelope, WireError> {
        let connected = self.connect(endpoint, server_name).await?;
        let result = connected
            .remote
            .exchange_within(request, self.limits().request_timeout)
            .await;
        match result {
            Ok(exchanged) => Ok(exchanged.response),
            Err(unanswered) => {
                if crate::connection_failed(unanswered.closed, &unanswered.error, true) {
                    self.close(&connected.remote);
                }
                Err(unanswered.error)
            }
        }
    }
    /// Close the route's connection of `generation`, which its caller found failed: the next call
    /// dials anew. A failure of a connection no longer the route's forgets nothing.
    pub fn forget(&self, endpoint: &str, server_name: &str, generation: u64) {
        let Ok(mut handed) = self.handed.lock() else {
            return;
        };
        let at = handed.iter().position(|((held, name), remote)| {
            held == endpoint && name == server_name && remote.epoch == generation
        });
        if let Some((_, remote)) = at.and_then(|at| handed.remove(at)) {
            self.close(&remote);
        }
    }
}

/// `endpoint` as an address: itself when it is one, else its host resolved, waited for no longer
/// than `period`.
async fn resolve(endpoint: &str, period: Duration) -> Result<SocketAddr, WireError> {
    resolve_all(endpoint, RESOLVED, period)
        .await?
        .first()
        .copied()
        .ok_or(WireError::Connection)
}
/// `endpoint` (`host:port`) as at most `most` addresses: itself when it is one, else what the
/// platform's resolver gives its host, in its order, waited for no longer than `period`
/// (`Timeout` past it). The resolver runs on the process's blocking pool under its DNS share.
pub async fn resolve_all(
    endpoint: &str,
    most: usize,
    period: Duration,
) -> Result<Vec<SocketAddr>, WireError> {
    if let Ok(address) = endpoint.parse::<SocketAddr>() {
        return Ok(vec![address]);
    }
    let (host, port) = endpoint.rsplit_once(':').ok_or(WireError::Connection)?;
    let port: u16 = port.parse().map_err(|_| WireError::Connection)?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    // Off a shard the wait for an answer cannot be kept: refused before any lookup is asked of
    // the pool, which would run it for nobody.
    crate::require_runtime()?;
    // Resolution runs on the process's blocking pool, under its share: started here once, so no
    // caller of a name depends on another having started it.
    focal_rt::blocking().map_err(|_| WireError::Connection)?;
    let nanos = u64::try_from(period.as_nanos()).unwrap_or(u64::MAX);
    hyper_rt::futures::within(nanos, hyper_rt::dns::resolve(host, port, most))
        .await
        .map_err(|_| WireError::Connection)?
        .ok_or(WireError::Timeout)?
        .map_err(|_| WireError::Connection)
}
