use std::{
    cmp,
    collections::{HashMap, VecDeque},
    io::{self, Write},
    mem,
    net::{Ipv6Addr, SocketAddr},
    ops::RangeFrom,
    str,
};

use lazy_static::lazy_static;

use assert_matches::assert_matches;
use bytes::BytesMut;
use rustls::{
    client::WebPkiServerVerifier,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use tracing::{info_span, trace};

use super::crypto::rustls::{QuicClientConfig, QuicServerConfig, node_provider};
use super::*;
use crate::{Duration, Instant};

pub(super) const DEFAULT_MTU: usize = 1452;
/// The server's port in every pair, upstream's first
const SERVER_PORT: u16 = 4433;
/// The client's first port in every pair, upstream's first
const FIRST_CLIENT_PORT: u16 = 44433;

/// An endpoint configuration whose connections do not warm their paths up
/// ([`CarefulResumeConfig::warm_up`]): for tests that pin another mechanism's exact traffic, which
/// the warm-up's packets would join
pub(super) fn endpoint_config_without_warm_up() -> EndpointConfig {
    let mut resume = CarefulResumeConfig::default();
    resume.warm_up(false);
    let mut config = EndpointConfig::default();
    config.careful_resume(Some(resume));
    config
}

pub(super) struct Pair {
    pub(super) server: TestEndpoint,
    pub(super) client: TestEndpoint,
    /// Start time
    epoch: Instant,
    /// Current time
    pub(super) time: Instant,
    /// Simulates the maximum size allowed for UDP payloads by the link (packets exceeding this size will be dropped)
    pub(super) mtu: usize,
    /// Simulates explicit congestion notification
    pub(super) congestion_experienced: bool,
    // One-way
    pub(super) latency: Duration,
    /// Number of spin bit flips
    pub(super) spins: u64,
    last_spin: bool,
    /// The client ports not yet used, in order
    client_ports: RangeFrom<u16>,
}

impl Pair {
    pub(super) fn default_with_deterministic_pns() -> Self {
        Self::deterministic_pns(Default::default())
    }

    /// A pair whose server numbers its packets deterministically, its endpoints configured by
    /// `endpoint_config`
    pub(super) fn deterministic_pns(endpoint_config: EndpointConfig) -> Self {
        let mut cfg = server_config();
        let mut transport = TransportConfig::default();
        transport.deterministic_packet_numbers(true);
        cfg.transport = transport;
        Self::new(endpoint_config, cfg)
    }

    pub(super) fn new(endpoint_config: EndpointConfig, server_config: ServerConfig) -> Self {
        let server =
            Endpoint::new(endpoint_config.clone(), Some(server_config), true, None).unwrap();
        let client = Endpoint::new(endpoint_config, None, true, None).unwrap();

        Self::new_from_endpoint(client, server)
    }

    pub(super) fn new_from_endpoint(client: Endpoint, server: Endpoint) -> Self {
        // The pair's network is its own, in memory, so its addresses need be distinct only from
        // one another: each pair numbers its client's ports itself, where upstream drew every
        // pair's ports from process-wide counters behind locks.
        let mut client_ports = FIRST_CLIENT_PORT..;
        let server_addr = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), SERVER_PORT);
        let client_addr = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), client_ports.next().unwrap());
        let now = Instant::now();
        Self {
            server: TestEndpoint::new(server, server_addr),
            client: TestEndpoint::new(client, client_addr),
            epoch: now,
            time: now,
            mtu: DEFAULT_MTU,
            latency: Duration::ZERO,
            spins: 0,
            last_spin: false,
            congestion_experienced: false,
            client_ports,
        }
    }

    /// A client address the pair has not used, for a test that moves the client
    pub(super) fn fresh_client_addr(&mut self) -> SocketAddr {
        SocketAddr::new(
            Ipv6Addr::LOCALHOST.into(),
            self.client_ports.next().unwrap(),
        )
    }

    /// Returns whether the connection is not idle
    pub(super) fn step(&mut self) -> bool {
        self.drive_client();
        self.drive_server();
        if self.client.is_idle() && self.server.is_idle() {
            return false;
        }

        let client_t = self.client.next_wakeup();
        let server_t = self.server.next_wakeup();
        match min_opt(client_t, server_t) {
            Some(t) if Some(t) == client_t => {
                if t != self.time {
                    self.time = self.time.max(t);
                    trace!("advancing to {:?} for client", self.time - self.epoch);
                }
                true
            }
            Some(t) if Some(t) == server_t => {
                if t != self.time {
                    self.time = self.time.max(t);
                    trace!("advancing to {:?} for server", self.time - self.epoch);
                }
                true
            }
            Some(_) => unreachable!(),
            None => false,
        }
    }

    /// Advance time until both connections are idle
    pub(super) fn drive(&mut self) {
        while self.step() {}
    }

    /// Advance time until both connections are idle, or after 100 steps have been executed
    ///
    /// Returns true if the amount of steps exceeds the bounds, because the connections never became
    /// idle
    pub(super) fn drive_bounded(&mut self) -> bool {
        for _ in 0..100 {
            if !self.step() {
                return false;
            }
        }

        true
    }

    pub(super) fn drive_client(&mut self) {
        let span = info_span!("client");
        let _guard = span.enter();
        self.client.drive(self.time, self.server.addr);
        for (packet, buffer) in self.client.outbound.drain(..) {
            let packet_size = packet_size(&packet, &buffer);
            if packet_size > self.mtu {
                info!(packet_size, "dropping packet (max size exceeded)");
                continue;
            }
            if buffer[0] & packet::LONG_HEADER_FORM == 0 {
                let spin = buffer[0] & packet::SPIN_BIT != 0;
                self.spins += (spin == self.last_spin) as u64;
                self.last_spin = spin;
            }
            if self.server.addr == packet.destination {
                let ecn = set_congestion_experienced(packet.ecn, self.congestion_experienced);
                self.server.inbound.push_back((
                    self.time + self.latency,
                    ecn,
                    buffer.as_ref().into(),
                ));
            }
        }
    }

    pub(super) fn drive_server(&mut self) {
        let span = info_span!("server");
        let _guard = span.enter();
        self.server.drive(self.time, self.client.addr);
        for (packet, buffer) in self.server.outbound.drain(..) {
            let packet_size = packet_size(&packet, &buffer);
            if packet_size > self.mtu {
                info!(packet_size, "dropping packet (max size exceeded)");
                continue;
            }
            if self.client.addr == packet.destination {
                let ecn = set_congestion_experienced(packet.ecn, self.congestion_experienced);
                self.client.inbound.push_back((
                    self.time + self.latency,
                    ecn,
                    buffer.as_ref().into(),
                ));
            }
        }
    }

    pub(super) fn connect(&mut self) -> (ConnectionHandle, ConnectionHandle) {
        self.connect_with(client_config())
    }

    /// Hand a client configuration to the client endpoint, for connections that share it
    pub(super) fn add_client_config(&mut self, config: ClientConfig) -> ClientConfigHandle {
        self.client.insert_client_config(config).unwrap()
    }

    /// Connect with a configuration of its own, retired once the connection exists so that its
    /// slot is reclaimed when the connection drains
    pub(super) fn connect_with(
        &mut self,
        config: ClientConfig,
    ) -> (ConnectionHandle, ConnectionHandle) {
        let config = self.add_client_config(config);
        let connection = self.connect_with_shared(config);
        self.client.retire_client_config(config);
        connection
    }

    /// Connect with a configuration added by `add_client_config`
    pub(super) fn connect_with_shared(
        &mut self,
        config: ClientConfigHandle,
    ) -> (ConnectionHandle, ConnectionHandle) {
        info!("connecting");
        let client_ch = self.begin_connect_shared(config);
        self.drive();
        let server_ch = self.server.assert_accept();
        self.finish_connect(client_ch, server_ch);
        (client_ch, server_ch)
    }

    /// Just start connecting the client, with a configuration of its own
    pub(super) fn begin_connect(&mut self, config: ClientConfig) -> ConnectionHandle {
        let config = self.add_client_config(config);
        let client_ch = self.begin_connect_shared(config);
        self.client.retire_client_config(config);
        client_ch
    }

    /// Just start connecting the client, with a configuration added by `add_client_config`
    pub(super) fn begin_connect_shared(&mut self, config: ClientConfigHandle) -> ConnectionHandle {
        let span = info_span!("client");
        let _guard = span.enter();
        let (client_ch, client_conn) = self
            .client
            .connect(self.time, config, self.server.addr, "localhost", None)
            .unwrap();
        self.client.connections.insert(client_ch, client_conn);
        client_ch
    }

    fn finish_connect(&mut self, client_ch: ConnectionHandle, server_ch: ConnectionHandle) {
        assert_matches!(
            self.client_conn_mut(client_ch).poll(),
            Some(Event::HandshakeDataReady)
        );
        assert_matches!(
            self.client_conn_mut(client_ch).poll(),
            Some(Event::Connected)
        );
        assert_matches!(
            self.server_conn_mut(server_ch).poll(),
            Some(Event::HandshakeDataReady)
        );
        assert_matches!(
            self.server_conn_mut(server_ch).poll(),
            Some(Event::Connected)
        );
    }

    pub(super) fn client_conn_mut(&mut self, ch: ConnectionHandle) -> &mut Connection {
        self.client.connections.get_mut(&ch).unwrap()
    }

    pub(super) fn client_streams(&mut self, ch: ConnectionHandle) -> Streams<'_> {
        self.client_conn_mut(ch).streams()
    }

    pub(super) fn client_send(&mut self, ch: ConnectionHandle, s: StreamId) -> SendStream<'_> {
        self.client_conn_mut(ch).send_stream(s)
    }

    pub(super) fn client_recv(&mut self, ch: ConnectionHandle, s: StreamId) -> RecvStream<'_> {
        self.client_conn_mut(ch).recv_stream(s)
    }

    pub(super) fn client_datagrams(&mut self, ch: ConnectionHandle) -> Datagrams<'_> {
        self.client_conn_mut(ch).datagrams()
    }

    pub(super) fn server_conn_mut(&mut self, ch: ConnectionHandle) -> &mut Connection {
        self.server.connections.get_mut(&ch).unwrap()
    }

    pub(super) fn server_streams(&mut self, ch: ConnectionHandle) -> Streams<'_> {
        self.server_conn_mut(ch).streams()
    }

    pub(super) fn server_send(&mut self, ch: ConnectionHandle, s: StreamId) -> SendStream<'_> {
        self.server_conn_mut(ch).send_stream(s)
    }

    pub(super) fn server_recv(&mut self, ch: ConnectionHandle, s: StreamId) -> RecvStream<'_> {
        self.server_conn_mut(ch).recv_stream(s)
    }

    pub(super) fn server_datagrams(&mut self, ch: ConnectionHandle) -> Datagrams<'_> {
        self.server_conn_mut(ch).datagrams()
    }
}

impl Default for Pair {
    fn default() -> Self {
        Self::new(Default::default(), server_config())
    }
}

pub(super) struct TestEndpoint {
    pub(super) endpoint: Endpoint,
    pub(super) addr: SocketAddr,
    timeout: Option<Instant>,
    pub(super) outbound: VecDeque<(Transmit, Bytes)>,
    delayed: VecDeque<(Transmit, Bytes)>,
    pub(super) inbound: VecDeque<(Instant, Option<EcnCodepoint>, BytesMut)>,
    pub(super) accepted: Option<Result<ConnectionHandle, ConnectionError>>,
    pub(super) connections: HashMap<ConnectionHandle, Connection>,
    conn_events: HashMap<ConnectionHandle, VecDeque<ConnectionEvent>>,
    pub(super) captured_packets: Vec<Vec<u8>>,
    pub(super) capture_inbound_packets: bool,
    pub(super) handle_incoming: Box<dyn FnMut(&Incoming) -> IncomingConnectionBehavior>,
    pub(super) waiting_incoming: Vec<Incoming>,
}

#[derive(Debug, Copy, Clone)]
pub(super) enum IncomingConnectionBehavior {
    Accept,
    Reject,
    Retry,
    Wait,
}

pub(super) fn validate_incoming(incoming: &Incoming) -> IncomingConnectionBehavior {
    if incoming.remote_address_validated() {
        IncomingConnectionBehavior::Accept
    } else {
        IncomingConnectionBehavior::Retry
    }
}

impl TestEndpoint {
    fn new(endpoint: Endpoint, addr: SocketAddr) -> Self {
        Self {
            endpoint,
            addr,
            timeout: None,
            outbound: VecDeque::new(),
            delayed: VecDeque::new(),
            inbound: VecDeque::new(),
            accepted: None,
            connections: HashMap::default(),
            conn_events: HashMap::default(),
            captured_packets: Vec::new(),
            capture_inbound_packets: false,
            handle_incoming: Box::new(|_| IncomingConnectionBehavior::Accept),
            waiting_incoming: Vec::new(),
        }
    }

    pub(super) fn drive(&mut self, now: Instant, remote: SocketAddr) {
        self.drive_incoming(now, remote);
        self.drive_outgoing(now);
    }

    pub(super) fn drive_incoming(&mut self, now: Instant, remote: SocketAddr) {
        let buffer_size = self.endpoint.config().get_max_udp_payload_size() as usize;
        let mut buf = Vec::with_capacity(buffer_size);

        while self.inbound.front().is_some_and(|x| x.0 <= now) {
            let (recv_time, ecn, packet) = self.inbound.pop_front().unwrap();
            if let Some(event) = self
                .endpoint
                .handle(recv_time, remote, None, ecn, packet, &mut buf)
            {
                match event {
                    DatagramEvent::NewConnection(incoming) => {
                        match (self.handle_incoming)(&incoming) {
                            IncomingConnectionBehavior::Accept => {
                                let _ = self.try_accept(incoming, now);
                            }
                            IncomingConnectionBehavior::Reject => {
                                self.reject(incoming);
                            }
                            IncomingConnectionBehavior::Retry => {
                                self.retry(incoming);
                            }
                            IncomingConnectionBehavior::Wait => {
                                self.waiting_incoming.push(incoming);
                            }
                        }
                    }
                    DatagramEvent::ConnectionEvent(ch, event) => {
                        if self.capture_inbound_packets {
                            let packet = self.connections[&ch].decode_packet(&event);
                            self.captured_packets.extend(packet);
                        }

                        self.conn_events.entry(ch).or_default().push_back(event);
                    }
                    DatagramEvent::Response(transmit) => {
                        let size = transmit.size;
                        self.outbound.extend(split_transmit(transmit, &buf[..size]));
                        buf.clear();
                    }
                }
            }
        }
    }

    /// Applies the events queued for connection `ch`, then its timeout at `now`: the order a
    /// driver that reads its socket before its timers takes, which `drive_outgoing` reverses.
    pub(super) fn events_then_timeout(&mut self, ch: ConnectionHandle, now: Instant) {
        let events = self.conn_events.remove(&ch).unwrap_or_default();
        let conn = self.connections.get_mut(&ch).unwrap();
        for event in events {
            conn.handle_event(event, self.endpoint.configs_mut());
        }
        conn.handle_timeout(now);
    }

    pub(super) fn drive_outgoing(&mut self, now: Instant) {
        let buffer_size = self.endpoint.config().get_max_udp_payload_size() as usize;
        let mut buf = Vec::with_capacity(buffer_size);

        loop {
            let mut endpoint_events: Vec<(ConnectionHandle, EndpointEvent)> = vec![];
            for (ch, conn) in self.connections.iter_mut() {
                if self.timeout.is_some_and(|x| x <= now) {
                    self.timeout = None;
                    conn.handle_timeout(now);
                }

                for (_, mut events) in self.conn_events.drain() {
                    for event in events.drain(..) {
                        conn.handle_event(event, self.endpoint.configs_mut());
                    }
                }

                while let Some(event) = conn.poll_endpoint_events() {
                    endpoint_events.push((*ch, event));
                }
                while let Some(transmit) =
                    conn.poll_transmit(now, MAX_DATAGRAMS, &mut buf, self.endpoint.configs())
                {
                    let size = transmit.size;
                    self.outbound.extend(split_transmit(transmit, &buf[..size]));
                    buf.clear();
                }
                self.timeout = conn.poll_timeout();
            }

            if endpoint_events.is_empty() {
                break;
            }

            for (ch, event) in endpoint_events {
                if let Some(event) = self.endpoint.handle_event(ch, event)
                    && let Some(conn) = self.connections.get_mut(&ch)
                {
                    conn.handle_event(event, self.endpoint.configs_mut());
                }
            }
        }
    }

    pub(super) fn next_wakeup(&self) -> Option<Instant> {
        let next_inbound = self.inbound.front().map(|x| x.0);
        min_opt(self.timeout, next_inbound)
    }

    fn is_idle(&self) -> bool {
        self.connections.values().all(|x| x.is_idle())
    }

    pub(super) fn delay_outbound(&mut self) {
        assert!(self.delayed.is_empty());
        mem::swap(&mut self.delayed, &mut self.outbound);
    }

    pub(super) fn finish_delay(&mut self) {
        self.outbound.extend(self.delayed.drain(..));
    }

    pub(super) fn try_accept(
        &mut self,
        incoming: Incoming,
        now: Instant,
    ) -> Result<ConnectionHandle, ConnectionError> {
        let mut buf = Vec::new();
        match self.endpoint.accept(incoming, now, &mut buf, None, None) {
            Ok((ch, conn)) => {
                self.connections.insert(ch, conn);
                self.accepted = Some(Ok(ch));
                Ok(ch)
            }
            Err(error) => {
                if let Some(transmit) = error.response {
                    let size = transmit.size;
                    self.outbound.extend(split_transmit(transmit, &buf[..size]));
                }
                self.accepted = Some(Err(error.cause.clone()));
                Err(error.cause)
            }
        }
    }

    pub(super) fn retry(&mut self, incoming: Incoming) {
        let mut buf = Vec::new();
        let transmit = self.endpoint.retry(incoming, &mut buf).unwrap();
        let size = transmit.size;
        self.outbound.extend(split_transmit(transmit, &buf[..size]));
    }

    pub(super) fn reject(&mut self, incoming: Incoming) {
        let mut buf = Vec::new();
        let transmit = self.endpoint.refuse(incoming, &mut buf).unwrap();
        let size = transmit.size;
        self.outbound.extend(split_transmit(transmit, &buf[..size]));
    }

    pub(super) fn assert_accept(&mut self) -> ConnectionHandle {
        self.accepted
            .take()
            .expect("server didn't try connecting")
            .expect("server experienced error connecting")
    }

    pub(super) fn assert_accept_error(&mut self) -> ConnectionError {
        self.accepted
            .take()
            .expect("server didn't try connecting")
            .expect_err("server did unexpectedly connect without error")
    }

    pub(super) fn assert_no_accept(&self) {
        assert!(self.accepted.is_none(), "server did unexpectedly connect")
    }
}

impl ::std::ops::Deref for TestEndpoint {
    type Target = Endpoint;
    fn deref(&self) -> &Endpoint {
        &self.endpoint
    }
}

impl ::std::ops::DerefMut for TestEndpoint {
    fn deref_mut(&mut self) -> &mut Endpoint {
        &mut self.endpoint
    }
}

pub(super) fn subscribe() -> tracing::subscriber::DefaultGuard {
    let builder = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(|| TestWriter);
    // tracing uses std::time to trace time, which panics in wasm.
    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    let builder = builder.without_time();
    tracing::subscriber::set_default(builder.finish())
}

struct TestWriter;

impl Write for TestWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        print!(
            "{}",
            str::from_utf8(buf).expect("tried to log invalid UTF-8")
        );
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        io::stdout().flush()
    }
}

/// aws-lc-rs's provider with classical key exchange only: a client of it puts its ClientHello in
/// one Initial datagram, the layout upstream's packet-counting tests were written against
static CLASSICAL: std::sync::LazyLock<rustls::crypto::CryptoProvider> =
    std::sync::LazyLock::new(|| rustls::crypto::CryptoProvider {
        kx_groups: vec![
            rustls::crypto::aws_lc_rs::kx_group::X25519,
            rustls::crypto::aws_lc_rs::kx_group::SECP256R1,
            rustls::crypto::aws_lc_rs::kx_group::SECP384R1,
        ],
        ..rustls::crypto::aws_lc_rs::default_provider()
    });

/// A server that accepts classical key exchange, for upstream's packet-counting tests: hyper-quic's
/// own configurations refuse it (`node_provider`)
pub(super) fn server_config_classical(
    identity: Option<(CertificateDer<'static>, PrivateKeyDer<'static>)>,
) -> ServerConfig {
    let (cert, key) = identity.unwrap_or_else(|| {
        (
            CERTIFIED_KEY.cert.der().clone(),
            PrivateKeyDer::Pkcs8(CERTIFIED_KEY.signing_key.serialize_der().into()),
        )
    });
    let provider: &'static rustls::crypto::CryptoProvider = &CLASSICAL;
    let mut inner = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .unwrap();
    inner.max_early_data_size = u32::MAX;
    ServerConfig::with_crypto(Box::new(QuicServerConfig::try_from(inner).unwrap()))
}

pub(super) fn server_config() -> ServerConfig {
    ServerConfig::with_crypto(Box::new(server_crypto()))
}

pub(super) fn server_config_with_cert(
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
) -> ServerConfig {
    ServerConfig::with_crypto(Box::new(server_crypto_with_cert(cert, key)))
}

pub(super) fn server_crypto() -> QuicServerConfig {
    server_crypto_inner(None, None)
}

pub(super) fn server_crypto_with_alpn(alpn: Vec<Vec<u8>>) -> QuicServerConfig {
    server_crypto_inner(None, Some(alpn))
}

pub(super) fn server_crypto_with_cert(
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
) -> QuicServerConfig {
    server_crypto_inner(Some((cert, key)), None)
}

fn server_crypto_inner(
    identity: Option<(CertificateDer<'static>, PrivateKeyDer<'static>)>,
    alpn: Option<Vec<Vec<u8>>>,
) -> QuicServerConfig {
    let (cert, key) = identity.unwrap_or_else(|| {
        (
            CERTIFIED_KEY.cert.der().clone(),
            PrivateKeyDer::Pkcs8(CERTIFIED_KEY.signing_key.serialize_der().into()),
        )
    });

    let mut config = QuicServerConfig::inner(vec![cert], key).unwrap();
    if let Some(alpn) = alpn {
        config.alpn_protocols = alpn;
    }

    config.try_into().unwrap()
}

pub(crate) fn client_config() -> ClientConfig {
    ClientConfig::new(Box::new(client_crypto()))
}

pub(super) fn client_config_with_deterministic_pns() -> ClientConfig {
    let mut cfg = ClientConfig::new(Box::new(client_crypto()));
    let mut transport = TransportConfig::default();
    transport.deterministic_packet_numbers(true);
    cfg.transport = transport;
    cfg
}

/// `config` sending each packet of the handshake's flights once
/// (`TransportConfig::handshake_copies` off): for tests of what one flight does on the wire, or of
/// how many attempts a server sees
pub(super) fn single_flights(mut config: ClientConfig) -> ClientConfig {
    let mut transport = TransportConfig::default();
    transport.handshake_copies(false);
    config.transport = transport;
    config
}

pub(super) fn client_config_with_certs(certs: Vec<CertificateDer<'static>>) -> ClientConfig {
    ClientConfig::new(Box::new(client_crypto_inner(Some(certs), None)))
}

/// A client offering only classical key exchange, so its ClientHello fits in one Initial
/// datagram: the layout upstream's packet-counting tests were written against. The default
/// client prefers SecP384r1MLKEM1024, whose 1,665-byte key share spreads the ClientHello over two.
pub(super) fn client_config_classical(certs: Option<Vec<CertificateDer<'static>>>) -> ClientConfig {
    let provider: &'static rustls::crypto::CryptoProvider = &CLASSICAL;
    let mut roots = rustls::RootCertStore::empty();
    for cert in certs.unwrap_or_else(|| vec![CERTIFIED_KEY.cert.der().clone()]) {
        roots.add(cert).unwrap();
    }
    let verifier = WebPkiServerVerifier::builder_with_provider(roots, provider)
        .build()
        .unwrap();
    let mut inner = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth();
    inner.enable_early_data = true;
    ClientConfig::new(Box::new(QuicClientConfig::try_from(inner).unwrap()))
}

pub(super) fn client_crypto() -> QuicClientConfig {
    client_crypto_inner(None, None)
}

pub(super) fn client_crypto_with_alpn(protocols: Vec<Vec<u8>>) -> QuicClientConfig {
    client_crypto_inner(None, Some(protocols))
}

fn client_crypto_inner(
    certs: Option<Vec<CertificateDer<'static>>>,
    alpn: Option<Vec<Vec<u8>>>,
) -> QuicClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    for cert in certs.unwrap_or_else(|| vec![CERTIFIED_KEY.cert.der().clone()]) {
        roots.add(cert).unwrap();
    }

    let mut inner = QuicClientConfig::inner(
        WebPkiServerVerifier::builder_with_provider(roots, node_provider())
            .build()
            .unwrap(),
    )
    .unwrap();
    if let Some(alpn) = alpn {
        inner.alpn_protocols = alpn;
    }

    inner.try_into().unwrap()
}

pub(super) fn min_opt<T: Ord>(x: Option<T>, y: Option<T>) -> Option<T> {
    match (x, y) {
        (Some(x), Some(y)) => Some(cmp::min(x, y)),
        (Some(x), _) => Some(x),
        (_, Some(y)) => Some(y),
        _ => None,
    }
}

/// The maximum of datagrams TestEndpoint will produce via `poll_transmit`
const MAX_DATAGRAMS: usize = 10;

fn split_transmit(transmit: Transmit, buffer: &[u8]) -> Vec<(Transmit, Bytes)> {
    let mut buffer = Bytes::copy_from_slice(buffer);
    let segment_size = match transmit.segment_size {
        Some(segment_size) => segment_size,
        _ => return vec![(transmit, buffer)],
    };

    let mut transmits = Vec::new();
    while !buffer.is_empty() {
        let end = segment_size.min(buffer.len());

        let contents = buffer.split_to(end);
        transmits.push((
            Transmit {
                destination: transmit.destination,
                size: contents.len(),
                ecn: transmit.ecn,
                segment_size: None,
                src_ip: transmit.src_ip,
            },
            contents,
        ));
    }

    transmits
}

fn packet_size(transmit: &Transmit, buffer: &Bytes) -> usize {
    if transmit.segment_size.is_some() {
        panic!("This transmit is meant to be split into multiple packets!");
    }

    buffer.len()
}

fn set_congestion_experienced(
    x: Option<EcnCodepoint>,
    congestion_experienced: bool,
) -> Option<EcnCodepoint> {
    x.map(|codepoint| match congestion_experienced {
        true => EcnCodepoint::Ce,
        false => codepoint,
    })
}

lazy_static! {
    /// The same identity every run, so a handshake's flights are the same length every run and the
    /// tests that count bytes, windows and datagrams are exact: rcgen's default is a random ECDSA
    /// P-256 key, whose signatures take a random nonce and are 70 to 72 bytes in DER, so the server's
    /// CertificateVerify changed length handshake to handshake. An Ed25519 key from a fixed seed (RFC
    /// 8410 §7's PKCS#8 form) signs deterministically (RFC 8032 §5.1.6), with a fixed serial.
    pub(crate) static ref CERTIFIED_KEY: rcgen::CertifiedKey<rcgen::KeyPair> = {
        let mut pkcs8 = vec![
            0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
            0x20,
        ];
        pkcs8.extend_from_slice(&[0x5a; 32]);
        let signing_key =
            rcgen::KeyPair::try_from(&rustls::pki_types::PrivatePkcs8KeyDer::from(pkcs8)).unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
        params.serial_number = Some(rcgen::SerialNumber::from(0x0123_4567_89ab_cdef_u64));
        let cert = params.self_signed(&signing_key).unwrap();
        rcgen::CertifiedKey { cert, signing_key }
    };
}
