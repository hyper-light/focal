use std::{
    cmp::{self, Reverse},
    collections::{BinaryHeap, HashMap, hash_map},
    convert::TryFrom,
    fmt, mem,
    net::{IpAddr, SocketAddr},
};

use bytes::{BufMut, Bytes, BytesMut};
use rand::{
    Rng, RngExt, SeedableRng,
    rngs::{StdRng, SysRng},
};
use rustc_hash::FxHashMap;
use slab::Slab;
use thiserror::Error;
use tracing::{debug, error, trace, warn};

use crate::{
    BloomTokenLog, Duration, Frame, INITIAL_MTU, Instant, MAX_CID_SIZE, MIN_INITIAL_SIZE,
    QlogStream, RESET_TOKEN_SIZE, ResetToken, Side, TIMER_GRANULARITY, TokenLog, TokenMemoryCache,
    TokenStore, Transmit, TransportConfig, TransportError,
    cid_generator::ConnectionIdGenerator,
    coding::BufMutExt,
    config::{
        ClientConfig, ClientConfigHandle, ConfigKey, Configs, ConfigsFull, EndpointConfig,
        ServerConfig, ServerConfigHandle,
    },
    connection::{
        CarefulResume, CongestionMemory, Connection, ConnectionError, Grant, Leaves, LossFit,
        LossMemory, SideArgs,
    },
    crypto::{self, Keys, UnsupportedVersion},
    frame,
    packet::{
        FixedLengthConnectionIdParser, Header, InitialHeader, InitialPacket, Packet,
        PacketDecodeError, PacketNumber, PartialDecode, ProtectedInitialHeader,
    },
    shared::{
        ConnectionEvent, ConnectionEventInner, ConnectionId, DatagramConnectionEvent, EcnCodepoint,
        EndpointEvent, EndpointEventInner, IssuedCid,
    },
    token::{IncomingToken, InvalidRetryTokenError, Token, TokenPayload},
    transport_parameters::{PreferredAddress, TransportParameters},
};

/// The main entry point to the library
///
/// This object performs no I/O whatsoever. Instead, it consumes incoming packets and
/// connection-generated events via `handle` and `handle_event`.
pub struct Endpoint {
    rng: StdRng,
    index: ConnectionIndex,
    connections: Slab<ConnectionMeta>,
    local_cid_generator: Box<dyn ConnectionIdGenerator>,
    config: EndpointConfig,
    /// The server and client configurations this endpoint and its connections share
    configs: Configs,
    /// The configuration new incoming connections are accepted under
    server_config: Option<ServerConfigHandle>,
    /// Whether the underlying UDP socket promises not to fragment packets
    allow_mtud: bool,
    /// Time at which a stateless reset was most recently sent
    last_stateless_reset: Option<Instant>,
    /// Buffered Initial and 0-RTT messages for pending incoming connections
    incoming_buffers: Slab<IncomingBuffer>,
    /// Initial datagrams of connection attempts whose ClientHello has not yet begun (see
    /// `handle_first_packet`); bounded with `incoming_buffers` by `max_incoming` and the incoming
    /// byte limits, and dropped after `HeldInitial::expires`
    held: Slab<HeldInitial>,
    /// `held` entries in order of expiry, each with the CID it was held for
    held_expiry: BinaryHeap<Reverse<(Instant, usize, ConnectionId)>>,
    /// Bytes buffered in `incoming_buffers` and `held` together
    all_incoming_buffers_total_bytes: u64,
    /// Address validation tokens already presented to this server (RFC 9000 §8.1.4)
    token_log: Box<dyn TokenLog>,
    /// Address validation tokens servers gave this client, for its later connections to them
    token_store: Box<dyn TokenStore>,
    /// What closed connections measured of their paths, for Careful Resume (RFC 9959)
    congestion_memory: CongestionMemory,
    /// What connections learned of each remote's loss bursts
    loss_memory: LossMemory,
}

impl Endpoint {
    /// Create a new endpoint
    ///
    /// `allow_mtud` enables path MTU detection when requested by `Connection` configuration for
    /// better performance. This requires that outgoing packets are never fragmented, which can be
    /// achieved via e.g. the `IPV6_DONTFRAG` socket option.
    ///
    /// If `rng_seed` is provided, it will be used to initialize the endpoint's rng (having priority
    /// over the rng seed configured in [`EndpointConfig`]). Note that the `rng_seed` parameter will
    /// be removed in a future release, so prefer setting it to `None` and configuring rng seeds
    /// using [`EndpointConfig::rng_seed`].
    ///
    /// Fails if no seed is given and the system's random number generator cannot give one, where
    /// upstream panicked.
    pub fn new(
        config: EndpointConfig,
        server_config: Option<ServerConfig>,
        allow_mtud: bool,
        rng_seed: Option<[u8; 32]>,
    ) -> Result<Self, RngUnavailable> {
        let rng = match rng_seed.or(config.rng_seed) {
            Some(seed) => StdRng::from_seed(seed),
            None => StdRng::try_from_rng(&mut SysRng).map_err(|_| RngUnavailable)?,
        };
        let mut configs = Configs::new(config.config_slots);
        // The slab is empty and has at least one slot, so the insertion cannot be refused
        let server_config = server_config.and_then(|c| configs.insert_server(c).ok());
        let congestion_memory =
            CongestionMemory::new(config.careful_resume.map_or(0, |c| c.remembered));
        let loss_memory = LossMemory::new(config.loss_memory.0);
        Ok(Self {
            rng,
            index: ConnectionIndex::default(),
            connections: Slab::new(),
            local_cid_generator: config.cid_generator.clone_box(),
            config,
            configs,
            server_config,
            allow_mtud,
            last_stateless_reset: None,
            incoming_buffers: Slab::new(),
            held: Slab::new(),
            held_expiry: BinaryHeap::new(),
            all_incoming_buffers_total_bytes: 0,
            token_log: Box::new(BloomTokenLog::default()),
            token_store: Box::new(TokenMemoryCache::default()),
            congestion_memory,
            loss_memory,
        })
    }

    /// Replace the log of address validation tokens presented to this server
    ///
    /// Defaults to a [`BloomTokenLog`], which is suitable for most internet applications.
    pub fn set_token_log(&mut self, log: Box<dyn TokenLog>) {
        self.token_log = log;
    }

    /// Replace the store of address validation tokens servers gave this client
    ///
    /// Defaults to a [`TokenMemoryCache`], which is suitable for most internet applications.
    pub fn set_token_store(&mut self, store: Box<dyn TokenStore>) {
        self.token_store = store;
    }

    /// Replace the server configuration, affecting new incoming connections only
    ///
    /// Pending incoming connections retain the configuration active when they first arrived. The
    /// replaced configuration is dropped once the last connection started under it drains. Refused
    /// with [`ConfigsFull`] when every slot holds a configuration still in use; the current
    /// configuration is then kept.
    pub fn set_server_config(
        &mut self,
        server_config: Option<ServerConfig>,
    ) -> Result<(), ConfigsFull> {
        let new = match server_config {
            Some(config) => Some(self.configs.insert_server(config)?),
            None => None,
        };
        if let Some(old) = mem::replace(&mut self.server_config, new) {
            self.configs.supersede(old.0);
        }
        Ok(())
    }

    /// Add a client configuration for [`connect`](Self::connect) to use
    ///
    /// Connections made with the returned handle share the configuration, including its TLS
    /// session cache, so later connections to a server can resume earlier sessions. Refused with
    /// [`ConfigsFull`] when every slot holds a configuration still in use.
    pub fn insert_client_config(
        &mut self,
        config: ClientConfig,
    ) -> Result<ClientConfigHandle, ConfigsFull> {
        self.configs.insert_client(config)
    }

    /// Stop offering a client configuration to new connections
    ///
    /// The configuration is dropped once the last connection made with it drains; `connect` with
    /// the handle fails from now on.
    pub fn retire_client_config(&mut self, handle: ClientConfigHandle) {
        self.configs.supersede(handle.0);
    }

    /// Insert a server configuration without making it current, for
    /// [`accept`](Self::accept) to use for chosen incoming connections
    pub fn insert_server_config(
        &mut self,
        config: ServerConfig,
    ) -> Result<ServerConfigHandle, ConfigsFull> {
        self.configs.insert_server(config)
    }

    /// Stop offering a server configuration inserted with
    /// [`insert_server_config`](Self::insert_server_config)
    ///
    /// The configuration is dropped once the last connection accepted with it drains.
    pub fn retire_server_config(&mut self, handle: ServerConfigHandle) {
        if self.server_config != Some(handle) {
            self.configs.supersede(handle.0);
        }
    }

    /// The current server configuration, mutably: a test changes it in place, keeping its keys
    #[cfg(test)]
    pub(crate) fn server_config_mut(&mut self) -> Option<&mut ServerConfig> {
        self.configs.server_config_mut(self.server_config?)
    }

    /// The configurations this endpoint and its connections share
    pub fn configs(&self) -> &Configs {
        &self.configs
    }

    /// The configurations this endpoint and its connections share, to lend to
    /// [`Connection::handle_event`]
    pub fn configs_mut(&mut self) -> &mut Configs {
        &mut self.configs
    }

    /// Process `EndpointEvent`s emitted from related `Connection`s
    ///
    /// In turn, processing this event may return a `ConnectionEvent` for the same `Connection`.
    pub fn handle_event(
        &mut self,
        ch: ConnectionHandle,
        event: EndpointEvent,
    ) -> Option<ConnectionEvent> {
        use EndpointEventInner::*;
        match event.0 {
            NeedIdentifiers(now, n) => {
                return Some(self.send_new_identifiers(now, ch, n));
            }
            ResetToken(remote, token) => {
                // Events come from the endpoint's own connections; one it no longer knows is
                // ignored, where upstream panicked
                let meta = self.connections.get_mut(ch.0)?;
                if let Some(old) = meta.reset_token.replace((remote, token)) {
                    self.index.connection_reset_tokens.remove(old.0, old.1);
                }
                if self.index.connection_reset_tokens.insert(remote, token, ch) {
                    warn!("duplicate reset token");
                }
            }
            RetireConnectionId(now, seq, allow_more_cids) => {
                if let Some(cid) = self
                    .connections
                    .get_mut(ch.0)
                    .and_then(|meta| meta.loc_cids.remove(&seq))
                {
                    trace!("peer retired CID {}: {}", seq, cid);
                    self.index.retire(cid);
                    if allow_more_cids {
                        return Some(self.send_new_identifiers(now, ch, 1));
                    }
                }
            }
            NewToken { server_name, token } => {
                self.token_store.insert(&server_name, token);
            }
            Resume(leaves) => self.remember(ch, leaves),
            Losses(now, fit) => self.learn_losses(ch, now, &fit),
            Drained => {
                if let Some(conn) = self.connections.try_remove(ch.0) {
                    self.index.remove(&conn);
                    self.configs.release(conn.config);
                } else {
                    // This indicates a bug in downstream code, which could cause spurious
                    // connection loss instead of this error if the CID was (re)allocated prior to
                    // the illegal call.
                    error!(id = ch.0, "unknown connection drained");
                }
            }
        }
        None
    }

    /// Adds what connection `ch` counted of its path's loss bursts to the endpoint's memory of its
    /// remote, for the next connection to it
    fn learn_losses(&mut self, ch: ConnectionHandle, now: Instant, fit: &LossFit) {
        if let Some(meta) = self.connections.get(ch.0) {
            let remote = meta.addresses.remote.ip();
            self.loss_memory
                .add(remote, fit, now, self.config.loss_memory.1);
        }
    }

    /// Keeps what connection `ch` measured of its path for the next connection to its remote
    /// (Careful Resume, RFC 9959 §3.1)
    /// What connection `ch` leaves the Careful Resume memory for its remote
    fn remember(&mut self, ch: ConnectionHandle, leaves: Leaves) {
        let Some(meta) = self.connections.get(ch.0) else {
            return;
        };
        let remote = meta.addresses.remote.ip();
        match leaves {
            Leaves::Keep(saved) => self.congestion_memory.put(remote, saved),
            Leaves::Tried(since) => self.congestion_memory.tried(remote, since),
            Leaves::Release => self.congestion_memory.release(remote),
            Leaves::Nothing => {}
        }
    }

    /// Process an incoming UDP datagram
    pub fn handle(
        &mut self,
        now: Instant,
        remote: SocketAddr,
        local_ip: Option<IpAddr>,
        ecn: Option<EcnCodepoint>,
        data: BytesMut,
        buf: &mut Vec<u8>,
    ) -> Option<DatagramEvent> {
        // Partially decode packet or short-circuit if unable
        let datagram_len = data.len();
        let event = match PartialDecode::new(
            data,
            &FixedLengthConnectionIdParser::new(self.local_cid_generator.cid_len()),
            &self.config.supported_versions,
            self.config.grease_quic_bit,
        ) {
            Ok((first_decode, remaining)) => DatagramConnectionEvent {
                now,
                remote,
                ecn,
                first_decode,
                remaining,
            },
            Err(PacketDecodeError::UnsupportedVersion {
                src_cid,
                dst_cid,
                version,
            }) => {
                return self.negotiate_version(
                    datagram_len,
                    remote,
                    local_ip,
                    src_cid,
                    dst_cid,
                    version,
                    buf,
                );
            }
            Err(e) => {
                trace!("malformed header: {}", e);
                return None;
            }
        };

        let addresses = FourTuple { remote, local_ip };
        self.expire_held(now);

        if let Some(route_to) = self.index.get(&addresses, &event.first_decode) {
            // Handle packet on existing connection
            match route_to {
                RouteDatagramTo::Incoming(incoming_idx) => {
                    self.buffer_incoming(incoming_idx, datagram_len, event);
                    None
                }
                // An attempt whose ClientHello has not begun: this datagram may begin it
                RouteDatagramTo::Held(_) => {
                    self.handle_first_packet(datagram_len, event, addresses, buf)
                }
                RouteDatagramTo::Connection(ch) => Some(DatagramEvent::ConnectionEvent(
                    ch,
                    ConnectionEvent(ConnectionEventInner::Datagram(event)),
                )),
            }
        } else {
            self.handle_unknown(datagram_len, event, addresses, buf)
        }
    }

    /// Answers a datagram of an unsupported version with a Version Negotiation packet
    fn negotiate_version(
        &mut self,
        datagram_len: usize,
        remote: SocketAddr,
        local_ip: Option<IpAddr>,
        src_cid: ConnectionId,
        dst_cid: ConnectionId,
        version: u32,
        buf: &mut Vec<u8>,
    ) -> Option<DatagramEvent> {
        if self.server_config.is_none() {
            debug!("dropping packet with unsupported version");
            return None;
        }
        // RFC 9000 §5.2.2: "Servers MUST drop smaller packets that specify unsupported
        // versions." Responding to short packets would let a spoofed source elicit a
        // Version Negotiation packet larger than the datagram that triggered it.
        if datagram_len < usize::from(MIN_INITIAL_SIZE) {
            debug!("dropping short packet with unsupported version");
            return None;
        }
        trace!("sending version negotiation");
        // Negotiate versions
        Header::VersionNegotiate {
            random: self.rng.random::<u8>() | 0x40,
            src_cid: dst_cid,
            dst_cid: src_cid,
        }
        .encode(buf);
        // Grease with a reserved version
        buf.write::<u32>(match version {
            0x0a1a_2a3a => 0x0a1a_2a4a,
            _ => 0x0a1a_2a3a,
        });
        for &version in &self.config.supported_versions {
            buf.write(version);
        }
        Some(DatagramEvent::Response(Transmit {
            destination: remote,
            ecn: None,
            size: buf.len(),
            segment_size: None,
            src_ip: local_ip,
        }))
    }

    /// Buffers a datagram for an attempt the application has not yet accepted, within the
    /// server's incoming buffer limits
    fn buffer_incoming(
        &mut self,
        incoming_idx: usize,
        datagram_len: usize,
        event: DatagramConnectionEvent,
    ) {
        // The index routes only to a pending attempt, which keeps its buffer and its
        // configuration's slot; were either gone, the datagram is not buffered
        let Some(incoming_buffer) = self.incoming_buffers.get_mut(incoming_idx) else {
            return;
        };
        let Some(config) = self.configs.server_config(incoming_buffer.server_config) else {
            return;
        };
        let len = datagram_len as u64;
        let total = incoming_buffer
            .total_bytes
            .checked_add(len)
            .filter(|&n| n <= config.incoming_buffer_size);
        let all = self
            .all_incoming_buffers_total_bytes
            .checked_add(len)
            .filter(|&n| n <= config.incoming_buffer_size_total);
        if let (Some(total), Some(all)) = (total, all) {
            incoming_buffer.datagrams.push(event);
            incoming_buffer.total_bytes = total;
            self.all_incoming_buffers_total_bytes = all;
        }
    }

    /// Handles a datagram for no known connection: a new attempt, or a stateless reset
    fn handle_unknown(
        &mut self,
        datagram_len: usize,
        event: DatagramConnectionEvent,
        addresses: FourTuple,
        buf: &mut Vec<u8>,
    ) -> Option<DatagramEvent> {
        let dst_cid = *event.first_decode.dst_cid();
        if event.first_decode.initial_header().is_some() {
            // Potentially create a new connection
            self.handle_first_packet(datagram_len, event, addresses, buf)
        } else if event.first_decode.has_long_header() {
            debug!(
                "ignoring non-initial packet for unknown connection {}",
                dst_cid
            );
            None
        } else if !event.first_decode.is_initial()
            && self.local_cid_generator.validate(&dst_cid).is_err()
        {
            debug!("dropping packet with invalid CID");
            None
        } else if dst_cid.is_empty() {
            trace!("dropping unrecognized short packet without ID");
            None
        } else {
            // If we got this far, we're receiving a seemingly valid packet for an unknown
            // connection. Send a stateless reset if possible.
            self.stateless_reset(event.now, datagram_len, addresses, dst_cid, buf)
                .map(DatagramEvent::Response)
        }
    }

    fn stateless_reset(
        &mut self,
        now: Instant,
        inciting_dgram_len: usize,
        addresses: FourTuple,
        dst_cid: ConnectionId,
        buf: &mut Vec<u8>,
    ) -> Option<Transmit> {
        if self
            .last_stateless_reset
            // An interval past the clock's range never ends
            .is_some_and(|last| {
                last.checked_add(self.config.min_reset_interval)
                    .is_none_or(|next| next > now)
            })
        {
            debug!("ignoring unexpected packet within minimum stateless reset interval");
            return None;
        }

        /// Minimum amount of padding for the stateless reset to look like a short-header packet
        const MIN_PADDING_LEN: usize = 5;

        // Prevent amplification attacks and reset loops by ensuring we pad to at most 1 byte
        // smaller than the inciting packet.
        let max_padding_len = match inciting_dgram_len.checked_sub(RESET_TOKEN_SIZE) {
            Some(headroom) if headroom > MIN_PADDING_LEN => headroom.saturating_sub(1),
            _ => {
                debug!(
                    "ignoring unexpected {} byte packet: not larger than minimum stateless reset size",
                    inciting_dgram_len
                );
                return None;
            }
        };

        debug!(
            "sending stateless reset for {} to {}",
            dst_cid, addresses.remote
        );
        self.last_stateless_reset = Some(now);
        // Resets with at least this much padding can't possibly be distinguished from real packets
        /// Resets with at least this much padding can't possibly be distinguished from real packets
        const IDEAL_MIN_PADDING_LEN: usize = MIN_PADDING_LEN + MAX_CID_SIZE;
        let padding_len = if max_padding_len <= IDEAL_MIN_PADDING_LEN {
            max_padding_len
        } else {
            self.rng
                .random_range(IDEAL_MIN_PADDING_LEN..max_padding_len)
        };
        // Less than the inciting datagram
        buf.reserve(padding_len.saturating_add(RESET_TOKEN_SIZE));
        buf.resize(padding_len, 0);
        self.rng.fill_bytes(buf.as_mut_slice());
        if let Some(first) = buf.first_mut() {
            *first = 0b0100_0000 | (*first >> 2);
        }
        buf.extend_from_slice(&ResetToken::new(&self.config.reset_key, dst_cid));

        Some(Transmit {
            destination: addresses.remote,
            ecn: None,
            size: buf.len(),
            segment_size: None,
            src_ip: addresses.local_ip,
        })
    }

    /// Initiate a connection with the client configuration `config` refers to
    ///
    /// `config` comes from [`insert_client_config`](Self::insert_client_config).
    pub fn connect(
        &mut self,
        now: Instant,
        config: ClientConfigHandle,
        remote: SocketAddr,
        server_name: &str,
        qlog: Option<QlogStream>,
    ) -> Result<(ConnectionHandle, Connection), ConnectError> {
        if self.cids_exhausted() {
            return Err(ConnectError::CidsExhausted);
        }
        if remote.port() == 0 || remote.ip().is_unspecified() {
            return Err(ConnectError::InvalidRemoteAddress(remote));
        }
        let Some(client) = self.configs.client_for_connect(config) else {
            return Err(ConnectError::UnknownConfig);
        };
        let version = client.version;
        if !self.config.supported_versions.contains(&version) {
            return Err(ConnectError::UnsupportedVersion);
        }

        let remote_id = (client.initial_dst_cid_provider)();
        trace!(initial_dcid = %remote_id);

        let ch = ConnectionHandle(self.connections.vacant_key());
        let Some(loc_cid) = self.new_cid(ch) else {
            return Err(ConnectError::CidsExhausted);
        };
        let Some(client) = self.configs.client_for_connect(config) else {
            self.index.connection_ids.remove(&loc_cid);
            return Err(ConnectError::UnknownConfig);
        };
        let params = TransportParameters::new(
            &client.transport,
            &self.config,
            self.local_cid_generator.as_ref(),
            loc_cid,
            None,
            &mut self.rng,
        );
        let tls = client.crypto.start_session(version, server_name, &params)?;
        let transport = client.transport.clone();
        if !self.configs.acquire(config.0) {
            return Err(ConnectError::UnknownConfig);
        }

        let token = self.token_store.take(server_name).unwrap_or_default();
        let conn = self.add_connection(
            ch,
            version,
            remote_id,
            loc_cid,
            remote_id,
            FourTuple {
                remote,
                local_ip: None,
            },
            now,
            tls,
            config.0,
            transport,
            qlog,
            SideArgs::Client {
                token,
                server_name: server_name.into(),
            },
        );
        Ok((ch, conn))
    }

    fn send_new_identifiers(
        &mut self,
        now: Instant,
        ch: ConnectionHandle,
        num: u64,
    ) -> ConnectionEvent {
        let mut ids = vec![];
        for _ in 0..num {
            // A CID the generator cannot find, or a connection the endpoint no longer knows,
            // ends the issue: the connection is given what was found
            let Some(id) = self.new_cid(ch) else {
                break;
            };
            let Some(meta) = self.connections.get_mut(ch.0) else {
                self.index.connection_ids.remove(&id);
                break;
            };
            let sequence = meta.cids_issued;
            // CIDs issued to one connection, far below 2^64
            meta.cids_issued = meta.cids_issued.saturating_add(1);
            meta.loc_cids.insert(sequence, id);
            ids.push(IssuedCid {
                sequence,
                id,
                reset_token: ResetToken::new(&self.config.reset_key, id),
            });
        }
        ConnectionEvent(ConnectionEventInner::NewIdentifiers(ids, now))
    }

    /// Generate a connection ID for `ch`
    ///
    /// Upstream looped until the generator gave an unused CID. The loop is bounded here: with
    /// at most three quarters of the CID space in use (`cids_exhausted`), a random CID is taken
    /// with probability at most 3/4, so `MAX_CID_ATTEMPTS` draws all fail with probability below
    /// 2^-64; a generator that keeps giving used CIDs gets `None`.
    fn new_cid(&mut self, ch: ConnectionHandle) -> Option<ConnectionId> {
        /// The least `k` with `(3/4)^k < 2^-64`: `64 / log2(4/3)`, rounded up
        const MAX_CID_ATTEMPTS: usize = 155;
        for _ in 0..MAX_CID_ATTEMPTS {
            let cid = self.local_cid_generator.generate_cid();
            if cid.is_empty() {
                // Zero-length CID; nothing to track
                return Some(cid);
            }
            if let hash_map::Entry::Vacant(e) = self.index.connection_ids.entry(cid) {
                e.insert(ch);
                return Some(cid);
            }
        }
        None
    }

    fn handle_first_packet(
        &mut self,
        datagram_len: usize,
        event: DatagramConnectionEvent,
        addresses: FourTuple,
        buf: &mut Vec<u8>,
    ) -> Option<DatagramEvent> {
        let (server_handle, crypto) =
            match self.admit_first_packet(datagram_len, &event, addresses, buf) {
                Ok(admitted) => admitted,
                Err(response) => return response.map(DatagramEvent::Response),
            };

        // Kept in case this datagram must be held and replayed to the connection later
        let raw = {
            let mut raw = BytesMut::from(event.first_decode.data());
            if let Some(rest) = &event.remaining {
                raw.extend_from_slice(rest);
            }
            raw
        };

        let packet = authenticate_first_packet(event.first_decode, &crypto)?;
        // `finish` gives an Initial header for an Initial, where upstream panicked otherwise
        let Header::Initial(header) = packet.header else {
            return None;
        };

        // A connection attempt begins with the first byte of the ClientHello: CRYPTO data at
        // offset 0 (RFC 9001 §4.1.3). A ClientHello larger than one datagram, as a post-quantum
        // key share makes it (X25519MLKEM768 adds 1,184 bytes, draft-ietf-tls-ecdhe-mlkem), spans
        // several Initials. Any other Initial is held, not surfaced: if it arrived before the
        // start it joins the attempt when the start arrives, and if it is a straggler from a
        // flight the server already answered with Retry, it expires instead of appearing to the
        // application as a second attempt.
        match classify_first_initial(&packet.payload) {
            FirstInitial::Begins => {}
            FirstInitial::Continues => {
                self.hold_initial(
                    event.now,
                    addresses,
                    event.ecn,
                    raw,
                    header.dst_cid,
                    server_handle,
                );
                return None;
            }
            FirstInitial::Closes => {
                debug!("dropping initial that closes an attempt the server never saw begin");
                return None;
            }
        }

        let server_config = self.configs.server_config(server_handle)?;
        let token = match IncomingToken::from_header(
            &header,
            server_config,
            &mut *self.token_log,
            addresses.remote,
        ) {
            Ok(token) => token,
            Err(InvalidRetryTokenError) => {
                debug!("rejecting invalid retry token");
                return self
                    .initial_close(
                        header.version,
                        addresses,
                        &crypto,
                        &header.src_cid,
                        TransportError::INVALID_TOKEN(""),
                        buf,
                    )
                    .map(DatagramEvent::Response);
            }
        };

        // Datagrams held for this attempt before its ClientHello began go to the connection with
        // the rest of its buffered datagrams.
        if !self.configs.acquire(server_handle.0) {
            return None;
        }
        let (datagrams, total_bytes) = self.release_held(header.dst_cid);
        let incoming_idx = self.incoming_buffers.insert(IncomingBuffer {
            server_config: server_handle,
            datagrams,
            total_bytes,
        });
        self.index
            .insert_initial_incoming(header.dst_cid, incoming_idx);

        Some(DatagramEvent::NewConnection(Incoming {
            received_at: event.now,
            addresses,
            ecn: event.ecn,
            packet: InitialPacket {
                header,
                header_data: packet.header_data,
                payload: packet.payload,
            },
            rest: event.remaining,
            crypto,
            token,
            incoming_idx,
            improper_drop_warner: IncomingImproperDropWarner { dismissed: false },
        }))
    }

    /// Decides whether a first Initial may begin an attempt: the server's configuration, the
    /// datagram's size, the load, the version's keys and the destination CID; `Err` holds what
    /// the endpoint answers instead
    fn admit_first_packet(
        &mut self,
        datagram_len: usize,
        event: &DatagramConnectionEvent,
        addresses: FourTuple,
        buf: &mut Vec<u8>,
    ) -> Result<(ServerConfigHandle, Keys), Option<Transmit>> {
        let dst_cid = event.first_decode.dst_cid();
        // Routed here only with an Initial header
        let header = event.first_decode.initial_header().ok_or(None)?;

        let Some(server_handle) = self.server_config else {
            debug!("packet for unrecognized connection {}", dst_cid);
            return Err(self.stateless_reset(event.now, datagram_len, addresses, *dst_cid, buf));
        };
        // The current server configuration always occupies its slot
        let server_config = self.configs.server_config(server_handle).ok_or(None)?;

        if datagram_len < usize::from(MIN_INITIAL_SIZE) {
            debug!("ignoring short initial for connection {}", dst_cid);
            return Err(None);
        }

        // Saturation only happens under heavy load, where deriving initial keys per Initial just to
        // reply with CONNECTION_REFUSED would starve packet processing for existing connections.
        // Datagrams already held for this destination CID are this attempt's, not another's: a
        // ClientHello whose first datagram was lost is held by its second and begun by the
        // retransmission, and the one attempt counts once.
        let own_held = usize::from(self.index.held(dst_cid).is_some());
        let pending = self
            .incoming_buffers
            .len()
            .saturating_add(self.held.len())
            .saturating_sub(own_held);
        if self.cids_exhausted() || pending >= server_config.max_incoming {
            debug!(
                "ignoring initial for connection {} due to saturation",
                dst_cid
            );
            return Err(None);
        }

        let crypto = match server_config.crypto.initial_keys(header.version, dst_cid) {
            Ok(keys) => keys,
            Err(UnsupportedVersion) => {
                // This probably indicates that the user set supported_versions incorrectly in
                // `EndpointConfig`.
                debug!(
                    "ignoring initial packet version {:#x} unsupported by cryptographic layer",
                    header.version
                );
                return Err(None);
            }
        };

        if let Err(reason) = self.early_validate_first_packet(header) {
            return Err(self.initial_close(
                header.version,
                addresses,
                &crypto,
                &header.src_cid,
                reason,
                buf,
            ));
        }
        Ok((server_handle, crypto))
    }

    /// Hold an authenticated Initial datagram whose attempt has not begun its ClientHello
    fn hold_initial(
        &mut self,
        now: Instant,
        addresses: FourTuple,
        ecn: Option<EcnCodepoint>,
        data: BytesMut,
        dst_cid: ConnectionId,
        server_config: ServerConfigHandle,
    ) {
        let Some(server_config) = self.configs.server_config(server_config) else {
            return;
        };
        let initial_rtt = server_config.transport.initial_rtt;
        let incoming_buffer_size = server_config.incoming_buffer_size;
        let incoming_buffer_size_total = server_config.incoming_buffer_size_total;
        let len = data.len() as u64;
        let key = match self.index.held(&dst_cid) {
            Some(key) => key,
            None => {
                // RFC 9002 §6.2.1: PTO = smoothed_rtt + max(4 × rttvar, kGranularity), with no
                // max_ack_delay in the Initial space; before any RTT sample, smoothed_rtt is the
                // initial RTT and rttvar half of it (§5.3).
                // Durations: saturating can only lengthen them; the divisor is a nonzero
                // constant, so the division never refuses
                let rtt = initial_rtt;
                let rttvar = rtt.checked_div(2).unwrap_or(Duration::ZERO);
                let pto = rtt.saturating_add(cmp::max(rttvar.saturating_mul(4), TIMER_GRANULARITY));
                let Some(expires) = pto.checked_mul(3).and_then(|t| now.checked_add(t)) else {
                    debug!("not holding initial for {}: expiry out of range", dst_cid);
                    return;
                };
                let key = self.held.insert(HeldInitial {
                    dst_cid,
                    expires,
                    datagrams: Vec::new(),
                    total_bytes: 0,
                });
                self.held_expiry.push(Reverse((expires, key, dst_cid)));
                self.index.insert_initial_held(dst_cid, key);
                key
            }
        };
        // The index names only live entries
        let Some(entry) = self.held.get_mut(key) else {
            return;
        };
        let total = entry
            .total_bytes
            .checked_add(len)
            .filter(|&n| n <= incoming_buffer_size);
        let all = self
            .all_incoming_buffers_total_bytes
            .checked_add(len)
            .filter(|&n| n <= incoming_buffer_size_total);
        let (Some(total), Some(all)) = (total, all) else {
            debug!("not holding initial for {}: incoming buffers full", dst_cid);
            return;
        };
        entry.datagrams.push(HeldDatagram {
            now,
            remote: addresses.remote,
            ecn,
            data,
        });
        entry.total_bytes = total;
        self.all_incoming_buffers_total_bytes = all;
    }

    /// Take the datagrams held for `dst_cid`, decoded for delivery to its connection
    fn release_held(&mut self, dst_cid: ConnectionId) -> (Vec<DatagramConnectionEvent>, u64) {
        let Some(key) = self.index.held(&dst_cid) else {
            return (Vec::new(), 0);
        };
        // The expiry heap keeps a stale entry, which `expire_held` recognises and skips; the
        // index names only live entries
        let Some(entry) = self.held.try_remove(key) else {
            return (Vec::new(), 0);
        };
        let parser = FixedLengthConnectionIdParser::new(self.local_cid_generator.cid_len());
        let mut datagrams = Vec::with_capacity(entry.datagrams.len());
        for held in entry.datagrams {
            // Each decoded once already when it arrived, so a failure here is not reachable
            if let Ok((first_decode, remaining)) = PartialDecode::new(
                held.data,
                &parser,
                &self.config.supported_versions,
                self.config.grease_quic_bit,
            ) {
                datagrams.push(DatagramConnectionEvent {
                    now: held.now,
                    remote: held.remote,
                    ecn: held.ecn,
                    first_decode,
                    remaining,
                });
            }
        }
        (datagrams, entry.total_bytes)
    }

    /// Drop held Initials whose ClientHello did not begin before they expired
    fn expire_held(&mut self, now: Instant) {
        while let Some(&Reverse((expires, key, dst_cid))) = self.held_expiry.peek() {
            if expires > now {
                break;
            }
            self.held_expiry.pop();
            let live = self
                .held
                .get(key)
                .is_some_and(|held| held.dst_cid == dst_cid && held.expires == expires);
            if live && let Some(held) = self.held.try_remove(key) {
                // Counted in the total as each was held
                self.all_incoming_buffers_total_bytes = self
                    .all_incoming_buffers_total_bytes
                    .saturating_sub(held.total_bytes);
                if self.index.held(&dst_cid) == Some(key) {
                    self.index.remove_initial(dst_cid);
                }
                trace!("held initials for {} expired", dst_cid);
            }
        }
    }

    /// Attempt to accept this incoming connection (an error may still occur)
    ///
    /// `server_config`, when given, is a configuration from
    /// [`insert_server_config`](Self::insert_server_config) to accept this connection under in
    /// place of the one active when the attempt arrived.
    // AcceptError cannot be made smaller without semver breakage
    #[allow(clippy::result_large_err)]
    pub fn accept(
        &mut self,
        incoming: Incoming,
        now: Instant,
        buf: &mut Vec<u8>,
        server_config: Option<ServerConfigHandle>,
        qlog: Option<QlogStream>,
    ) -> Result<(ConnectionHandle, Connection), AcceptError> {
        let remote_address_validated = incoming.remote_address_validated();
        incoming.improper_drop_warner.dismiss();
        // An attempt this endpoint made keeps its buffer until it is accepted, refused, retried
        // or ignored; another's is refused, where upstream panicked
        let Some(incoming_buffer) = self.incoming_buffers.try_remove(incoming.incoming_idx) else {
            return Err(AcceptError {
                cause: ConnectionError::TransportError(TransportError::INTERNAL_ERROR(
                    "unknown incoming attempt",
                )),
                response: None,
            });
        };
        // Counted in the total as each datagram was buffered
        self.all_incoming_buffers_total_bytes = self
            .all_incoming_buffers_total_bytes
            .saturating_sub(incoming_buffer.total_bytes);

        let packet_number = incoming.packet.header.number.expand(0);
        let InitialHeader {
            src_cid,
            dst_cid,
            version,
            ..
        } = incoming.packet.header;
        let handle = server_config.unwrap_or(incoming_buffer.server_config);
        // The connection, if one is made, counts on `handle`; the attempt no longer counts on the
        // configuration it arrived under
        let counted = self.configs.acquire(handle.0);
        self.configs.release(incoming_buffer.server_config.0);
        let Some(server_config) = self.configs.server_config(handle).filter(|_| counted) else {
            if counted {
                self.configs.release(handle.0);
            }
            debug!("refusing connection: its server configuration is no longer held");
            self.index.remove_initial(dst_cid);
            return Err(AcceptError {
                cause: ConnectionError::TransportError(released_server_config()),
                response: self.initial_close(
                    version,
                    incoming.addresses,
                    &incoming.crypto,
                    &src_cid,
                    released_server_config(),
                    buf,
                ),
            });
        };

        if server_config
            .transport
            .max_idle_timeout
            .is_some_and(|timeout| {
                // A deadline past the clock's range is never reached
                incoming
                    .received_at
                    .checked_add(Duration::from_millis(timeout.into()))
                    .is_some_and(|deadline| deadline <= now)
            })
        {
            debug!("abandoning accept of stale initial");
            self.configs.release(handle.0);
            self.index.remove_initial(dst_cid);
            return Err(AcceptError {
                cause: ConnectionError::TimedOut,
                response: None,
            });
        }

        if self.cids_exhausted() {
            debug!("refusing connection");
            self.configs.release(handle.0);
            self.index.remove_initial(dst_cid);
            return Err(AcceptError {
                cause: ConnectionError::CidsExhausted,
                response: self.initial_close(
                    version,
                    incoming.addresses,
                    &incoming.crypto,
                    &src_cid,
                    TransportError::CONNECTION_REFUSED(""),
                    buf,
                ),
            });
        }

        let ch = ConnectionHandle(self.connections.vacant_key());
        let Some(loc_cid) = self.new_cid(ch) else {
            return Err(self.refuse_accept(
                handle,
                [None, None],
                (
                    ConnectionError::CidsExhausted,
                    TransportError::CONNECTION_REFUSED(""),
                ),
                version,
                incoming.addresses,
                &incoming.crypto,
                (src_cid, dst_cid),
                buf,
            ));
        };
        let Some(server_config) = self.configs.server_config(handle) else {
            self.configs.release(handle.0);
            return Err(AcceptError {
                cause: ConnectionError::TransportError(released_server_config()),
                response: None,
            });
        };
        let mut params = TransportParameters::new(
            &server_config.transport,
            &self.config,
            self.local_cid_generator.as_ref(),
            loc_cid,
            Some(server_config),
            &mut self.rng,
        );
        let migration = server_config.migration;
        let has_preferred_address = server_config.has_preferred_address();
        let (address_v4, address_v6) = (
            server_config.preferred_address_v4,
            server_config.preferred_address_v6,
        );
        let validation_tokens_sent = server_config.validation_token.sent;
        params.stateless_reset_token = Some(ResetToken::new(&self.config.reset_key, loc_cid));
        params.original_dst_cid = Some(incoming.token.orig_dst_cid);
        params.retry_src_cid = incoming.token.retry_src_cid;
        let mut pref_addr_cid = None;
        if has_preferred_address {
            let Some(cid) = self.new_cid(ch) else {
                return Err(self.refuse_accept(
                    handle,
                    [Some(loc_cid), None],
                    (
                        ConnectionError::CidsExhausted,
                        TransportError::CONNECTION_REFUSED(""),
                    ),
                    version,
                    incoming.addresses,
                    &incoming.crypto,
                    (src_cid, dst_cid),
                    buf,
                ));
            };
            pref_addr_cid = Some(cid);
            params.preferred_address = Some(PreferredAddress {
                address_v4,
                address_v6,
                connection_id: cid,
                stateless_reset_token: ResetToken::new(&self.config.reset_key, cid),
            });
        }

        let Some(server_config) = self.configs.server_config(handle) else {
            self.configs.release(handle.0);
            return Err(AcceptError {
                cause: ConnectionError::TransportError(released_server_config()),
                response: None,
            });
        };
        let tls = match server_config.crypto.start_session(version, &params) {
            Ok(tls) => tls,
            Err(e) => {
                return Err(self.refuse_accept(
                    handle,
                    [Some(loc_cid), pref_addr_cid],
                    (ConnectionError::TransportError(e.clone()), e),
                    version,
                    incoming.addresses,
                    &incoming.crypto,
                    (src_cid, dst_cid),
                    buf,
                ));
            }
        };
        let transport_config = server_config.transport.clone();
        let side_args = SideArgs::Server {
            migration,
            has_preferred_address,
            validation_tokens_sent,
            pref_addr_cid,
            path_validated: remote_address_validated,
        };
        let mut conn = self.add_connection(
            ch,
            version,
            dst_cid,
            loc_cid,
            src_cid,
            incoming.addresses,
            incoming.received_at,
            tls,
            handle.0,
            transport_config,
            qlog,
            side_args,
        );
        self.index.insert_initial(dst_cid, ch);

        match conn.handle_first_packet(
            &mut self.configs,
            incoming.received_at,
            incoming.addresses.remote,
            incoming.ecn,
            packet_number,
            incoming.packet,
            incoming.rest,
        ) {
            Ok(()) => {
                trace!(id = ch.0, icid = %dst_cid, "new connection");

                for event in incoming_buffer.datagrams {
                    conn.handle_event(
                        ConnectionEvent(ConnectionEventInner::Datagram(event)),
                        &mut self.configs,
                    )
                }

                Ok((ch, conn))
            }
            Err(e) => {
                debug!("handshake failed: {}", e);
                self.handle_event(ch, EndpointEvent(EndpointEventInner::Drained));
                let response = match e {
                    ConnectionError::TransportError(ref e) => self.initial_close(
                        version,
                        incoming.addresses,
                        &incoming.crypto,
                        &src_cid,
                        e.clone(),
                        buf,
                    ),
                    _ => None,
                };
                Err(AcceptError { cause: e, response })
            }
        }
    }

    /// Refuses an attempt part way through `accept`, taking back the CIDs already found for it
    /// and the configuration slot it counted on
    fn refuse_accept(
        &mut self,
        handle: ServerConfigHandle,
        found: [Option<ConnectionId>; 2],
        (cause, reason): (ConnectionError, TransportError),
        version: u32,
        addresses: FourTuple,
        crypto: &Keys,
        (src_cid, dst_cid): (ConnectionId, ConnectionId),
        buf: &mut Vec<u8>,
    ) -> AcceptError {
        debug!("refusing connection: {}", cause);
        for cid in found.into_iter().flatten() {
            self.index.connection_ids.remove(&cid);
        }
        self.configs.release(handle.0);
        self.index.remove_initial(dst_cid);
        AcceptError {
            cause,
            response: self.initial_close(version, addresses, crypto, &src_cid, reason, buf),
        }
    }

    /// Check if we should refuse a connection attempt regardless of the packet's contents
    fn early_validate_first_packet(
        &mut self,
        header: &ProtectedInitialHeader,
    ) -> Result<(), TransportError> {
        // RFC9000 §7.2 dictates that initial (client-chosen) destination CIDs must be at least 8
        // bytes. If this is a Retry packet, then the length must instead match our usual CID
        // length. If we ever issue non-Retry address validation tokens via `NEW_TOKEN`, then we'll
        // also need to validate CID length for those after decoding the token.
        if header.dst_cid.len() < 8
            && (header.token_pos.is_empty()
                || header.dst_cid.len() != self.local_cid_generator.cid_len())
        {
            debug!(
                "rejecting connection due to invalid DCID length {}",
                header.dst_cid.len()
            );
            return Err(TransportError::PROTOCOL_VIOLATION(
                "invalid destination CID length",
            ));
        }

        Ok(())
    }

    /// Reject this incoming connection attempt
    ///
    /// `None` where the close cannot be protected (upstream panicked); the attempt is cleaned up
    /// either way.
    pub fn refuse(&mut self, incoming: Incoming, buf: &mut Vec<u8>) -> Option<Transmit> {
        self.clean_up_incoming(&incoming);
        incoming.improper_drop_warner.dismiss();

        self.initial_close(
            incoming.packet.header.version,
            incoming.addresses,
            &incoming.crypto,
            &incoming.packet.header.src_cid,
            TransportError::CONNECTION_REFUSED(""),
            buf,
        )
    }

    /// Respond with a retry packet, requiring the client to retry with address validation
    ///
    /// Errors if `incoming.may_retry()` is false.
    pub fn retry(&mut self, incoming: Incoming, buf: &mut Vec<u8>) -> Result<Transmit, RetryError> {
        if !incoming.may_retry() {
            return Err(RetryError(Box::new(incoming)));
        }

        // An attempt this endpoint made keeps its buffer; another's is handed back untouched
        let Some(server_handle) = self
            .incoming_buffers
            .get(incoming.incoming_idx)
            .map(|buffer| buffer.server_config)
        else {
            return Err(RetryError(Box::new(incoming)));
        };
        let Some(server_config) = self.configs.server_config(server_handle) else {
            // A pending attempt keeps its configuration's slot; were it gone, nothing could sign
            // the token, and the attempt is handed back untouched
            return Err(RetryError(Box::new(incoming)));
        };

        // First Initial
        // The peer will use this as the DCID of its following Initials. Initial DCIDs are
        // looked up separately from Handshake/Data DCIDs, so there is no risk of collision
        // with established connections. In the unlikely event that a collision occurs
        // between two connections in the initial phase, both will fail fast and may be
        // retried by the application layer.
        let loc_cid = self.local_cid_generator.generate_cid();

        let payload = TokenPayload::Retry {
            address: incoming.addresses.remote,
            orig_dst_cid: incoming.packet.header.dst_cid,
            issued: server_config.time_source.now(),
        };
        let Some(token) = Token::new(payload, &mut self.rng).encode(&*server_config.token_key)
        else {
            // A key that cannot seal a token cannot validate one either; the attempt is handed
            // back untouched
            return Err(RetryError(Box::new(incoming)));
        };

        let header = Header::Retry {
            src_cid: loc_cid,
            dst_cid: incoming.packet.header.src_cid,
            version: incoming.packet.header.version,
        };

        let start = buf.len();
        let encode = header.encode(buf);
        buf.put_slice(&token);
        // A key that cannot tag the Retry hands the attempt back untouched (upstream panicked);
        // a Retry has no packet number, so `finish` protects nothing and cannot fail
        let Ok(tag) = server_config.crypto.retry_tag(
            incoming.packet.header.version,
            &incoming.packet.header.dst_cid,
            buf,
        ) else {
            buf.truncate(start);
            return Err(RetryError(Box::new(incoming)));
        };
        buf.extend_from_slice(&tag);
        if encode
            .finish(buf, &*incoming.crypto.header.local, None)
            .is_err()
        {
            buf.truncate(start);
            return Err(RetryError(Box::new(incoming)));
        }

        self.clean_up_incoming(&incoming);
        incoming.improper_drop_warner.dismiss();

        Ok(Transmit {
            destination: incoming.addresses.remote,
            ecn: None,
            size: buf.len(),
            segment_size: None,
            src_ip: incoming.addresses.local_ip,
        })
    }

    /// Ignore this incoming connection attempt, not sending any packet in response
    ///
    /// Doing this actively, rather than merely dropping the [`Incoming`], is necessary to prevent
    /// memory leaks due to state within [`Endpoint`] tracking the incoming connection.
    pub fn ignore(&mut self, incoming: Incoming) {
        self.clean_up_incoming(&incoming);
        incoming.improper_drop_warner.dismiss();
    }

    /// Clean up endpoint data structures associated with an `Incoming`.
    fn clean_up_incoming(&mut self, incoming: &Incoming) {
        self.index.remove_initial(incoming.packet.header.dst_cid);
        // An attempt this endpoint made keeps its buffer until now; another's has nothing here
        // to clean up, where upstream panicked
        if let Some(incoming_buffer) = self.incoming_buffers.try_remove(incoming.incoming_idx) {
            // Counted in the total as each datagram was buffered
            self.all_incoming_buffers_total_bytes = self
                .all_incoming_buffers_total_bytes
                .saturating_sub(incoming_buffer.total_bytes);
            self.configs.release(incoming_buffer.server_config.0);
        }
    }

    fn add_connection(
        &mut self,
        ch: ConnectionHandle,
        version: u32,
        init_cid: ConnectionId,
        loc_cid: ConnectionId,
        rem_cid: ConnectionId,
        addresses: FourTuple,
        now: Instant,
        tls: Box<dyn crypto::Session>,
        shared_config: ConfigKey,
        transport_config: TransportConfig,
        qlog: Option<QlogStream>,
        side_args: SideArgs,
    ) -> Connection {
        let mut rng_seed = [0; 32];
        self.rng.fill_bytes(&mut rng_seed);
        let side = side_args.side();
        let pref_addr_cid = side_args.pref_addr_cid();
        let resume = match self.config.careful_resume {
            Some(config) => CarefulResume::new(
                self.congestion_memory
                    .take(addresses.remote.ip(), now, config.lifetime),
                config.max_jump,
                config.warm_up,
                now,
            ),
            None => CarefulResume::new(Grant::Neither, 0, false, now),
        };
        let losses = self
            .loss_memory
            .get(addresses.remote.ip(), now, self.config.loss_memory.1);
        let conn = Connection::new(
            self.config.grease_quic_bit,
            transport_config,
            qlog.into(),
            init_cid,
            loc_cid,
            rem_cid,
            addresses.remote,
            addresses.local_ip,
            tls,
            shared_config,
            self.local_cid_generator.as_ref(),
            now,
            version,
            self.allow_mtud,
            rng_seed,
            side_args,
            resume,
            losses,
        );

        // The handshake CID is sequence 0, a preferred address's sequence 1
        let mut cids_issued = 1;
        let mut loc_cids = FxHashMap::default();
        loc_cids.insert(0, loc_cid);
        if let Some(cid) = pref_addr_cid {
            loc_cids.insert(1, cid);
            cids_issued = 2;
        }

        // `ch` is the slab's vacant key, taken just before with nothing inserted since
        self.connections.insert(ConnectionMeta {
            init_cid,
            cids_issued,
            loc_cids,
            addresses,
            side,
            reset_token: None,
            config: shared_config,
        });

        self.index.insert_conn(addresses, loc_cid, ch, side);

        conn
    }

    fn initial_close(
        &mut self,
        version: u32,
        addresses: FourTuple,
        crypto: &Keys,
        remote_id: &ConnectionId,
        reason: TransportError,
        buf: &mut Vec<u8>,
    ) -> Option<Transmit> {
        // We don't need to worry about CID collisions in initial closes because the peer
        // shouldn't respond, and if it does, and the CID collides, we'll just drop the
        // unexpected response.
        let local_id = self.local_cid_generator.generate_cid();
        let number = PacketNumber::U8(0);
        let header = Header::Initial(InitialHeader {
            dst_cid: *remote_id,
            src_cid: local_id,
            number,
            token: Bytes::new(),
            version,
        });

        let start = buf.len();
        let partial_encode = header.encode(buf);
        // Room for the close: saturating can only under-state it, which shortens the reason
        let max_len = usize::from(INITIAL_MTU)
            .saturating_sub(partial_encode.header_len)
            .saturating_sub(crypto.packet.local.tag_len());
        frame::Close::from(reason).encode(buf, max_len);
        buf.resize(buf.len().saturating_add(crypto.packet.local.tag_len()), 0);
        // A close packet is long enough for its header protection sample; were the keys to
        // refuse it, nothing is sent rather than an unprotected packet (upstream panicked)
        if partial_encode
            .finish(buf, &*crypto.header.local, Some((0, &*crypto.packet.local)))
            .is_err()
        {
            buf.truncate(start);
            return None;
        }
        Some(Transmit {
            destination: addresses.remote,
            ecn: None,
            size: buf.len(),
            segment_size: None,
            src_ip: addresses.local_ip,
        })
    }

    /// Access the configuration used by this endpoint
    pub fn config(&self) -> &EndpointConfig {
        &self.config
    }

    /// Number of connections that are currently open
    pub fn open_connections(&self) -> usize {
        self.connections.len()
    }

    /// Counter for the number of bytes currently used
    /// in the buffers for Initial and 0-RTT messages for pending incoming connections
    pub fn incoming_buffer_bytes(&self) -> u64 {
        self.all_incoming_buffers_total_bytes
    }

    #[cfg(test)]
    pub(crate) fn known_connections(&self) -> usize {
        let x = self.connections.len();
        // Held Initials (`held_initials`) are routed by initial CID too, but are not connections
        debug_assert_eq!(x, self.index.connection_ids_initial.len() - self.held.len());
        // Not all connections have known reset tokens
        debug_assert!(x >= self.index.connection_reset_tokens.0.len());
        // Not all connections have unique remotes, and 0-length CIDs might not be in use.
        debug_assert!(x >= self.index.incoming_connection_remotes.len());
        debug_assert!(x >= self.index.outgoing_connection_remotes.len());
        x
    }

    #[cfg(test)]
    pub(crate) fn held_initials(&self) -> usize {
        self.held.len()
    }

    #[cfg(test)]
    pub(crate) fn known_cids(&self) -> usize {
        self.index.connection_ids.len()
    }

    /// Whether we've used up 3/4 of the available CID space
    ///
    /// We leave some space unused so that `new_cid` can be relied upon to finish quickly. We don't
    /// bother to check when CID longer than 4 bytes are used because 2^40 connections is a lot.
    fn cids_exhausted(&self) -> bool {
        let cid_len = self.local_cid_generator.cid_len();
        if cid_len == 0 || cid_len > 4 {
            return false;
        }
        // Counted in u64, where upstream's `usize` overflowed for 4-byte CIDs on 32-bit
        // targets: 2^(8 * len) CIDs, a quarter of them kept free
        let space = 1u64 << (cid_len << 3);
        space.saturating_sub(self.index.connection_ids.len() as u64) < space >> 2
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Endpoint")
            .field("rng", &self.rng)
            .field("index", &self.index)
            .field("connections", &self.connections)
            .field("config", &self.config)
            .field("server_config", &self.server_config)
            // incoming_buffers too large
            .field("incoming_buffers.len", &self.incoming_buffers.len())
            .field(
                "all_incoming_buffers_total_bytes",
                &self.all_incoming_buffers_total_bytes,
            )
            .finish()
    }
}

/// Initial datagrams of a connection attempt whose ClientHello has not begun
struct HeldInitial {
    dst_cid: ConnectionId,
    /// When the entry is dropped if the ClientHello has still not begun: three probe timeouts
    /// computed from the server's initial RTT, the time RFC 9000 §10.2 allows a peer's stray
    /// packets to keep arriving after state ends
    expires: Instant,
    datagrams: Vec<HeldDatagram>,
    total_bytes: u64,
}

/// One held datagram, kept whole so the connection can decode it as if it had just arrived
struct HeldDatagram {
    now: Instant,
    remote: SocketAddr,
    ecn: Option<EcnCodepoint>,
    data: BytesMut,
}

/// What a decrypted Initial from an unknown attempt carries
enum FirstInitial {
    /// The first byte of the ClientHello: the attempt begins here
    Begins,
    /// More of a ClientHello whose start has not arrived
    Continues,
    /// A CONNECTION_CLOSE, or no valid frames, before any ClientHello: nothing can follow, so
    /// nothing is kept
    Closes,
}

fn classify_first_initial(payload: &BytesMut) -> FirstInitial {
    let Ok(frames) = frame::Iter::new(Bytes::copy_from_slice(payload)) else {
        return FirstInitial::Closes;
    };
    let mut closes = false;
    for frame in frames.filter_map(Result::ok) {
        match frame {
            Frame::Crypto(ref crypto) if crypto.offset == 0 => return FirstInitial::Begins,
            Frame::Close(_) => closes = true,
            _ => {}
        }
    }
    if closes {
        FirstInitial::Closes
    } else {
        FirstInitial::Continues
    }
}

/// Buffered Initial and 0-RTT messages for a pending incoming connection
struct IncomingBuffer {
    /// The configuration the attempt arrived under, counted on until it is accepted or dismissed
    server_config: ServerConfigHandle,
    datagrams: Vec<DatagramConnectionEvent>,
    total_bytes: u64,
}

/// Part of protocol state incoming datagrams can be routed to
#[derive(Copy, Clone, Debug)]
enum RouteDatagramTo {
    Incoming(usize),
    /// An index into `Endpoint::held`
    Held(usize),
    Connection(ConnectionHandle),
}

/// Maps packets to existing connections
#[derive(Default, Debug)]
struct ConnectionIndex {
    /// Identifies connections based on the initial DCID the peer utilized
    ///
    /// Uses a standard `HashMap` to protect against hash collision attacks.
    ///
    /// Used by the server, not the client.
    connection_ids_initial: HashMap<ConnectionId, RouteDatagramTo>,
    /// Identifies connections based on locally created CIDs
    ///
    /// Uses a cheaper hash function since keys are locally created
    connection_ids: FxHashMap<ConnectionId, ConnectionHandle>,
    /// Identifies incoming connections with zero-length CIDs
    ///
    /// Uses a standard `HashMap` to protect against hash collision attacks.
    incoming_connection_remotes: HashMap<FourTuple, ConnectionHandle>,
    /// Identifies outgoing connections with zero-length CIDs
    ///
    /// We don't yet support explicit source addresses for client connections, and zero-length CIDs
    /// require a unique four-tuple, so at most one client connection with zero-length local CIDs
    /// may be established per remote. We must omit the local address from the key because we don't
    /// necessarily know what address we're sending from, and hence receiving at.
    ///
    /// Uses a standard `HashMap` to protect against hash collision attacks.
    outgoing_connection_remotes: HashMap<SocketAddr, ConnectionHandle>,
    /// Reset tokens provided by the peer for the CID each connection is currently sending to
    ///
    /// Incoming stateless resets do not have correct CIDs, so we need this to identify the correct
    /// recipient, if any.
    connection_reset_tokens: ResetTokenTable,
}

impl ConnectionIndex {
    /// Associate an incoming connection with its initial destination CID
    fn insert_initial_incoming(&mut self, dst_cid: ConnectionId, incoming_key: usize) {
        if dst_cid.is_empty() {
            return;
        }
        self.connection_ids_initial
            .insert(dst_cid, RouteDatagramTo::Incoming(incoming_key));
    }

    /// Associate held Initial datagrams with their initial destination CID
    fn insert_initial_held(&mut self, dst_cid: ConnectionId, held_key: usize) {
        if dst_cid.is_empty() {
            return;
        }
        self.connection_ids_initial
            .insert(dst_cid, RouteDatagramTo::Held(held_key));
    }

    /// The held entry for an initial destination CID, if it has one
    fn held(&self, dst_cid: &ConnectionId) -> Option<usize> {
        match self.connection_ids_initial.get(dst_cid) {
            Some(&RouteDatagramTo::Held(key)) => Some(key),
            _ => None,
        }
    }

    /// Remove an association with an initial destination CID
    fn remove_initial(&mut self, dst_cid: ConnectionId) {
        if dst_cid.is_empty() {
            return;
        }
        self.connection_ids_initial.remove(&dst_cid);
    }

    /// Associate a connection with its initial destination CID
    fn insert_initial(&mut self, dst_cid: ConnectionId, connection: ConnectionHandle) {
        if dst_cid.is_empty() {
            return;
        }
        self.connection_ids_initial
            .insert(dst_cid, RouteDatagramTo::Connection(connection));
    }

    /// Associate a connection with its first locally-chosen destination CID if used, or otherwise
    /// its current 4-tuple
    fn insert_conn(
        &mut self,
        addresses: FourTuple,
        dst_cid: ConnectionId,
        connection: ConnectionHandle,
        side: Side,
    ) {
        match dst_cid.len() {
            0 => match side {
                Side::Server => {
                    self.incoming_connection_remotes
                        .insert(addresses, connection);
                }
                Side::Client => {
                    self.outgoing_connection_remotes
                        .insert(addresses.remote, connection);
                }
            },
            _ => {
                self.connection_ids.insert(dst_cid, connection);
            }
        }
    }

    /// Discard a connection ID
    fn retire(&mut self, dst_cid: ConnectionId) {
        self.connection_ids.remove(&dst_cid);
    }

    /// Remove all references to a connection
    fn remove(&mut self, conn: &ConnectionMeta) {
        if conn.side.is_server() {
            self.remove_initial(conn.init_cid);
        }
        for cid in conn.loc_cids.values() {
            self.connection_ids.remove(cid);
        }
        self.incoming_connection_remotes.remove(&conn.addresses);
        self.outgoing_connection_remotes
            .remove(&conn.addresses.remote);
        if let Some((remote, token)) = conn.reset_token {
            self.connection_reset_tokens.remove(remote, token);
        }
    }

    /// Find the existing connection that `datagram` should be routed to, if any
    fn get(&self, addresses: &FourTuple, datagram: &PartialDecode) -> Option<RouteDatagramTo> {
        if !datagram.dst_cid().is_empty()
            && let Some(&ch) = self.connection_ids.get(datagram.dst_cid())
        {
            return Some(RouteDatagramTo::Connection(ch));
        }
        if (datagram.is_initial() || datagram.is_0rtt())
            && let Some(&ch) = self.connection_ids_initial.get(datagram.dst_cid())
        {
            return Some(ch);
        }
        if datagram.dst_cid().is_empty() {
            if let Some(&ch) = self.incoming_connection_remotes.get(addresses) {
                return Some(RouteDatagramTo::Connection(ch));
            }
            if let Some(&ch) = self.outgoing_connection_remotes.get(&addresses.remote) {
                return Some(RouteDatagramTo::Connection(ch));
            }
        }
        let data = datagram.data();
        let token = data
            .len()
            .checked_sub(RESET_TOKEN_SIZE)
            .and_then(|start| data.get(start..))?;
        self.connection_reset_tokens
            .get(addresses.remote, token)
            .cloned()
            .map(RouteDatagramTo::Connection)
    }
}

#[derive(Debug)]
pub(crate) struct ConnectionMeta {
    init_cid: ConnectionId,
    /// Number of local connection IDs that have been issued in NEW_CONNECTION_ID frames.
    cids_issued: u64,
    loc_cids: FxHashMap<u64, ConnectionId>,
    /// Remote/local addresses the connection began with
    ///
    /// Only needed to support connections with zero-length CIDs, which cannot migrate, so we don't
    /// bother keeping it up to date.
    addresses: FourTuple,
    side: Side,
    /// Reset token provided by the peer for the CID we're currently sending to, and the address
    /// being sent to
    reset_token: Option<(SocketAddr, ResetToken)>,
    /// The configuration slot the connection counts on, released when it drains
    config: ConfigKey,
}

/// Internal identifier for a `Connection` currently associated with an endpoint
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ConnectionHandle(pub usize);

impl From<ConnectionHandle> for usize {
    fn from(x: ConnectionHandle) -> Self {
        x.0
    }
}

/// Event resulting from processing a single datagram
pub enum DatagramEvent {
    /// The datagram is redirected to its `Connection`
    ConnectionEvent(ConnectionHandle, ConnectionEvent),
    /// The datagram may result in starting a new `Connection`
    NewConnection(Incoming),
    /// Response generated directly by the endpoint
    Response(Transmit),
}

/// An incoming connection for which the server has not yet begun its part of the handshake.
pub struct Incoming {
    received_at: Instant,
    addresses: FourTuple,
    ecn: Option<EcnCodepoint>,
    /// The first packet, its payload already authenticated and decrypted
    packet: InitialPacket,
    rest: Option<BytesMut>,
    crypto: Keys,
    token: IncomingToken,
    incoming_idx: usize,
    improper_drop_warner: IncomingImproperDropWarner,
}

impl Incoming {
    /// The local IP address which was used when the peer established the connection
    ///
    /// This has the same behavior as [`Connection::local_ip`].
    pub fn local_ip(&self) -> Option<IpAddr> {
        self.addresses.local_ip
    }

    /// The peer's UDP address
    pub fn remote_address(&self) -> SocketAddr {
        self.addresses.remote
    }

    /// Whether the socket address that is initiating this connection has been validated
    ///
    /// This means that the sender of the initial packet has proved that they can receive traffic
    /// sent to `self.remote_address()`.
    ///
    /// If `self.remote_address_validated()` is false, `self.may_retry()` is guaranteed to be true.
    /// The inverse is not guaranteed.
    pub fn remote_address_validated(&self) -> bool {
        self.token.validated
    }

    /// Whether it is legal to respond with a retry packet
    ///
    /// If `self.remote_address_validated()` is false, `self.may_retry()` is guaranteed to be true.
    /// The inverse is not guaranteed.
    pub fn may_retry(&self) -> bool {
        self.token.retry_src_cid.is_none()
    }

    /// The original destination connection ID sent by the client
    pub fn orig_dst_cid(&self) -> &ConnectionId {
        &self.token.orig_dst_cid
    }
}

impl fmt::Debug for Incoming {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Incoming")
            .field("addresses", &self.addresses)
            .field("ecn", &self.ecn)
            // packet doesn't implement debug
            // rest is too big and not meaningful enough
            .field("token", &self.token)
            .field("incoming_idx", &self.incoming_idx)
            // improper drop warner contains no information
            .finish_non_exhaustive()
    }
}

/// Unprotects and authenticates a first Initial: `None` if it does not decode, has reserved
/// bits set, or fails AEAD
///
/// Authenticating the payload comes before anything acts on this packet: an Initial whose
/// payload fails AEAD is not a connection attempt, and must not reach the application or spend
/// an address validation token in the token log.
fn authenticate_first_packet(first_decode: PartialDecode, crypto: &Keys) -> Option<Packet> {
    let mut packet = match first_decode.finish(Some(&*crypto.header.remote)) {
        Ok(packet) => packet,
        Err(e) => {
            trace!("unable to decode initial packet: {}", e);
            return None;
        }
    };

    if !packet.reserved_bits_valid() {
        debug!("dropping connection attempt with invalid reserved bits");
        return None;
    }

    let packet_number = packet.header.number()?.expand(0);
    if crypto
        .packet
        .remote
        .decrypt(packet_number, &packet.header_data, &mut packet.payload)
        .is_err()
    {
        debug!(
            packet_number,
            "dropping initial packet that fails authentication"
        );
        return None;
    }
    Some(packet)
}

/// The system's random number generator could not seed an endpoint's
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("the system's random number generator is unavailable")]
pub struct RngUnavailable;

/// Warns when an [`Incoming`] is dropped undecided; dismissed once it is decided (upstream
/// forgot it instead)
struct IncomingImproperDropWarner {
    dismissed: bool,
}

impl IncomingImproperDropWarner {
    fn dismiss(mut self) {
        self.dismissed = true;
    }
}

impl Drop for IncomingImproperDropWarner {
    fn drop(&mut self) {
        if self.dismissed {
            return;
        }
        warn!(
            "hyper_quic::Incoming dropped without passing to Endpoint::accept/refuse/retry/ignore \
               (may cause memory leak and eventual inability to accept new connections)"
        );
    }
}

/// The error for an incoming attempt whose server configuration's slot no longer holds it
///
/// The endpoint keeps a slot while attempts or connections count on it, so this is reached only
/// through a handle from [`Endpoint::insert_server_config`] that was retired with nothing on it.
fn released_server_config() -> TransportError {
    TransportError::CONNECTION_REFUSED("server configuration no longer held")
}

/// Errors in the parameters being used to create a new connection
///
/// These arise before any I/O has been performed.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConnectError {
    /// The endpoint can no longer create new connections
    ///
    /// Indicates that a necessary component of the endpoint has been dropped or otherwise disabled.
    #[error("endpoint stopping")]
    EndpointStopping,
    /// The connection could not be created because not enough of the CID space is available
    ///
    /// Try using longer connection IDs
    #[error("CIDs exhausted")]
    CidsExhausted,
    /// The given server name was malformed
    #[error("invalid server name: {0}")]
    InvalidServerName(String),
    /// The remote [`SocketAddr`] supplied was malformed
    ///
    /// Examples include attempting to connect to port 0, or using an inappropriate address family.
    #[error("invalid remote address: {0}")]
    InvalidRemoteAddress(SocketAddr),
    /// No default client configuration was set up
    ///
    /// Use `Endpoint::connect_with` to specify a client configuration.
    #[error("no default client config")]
    NoDefaultClientConfig,
    /// The client configuration handle does not refer to a configuration this endpoint holds
    ///
    /// The configuration was retired, or the handle came from another endpoint.
    #[error("unknown client configuration")]
    UnknownConfig,
    /// The local endpoint does not support the QUIC version specified in the client configuration
    #[error("unsupported QUIC version")]
    UnsupportedVersion,
    /// The client configuration's TLS configuration cannot start a QUIC session (it lacks
    /// TLS 1.3, for one); upstream panicked
    #[error("TLS configuration cannot start a QUIC session")]
    InvalidTlsConfig,
}

/// Error type for attempting to accept an [`Incoming`]
#[derive(Debug)]
pub struct AcceptError {
    /// Underlying error describing reason for failure
    pub cause: ConnectionError,
    /// Optional response to transmit back
    pub response: Option<Transmit>,
}

/// Error for attempting to retry an [`Incoming`] which already bears a token from a previous retry
#[derive(Debug, Error)]
#[error("retry() with validated Incoming")]
pub struct RetryError(Box<Incoming>);

impl RetryError {
    /// Get the [`Incoming`]
    pub fn into_incoming(self) -> Incoming {
        *self.0
    }
}

/// Reset Tokens which are associated with peer socket addresses
///
/// The standard `HashMap` is used since both `SocketAddr` and `ResetToken` are
/// peer generated and might be usable for hash collision attacks.
#[derive(Default, Debug)]
struct ResetTokenTable(HashMap<SocketAddr, HashMap<ResetToken, ConnectionHandle>>);

impl ResetTokenTable {
    fn insert(&mut self, remote: SocketAddr, token: ResetToken, ch: ConnectionHandle) -> bool {
        self.0
            .entry(remote)
            .or_default()
            .insert(token, ch)
            .is_some()
    }

    fn remove(&mut self, remote: SocketAddr, token: ResetToken) {
        use std::collections::hash_map::Entry;
        match self.0.entry(remote) {
            Entry::Vacant(_) => {}
            Entry::Occupied(mut e) => {
                e.get_mut().remove(&token);
                if e.get().is_empty() {
                    e.remove_entry();
                }
            }
        }
    }

    fn get(&self, remote: SocketAddr, token: &[u8]) -> Option<&ConnectionHandle> {
        let token = ResetToken::from(<[u8; RESET_TOKEN_SIZE]>::try_from(token).ok()?);
        self.0.get(&remote)?.get(&token)
    }
}

/// Identifies a connection by the combination of remote and local addresses
///
/// Including the local ensures good behavior when the host has multiple IP addresses on the same
/// subnet and zero-length connection IDs are in use.
#[derive(Hash, Eq, PartialEq, Debug, Copy, Clone)]
struct FourTuple {
    remote: SocketAddr,
    // A single socket can only listen on a single port, so no need to store it explicitly
    local_ip: Option<IpAddr>,
}
