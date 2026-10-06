#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
//! The congestion controllers quinn can run, measured against each other
//! (27 §3.1 P10, stage G).
//!
//! Two real QUIC endpoints (`quinn-proto`: TLS 1.3, packet protection, loss
//! recovery, pacing) exchange over `focal_sim::path` in virtual time: a
//! bottleneck of a stated rate with a drop-tail queue of one
//! bandwidth-delay product in each direction, a propagation delay and a
//! loss process. One connection carries what a focal connection carries
//! between two nodes: a transfer that sends as fast as it may (custody, a
//! checkpoint's seeds) and small exchanges that must not wait behind it
//! (consensus, probes, control). Every 50 ms the client asks 200 bytes on a
//! stream of its own and the server answers 200; what is measured is how
//! long an exchange takes and how much of the link the transfer carries.
//!
//! **The rule, fixed before any run.** A law *stalls* in a scenario where
//! its transfer carries less than a tenth of what the best law carries
//! there, or where fewer than nine in ten of its exchanges are answered.
//! A law that stalls anywhere is not chosen. Of the others, the one whose
//! exchanges' 99th percentile, as a multiple of the best law's in each
//! scenario, has the least geometric mean is preferred. quinn's default
//! (CUBIC) is replaced only by a law that is preferred by a tenth at least
//! and whose transfer carries, by the same mean, nine tenths at least of
//! what CUBIC's carries.
//!
//! The gate runs a few scenarios for a few seconds, to keep the harness and
//! the laws honest. `FOCAL_CONGESTION_FULL=1` runs the grid, which is what
//! the decision is recorded from (release, nightly).
use bytes::BytesMut;
use focal_sim::path::{Fabric, Fate, Link, Loss, Marking, Path};
use quinn_proto::{
    ClientConfig, Connection, ConnectionHandle, DatagramEvent, Dir, EcnCodepoint, Endpoint,
    EndpointConfig, Event, ServerConfig, StreamEvent, StreamId, TransportConfig, VarInt,
    congestion::{BbrConfig, ControllerFactory, CubicConfig, NewRenoConfig},
};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

const MS: u64 = 1_000_000;
const SECOND: u64 = 1_000 * MS;
const DATAGRAM: usize = 1_200;
const EXCHANGE_EVERY: u64 = 50 * MS;
const EXCHANGE_BYTES: usize = 200;
/// What the transfer hands the connection at once.
const CHUNK: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Law {
    NewReno,
    Cubic,
    Bbr,
    Copa,
    /// Copa whose window a round trip moves by this part of itself at most.
    Stride(u64),
    /// Copa whose window a mark multiplies by this fraction, as numerator
    /// and denominator.
    Backoff(u64, u64),
}
impl Law {
    const ALL: [Law; 4] = [Law::NewReno, Law::Cubic, Law::Bbr, Law::Copa];
    fn factory(self) -> Arc<dyn ControllerFactory + Send + Sync> {
        match self {
            Law::NewReno => Arc::new(NewRenoConfig::default()),
            Law::Cubic => Arc::new(CubicConfig::default()),
            Law::Bbr => Arc::new(BbrConfig::default()),
            Law::Copa => Arc::new(focal_wire::congestion::CopaConfig::default()),
            Law::Stride(stride) => Arc::new(focal_wire::congestion::CopaConfig {
                stride,
                ..focal_wire::congestion::CopaConfig::default()
            }),
            Law::Backoff(numerator, denominator) => Arc::new(focal_wire::congestion::CopaConfig {
                mark_backoff: focal_wire::congestion::MarkBackoff {
                    numerator,
                    denominator,
                },
                ..focal_wire::congestion::CopaConfig::default()
            }),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Scenario {
    rate_bits_per_second: u64,
    rtt_ns: u64,
    loss_ppm: u32,
    /// The bottleneck's queue, in bandwidth-delay products.
    queue_bdps: u64,
    /// Losses come in bursts.
    bursts: bool,
    seconds: u64,
    seed: u64,
    /// Transfers under way at once, each on a stream of its own.
    transfers: usize,
    /// Whether what is asked goes before what is transferred
    /// (`SendStream::set_priority`).
    classes: bool,
    /// Whether the transfers under way are as many as the law's window
    /// holds megabytes, and one: `focal_wire::bulk_width`. `transfers` is
    /// the most there are.
    derived: bool,
    /// Whether the path carries the IP header's ECN field (RFC 3168). One
    /// that does not bleaches it, as many do: quinn's validation hears no
    /// marks echoed and sends none ECN-capable (RFC 9000 §13.4.2).
    ecn: bool,
    /// What the bottleneck's queue manager does short of dropping.
    marking: Marking,
}
impl Scenario {
    fn bdp_bytes(&self) -> u64 {
        let bits = u128::from(self.rate_bits_per_second) * u128::from(self.rtt_ns) / 1_000_000_000;
        u64::try_from(bits / 8).unwrap()
    }
    fn name(&self) -> String {
        let rate = if self.rate_bits_per_second >= 1_000_000 {
            format!("{}M", self.rate_bits_per_second / 1_000_000)
        } else {
            format!("{}k", self.rate_bits_per_second / 1_000)
        };
        let marking = match self.marking {
            Marking::Off => String::new(),
            Marking::Step { threshold_bytes } => {
                format!(" step {}p", threshold_bytes.div_ceil(DATAGRAM as u64))
            }
            Marking::CoDel {
                target_ns,
                interval_ns,
            } => format!(" codel {}/{}ms", target_ns / MS, interval_ns / MS),
        };
        format!(
            "{rate} {}ms {}%{}{}{}{marking}",
            self.rtt_ns / MS,
            f64::from(self.loss_ppm) / 10_000.0,
            if self.bursts { " bursts" } else { "" },
            if self.queue_bdps == 1 {
                String::new()
            } else {
                format!(" queue x{}", self.queue_bdps)
            },
            if self.ecn { " ecn" } else { "" },
        )
    }
}

#[derive(Clone, Debug, Default)]
struct Measured {
    /// Exchanges asked after the warm-up, and those of them answered.
    asked: u64,
    answered: u64,
    p50_ns: u64,
    p99_ns: u64,
    worst_ns: u64,
    /// What the transfer delivered after the warm-up, of what the link
    /// could carry in that time.
    carried_ppm: u64,
    queue_drops: u64,
    lost: u64,
    datagrams: u64,
    /// The transfers that were under way at once at the end.
    streams: usize,
    /// Why the connection ended before the run did, and when.
    closed: Option<(u64, String)>,
    /// Datagrams a queue manager marked Congestion Experienced, in the
    /// whole run and by the end of the warm-up (slow start's).
    marked: u64,
    marked_at_warm: u64,
    /// How long the client's datagrams (its transfer's) waited in the
    /// bottleneck's queue after the warm-up: the median and the 99th
    /// percentile.
    queue_p50_ns: u64,
    queue_p99_ns: u64,
    /// The congestion events the client's law was told of: losses and
    /// marks alike.
    congestion_events: u64,
    /// Whether the client's last datagram was ECN-capable: quinn stops
    /// marking its own when the path fails its validation (RFC 9000
    /// §13.4.2).
    ecn: bool,
}

struct Pki {
    certificate: rustls::pki_types::CertificateDer<'static>,
    key: rustls::pki_types::PrivateKeyDer<'static>,
}
impl Pki {
    fn new() -> Self {
        let key = rcgen::KeyPair::generate().unwrap();
        let certificate = rcgen::CertificateParams::new(vec!["localhost".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        Self {
            certificate: certificate.der().clone(),
            key: rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        }
    }
}
fn transport(law: Law) -> Arc<TransportConfig> {
    // What two of focal's nodes set for each other
    // (`ReplicaHost::wire_limits`), with the law that is measured. The
    // transfer goes by a stream of its own direction, which focal's
    // connections do not open, and the exchanges by as many streams as are
    // under way at once.
    let limits = focal_wire::WireLimits {
        max_frame_bytes: 10 * 1024 * 1024,
        max_cost: 40 * 1024 * 1024,
        ..focal_wire::WireLimits::default()
    };
    let mut transport = focal_wire::quic_transport(&limits).unwrap();
    transport.max_concurrent_bidi_streams(VarInt::from_u32(4096));
    transport.max_concurrent_uni_streams(VarInt::from_u32(16));
    transport.congestion_controller_factory(law.factory());
    Arc::new(transport)
}

/// A datagram on the fabric, with the ECN codepoint its IP header carries.
struct Datagram {
    bytes: Vec<u8>,
    ecn: Option<EcnCodepoint>,
}
/// A datagram onto the fabric: ECN-capable where the path carries the
/// field and quinn sent it so, a queue manager marking it Congestion
/// Experienced (RFC 3168 §5); on a path that bleaches the field, as one
/// that does not carry it.
fn send_datagram(
    fabric: &mut Fabric<Datagram>,
    carries_ecn: bool,
    from: u64,
    to: u64,
    bytes: Vec<u8>,
    ecn: Option<EcnCodepoint>,
) -> Fate {
    let size = bytes.len();
    let ecn = if carries_ecn { ecn } else { None };
    let capable = matches!(ecn, Some(EcnCodepoint::Ect0 | EcnCodepoint::Ect1));
    let datagram = Datagram { bytes, ecn };
    if capable {
        fabric.send_ecn(from, to, datagram, size, |datagram| {
            datagram.ecn = Some(EcnCodepoint::Ce);
        })
    } else {
        fabric.send(from, to, datagram, size)
    }
}

struct Side {
    node: u64,
    address: SocketAddr,
    endpoint: Endpoint,
    connection: Option<(ConnectionHandle, Connection)>,
}
struct Exchange {
    asked_at: u64,
    received: usize,
}
/// One connection: its two ends, the law its client sends by, and what it
/// carries. Flow `f` is nodes `2f+1` (the client) and `2f+2`.
struct Flow {
    law: Law,
    client: Side,
    server: Side,
    server_config: Arc<ServerConfig>,
    connected: bool,
    transfers: Vec<StreamId>,
    transfer_received: u64,
    transfer_at_warm: u64,
    next_exchange: u64,
    asked: BTreeMap<StreamId, Exchange>,
    /// What the server has read of each exchange's question.
    questions: BTreeMap<StreamId, usize>,
    took: Vec<u64>,
    asked_after_warm: u64,
    datagrams: u64,
    closed: Option<(u64, String)>,
    /// How long each of the client's datagrams after the warm-up waits in
    /// the bottleneck's queue.
    queued: Vec<u64>,
    /// Whether the client's last datagram was ECN-capable.
    ecn: bool,
}
impl Flow {
    fn new(index: u64, law: Law, seed: u64, pki: &Pki) -> Self {
        let mut server_config =
            ServerConfig::with_single_cert(vec![pki.certificate.clone()], pki.key.clone_key())
                .unwrap();
        server_config.transport_config(transport(law));
        let server_config = Arc::new(server_config);
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&seed.to_le_bytes());
        let endpoint = |server: Option<Arc<ServerConfig>>, salt: u64| {
            let mut bytes = bytes;
            bytes[31] = u8::try_from(salt).unwrap();
            Endpoint::new(
                Arc::new(EndpointConfig::default()),
                server,
                false,
                Some(bytes),
            )
        };
        let (client, server) = (2 * index + 1, 2 * index + 2);
        Self {
            law,
            client: Side {
                node: client,
                address: format!("10.0.{index}.1:4433").parse().unwrap(),
                endpoint: endpoint(None, client),
                connection: None,
            },
            server: Side {
                node: server,
                address: format!("10.0.{index}.2:4433").parse().unwrap(),
                endpoint: endpoint(Some(server_config.clone()), server),
                connection: None,
            },
            server_config,
            connected: false,
            transfers: Vec::new(),
            transfer_received: 0,
            transfer_at_warm: 0,
            next_exchange: 0,
            asked: BTreeMap::new(),
            questions: BTreeMap::new(),
            took: Vec::new(),
            asked_after_warm: 0,
            datagrams: 0,
            closed: None,
            queued: Vec::new(),
            ecn: false,
        }
    }
    /// What the two ends do with what they were told.
    fn apply(&mut self, now: u64, scenario: &Scenario, warm: u64, end: u64) {
        if let Some((_, connection)) = &mut self.client.connection {
            while let Some(event) = connection.poll() {
                match event {
                    Event::Connected => self.connected = true,
                    Event::ConnectionLost { reason } => {
                        self.closed
                            .get_or_insert((now, format!("client: {reason}")));
                    }
                    Event::Stream(StreamEvent::Readable { id }) => {
                        let Some(exchange) = self.asked.get_mut(&id) else {
                            continue;
                        };
                        let mut stream = connection.recv_stream(id);
                        let Ok(mut chunks) = stream.read(true) else {
                            continue;
                        };
                        let mut ended = false;
                        loop {
                            match chunks.next(usize::MAX) {
                                Ok(Some(chunk)) => exchange.received += chunk.bytes.len(),
                                Ok(None) => {
                                    ended = true;
                                    break;
                                }
                                Err(_) => break,
                            }
                        }
                        let _ = chunks.finalize();
                        if ended {
                            assert_eq!(exchange.received, EXCHANGE_BYTES);
                            if exchange.asked_at >= warm {
                                self.took.push(now - exchange.asked_at);
                            }
                            self.asked.remove(&id);
                        }
                    }
                    _ => {}
                }
            }
            if self.connected {
                let width = if scenario.derived {
                    focal_wire::bulk_width(connection.stats().path.cwnd, scenario.transfers)
                } else {
                    scenario.transfers
                };
                while self.transfers.len() < width {
                    let id = connection.streams().open(Dir::Uni).unwrap();
                    if scenario.classes {
                        connection
                            .send_stream(id)
                            .set_priority(focal_wire::TrafficClass::Bulk.priority())
                            .unwrap();
                    }
                    self.transfers.push(id);
                }
                let chunk = [7u8; CHUNK];
                // As much as the connection takes: it is the law that
                // decides what leaves.
                for transfer in &self.transfers {
                    while connection.send_stream(*transfer).write(&chunk).is_ok() {}
                }
                while self.next_exchange <= now && self.next_exchange < end {
                    if let Some(id) = connection.streams().open(Dir::Bi) {
                        let mut stream = connection.send_stream(id);
                        if scenario.classes {
                            stream
                                .set_priority(focal_wire::TrafficClass::Control.priority())
                                .unwrap();
                        }
                        assert_eq!(
                            stream.write(&[1u8; EXCHANGE_BYTES]).unwrap(),
                            EXCHANGE_BYTES
                        );
                        stream.finish().unwrap();
                        self.asked.insert(
                            id,
                            Exchange {
                                asked_at: self.next_exchange,
                                received: 0,
                            },
                        );
                    }
                    if self.next_exchange >= warm {
                        self.asked_after_warm += 1;
                    }
                    self.next_exchange += EXCHANGE_EVERY;
                }
            }
        }
        if let Some((_, connection)) = &mut self.server.connection {
            let mut readable = Vec::new();
            while let Some(event) = connection.poll() {
                match event {
                    Event::ConnectionLost { reason } => {
                        self.closed
                            .get_or_insert((now, format!("server: {reason}")));
                    }
                    Event::Stream(StreamEvent::Opened { dir }) => {
                        while let Some(id) = connection.streams().accept(dir) {
                            readable.push(id);
                        }
                    }
                    Event::Stream(StreamEvent::Readable { id }) => readable.push(id),
                    _ => {}
                }
            }
            for id in readable {
                let mut stream = connection.recv_stream(id);
                let Ok(mut chunks) = stream.read(true) else {
                    continue;
                };
                let mut read = 0usize;
                let mut ended = false;
                loop {
                    match chunks.next(usize::MAX) {
                        Ok(Some(chunk)) => read += chunk.bytes.len(),
                        Ok(None) => {
                            ended = true;
                            break;
                        }
                        Err(_) => break,
                    }
                }
                let _ = chunks.finalize();
                if id.dir() == Dir::Uni {
                    self.transfer_received += read as u64;
                    continue;
                }
                let question = self.questions.entry(id).or_default();
                *question += read;
                if ended {
                    assert_eq!(*question, EXCHANGE_BYTES);
                    self.questions.remove(&id);
                    let mut answer = connection.send_stream(id);
                    if scenario.classes {
                        answer
                            .set_priority(focal_wire::TrafficClass::Control.priority())
                            .unwrap();
                    }
                    assert_eq!(
                        answer.write(&[2u8; EXCHANGE_BYTES]).unwrap(),
                        EXCHANGE_BYTES
                    );
                    answer.finish().unwrap();
                }
            }
        }
    }
}
struct Run {
    scenario: Scenario,
    began: Instant,
    fabric: Fabric<Datagram>,
    flows: Vec<Flow>,
    scratch: Vec<u8>,
    marked_at_warm: u64,
}
impl Run {
    fn warm(&self) -> u64 {
        self.scenario.seconds * SECOND / 4
    }
    fn end(&self) -> u64 {
        self.scenario.seconds * SECOND
    }
    fn at(&self, now: u64) -> Instant {
        self.began + Duration::from_nanos(now)
    }
    /// One flow for each law, all of them through the one bottleneck in
    /// each direction: the dumbbell of RFC 5166.
    fn new(scenario: Scenario, laws: &[Law], pki: &Pki) -> Self {
        let mut fabric = Fabric::new(scenario.seed, 1 << 20, 1 << 30);
        let queue = (scenario.bdp_bytes() * scenario.queue_bdps).max(4 * DATAGRAM as u64);
        let loss = if scenario.bursts {
            // Bursts of five messages on average that lose half of what
            // they hold, entered so often that the whole loses what is
            // stated.
            Loss::bursty(scenario.loss_ppm * 2 / 5, 200_000, 500_000)
        } else {
            Loss::random(scenario.loss_ppm)
        };
        let link = Link {
            marking: scenario.marking,
            ..Link::drop_tail(scenario.rate_bits_per_second, queue)
        };
        let up = fabric.add_link(link).unwrap();
        let down = fabric.add_link(link).unwrap();
        let flows: Vec<Flow> = laws
            .iter()
            .enumerate()
            .map(|(index, law)| Flow::new(index as u64, *law, scenario.seed, pki))
            .collect();
        for flow in &flows {
            for (from, to, link) in [
                (flow.client.node, flow.server.node, up),
                (flow.server.node, flow.client.node, down),
            ] {
                fabric
                    .set_pair_path(
                        from,
                        to,
                        Path::in_order(scenario.rtt_ns / 2, 0)
                            .with_loss(loss)
                            .through(link),
                    )
                    .unwrap();
            }
        }
        Self {
            scenario,
            began: Instant::now(),
            fabric,
            flows,
            scratch: Vec::with_capacity(2 * DATAGRAM),
            marked_at_warm: 0,
        }
    }
    fn connect(&mut self, pki: &Pki) {
        let now = self.at(0);
        for flow in &mut self.flows {
            let mut roots = rustls::RootCertStore::empty();
            roots.add(pki.certificate.clone()).unwrap();
            let mut config = ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
            config.transport_config(transport(flow.law));
            let connection = flow
                .client
                .endpoint
                .connect(now, config, flow.server.address, "localhost")
                .unwrap();
            flow.client.connection = Some(connection);
        }
    }
    /// What arrived by `now` is handed to whom it is for.
    fn deliver(&mut self, now: u64) {
        let at = self.at(now);
        let carries_ecn = self.scenario.ecn;
        while let Some(delivery) = self.fabric.receive() {
            let flow = &mut self.flows[usize::try_from((delivery.to - 1) / 2).unwrap()];
            let (side, from) = if delivery.to % 2 == 0 {
                (&mut flow.server, flow.client.address)
            } else {
                (&mut flow.client, flow.server.address)
            };
            let ecn = if carries_ecn {
                delivery.message.ecn
            } else {
                None
            };
            self.scratch.clear();
            let event = side.endpoint.handle(
                at,
                from,
                None,
                ecn,
                BytesMut::from(delivery.message.bytes.as_slice()),
                &mut self.scratch,
            );
            match event {
                Some(DatagramEvent::ConnectionEvent(handle, event)) => {
                    if let Some((own, connection)) = &mut side.connection
                        && *own == handle
                    {
                        connection.handle_event(event);
                    }
                }
                Some(DatagramEvent::NewConnection(incoming)) => {
                    self.scratch.clear();
                    let accepted = side
                        .endpoint
                        .accept(
                            incoming,
                            at,
                            &mut self.scratch,
                            Some(flow.server_config.clone()),
                        )
                        .unwrap_or_else(|error| panic!("the server refused: {:?}", error.cause));
                    side.connection = Some(accepted);
                }
                Some(DatagramEvent::Response(transmit)) => {
                    let (node, peer) = (side.node, delivery.from);
                    let bytes = self.scratch[..transmit.size].to_vec();
                    let _ = send_datagram(
                        &mut self.fabric,
                        carries_ecn,
                        node,
                        peer,
                        bytes,
                        transmit.ecn,
                    );
                }
                None => {}
            }
        }
    }
    /// What the ends of every flow do with what they were told.
    fn apply(&mut self, now: u64) {
        let (warm, end) = (self.warm(), self.end());
        let scenario = self.scenario;
        for flow in &mut self.flows {
            flow.apply(now, &scenario, warm, end);
        }
    }
    /// Timers that are due fire, and what each end has to send leaves.
    /// Whether anything did.
    fn transmit(&mut self, now: u64) -> bool {
        let at = self.at(now);
        let warm = self.warm();
        let scenario = self.scenario;
        let mut sent = false;
        for flow in &mut self.flows {
            let client = flow.client.node;
            for side in [&mut flow.client, &mut flow.server] {
                let peer = if side.node == client {
                    client + 1
                } else {
                    client
                };
                let Some((handle, connection)) = &mut side.connection else {
                    continue;
                };
                if connection.poll_timeout().is_some_and(|due| due <= at) {
                    connection.handle_timeout(at);
                }
                while let Some(event) = connection.poll_endpoint_events() {
                    if let Some(event) = side.endpoint.handle_event(*handle, event) {
                        connection.handle_event(event);
                    }
                }
                loop {
                    self.scratch.clear();
                    let Some(transmit) = connection.poll_transmit(at, 1, &mut self.scratch) else {
                        break;
                    };
                    assert!(transmit.segment_size.is_none());
                    let bytes = self.scratch[..transmit.size].to_vec();
                    let size = bytes.len();
                    flow.datagrams += 1;
                    sent = true;
                    let fate = send_datagram(
                        &mut self.fabric,
                        scenario.ecn,
                        side.node,
                        peer,
                        bytes,
                        transmit.ecn,
                    );
                    if side.node != client {
                        continue;
                    }
                    flow.ecn = scenario.ecn && transmit.ecn.is_some();
                    // The wait in the queue: what is left of the arrival
                    // after the propagation and the datagram's own time on
                    // the link.
                    if let Fate::Arrives { at: arrives } = fate
                        && now >= warm
                    {
                        let serialization = (size as u64 * 8 * SECOND)
                            .div_ceil(scenario.rate_bits_per_second.max(1));
                        flow.queued.push(
                            arrives
                                .saturating_sub(now)
                                .saturating_sub(scenario.rtt_ns / 2)
                                .saturating_sub(serialization),
                        );
                    }
                }
            }
        }
        sent
    }
    fn next(&mut self, now: u64) -> u64 {
        let mut next = self.end();
        if let Some(arrival) = self.fabric.next_arrival() {
            next = next.min(arrival);
        }
        let began = self.began;
        for flow in &mut self.flows {
            if flow.connected {
                next = next.min(flow.next_exchange.max(now));
            }
            for side in [&mut flow.client, &mut flow.server] {
                if let Some((_, connection)) = &mut side.connection
                    && let Some(due) = connection.poll_timeout()
                {
                    let due =
                        u64::try_from(due.saturating_duration_since(began).as_nanos()).unwrap();
                    next = next.min(due);
                }
            }
        }
        next
    }
    fn closed(&self) -> bool {
        self.flows.iter().any(|flow| flow.closed.is_some())
    }
    fn run(mut self, pki: &Pki) -> Vec<Measured> {
        self.connect(pki);
        let mut now = 0u64;
        let mut warmed = false;
        // Every pass either sends, or moves the clock: a run ends.
        let mut passes = 0u64;
        let bound = self.scenario.seconds
            * (self.scenario.rate_bits_per_second / 8 / DATAGRAM as u64 + 1_000)
            * 64
            * self.flows.len() as u64;
        let laws: Vec<Law> = self.flows.iter().map(|flow| flow.law).collect();
        while now < self.end() && !self.closed() {
            passes += 1;
            assert!(
                passes < bound,
                "{laws:?} in {}: the run spins",
                self.scenario.name()
            );
            self.deliver(now);
            self.apply(now);
            let sent = self.transmit(now);
            if !warmed && now >= self.warm() {
                warmed = true;
                for flow in &mut self.flows {
                    flow.transfer_at_warm = flow.transfer_received;
                }
                self.marked_at_warm = self.fabric.stats().marked;
            }
            if sent {
                // What was sent may be due at once, and may let more leave.
                if self.fabric.next_arrival().is_some_and(|due| due <= now) {
                    continue;
                }
            }
            let next = self.next(now);
            if next <= now {
                // A timer due now: the next pass fires it.
                if !sent {
                    now += 1;
                    self.fabric.advance_to(now).unwrap();
                }
                continue;
            }
            now = next;
            self.fabric.advance_to(now).unwrap();
        }
        let stats = self.fabric.stats();
        let measured_ns = self.end() - self.warm();
        let could = u128::from(self.scenario.rate_bits_per_second) / 8 * u128::from(measured_ns)
            / 1_000_000_000;
        let marked_at_warm = self.marked_at_warm;
        self.flows
            .iter_mut()
            .map(|flow| {
                let mut took = std::mem::take(&mut flow.took);
                took.sort_unstable();
                let mut queued = std::mem::take(&mut flow.queued);
                queued.sort_unstable();
                let at = |values: &[u64], per_cent: usize| {
                    if values.is_empty() {
                        return 0;
                    }
                    values[((values.len() * per_cent).div_ceil(100)).clamp(1, values.len()) - 1]
                };
                let carried = u128::from(flow.transfer_received - flow.transfer_at_warm);
                let congestion_events = flow
                    .client
                    .connection
                    .as_ref()
                    .map_or(0, |(_, connection)| {
                        connection.stats().path.congestion_events
                    });
                Measured {
                    // Exchanges asked within the last second may be on
                    // their way.
                    asked: flow.asked_after_warm,
                    answered: took.len() as u64,
                    p50_ns: at(&took, 50),
                    p99_ns: at(&took, 99),
                    worst_ns: took.last().copied().unwrap_or(0),
                    carried_ppm: u64::try_from(carried * 1_000_000 / could.max(1)).unwrap(),
                    queue_drops: stats.dropped_queue,
                    lost: stats.dropped_loss,
                    datagrams: flow.datagrams,
                    streams: flow.transfers.len(),
                    closed: flow.closed.clone(),
                    marked: stats.marked,
                    marked_at_warm,
                    queue_p50_ns: at(&queued, 50),
                    queue_p99_ns: at(&queued, 99),
                    congestion_events,
                    ecn: flow.ecn,
                }
            })
            .collect()
    }
}

fn measure(scenario: Scenario, law: Law, pki: &Pki) -> Measured {
    Run::new(scenario, &[law], pki).run(pki).remove(0)
}
/// Flows of these laws at once through the scenario's bottleneck.
fn compete(scenario: Scenario, laws: &[Law], pki: &Pki) -> Vec<Measured> {
    Run::new(scenario, laws, pki).run(pki)
}
/// Jain's index of what each flow carried (Jain, Chiu and Hawe, 1984):
/// one when they share alike, `1/n` when one takes everything.
fn fairness(measured: &[Measured]) -> f64 {
    let sum: f64 = measured.iter().map(|m| m.carried_ppm as f64).sum();
    let squares: f64 = measured
        .iter()
        .map(|m| (m.carried_ppm as f64).powi(2))
        .sum();
    if squares == 0.0 {
        return 0.0;
    }
    sum * sum / (measured.len() as f64 * squares)
}

fn geomean(values: impl Iterator<Item = f64>) -> f64 {
    let (sum, count) = values.fold((0.0, 0u32), |(sum, count), value| {
        (sum + value.max(f64::MIN_POSITIVE).ln(), count + 1)
    });
    if count == 0 {
        return 1.0;
    }
    (sum / f64::from(count)).exp()
}

struct Verdict {
    stalled: BTreeMap<Law, Vec<String>>,
    /// The 99th percentile as a multiple of the best, and what the
    /// transfer carried as a share of the best, by geometric mean.
    delay: BTreeMap<Law, f64>,
    carried: BTreeMap<Law, f64>,
    chosen: Law,
}
fn judge(results: &[(Scenario, BTreeMap<Law, Measured>)]) -> Verdict {
    let mut stalled: BTreeMap<Law, Vec<String>> = BTreeMap::new();
    let mut delay: BTreeMap<Law, Vec<f64>> = BTreeMap::new();
    let mut carried: BTreeMap<Law, Vec<f64>> = BTreeMap::new();
    for (scenario, by_law) in results {
        let most = by_law.values().map(|m| m.carried_ppm).max().unwrap().max(1);
        let answered: Vec<u64> = by_law
            .values()
            .filter(|m| m.answered * 10 >= m.asked * 9 && m.answered > 0)
            .map(|m| m.p99_ns)
            .collect();
        let best = answered.iter().copied().min().unwrap_or(1).max(1);
        for (law, measured) in by_law {
            let unanswered =
                measured.answered * 10 < measured.asked * 9 || measured.closed.is_some();
            if measured.carried_ppm * 10 < most || unanswered {
                stalled.entry(*law).or_default().push(scenario.name());
            }
            carried
                .entry(*law)
                .or_default()
                .push(measured.carried_ppm.max(1) as f64 / most as f64);
            // What was not answered took no time that could be compared.
            if !unanswered {
                delay
                    .entry(*law)
                    .or_default()
                    .push(measured.p99_ns.max(1) as f64 / best as f64);
            }
        }
    }
    let delay: BTreeMap<Law, f64> = Law::ALL
        .into_iter()
        .map(|law| {
            let values = delay.remove(&law).unwrap_or_default();
            (law, geomean(values.into_iter()))
        })
        .collect();
    let carried: BTreeMap<Law, f64> = carried
        .into_iter()
        .map(|(law, values)| (law, geomean(values.into_iter())))
        .collect();
    let preferred = Law::ALL
        .into_iter()
        .filter(|law| !stalled.contains_key(law))
        .min_by(|left, right| delay[left].total_cmp(&delay[right]));
    let chosen = match preferred {
        Some(law)
            if law != Law::Cubic
                && (stalled.contains_key(&Law::Cubic)
                    || (delay[&law] * 1.1 <= delay[&Law::Cubic]
                        && carried[&law] >= carried[&Law::Cubic] * 0.9)) =>
        {
            law
        }
        _ => Law::Cubic,
    };
    Verdict {
        stalled,
        delay,
        carried,
        chosen,
    }
}

fn report(results: &[(Scenario, BTreeMap<Law, Measured>)], verdict: &Verdict) {
    println!(
        "| Scenario | Law | p50 ms | p99 ms | worst ms | answered | carried | queue drops | lost |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for (scenario, by_law) in results {
        for (law, m) in by_law {
            println!(
                "| {} | {law:?} | {:.1} | {:.1} | {:.1} | {}/{} | {:.1}% | {} | {} |",
                scenario.name(),
                m.p50_ns as f64 / MS as f64,
                m.p99_ns as f64 / MS as f64,
                m.worst_ns as f64 / MS as f64,
                m.answered,
                m.asked,
                m.carried_ppm as f64 / 10_000.0,
                m.queue_drops,
                m.lost
            );
            if let Some((at, why)) = &m.closed {
                println!(
                    "closed: {law:?} in {} after {} ms: {why}",
                    scenario.name(),
                    at / MS
                );
            }
        }
    }
    println!("| Law | p99 of the best (geomean) | carried of the best (geomean) | stalled in |");
    println!("|---|---|---|---|");
    for law in Law::ALL {
        println!(
            "| {law:?} | {:.3} | {:.3} | {} |",
            verdict.delay[&law],
            verdict.carried[&law],
            verdict
                .stalled
                .get(&law)
                .map_or("nowhere".to_owned(), |names| names.join("; "))
        );
    }
    println!("chosen: {:?}", verdict.chosen);
}

fn all(scenarios: &[Scenario]) -> Vec<(Scenario, BTreeMap<Law, Measured>)> {
    let pki = Pki::new();
    scenarios
        .iter()
        .map(|scenario| {
            (
                *scenario,
                Law::ALL
                    .into_iter()
                    .map(|law| (law, measure(*scenario, law, &pki)))
                    .collect(),
            )
        })
        .collect()
}

fn scenario(rate: u64, rtt_ms: u64, loss_ppm: u32, seconds: u64) -> Scenario {
    Scenario {
        rate_bits_per_second: rate,
        rtt_ns: rtt_ms * MS,
        loss_ppm,
        queue_bdps: 1,
        bursts: false,
        seconds,
        seed: 0xF0CA1,
        transfers: 1,
        classes: false,
        derived: false,
        ecn: false,
        marking: Marking::Off,
    }
}

/// Every law moves the transfer and answers the exchanges on a clean path,
/// none is answered faster than the path allows, and a run is its seed.
#[test]
fn every_law_carries_a_transfer_and_answers_beside_it() {
    let pki = Pki::new();
    let path = scenario(10_000_000, 20, 0, 8);
    for law in Law::ALL {
        let measured = measure(path, law, &pki);
        println!("{law:?}: {measured:?}");
        assert!(measured.asked >= 100, "{law:?}: {measured:?}");
        assert!(
            measured.answered * 10 >= measured.asked * 9,
            "{law:?}: {measured:?}"
        );
        assert!(measured.p50_ns >= 20 * MS, "{law:?}: {measured:?}");
        assert!(measured.p99_ns < 2 * SECOND, "{law:?}: {measured:?}");
        assert!(
            (300_000..=1_000_000).contains(&measured.carried_ppm),
            "{law:?}: {measured:?}"
        );
        assert_eq!(measured.lost, 0);
        assert_eq!(measured.closed, None);
        let again = measure(path, law, &pki);
        assert_eq!(
            (
                again.answered,
                again.p99_ns,
                again.carried_ppm,
                again.datagrams
            ),
            (
                measured.answered,
                measured.p99_ns,
                measured.carried_ppm,
                measured.datagrams
            ),
            "{law:?}: a run is not its seed"
        );
    }
}

/// Copa keeps the queue short: beside a transfer that fills the link its
/// exchanges take little more than the path, where a law that fills the
/// queue makes them wait for it.
#[test]
fn copa_answers_within_the_path_and_a_short_queue() {
    let pki = Pki::new();
    let path = scenario(10_000_000, 100, 0, 12);
    let copa = measure(path, Law::Copa, &pki);
    let reno = measure(path, Law::NewReno, &pki);
    println!("copa {copa:?}\nreno {reno:?}");
    assert!(copa.answered * 10 >= copa.asked * 9);
    assert!(copa.p50_ns < 150 * MS, "{copa:?}");
    assert!(copa.carried_ppm >= 500_000, "{copa:?}");
    assert!(
        copa.queue_drops <= reno.queue_drops.max(1) * 2,
        "{copa:?} {reno:?}"
    );
}

/// The rule, on results that are made up: a law that stalls is not chosen
/// however fast it answers, and the default stands unless another is
/// better by what the rule asks.
#[test]
fn the_rule_chooses_as_it_says() {
    let one = scenario(1_000_000, 20, 0, 1);
    let made = |p99: u64, carried: u64, answered: u64| Measured {
        asked: 100,
        answered,
        p99_ns: p99 * MS,
        carried_ppm: carried,
        ..Measured::default()
    };
    let results = |rows: [(Law, Measured); 4]| vec![(one, rows.into_iter().collect())];
    // Better by a tenth and carrying as much: chosen.
    let verdict = judge(&results([
        (Law::NewReno, made(90, 900_000, 100)),
        (Law::Cubic, made(100, 900_000, 100)),
        (Law::Bbr, made(95, 900_000, 100)),
        (Law::Copa, made(40, 850_000, 100)),
    ]));
    assert_eq!(verdict.chosen, Law::Copa);
    assert!(verdict.stalled.is_empty());
    // Better by less than a tenth: the default stands.
    let verdict = judge(&results([
        (Law::NewReno, made(99, 900_000, 100)),
        (Law::Cubic, made(100, 900_000, 100)),
        (Law::Bbr, made(98, 900_000, 100)),
        (Law::Copa, made(95, 900_000, 100)),
    ]));
    assert_eq!(verdict.chosen, Law::Cubic);
    // Faster, and carrying too little: the default stands.
    let verdict = judge(&results([
        (Law::NewReno, made(100, 900_000, 100)),
        (Law::Cubic, made(100, 900_000, 100)),
        (Law::Bbr, made(100, 900_000, 100)),
        (Law::Copa, made(40, 500_000, 100)),
    ]));
    assert_eq!(verdict.chosen, Law::Cubic);
    // The fastest stalls, by what it carries or by what it answers.
    for stalls in [made(10, 50_000, 100), made(10, 900_000, 80)] {
        let verdict = judge(&results([
            (Law::NewReno, made(100, 900_000, 100)),
            (Law::Cubic, made(100, 900_000, 100)),
            (Law::Bbr, stalls),
            (Law::Copa, made(50, 900_000, 100)),
        ]));
        assert_eq!(verdict.stalled.keys().collect::<Vec<_>>(), [&Law::Bbr]);
        assert_eq!(verdict.chosen, Law::Copa);
    }
    // The default stalls: what is preferred of the rest is chosen.
    let verdict = judge(&results([
        (Law::NewReno, made(100, 900_000, 100)),
        (Law::Cubic, made(100, 50_000, 100)),
        (Law::Bbr, made(99, 900_000, 100)),
        (Law::Copa, made(98, 900_000, 100)),
    ]));
    assert_eq!(verdict.chosen, Law::Copa);
}

/// The grid the decision is recorded from, or a part of it that keeps the
/// harness honest where every change is checked.
#[test]
fn the_laws_are_measured_against_each_other() {
    let full = std::env::var_os("FOCAL_CONGESTION_FULL").is_some();
    let mut scenarios = Vec::new();
    if full {
        for rate in [1_000_000, 10_000_000, 100_000_000] {
            for rtt in [20, 100, 300] {
                for loss in [0, 1_000, 10_000] {
                    scenarios.push(scenario(rate, rtt, loss, 30));
                }
            }
        }
        scenarios.push(Scenario {
            queue_bdps: 4,
            ..scenario(10_000_000, 100, 0, 30)
        });
        scenarios.push(Scenario {
            bursts: true,
            ..scenario(10_000_000, 100, 10_000, 30)
        });
        scenarios.push(scenario(100_000_000, 1, 0, 30));
    } else {
        scenarios.push(scenario(1_000_000, 100, 0, 8));
        scenarios.push(scenario(10_000_000, 20, 10_000, 6));
    }
    let results = all(&scenarios);
    let verdict = judge(&results);
    report(&results, &verdict);
    for (scenario, by_law) in &results {
        for (law, measured) in by_law {
            assert_eq!(
                measured.closed,
                None,
                "{law:?} in {}: the connection closed",
                scenario.name()
            );
            assert!(measured.asked > 0, "{law:?} in {}", scenario.name());
            assert!(
                measured.p99_ns == 0 || measured.p50_ns >= scenario.rtt_ns,
                "{law:?} in {}: answered faster than the path: {measured:?}",
                scenario.name()
            );
            assert!(measured.carried_ppm <= 1_000_000, "{measured:?}");
        }
    }
}

/// One datagram lost on a path that carries thousands in a round trip
/// leaves the receiver thousands of frames it cannot hand on until the one
/// is sent again. quinn-proto 0.11.17 counted them as a peer that sends
/// fragments to exhaust it and closed the connection (`too many gaps in
/// stream buffer`): at 100 Mbit/s and 100 ms every law lost its connection
/// within a second and a half. 0.11.18 merges them.
#[test]
fn a_loss_on_a_fast_long_path_does_not_close_the_connection() {
    let pki = Pki::new();
    for law in [Law::Cubic, Law::Copa] {
        let measured = measure(scenario(100_000_000, 100, 1_000, 4), law, &pki);
        println!("{law:?}: {measured:?}");
        assert_eq!(measured.closed, None, "{law:?}");
        assert!(measured.lost > 0, "{law:?}: nothing was lost: {measured:?}");
        assert!(
            measured.answered * 10 >= measured.asked * 9,
            "{law:?}: {measured:?}"
        );
    }
}

/// What is asked beside several transfers goes before them
/// (`focal_wire::TrafficClass`): it waits for the path and for what is
/// already on it, and no longer for a turn among the transfers. The
/// transfers carry what they carried.
#[test]
fn what_is_asked_goes_before_what_is_transferred() {
    let pki = Pki::new();
    println!("| Path | Transfers | Classes | p50 ms | p99 ms | worst ms | carried |");
    println!("|---|---|---|---|---|---|---|");
    for (rate, rtt) in [(1_000_000, 100), (10_000_000, 20), (100_000_000, 1)] {
        let mut measured = Vec::new();
        for (transfers, classes) in [(1, false), (8, false), (8, true)] {
            let path = Scenario {
                transfers,
                classes,
                ..scenario(rate, rtt, 0, 10)
            };
            let m = measure(path, Law::Copa, &pki);
            assert_eq!(m.closed, None);
            println!(
                "| {} | {transfers} | {classes} | {:.1} | {:.1} | {:.1} | {:.1}% |",
                path.name(),
                m.p50_ns as f64 / MS as f64,
                m.p99_ns as f64 / MS as f64,
                m.worst_ns as f64 / MS as f64,
                m.carried_ppm as f64 / 10_000.0
            );
            measured.push(m);
        }
        let [one, turns, classes] = measured.as_slice() else {
            unreachable!()
        };
        assert!(classes.p99_ns < turns.p99_ns, "{classes:?} {turns:?}");
        assert!(classes.p50_ns < turns.p50_ns, "{classes:?} {turns:?}");
        // Beside eight transfers as beside one.
        assert!(
            classes.p99_ns <= one.p99_ns * 11 / 10,
            "{classes:?} {one:?}"
        );
        assert!(
            classes.carried_ppm * 100 >= turns.carried_ppm * 98,
            "{classes:?} {turns:?}"
        );
    }
}

/// One stream is given a megabyte ahead of its reader
/// (`STREAM_WINDOW_CEILING`), so it carries a megabyte in a round trip at
/// most. A transfer that goes by several streams carries what the path
/// carries, and what is asked beside it goes before it as before.
#[test]
fn a_transfer_by_several_streams_carries_what_the_path_carries() {
    let pki = Pki::new();
    println!("| Path | Streams | carried | p50 ms | p99 ms |");
    println!("|---|---|---|---|---|");
    for (rate, rtt) in [
        (1_000_000, 100),
        (10_000_000, 100),
        (100_000_000, 1),
        (100_000_000, 100),
        (100_000_000, 300),
        (1_000_000_000, 100),
    ] {
        let mut carried = Vec::new();
        let megabytes = usize::try_from(scenario(rate, rtt, 0, 20).bdp_bytes() >> 20).unwrap();
        // The lane of content on a connection of sixteen streams
        // (`PeerConnectionPool::bulk_lane`).
        const LANE: usize = 16 - 2 - 1;
        for (transfers, derived) in [(1, false), (4, false), (8, false), (LANE, true)] {
            let path = Scenario {
                transfers,
                classes: true,
                derived,
                ..scenario(rate, rtt, 0, 20)
            };
            let m = measure(path, Law::Copa, &pki);
            assert_eq!(m.closed, None, "{}", path.name());
            println!(
                "| {} | {}{} | {:.1}% | {:.1} | {:.1} |",
                path.name(),
                m.streams,
                if derived { " derived" } else { "" },
                m.carried_ppm as f64 / 10_000.0,
                m.p50_ns as f64 / MS as f64,
                m.p99_ns as f64 / MS as f64
            );
            carried.push((m.carried_ppm, m.streams));
        }
        let [(one, _), _, (eight, _), (derived, streams)] = carried[..] else {
            panic!("{carried:?}")
        };
        // Eight streams carry no less than one, and on a path that holds
        // more than a megabyte in flight they carry more.
        assert!(eight * 100 >= one * 95, "{carried:?}");
        if megabytes >= 1 {
            assert!(eight * 100 >= one * 115, "{carried:?}");
        }
        // As many streams as the window holds megabytes, and one, carry
        // what the most streams carry, and are no more than the path
        // holds megabytes and two: one where it holds less than one.
        assert!(derived * 100 >= eight * 97, "{carried:?}");
        // As many streams as the window holds megabytes, and one: no
        // more than the path holds and two, and more than the window
        // its transfer carried holds.
        assert!(streams <= (megabytes + 2).min(LANE), "{carried:?}");
        let held = megabytes * usize::try_from(derived).unwrap() / 1_000_000;
        assert!(streams > held.min(LANE - 1), "{carried:?}");
    }
}

/// Copa by what part of its window a round trip may move it by, over
/// paths that carry a transfer by eight streams. **The rule, fixed before
/// the run:** of the strides that carry, by geometric mean, ninety-nine
/// hundredths of what the best carries, the one whose exchanges' 99th
/// percentile has the least geometric mean is the law's.
#[test]
fn the_stride_of_the_law_is_the_one_that_was_measured() {
    let pki = Pki::new();
    let full = std::env::var_os("FOCAL_CONGESTION_FULL").is_some();
    let mut scenarios = Vec::new();
    let (rates, rtts, losses, seconds): (&[u64], &[u64], &[u32], u64) = if full {
        (
            &[1_000_000, 10_000_000, 100_000_000],
            &[20, 100, 300],
            &[0, 1_000, 10_000],
            30,
        )
    } else {
        (&[10_000_000], &[20, 100], &[0], 8)
    };
    for rate in rates {
        for rtt in rtts {
            for loss in losses {
                scenarios.push(Scenario {
                    transfers: 8,
                    classes: true,
                    ..scenario(*rate, *rtt, *loss, seconds)
                });
            }
        }
    }
    if full {
        scenarios.push(Scenario {
            transfers: 8,
            classes: true,
            ..scenario(100_000_000, 1, 0, 30)
        });
    }
    let strides = [1u64, 2, 4, 8, 16];
    let mut delay = vec![Vec::new(); strides.len()];
    let mut carried = vec![Vec::new(); strides.len()];
    for path in &scenarios {
        let measured: Vec<Measured> = strides
            .iter()
            .map(|stride| measure(*path, Law::Stride(*stride), &pki))
            .collect();
        let best = measured.iter().map(|m| m.p99_ns).min().unwrap().max(1);
        let most = measured.iter().map(|m| m.carried_ppm).max().unwrap().max(1);
        let mut line = format!("| {} |", path.name());
        for (at, m) in measured.iter().enumerate() {
            assert_eq!(m.closed, None);
            delay[at].push(m.p99_ns.max(1) as f64 / best as f64);
            carried[at].push(m.carried_ppm.max(1) as f64 / most as f64);
            line += &format!(
                " {:.0} / {:.1}% / {} |",
                m.p99_ns as f64 / MS as f64,
                m.carried_ppm as f64 / 10_000.0,
                m.queue_drops
            );
        }
        println!("{line}");
    }
    let judged: Vec<(u64, f64, f64)> = strides
        .iter()
        .enumerate()
        .map(|(at, stride)| {
            (
                *stride,
                geomean(delay[at].iter().copied()),
                geomean(carried[at].iter().copied()),
            )
        })
        .collect();
    for (stride, delay, carried) in &judged {
        println!("stride {stride}: p99 {delay:.3} carried {carried:.3}");
    }
    if full {
        let most = judged
            .iter()
            .map(|(_, _, carried)| *carried)
            .fold(0.0, f64::max);
        let chosen = judged
            .iter()
            .filter(|(_, _, carried)| *carried >= most * 0.99)
            .min_by(|left, right| left.1.total_cmp(&right.1))
            .unwrap()
            .0;
        assert_eq!(chosen, focal_wire::congestion::DEFAULT_STRIDE);
    }
}

/// The queue managers the marked scenarios run: a step at one datagram (the
/// low threshold DCTCP's switches mark at, RFC 8257 §3.1) and CoDel's own
/// defaults (RFC 8289 §4.3), both below the queue of `1/δ` datagrams Copa
/// keeps on its own on the paths measured.
fn managers() -> [Marking; 2] {
    [
        Marking::Step {
            threshold_bytes: DATAGRAM as u64,
        },
        Marking::CoDel {
            target_ns: 5 * MS,
            interval_ns: 100 * MS,
        },
    ]
}

/// Whether a queue manager must mark a law that keeps, without it, the
/// queue measured in `unmanaged`: a step marks whatever finds more than its
/// threshold ahead, so a 99th percentile above the threshold (its bytes at
/// the link's rate) is marked; CoDel marks only a sojourn that has stood
/// above its target for an interval (RFC 8289 §5), so a queue whose median
/// stands above the target is.
fn must_mark(marking: Marking, rate_bits_per_second: u64, unmanaged: &Measured) -> bool {
    match marking {
        Marking::Off => false,
        Marking::Step { threshold_bytes } => {
            unmanaged.queue_p99_ns > threshold_bytes * 8 * SECOND / rate_bits_per_second.max(1)
        }
        Marking::CoDel { target_ns, .. } => unmanaged.queue_p50_ns > target_ns,
    }
}

/// The least an incumbent law's flow should carry beside a newcomer through
/// the scenario's bottleneck: what it carries beside the deployed standard
/// that harms it most — a flow of its own kind, or CUBIC (RFC 9438, quinn's
/// and most hosts' default) — the worse-off of each pair. A newcomer may
/// harm an incumbent no more than the incumbent harms itself (Ware,
/// Mukerjee, Seshan and Sherry, "Beyond Jain's Fairness Index: Setting the
/// Bar for the Deployment of Congestion Control Algorithms", HotNets 2019),
/// and the IETF admits a sender no more aggressive than CUBIC is (RFC 8511
/// §5). The worse-off of one run's pair stands for the incumbent's
/// distribution.
fn harm_bar(path: Scenario, incumbent: Law, pki: &Pki) -> u64 {
    let own = compete(path, &[incumbent, incumbent], pki)
        .iter()
        .map(|m| m.carried_ppm)
        .min()
        .unwrap();
    if incumbent == Law::Cubic {
        return own;
    }
    own.min(compete(path, &[Law::Cubic, incumbent], pki)[1].carried_ppm)
}

/// What RFC 9002's sender, NewReno, carries alone under one queue manager:
/// the standard Copa's answer to a mark is held to.
fn standard_under(path: Scenario, pki: &Pki) -> Measured {
    measure(path, Law::NewReno, pki)
}

/// Copa alone answers a queue manager's marks (the audit's F39). **The
/// rule:** on a path that carries ECN, quinn's validation keeps it
/// ECN-capable; wherever the queue Copa keeps without the manager stands
/// where the manager acts ([`must_mark`]), it marks (the scenario is not
/// vacuous); under the manager Copa drops no more than without it, nine in
/// ten of its exchanges are answered, and its queue's 99th percentile is
/// ten ninths at most of its 99th percentile without the manager (the
/// harness's tolerance for "about as much"): a manager never lengthens the
/// queue Copa keeps; and over the scenarios, by geometric mean as the
/// harness judges its other choices, Copa's queue's 99th percentile under
/// the manager is no longer than without it and it carries no less than
/// NewReno, RFC 9002's sender, carries under the same manager. A first
/// statement judged each scenario's queue against its own without the
/// manager exactly and fell to one run's noise; a second judged the queue
/// by geometric mean alone, under which Copa alone at 100 Mbit/s, 20 ms
/// kept CoDel's queue at its target, 5.34 ms where it keeps 0.73 ms without
/// a manager (2026-10-03).
#[test]
fn copa_takes_a_mark_for_a_queue_longer_than_its_manager_wants() {
    let pki = Pki::new();
    let full = std::env::var_os("FOCAL_CONGESTION_FULL").is_some();
    let (paths, seconds): (&[(u64, u64)], u64) = if full {
        (
            &[
                (1_000_000, 20),
                (1_000_000, 100),
                (10_000_000, 20),
                (10_000_000, 100),
                (100_000_000, 20),
                (100_000_000, 100),
            ],
            30,
        )
    } else {
        (&[(10_000_000, 20), (10_000_000, 100)], 8)
    };
    println!(
        "| Path | marks (by warm-up) | events | queue p50 / p99 ms (unmanaged p99) | carried | NewReno carried | p99 ms | drops |"
    );
    println!("|---|---|---|---|---|---|---|---|");
    let mut queue = Vec::new();
    let mut carried = Vec::new();
    for (rate, rtt) in paths {
        let plain = Scenario {
            ecn: true,
            ..scenario(*rate, *rtt, 0, seconds)
        };
        let unmarked = measure(plain, Law::Copa, &pki);
        assert!(unmarked.ecn, "{}: {unmarked:?}", plain.name());
        for marking in managers() {
            let path = Scenario { marking, ..plain };
            let m = measure(path, Law::Copa, &pki);
            let reno = standard_under(path, &pki);
            println!(
                "| {} | {} ({}) | {} | {:.2} / {:.2} ({:.2}) | {:.1}% | {:.1}% | {:.1} | {} |",
                path.name(),
                m.marked,
                m.marked_at_warm,
                m.congestion_events,
                m.queue_p50_ns as f64 / MS as f64,
                m.queue_p99_ns as f64 / MS as f64,
                unmarked.queue_p99_ns as f64 / MS as f64,
                m.carried_ppm as f64 / 10_000.0,
                reno.carried_ppm as f64 / 10_000.0,
                m.p99_ns as f64 / MS as f64,
                m.queue_drops
            );
            assert_eq!(m.closed, None, "{}", path.name());
            assert!(m.ecn, "{}: quinn stopped sending ECN: {m:?}", path.name());
            if must_mark(marking, *rate, &unmarked) {
                assert!(m.marked > 0, "{}: nothing was marked: {m:?}", path.name());
            }
            assert!(
                m.queue_drops <= unmarked.queue_drops,
                "{}: {m:?} {unmarked:?}",
                path.name()
            );
            assert!(m.answered * 10 >= m.asked * 9, "{}: {m:?}", path.name());
            assert!(
                m.queue_p99_ns * 9 <= unmarked.queue_p99_ns * 10,
                "{}: the manager lengthened the queue: {m:?} {unmarked:?}",
                path.name()
            );
            queue.push(m.queue_p99_ns.max(1) as f64 / unmarked.queue_p99_ns.max(1) as f64);
            carried.push(m.carried_ppm.max(1) as f64 / reno.carried_ppm.max(1) as f64);
        }
    }
    let queue = geomean(queue.into_iter());
    let carried = geomean(carried.into_iter());
    println!("queue under the manager / without it: {queue:.3}; carried / NewReno's: {carried:.3}");
    assert!(queue <= 1.0, "the managed queue is longer: {queue:.3}");
    assert!(carried >= 1.0, "carries less than NewReno: {carried:.3}");
}

/// What a flow gives and takes beside NewReno or CUBIC through one
/// bottleneck in one run: the incumbent's share beside it over the
/// incumbent's bar ([`harm_bar`]), and whether either stalled.
struct Beside {
    law: u64,
    incumbent: u64,
    bar: u64,
    jain: f64,
    marks: u64,
}
impl Beside {
    fn ratio(&self) -> f64 {
        self.incumbent.max(1) as f64 / self.bar.max(1) as f64
    }
    fn stalled(&self) -> bool {
        self.law * 10 < self.incumbent || self.incumbent * 10 < self.law
    }
    fn row(&self) -> String {
        format!(
            "{:.1}% / {:.1}% (bar {:.1}%; Jain {:.3}; marks {})",
            self.law as f64 / 10_000.0,
            self.incumbent as f64 / 10_000.0,
            self.bar as f64 / 10_000.0,
            self.jain,
            self.marks
        )
    }
}
fn beside(path: Scenario, law: Law, incumbent: Law, bar: u64, pki: &Pki) -> Beside {
    let pair = compete(path, &[law, incumbent], pki);
    let [newcomer, them] = pair.as_slice() else {
        panic!("{pair:?}")
    };
    for m in &pair {
        assert_eq!(m.closed, None, "{}: {m:?}", path.name());
    }
    Beside {
        law: newcomer.carried_ppm,
        incumbent: them.carried_ppm,
        bar,
        jain: fairness(&pair),
        marks: newcomer.marked,
    }
}

/// The seeds a scenario's harm is judged over. The incumbent's share beside
/// Copa over its bar is one run's, and a run is its seed: over eight seeds
/// at 1 Mbit/s, 100 ms under CoDel it spread with a standard deviation of
/// 0.096 beside CUBIC and 0.064 beside NewReno, the bar itself from 35.5% to
/// 42.6% (2026-10-03), so one seed's ratio judged against the floor of nine
/// tenths judges the seed. Over `n` seeds the mean's deviation is the run's
/// over `√n`: at that spread, eight seeds hold a law at its bar 2.95 of the
/// mean's deviations above the floor, past [`FLOOR_DEVIATIONS`].
const HARM_SEEDS: u64 = 8;
/// How many of the seeds' mean's deviations a scenario's ratio must stand
/// from the floor of nine tenths, either side, for the seeds to have judged
/// it and not chance: the one-sided normal quantile at which the eight pairs
/// of incumbent and path a grid judges are judged at the conventional 5%
/// together (Bonferroni: 0.05/8, z = 2.50). Each judgement checks it on its
/// own seeds ([`Harm::resolved`]); one that falls short asks for more seeds.
const FLOOR_DEVIATIONS: f64 = 2.5;

/// `measure` of each seed of the harm's judgement, the seeds' runs at once:
/// they share nothing.
fn per_seed<T: Send>(measure: impl Fn(u64) -> T + Sync) -> Vec<T> {
    std::thread::scope(|scope| {
        let measure = &measure;
        let runs: Vec<_> = (1..=HARM_SEEDS)
            .map(|seed| scope.spawn(move || measure(seed)))
            .collect();
        runs.into_iter().map(|run| run.join().unwrap()).collect()
    })
}

/// The incumbent's bar under each seed of the harm's judgement.
fn bars(path: Scenario, incumbent: Law, pki: &Pki) -> Vec<u64> {
    per_seed(|seed| harm_bar(Scenario { seed, ..path }, incumbent, pki))
}

/// What a law does beside an incumbent in one scenario over the seeds of
/// the harm's judgement ([`HARM_SEEDS`]), against the incumbent's bar under
/// each seed: the incumbent's share over its bar by geometric mean over the
/// seeds, as the harness judges its other choices, and their spread.
struct Harm {
    ratio: f64,
    /// The standard deviation of the seeds' ratios' logarithms: the
    /// geometric mean's spread.
    deviation: f64,
    stalled: bool,
    /// The fewest marks a seed's newcomer took.
    least_marks: u64,
    row: String,
}
impl Harm {
    /// Whether the ratio stands [`FLOOR_DEVIATIONS`] of the seeds' mean's
    /// deviations from the floor of nine tenths, either side, in the
    /// logarithms the geometric mean is taken in: the floor judged the law,
    /// not the seeds.
    fn resolved(&self) -> bool {
        (self.ratio.ln() - 0.9_f64.ln()).abs()
            >= FLOOR_DEVIATIONS * self.deviation / (HARM_SEEDS as f64).sqrt()
    }
}
fn harm(path: Scenario, law: Law, incumbent: Law, bars: &[u64], pki: &Pki) -> Harm {
    let runs = per_seed(|seed| {
        let at = usize::try_from(seed - 1).unwrap();
        beside(Scenario { seed, ..path }, law, incumbent, bars[at], pki)
    });
    let ratios: Vec<f64> = runs.iter().map(Beside::ratio).collect();
    let logs: Vec<f64> = ratios
        .iter()
        .map(|ratio| ratio.max(f64::MIN_POSITIVE).ln())
        .collect();
    let mean = logs.iter().sum::<f64>() / logs.len() as f64;
    let deviation = (logs.iter().map(|log| (log - mean).powi(2)).sum::<f64>()
        / (logs.len() as f64 - 1.0).max(1.0))
    .sqrt();
    let average = |part: fn(&Beside) -> u64| {
        runs.iter().map(part).sum::<u64>() as f64 / runs.len() as f64 / 10_000.0
    };
    let ratio = geomean(ratios.iter().copied());
    let least_marks = runs.iter().map(|run| run.marks).min().unwrap_or(0);
    Harm {
        ratio,
        deviation,
        stalled: runs.iter().any(Beside::stalled),
        least_marks,
        row: format!(
            "{:.1}% / {:.1}% (bar {:.1}%; {ratio:.3} of the bar over {HARM_SEEDS} seeds, deviation {deviation:.3} in logarithms; marks {least_marks} at least)",
            average(|run| run.law),
            average(|run| run.incumbent),
            average(|run| run.bar),
        ),
    }
}

/// Copa beside NewReno and beside CUBIC through one bottleneck, with the
/// queue managed by CoDel — the classic manager a sender of ECT(0) meets
/// (RFC 7567, RFC 8289) — and without a manager (the audit's F39). **The
/// rule:** with CoDel or without a manager, neither flow carries less than a
/// tenth of what the other carries (this harness's rule for a stall); under
/// CoDel, where Copa answers marks, CoDel marks Copa beside each, and each
/// incumbent carries beside Copa nine tenths at least of its bar
/// ([`harm_bar`]) in every scenario (the harness's tolerance for "about as
/// much", as in the law's choice), judged over the seeds of [`HARM_SEEDS`],
/// and its bar at least over the scenarios by geometric mean: Copa harms it
/// no more than its own kind or CUBIC does. Without a manager the shares
/// beside the bar are reported: there Copa competing answers a loss by `1/δ`
/// alone, a loss being no proof of congestion on a lossy path, and takes
/// more than the bar at long round trips (the record's F39, open). Under a
/// step at one datagram — DCTCP's threshold for senders of ECT(1) (RFC 8257,
/// RFC 9330), where a classic sender starves itself — the shares are
/// reported only. A first statement judged each scenario by one seed's run,
/// which judged the seed ([`HARM_SEEDS`], 2026-10-03).
#[test]
fn copa_shares_a_bottleneck_with_newreno_and_cubic() {
    let pki = Pki::new();
    let full = std::env::var_os("FOCAL_CONGESTION_FULL").is_some();
    let (paths, seconds): (&[(u64, u64)], u64) = if full {
        (
            &[
                (1_000_000, 100),
                (10_000_000, 20),
                (10_000_000, 100),
                (100_000_000, 20),
            ],
            30,
        )
    } else {
        (&[(10_000_000, 20)], 10)
    };
    println!("| Path | Incumbent | Copa / incumbent carried (bar; Jain; marks) |");
    println!("|---|---|---|");
    let mut ratios: BTreeMap<Law, Vec<f64>> = BTreeMap::new();
    let mut failed = Vec::new();
    for (rate, rtt) in paths {
        for marking in std::iter::once(Marking::Off).chain(managers()) {
            let path = Scenario {
                ecn: true,
                marking,
                ..scenario(*rate, *rtt, 0, seconds)
            };
            for incumbent in [Law::NewReno, Law::Cubic] {
                if !matches!(marking, Marking::CoDel { .. }) {
                    let bar = harm_bar(path, incumbent, &pki);
                    let measured = beside(path, Law::Copa, incumbent, bar, &pki);
                    println!("| {} | {incumbent:?} | {} |", path.name(), measured.row());
                    if marking == Marking::Off && measured.stalled() {
                        failed.push(format!(
                            "{} beside {incumbent:?}: a stall: {}",
                            path.name(),
                            measured.row()
                        ));
                    }
                    continue;
                }
                let measured = harm(
                    path,
                    Law::Copa,
                    incumbent,
                    &bars(path, incumbent, &pki),
                    &pki,
                );
                println!("| {} | {incumbent:?} | {} |", path.name(), measured.row);
                if !measured.resolved() {
                    failed.push(format!(
                        "{} beside {incumbent:?}: the seeds spread too far to judge the floor: {}",
                        path.name(),
                        measured.row
                    ));
                }
                if measured.stalled {
                    failed.push(format!(
                        "{} beside {incumbent:?}: a stall: {}",
                        path.name(),
                        measured.row
                    ));
                }
                if measured.ratio * 10.0 < 9.0 {
                    failed.push(format!(
                        "{} beside {incumbent:?}: under nine tenths of its bar: {}",
                        path.name(),
                        measured.row
                    ));
                }
                if measured.least_marks == 0 {
                    failed.push(format!(
                        "{} beside {incumbent:?}: nothing was marked",
                        path.name()
                    ));
                }
                ratios.entry(incumbent).or_default().push(measured.ratio);
            }
        }
    }
    for (incumbent, values) in &ratios {
        let ratio = geomean(values.iter().copied());
        println!("{incumbent:?} beside Copa / its bar under CoDel: {ratio:.3}");
        if ratio < 1.0 {
            failed.push(format!(
                "Copa harms {incumbent:?} more than its bar: {ratio:.3}"
            ));
        }
    }
    assert!(failed.is_empty(), "{failed:#?}");
}

/// Copa by what a mark multiplies its window by, of the backoffs the RFCs
/// give a classic sender (RFC 3168 and RFC 9002 §B.2: 1/2; RFC 9438: 7/10;
/// RFC 8511's experimental β_ecn: 4/5). **The rule:** a backoff is
/// admissible where, over every queue manager and path by geometric mean,
/// Copa alone keeps the queue's 99th percentile no longer than without the
/// manager and carries no less than NewReno under it, and where, under
/// CoDel — where the backoff is what Copa does, marks being what it
/// answers — no flow beside NewReno or CUBIC stalls and each carries beside
/// Copa nine tenths at least of its bar ([`harm_bar`]) in every scenario,
/// judged over the seeds of [`HARM_SEEDS`], and its bar at least by
/// geometric mean; of the admissible, the one that carries the most alone
/// is the law's. A first statement judged harm by geometric mean alone,
/// over the scenarios without a manager too: those never mark, are the same
/// for every backoff, and lifted the mean over a scenario where 4/5 left
/// CUBIC 0.72 of its bar; a second judged each scenario by one seed's run
/// (2026-10-03).
#[test]
fn the_mark_backoff_of_the_law_is_the_one_that_was_measured() {
    let pki = Pki::new();
    let full = std::env::var_os("FOCAL_CONGESTION_FULL").is_some();
    let (paths, seconds): (&[(u64, u64)], u64) = if full {
        (
            &[
                (1_000_000, 100),
                (10_000_000, 20),
                (10_000_000, 100),
                (100_000_000, 20),
            ],
            30,
        )
    } else {
        (&[(10_000_000, 20)], 8)
    };
    let backoffs = [(1u64, 2u64), (7, 10), (4, 5)];
    let mut queue = vec![Vec::new(); backoffs.len()];
    let mut carried = vec![Vec::new(); backoffs.len()];
    let mut harms: Vec<BTreeMap<Law, Vec<f64>>> = vec![BTreeMap::new(); backoffs.len()];
    let mut stalled = vec![false; backoffs.len()];
    let mut under_floor = vec![false; backoffs.len()];
    println!(
        "| Path | Backoff | alone: queue p99 ms (unmanaged) / carried (NewReno) | beside NewReno | beside CUBIC |"
    );
    println!("|---|---|---|---|---|");
    for (rate, rtt) in paths {
        let plain = Scenario {
            ecn: true,
            ..scenario(*rate, *rtt, 0, seconds)
        };
        let unmarked = measure(plain, Law::Copa, &pki);
        for marking in std::iter::once(Marking::Off).chain(managers()) {
            let path = Scenario { marking, ..plain };
            let judged = matches!(marking, Marking::CoDel { .. });
            let incumbents = [Law::NewReno, Law::Cubic];
            // Under CoDel each incumbent's bar under each seed; elsewhere one
            // run's, reported.
            let bars: Vec<Vec<u64>> = incumbents
                .iter()
                .map(|incumbent| {
                    if judged {
                        bars(path, *incumbent, &pki)
                    } else {
                        vec![harm_bar(path, *incumbent, &pki)]
                    }
                })
                .collect();
            let reno = standard_under(path, &pki);
            for (at, (numerator, denominator)) in backoffs.iter().enumerate() {
                let law = Law::Backoff(*numerator, *denominator);
                let mut rows = Vec::new();
                for (index, incumbent) in incumbents.into_iter().enumerate() {
                    if !judged {
                        rows.push(beside(path, law, incumbent, bars[index][0], &pki).row());
                        continue;
                    }
                    let measured = harm(path, law, incumbent, &bars[index], &pki);
                    stalled[at] |= measured.stalled;
                    // A ratio its seeds did not resolve from the floor is
                    // not shown above it.
                    under_floor[at] |= measured.ratio * 10.0 < 9.0 || !measured.resolved();
                    harms[at].entry(incumbent).or_default().push(measured.ratio);
                    rows.push(measured.row);
                }
                let alone = if marking == Marking::Off {
                    None
                } else {
                    let alone = measure(path, law, &pki);
                    queue[at].push(
                        alone.queue_p99_ns.max(1) as f64 / unmarked.queue_p99_ns.max(1) as f64,
                    );
                    carried[at]
                        .push(alone.carried_ppm.max(1) as f64 / reno.carried_ppm.max(1) as f64);
                    Some(alone)
                };
                println!(
                    "| {} | {numerator}/{denominator} | {} | {} | {} |",
                    path.name(),
                    alone.map_or("—".to_owned(), |alone| format!(
                        "{:.2} ({:.2}) / {:.1}% ({:.1}%)",
                        alone.queue_p99_ns as f64 / MS as f64,
                        unmarked.queue_p99_ns as f64 / MS as f64,
                        alone.carried_ppm as f64 / 10_000.0,
                        reno.carried_ppm as f64 / 10_000.0
                    )),
                    rows[0],
                    rows[1]
                );
            }
        }
    }
    let judged: Vec<(MarkBackoffPair, bool, f64)> = backoffs
        .iter()
        .enumerate()
        .map(|(at, (numerator, denominator))| {
            let queue = geomean(queue[at].iter().copied());
            let carried = geomean(carried[at].iter().copied());
            let harms: Vec<(Law, f64)> = harms[at]
                .iter()
                .map(|(law, values)| (*law, geomean(values.iter().copied())))
                .collect();
            let admissible = !stalled[at]
                && !under_floor[at]
                && queue <= 1.0
                && carried >= 1.0
                && harms.iter().all(|(_, ratio)| *ratio >= 1.0);
            println!(
                "backoff {numerator}/{denominator}: admissible {admissible}; alone queue {queue:.3} of unmanaged, carried {carried:.3} of NewReno; beside under CoDel: {harms:?}; stalled {}; under nine tenths of a bar, or not resolved from it, somewhere {}",
                stalled[at],
                under_floor[at]
            );
            ((*numerator, *denominator), admissible, carried)
        })
        .collect();
    if full {
        let chosen = judged
            .iter()
            .filter(|(_, admissible, _)| *admissible)
            .max_by(|left, right| left.2.total_cmp(&right.2))
            .map(|(backoff, _, _)| *backoff);
        let default = focal_wire::congestion::DEFAULT_MARK_BACKOFF;
        assert_eq!(chosen, Some((default.numerator, default.denominator)));
    }
}
type MarkBackoffPair = (u64, u64);
