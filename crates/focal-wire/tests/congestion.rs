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
use focal_sim::path::{Fabric, Fate, Link, Loss, Path};
use quinn_proto::{
    ClientConfig, Connection, ConnectionHandle, DatagramEvent, Dir, Endpoint, EndpointConfig,
    Event, ServerConfig, StreamEvent, StreamId, TransportConfig, VarInt,
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
const CLIENT: u64 = 1;
const SERVER: u64 = 2;
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
}
impl Law {
    const ALL: [Law; 4] = [Law::NewReno, Law::Cubic, Law::Bbr, Law::Copa];
    fn factory(self) -> Arc<dyn ControllerFactory + Send + Sync> {
        match self {
            Law::NewReno => Arc::new(NewRenoConfig::default()),
            Law::Cubic => Arc::new(CubicConfig::default()),
            Law::Bbr => Arc::new(BbrConfig::default()),
            Law::Copa => Arc::new(focal_wire::congestion::CopaConfig::default()),
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
        format!(
            "{rate} {}ms {}%{}{}",
            self.rtt_ns / MS,
            f64::from(self.loss_ppm) / 10_000.0,
            if self.bursts { " bursts" } else { "" },
            if self.queue_bdps == 1 {
                String::new()
            } else {
                format!(" queue x{}", self.queue_bdps)
            }
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
    /// Why the connection ended before the run did, and when.
    closed: Option<(u64, String)>,
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
struct Run {
    scenario: Scenario,
    began: Instant,
    fabric: Fabric<Vec<u8>>,
    client: Side,
    server: Side,
    server_config: Arc<ServerConfig>,
    scratch: Vec<u8>,
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
    fn new(scenario: Scenario, law: Law, pki: &Pki) -> Self {
        let mut server_config =
            ServerConfig::with_single_cert(vec![pki.certificate.clone()], pki.key.clone_key())
                .unwrap();
        server_config.transport_config(transport(law));
        let server_config = Arc::new(server_config);
        let mut seed = [0u8; 32];
        seed[..8].copy_from_slice(&scenario.seed.to_le_bytes());
        let endpoint = |server: Option<Arc<ServerConfig>>, salt: u8| {
            let mut seed = seed;
            seed[31] = salt;
            Endpoint::new(
                Arc::new(EndpointConfig::default()),
                server,
                false,
                Some(seed),
            )
        };
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
        for (from, to) in [(CLIENT, SERVER), (SERVER, CLIENT)] {
            let link = fabric
                .add_link(Link {
                    rate_bits_per_second: scenario.rate_bits_per_second,
                    queue_bytes: queue,
                })
                .unwrap();
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
        Self {
            scenario,
            began: Instant::now(),
            fabric,
            client: Side {
                node: CLIENT,
                address: "10.0.0.1:4433".parse().unwrap(),
                endpoint: endpoint(None, 1),
                connection: None,
            },
            server: Side {
                node: SERVER,
                address: "10.0.0.2:4433".parse().unwrap(),
                endpoint: endpoint(Some(server_config.clone()), 2),
                connection: None,
            },
            server_config,
            scratch: Vec::with_capacity(2 * DATAGRAM),
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
        }
    }
    fn connect(&mut self, law: Law, pki: &Pki) {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(pki.certificate.clone()).unwrap();
        let mut config = ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        config.transport_config(transport(law));
        let now = self.at(0);
        let connection = self
            .client
            .endpoint
            .connect(now, config, self.server.address, "localhost")
            .unwrap();
        self.client.connection = Some(connection);
    }
    /// What arrived by `now` is handed to whom it is for.
    fn deliver(&mut self, now: u64) {
        let at = self.at(now);
        while let Some(delivery) = self.fabric.receive() {
            let (side, from) = if delivery.to == SERVER {
                (&mut self.server, self.client.address)
            } else {
                (&mut self.client, self.server.address)
            };
            self.scratch.clear();
            let event = side.endpoint.handle(
                at,
                from,
                None,
                None,
                BytesMut::from(delivery.message.as_slice()),
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
                            Some(self.server_config.clone()),
                        )
                        .unwrap_or_else(|error| panic!("the server refused: {:?}", error.cause));
                    side.connection = Some(accepted);
                }
                Some(DatagramEvent::Response(transmit)) => {
                    let (node, peer) = (side.node, delivery.from);
                    let bytes = self.scratch[..transmit.size].to_vec();
                    let size = bytes.len();
                    let _ = self.fabric.send(node, peer, bytes, size);
                }
                None => {}
            }
        }
    }
    /// What the two ends do with what they were told.
    fn apply(&mut self, now: u64) {
        let (warm, end) = (self.warm(), self.end());
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
                while self.transfers.len() < self.scenario.transfers {
                    let id = connection.streams().open(Dir::Uni).unwrap();
                    if self.scenario.classes {
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
                        if self.scenario.classes {
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
                    if self.scenario.classes {
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
    /// Timers that are due fire, and what each end has to send leaves.
    /// Whether anything did.
    fn transmit(&mut self, now: u64) -> bool {
        let at = self.at(now);
        let mut sent = false;
        for side in [&mut self.client, &mut self.server] {
            let peer = if side.node == CLIENT { SERVER } else { CLIENT };
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
                self.datagrams += 1;
                sent = true;
                if let Fate::Arrives { .. } = self.fabric.send(side.node, peer, bytes, size) {}
            }
        }
        sent
    }
    fn next(&mut self, now: u64) -> u64 {
        let mut next = self.end();
        if let Some(arrival) = self.fabric.next_arrival() {
            next = next.min(arrival);
        }
        if self.connected {
            next = next.min(self.next_exchange.max(now));
        }
        let began = self.began;
        for side in [&mut self.client, &mut self.server] {
            if let Some((_, connection)) = &mut side.connection
                && let Some(due) = connection.poll_timeout()
            {
                let due = u64::try_from(due.saturating_duration_since(began).as_nanos()).unwrap();
                next = next.min(due);
            }
        }
        next
    }
    fn run(mut self, law: Law, pki: &Pki) -> Measured {
        self.connect(law, pki);
        let mut now = 0u64;
        let mut warmed = false;
        // Every pass either sends, or moves the clock: a run ends.
        let mut passes = 0u64;
        let bound = self.scenario.seconds
            * (self.scenario.rate_bits_per_second / 8 / DATAGRAM as u64 + 1_000)
            * 64;
        while now < self.end() && self.closed.is_none() {
            passes += 1;
            assert!(
                passes < bound,
                "{law:?} in {}: the run spins",
                self.scenario.name()
            );
            self.deliver(now);
            self.apply(now);
            let sent = self.transmit(now);
            if !warmed && now >= self.warm() {
                warmed = true;
                self.transfer_at_warm = self.transfer_received;
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
        let mut took = std::mem::take(&mut self.took);
        took.sort_unstable();
        let at = |per_cent: usize| {
            if took.is_empty() {
                return 0;
            }
            took[((took.len() * per_cent).div_ceil(100)).clamp(1, took.len()) - 1]
        };
        let measured_ns = self.end() - self.warm();
        let could = u128::from(self.scenario.rate_bits_per_second) / 8 * u128::from(measured_ns)
            / 1_000_000_000;
        let carried = u128::from(self.transfer_received - self.transfer_at_warm);
        // Exchanges asked within the last second may be on their way.
        let asked = self.asked_after_warm;
        Measured {
            asked,
            answered: took.len() as u64,
            p50_ns: at(50),
            p99_ns: at(99),
            worst_ns: took.last().copied().unwrap_or(0),
            carried_ppm: u64::try_from(carried * 1_000_000 / could.max(1)).unwrap(),
            queue_drops: stats.dropped_queue,
            lost: stats.dropped_loss,
            datagrams: self.datagrams,
            closed: self.closed.clone(),
        }
    }
}

fn measure(scenario: Scenario, law: Law, pki: &Pki) -> Measured {
    Run::new(scenario, law, pki).run(law, pki)
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
