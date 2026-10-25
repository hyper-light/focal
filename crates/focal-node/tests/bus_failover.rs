//! kill -9 of a bus group's leader under its client (doc 34 §5 and P2): three `focal start bus`
//! units replicating one part on loopback, and in this process a client of the group as focal-bus
//! makes one (`client::Client::group`), publishing batches on a session of its own. The leader's
//! process is killed (SIGKILL, TerminateProcess on Windows): it says nothing, its socket is gone,
//! and its connection stays open on the client's side. Every batch, before the kill and after, is
//! acknowledged, the stream holds each once and in order, and the client found the new leader by
//! itself. Found by Quiver on fedora (2026-10-25): the client kept publishing into the dead leader
//! to the end of its run, as nothing but its idle timeout told it the leader was gone.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation
)]

#[path = "support/ports.rs"]
mod ports;
#[path = "support/progress.rs"]
mod progress;

use std::collections::{BTreeMap, BTreeSet};
use std::future::{Future, poll_fn};
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::Path;
use std::pin::pin;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::task::Poll;
use std::time::{Duration, Instant};

use focal_bus::batch::{BatchBuilder, Producing};
use focal_bus::budget::BusBudget;
use focal_bus::client::{Client, Delivery};
use focal_bus::directory::BusDirectory;
use focal_bus::limits::{IO, SERVE_PERIOD};
use focal_bus::wire::{self, Ack, Bus, Durability};
use focal_memory::MemoryBudget;
use focal_timing::ProgressDeadline;
use hyper_rt::runtime::LocalRuntime;
use hyper_rt_transport::Driver;
use hyper_transport::{Event, Refusal};

type Drv = Driver<Bus, BusBudget, BusDirectory>;

/// The group's nodes, node 1 first.
const NODES: u64 = 3;
/// The stream published to.
const STREAM: u64 = 1;
/// The client's session (doc 34 P6): the units are made fresh, so no run shares it.
const SESSION: u64 = 1;
/// Batches published before the kill, and in all.
const BEFORE: u32 = 50;
const TOTAL: u32 = 100;
/// Batches the client has in flight at once.
const WINDOW: u32 = 4;
/// A wait fails only when what it waits on did not move for this long: a wedge, not slowness.
const FROZEN: Duration = Duration::from_secs(30);

/// Milliseconds since the Unix epoch, the clock a unit's lines state.
fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(0)
}

/// One `focal start bus` unit: its process and the JSON lines it prints.
struct Unit {
    child: Child,
    lines: mpsc::Receiver<serde_json::Value>,
}

/// Every unit of the run, killed when the run ends, however it ends: a test that fails leaves no
/// node running to hold its ports and directories.
struct Units(Vec<Option<Unit>>);

impl Drop for Units {
    fn drop(&mut self) {
        for unit in self.0.iter_mut().flatten() {
            let _ = unit.child.kill();
            let _ = unit.child.wait();
        }
    }
}

/// Starts node `n` of the group whose nodes listen at `addresses`, its directories under `dir`,
/// and waits for the line saying it listens: while it prints nothing yet, a wall-clock window
/// ([`FROZEN`]), the binary admitted before (an OS's assessment of a first launch is not the
/// node's start).
fn start(n: u64, addresses: &[SocketAddr], dir: &Path) -> Unit {
    let mut command = Command::new(env!("CARGO_BIN_EXE_focal"));
    command
        .args(["start", "bus", "--listen"])
        .arg(addresses[(n - 1) as usize].to_string())
        .arg("--pki")
        .arg(dir.join("pki"))
        .args(["--node", &n.to_string()])
        .arg("--data")
        .arg(dir.join(format!("node-{n}")))
        .arg("--key-file")
        .arg(dir.join("keys").join(format!("node-{n}.key")));
    for (at, address) in addresses.iter().enumerate() {
        let peer = at as u64 + 1;
        if peer != n {
            command.arg("--peer").arg(format!("{peer}={address}"));
        }
    }
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (send, lines) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(output).lines() {
            let Ok(line) = line else { break };
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&line)
                && send.send(value).is_err()
            {
                break;
            }
        }
    });
    let began = Instant::now();
    loop {
        match lines.recv_timeout(Duration::from_millis(100)) {
            Ok(line) if line["condition"] == "Listening" => break,
            Ok(_) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("node {n} exited before it listened: {:?}", child.try_wait())
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                assert!(
                    began.elapsed() < FROZEN,
                    "node {n} printed nothing for {FROZEN:?}"
                );
            }
        }
    }
    Unit { child, lines }
}

/// The client's side: what it published, what was acknowledged, and what it owes.
struct Publisher {
    driver: Drv,
    bus: Client,
    builder: BatchBuilder,
    /// Every batch made and not acknowledged, by sequence.
    unacked: BTreeMap<u32, Vec<u8>>,
    /// Those to send, again or for the first time, to the node the client sends to.
    pending: BTreeSet<u32>,
    /// Each batch acknowledged at its offsets, and those acknowledged as taken already without
    /// them (sent again, not their session's newest: doc 34 P6).
    acked: BTreeMap<u32, (u64, u16)>,
    taken: BTreeSet<u32>,
    sent: u32,
    redirects: u64,
    reroutes: u64,
    lost: u64,
    /// What a subscription from the stream's start was delivered, by offset, and the window it
    /// stated.
    delivered: Vec<(u64, u32)>,
    subscribed: Option<wire::Subscribed>,
    /// From when the first acknowledgement is timed (the kill), and how long after it came.
    mark: Option<Instant>,
    first_after_mark: Option<Duration>,
}

impl Publisher {
    fn acknowledged(&self) -> usize {
        self.acked.len()
            + self
                .taken
                .iter()
                .filter(|sequence| !self.acked.contains_key(sequence))
                .count()
    }

    /// What the client did besides acknowledgements: a wait on it moves as the client does.
    fn moves(&self) -> [u64; 4] {
        [
            self.acknowledged() as u64,
            self.redirects,
            self.reroutes,
            self.lost,
        ]
    }

    /// Makes batches up to `until`, one record each naming its sequence, and sends what waits, in
    /// the session's order, unless the client's back-off holds it.
    fn publish(&mut self, until: u32) {
        while self.sent < until {
            self.builder.begin(&Producing {
                stream: STREAM,
                producer: SESSION,
                sequence: self.sent,
                flags: 0,
                timestamp: 0,
            });
            self.builder.push(0, &self.sent.to_le_bytes()).unwrap();
            let bytes = self.builder.finish().unwrap().to_vec();
            self.unacked.insert(self.sent, bytes);
            self.pending.insert(self.sent);
            self.sent += 1;
        }
        let now = Instant::now();
        if !self.bus.ready(now) {
            return;
        }
        // While the client asks a node about the leader, only the oldest batch owed goes: its
        // answer names the leader or acknowledges it.
        let mut sends = if self.bus.asking() {
            1
        } else {
            self.pending.len()
        };
        while sends > 0
            && let Some(&first) = self.pending.first()
        {
            sends -= 1;
            let Some(bytes) = self.unacked.get(&first) else {
                self.pending.remove(&first);
                continue;
            };
            match self.bus.publish(self.driver.endpoint(), bytes) {
                Ok(()) => {
                    self.pending.remove(&first);
                }
                Err(Refusal::NotConnected) => {
                    let _ = self.bus.dial(self.driver.endpoint(), now);
                    return;
                }
                Err(Refusal::LaneFull) => return,
                Err(other) => panic!("publish refused: {other:?}"),
            }
        }
    }

    /// Takes the client's events for at most a tick.
    async fn step(&mut self) {
        let mut tick = pin!(hyper_rt::futures::sleep(1_000_000));
        let driver = &mut self.driver;
        let first = poll_fn(|context| {
            if let Poll::Ready(event) = driver.poll_event(context) {
                return Poll::Ready(Some(event.unwrap()));
            }
            if tick.as_mut().poll(context).is_ready() {
                return Poll::Ready(None);
            }
            Poll::Pending
        })
        .await;
        let mut events: Vec<Event<Bus>> = first.into_iter().collect();
        while let Some(event) = self.driver.endpoint().poll_event() {
            events.push(event);
        }
        for event in events {
            let mut acks = Vec::new();
            let mut redirected = Vec::new();
            let mut rerouted = false;
            let delivered = &mut self.delivered;
            let subscribed = &mut self.subscribed;
            let lost = &mut self.lost;
            self.bus
                .on_event(
                    self.driver.endpoint(),
                    event,
                    Instant::now(),
                    |delivery| match delivery {
                        Delivery::Acked(ack) => acks.push(ack),
                        Delivery::Redirected(redirect) => redirected.push(redirect.sequence),
                        Delivery::Rerouted { .. } => rerouted = true,
                        Delivery::Lost { .. } => *lost += 1,
                        Delivery::Batch(batch) => {
                            delivered.push((batch.base_offset(), batch.sequence()))
                        }
                        Delivery::Subscribed {
                            subscribed: window, ..
                        } => *subscribed = Some(window),
                        other @ (Delivery::Gap(_)
                        | Delivery::Moved(_)
                        | Delivery::SubscribeRedirected { .. }
                        | Delivery::Unsubscribed { .. }
                        | Delivery::Refused(_)) => panic!("{other:?}"),
                    },
                );
            for ack in acks {
                self.acknowledge(ack);
            }
            self.redirects += redirected.len() as u64;
            for sequence in redirected {
                if self.unacked.contains_key(&sequence) {
                    self.pending.insert(sequence);
                }
            }
            if rerouted {
                self.reroutes += 1;
                self.pending.extend(self.unacked.keys().copied());
            }
        }
    }

    fn acknowledge(&mut self, ack: Ack) {
        assert_eq!((ack.stream, ack.producer), (STREAM, SESSION), "{ack:?}");
        if ack.class == Ack::OUT_OF_ORDER {
            // Past a gap in the session: everything from the sequence the part waits for goes
            // again, in order.
            let from = u32::try_from(ack.base).unwrap();
            self.pending
                .extend(self.unacked.range(from..).map(|(sequence, _)| *sequence));
            return;
        }
        assert_eq!(
            Durability::of(ack.class),
            Some(Durability::Persisted),
            "{ack:?}"
        );
        self.unacked.remove(&ack.sequence);
        self.pending.remove(&ack.sequence);
        if let Some(mark) = self.mark
            && self.first_after_mark.is_none()
        {
            self.first_after_mark = Some(mark.elapsed());
        }
        if ack.count == 0 {
            self.taken.insert(ack.sequence);
        } else if let Some(first) = self.acked.insert(ack.sequence, (ack.base, ack.count)) {
            assert_eq!(first, (ack.base, ack.count), "acknowledged at one offset");
        }
    }

    /// Publishes until `until` batches are acknowledged, at most [`WINDOW`] in flight, charged to
    /// what the client does.
    async fn publish_until(&mut self, until: u32, what: &str) {
        let mut wait = ProgressDeadline::begin(&self.moves(), u64::MAX, FROZEN);
        while self.acknowledged() < until as usize {
            let acknowledged = self.acknowledged() as u32;
            self.publish((acknowledged + WINDOW).min(until));
            self.step().await;
            if let Err(frozen) = wait.check(&self.moves()) {
                let (leader, term) = self.bus.leader();
                panic!(
                    "{what}: {frozen:?}; {} acknowledged of {until}, client sends to {}, leader \
                     {leader} term {term}, {} redirects, {} reroutes, {} lost",
                    self.acknowledged(),
                    self.bus.node(),
                    self.redirects,
                    self.reroutes,
                    self.lost
                );
            }
        }
    }
}

#[test]
fn a_bus_groups_client_finds_the_new_leader_after_kill_9_of_its_leader() {
    progress::admit();
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("keys")).unwrap();
    let addresses: Vec<SocketAddr> = (0..NODES)
        .map(|_| ports::address().parse().unwrap())
        .collect();
    // Node 1 makes the identity directory; the others read it.
    let mut units = Units(vec![Some(start(1, &addresses, dir.path()))]);
    for n in 2..=NODES {
        units.0.push(Some(start(n, &addresses, dir.path())));
    }
    let local =
        focal_bus::pki::load_or_make(&dir.path().join("pki"), None, NODES as usize, 2).unwrap();
    let budget = MemoryBudget::new(64 << 20, 0).unwrap();
    let endpoint =
        focal_bus::client::endpoint(&local, 0, NODES as usize, budget, Instant::now()).unwrap();
    let nodes: Vec<(u64, SocketAddr)> = (1..=NODES).zip(addresses.iter().copied()).collect();
    let mut runtime = LocalRuntime::new(&focal_rt::local_config()).unwrap();
    runtime
        .block_on(async move {
            let driver: Drv = Driver::bind(endpoint, "127.0.0.1:0".parse().unwrap(), IO).unwrap();
            let mut publisher = Publisher {
                driver,
                bus: Client::group(&nodes).unwrap(),
                builder: BatchBuilder::new(),
                unacked: BTreeMap::new(),
                pending: BTreeSet::new(),
                acked: BTreeMap::new(),
                taken: BTreeSet::new(),
                sent: 0,
                redirects: 0,
                reroutes: 0,
                lost: 0,
                delivered: Vec::new(),
                subscribed: None,
                mark: None,
                first_after_mark: None,
            };
            publisher
                .bus
                .dial(publisher.driver.endpoint(), Instant::now())
                .unwrap();
            publisher
                .publish_until(BEFORE, "the batches before the kill")
                .await;

            // The node that acknowledged leads; its process is killed with nothing said.
            let leader = publisher.bus.node();
            let at = (leader - 1) as usize;
            let mut killed = units.0[at].take().unwrap();
            let terms: Vec<serde_json::Value> = killed.lines.try_iter().collect();
            for unit in units.0.iter().flatten() {
                while unit.lines.try_recv().is_ok() {}
            }
            let killed_ms = unix_ms();
            killed.child.kill().unwrap();
            killed.child.wait().unwrap();
            let reroutes = publisher.reroutes;
            let since = Instant::now();
            publisher.mark = Some(since);
            publisher
                .publish_until(TOTAL, "the batches after the kill")
                .await;
            eprintln!(
                "node {leader} killed: the first acknowledgement {:?} after it, every batch {:?} \
                 after it, {} reroutes, {} redirects, the client at node {}",
                publisher.first_after_mark,
                since.elapsed(),
                publisher.reroutes - reroutes,
                publisher.redirects,
                publisher.bus.node()
            );
            // What the survivors said of the kill, from it: when each suspected the leader and
            // when the group's next term began.
            for (at, unit) in units.0.iter().enumerate() {
                let Some(unit) = unit else { continue };
                for line in unit.lines.try_iter() {
                    if matches!(line["condition"].as_str(), Some("Suspected" | "Term")) {
                        let after = line["unix_ms"]
                            .as_u64()
                            .unwrap_or(0)
                            .saturating_sub(killed_ms);
                        eprintln!("node {}: {after} ms after the kill: {line}", at + 1);
                    }
                }
            }
            assert_ne!(
                publisher.bus.node(),
                leader,
                "the client sends to the killed leader; its last lines: {terms:?}"
            );
            assert!(
                publisher.reroutes > reroutes,
                "the client never turned away from the killed leader"
            );
            for (sequence, (base, count)) in &publisher.acked {
                assert_eq!(
                    (*base, *count),
                    (u64::from(*sequence), 1),
                    "in order, each once"
                );
            }

            // The stream from its start, at the leader: every batch once, in order, none twice.
            let mut wait = ProgressDeadline::begin(&[0], u64::MAX, FROZEN);
            let mut asked = false;
            while publisher.delivered.len() < TOTAL as usize {
                let now = Instant::now();
                if !asked && publisher.bus.ready(now) {
                    asked = publisher
                        .bus
                        .subscribe(
                            publisher.driver.endpoint(),
                            now,
                            STREAM,
                            wire::From::Offset(0),
                            SERVE_PERIOD,
                        )
                        .is_ok();
                }
                publisher.step().await;
                wait.check(&[publisher.delivered.len() as u64])
                    .expect("the stream was not delivered from its start");
            }
            assert_eq!(
                publisher.subscribed.map(|window| window.next),
                Some(u64::from(TOTAL)),
                "the stream holds each batch once"
            );
            for (offset, (base, sequence)) in publisher.delivered.iter().enumerate() {
                assert_eq!((*base, *sequence), (offset as u64, offset as u32));
            }
        })
        .unwrap();
}
