//! The simulation driver and runtime: virtual time, a seeded generator, kicks as flags, and fault
//! injection, so a whole set of shards runs deterministically on one thread (§4.3, D-20).
//!
//! Every simulated shard reads one clock, which the runtime sets on each shard's flags.
//! `SimRuntime::run_until_idle` steps each shard in turn; when all are idle it advances the clock to the
//! earliest deadline any shard asked for or the earliest datagram in flight, and stops when nothing is
//! pending. Fault injection: `kill_driver` makes a shard's next wait fail with `DriverLost`, which the
//! shard answers by cancelling every task with a terminal completion and exiting (T-0.7). The registry
//! owns each shard's flags; the runtime owns its shards, by value, and the fabric between them.
//!
//! The simulated UDP fabric below carries the fleet plane at N=1 and models the network under test
//! ([`SimPath`]): a one-way delay with seeded jitter (since 2026-09-14, so the consensus timing rules of
//! §4.8 are provable on a WAN profile, `docs/wip/wan-timeout.md`), and since 2026-09-27 a bottleneck link
//! with a drop-tail queue shared by the flows through it ([`SimLink`]), Gilbert–Elliott random and burst
//! loss ([`SimLoss`]), a path MTU, a bounded receive buffer, and a NAT whose mapping expires and rebinds
//! ([`SimNat`]) — every condition the session plane's constrained-link design is proven against
//! (`docs/wip/research/nfs-transport-constrained-links.md` §5, §9), deterministic from the seed.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::cells::CellStack;
use crate::driver::{Completion, Driver, DriverKind, DriverSeed, Kick};
use crate::error::RtError;
use crate::machine::stats::Xorshift;
use crate::runtime::RuntimeConfig;
use crate::shard::{Kept, ShardContext, ShardId, TaskId};
use crate::shard_loop::{Counters, Shard, ShardSeed, StepOutcome};
use crate::task::SpawnRequest;

/// Format: the "no deadline requested" sentinel.
const NO_DEADLINE: u64 = u64::MAX;

use std::collections::{BTreeMap, VecDeque};

use crate::mem::Encoded;

/// Format: parts per million, the unit every probability of the path model is stated in (an integer
/// so a profile is exact, comparable and replayable; one million is certainty).
pub const PPM: u32 = 1_000_000;

/// Format: nanoseconds per second, for the serialization time of a datagram at a link's bit rate.
const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// Format: bits per byte.
const BITS_PER_BYTE: u128 = 8;

/// A packet-loss process on a modelled path: the two-state Gilbert–Elliott channel [A: Gilbert, BSTJ
/// 1960; Elliott, BSTJ 1963], the standard model of both independent loss (one state) and bursty loss
/// (a "bad" state entered and left with the given per-datagram probabilities). Every probability is in
/// parts per million ([`PPM`]); the state is kept per directed flow and drawn from the fabric's seeded
/// generator, so a scenario replays exactly from its seed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SimLoss {
    /// Per-datagram probability of moving from the good state to the bad one.
    good_to_bad_ppm: u32,
    /// Per-datagram probability of moving from the bad state back to the good one.
    bad_to_good_ppm: u32,
    /// Loss probability of a datagram sent in the good state.
    good_loss_ppm: u32,
    /// Loss probability of a datagram sent in the bad state.
    bad_loss_ppm: u32,
}

impl SimLoss {
    /// No loss.
    pub const NONE: SimLoss = SimLoss {
        good_to_bad_ppm: 0,
        bad_to_good_ppm: PPM,
        good_loss_ppm: 0,
        bad_loss_ppm: 0,
    };

    /// Independent (Bernoulli) loss of `loss_ppm` per datagram.
    pub const fn random(loss_ppm: u32) -> SimLoss {
        SimLoss {
            good_to_bad_ppm: 0,
            bad_to_good_ppm: PPM,
            good_loss_ppm: loss_ppm,
            bad_loss_ppm: loss_ppm,
        }
    }

    /// Bursty loss: the channel enters a burst with probability `enter_ppm` per datagram, leaves it with
    /// `leave_ppm` (so a burst lasts `PPM / leave_ppm` datagrams on average), and loses `burst_loss_ppm`
    /// of the datagrams sent inside a burst and none outside it.
    pub const fn bursty(enter_ppm: u32, leave_ppm: u32, burst_loss_ppm: u32) -> SimLoss {
        SimLoss {
            good_to_bad_ppm: enter_ppm,
            bad_to_good_ppm: leave_ppm,
            good_loss_ppm: 0,
            bad_loss_ppm: burst_loss_ppm,
        }
    }

    /// Whether this process ever loses anything (so a loss-free path draws nothing from the generator).
    const fn is_lossless(&self) -> bool {
        self.good_loss_ppm == 0 && self.bad_loss_ppm == 0
    }
}

/// A draw that succeeds with probability `ppm` parts per million.
fn chance(rng: &mut Xorshift, ppm: u32) -> bool {
    let bound = usize::try_from(PPM).unwrap_or(usize::MAX);
    u32::try_from(rng.below(bound)).unwrap_or(PPM) < ppm
}

/// A bottleneck link on the simulated fabric: datagrams through it are serialized at its bit rate one
/// after another, and wait in its drop-tail queue of `queue_bytes` while it is busy — a datagram that
/// would overflow the queue is dropped (the congestion loss a sender's controller must react to, and the
/// standing queue whose delay bufferbloat is). Several directed paths may name one link, so their flows
/// compete for its capacity (the single-bottleneck "dumbbell" of congestion-control evaluation, RFC 5166).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SimLink {
    /// The link's capacity in bits per second.
    pub rate_bits_per_second: u64,
    /// The queue ahead of the link, in bytes; a datagram arriving when the backlog plus itself exceeds it
    /// is dropped.
    pub queue_bytes: u64,
}

/// A link on the fabric ([`sim_udp_add_link`], [`SimRuntime::add_link`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SimLinkId(u32);

/// A link's running state: its configuration and the instant its transmitter is next free.
#[derive(Debug)]
struct LinkState {
    link: SimLink,
    busy_until_ns: u64,
}

impl LinkState {
    /// The time `bytes` occupy the link at its rate, nanoseconds (rounded up, so a datagram always takes
    /// time on a finite link).
    fn serialization_ns(&self, bytes: usize) -> u64 {
        let rate = u128::from(self.link.rate_bits_per_second.max(1));
        let bits = (bytes as u128).saturating_mul(BITS_PER_BYTE);
        u64::try_from((bits.saturating_mul(NANOS_PER_SECOND)).div_ceil(rate)).unwrap_or(u64::MAX)
    }

    /// The bytes waiting ahead of a datagram arriving at `now` — the backlog the transmitter still has to
    /// send, from how long it stays busy at its rate.
    fn backlog_bytes(&self, now: u64) -> u64 {
        let busy_ns = u128::from(self.busy_until_ns.saturating_sub(now));
        let bits =
            busy_ns.saturating_mul(u128::from(self.link.rate_bits_per_second)) / NANOS_PER_SECOND;
        u64::try_from(bits / BITS_PER_BYTE).unwrap_or(u64::MAX)
    }
}

/// The model of one directed path on the simulated fabric (§4.8 A-9; §4.10a): a one-way propagation
/// delay with seeded jitter (in order per flow unless told it may reorder), an optional bottleneck
/// [`SimLink`] ahead of it, a [`SimLoss`] process, and an optional path MTU above which a datagram is
/// dropped (the black hole a path-MTU prober must find, RFC 8899). The fabric's default is the zero
/// path — delivered the instant it is sent, as the fabric always did — so a simulation that never sets
/// a profile runs unchanged. A profile is data describing the network under test, never a mode of the
/// code that runs over it (R8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SimPath {
    /// The one-way propagation delay of the path, nanoseconds.
    one_way_ns: u64,
    /// The half-width of the jitter, nanoseconds: each datagram's propagation is `one_way ± jitter`.
    jitter_ns: u64,
    /// Whether a datagram may overtake an earlier one on the same directed flow. `false` — an arrival is
    /// clamped to no earlier than the previous datagram's on that flow; `true` — the jitter alone decides.
    reorders: bool,
    /// The loss process.
    loss: SimLoss,
    /// The largest datagram the path carries; a larger one is dropped. `None` — no limit.
    mtu: Option<usize>,
    /// The bottleneck the path's datagrams are serialized through, if any.
    link: Option<SimLinkId>,
}

impl Default for SimPath {
    fn default() -> SimPath {
        SimPath::NONE
    }
}

impl SimPath {
    /// The zero path: delivered at once, nothing lost (the fabric's default).
    pub const NONE: SimPath = SimPath {
        one_way_ns: 0,
        jitter_ns: 0,
        reorders: false,
        loss: SimLoss::NONE,
        mtu: None,
        link: None,
    };

    /// A path of `one_way_ns ± jitter_ns` that keeps each flow in send order.
    pub const fn in_order(one_way_ns: u64, jitter_ns: u64) -> SimPath {
        SimPath {
            one_way_ns,
            jitter_ns,
            reorders: false,
            loss: SimLoss::NONE,
            mtu: None,
            link: None,
        }
    }

    /// A path of `one_way_ns ± jitter_ns` whose jitter may reorder a flow.
    pub const fn reordering(one_way_ns: u64, jitter_ns: u64) -> SimPath {
        SimPath {
            one_way_ns,
            jitter_ns,
            reorders: true,
            loss: SimLoss::NONE,
            mtu: None,
            link: None,
        }
    }

    /// This path, losing datagrams by `loss`.
    pub const fn with_loss(self, loss: SimLoss) -> SimPath {
        SimPath { loss, ..self }
    }

    /// This path, dropping every datagram longer than `mtu` bytes.
    pub const fn with_mtu(self, mtu: usize) -> SimPath {
        SimPath {
            mtu: Some(mtu),
            ..self
        }
    }

    /// This path, serialized through `link` before it propagates.
    pub const fn through(self, link: SimLinkId) -> SimPath {
        SimPath {
            link: Some(link),
            ..self
        }
    }

    /// The one-way delay this profile centres on.
    pub fn one_way_ns(&self) -> u64 {
        self.one_way_ns
    }

    /// The half-width of this profile's jitter.
    pub fn jitter_ns(&self) -> u64 {
        self.jitter_ns
    }

    /// One datagram's propagation time: `one_way − jitter + U[0, 2·jitter]` from `rng`, or exactly the
    /// one-way delay when there is no jitter (drawing nothing).
    fn draw(&self, rng: &mut Xorshift) -> u64 {
        if self.jitter_ns == 0 {
            return self.one_way_ns;
        }
        let span = self.jitter_ns.saturating_mul(2).saturating_add(1);
        let offset =
            u64::try_from(rng.below(usize::try_from(span).unwrap_or(usize::MAX))).unwrap_or(0);
        self.one_way_ns
            .saturating_sub(self.jitter_ns)
            .saturating_add(offset)
    }
}

/// A NAT in front of a fabric port (RFC 4787's endpoint-independent mapping): the port's outbound
/// datagrams leave from an external port the NAT allocates; inbound datagrams to that external port reach
/// the inside port only while the mapping is alive — refreshed by each outbound datagram and expired
/// after `idle_timeout_ns` without one. The next outbound datagram after an expiry allocates a fresh
/// external port: the address change a peer sees as a NAT rebinding (RFC 9000 §9.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SimNat {
    /// How long a mapping survives without outbound traffic, nanoseconds.
    pub idle_timeout_ns: u64,
}

/// A NAT's running state for one inside port.
#[derive(Debug)]
struct NatState {
    nat: SimNat,
    /// The current external port and the time of the last outbound datagram through it.
    mapping: Option<(u16, u64)>,
}

/// What the fabric did with the datagrams sent on it — the non-vacuity counters of every stress
/// scenario (a test of loss recovery asserts `dropped_loss` moved, of congestion `dropped_queue`, of
/// path-MTU discovery `dropped_mtu`, of migration `dropped_nat`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SimFabricStats {
    /// Datagrams handed to a receiver's mailbox.
    pub delivered: u64,
    /// Datagrams dropped because a link's queue was full.
    pub dropped_queue: u64,
    /// Datagrams dropped by a path's loss process.
    pub dropped_loss: u64,
    /// Datagrams dropped for exceeding a path's MTU.
    pub dropped_mtu: u64,
    /// Datagrams dropped at a NAT whose mapping had expired or never existed.
    pub dropped_nat: u64,
    /// Datagrams dropped because the receiver's buffer was full.
    pub dropped_receive_buffer: u64,
    /// Datagrams addressed to a port no socket is bound to.
    pub dropped_unbound: u64,
    /// The largest backlog any link's queue held, in bytes.
    pub peak_queue_bytes: u64,
}

/// A datagram the fabric holds until its arrival time.
#[derive(Debug)]
struct InFlight {
    dest: u16,
    bytes: Vec<u8>,
    from: u16,
}

/// A datagram the fabric hands to a port: its bytes, and the source port it carries.
#[derive(Debug)]
struct Arrival {
    port: u16,
    bytes: Vec<u8>,
    from: u16,
}

/// The simulated UDP fabric (slates §4.10a): a deterministic, in-memory datagram network, owned by its
/// [`SimRuntime`] (slates kept it in a thread-local `RefCell`). It routes and times datagrams; the sockets'
/// mailboxes are on their shards' desks ([`SimSockets`]), which the runtime fills from what the fabric hands
/// over.
///
/// Every send is timed against the simulation clock and the directed pair's [`SimPath`] (else the
/// fabric-wide one): a datagram is dropped past the path MTU, is serialized through its bottleneck link
/// (dropped if the link's queue is full), is lost by the path's loss process, then propagates. A datagram
/// whose arrival is now is handed over at once; a later one waits in flight, ordered by arrival, and is
/// handed over once the clock reaches it ([`SimRuntime::run_until_idle`] treats the earliest arrival as a
/// deadline the clock may advance to).
#[derive(Debug)]
pub struct SimFabric {
    /// The next external port a NAT may allocate: above every shard's socket ports.
    next_external: u16,
    /// The seeded generator the jitter and loss are drawn from — the fabric's own stream, so a profile's
    /// draws never perturb the shards' and a run replays exactly from its seed.
    rng: Xorshift,
    default_path: SimPath,
    pair_paths: BTreeMap<(u16, u16), SimPath>,
    links: BTreeMap<SimLinkId, LinkState>,
    loss_state: BTreeMap<(u16, u16), bool>,
    nats: BTreeMap<u16, NatState>,
    external: BTreeMap<u16, u16>,
    /// Every external port a NAT has allocated, live or expired, so a datagram to an expired mapping is
    /// counted as the NAT's drop. Bounded by the 16-bit port space.
    nat_ports: std::collections::BTreeSet<u16>,
    in_flight: BTreeMap<(u64, u64), InFlight>,
    next_sequence: u64,
    last_arrival: BTreeMap<(u16, u16), u64>,
    stats: SimFabricStats,
}

/// Format: the salt that separates the fabric's generator stream from the shards' (both are seeded from
/// the runtime seed; `xorshift64*` seeded identically would draw identical words), a fixed odd word.
const FABRIC_SEED_SALT: u64 = 0xD1B5_4A32_D192_ED03;

impl SimFabric {
    fn new(seed: u64, first_external: u16) -> SimFabric {
        SimFabric {
            next_external: first_external,
            rng: Xorshift::new(seed ^ FABRIC_SEED_SALT),
            default_path: SimPath::NONE,
            pair_paths: BTreeMap::new(),
            links: BTreeMap::new(),
            loss_state: BTreeMap::new(),
            nats: BTreeMap::new(),
            external: BTreeMap::new(),
            nat_ports: std::collections::BTreeSet::new(),
            in_flight: BTreeMap::new(),
            next_sequence: 0,
            last_arrival: BTreeMap::new(),
            stats: SimFabricStats::default(),
        }
    }

    /// The source port a datagram from `from` carries on the wire: `from` itself, or its NAT's current
    /// external port — allocating a fresh one when the mapping is absent or expired (a rebinding), and
    /// refreshing the mapping either way.
    fn translate_outbound(&mut self, from: u16, now: u64) -> u16 {
        let Some((idle_timeout_ns, mapping)) = self
            .nats
            .get(&from)
            .map(|state| (state.nat.idle_timeout_ns, state.mapping))
        else {
            return from;
        };
        let alive = mapping
            .filter(|(_, last)| now.saturating_sub(*last) <= idle_timeout_ns)
            .map(|(port, _)| port);
        let port = match alive {
            Some(port) => port,
            None => {
                if let Some((old, _)) = mapping {
                    self.external.remove(&old);
                }
                let fresh = self.next_external;
                self.next_external = self.next_external.saturating_add(1);
                self.external.insert(fresh, from);
                self.nat_ports.insert(fresh);
                fresh
            }
        };
        if let Some(state) = self.nats.get_mut(&from) {
            state.mapping = Some((port, now));
        }
        port
    }

    /// The inside port a datagram addressed to `dest` reaches at `now`: `dest` itself when no NAT owns it,
    /// the inside port behind a live mapping, or `None` when it names an expired or unknown mapping.
    fn translate_inbound(&self, dest: u16, now: u64) -> Option<u16> {
        let Some(&inside) = self.external.get(&dest) else {
            return (!self.nats.contains_key(&dest) && !self.nat_ports.contains(&dest))
                .then_some(dest);
        };
        let state = self.nats.get(&inside)?;
        let (port, last) = state.mapping?;
        (port == dest && now.saturating_sub(last) <= state.nat.idle_timeout_ns).then_some(inside)
    }

    fn path(&self, from: u16, dest: u16) -> SimPath {
        self.pair_paths
            .get(&(from, dest))
            .copied()
            .unwrap_or(self.default_path)
    }

    /// Whether the flow's loss process drops the next datagram, advancing its Gilbert–Elliott state.
    fn lose(&mut self, flow: (u16, u16), loss: SimLoss) -> bool {
        if loss.is_lossless() {
            return false;
        }
        let bad = self.loss_state.get(&flow).copied().unwrap_or(false);
        let next = if bad {
            !chance(&mut self.rng, loss.bad_to_good_ppm)
        } else {
            chance(&mut self.rng, loss.good_to_bad_ppm)
        };
        self.loss_state.insert(flow, next);
        chance(
            &mut self.rng,
            if next {
                loss.bad_loss_ppm
            } else {
                loss.good_loss_ppm
            },
        )
    }

    /// Sends a datagram from socket `from` to address port `dest` at `now`: hands it over at once when it
    /// arrives now, or schedules (or drops) it per the path.
    fn send(&mut self, now: u64, dest: u16, bytes: Vec<u8>, from: u16) -> Option<Arrival> {
        let wire_from = self.translate_outbound(from, now);
        let path_dest = self.external.get(&dest).copied().unwrap_or(dest);
        let path = self.path(from, path_dest);
        if path.mtu.is_some_and(|mtu| bytes.len() > mtu) {
            self.stats.dropped_mtu = self.stats.dropped_mtu.saturating_add(1);
            return None;
        }
        let mut departure = now;
        if let Some(id) = path.link
            && let Some(link) = self.links.get_mut(&id)
        {
            let backlog = link.backlog_bytes(now);
            let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if backlog.saturating_add(size) > link.link.queue_bytes {
                self.stats.dropped_queue = self.stats.dropped_queue.saturating_add(1);
                return None;
            }
            self.stats.peak_queue_bytes = self.stats.peak_queue_bytes.max(backlog);
            departure = link
                .busy_until_ns
                .max(now)
                .saturating_add(link.serialization_ns(bytes.len()));
            link.busy_until_ns = departure;
        }
        if self.lose((from, path_dest), path.loss) {
            self.stats.dropped_loss = self.stats.dropped_loss.saturating_add(1);
            return None;
        }
        let mut arrival = departure.saturating_add(path.draw(&mut self.rng));
        if !path.reorders
            && let Some(previous) = self.last_arrival.get(&(from, path_dest))
        {
            arrival = arrival.max(*previous);
        }
        self.last_arrival.insert((from, path_dest), arrival);
        if arrival <= now {
            return self.arrive(dest, bytes, wire_from, now);
        }
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.in_flight.insert(
            (arrival, sequence),
            InFlight {
                dest,
                bytes,
                from: wire_from,
            },
        );
        None
    }

    /// A datagram reaching address port `dest` at `now`: through a NAT's live mapping to its inside port.
    fn arrive(&mut self, dest: u16, bytes: Vec<u8>, from: u16, now: u64) -> Option<Arrival> {
        let Some(inside) = self.translate_inbound(dest, now) else {
            self.stats.dropped_nat = self.stats.dropped_nat.saturating_add(1);
            return None;
        };
        Some(Arrival {
            port: inside,
            bytes,
            from,
        })
    }

    /// Hands over every in-flight datagram whose arrival is at or before `now`, in arrival order.
    fn deliver_due(&mut self, now: u64, out: &mut Vec<Arrival>) {
        while let Some(entry) = self.in_flight.first_entry() {
            if entry.key().0 > now {
                break;
            }
            let arrival = entry.key().0;
            let InFlight { dest, bytes, from } = entry.remove();
            if let Some(handed) = self.arrive(dest, bytes, from, arrival) {
                out.push(handed);
            }
        }
    }

    /// The earliest arrival still in flight, if any — a deadline the simulation clock may advance to.
    fn earliest_arrival(&self) -> Option<u64> {
        self.in_flight.keys().next().map(|(arrival, _)| *arrival)
    }

    fn add_link(&mut self, id: SimLinkId, link: SimLink) {
        self.links.insert(
            id,
            LinkState {
                link,
                busy_until_ns: 0,
            },
        );
    }

    fn apply(&mut self, change: Scenario) {
        match change {
            Scenario::Path(path) => self.default_path = path,
            Scenario::PairPath { from, dest, path } => {
                self.pair_paths.insert((from, dest), path);
            }
            Scenario::AddLink { id, link } => self.add_link(id, link),
            Scenario::SetLink { id, link } => {
                if let Some(state) = self.links.get_mut(&id) {
                    state.link = link;
                }
            }
            Scenario::Nat { inside, nat } => {
                self.nats.insert(inside, NatState { nat, mapping: None });
            }
            Scenario::Rebind(inside) => {
                let old = self
                    .nats
                    .get_mut(&inside)
                    .and_then(|state| state.mapping.take());
                if let Some((port, _)) = old {
                    self.external.remove(&port);
                }
            }
        }
    }
}

/// A change to the modelled network a task asks for (applied by the runtime between steps, in order).
#[derive(Clone, Copy, Debug)]
enum Scenario {
    Path(SimPath),
    PairPath { from: u16, dest: u16, path: SimPath },
    AddLink { id: SimLinkId, link: SimLink },
    SetLink { id: SimLinkId, link: SimLink },
    Nat { inside: u16, nat: SimNat },
    Rebind(u16),
}

/// Shape: the receive buffer of a simulated datagram socket — what its mailbox holds before dropping, and
/// what `UdpSocket::recv_buffer_bytes` reports — the smaller of the default kernel datagram buffers on the
/// machines this runs on (Linux 208 KiB, macOS 768 KiB), so a consumer is sized and stressed as it would
/// be on the stricter host.
pub const SIM_RECV_BUFFER_BYTES: usize = 208 * 1024;

/// How a simulation's sockets are sized (configuration; docs/runtime.md §11).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SimConfig {
    /// Sockets each shard may have bound at once; past it a bind is refused `Capacity`.
    pub sockets_per_shard: u16,
}

impl Default for SimConfig {
    /// One socket per simulated host role a test builds (a node's transport, its datagram plane, a client) is
    /// a handful; sixty-four leaves room for a shard that plays many endpoints. An unbound socket costs a few
    /// words: its queues hold nothing until it receives or sends.
    fn default() -> Self {
        Self {
            sockets_per_shard: 64,
        }
    }
}

/// One simulated socket's desk side.
pub(crate) struct SimSocketCell {
    bound: Cell<bool>,
    /// The datagrams waiting, moved out and back in whole by each leaf operation (no reference into it
    /// escapes a call), bounded by `held` against [`SIM_RECV_BUFFER_BYTES`].
    mailbox: Cell<VecDeque<(Vec<u8>, u16)>>,
    held: Cell<usize>,
    read_interest: Cell<Option<(Encoded, crate::shard::Ticket)>>,
    write_interest: Cell<Option<(Encoded, crate::shard::Ticket)>>,
    send_blocked: Cell<bool>,
}

/// A datagram a simulated socket sent, waiting for the runtime to put it on the fabric.
pub(crate) struct SimSend {
    from: u16,
    dest: u16,
    bytes: Vec<u8>,
}

/// A simulated shard's sockets, on its desk: what its tasks' sockets read and write without reaching the
/// fabric, which the runtime owns.
pub(crate) struct SimSockets {
    port_base: u16,
    sockets: Box<[SimSocketCell]>,
    free: CellStack<u16>,
    /// What the shard's sockets sent since the runtime last drained, and the bytes it holds: bounded by
    /// every socket's receive buffer at once (DERIVED: no step can usefully send more than the receivers
    /// could hold), past which a send would block.
    outbound: Cell<Vec<SimSend>>,
    outbound_bytes: Cell<usize>,
    /// The network changes the shard's tasks asked for since the runtime last drained: at most one per
    /// directed pair of its sockets (DERIVED), past which a change is dropped and counted.
    scenario: Cell<Vec<Scenario>>,
    /// The sending host's interface MTU, when one is modelled: a larger send is refused `EMSGSIZE` at once.
    interface_mtu: Cell<Option<usize>>,
    /// Links this shard's tasks added, counted so each gets the id the fabric will give it.
    links_added: Cell<u32>,
}

impl SimSockets {
    fn new(port_base: u16, config: SimConfig) -> Self {
        let count = usize::from(config.sockets_per_shard);
        Self {
            port_base,
            sockets: (0..count)
                .map(|_| SimSocketCell {
                    bound: Cell::new(false),
                    mailbox: Cell::new(VecDeque::new()),
                    held: Cell::new(0),
                    read_interest: Cell::new(None),
                    write_interest: Cell::new(None),
                    send_blocked: Cell::new(false),
                })
                .collect(),
            free: CellStack::full_of((0..config.sockets_per_shard).rev()),
            outbound: Cell::new(Vec::new()),
            outbound_bytes: Cell::new(0),
            scenario: Cell::new(Vec::new()),
            interface_mtu: Cell::new(None),
            links_added: Cell::new(0),
        }
    }

    fn socket(&self, index: u16) -> Option<&SimSocketCell> {
        self.sockets.get(usize::from(index))
    }

    /// The socket a port names on this shard.
    fn index_of(&self, port: u16) -> Option<u16> {
        let index = port.checked_sub(self.port_base)?;
        (usize::from(index) < self.sockets.len()).then_some(index)
    }

    /// The port of socket `index`.
    fn port_of(&self, index: u16) -> u16 {
        self.port_base.saturating_add(index)
    }

    /// The bytes every socket's receive buffer holds at once: the send queue's bound.
    fn outbound_bound(&self) -> usize {
        self.sockets.len().saturating_mul(SIM_RECV_BUFFER_BYTES)
    }

    /// One change per directed pair of this shard's sockets: the scenario queue's bound.
    fn scenario_bound(&self) -> usize {
        self.sockets.len().saturating_mul(self.sockets.len()).max(1)
    }

    fn push_scenario(&self, change: Scenario) {
        let mut queue = self.scenario.take();
        if queue.len() < self.scenario_bound() {
            queue.push(change);
        }
        self.scenario.set(queue);
    }
}

impl SimSocketCell {
    fn push(&self, datagram: (Vec<u8>, u16)) {
        let mut mailbox = self.mailbox.take();
        mailbox.push_back(datagram);
        self.mailbox.set(mailbox);
    }

    fn pop(&self) -> Option<(Vec<u8>, u16)> {
        let mut mailbox = self.mailbox.take();
        let datagram = mailbox.pop_front();
        self.mailbox.set(mailbox);
        datagram
    }

    fn is_empty(&self) -> bool {
        let mailbox = self.mailbox.take();
        let empty = mailbox.is_empty();
        self.mailbox.set(mailbox);
        empty
    }
}

/// The simulated sockets of the shard running on this thread.
fn with_sockets<R>(f: impl FnOnce(&ShardContext, &SimSockets) -> R) -> Option<R> {
    crate::registry::with_current(|desk| desk.sim.as_ref().map(|sockets| f(desk, sockets)))
        .flatten()
}

/// Binds a simulated socket on the current shard: its index and port.
pub(crate) fn sim_bind() -> Result<(u16, u16), RtError> {
    with_sockets(|_, sockets| {
        let index = sockets.free.pop().ok_or(RtError::Capacity {
            what: "simulated sockets",
            bound: sockets.sockets.len(),
        })?;
        if let Some(socket) = sockets.socket(index) {
            socket.bound.set(true);
            socket.held.set(0);
            socket.send_blocked.set(false);
        }
        Ok((index, sockets.port_of(index)))
    })
    .unwrap_or(Err(RtError::NotOnShardThread))
}

/// Closes a simulated socket: its mailbox emptied and its index given back.
pub(crate) fn sim_close(index: u16) {
    let _ = with_sockets(|desk, sockets| {
        let Some(socket) = sockets.socket(index) else {
            return;
        };
        if !socket.bound.replace(false) {
            return;
        }
        drop(socket.mailbox.take());
        socket.held.set(0);
        // A wait on a closed socket ends: fired, its call then sees the socket gone.
        for interest in [socket.read_interest.take(), socket.write_interest.take()]
            .into_iter()
            .flatten()
        {
            desk.fire_wait(interest.1, interest.0);
        }
        let _ = sockets.free.push(index);
    });
}

/// Sends a datagram from simulated socket `index` to port `dest`: the bytes accepted, or `None` when the
/// scenario blocked the socket's sends (local pressure) or this step's send queue is full.
pub(crate) fn sim_send(index: u16, dest: u16, bytes: &[u8]) -> Result<Option<usize>, RtError> {
    with_sockets(|_, sockets| {
        if sockets
            .interface_mtu
            .get()
            .is_some_and(|mtu| bytes.len() > mtu)
        {
            return Err(RtError::message_too_large("sendto"));
        }
        if sockets
            .socket(index)
            .is_some_and(|socket| socket.send_blocked.get())
        {
            return Ok(None);
        }
        let queued = sockets.outbound_bytes.get();
        if queued.saturating_add(bytes.len()) > sockets.outbound_bound() {
            return Ok(None);
        }
        let mut outbound = sockets.outbound.take();
        outbound.push(SimSend {
            from: sockets.port_of(index),
            dest,
            bytes: bytes.to_vec(),
        });
        sockets.outbound.set(outbound);
        sockets
            .outbound_bytes
            .set(queued.saturating_add(bytes.len()));
        Ok(Some(bytes.len()))
    })
    .unwrap_or(Err(RtError::NotOnShardThread))
}

/// Takes one waiting datagram for simulated socket `index` into `buf` (truncated to it): its length and
/// source port.
pub(crate) fn sim_recv(index: u16, buf: &mut [u8]) -> Option<(usize, u16)> {
    with_sockets(|_, sockets| {
        let socket = sockets.socket(index)?;
        let (bytes, from) = socket.pop()?;
        socket
            .held
            .set(socket.held.get().saturating_sub(bytes.len()));
        let n = bytes.len().min(buf.len());
        if let (Some(into), Some(from_bytes)) = (buf.get_mut(..n), bytes.get(..n)) {
            into.copy_from_slice(from_bytes);
        }
        Some((n, from))
    })
    .flatten()
}

/// Registers one-shot interest in simulated socket `index`: readable when a datagram waits, writable when
/// its sends are not blocked. Ready now wakes the task at once.
pub(crate) fn sim_register(
    index: u16,
    writable: bool,
    word: Encoded,
    ticket: crate::shard::Ticket,
) -> Result<(), RtError> {
    with_sockets(|desk, sockets| {
        let socket = sockets.socket(index).ok_or(RtError::Capacity {
            what: "simulated sockets",
            bound: sockets.sockets.len(),
        })?;
        let ready = if writable {
            !socket.send_blocked.get()
        } else {
            !socket.is_empty()
        };
        if ready {
            desk.fire_wait(ticket, word);
            return Ok(());
        }
        let cell = if writable {
            &socket.write_interest
        } else {
            &socket.read_interest
        };
        // One waiter per direction in the model: a second one fires the first, whose call retries and
        // finds the socket as it is (a simulated socket has one reader and one writer in every scenario).
        if let Some((earlier, earlier_ticket)) = cell.replace(Some((word, ticket))) {
            desk.fire_wait(earlier_ticket, earlier);
        }
        Ok(())
    })
    .unwrap_or(Err(RtError::NotOnShardThread))
}

/// Queues a scenario change on the current shard; the runtime applies it before the next step.
fn scenario(change: Scenario) {
    let _ = with_sockets(|_, sockets| sockets.push_scenario(change));
}

/// Sets the path of every directed pair that has no override, from now on.
pub fn sim_udp_set_path(path: SimPath) {
    scenario(Scenario::Path(path));
}

/// Models every sending host's interface MTU from now on (`None` removes it): a datagram larger than `mtu`
/// is refused at the send with the OS's too-large code (`RtError::is_message_too_large`), as a real host
/// with don't-fragment set refuses it (RFC 8899 §4.4). An Ethernet host is 1,500.
pub fn sim_udp_set_interface_mtu(mtu: Option<usize>) {
    let _ = with_sockets(|_, sockets| sockets.interface_mtu.set(mtu));
}

/// Sets the path from socket port `from` to socket port `dest`, overriding the default for that directed
/// pair only.
pub fn sim_udp_set_pair_path(from: u16, dest: u16, path: SimPath) {
    scenario(Scenario::PairPath { from, dest, path });
}

/// Adds a bottleneck link; paths name it with [`SimPath::through`]. Its id is the adding shard's (its first
/// port, in the high half) and its count of links so far, so links added on different shards never share
/// one.
pub fn sim_udp_add_link(link: SimLink) -> SimLinkId {
    with_sockets(|_, sockets| {
        let count = sockets.links_added.get();
        sockets.links_added.set(count.saturating_add(1));
        let id = SimLinkId((u32::from(sockets.port_base) << 16) | (count & 0xFFFF));
        sockets.push_scenario(Scenario::AddLink { id, link });
        id
    })
    .unwrap_or(SimLinkId(u32::MAX))
}

/// Changes a link's rate and queue from now on.
pub fn sim_udp_set_link(id: SimLinkId, link: SimLink) {
    scenario(Scenario::SetLink { id, link });
}

/// Puts a NAT in front of socket port `inside`.
pub fn sim_udp_set_nat(inside: u16, nat: SimNat) {
    scenario(Scenario::Nat { inside, nat });
}

/// Expires the NAT mapping in front of `inside` now, so its next datagram leaves from a fresh external port.
pub fn sim_udp_rebind(inside: u16) {
    scenario(Scenario::Rebind(inside));
}

/// Blocks socket port `port`'s sends from now on (local send pressure, AUD-29-61), until
/// [`sim_udp_release_sends`]. The port must be one of the current shard's.
pub fn sim_udp_block_sends(port: u16) {
    let _ = with_sockets(|_, sockets| {
        if let Some(socket) = sockets
            .index_of(port)
            .and_then(|index| sockets.socket(index))
        {
            socket.send_blocked.set(true);
        }
    });
}

/// Releases socket port `port`'s sends and wakes a sender waiting for its writability. The port must be
/// one of the current shard's.
pub fn sim_udp_release_sends(port: u16) {
    let _ = with_sockets(|desk, sockets| {
        if let Some(socket) = sockets
            .index_of(port)
            .and_then(|index| sockets.socket(index))
        {
            socket.send_blocked.set(false);
            if let Some((word, ticket)) = socket.write_interest.take() {
                desk.fire_wait(ticket, word);
            }
        }
    });
}

/// The state a simulated driver shares with its kick and the simulation clock: the shard's own, owned by
/// its registry slot.
#[derive(Debug)]
pub struct SimShared {
    now_ns: AtomicU64,
    kicked: AtomicBool,
    requested_deadline: AtomicU64,
    kill_at_wait: AtomicU64,
    waits: AtomicU64,
}

impl SimShared {
    fn new() -> Self {
        Self {
            now_ns: AtomicU64::new(0),
            kicked: AtomicBool::new(false),
            requested_deadline: AtomicU64::new(NO_DEADLINE),
            kill_at_wait: AtomicU64::new(NO_DEADLINE),
            waits: AtomicU64::new(0),
        }
    }

    /// Marks the driver kicked.
    pub fn set_kicked(&self) {
        self.kicked.store(true, Ordering::Release);
    }

    /// Virtual now.
    pub fn now_ns(&self) -> u64 {
        self.now_ns.load(Ordering::Acquire)
    }

    fn requested_deadline(&self) -> Option<u64> {
        match self.requested_deadline.load(Ordering::Acquire) {
            NO_DEADLINE => None,
            d => Some(d),
        }
    }
}

/// The simulation driver of one shard: virtual time and kicks as flags, never blocking.
#[derive(Debug)]
pub struct SimDriver {
    kick: Kick,
    shared: &'static SimShared,
    nops: Vec<u64>,
}

impl Driver for SimDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::Simulation
    }

    fn kick_handle(&self) -> Kick {
        self.kick
    }

    fn now_ns(&self) -> u64 {
        self.shared.now_ns()
    }

    fn clock(&self) -> crate::driver::Clock {
        crate::driver::Clock::Sim(self.shared)
    }

    fn wait(&mut self, timeout_ns: Option<u64>, out: &mut Vec<Completion>) -> Result<(), RtError> {
        let waits = self
            .shared
            .waits
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        if waits >= self.shared.kill_at_wait.load(Ordering::Acquire) {
            return Err(RtError::DriverLost);
        }
        out.extend(self.nops.drain(..).map(|user_data| Completion {
            user_data,
            result: 0,
        }));
        if self.shared.kicked.swap(false, Ordering::AcqRel) || !out.is_empty() {
            self.shared
                .requested_deadline
                .store(NO_DEADLINE, Ordering::Release);
            return Ok(());
        }
        // Nothing to deliver: record the deadline for the simulation clock and return without blocking; the
        // runtime advances time when every shard is idle.
        let deadline = timeout_ns.map_or(NO_DEADLINE, |t| self.now_ns().saturating_add(t));
        self.shared
            .requested_deadline
            .store(deadline, Ordering::Release);
        Ok(())
    }

    fn submit_nop(&mut self, user_data: u64) -> Result<(), RtError> {
        self.nops.push(user_data);
        Ok(())
    }

    fn arm(
        &mut self,
        _raw: i32,
        _want: crate::interests::Readiness,
        _tag: u64,
    ) -> Result<(), RtError> {
        // Simulated sockets register their interest on the desk (`sim_register`); no OS handle exists here.
        Err(RtError::DriverRefused {
            call: "arm on the simulation driver",
            code: None,
        })
    }

    fn has_pending(&self) -> bool {
        !self.nops.is_empty() || self.shared.kicked.load(Ordering::Acquire)
    }

    fn is_sim(&self) -> bool {
        true
    }
}

/// One simulated shard: the shard, its flags, and the guard that gives its registry slot back after it.
struct SimShard {
    /// Declared before `slot`: dropped while the slot (and the flags the driver borrows) still exists.
    shard: Shard,
    shared: &'static SimShared,
    /// Gives the slot back when dropped, after `shard`.
    _slot: crate::runtime::SlotGuard,
}

/// A set of simulated shards on the calling thread, with the network between them.
pub struct SimRuntime {
    shards: Vec<SimShard>,
    fabric: SimFabric,
    now_ns: u64,
    arrivals: Vec<Arrival>,
    links_added: u32,
}

impl std::fmt::Debug for SimRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimRuntime")
            .field("shards", &self.shards.len())
            .finish()
    }
}

impl SimRuntime {
    /// Builds `config.shards` simulated shards sharing one clock and one fabric seeded by `seed`, each with
    /// the default socket table ([`SimConfig`]).
    pub fn new(config: &RuntimeConfig, seed: u64) -> Result<SimRuntime, RtError> {
        Self::with(config, seed, SimConfig::default())
    }

    /// Builds the simulation with `sim` sizing its sockets.
    pub fn with(config: &RuntimeConfig, seed: u64, sim: SimConfig) -> Result<SimRuntime, RtError> {
        config.validate_shape()?;
        let per_shard = sim.sockets_per_shard;
        let mut shards = Vec::new();
        for index in 0..config.shards {
            // Ports start at 1 so 0 stays the "unspecified" address; each shard's sockets take the next range.
            let port_base = u16::try_from(
                usize::from(index)
                    .saturating_mul(usize::from(per_shard))
                    .saturating_add(1),
            )
            .map_err(|_| RtError::BadConfig {
                what: "simulated ports past the 16-bit port space",
            })?;
            let driver: DriverSeed = Box::new(move |kick| match kick {
                Kick::Sim(holder) => {
                    let flags = crate::registry::entry(holder.shard())
                        .and_then(|entry| entry.sim_shared.as_deref())
                        .ok_or(RtError::ShardGone {
                            shard: holder.shard(),
                        })?;
                    Ok(Box::new(SimDriver {
                        kick,
                        shared: flags,
                        nops: Vec::new(),
                    }) as Box<dyn Driver>)
                }
                _ => Err(RtError::DriverRefused {
                    call: "a simulated shard registered without simulated flags",
                    code: None,
                }),
            });
            let seed = ShardSeed::register(
                config,
                driver,
                crate::registry::RegisterKick::Sim(Box::new(SimShared::new())),
            )?;
            let id = seed.id;
            let slot = crate::runtime::SlotGuard::new(seed.holder);
            let shared = crate::registry::entry(id)
                .and_then(|entry| entry.sim_shared.as_deref())
                .ok_or(RtError::ShardGone { shard: id })?;
            let mut shard = Shard::build(seed)?;
            shard.attach_sim(SimSockets::new(port_base, sim));
            shards.push(SimShard {
                shard,
                shared,
                _slot: slot,
            });
        }
        let first_external = u16::try_from(
            usize::from(config.shards)
                .saturating_mul(usize::from(per_shard))
                .saturating_add(1),
        )
        .unwrap_or(u16::MAX);
        Ok(SimRuntime {
            shards,
            fabric: SimFabric::new(seed, first_external),
            now_ns: 0,
            arrivals: Vec::new(),
            links_added: 0,
        })
    }

    /// The shard ids, in order.
    pub fn shard_ids(&self) -> Vec<ShardId> {
        self.shards.iter().map(|s| s.shard.id()).collect()
    }

    /// Virtual now.
    pub fn now_ns(&self) -> u64 {
        self.now_ns
    }

    /// Spawns a detached task on a shard.
    pub fn spawn_on(
        &mut self,
        shard: ShardId,
        future: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<TaskId, RtError> {
        self.shard_mut(shard)?
            .shard
            .spawn_request(SpawnRequest::new(Box::pin(future), None))
    }

    /// Keeps `value` for a shard's life and hands back its handle.
    pub fn keep<T: 'static>(&mut self, shard: ShardId, value: T) -> Result<Kept<T>, RtError> {
        self.shard_mut(shard)?.shard.keep(value)
    }

    /// Makes a shard's driver fail at its `nth` wait from now (1 = the very next one).
    pub fn kill_driver(&mut self, shard: ShardId, nth: u64) -> Result<(), RtError> {
        let shared = self.shard_mut(shard)?.shared;
        shared.kill_at_wait.store(
            shared.waits.load(Ordering::Acquire).saturating_add(nth),
            Ordering::Release,
        );
        Ok(())
    }

    /// Steps every shard until none has work, advancing virtual time to the earliest deadline — a shard's
    /// timer or a datagram's arrival — whenever all are idle. What the shards sent is put on the fabric after
    /// each shard's step, and datagrams due by the current time are handed over (and their receivers woken)
    /// before each pass. Returns the number of steps taken.
    pub fn run_until_idle(&mut self) -> u64 {
        let mut steps = 0u64;
        loop {
            self.pump();
            let (any_work, mut earliest, taken) = self.step_all();
            steps = steps.saturating_add(taken);
            if any_work {
                continue;
            }
            if let Some(arrival) = self.fabric.earliest_arrival() {
                earliest = Some(earliest.map_or(arrival, |e| e.min(arrival)));
            }
            match earliest {
                Some(deadline) if deadline > self.now_ns => self.set_clock(deadline),
                Some(_) => {}
                None => break,
            }
        }
        steps
    }

    /// One pass over every live shard: a step each, a park for each that had nothing to do, and what each
    /// sent put on the fabric; whether any did work or was kicked, the earliest deadline the idle ones asked
    /// for, and the steps taken.
    fn step_all(&mut self) -> (bool, Option<u64>, u64) {
        let mut any_work = false;
        let mut earliest: Option<u64> = None;
        let mut steps = 0u64;
        for index in 0..self.shards.len() {
            let Some(sim_shard) = self.shards.get_mut(index) else {
                continue;
            };
            if sim_shard.shard.exited() {
                continue;
            }
            let outcome: StepOutcome = sim_shard.shard.step();
            steps = steps.saturating_add(1);
            if outcome.did_work {
                any_work = true;
            } else {
                sim_shard.shard.park(outcome.next_deadline_ns);
                if let Some(deadline) = sim_shard.shared.requested_deadline() {
                    earliest = Some(earliest.map_or(deadline, |e| e.min(deadline)));
                }
                any_work |= sim_shard.shared.kicked.load(Ordering::Acquire);
            }
            any_work |= self.pump();
        }
        (any_work, earliest, steps)
    }

    /// Moves what the shards asked of the network onto the fabric and hands over what is due; true when a
    /// receiver was woken.
    fn pump(&mut self) -> bool {
        let now = self.now_ns;
        let mut arrivals = std::mem::take(&mut self.arrivals);
        for sim_shard in &self.shards {
            let Some(sockets) = sim_shard.shard.context().sim.as_ref() else {
                continue;
            };
            let mut changes = sockets.scenario.take();
            for change in changes.drain(..) {
                self.fabric.apply(change);
            }
            sockets.scenario.set(changes);
            let mut sends = sockets.outbound.take();
            sockets.outbound_bytes.set(0);
            for send in sends.drain(..) {
                if let Some(arrival) = self.fabric.send(now, send.dest, send.bytes, send.from) {
                    arrivals.push(arrival);
                }
            }
            sockets.outbound.set(sends);
        }
        self.fabric.deliver_due(now, &mut arrivals);
        let mut woke = false;
        for arrival in arrivals.drain(..) {
            woke |= self.deliver(arrival);
        }
        self.arrivals = arrivals;
        woke
    }

    /// Puts an arrival in its socket's mailbox within its bounds and wakes a waiting receiver.
    fn deliver(&mut self, arrival: Arrival) -> bool {
        let Arrival { port, bytes, from } = arrival;
        let target = self.shards.iter().find_map(|sim_shard| {
            let desk = sim_shard.shard.context();
            let sockets = desk.sim.as_ref()?;
            let socket = sockets.socket(sockets.index_of(port)?)?;
            Some((desk, socket))
        });
        let Some((desk, socket)) = target.filter(|(_, socket)| socket.bound.get()) else {
            self.fabric.stats.dropped_unbound = self.fabric.stats.dropped_unbound.saturating_add(1);
            return false;
        };
        let held = socket.held.get();
        if held.saturating_add(bytes.len()) > SIM_RECV_BUFFER_BYTES {
            self.fabric.stats.dropped_receive_buffer =
                self.fabric.stats.dropped_receive_buffer.saturating_add(1);
            return false;
        }
        socket.held.set(held.saturating_add(bytes.len()));
        socket.push((bytes, from));
        self.fabric.stats.delivered = self.fabric.stats.delivered.saturating_add(1);
        match socket.read_interest.take() {
            Some((word, ticket)) => desk.fire_wait(ticket, word),
            None => false,
        }
    }

    fn set_clock(&mut self, now_ns: u64) {
        self.now_ns = now_ns;
        for sim_shard in &self.shards {
            sim_shard.shared.now_ns.store(now_ns, Ordering::Release);
        }
    }

    /// Advances the clock by `ns` without running anything (a pause in the story).
    pub fn advance(&mut self, ns: u64) {
        self.set_clock(self.now_ns.saturating_add(ns));
    }

    /// A shard's desk, for reads in tests.
    pub fn context(&self, shard: ShardId) -> Result<&ShardContext, RtError> {
        Ok(self.shard_ref(shard)?.shard.context())
    }

    /// A shard's counters.
    pub fn counters(&self, shard: ShardId) -> Result<Counters, RtError> {
        Ok(self.shard_ref(shard)?.shard.counters())
    }

    /// One step of a shard without blocking.
    pub fn step(&mut self, shard: ShardId) -> Result<StepOutcome, RtError> {
        Ok(self.shard_mut(shard)?.shard.step())
    }

    /// Whether a shard left its loop.
    pub fn exited(&self, shard: ShardId) -> Result<bool, RtError> {
        Ok(self.shard_ref(shard)?.shard.exited())
    }

    /// Live tasks on a shard.
    pub fn live_tasks(&self, shard: ShardId) -> Result<usize, RtError> {
        Ok(self.shard_ref(shard)?.shard.live_tasks())
    }

    /// What the fabric has done so far.
    pub fn stats(&self) -> SimFabricStats {
        self.fabric.stats
    }

    /// Sets the path of every directed pair that has no override, from now on.
    pub fn set_path(&mut self, path: SimPath) {
        self.fabric.apply(Scenario::Path(path));
    }

    /// Sets one directed pair's path.
    pub fn set_pair_path(&mut self, from: u16, dest: u16, path: SimPath) {
        self.fabric.apply(Scenario::PairPath { from, dest, path });
    }

    /// Adds a bottleneck link from the test's thread: ids above every shard's (port 0 is no shard's, so its
    /// high half is free), counted from zero.
    pub fn add_link(&mut self, link: SimLink) -> SimLinkId {
        let id = SimLinkId(self.links_added);
        self.links_added = self.links_added.saturating_add(1);
        self.fabric.add_link(id, link);
        id
    }

    /// Models every shard's interface MTU from now on.
    pub fn set_interface_mtu(&mut self, mtu: Option<usize>) {
        for sim_shard in &self.shards {
            if let Some(sockets) = sim_shard.shard.context().sim.as_ref() {
                sockets.interface_mtu.set(mtu);
            }
        }
    }

    fn shard_ref(&self, shard: ShardId) -> Result<&SimShard, RtError> {
        self.shards
            .iter()
            .find(|s| s.shard.id() == shard)
            .ok_or(RtError::NotOnShardThread)
    }

    fn shard_mut(&mut self, shard: ShardId) -> Result<&mut SimShard, RtError> {
        self.shards
            .iter_mut()
            .find(|s| s.shard.id() == shard)
            .ok_or(RtError::NotOnShardThread)
    }
}
