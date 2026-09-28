//! A modelled network path between nodes (27 §3.1 P7): propagation delay
//! with jitter, a loss process, a bottleneck link with a drop-tail queue, a
//! path MTU and a NAT whose mapping expires. A [`Fabric`] decides each
//! message's fate from its path and a seed, and schedules what survives on a
//! [`Network`]; a scenario replays exactly from its seed.
//!
//! A profile is data describing the network under test, never a mode of the
//! code that runs over it. The zero path delivers at once and loses nothing.
use crate::{
    Seeded,
    network::{Delivery, Network, NetworkError},
};
use std::collections::{BTreeMap, VecDeque};

/// Parts per million: the unit of every probability here, so a profile is
/// exact, comparable and replayable.
pub const PPM: u32 = 1_000_000;
const NANOS_PER_SECOND: u128 = 1_000_000_000;
const BITS_PER_BYTE: u128 = 8;
const MILLISECOND: u64 = 1_000_000;

/// The two-state Gilbert–Elliott channel (Gilbert, BSTJ 1960; Elliott, BSTJ
/// 1963): independent loss with one state, bursty loss with a bad state
/// entered and left per message. State is kept per directed flow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Loss {
    good_to_bad_ppm: u32,
    bad_to_good_ppm: u32,
    good_loss_ppm: u32,
    bad_loss_ppm: u32,
}
impl Default for Loss {
    fn default() -> Self {
        Self::NONE
    }
}
impl Loss {
    pub const NONE: Self = Self {
        good_to_bad_ppm: 0,
        bad_to_good_ppm: PPM,
        good_loss_ppm: 0,
        bad_loss_ppm: 0,
    };
    /// Independent (Bernoulli) loss of `loss_ppm` per message.
    pub const fn random(loss_ppm: u32) -> Self {
        Self {
            good_to_bad_ppm: 0,
            bad_to_good_ppm: PPM,
            good_loss_ppm: loss_ppm,
            bad_loss_ppm: loss_ppm,
        }
    }
    /// A burst is entered with `enter_ppm` per message and left with
    /// `leave_ppm`, so it lasts `PPM / leave_ppm` messages on average; it
    /// loses `burst_loss_ppm` of what is sent inside it and nothing outside.
    pub const fn bursty(enter_ppm: u32, leave_ppm: u32, burst_loss_ppm: u32) -> Self {
        Self {
            good_to_bad_ppm: enter_ppm,
            bad_to_good_ppm: leave_ppm,
            good_loss_ppm: 0,
            bad_loss_ppm: burst_loss_ppm,
        }
    }
    /// A lossless path draws nothing from the generator.
    const fn is_lossless(&self) -> bool {
        self.good_loss_ppm == 0 && self.bad_loss_ppm == 0
    }
}
fn chance(random: &mut Seeded, ppm: u32) -> bool {
    random.below(u64::from(PPM)) < u64::from(ppm)
}

/// A bottleneck: messages are serialized at its rate one after another and
/// wait in its drop-tail queue while it is busy. Several paths may name one
/// link, so their flows compete for it (the dumbbell of RFC 5166).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Link {
    pub rate_bits_per_second: u64,
    /// A message arriving when the backlog plus itself exceeds this is
    /// dropped.
    pub queue_bytes: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LinkId(usize);
/// A message on a link: when its transmission starts and ends, and its size.
#[derive(Debug, Clone, Copy)]
struct Queued {
    starts: u64,
    departs: u64,
    bytes: u64,
}
#[derive(Debug)]
struct LinkState {
    link: Link,
    busy_until: u64,
    /// What the transmitter has accepted and not finished sending, in
    /// order. Each keeps the timing of the rate it was accepted at.
    queued: VecDeque<Queued>,
}
impl LinkState {
    /// Rounded up, so a message always takes time on a finite link.
    fn serialization_ns(&self, bytes: usize) -> u64 {
        let rate = u128::from(self.link.rate_bits_per_second.max(1));
        let bits = u128::try_from(bytes)
            .unwrap_or(u128::MAX)
            .saturating_mul(BITS_PER_BYTE);
        u64::try_from(bits.saturating_mul(NANOS_PER_SECOND).div_ceil(rate)).unwrap_or(u64::MAX)
    }
    /// What the transmitter still has to send at `now`: every message
    /// waiting, and the part of the one being sent that has not left.
    fn backlog_bytes(&mut self, now: u64) -> u64 {
        while self.queued.front().is_some_and(|head| head.departs <= now) {
            self.queued.pop_front();
        }
        self.queued.iter().fold(0_u64, |backlog, message| {
            let whole = message.departs.saturating_sub(message.starts);
            let left = message.departs.saturating_sub(now.max(message.starts));
            let bytes = u128::from(message.bytes)
                .saturating_mul(u128::from(left))
                .checked_div(u128::from(whole))
                .unwrap_or(0);
            backlog.saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX))
        })
    }
}

/// One directed path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Path {
    one_way_ns: u64,
    /// Each message propagates in `one_way ± jitter`.
    jitter_ns: u64,
    /// Whether a message may overtake an earlier one on its flow.
    reorders: bool,
    loss: Loss,
    /// A longer message is dropped: the black hole of RFC 8899.
    mtu: Option<usize>,
    link: Option<LinkId>,
}
impl Default for Path {
    fn default() -> Self {
        Self::NONE
    }
}
impl Path {
    pub const NONE: Self = Self::in_order(0, 0);
    /// One switch apart: 0.2 ms ± 0.1 ms one way.
    pub const LAN: Self = Self::in_order(200_000, 100_000);
    /// One continent: 80 ms ± 20 ms one way.
    pub const REGIONAL: Self = Self::in_order(80 * MILLISECOND, 20 * MILLISECOND);
    /// Across the planet on a poor route: 500 ms ± 100 ms one way.
    pub const GEOGRAPHIC: Self = Self::in_order(500 * MILLISECOND, 100 * MILLISECOND);

    pub const fn in_order(one_way_ns: u64, jitter_ns: u64) -> Self {
        Self {
            one_way_ns,
            jitter_ns,
            reorders: false,
            loss: Loss::NONE,
            mtu: None,
            link: None,
        }
    }
    pub const fn reordering(one_way_ns: u64, jitter_ns: u64) -> Self {
        Self {
            reorders: true,
            ..Self::in_order(one_way_ns, jitter_ns)
        }
    }
    pub const fn with_loss(self, loss: Loss) -> Self {
        Self { loss, ..self }
    }
    pub const fn with_mtu(self, mtu: usize) -> Self {
        Self {
            mtu: Some(mtu),
            ..self
        }
    }
    pub const fn through(self, link: LinkId) -> Self {
        Self {
            link: Some(link),
            ..self
        }
    }
    pub const fn one_way_ns(&self) -> u64 {
        self.one_way_ns
    }
    pub const fn jitter_ns(&self) -> u64 {
        self.jitter_ns
    }
    /// `one_way − jitter + U[0, 2·jitter]`; nothing is drawn without jitter.
    fn draw(&self, random: &mut Seeded) -> u64 {
        if self.jitter_ns == 0 {
            return self.one_way_ns;
        }
        let span = self.jitter_ns.saturating_mul(2).saturating_add(1);
        self.one_way_ns
            .saturating_sub(self.jitter_ns)
            .saturating_add(random.below(span))
    }
}

/// A NAT in front of a node (RFC 4787, endpoint-independent mapping): what
/// is sent to the node arrives only while its mapping is alive, which each
/// message the node sends refreshes. After an expiry the node is unreachable
/// until it sends again: the rebinding of RFC 9000 §9.3 as its peers see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Nat {
    pub idle_timeout_ns: u64,
}

/// Why a message did not arrive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dropped {
    Mtu,
    Queue,
    Loss,
    Partition,
    Capacity,
}
/// What the fabric did with a message at its send.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fate {
    Arrives { at: u64 },
    Dropped(Dropped),
}
/// The non-vacuity counters of a scenario: a test of loss recovery asserts
/// that `dropped_loss` moved, of congestion `dropped_queue`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FabricStats {
    pub sent: u64,
    pub delivered: u64,
    pub dropped_mtu: u64,
    pub dropped_queue: u64,
    pub dropped_loss: u64,
    pub dropped_partition: u64,
    pub dropped_capacity: u64,
    pub dropped_nat: u64,
    pub peak_queue_bytes: u64,
}

/// How many directed flows, links and NATs a fabric models. A scenario that
/// names more is refused; nothing here grows with what is sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FabricLimits {
    pub flows: usize,
    pub links: usize,
    pub nats: usize,
    /// Messages one link holds, whatever their size.
    pub link_messages: usize,
}
impl Default for FabricLimits {
    fn default() -> Self {
        Self {
            flows: 4096,
            links: 64,
            nats: 1024,
            link_messages: 65536,
        }
    }
}
#[derive(Debug)]
pub struct Fabric<M> {
    limits: FabricLimits,
    network: Network<M>,
    random: Seeded,
    default_path: Path,
    pair_paths: BTreeMap<(u64, u64), Path>,
    links: Vec<LinkState>,
    /// Whether each flow's channel is in its bad state.
    loss_state: BTreeMap<(u64, u64), bool>,
    last_arrival: BTreeMap<(u64, u64), u64>,
    /// Each NAT and when the node behind it last sent.
    nats: BTreeMap<u64, (Nat, Option<u64>)>,
    stats: FabricStats,
}
fn bump(counter: &mut u64) {
    *counter = counter.saturating_add(1);
}
impl<M> Fabric<M> {
    /// Time is in nanoseconds from zero.
    pub fn new(seed: u64, max_messages: usize, max_bytes: usize) -> Self {
        Self::with_limits(seed, max_messages, max_bytes, FabricLimits::default())
    }
    pub fn with_limits(
        seed: u64,
        max_messages: usize,
        max_bytes: usize,
        limits: FabricLimits,
    ) -> Self {
        Self {
            limits,
            network: Network::new(max_messages, max_bytes),
            random: Seeded::new(seed),
            default_path: Path::NONE,
            pair_paths: BTreeMap::new(),
            links: Vec::new(),
            loss_state: BTreeMap::new(),
            last_arrival: BTreeMap::new(),
            nats: BTreeMap::new(),
            stats: FabricStats::default(),
        }
    }
    /// The path of every directed pair without one of its own.
    pub fn set_path(&mut self, path: Path) {
        self.default_path = path;
    }
    pub fn set_pair_path(&mut self, from: u64, to: u64, path: Path) -> Result<(), NetworkError> {
        if !self.pair_paths.contains_key(&(from, to)) && self.pair_paths.len() >= self.limits.flows
        {
            return Err(NetworkError::Capacity);
        }
        self.pair_paths.insert((from, to), path);
        Ok(())
    }
    pub fn add_link(&mut self, link: Link) -> Result<LinkId, NetworkError> {
        if self.links.len() >= self.limits.links {
            return Err(NetworkError::Capacity);
        }
        let id = LinkId(self.links.len());
        self.links.push(LinkState {
            link,
            busy_until: 0,
            queued: VecDeque::new(),
        });
        Ok(id)
    }
    /// A capacity that drops or recovers mid-run; what is queued drains at
    /// the old timing.
    pub fn set_link(&mut self, id: LinkId, link: Link) {
        if let Some(state) = self.links.get_mut(id.0) {
            state.link = link;
        }
    }
    /// The mapping is alive from now, as if the node had just sent.
    pub fn set_nat(&mut self, node: u64, nat: Nat) -> Result<(), NetworkError> {
        if !self.nats.contains_key(&node) && self.nats.len() >= self.limits.nats {
            return Err(NetworkError::Capacity);
        }
        self.nats.insert(node, (nat, Some(self.network.now())));
        Ok(())
    }
    /// Forget a node: its paths, its flows' state and its NAT. What it has
    /// in flight still arrives.
    pub fn forget(&mut self, node: u64) {
        self.pair_paths
            .retain(|(from, to), _| *from != node && *to != node);
        self.loss_state
            .retain(|(from, to), _| *from != node && *to != node);
        self.last_arrival
            .retain(|(from, to), _| *from != node && *to != node);
        self.nats.remove(&node);
    }
    /// Whether the flow has state here or room for it.
    fn admits(&self, flow: (u64, u64)) -> bool {
        let known = |flows: &BTreeMap<(u64, u64), _>| {
            flows.contains_key(&flow) || flows.len() < self.limits.flows
        };
        known(&self.last_arrival)
            && (self.loss_state.contains_key(&flow) || self.loss_state.len() < self.limits.flows)
    }
    /// Expire the node's mapping now.
    pub fn rebind(&mut self, node: u64) {
        if let Some((_, last)) = self.nats.get_mut(&node) {
            *last = None;
        }
    }
    pub fn partition(&mut self, from: u64, to: u64, blocked: bool) {
        self.network.partition(from, to, blocked);
    }
    pub fn now(&self) -> u64 {
        self.network.now()
    }
    pub fn advance_to(&mut self, now: u64) -> Result<(), NetworkError> {
        self.network.advance_to(now)
    }
    /// The earliest arrival still in flight: a deadline the clock may
    /// advance to.
    pub fn next_arrival(&self) -> Option<u64> {
        self.network.next_due()
    }
    pub fn stats(&self) -> FabricStats {
        self.stats
    }
    fn path(&self, from: u64, to: u64) -> Path {
        self.pair_paths
            .get(&(from, to))
            .copied()
            .unwrap_or(self.default_path)
    }
    fn lose(&mut self, flow: (u64, u64), loss: Loss) -> bool {
        if loss.is_lossless() {
            return false;
        }
        let bad = self.loss_state.get(&flow).copied().unwrap_or(false);
        let next = if bad {
            !chance(&mut self.random, loss.bad_to_good_ppm)
        } else {
            chance(&mut self.random, loss.good_to_bad_ppm)
        };
        self.loss_state.insert(flow, next);
        let ppm = if next {
            loss.bad_loss_ppm
        } else {
            loss.good_loss_ppm
        };
        chance(&mut self.random, ppm)
    }
    pub fn send(&mut self, from: u64, to: u64, message: M, bytes: usize) -> Fate {
        let now = self.network.now();
        bump(&mut self.stats.sent);
        if let Some((_, last)) = self.nats.get_mut(&from) {
            *last = Some(now);
        }
        if !self.admits((from, to)) {
            bump(&mut self.stats.dropped_capacity);
            return Fate::Dropped(Dropped::Capacity);
        }
        let path = self.path(from, to);
        if path.mtu.is_some_and(|mtu| bytes > mtu) {
            bump(&mut self.stats.dropped_mtu);
            return Fate::Dropped(Dropped::Mtu);
        }
        let mut departure = now;
        if let Some(link) = path.link.and_then(|id| self.links.get_mut(id.0)) {
            let backlog = link.backlog_bytes(now);
            let size = u64::try_from(bytes).unwrap_or(u64::MAX);
            if backlog.saturating_add(size) > link.link.queue_bytes
                || link.queued.len() >= self.limits.link_messages
            {
                bump(&mut self.stats.dropped_queue);
                return Fate::Dropped(Dropped::Queue);
            }
            self.stats.peak_queue_bytes = self.stats.peak_queue_bytes.max(backlog);
            let starts = link.busy_until.max(now);
            departure = starts.saturating_add(link.serialization_ns(bytes));
            link.busy_until = departure;
            link.queued.push_back(Queued {
                starts,
                departs: departure,
                bytes: size,
            });
        }
        if self.lose((from, to), path.loss) {
            bump(&mut self.stats.dropped_loss);
            return Fate::Dropped(Dropped::Loss);
        }
        let mut arrival = departure.saturating_add(path.draw(&mut self.random));
        if !path.reorders
            && let Some(previous) = self.last_arrival.get(&(from, to))
        {
            arrival = arrival.max(*previous);
        }
        let delivery = Delivery { from, to, message };
        match self
            .network
            .send(delivery, bytes, arrival.saturating_sub(now))
        {
            Ok(()) => {
                self.last_arrival.insert((from, to), arrival);
                Fate::Arrives { at: arrival }
            }
            Err(NetworkError::Partitioned) => {
                bump(&mut self.stats.dropped_partition);
                Fate::Dropped(Dropped::Partition)
            }
            Err(_) => {
                bump(&mut self.stats.dropped_capacity);
                Fate::Dropped(Dropped::Capacity)
            }
        }
    }
    /// One message due now, or none.
    pub fn receive(&mut self) -> Option<Delivery<M>> {
        loop {
            let before = self.network.dropped();
            let delivery = self.network.receive();
            let cut = self.network.dropped().saturating_sub(before);
            self.stats.dropped_partition = self.stats.dropped_partition.saturating_add(cut);
            let delivery = delivery?;
            let now = self.network.now();
            let reachable = self.nats.get(&delivery.to).is_none_or(|(nat, last)| {
                last.is_some_and(|last| now.saturating_sub(last) <= nat.idle_timeout_ns)
            });
            if reachable {
                bump(&mut self.stats.delivered);
                return Some(delivery);
            }
            bump(&mut self.stats.dropped_nat);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fabric() -> Fabric<u64> {
        Fabric::new(7, 1 << 20, 1 << 30)
    }
    /// Advance to each arrival in turn and collect what is delivered.
    fn drain(fabric: &mut Fabric<u64>) -> Vec<(u64, u64)> {
        let mut delivered = vec![];
        while let Some(at) = fabric.next_arrival() {
            fabric.advance_to(at.max(fabric.now())).unwrap();
            while let Some(delivery) = fabric.receive() {
                delivered.push((fabric.now(), delivery.message));
            }
        }
        delivered
    }

    #[test]
    fn the_zero_path_delivers_at_once_and_draws_nothing() {
        let mut fabric = fabric();
        let untouched = fabric.random.clone().next_u64();
        assert_eq!(fabric.send(1, 2, 10, 100), Fate::Arrives { at: 0 });
        assert_eq!(fabric.receive().unwrap().message, 10);
        assert_eq!(fabric.random.next_u64(), untouched);
    }
    #[test]
    fn propagation_stays_inside_the_jitter_and_in_order() {
        let mut fabric = fabric();
        fabric.set_path(Path::REGIONAL);
        for message in 0..500 {
            fabric.advance_to(message * 1_000).unwrap();
            fabric.send(1, 2, message, 100);
        }
        let delivered = drain(&mut fabric);
        assert_eq!(delivered.len(), 500);
        let mut distinct = std::collections::BTreeSet::new();
        for (index, (at, message)) in delivered.iter().enumerate() {
            assert_eq!(*message, index as u64, "in send order");
            let sent = message * 1_000;
            assert!(at - sent >= 60 * MILLISECOND, "{at}");
            // An arrival held behind its predecessor is late by at most
            // what the predecessor's was.
            assert!(at - sent <= 100 * MILLISECOND + 500_000, "{at}");
            distinct.insert(at - sent);
        }
        assert!(distinct.len() > 50, "jitter varies: {}", distinct.len());
    }
    #[test]
    fn a_reordering_path_lets_messages_overtake() {
        let mut fabric = fabric();
        fabric.set_path(Path::reordering(80 * MILLISECOND, 20 * MILLISECOND));
        for message in 0..200 {
            fabric.send(1, 2, message, 100);
        }
        let order: Vec<u64> = drain(&mut fabric).into_iter().map(|(_, m)| m).collect();
        assert_eq!(order.len(), 200);
        assert!(order.windows(2).any(|pair| pair[0] > pair[1]));
    }
    #[test]
    fn independent_loss_loses_its_rate() {
        let mut fabric = fabric();
        fabric.set_path(Path::LAN.with_loss(Loss::random(50_000)));
        for message in 0..100_000 {
            fabric.send(1, 2, message, 100);
            fabric.receive();
        }
        let lost = fabric.stats().dropped_loss;
        // 5% of 100,000 with a standard deviation of 69.
        assert!((4_600..=5_400).contains(&lost), "{lost}");
    }
    #[test]
    fn bursty_loss_comes_in_runs() {
        let mut fabric = fabric();
        // Bursts of ten messages on average, all lost, entered once in a
        // hundred: the same 9% as an independent process would lose, in runs.
        fabric.set_path(Path::NONE.with_loss(Loss::bursty(10_000, 100_000, PPM)));
        let mut fates = vec![];
        for message in 0..100_000 {
            fates.push(matches!(
                fabric.send(1, 2, message, 100),
                Fate::Dropped(Dropped::Loss)
            ));
            fabric.receive();
        }
        let lost = fates.iter().filter(|lost| **lost).count();
        assert!((7_500..=10_500).contains(&lost), "{lost}");
        let runs = fates.windows(2).filter(|pair| !pair[0] && pair[1]).count();
        let mean = lost as f64 / runs as f64;
        assert!((8.0..=12.0).contains(&mean), "mean run {mean}");
    }
    #[test]
    fn loss_state_is_per_flow() {
        let mut fabric = fabric();
        fabric
            .set_pair_path(1, 2, Path::NONE.with_loss(Loss::random(PPM)))
            .unwrap();
        assert_eq!(fabric.send(1, 2, 1, 10), Fate::Dropped(Dropped::Loss));
        assert_eq!(fabric.send(2, 1, 2, 10), Fate::Arrives { at: 0 });
        assert_eq!(fabric.send(1, 3, 3, 10), Fate::Arrives { at: 0 });
    }
    #[test]
    fn a_bottleneck_serializes_queues_and_drops_the_tail() {
        let mut fabric = fabric();
        // 8,000 bits a second is 1,000 bytes a second; the queue holds two
        // messages of 500 bytes behind the one being sent.
        let link = fabric
            .add_link(Link {
                rate_bits_per_second: 8_000,
                queue_bytes: 1_500,
            })
            .unwrap();
        fabric.set_path(Path::NONE.through(link));
        let half = 500_000_000;
        assert_eq!(fabric.send(1, 2, 1, 500), Fate::Arrives { at: half });
        assert_eq!(fabric.send(1, 2, 2, 500), Fate::Arrives { at: 2 * half });
        assert_eq!(fabric.send(3, 2, 3, 500), Fate::Arrives { at: 3 * half });
        assert_eq!(fabric.send(1, 2, 4, 500), Fate::Dropped(Dropped::Queue));
        assert_eq!(fabric.stats().dropped_queue, 1);
        assert_eq!(fabric.stats().peak_queue_bytes, 1_000);
        // Half of the message being sent has left: still no room for a whole
        // one, and room for what has left.
        fabric.advance_to(half / 2).unwrap();
        assert_eq!(fabric.send(1, 2, 9, 500), Fate::Dropped(Dropped::Queue));
        assert!(matches!(fabric.send(1, 2, 8, 250), Fate::Arrives { .. }));
        assert_eq!(fabric.send(1, 2, 7, 1), Fate::Dropped(Dropped::Queue));
        // The backlog drains at the rate: one message later there is room.
        fabric.advance_to(half).unwrap();
        assert_eq!(fabric.send(1, 2, 6, 500), Fate::Dropped(Dropped::Queue));
        fabric.advance_to(half + half / 2).unwrap();
        assert_eq!(
            fabric.send(1, 2, 5, 500),
            Fate::Arrives {
                at: 4 * half + half / 2
            }
        );
        assert_eq!(fabric.stats().dropped_queue, 4);
        assert_eq!(drain(&mut fabric).len(), 5);
    }
    #[test]
    fn a_link_whose_capacity_changes_is_followed() {
        let mut fabric = fabric();
        let link = fabric
            .add_link(Link {
                rate_bits_per_second: 8_000,
                queue_bytes: 10_000,
            })
            .unwrap();
        fabric.set_path(Path::NONE.through(link));
        assert_eq!(
            fabric.send(1, 2, 1, 1_000),
            Fate::Arrives { at: 1_000_000_000 }
        );
        fabric.set_link(
            link,
            Link {
                rate_bits_per_second: 80_000,
                queue_bytes: 10_000,
            },
        );
        assert_eq!(
            fabric.send(1, 2, 2, 1_000),
            Fate::Arrives { at: 1_100_000_000 }
        );
    }
    #[test]
    fn a_message_over_the_path_mtu_is_a_black_hole() {
        let mut fabric = fabric();
        fabric.set_path(Path::LAN.with_mtu(1_200));
        assert!(matches!(fabric.send(1, 2, 1, 1_200), Fate::Arrives { .. }));
        assert_eq!(fabric.send(1, 2, 2, 1_201), Fate::Dropped(Dropped::Mtu));
        assert_eq!(fabric.stats().dropped_mtu, 1);
    }
    #[test]
    fn a_nat_mapping_expires_and_the_node_is_reached_again_once_it_sends() {
        let mut fabric = fabric();
        let second = 1_000_000_000;
        fabric
            .set_nat(
                2,
                Nat {
                    idle_timeout_ns: 30 * second,
                },
            )
            .unwrap();
        fabric.send(1, 2, 1, 10);
        assert_eq!(fabric.receive().unwrap().message, 1);
        fabric.advance_to(31 * second).unwrap();
        fabric.send(1, 2, 2, 10);
        assert_eq!(fabric.receive(), None);
        assert_eq!(fabric.stats().dropped_nat, 1);
        // The node sends: its mapping is alive again.
        fabric.send(2, 1, 3, 10);
        fabric.send(1, 2, 4, 10);
        assert_eq!(fabric.receive().unwrap().message, 3);
        assert_eq!(fabric.receive().unwrap().message, 4);
        // A rebinding the scenario chooses.
        fabric.rebind(2);
        fabric.send(1, 2, 5, 10);
        assert_eq!(fabric.receive(), None);
        assert_eq!(fabric.stats().dropped_nat, 2);
    }
    #[test]
    fn a_partition_cuts_at_send_and_in_flight() {
        let mut fabric = fabric();
        fabric.set_path(Path::REGIONAL);
        fabric.send(1, 2, 1, 10);
        fabric.partition(1, 2, true);
        assert_eq!(fabric.send(1, 2, 2, 10), Fate::Dropped(Dropped::Partition));
        assert_eq!(drain(&mut fabric), vec![]);
        assert_eq!(fabric.stats().dropped_partition, 2);
        fabric.partition(1, 2, false);
        assert!(matches!(fabric.send(1, 2, 3, 10), Fate::Arrives { .. }));
        assert_eq!(drain(&mut fabric).len(), 1);
    }
    #[test]
    fn a_scenario_replays_from_its_seed() {
        let run = |seed| {
            let mut fabric: Fabric<u64> = Fabric::new(seed, 1 << 20, 1 << 30);
            fabric.set_path(
                Path::reordering(80 * MILLISECOND, 20 * MILLISECOND)
                    .with_loss(Loss::bursty(20_000, 200_000, 500_000)),
            );
            for message in 0..2_000 {
                fabric.send(1 + message % 3, 1 + (message + 1) % 3, message, 100);
            }
            (drain(&mut fabric), fabric.stats())
        };
        assert_eq!(run(11), run(11));
        assert_ne!(run(11).0, run(12).0);
    }
    #[test]
    fn every_message_is_accounted_for() {
        let mut fabric = fabric();
        let link = fabric
            .add_link(Link {
                rate_bits_per_second: 1_000_000,
                queue_bytes: 20_000,
            })
            .unwrap();
        fabric.set_path(
            Path::REGIONAL
                .with_loss(Loss::random(30_000))
                .with_mtu(1_200)
                .through(link),
        );
        for message in 0..5_000_u64 {
            fabric.advance_to(message * 2_000_000).unwrap();
            let bytes = if message % 50 == 0 { 1_400 } else { 600 };
            fabric.send(1, 2, message, bytes);
            while fabric.receive().is_some() {}
        }
        drain(&mut fabric);
        let stats = fabric.stats();
        assert!(stats.dropped_mtu > 0 && stats.dropped_loss > 0 && stats.dropped_queue > 0);
        assert_eq!(
            stats.sent,
            stats.delivered
                + stats.dropped_mtu
                + stats.dropped_queue
                + stats.dropped_loss
                + stats.dropped_partition
                + stats.dropped_capacity
                + stats.dropped_nat
        );
    }
    #[test]
    fn every_table_is_bounded_and_a_forgotten_node_frees_its_rows() {
        let limits = FabricLimits {
            flows: 2,
            links: 1,
            nats: 1,
            link_messages: 3,
        };
        let mut fabric: Fabric<u64> = Fabric::with_limits(1, 1 << 20, 1 << 30, limits);
        let link = fabric
            .add_link(Link {
                rate_bits_per_second: 8,
                queue_bytes: u64::MAX,
            })
            .unwrap();
        assert_eq!(
            fabric.add_link(Link {
                rate_bits_per_second: 8,
                queue_bytes: 1
            }),
            Err(NetworkError::Capacity)
        );
        fabric.set_nat(1, Nat { idle_timeout_ns: 1 }).unwrap();
        fabric.set_nat(1, Nat { idle_timeout_ns: 2 }).unwrap();
        assert_eq!(
            fabric.set_nat(2, Nat { idle_timeout_ns: 1 }),
            Err(NetworkError::Capacity)
        );
        fabric.set_path(Path::LAN.with_loss(Loss::random(1)).through(link));
        // The link's bytes never fill: its count of messages holds.
        assert!(matches!(fabric.send(1, 2, 1, 1), Fate::Arrives { .. }));
        assert!(matches!(fabric.send(1, 2, 2, 1), Fate::Arrives { .. }));
        assert!(matches!(fabric.send(2, 1, 3, 1), Fate::Arrives { .. }));
        assert_eq!(fabric.send(2, 1, 4, 1), Fate::Dropped(Dropped::Queue));
        // A third flow has no row to take.
        assert_eq!(fabric.send(1, 3, 5, 1), Fate::Dropped(Dropped::Capacity));
        assert_eq!(fabric.last_arrival.len(), 2);
        assert_eq!(fabric.loss_state.len(), 2);
        fabric.set_pair_path(1, 2, Path::NONE).unwrap();
        fabric.set_pair_path(2, 1, Path::NONE).unwrap();
        assert_eq!(
            fabric.set_pair_path(1, 3, Path::NONE),
            Err(NetworkError::Capacity)
        );
        fabric.forget(2);
        assert!(fabric.last_arrival.is_empty() && fabric.loss_state.is_empty());
        assert!(fabric.pair_paths.is_empty());
        fabric.set_pair_path(1, 3, Path::NONE).unwrap();
        assert!(matches!(fabric.send(1, 3, 6, 1), Fate::Arrives { .. }));
    }
}
