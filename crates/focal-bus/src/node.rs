//! A bus node: one part, its publishers and its subscribers, driven by hyper-transport's events.
//!
//! A publish is appended to its stream (checked whole, copied once) and then delivered to each of
//! the stream's subscribers by the subscriber's own cursor: a delivery reads the stored batch where
//! the stream's chain leads and queues it on the subscriber's lane, as long as the lane has room. A
//! subscriber whose lane is full is left where it is, holding nothing but its cursor, and goes on
//! when hyper-transport says the lane has room again (`Event::LaneRoom`); the node keeps no queue
//! for it.
//!
//! Nothing a live subscriber is owed is dropped. Its lane is lossless and holds about a congestion
//! window (hyper-transport `Classes::lossless`), so a full lane says its path carries no more; the
//! node then holds the publishers feeding its stream (`Endpoint::hold_lane`), whose batches wait in
//! QUIC behind the node's receive window, and lets them go once the stream's subscribers take
//! again. A publisher past what the slowest live subscriber takes is held at its own lane, and no
//! queue grows past what the path carries. A subscriber that goes, or hangs until the transport's
//! idle timeout closes it, holds nothing. The part keeps what the node's memory holds of its
//! batches, for subscriptions that start behind (a reconnect, a replay): a batch, or a frame the
//! transport waits to take (`Endpoint::frames_reserving`), that the memory cannot hold now is given the
//! room of the oldest batches the part retains, let go oldest first ([`Node::relieve_transport`]). A
//! subscriber whose cursor falls below its stream's retained floor is told the offsets it missed
//! (`Gap`) and goes on from the floor, where NATS and Redis disconnect such a subscriber without
//! saying what it lost.
//!
//! Every table is bounded, and its memory charged to the node's budget before it grows: streams at
//! the part's bound, subscriptions at `subscriptions`, both the rows the node's memory holds, and
//! the window at what the memory holds.

use std::collections::{HashMap, VecDeque};

use focal_memory::{Allocation, BudgetKind, BudgetLane, MemoryBudget};
use hyper_transport::{
    Budget, Classes, Directory, Endpoint, Epoch, Event, ExchangeId, LaneId, PeerId, Refusal,
};

use crate::BusError;
use crate::batch::Batch;
use crate::budget::Headroom;
use crate::log::{Position, SegmentPool};
use crate::part::{Appended, Part, PartLimits, Taken};
use crate::producer::{self, Entry};
use crate::stream::STREAM_BYTES;
use crate::wire::{self, Bus, Gap, Kind, LANE, Reply, Subscribe, Subscribed};

/// What a node holds at most.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeConfig {
    /// Each segment's bytes.
    pub segment_bytes: usize,
    /// Streams the part holds.
    pub streams: usize,
    /// Subscriptions at once.
    pub subscriptions: usize,
    /// Publishing identities' sessions the part keeps (`PartLimits::producers`).
    pub producers: usize,
}

/// What one subscription costs the node's tables at least: its slot, its place on the free list,
/// and its place among its stream's subscribers.
const SUBSCRIPTION_BYTES: usize = std::mem::size_of::<Option<Subscription>>()
    + std::mem::size_of::<usize>()
    + std::mem::size_of::<usize>();

impl NodeConfig {
    /// Each segment's bytes: [`wire::SEGMENT_BATCHES`] of the largest batch, so a segment's last
    /// batch leaves at most a fifth of it unused.
    pub const SEGMENT_BYTES: usize = {
        let bytes = wire::BATCH_BOUND.saturating_mul(wire::SEGMENT_BATCHES);
        if bytes > usize::MAX as u64 {
            usize::MAX
        } else {
            bytes as usize
        }
    };
    /// A node's bounds for a node of `memory_bytes`: the streams and the subscriptions whose rows
    /// its memory holds ([`STREAM_BYTES`], [`SUBSCRIPTION_BYTES`] each at least). Each row is
    /// charged as it is taken, so the budget refuses no later than these; nothing is held before
    /// it is used.
    pub const fn sized(memory_bytes: usize) -> Self {
        Self {
            segment_bytes: Self::SEGMENT_BYTES,
            streams: memory_bytes / STREAM_BYTES,
            subscriptions: memory_bytes / SUBSCRIPTION_BYTES,
            producers: memory_bytes / producer::ENTRY_BYTES,
        }
    }
}

/// What a node has done.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeStats {
    /// Batches appended.
    pub published: u64,
    /// Publishes refused, each for its typed reason.
    pub refused: u64,
    /// Batches queued to subscribers.
    pub delivered: u64,
    /// Gap notices sent.
    pub gaps: u64,
    /// Times a subscriber's lane was full.
    pub blocked: u64,
    /// Subscriptions made.
    pub subscribed: u64,
    /// Times a publisher's lane was held for a stream whose subscriber took no more.
    pub held: u64,
    /// Deliveries the node had no memory for once the window had given what it holds: each waited
    /// for the transport to give memory back, its subscriber held as a full lane holds it.
    pub starved: u64,
    /// Subscriptions let go because the transport could no longer reach their peer.
    pub dropped: u64,
    /// Segments allocated from the system, each faulted in as it fills.
    pub segments_allocated: u64,
    /// Segments returned to the system.
    pub segments_released: u64,
}

/// A batch the part retains, in the order appended: its stream, the offset after it, and, in a
/// part its group replicates, the group's entry it was. The window lets batches go oldest first,
/// so the part's floors are its state as of the newest entry it let go (`group`'s image).
#[derive(Clone, Copy, Debug)]
struct Retained {
    stream: u64,
    end: u64,
    /// The group's entry, its index and term; `(0, 0)` for a lone node's batch.
    entry: (u64, u64),
    /// The identity that published it, in a part its group replicates: the floor's sessions take
    /// it as it goes (`Part::release_below`).
    owner: Option<u64>,
}

/// A publisher's lane the node holds while a stream it fed has a subscriber that takes no more.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Holding {
    peer: PeerId,
    lane: LaneId,
    stream: u64,
}

/// A subscriber of one stream on one connection.
#[derive(Clone, Copy, Debug)]
struct Subscription {
    peer: PeerId,
    epoch: Epoch,
    stream: u64,
    /// The next offset to deliver.
    cursor: u64,
    /// Where the batch last delivered is (a position word), to follow the chain from.
    last: Option<u64>,
    /// Its lane was full: it waits for room.
    blocked: bool,
}

/// One bus node's state.
#[derive(Debug)]
pub struct Node {
    pool: SegmentPool,
    part: Part,
    retained: VecDeque<Retained>,
    config: NodeConfig,
    budget: MemoryBudget,
    /// The charge for the retained batches' index, by its capacity.
    index: Option<Allocation>,
    /// The charge for the subscriptions' tables and what serves them, by their capacities.
    tables: Option<Allocation>,
    subscriptions: Vec<Option<Subscription>>,
    free: Vec<usize>,
    by_stream: HashMap<u64, Vec<usize>>,
    /// The subscriptions one event delivers to, gathered here and pumped after: kept between
    /// events, so a publish or a lane's room allocates nothing once it has grown to the most
    /// subscribers one stream has had.
    scratch: Vec<usize>,
    /// The publishers' lanes held for streams whose subscribers take no more: one for each
    /// (publisher's lane, stream), at most a publisher's lanes for each subscription that blocks.
    holding: Vec<Holding>,
    /// The lanes one release found free, kept between releases.
    freed: Vec<(PeerId, LaneId)>,
    /// Subscriptions whose delivery found no memory once the window had none left to give: asked
    /// again whenever the transport gives memory back ([`Node::relieve_transport`]). The list is
    /// swapped with `retrying` to be walked, so neither ever allocates past the subscriptions.
    starved: Vec<usize>,
    retrying: Vec<usize>,
    /// The newest group entry the window let go, its index and term: the part's floors are its
    /// state as of it. `(0, 0)` for none, and for a lone node.
    released: (u64, u64),
    stats: NodeStats,
}

/// How a delivery attempt ended.
enum Sent {
    Queued,
    /// The lane holds what its path carries: the transport says `LaneRoom` when it has room.
    Full,
    /// No memory for the frame (`Refusal::Budget`).
    Short,
    /// The peer cannot be reached any more.
    Gone,
}

impl Node {
    /// A node holding nothing, its segments and tables charged to `budget`.
    pub fn new(config: NodeConfig, budget: MemoryBudget) -> Result<Self, BusError> {
        let batch_bytes = usize::try_from(wire::BATCH_BOUND)
            .map_err(|_| BusError::Configuration("the batch bound"))?;
        if config.segment_bytes < batch_bytes {
            return Err(BusError::Configuration(
                "a segment holds at least one batch of the largest size",
            ));
        }
        Ok(Self {
            pool: SegmentPool::new(config.segment_bytes, budget.clone())?,
            part: Part::new(
                PartLimits {
                    streams: config.streams,
                    batch_bytes,
                    producers: config.producers,
                },
                budget.clone(),
            ),
            retained: VecDeque::new(),
            config,
            budget,
            index: None,
            tables: None,
            subscriptions: Vec::new(),
            free: Vec::new(),
            by_stream: HashMap::new(),
            scratch: Vec::new(),
            holding: Vec::new(),
            freed: Vec::new(),
            starved: Vec::new(),
            retrying: Vec::new(),
            released: (0, 0),
            stats: NodeStats::default(),
        })
    }

    /// What the node has done.
    pub const fn stats(&self) -> NodeStats {
        let (segments_allocated, segments_released) = self.pool.churn();
        NodeStats {
            segments_allocated,
            segments_released,
            ..self.stats
        }
    }

    /// The node's memory as its budget holds it now: the segments (`Payload`), the transport's
    /// buffers and socket (`Pending`) and the tables (`Index`).
    pub fn memory(&self) -> focal_memory::BudgetStats {
        self.budget.stats()
    }

    /// The node's part.
    pub const fn part(&self) -> &Part {
        &self.part
    }

    /// The node's part's bounds.
    pub const fn part_limits(&self) -> PartLimits {
        self.part.limits()
    }

    /// The segment pool.
    pub const fn pool(&self) -> &SegmentPool {
        &self.pool
    }

    /// Acts on one of the endpoint's events.
    pub fn on_event<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>, event: Event<Bus>)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        match event {
            Event::Frame {
                kind: Kind::Publish,
                peer,
                lane,
                frame,
            } => {
                let stream = self.publish(endpoint, frame.bytes());
                endpoint.release(frame);
                if let Some(stream) = stream {
                    self.hold_if_blocked(endpoint, peer, lane, stream);
                }
            }
            Event::Frame { frame, .. } => endpoint.release(frame),
            Event::Request {
                exchange,
                peer,
                epoch,
                kind: Kind::Subscribe,
                ..
            } => self.subscribe(endpoint, exchange, peer, epoch),
            Event::Request { exchange, .. } => endpoint.refuse_exchange(exchange, Refusal::Kind),
            Event::LaneRoom { peer, epoch, .. } => {
                self.resume(endpoint, peer, epoch);
                self.release_unblocked(endpoint);
            }
            Event::Closed { peer, epoch, .. } => {
                self.forget(peer, epoch);
                self.holding.retain(|held| held.peer != peer);
                self.release_unblocked(endpoint);
            }
            Event::Connected { .. }
            | Event::Reply { .. }
            | Event::BodyReady { .. }
            | Event::Writable { .. }
            | Event::Refused { .. }
            | Event::Unreachable { .. } => {}
        }
    }

    /// Appends and delivers a published batch; its stream, if it was appended. What the memory
    /// cannot hold for it, the oldest batches retained give up ([`Node::relieve`]).
    fn publish<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>, bytes: &[u8]) -> Option<u64>
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let mut relieved = false;
        // Bounded: each turn but the last frees a segment for the append, or gives a warm segment
        // or a retained batch back.
        let appended = loop {
            match self.take(bytes) {
                Ok(appended) => break appended,
                // A segment the window empties is the append's own: kept warm, it is taken again as
                // it is charged, with nothing allocated and no page faulted afresh. Given back, the
                // next segment was allocated anew and every page of it faulted and cleared, a trip
                // to the allocator for each segment the log took once the window was full. Only
                // where a warm segment is not the answer (the window's index wanted the room) is
                // memory given back.
                Err(BusError::Memory(_)) if self.pool.warm() == 0 && self.free_segment() => {}
                Err(BusError::Memory(_)) if self.relieve() => relieved = true,
                Err(_) => {
                    self.stats.refused = self.stats.refused.saturating_add(1);
                    if relieved {
                        endpoint.retry_reserving();
                    }
                    return None;
                }
            }
        };
        self.stats.published = self.stats.published.saturating_add(1);
        // The budget holds more now: a lane whose frame waited for it reads on.
        if relieved {
            endpoint.retry_reserving();
        }
        let mut slots = std::mem::take(&mut self.scratch);
        slots.clear();
        if let Some(subscribers) = self.by_stream.get(&appended.stream) {
            slots.extend_from_slice(subscribers);
        }
        for slot in slots.iter().copied() {
            self.pump(endpoint, slot);
        }
        self.scratch = slots;
        Some(appended.stream)
    }

    /// Takes a published batch into the part, its place in the window's index charged first, so
    /// the part never holds a batch the window cannot let go.
    fn take(&mut self, bytes: &[u8]) -> Result<Appended, BusError> {
        self.reserve_retained()?;
        // A publish came through the transport, which checked its batch's CRC where the wire says
        // it carries one.
        let appended = if Bus::embedded_checksum(Kind::Publish).is_some() {
            self.part.append_verified(&mut self.pool, bytes)?
        } else {
            self.part.append(&mut self.pool, bytes)?
        };
        self.retain(appended, (0, 0), None);
        Ok(appended)
    }

    /// Takes a batch the part's group committed at `entry` (`group`), published by `owner`, unless
    /// its session took it already or it comes out of its session's order (`Part::append_committed`). What the memory cannot hold for it, the window gives up,
    /// oldest first, as a publish's does; whether the window let any go is the second value, for
    /// the owner to ask the transport's waiting frames again. A committed batch is refused only
    /// where the window has nothing left to give.
    pub fn apply_committed(
        &mut self,
        bytes: &[u8],
        entry: (u64, u64),
        owner: u64,
    ) -> Result<(Taken, bool), BusError> {
        let mut relieved = false;
        // Bounded: each turn but the last frees a segment, or gives a warm segment or a retained
        // batch back.
        loop {
            let taken = self
                .reserve_retained()
                .and_then(|()| self.part.append_committed(&mut self.pool, bytes, owner));
            match taken {
                Ok(taken) => {
                    if let Taken::Appended(appended) = taken {
                        self.retain(appended, entry, Some(owner));
                        self.stats.published = self.stats.published.saturating_add(1);
                    }
                    return Ok((taken, relieved));
                }
                Err(BusError::Memory(_)) if self.pool.warm() == 0 && self.free_segment() => {}
                Err(BusError::Memory(_)) if self.relieve() => relieved = true,
                Err(error) => return Err(error),
            }
        }
    }

    /// Lets every batch the part holds go, and holds `floors`' streams, each from its floor on, and
    /// `producers`' sessions: a part opened from its image, or given one by its group's leader.
    /// Subscribers below a floor are told what they missed as they read on.
    pub fn restore(
        &mut self,
        floors: impl IntoIterator<Item = (u64, u64)>,
        producers: &[Entry],
        at: (u64, u64),
    ) -> Result<(), BusError> {
        self.retained.clear();
        self.released = at;
        self.part.restore(&mut self.pool, floors, producers)
    }

    /// Delivers what `stream` holds past each of its subscribers' cursors: after a batch of it
    /// was appended.
    pub fn deliver<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>, stream: u64)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let mut slots = std::mem::take(&mut self.scratch);
        slots.clear();
        if let Some(subscribers) = self.by_stream.get(&stream) {
            slots.extend_from_slice(subscribers);
        }
        for slot in slots.iter().copied() {
            self.pump(endpoint, slot);
        }
        self.scratch = slots;
    }

    /// Room for one more batch in the window's index, charged before the batch is taken.
    fn reserve_retained(&mut self) -> Result<(), BusError> {
        if self.retained.len() == self.retained.capacity() {
            self.retained
                .try_reserve(1)
                .map_err(|_| BusError::Memory(focal_memory::MemoryError::AllocationFailed))?;
        }
        let index = self
            .retained
            .capacity()
            .checked_mul(std::mem::size_of::<Retained>())
            .ok_or(BusError::Capacity("the window's index"))?;
        charge(&self.budget, &mut self.index, index)
    }

    /// Keeps `appended` in the window, its room reserved before it was taken
    /// ([`Node::reserve_retained`]): nothing is allocated here.
    fn retain(&mut self, appended: Appended, entry: (u64, u64), owner: Option<u64>) {
        self.retained.push_back(Retained {
            stream: appended.stream,
            end: appended.base.saturating_add(u64::from(appended.count)),
            entry,
            owner,
        });
        self.recycle_ahead();
    }

    /// The window's oldest batch, taken out of it, its entry recorded as the newest let go: the
    /// caller lets its stream's floor move past it.
    fn let_go_oldest(&mut self) -> Option<Retained> {
        let oldest = self.retained.pop_front()?;
        if oldest.entry != (0, 0) {
            self.released = oldest.entry;
        }
        Some(oldest)
    }

    /// The newest group entry the window let go, its index and term, `(0, 0)` for none: the part's
    /// floors are its state as of it (`group`'s image).
    pub const fn released(&self) -> (u64, u64) {
        self.released
    }

    /// The window's segments emptied over the node's life: one each time it let go a segment's
    /// batches.
    pub const fn segments_emptied(&self) -> u64 {
        self.pool.emptied()
    }

    /// Once the window holds what the memory holds, lets the oldest batches go as the tail fills:
    /// the oldest segment is let go in the share the tail has filled, so it is empty and warm when
    /// the tail takes the next. Let go all at once as the tail rolled over, a segment's batches held
    /// the node for as long as it took to give each back (about 4,100 of the claim corpus's every
    /// 5 MiB at 100,000 a second). The same batches go, oldest first, spread over the appends.
    fn recycle_ahead(&mut self) {
        if self.pool.warm() > 0
            || self.budget.headroom(BudgetLane::Ordinary) >= self.config.segment_bytes
        {
            return;
        }
        let need = self.part.log().recycle_need(self.config.segment_bytes);
        // Bounded: at most the oldest segment's batches, each popped once.
        for _ in 0..need {
            let Some(oldest) = self.let_go_oldest() else {
                break;
            };
            // A stream's batches leave in order, so its floor moves past the oldest's end.
            let _ =
                self.part
                    .release_below(&mut self.pool, oldest.stream, oldest.end, oldest.owner);
        }
    }

    /// Lets the oldest batches retained go until a segment comes back to the pool, where it stays
    /// warm for the append that wanted one (the pool keeps what its demand needed, and every
    /// segment was out), or the pool returns it and the budget has its room. Whether either did.
    fn free_segment(&mut self) -> bool {
        let (_, released) = self.pool.churn();
        let freed = |pool: &SegmentPool| pool.warm() > 0 || pool.churn().1 > released;
        // Bounded: each turn lets one retained batch go.
        while let Some(oldest) = self.let_go_oldest() {
            // A stream's batches leave in order, so its floor moves past the oldest's end.
            let _ =
                self.part
                    .release_below(&mut self.pool, oldest.stream, oldest.end, oldest.owner);
            if freed(&self.pool) {
                return true;
            }
        }
        // The last batches let go were the tail's: with nothing in it, it comes back.
        self.part.seal(&mut self.pool) && freed(&self.pool)
    }

    /// Gives memory back for what is live, at the window's expense: the pool's warm segments first,
    /// then the oldest batches the part retains, until a segment is given back. Whether anything
    /// was.
    pub fn relieve(&mut self) -> bool {
        if self.pool.warm() > 0 {
            self.pool.shed();
            return true;
        }
        let (_, returned) = self.pool.churn();
        // Bounded: each turn lets one retained batch go.
        while let Some(oldest) = self.let_go_oldest() {
            // A stream's batches leave in order, so its floor moves past the oldest's end.
            let _ =
                self.part
                    .release_below(&mut self.pool, oldest.stream, oldest.end, oldest.owner);
            if self.pool.warm() > 0 || self.pool.churn().1 > returned {
                self.pool.shed();
                return true;
            }
        }
        // The last batches let go were the tail's, which is kept while it takes batches: with
        // nothing in it, it goes back, and the next batch starts a segment of its own.
        if self.part.seal(&mut self.pool) {
            self.pool.shed();
            return true;
        }
        false
    }

    /// Gives the transport's frames that wait for memory (`Endpoint::frames_reserving`, which
    /// counts none whose lane the node holds) what the budget holds now, then the room of the
    /// oldest batches retained while memory is what they lack: while the transport's budget could
    /// not hold the largest frame it reserves ([`crate::budget::largest_grant`]). Live traffic goes
    /// before what is kept for subscribers that start behind, but a frame that waits for anything
    /// else is given nothing: the window would let every batch go, those its subscribers are owed
    /// among them, and the frame would wait on. Asked once a turn of the owner's.
    pub fn relieve_transport<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>)
    where
        B: Budget<<Bus as Classes>::Class> + Headroom,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        if endpoint.frames_reserving() > 0 {
            // What the budget holds now goes first: a frame given back since the waiting frame was
            // refused, or a warm buffer, which the budget gives back before it refuses.
            endpoint.retry_reserving();
            let largest = crate::budget::largest_grant();
            // Bounded: each turn gives a segment back, and `relieve` is false once the part
            // retains nothing.
            while endpoint.frames_reserving() > 0
                && endpoint.budget().headroom() < largest
                && self.relieve()
            {
                endpoint.retry_reserving();
            }
        }
        self.retry_starved(endpoint);
    }

    /// Whether a delivery waits for the transport to give memory back: its owner asks again
    /// ([`Node::relieve_transport`]) when the transport has returned memory, not only on its next
    /// event, which may never come while every lane waits.
    pub fn starving(&self) -> bool {
        !self.starved.is_empty()
    }

    /// Asks again for the deliveries that found no memory: each subscription is let out of its
    /// wait and pumped; one that finds none again waits again.
    fn retry_starved<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        if self.starved.is_empty() {
            return;
        }
        let mut retrying = std::mem::take(&mut self.retrying);
        std::mem::swap(&mut retrying, &mut self.starved);
        for slot in retrying.iter().copied() {
            self.update(slot, |s| s.blocked = false);
            self.pump(endpoint, slot);
        }
        retrying.clear();
        self.retrying = retrying;
        // A stream no subscriber waits on any more lets its publishers go.
        self.release_unblocked(endpoint);
    }

    /// `slot`'s delivery found no memory: its subscriber waits, held as a full lane holds it (the
    /// publishers feeding its stream are held too), until the transport gives memory back.
    fn starve(&mut self, slot: usize) {
        self.stats.starved = self.stats.starved.saturating_add(1);
        self.update(slot, |s| s.blocked = true);
        // Room reserved with the subscription (`reserve_subscription`): no allocation.
        if !self.starved.contains(&slot) {
            self.starved.push(slot);
        }
    }

    /// The bytes the subscriptions' tables and what serves them hold, by their capacities: the
    /// slots, the free list, the scratch lists and the held lanes, and the map of each stream's
    /// subscribers (its buckets kept at most seven eighths full, a power of two, and each stream's
    /// list).
    fn table_bytes(&self) -> Option<usize> {
        let of = |capacity: usize, item: usize| capacity.checked_mul(item);
        let buckets = self
            .by_stream
            .capacity()
            .checked_mul(8)?
            .checked_div(7)?
            .checked_next_power_of_two()?;
        let mut bytes = of(
            self.subscriptions.capacity(),
            std::mem::size_of::<Option<Subscription>>(),
        )?
        .checked_add(of(self.free.capacity(), std::mem::size_of::<usize>())?)?
        .checked_add(of(self.scratch.capacity(), std::mem::size_of::<usize>())?)?
        .checked_add(of(self.holding.capacity(), std::mem::size_of::<Holding>())?)?
        .checked_add(of(
            self.freed.capacity(),
            std::mem::size_of::<(PeerId, LaneId)>(),
        )?)?
        .checked_add(of(self.starved.capacity(), std::mem::size_of::<usize>())?)?
        .checked_add(of(self.retrying.capacity(), std::mem::size_of::<usize>())?)?
        .checked_add(of(
            buckets,
            std::mem::size_of::<(u64, Vec<usize>)>().checked_add(1)?,
        )?)?;
        for slots in self.by_stream.values() {
            bytes = bytes.checked_add(of(slots.capacity(), std::mem::size_of::<usize>())?)?;
        }
        Some(bytes)
    }

    /// Room for one more subscription to `stream`, charged before it is taken: its slot, its
    /// stream's entry and list, and the free and scratch lists it may come to occupy, so dropping it
    /// or delivering to it allocates nothing.
    fn reserve_subscription(&mut self, stream: u64) -> Result<(), BusError> {
        let short = |_| BusError::Memory(focal_memory::MemoryError::AllocationFailed);
        if self.free.is_empty() {
            self.subscriptions.try_reserve(1).map_err(short)?;
        }
        let slots = self.subscriptions.capacity();
        self.free
            .try_reserve(slots.saturating_sub(self.free.len()))
            .map_err(short)?;
        self.scratch
            .try_reserve(slots.saturating_sub(self.scratch.len()))
            .map_err(short)?;
        self.starved
            .try_reserve(slots.saturating_sub(self.starved.len()))
            .map_err(short)?;
        self.retrying
            .try_reserve(slots.saturating_sub(self.retrying.len()))
            .map_err(short)?;
        self.by_stream.try_reserve(1).map_err(short)?;
        if let Some(list) = self.by_stream.get_mut(&stream) {
            list.try_reserve(1).map_err(short)?;
        }
        let bytes = self
            .table_bytes()
            .ok_or(BusError::Capacity("the subscriptions' tables"))?;
        charge(&self.budget, &mut self.tables, bytes)
    }

    /// Room for one more held lane, charged before it is taken, and for the lanes a release may
    /// find free.
    fn reserve_hold(&mut self) -> Result<(), BusError> {
        let short = |_| BusError::Memory(focal_memory::MemoryError::AllocationFailed);
        self.holding.try_reserve(1).map_err(short)?;
        let held = self.holding.capacity();
        self.freed
            .try_reserve(held.saturating_sub(self.freed.len()))
            .map_err(short)?;
        let bytes = self
            .table_bytes()
            .ok_or(BusError::Capacity("the subscriptions' tables"))?;
        charge(&self.budget, &mut self.tables, bytes)
    }

    /// Whether a subscriber of `stream` takes no more: its lane is full, its path carrying what it
    /// can.
    fn blocked(&self, stream: u64) -> bool {
        self.by_stream.get(&stream).is_some_and(|slots| {
            slots.iter().any(|slot| {
                self.subscriptions
                    .get(*slot)
                    .and_then(Option::as_ref)
                    .is_some_and(|subscription| subscription.blocked)
            })
        })
    }

    /// Holds `peer`'s `lane`, which fed `stream`, while a subscriber of the stream takes no more.
    fn hold_if_blocked<B, D>(
        &mut self,
        endpoint: &mut Endpoint<Bus, B, D>,
        peer: PeerId,
        lane: LaneId,
        stream: u64,
    ) where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        if !self.blocked(stream) {
            return;
        }
        let held = Holding { peer, lane, stream };
        if self.holding.contains(&held) {
            return;
        }
        // A hold the node has no room to record, or the transport refuses (a lane past the peer's
        // bound), leaves the publisher unheld: its batches are taken as they come, the window
        // keeping what the memory holds of them.
        if self.reserve_hold().is_err() {
            return;
        }
        if endpoint.hold_lane(peer, lane).is_ok() {
            self.holding.push(held);
            self.stats.held = self.stats.held.saturating_add(1);
        }
    }

    /// Lets go every publisher's lane no stream it was held for still holds.
    fn release_unblocked<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        if self.holding.is_empty() {
            return;
        }
        let mut holding = std::mem::take(&mut self.holding);
        let mut freed = std::mem::take(&mut self.freed);
        freed.clear();
        holding.retain(|held| {
            let keep = self.blocked(held.stream);
            if !keep {
                freed.push((held.peer, held.lane));
            }
            keep
        });
        for (peer, lane) in freed.iter().copied() {
            // A lane another blocked stream holds stays held.
            if !holding
                .iter()
                .any(|held| held.peer == peer && held.lane == lane)
            {
                let _ = endpoint.release_lane(peer, lane);
            }
        }
        self.holding = holding;
        self.freed = freed;
    }

    fn subscribe<B, D>(
        &mut self,
        endpoint: &mut Endpoint<Bus, B, D>,
        exchange: ExchangeId,
        peer: PeerId,
        epoch: Epoch,
    ) where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let Some(head) = endpoint.head(exchange) else {
            endpoint.refuse_exchange(exchange, Refusal::Kind);
            return;
        };
        let Ok(asked) = Subscribe::decode(head) else {
            endpoint.refuse_exchange(exchange, Refusal::Kind);
            return;
        };
        let live = self.subscriptions.len().saturating_sub(self.free.len());
        if live >= self.config.subscriptions {
            endpoint.refuse_exchange(exchange, Refusal::Exchanges);
            return;
        }
        if self.reserve_subscription(asked.stream).is_err() {
            endpoint.refuse_exchange(exchange, Refusal::Budget);
            return;
        }
        let (floor, next) = self.part.bounds(asked.stream).unwrap_or((0, 0));
        let start = match asked.from {
            wire::From::Tail => next,
            wire::From::Offset(offset) => offset.max(floor),
        };
        let subscription = Subscription {
            peer,
            epoch,
            stream: asked.stream,
            cursor: start,
            last: None,
            blocked: false,
        };
        let slot = match self.free.pop() {
            Some(slot) => {
                if let Some(place) = self.subscriptions.get_mut(slot) {
                    *place = Some(subscription);
                }
                slot
            }
            None => {
                self.subscriptions.push(Some(subscription));
                self.subscriptions.len().saturating_sub(1)
            }
        };
        self.by_stream.entry(asked.stream).or_default().push(slot);
        self.stats.subscribed = self.stats.subscribed.saturating_add(1);
        let reply = Reply::Subscribed(Subscribed { floor, next, start });
        if endpoint.reply(exchange, &reply.encode(), None).is_ok() {
            endpoint.end(exchange);
        }
        self.pump(endpoint, slot);
    }

    fn resume<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>, peer: PeerId, epoch: Epoch)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let mut waiting = std::mem::take(&mut self.scratch);
        waiting.clear();
        waiting.extend(
            self.subscriptions
                .iter()
                .enumerate()
                .filter_map(|(slot, subscription)| {
                    subscription
                        .filter(|s| s.peer == peer && s.epoch == epoch && s.blocked)
                        .map(|_| slot)
                }),
        );
        for slot in waiting.iter().copied() {
            if let Some(Some(subscription)) = self.subscriptions.get_mut(slot) {
                subscription.blocked = false;
            }
            self.pump(endpoint, slot);
        }
        self.scratch = waiting;
    }

    fn forget(&mut self, peer: PeerId, epoch: Epoch) {
        for slot in 0..self.subscriptions.len() {
            let gone = self
                .subscriptions
                .get(slot)
                .and_then(Option::as_ref)
                .is_some_and(|s| s.peer == peer && s.epoch == epoch);
            if gone {
                self.drop_slot(slot);
            }
        }
    }

    fn drop_slot(&mut self, slot: usize) {
        let Some(place) = self.subscriptions.get_mut(slot) else {
            return;
        };
        let Some(subscription) = place.take() else {
            return;
        };
        if let Some(slots) = self.by_stream.get_mut(&subscription.stream) {
            slots.retain(|known| *known != slot);
            if slots.is_empty() {
                self.by_stream.remove(&subscription.stream);
            }
        }
        self.starved.retain(|known| *known != slot);
        self.free.push(slot);
    }

    /// Delivers what `slot`'s subscriber has not taken, as long as its lane has room.
    fn pump<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>, slot: usize)
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        loop {
            let Some(Some(subscription)) = self.subscriptions.get(slot).copied() else {
                return;
            };
            if subscription.blocked {
                return;
            }
            let Some((floor, next)) = self.part.bounds(subscription.stream) else {
                return;
            };
            if subscription.cursor >= next {
                return;
            }
            if subscription.cursor < floor {
                let gap = Gap {
                    stream: subscription.stream,
                    from: subscription.cursor,
                    to: floor,
                };
                match send(endpoint, subscription.peer, Kind::Gap, &gap.encode()) {
                    Sent::Queued => {
                        self.stats.gaps = self.stats.gaps.saturating_add(1);
                        self.update(slot, |s| {
                            s.cursor = floor;
                            s.last = None;
                        });
                        continue;
                    }
                    Sent::Full => return self.block(slot),
                    Sent::Short => {
                        if self.short(endpoint) {
                            continue;
                        }
                        return self.starve(slot);
                    }
                    Sent::Gone => return self.gone(slot),
                }
            }
            let Some(word) = self.next_word(&subscription) else {
                return;
            };
            let Some(batch) = self.part.batch(Position::from_word(word)) else {
                return;
            };
            let end = batch.end_offset();
            match send(endpoint, subscription.peer, Kind::Deliver, batch.bytes()) {
                Sent::Queued => {
                    self.stats.delivered = self.stats.delivered.saturating_add(1);
                    self.update(slot, |s| {
                        s.cursor = end;
                        s.last = Some(word);
                    });
                }
                Sent::Full => return self.block(slot),
                Sent::Short => {
                    // The batch may have been the window's to give up: the loop finds where the
                    // subscriber stands now, a gap told if it was.
                    if self.short(endpoint) {
                        continue;
                    }
                    return self.starve(slot);
                }
                Sent::Gone => return self.gone(slot),
            }
        }
    }

    /// Where the subscriber's next batch is: after the batch it took last while that is held,
    /// else found from the stream's first batch held.
    fn next_word(&self, subscription: &Subscription) -> Option<u64> {
        if let Some(last) = subscription.last
            && let Some(batch) = self.part.batch(Position::from_word(last))
        {
            return batch.next();
        }
        let mut word = self.part.first_word(subscription.stream)?;
        loop {
            let batch: Batch<'_> = self.part.batch(Position::from_word(word))?;
            if subscription.cursor < batch.end_offset() {
                return Some(word);
            }
            word = batch.next()?;
        }
    }

    fn update(&mut self, slot: usize, change: impl FnOnce(&mut Subscription)) {
        if let Some(Some(subscription)) = self.subscriptions.get_mut(slot) {
            change(subscription);
        }
    }

    fn block(&mut self, slot: usize) {
        self.stats.blocked = self.stats.blocked.saturating_add(1);
        self.update(slot, |s| s.blocked = true);
    }

    /// A frame found no memory: the window gives up its oldest batches until a segment goes back,
    /// and the lanes that waited for memory read on. Whether memory came back, and the frame may be
    /// asked for again.
    fn short<B, D>(&mut self, endpoint: &mut Endpoint<Bus, B, D>) -> bool
    where
        B: Budget<<Bus as Classes>::Class>,
        D: Directory<Role = <Bus as Classes>::Role>,
    {
        let relieved = self.relieve();
        if relieved {
            endpoint.retry_reserving();
        }
        relieved
    }

    /// `slot`'s peer cannot be reached any more: its subscription goes, counted.
    fn gone(&mut self, slot: usize) {
        self.stats.dropped = self.stats.dropped.saturating_add(1);
        self.drop_slot(slot);
    }
}

/// Charges `charge` up to `held` bytes, what a table's capacities hold now; a charge already past
/// them stays.
fn charge(
    budget: &MemoryBudget,
    charge: &mut Option<Allocation>,
    held: usize,
) -> Result<(), BusError> {
    let charged = charge.as_ref().map_or(0, Allocation::bytes);
    if held <= charged {
        return Ok(());
    }
    let more = held.saturating_sub(charged);
    match charge {
        Some(charge) => charge.grow(more).map_err(BusError::Memory),
        None => {
            *charge = Some(
                budget
                    .reserve(BudgetKind::Index, BudgetLane::Ordinary, more)
                    .map_err(BusError::Memory)?
                    .commit(),
            );
            Ok(())
        }
    }
}

fn send<B, D>(endpoint: &mut Endpoint<Bus, B, D>, peer: PeerId, kind: Kind, bytes: &[u8]) -> Sent
where
    B: Budget<<Bus as Classes>::Class>,
    D: Directory<Role = <Bus as Classes>::Role>,
{
    match endpoint.send_frame(peer, LANE, kind, bytes) {
        Ok(()) => Sent::Queued,
        Err(Refusal::LaneFull) => Sent::Full,
        // Memory is not the peer's going: taken as gone, a subscriber the node could not find a
        // frame's bytes for was let go in silence while it waited (the pubsub matrix of
        // 2026-10-16 lost 1.1 million messages so at 100,000 a second).
        Err(Refusal::Budget) => Sent::Short,
        Err(_) => Sent::Gone,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::{BatchBuilder, Producing};

    /// A batch of one record of `payload` bytes on stream 1.
    fn batch(builder: &mut BatchBuilder, sequence: u32, payload: usize) -> Vec<u8> {
        builder.begin(&Producing {
            stream: 1,
            producer: 2,
            sequence,
            flags: 0,
            timestamp: 1_792_022_400_000_000,
        });
        builder
            .push(1_792_022_400_000_000, &vec![7u8; payload])
            .unwrap();
        builder.finish().unwrap().to_vec()
    }

    /// Appends `bytes` as a publish does, the memory's relief included; how many retained batches
    /// the append let go.
    fn append(node: &mut Node, bytes: &[u8]) -> usize {
        let before = node.retained.len();
        for _ in 0..1_000 {
            match node.take(bytes) {
                Ok(_) => return (before + 1).saturating_sub(node.retained.len()),
                Err(BusError::Memory(_)) if node.pool.warm() == 0 && node.free_segment() => {}
                Err(BusError::Memory(_)) if node.relieve() => {}
                Err(other) => panic!("append refused: {other:?}"),
            }
        }
        panic!("the append found no room");
    }

    /// Once the window holds what the memory holds, an append lets go no more than its share of the
    /// oldest segment: the oldest segment is let go as the tail fills, and is empty when the tail
    /// rolls over. Let go at the rollover, a segment's batches left in one append, the node held for
    /// as long as it took to give each back.
    #[test]
    fn a_full_window_lets_its_oldest_segment_go_as_the_tail_fills() {
        let segment = usize::try_from(wire::BATCH_BOUND).unwrap();
        // Three segments and room for the window's index beside them.
        let budget = MemoryBudget::new(3 * segment + (1 << 20), 0).unwrap();
        let config = NodeConfig {
            segment_bytes: segment,
            streams: 4,
            subscriptions: 4,
            producers: 4,
        };
        let mut node = Node::new(config, budget).unwrap();
        let mut builder = BatchBuilder::new();
        let one = batch(&mut builder, 0, 1_000).len();
        let per_segment = segment / one;
        let mut most = 0;
        let mut filled = false;
        for sequence in 0..u32::try_from(per_segment * 8).unwrap() {
            let released = append(&mut node, &batch(&mut builder, sequence, 1_000));
            if released > 0 {
                filled = true;
            }
            if filled {
                most = most.max(released);
            }
        }
        assert!(filled, "the window never reached the memory's bound");
        assert!(
            most <= 2,
            "an append let {most} batches go at once ({per_segment} a segment)"
        );
        // The window still holds what the memory does: about three segments of batches.
        assert!(
            node.retained.len() >= 2 * per_segment,
            "{}",
            node.retained.len()
        );
    }
}
