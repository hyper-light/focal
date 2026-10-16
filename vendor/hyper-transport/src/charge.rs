//! A socket's receive charge (`docs/transport.md` §4i): what the kernel charges a socket's receive
//! queue for what its peers send, learned from the kernel's own account, so that the windows the
//! socket's peers are granted never pass what its queue holds while its owner does not read.
//!
//! The queue holds `buffer` bytes of charge, the buffer the socket reports, less what the kernel
//! may still charge for datagrams already read (Linux releases a reader's charge in batches of up
//! to a quarter of the buffer, `udp_rmem_release`). While the owner does not read, two kinds of
//! message arrive:
//!
//! - **The peer's stream data**, at most what its windows grant. Each byte of it costs what the
//!   kernel charged a message carrying stream data, per stream byte it carried. The kernel charges
//!   a datagram its allocation, a GRO train a little more than its payload and a 100-byte datagram
//!   nine times it (Linux 6.x), so the charge is learned, as Linux TCP learns its `scaling_ratio`
//!   from each segment's length over its truesize (`tcp_measure_rcv_mss`), and not assumed: a fixed
//!   four times the payload, the rule this replaces, was wrong both ways.
//! - **Messages the peer sends unprompted by window**: an acknowledgement for each ack-eliciting
//!   packet this side has in flight, at most (RFC 9000 §13.2: a receiver may acknowledge every
//!   one), and its loss probes, at most two at each probe timeout (RFC 9002 §6.2.4), the timeouts
//!   doubling (§6.2.1) from at least `kGranularity` until the idle timeout ends the connection.
//!   Retransmissions take no new window, so they are counted here, at the largest charge seen.
//!
//! A sample is one message the driver read as the only one queued, so the charge the kernel
//! reported before the read was that message's exactly (the driver's singleton rule). The worst of
//! each kind is held over the current window epoch and the last that brought a sample: an epoch
//! ends once the peer has consumed the room it began with, so every window still granted was
//! granted within the two, and the worst covers what it may still bring. An epoch without a sample
//! (a socket read behind a backlog, under load, yields none) keeps the worst it had. A drop the
//! kernel reports says the samples no longer describe the traffic, and they are forgotten: the room
//! falls to the first window until new ones come.

use std::time::Duration;

use crate::credit::K_GRANULARITY;

/// One message the kernel charged, read as the only one queued.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Charged {
    /// The bytes the kernel charged it.
    pub charged: u64,
    /// The flow-controlled stream bytes it carried, the window it consumed.
    pub stream: u64,
    /// The datagrams it held: one, or a coalesced train's (`UDP_GRO`), which the kernel charges as
    /// one message.
    pub datagrams: u64,
}

/// A socket's queue as the kernel accounts it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Queue {
    /// The buffer the queue is charged against, as the socket reports it.
    pub buffer: u64,
    /// The most the kernel may still charge for datagrams already read.
    pub release_lag: u64,
}

/// The messages a socket's peers may send unprompted by window while its owner does not read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Unprompted {
    /// Acknowledgements: this side's ack-eliciting packets in flight to them.
    pub acknowledgements: u64,
    /// Loss probes, possibly full-sized: [`probes_within`] their idle timeouts.
    pub probes: u64,
}

/// The loss probes a peer may send before its idle timeout `idle` ends the connection: two at
/// each probe timeout (RFC 9002 §6.2.4), the timeouts doubling (§6.2.1) from at least
/// `kGranularity` (§6.1.2), so twice the doublings from `kGranularity` to `idle`.
pub fn probes_within(idle: Duration) -> u64 {
    let base = K_GRANULARITY.as_nanos().max(1);
    let mut reach = base;
    let mut doublings = 0u64;
    // Bound: 128 doublings pass any `u128` of nanoseconds, a `Duration`'s whole range.
    for _ in 0..u128::BITS {
        if reach >= idle.as_nanos() {
            break;
        }
        reach = reach.saturating_mul(2);
        doublings = doublings.saturating_add(1);
    }
    doublings.saturating_add(1).saturating_mul(2)
}

/// The worst seen of a kind of sample, over the current epoch and the last.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Worst<T> {
    now: Option<T>,
    last: Option<T>,
}

impl<T: Copy> Worst<T> {
    /// Takes `sample` if `worse` says it is worse than the epoch's.
    fn take(&mut self, sample: T, worse: impl Fn(T, T) -> bool) {
        self.now = Some(match self.now {
            Some(held) if !worse(sample, held) => held,
            _ => sample,
        });
    }
    /// The worst of the two epochs.
    fn get(&self, worse: impl Fn(T, T) -> bool) -> Option<T> {
        match (self.now, self.last) {
            (Some(now), Some(last)) => Some(if worse(last, now) { last } else { now }),
            (now, last) => now.or(last),
        }
    }
    /// A new epoch: the current one's worst becomes the last's. An epoch that brought no sample
    /// leaves the last as it was, so a socket that is never read alone (behind a backlog under
    /// sustained load, where no read is a sample) keeps the worst it learned.
    fn next_epoch(&mut self) {
        if self.now.is_some() {
            self.last = self.now.take();
        }
    }
}

/// Whether `(charged, stream)` `a` costs more a stream byte than `b`.
fn costlier(a: (u64, u64), b: (u64, u64)) -> bool {
    u128::from(a.0).saturating_mul(u128::from(b.1))
        > u128::from(b.0).saturating_mul(u128::from(a.1))
}

fn greater(a: u64, b: u64) -> bool {
    a > b
}

/// What a socket's queue is charged for its peers' traffic, learned from exact samples (the
/// module's doc).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReceiveCharge {
    /// The greatest charge a stream byte of a message carrying stream data, as `(charged, stream)`:
    /// a datagram's or a coalesced train's.
    data: Worst<(u64, u64)>,
    /// The greatest charge of a datagram that came alone: what a probe, one datagram, may cost.
    lone: Worst<u64>,
    /// The greatest charge of a datagram that came alone carrying no stream data: what an
    /// acknowledgement costs.
    control: Worst<u64>,
    /// The greatest charge a datagram of a coalesced train, its charge over its datagrams: what a
    /// datagram is taken to cost until one has come alone.
    share: Worst<u64>,
    /// Stream bytes the peers consumed in the current epoch.
    consumed: u64,
    /// The room the current epoch began with: it ends once that much is consumed.
    epoch: u64,
}

impl ReceiveCharge {
    /// Nothing measured yet.
    pub const fn new() -> Self {
        Self {
            data: Worst {
                now: None,
                last: None,
            },
            lone: Worst {
                now: None,
                last: None,
            },
            control: Worst {
                now: None,
                last: None,
            },
            share: Worst {
                now: None,
                last: None,
            },
            consumed: 0,
            epoch: 0,
        }
    }

    /// One message the kernel charged, read as the only one queued.
    pub fn sample(&mut self, message: Charged) {
        if message.charged == 0 || message.datagrams == 0 {
            return;
        }
        if message.stream > 0 {
            self.data.take((message.charged, message.stream), costlier);
        }
        if message.datagrams == 1 {
            self.lone.take(message.charged, greater);
            if message.stream == 0 {
                self.control.take(message.charged, greater);
            }
        } else {
            let share = message.charged.div_ceil(message.datagrams);
            self.share.take(share, greater);
        }
    }

    /// The peers consumed `stream` bytes more of their windows; `room` is the room in force. An
    /// epoch ends once the room it began with is consumed.
    pub fn consumed(&mut self, stream: u64, room: u64) {
        if self.epoch == 0 {
            self.epoch = room.max(1);
        }
        self.consumed = self.consumed.saturating_add(stream);
        if self.consumed >= self.epoch {
            self.data.next_epoch();
            self.lone.next_epoch();
            self.control.next_epoch();
            self.share.next_epoch();
            self.consumed = 0;
            self.epoch = room.max(1);
        }
    }

    /// The kernel dropped what the queue could not hold: the samples no longer describe the
    /// traffic, and are forgotten until new ones come.
    pub fn forget(&mut self) {
        *self = Self::new();
    }

    /// Whether a message carrying stream data has been sampled, and so a room can be told.
    pub fn measured(&self) -> bool {
        self.data.get(costlier).is_some()
    }

    /// The stream bytes the queue holds for the peers' windows, `unprompted` messages on top:
    /// `None` until a message carrying stream data has been sampled. Each unprompted message is a
    /// datagram that comes alone: a probe is charged as the costliest datagram that came alone,
    /// an acknowledgement as the costliest that carried no stream data (or as a probe before one
    /// has been sampled). Before any datagram has come alone, a coalesced train's share a
    /// datagram stands in; a train is never a probe's charge, whose datagrams it holds many of.
    pub fn room(&self, queue: Queue, unprompted: Unprompted) -> Option<u64> {
        let (charged, stream) = self.data.get(costlier)?;
        let lone = self
            .lone
            .get(greater)
            .or_else(|| self.share.get(greater))
            .unwrap_or(charged);
        let acknowledgement = self.control.get(greater).unwrap_or(lone);
        let reserved = unprompted
            .acknowledgements
            .saturating_mul(acknowledgement)
            .saturating_add(unprompted.probes.saturating_mul(lone));
        let usable = queue
            .buffer
            .saturating_sub(queue.release_lag)
            .saturating_sub(reserved);
        let room = u128::from(usable)
            .saturating_mul(u128::from(stream))
            .checked_div(u128::from(charged))
            .unwrap_or(0);
        Some(u64::try_from(room).unwrap_or(u64::MAX))
    }
}

/// The kernel's account of a socket's queue at one read (hyper-rt's `UdpSocket::queue_charge`,
/// hyper-tokio's own).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Account {
    /// Bytes charged for what the queue holds.
    pub charged: u64,
    /// The buffer they are charged against.
    pub buffer: u64,
    /// The datagrams dropped for want of room since the socket was made, where the kernel says.
    pub drops: Option<u64>,
    /// The most the kernel may still charge for datagrams already read.
    pub release_lag: u64,
    /// The least the queue may be charged one message beyond what `charged` tells.
    pub message_floor: u64,
}

/// One socket's [`ReceiveCharge`] as its driver keeps it: the kernel's account of its queue, what
/// it is learned to charge, and the room in force. A driver reads the account before each drain
/// ([`SocketCharge::before`]), and after it ([`SocketCharge::wants_after`]) only when the drain may
/// have been a sample: it took exactly one message from a queue it found with nothing behind it
/// and left empty. Then the account before was that message's charge exactly, if the account
/// after is nothing (the singleton rule: a datagram that came between the two reads shows as a
/// second message, or as a charge after).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SocketCharge {
    queue: Option<Queue>,
    charge: ReceiveCharge,
    drops: Option<u64>,
    room: u64,
    backlog: bool,
}

impl SocketCharge {
    /// Nothing measured, the room `room` in force.
    pub const fn new(room: u64) -> Self {
        Self::seeded(room, ReceiveCharge::new())
    }

    /// Seeded with `charge`, what another socket of the host has learned of its receive path,
    /// until its own account is read.
    pub const fn seeded(room: u64, charge: ReceiveCharge) -> Self {
        Self {
            queue: None,
            charge,
            drops: None,
            room,
            backlog: false,
        }
    }

    /// What its queue is learned to charge.
    pub fn charge(&self) -> ReceiveCharge {
        self.charge
    }

    /// The room in force.
    pub fn room_in_force(&self) -> u64 {
        self.room
    }

    /// Whether its room is told from what it measured, not the first windows before anything was.
    pub fn measured(&self) -> bool {
        self.queue.is_some() && self.charge.measured()
    }

    /// A drain begins with the queue's account `before`: the queue's buffer and lag are taken, and
    /// a drop since the last account forgets the samples.
    pub fn before(&mut self, before: Account) {
        if before
            .drops
            .zip(self.drops)
            .is_some_and(|(now, seen)| now > seen)
        {
            self.charge.forget();
        }
        self.drops = before.drops.or(self.drops);
        self.queue = Some(Queue {
            buffer: before.buffer,
            release_lag: before.release_lag,
        });
    }

    /// Whether the drain that began with `before` may have been a sample, so its driver reads the
    /// account after it: it took exactly `messages == 1` from a queue it found charged and with no
    /// backlog left by the drain before, and `emptied` it.
    pub fn wants_after(&self, before: &Account, messages: usize, emptied: bool) -> bool {
        !self.backlog && messages == 1 && emptied && before.charged > 0
    }

    /// A drain ended: it began with `before`, the account after it was `after` (when
    /// [`SocketCharge::wants_after`] said to read it), it took `datagrams` of `bytes` payload that
    /// carried `consumed` stream bytes, and it left the socket `emptied` or not. A message carries
    /// no more stream bytes than its own payload, and the kernel charges it no less than that
    /// payload: a sample past either bound says the counts did not describe the message alone, and
    /// is not taken.
    pub fn after(
        &mut self,
        before: Option<Account>,
        after: Option<Account>,
        consumed: u64,
        (datagrams, bytes): (u64, u64),
        emptied: bool,
    ) {
        if let (Some(before), Some(after)) = (before, after)
            && after.charged == 0
            && consumed <= bytes
            && before.charged >= bytes
        {
            self.charge.sample(Charged {
                charged: before.charged.max(before.message_floor),
                stream: consumed,
                datagrams,
            });
        }
        self.charge.consumed(consumed, self.room);
        self.backlog = !emptied;
    }

    /// The room its queue holds for its peers' stream data with `unprompted` messages on top,
    /// once its account has been read and a message carrying stream data measured.
    pub fn room(&self, unprompted: Unprompted) -> Option<u64> {
        self.charge.room(self.queue?, unprompted)
    }

    /// The room now in force.
    pub fn set_room(&mut self, room: u64) {
        self.room = room;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUEUE: Queue = Queue {
        buffer: 425_984,
        release_lag: 425_984 >> 2,
    };

    /// Nothing is told before a message carrying stream data is sampled.
    #[test]
    fn no_room_is_told_before_stream_data_is_sampled() {
        let mut charge = ReceiveCharge::new();
        assert_eq!(charge.room(QUEUE, Unprompted::default()), None);
        charge.sample(Charged {
            charged: 895,
            stream: 0,
            datagrams: 1,
        });
        assert_eq!(charge.room(QUEUE, Unprompted::default()), None);
        assert!(!charge.measured());
    }

    /// A coalesced train is never a probe's charge: probes are charged at its share a datagram
    /// until a datagram comes alone, then at that datagram's.
    #[test]
    fn a_train_is_no_probes_charge() {
        let mut charge = ReceiveCharge::new();
        charge.sample(Charged {
            charged: 70_000,
            stream: 64_000,
            datagrams: 45,
        });
        let unprompted = Unprompted {
            acknowledgements: 0,
            probes: 32,
        };
        let share = 70_000u64.div_ceil(45);
        let usable = QUEUE.buffer - QUEUE.release_lag - 32 * share;
        assert_eq!(
            charge.room(QUEUE, unprompted),
            Some(usable * 64_000 / 70_000),
            "probes at the train's share a datagram until a datagram comes alone"
        );
        charge.sample(Charged {
            charged: 2_304,
            stream: 1_200,
            datagrams: 1,
        });
        let usable = QUEUE.buffer - QUEUE.release_lag - 32 * 2_304;
        assert_eq!(
            charge.room(QUEUE, unprompted),
            Some(usable * 1_200 / 2_304),
            "then at the datagram's that came alone"
        );
    }
    /// The room is the usable queue at the worst charge a stream byte: a GRO train of 64,000
    /// stream bytes charged 70,000 and a 100-byte datagram charged 895 leave the room at the
    /// small one's cost.
    #[test]
    fn the_room_is_the_queue_at_the_costliest_stream_byte() {
        let mut charge = ReceiveCharge::new();
        charge.sample(Charged {
            charged: 70_000,
            stream: 64_000,
            datagrams: 1,
        });
        let usable = QUEUE.buffer - QUEUE.release_lag;
        assert_eq!(
            charge.room(QUEUE, Unprompted::default()),
            Some(usable * 64_000 / 70_000)
        );
        charge.sample(Charged {
            charged: 895,
            stream: 100,
            datagrams: 1,
        });
        assert_eq!(
            charge.room(QUEUE, Unprompted::default()),
            Some(usable * 100 / 895)
        );
    }

    /// Unprompted messages are taken off the queue first: acknowledgements at the costliest
    /// message without stream data, probes at the costliest of any.
    #[test]
    fn unprompted_messages_are_reserved_first() {
        let mut charge = ReceiveCharge::new();
        charge.sample(Charged {
            charged: 2_290,
            stream: 1_400,
            datagrams: 1,
        });
        charge.sample(Charged {
            charged: 768,
            stream: 0,
            datagrams: 1,
        });
        let unprompted = Unprompted {
            acknowledgements: 10,
            probes: 4,
        };
        let usable = QUEUE.buffer - QUEUE.release_lag - 10 * 768 - 4 * 2_290;
        assert_eq!(charge.room(QUEUE, unprompted), Some(usable * 1_400 / 2_290));
    }

    /// An epoch ends once the room it began with is consumed; the worst of the last epoch holds
    /// through the next, and is let go after once a later epoch brought its own.
    #[test]
    fn the_worst_holds_for_the_epoch_after_its_own() {
        let mut charge = ReceiveCharge::new();
        let room = 100_000;
        charge.sample(Charged {
            charged: 895,
            stream: 100,
            datagrams: 1,
        });
        charge.consumed(room, room);
        charge.sample(Charged {
            charged: 70_000,
            stream: 64_000,
            datagrams: 1,
        });
        let usable = QUEUE.buffer - QUEUE.release_lag;
        assert_eq!(
            charge.room(QUEUE, Unprompted::default()),
            Some(usable * 100 / 895),
            "the last epoch's worst still holds"
        );
        charge.consumed(room, room);
        assert_eq!(
            charge.room(QUEUE, Unprompted::default()),
            Some(usable * 64_000 / 70_000),
            "two epochs on, only the train's"
        );
    }

    /// Epochs that bring no sample keep the worst learned: a socket read only behind a backlog
    /// keeps its room.
    #[test]
    fn epochs_without_samples_keep_what_was_learned() {
        let mut charge = ReceiveCharge::new();
        charge.sample(Charged {
            charged: 2_290,
            stream: 1_400,
            datagrams: 1,
        });
        let before = charge.room(QUEUE, Unprompted::default());
        for _ in 0..8 {
            charge.consumed(100_000, 100_000);
        }
        assert_eq!(charge.room(QUEUE, Unprompted::default()), before);
    }

    /// A drop forgets every sample.
    #[test]
    fn a_drop_forgets_the_samples() {
        let mut charge = ReceiveCharge::new();
        charge.sample(Charged {
            charged: 2_290,
            stream: 1_400,
            datagrams: 1,
        });
        charge.forget();
        assert_eq!(charge.room(QUEUE, Unprompted::default()), None);
    }

    /// Two probes at each timeout, the timeouts doubling from 1 ms: a 30-second idle timeout is
    /// reached at the fifteenth doubling (32.8 s), so sixteen timeouts, 32 probes.
    #[test]
    fn probes_double_from_the_granularity_to_the_idle_timeout() {
        assert_eq!(probes_within(Duration::from_secs(30)), 32);
        assert_eq!(probes_within(Duration::from_millis(1)), 2);
        assert_eq!(probes_within(Duration::ZERO), 2);
    }

    /// A drain is a sample only within the physical bounds: a message carries no more stream
    /// bytes than its payload, and is charged no less than its payload. Do: one message of 1,200
    /// bytes charged 2,304 whose window count moved by 9,000 (a frame landed past a gap), then the
    /// same message counted right. Expect: the first is no sample, the second is.
    #[test]
    fn a_sample_past_the_messages_own_bytes_is_not_taken() {
        let before = Account {
            charged: 2_304,
            buffer: QUEUE.buffer,
            drops: Some(0),
            release_lag: QUEUE.release_lag,
            message_floor: 0,
        };
        let after = Account {
            charged: 0,
            ..before
        };
        let mut socket = SocketCharge::new(15_000);
        socket.before(before);
        socket.after(Some(before), Some(after), 9_000, (1, 1_200), true);
        assert_eq!(socket.room(Unprompted::default()), None, "no sample taken");
        socket.before(before);
        socket.after(Some(before), Some(after), 1_150, (1, 1_200), true);
        let usable = QUEUE.buffer - QUEUE.release_lag;
        assert_eq!(
            socket.room(Unprompted::default()),
            Some(usable * 1_150 / 2_304)
        );
    }
}
