//! A leader's patience with its own disk in the disk mode (kiln's CP-T2.2, Focal's condition 5,
//! `docs/durable.md` §4.2). Its followers' disks commit what it proposes while its own lags
//! (`Config::apply_unpersisted`), so a stalled disk costs the group nothing until the backlog the
//! disk doesn't hold fills the room the leader's budget has left and its proposals are refused:
//! that is when to hand the lead on, less the time a hand-over takes, so that the heir leads before
//! the room is gone and not after a burst. A hand-over restores no commit the stalled disk blocks,
//! every majority needing that disk, so it comes no sooner. Each part is measured, none picked:
//!
//! - the room: the budget's own allowance ([`Budget::room`](crate::Budget::room)), and the entries
//!   the core takes not yet durable before it refuses one (`Limits::unstable_entries`);
//! - the entries the backlog takes before a proposal is refused, reckoned as the core holds them:
//!   each its payload, at the mean of those it holds, and its slot, the core's `Vec` of slots
//!   doubling when full, charged at once ([`entries_to_fill`]);
//! - the backlog's growth, entries a second at the tail of its rate: RFC 6298's bound over the rate
//!   measured over each wait on the disk a completion ended, `smoothed + 4 · deviation` (§2.2 and
//!   §2.3, alpha 1/8 and beta 1/4, the fold hyper-timing keeps; RFC 6298's `G` has no part, a rate
//!   having no clock's granularity), or the rate of the wait under way where it is faster, so that
//!   a burst counts as it comes;
//! - the hand-over's time, from the leader's order to the first commit of a later term it sees, at
//!   the same bound over the hand-overs it ordered; before any, the election its group's timing law
//!   expects and that election's round (`hyper_raft::Timing`).
//!
//! Zero growth fills nothing and sets no deadline, and every product and quotient is checked: one
//! past what 64 bits count sets none. Nothing doubles: a second hand-over needs the new leader's
//! own backlog to fill its room, its disk not slow when it was named (the heir rule), so a group
//! hands over at most once a fill time, less a hand-over, however its disks stall in turn.

use hyper_timing::ExchangeRtt;

/// Nanoseconds a second.
const SECOND_NS: u128 = 1_000_000_000;

/// The backlog's growth is counted in thousandths of an entry a second: RFC 6298's fold works in
/// whole numbers, and a group taking a few entries a second keeps its rate's precision.
const PER_ENTRY: u128 = 1_000;

/// The fewest slots `Vec` grows a buffer of slots this size to (Rust's `RawVec`, its
/// `MIN_NON_ZERO_CAP` for elements of up to 1 KiB).
const FEWEST_SLOTS: u64 = 4;

/// What a replica in the disk mode measures for its patience.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Patience {
    /// The backlog's growth over each wait on the disk a durable completion ended, thousandths of
    /// an entry a second.
    growth: ExchangeRtt,
    /// Where the wait under way is counted from, and the log's last index there: the latest drive
    /// that took a durable completion, as it settled, or the first drive.
    mark: Option<(u64, u64)>,
    /// A durable completion was taken in the drive under way: the mark moves once it has settled.
    waited: bool,
    /// Each hand-over's time, nanoseconds.
    hand_overs: ExchangeRtt,
    /// A hand-over ordered and not yet seen done: when first ordered, and in which term.
    ordered: Option<(u64, u64)>,
    /// The bytes an entry's payload takes, the mean of the entries the core held not yet durable at
    /// the latest drive that held any.
    payload: Option<u64>,
}

impl Patience {
    /// A durable completion taken at `now_ns`, the log ending at `last` as the drive began: the
    /// wait it ended is a sample of the backlog's growth, once a drive.
    pub(crate) fn on_durable(&mut self, now_ns: u64, last: u64) {
        if std::mem::replace(&mut self.waited, true) {
            return;
        }
        if let Some((since, from)) = self.mark
            && let Some(rate) = rate(last.saturating_sub(from), now_ns.saturating_sub(since))
        {
            self.growth.on_sample(rate);
        }
    }

    /// The drive at `now_ns` settled, the log ending at `last` and the entries not yet durable
    /// taking `payload` bytes each, if it holds any: the next wait counts from here if the drive
    /// took a completion, or if none was counted from yet.
    pub(crate) fn settled(&mut self, now_ns: u64, last: u64, payload: Option<u64>) {
        if std::mem::replace(&mut self.waited, false) || self.mark.is_none() {
            self.mark = Some((now_ns, last));
        }
        if payload.is_some() {
            self.payload = payload;
        }
    }

    /// The backlog's growth at `now_ns`, the log ending at `last`, thousandths of an entry a second:
    /// the tail of the waits measured, or the rate of the wait under way where it is faster; `None`
    /// where neither grows.
    pub(crate) fn growth(&self, now_ns: u64, last: u64) -> Option<u64> {
        let tail = self.growth.tail_ns(0).unwrap_or(0);
        let under_way = self
            .mark
            .and_then(|(since, from)| rate(last.saturating_sub(from), now_ns.saturating_sub(since)))
            .unwrap_or(0);
        Some(tail.max(under_way)).filter(|rate| *rate > 0)
    }

    /// The bytes an entry's payload takes, as measured; zero before an entry was held.
    pub(crate) fn payload(&self) -> u64 {
        self.payload.unwrap_or(0)
    }

    /// A hand-over ordered at `now_ns` in `term`. One ordered again in the same term, the core
    /// having given up the first, is timed from the first: what the group waited is the whole.
    pub(crate) fn ordered(&mut self, now_ns: u64, term: u64) {
        if self
            .ordered
            .is_none_or(|(_, ordered_in)| ordered_in != term)
        {
            self.ordered = Some((now_ns, term));
        }
    }

    /// At `now_ns` the member's commit is an entry of `term`: a later term's than a hand-over
    /// ordered ends that hand-over, timed.
    pub(crate) fn committed(&mut self, now_ns: u64, term: u64) {
        if let Some((since, ordered_in)) = self.ordered
            && term > ordered_in
        {
            self.hand_overs.on_sample(now_ns.saturating_sub(since));
            self.ordered = None;
        }
    }

    /// The time a hand-over is taken to need, nanoseconds: the tail of those timed, or before any
    /// `prior_ns`.
    pub(crate) fn hand_over_ns(&self, prior_ns: u64) -> u64 {
        self.hand_overs.tail_ns(0).unwrap_or(prior_ns)
    }

    /// The hand-overs timed.
    pub(crate) fn hand_overs_timed(&self) -> u64 {
        self.hand_overs.samples()
    }
}

/// `entries` over `over_ns`, thousandths of an entry a second: `None` over no time, or past what
/// 64 bits count.
fn rate(entries: u64, over_ns: u64) -> Option<u64> {
    let per_second = u128::from(entries)
        .checked_mul(PER_ENTRY.checked_mul(SECOND_NS)?)?
        .checked_div(u128::from(over_ns))?;
    u64::try_from(per_second).ok()
}

/// What the room's fill is reckoned from, as the leader holds its backlog.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Backlog {
    /// The bytes the budget has left ([`Budget::room`](crate::Budget::room)).
    pub(crate) room: u64,
    /// The entries the core holds not yet durable.
    pub(crate) entries: u64,
    /// The slots it holds them in.
    pub(crate) slots: u64,
    /// The entries more it takes not yet durable before it refuses one.
    pub(crate) entry_room: u64,
    /// The bytes a slot takes.
    pub(crate) slot_bytes: u64,
    /// The bytes an entry's payload takes.
    pub(crate) payload: u64,
}

/// The entries the backlog takes before a proposal is refused. Each proposal reserves its payload
/// and is refused where the room is short of it (hyper-durable's budget, `docs/durable.md` §6);
/// one that finds every slot taken is taken, and the slots it doubles (`Vec`'s growth, at least
/// [`FEWEST_SLOTS`]) are charged at once, a jump past which the next proposal is refused. The core
/// refuses past its own bound on entries not yet durable as well. Reckoned a doubling at a time,
/// 64 at the most: a slot count 64 bits hold.
pub(crate) fn entries_to_fill(backlog: &Backlog) -> u64 {
    let (payload, slot) = (backlog.payload, backlog.slot_bytes);
    let mut room = backlog.room;
    let mut held = backlog.entries;
    let mut slots = backlog.slots.max(held);
    let mut taken = 0u64;
    for _ in 0..u64::BITS {
        // Into a free slot an entry takes its payload alone.
        let free = slots.saturating_sub(held);
        let fit = room.checked_div(payload).map_or(free, |fit| fit.min(free));
        taken = taken.saturating_add(fit);
        room = room.saturating_sub(fit.saturating_mul(payload));
        held = held.saturating_add(fit);
        if fit < free || payload > room {
            break;
        }
        // The next finds every slot taken: taken, its doubling charged at once.
        taken = taken.saturating_add(1);
        held = held.saturating_add(1);
        let doubled = slots.saturating_mul(2).max(FEWEST_SLOTS);
        let cost = doubled
            .saturating_sub(slots)
            .saturating_mul(slot)
            .saturating_add(payload);
        if cost > room {
            break;
        }
        room = room.saturating_sub(cost);
        slots = doubled;
    }
    taken.min(backlog.entry_room)
}

/// The time `entries` take to come at `growth` thousandths of an entry a second, nanoseconds:
/// `None` at no growth, which fills nothing, or past what 64 bits count.
pub(crate) fn fill_ns(entries: u64, growth: u64) -> Option<u64> {
    let ns = u128::from(entries)
        .checked_mul(PER_ENTRY.checked_mul(SECOND_NS)?)?
        .checked_div(u128::from(growth))?;
    u64::try_from(ns).ok()
}

/// When a leader's patience ends, given the time `fill_ns` its room takes to fill as it reads it at
/// `now_ns`, the time `hand_over_ns` a hand-over takes, and `anchor`, the later of its oldest write
/// out's submission and the time since which a voter has been there to take the lead: once its
/// room would fill within a hand-over, and not before a hand-over's time has passed on both, so
/// that a disk answering sooner than a hand-over could, or an heir come a moment before the
/// leader's own disk answers (Focal's condition 6, a disk the members share), begins none.
pub(crate) fn point(now_ns: u64, fill_ns: u64, hand_over_ns: u64, anchor: u64) -> u64 {
    now_ns
        .saturating_add(fill_ns)
        .saturating_sub(hand_over_ns)
        .max(anchor.saturating_add(hand_over_ns))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;

    /// Zero growth fills nothing: no deadline. A count or rate past what 64 bits count in
    /// nanoseconds sets none either, and no quotient divides by zero.
    #[test]
    fn zero_growth_sets_no_deadline_and_the_arithmetic_is_checked() {
        assert_eq!(fill_ns(1_000, 0), None, "no growth");
        assert_eq!(fill_ns(0, 0), None, "no room and no growth");
        assert_eq!(fill_ns(0, 1), Some(0), "no room fills at once");
        assert_eq!(fill_ns(u64::MAX, 1), None, "past 64 bits of nanoseconds");
        // 2,048 entries at a thousand a second: 2.048 s.
        assert_eq!(fill_ns(2_048, 1_000_000), Some(2_048 * MS));
        assert_eq!(rate(1, 0), None, "over no time");
        assert_eq!(rate(u64::MAX, 1), None, "past 64 bits");
        assert_eq!(
            rate(2, 2 * MS),
            Some(1_000_000),
            "a thousand entries a second"
        );
        let idle = Patience::default();
        assert_eq!(
            idle.growth(10 * MS, 4_096),
            None,
            "nothing measured, nothing grows"
        );
    }

    /// The entries the backlog takes before a refusal, reckoned as the core's slots double: a
    /// payload into each free slot, the doubling charged with the entry that finds them full, and
    /// the proposal after a doubling past the room refused.
    #[test]
    fn the_backlog_fills_its_room_as_its_slots_double() {
        // Free slots alone: 100 bytes a payload into 1,000 bytes of room and 64 free slots.
        let roomy = Backlog {
            room: 1_000,
            entries: 0,
            slots: 64,
            entry_room: u64::MAX,
            slot_bytes: 56,
            payload: 100,
        };
        assert_eq!(entries_to_fill(&roomy), 10);
        // Slots full at 4: the fifth is taken and doubles them (4 slots more, 224 bytes, and its
        // payload, 12); 8 then fill by payloads; the ninth doubles them again (8 slots, 448 bytes)
        // past the room left.
        let full = Backlog {
            room: 300 + 12 * 3,
            entries: 4,
            slots: 4,
            entry_room: u64::MAX,
            slot_bytes: 56,
            payload: 12,
        };
        // 336: the fifth (236) leaves 100; three payloads (36) leave 64; the ninth costs 460.
        assert_eq!(entries_to_fill(&full), 1 + 3 + 1);
        // The core's own bound comes first where it is nearer.
        assert_eq!(
            entries_to_fill(&Backlog {
                entry_room: 2,
                ..roomy
            }),
            2
        );
        // No payload and no slot cost fill nothing but the core's bound.
        assert_eq!(
            entries_to_fill(&Backlog {
                payload: 0,
                slot_bytes: 0,
                entry_room: 7,
                ..roomy
            }),
            7
        );
        // A room short of one payload takes none.
        assert_eq!(entries_to_fill(&Backlog { room: 11, ..full }), 0);
    }

    /// Focal's correction to condition 5: the growth [`entries_to_fill`] reckons (no slots, then
    /// [`FEWEST_SLOTS`], then twice as many each time every slot is taken) is std's `RawVec` at
    /// this toolchain, not a promise of `Vec`'s. Do: push the core's entries into a `Vec` one at a
    /// time. Expect: each capacity the model predicts, so a toolchain that grows `Vec` otherwise
    /// fails here rather than moving the patience unseen.
    #[test]
    fn a_vec_of_the_cores_entries_grows_as_the_patience_reckons() {
        let mut entries: Vec<hyper_raft::proto::Entry> = Vec::new();
        let mut slots = 0u64;
        let count = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
        assert_eq!(
            count(entries.capacity()),
            slots,
            "an empty Vec holds no slots"
        );
        for _ in 0..4_096 {
            if count(entries.len()) == slots {
                slots = slots.saturating_mul(2).max(FEWEST_SLOTS);
            }
            entries.push(hyper_raft::proto::Entry::default());
            assert_eq!(
                count(entries.capacity()),
                slots,
                "after {} entries",
                entries.len()
            );
        }
    }

    /// The growth is RFC 6298's fold over each wait's rate, its tail `smoothed + 4 · deviation`,
    /// sample for sample; and the wait under way's rate counts where faster, a burst as it comes.
    #[test]
    fn the_growth_is_the_tail_of_the_waits_and_the_wait_under_way() {
        let mut patience = Patience::default();
        patience.settled(0, 1_000, None);
        // Three waits of 1 ms each, one, one and two entries in each.
        let mut last = 1_000;
        for (at, grown) in [(MS, 1), (2 * MS, 1), (3 * MS, 2)] {
            last += grown;
            patience.on_durable(at, last);
            // A second completion in the same drive is no second wait.
            patience.on_durable(at, last + 1_000);
            patience.settled(at, last, Some(12));
        }
        assert_eq!(patience.payload(), 12);
        // RFC 6298 §2.2 and §2.3 by hand: R = 1, then 1, then 2 entries a millisecond, in
        // thousandths of an entry a second.
        let (mut srtt, mut rttvar) = (1_000_000u64, 500_000u64);
        for r in [1_000_000u64, 2_000_000] {
            rttvar = rttvar - rttvar / 4 + srtt.abs_diff(r) / 4;
            srtt = srtt - srtt / 8 + r / 8;
        }
        assert_eq!(patience.growth(3 * MS, last), Some(srtt + 4 * rttvar));
        // The wait under way grows faster than the tail: 100 entries in a millisecond.
        assert_eq!(patience.growth(4 * MS, last + 100), Some(100_000_000));
    }

    /// A hand-over is timed from its first order to the first commit of a later term, an order
    /// again in the same term timed from the first; the tail of those timed stands for the next,
    /// and before any the prior does.
    #[test]
    fn a_hand_over_is_timed_from_its_first_order_to_a_later_terms_commit() {
        let mut patience = Patience::default();
        assert_eq!(
            patience.hand_over_ns(70 * MS),
            70 * MS,
            "the prior before any"
        );
        patience.ordered(10 * MS, 4);
        patience.ordered(30 * MS, 4);
        patience.committed(40 * MS, 4);
        assert_eq!(
            patience.hand_overs_timed(),
            0,
            "a commit of its own term ends nothing"
        );
        patience.committed(50 * MS, 5);
        assert_eq!(patience.hand_overs_timed(), 1);
        // One sample of 40 ms: smoothed 40, deviation 20, the tail 40 + 4 · 20.
        assert_eq!(patience.hand_over_ns(70 * MS), 120 * MS);
    }

    /// The patience ends once the room would fill within a hand-over, and not before a hand-over's
    /// time has passed since the anchor.
    #[test]
    fn the_point_is_a_hand_over_before_the_room_fills_and_after_the_anchor() {
        // 10 s to fill, a hand-over of 100 ms: 9.9 s on.
        assert_eq!(point(MS, 10_000 * MS, 100 * MS, 0), 9_901 * MS);
        // Already within a hand-over of full: at once, once the anchor's hand-over has passed.
        assert_eq!(point(1_000 * MS, 50 * MS, 100 * MS, 0), 950 * MS);
        assert_eq!(point(1_000 * MS, 50 * MS, 100 * MS, 990 * MS), 1_090 * MS);
        // Saturating, never wrapping.
        assert_eq!(point(u64::MAX, u64::MAX, 0, 0), u64::MAX);
        assert_eq!(point(0, 0, u64::MAX, 0), u64::MAX);
    }
}
