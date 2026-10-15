//! A hierarchical timing wheel [A: Varghese & Lauck, "Hashed and hierarchical timing wheels", SOSP'87;
//! IEEE/ACM ToN 5(6) 1997 §VI.B]: arming, disarming and expiry in O(1), entries cascading to a finer level
//! as their deadline approaches (slates' `timer.rs`, ORIGIN.md).
//!
//! Each level has 64 slots and there are six levels, so the wheel spans 2^36 ticks; a deadline beyond
//! that is clamped to the horizon and re-armed when it cascades (tokio's and Kafka's rule). The tick is
//! the measured mean wake (docs/runtime.md §3.6).
//!
//! **Identifiers come from outside.** A timer is named by a slot index the caller allocates (the shard's
//! desk hands them to tasks, §3.4), and the wheel keeps one entry per index, allocated once: arming,
//! disarming, cascading and firing never allocate.
//!
//! **The next event in O(levels).** Each level keeps a 64-bit occupancy word, one bit per slot, so the
//! next tick at which some level has work is found with a rotate and a `trailing_zeros` per level. slates
//! rescanned every armed entry after a fire or a cancel of the earliest (docs/runtime.md §3.6).

use crate::error::RtError;

/// Format: slots per level, one bit each in a level's occupancy word.
pub const SLOTS_PER_LEVEL: usize = 64;
/// Format: levels: 64^6 = 2^36 ticks of range.
pub const LEVELS: usize = 6;
/// Format: log2 of the slots per level.
const SLOT_BITS: u32 = 6;
/// Format: the mask of a slot index within a level.
const SLOT_MASK: u64 = 63;
/// Format: "no entry" in a slot list's links.
const NONE: u32 = u32::MAX;

/// A timer entry.
#[derive(Clone, Copy, Debug)]
struct Entry {
    /// The deadline in ticks.
    deadline: u64,
    /// The word to report when the deadline passes.
    word: u64,
    next: u32,
    prev: u32,
    level: u8,
    slot: u8,
    armed: bool,
}

impl Entry {
    /// Format: an unarmed entry.
    const IDLE: Entry = Entry {
        deadline: 0,
        word: 0,
        next: NONE,
        prev: NONE,
        level: 0,
        slot: 0,
        armed: false,
    };
}

/// The wheel.
#[derive(Debug)]
pub struct Wheel {
    tick_ns: u64,
    now_tick: u64,
    entries: Box<[Entry]>,
    heads: Box<[u32]>,
    occupied: [u64; LEVELS],
    armed: usize,
    /// Ticks the wheel has examined (`expire_tick` calls) — the cost witness of `advance`.
    visits: u64,
}

impl Wheel {
    /// A wheel with `tick_ns` per tick, room for timers `0..capacity`, starting at `now_ns`.
    pub fn new(tick_ns: u64, capacity: usize, now_ns: u64) -> Self {
        let tick_ns = tick_ns.max(1);
        Self {
            tick_ns,
            now_tick: now_ns.checked_div(tick_ns).unwrap_or(0),
            entries: vec![Entry::IDLE; capacity].into_boxed_slice(),
            heads: vec![NONE; SLOTS_PER_LEVEL.saturating_mul(LEVELS)].into_boxed_slice(),
            occupied: [0; LEVELS],
            armed: 0,
            visits: 0,
        }
    }

    /// Ticks examined so far — what `advance` cost, in the unit its work is done in.
    pub const fn visits(&self) -> u64 {
        self.visits
    }

    /// Nanoseconds per tick.
    pub const fn tick_ns(&self) -> u64 {
        self.tick_ns
    }

    /// Armed timers.
    pub const fn armed(&self) -> usize {
        self.armed
    }

    /// The timers it has room for.
    pub fn capacity(&self) -> usize {
        self.entries.len()
    }

    /// Arms timer `id` to report `word` at `deadline_ns` (no earlier than the next tick). Refused for an id
    /// past the capacity or one already armed.
    pub fn arm(&mut self, id: u32, deadline_ns: u64, word: u64) -> Result<(), RtError> {
        let deadline = deadline_ns
            .div_ceil(self.tick_ns)
            .max(self.now_tick.saturating_add(1));
        let entry = self.entry_mut(id)?;
        if entry.armed {
            return Err(RtError::TimerState { id, armed: true });
        }
        *entry = Entry {
            deadline,
            word,
            armed: true,
            ..Entry::IDLE
        };
        self.link(id, deadline);
        self.armed = self.armed.saturating_add(1);
        Ok(())
    }

    /// Disarms timer `id`; refused for an id that is not armed (fired, or never armed).
    pub fn disarm(&mut self, id: u32) -> Result<(), RtError> {
        let entry = *self.entry(id)?;
        if !entry.armed {
            return Err(RtError::TimerState { id, armed: false });
        }
        self.unlink(id, entry);
        if let Ok(entry) = self.entry_mut(id) {
            *entry = Entry::IDLE;
        }
        self.armed = self.armed.saturating_sub(1);
        Ok(())
    }

    /// The next tick's time in nanoseconds at which the wheel has work (a timer to fire, or a level to
    /// cascade toward one), if any timer is armed: never later than the earliest deadline, and exactly it for
    /// a deadline within the finest level's rotation.
    pub fn next_deadline_ns(&self) -> Option<u64> {
        if self.armed == 0 {
            return None;
        }
        let tick = self.next_event_tick(u64::MAX);
        (tick != u64::MAX).then(|| tick.saturating_mul(self.tick_ns))
    }

    /// Advances to `now_ns`, collecting `(id, word)` of every expired timer into `fired` in deadline order
    /// within a tick; a fired timer is disarmed. Each step jumps straight to the next tick that has work
    /// ([`Wheel::next_event_tick`]), so the cost is per timer event, never per idle tick (slates measured a
    /// shard idle for 66,600 ticks examining 66,595 of them before, 2026-09-27).
    pub fn advance(&mut self, now_ns: u64, fired: &mut Vec<(u32, u64)>) {
        let target = now_ns.checked_div(self.tick_ns).unwrap_or(0);
        while self.now_tick < target {
            self.now_tick = self.next_event_tick(target);
            self.expire_tick(fired);
        }
    }

    /// The next tick after `now_tick`, at most `target`, at which some level has an occupied slot to process:
    /// for level `L`, the boundaries of its slots after `now_tick` fall every `64^L` ticks, and the first
    /// occupied one is the occupancy word rotated to start just past the current slot, its trailing zeros
    /// counting the boundaries skipped. An entry at level `L` lies less than one rotation of that level ahead
    /// (`level_and_slot`), so one rotation always meets it.
    fn next_event_tick(&self, target: u64) -> u64 {
        let mut best = target;
        for (level, occupied) in self.occupied.iter().enumerate() {
            if *occupied == 0 {
                continue;
            }
            let shift = level_shift(level);
            let base = self.now_tick >> shift;
            let start = u32::try_from(base.wrapping_add(1) & SLOT_MASK).unwrap_or(0);
            let skipped = u64::from(occupied.rotate_right(start).trailing_zeros());
            let boundary = base
                .saturating_add(1)
                .saturating_add(skipped)
                .checked_shl(shift)
                .unwrap_or(u64::MAX);
            best = best.min(boundary);
        }
        best
    }

    fn expire_tick(&mut self, fired: &mut Vec<(u32, u64)>) {
        self.visits = self.visits.saturating_add(1);
        let tick = self.now_tick;
        // The level-0 slot for this tick fires; a higher level's slot that this tick enters cascades.
        for level in 0..LEVELS {
            let shift = level_shift(level);
            let below = 1u64
                .checked_shl(shift)
                .map_or(0, |span| span.saturating_sub(1));
            if level > 0 && tick & below != 0 {
                break;
            }
            let slot = slot_index(tick >> shift);
            let head = head_index(level, slot);
            let Some(first) = self.heads.get_mut(head).map(|h| std::mem::replace(h, NONE)) else {
                continue;
            };
            self.clear_occupied(level, slot);
            let mut id = first;
            while id != NONE {
                let Ok(entry) = self.entry(id).copied() else {
                    break;
                };
                if entry.deadline <= tick || level == 0 {
                    fired.push((id, entry.word));
                    if let Ok(entry) = self.entry_mut(id) {
                        *entry = Entry::IDLE;
                    }
                    self.armed = self.armed.saturating_sub(1);
                } else {
                    self.link(id, entry.deadline);
                }
                id = entry.next;
            }
        }
    }

    fn level_and_slot(&self, deadline: u64) -> (usize, usize) {
        let delta = deadline.saturating_sub(self.now_tick).max(1);
        let magnitude = (u64::BITS - 1).saturating_sub(delta.leading_zeros());
        let level = usize::try_from(magnitude / SLOT_BITS)
            .unwrap_or(0)
            .min(LEVELS.saturating_sub(1));
        (level, slot_index(deadline >> level_shift(level)))
    }

    fn link(&mut self, id: u32, deadline: u64) {
        let (level, slot) = self.level_and_slot(deadline);
        let head = head_index(level, slot);
        let old = self.heads.get(head).copied().unwrap_or(NONE);
        if let Ok(entry) = self.entry_mut(id) {
            entry.next = old;
            entry.prev = NONE;
            entry.level = u8::try_from(level).unwrap_or(0);
            entry.slot = u8::try_from(slot).unwrap_or(0);
        }
        if old != NONE
            && let Ok(next) = self.entry_mut(old)
        {
            next.prev = id;
        }
        if let Some(h) = self.heads.get_mut(head) {
            *h = id;
        }
        if let Some(word) = self.occupied.get_mut(level) {
            *word |= 1u64 << slot;
        }
    }

    /// Splices `entry` (timer `id`, still linked) out of its slot list.
    fn unlink(&mut self, id: u32, entry: Entry) {
        let level = usize::from(entry.level);
        let slot = usize::from(entry.slot);
        let head = head_index(level, slot);
        if entry.prev == NONE {
            if let Some(h) = self.heads.get_mut(head)
                && *h == id
            {
                *h = entry.next;
            }
        } else if let Ok(p) = self.entry_mut(entry.prev) {
            p.next = entry.next;
        }
        if entry.next != NONE
            && let Ok(n) = self.entry_mut(entry.next)
        {
            n.prev = entry.prev;
        }
        if self.heads.get(head).copied() == Some(NONE) {
            self.clear_occupied(level, slot);
        }
    }

    fn clear_occupied(&mut self, level: usize, slot: usize) {
        if let Some(word) = self.occupied.get_mut(level) {
            *word &= !(1u64 << slot);
        }
    }

    fn entry(&self, id: u32) -> Result<&Entry, RtError> {
        usize::try_from(id)
            .ok()
            .and_then(|index| self.entries.get(index))
            .ok_or(RtError::TimerState { id, armed: false })
    }

    fn entry_mut(&mut self, id: u32) -> Result<&mut Entry, RtError> {
        usize::try_from(id)
            .ok()
            .and_then(|index| self.entries.get_mut(index))
            .ok_or(RtError::TimerState { id, armed: false })
    }
}

/// The bit shift of a level's slot boundaries: `64^level` ticks.
fn level_shift(level: usize) -> u32 {
    SLOT_BITS.saturating_mul(u32::try_from(level).unwrap_or(0))
}

/// A slot index within a level, from a tick shifted to the level.
fn slot_index(shifted: u64) -> usize {
    usize::try_from(shifted & SLOT_MASK).unwrap_or(0)
}

/// A slot's list head in the flat table of every level's slots.
fn head_index(level: usize, slot: usize) -> usize {
    level.saturating_mul(SLOTS_PER_LEVEL).saturating_add(slot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::machine::stats::Xorshift;

    fn words(fired: &[(u32, u64)]) -> Vec<u64> {
        fired.iter().map(|(_, word)| *word).collect()
    }

    #[test]
    fn timers_fire_in_deadline_order_within_one_tick_of_accuracy() {
        let tick = 1_000;
        let mut wheel = Wheel::new(tick, 20_000, 0);
        let mut rng = Xorshift::new(Xorshift::SEED);
        let mut expected: Vec<(u64, u64)> = Vec::new();
        for word in 0..10_000u64 {
            let deadline_ns = u64::try_from(rng.below(5_000_000)).unwrap() + 1;
            wheel
                .arm(u32::try_from(word).unwrap(), deadline_ns, word)
                .unwrap();
            expected.push((deadline_ns.div_ceil(tick), word));
        }
        assert_eq!(wheel.armed(), 10_000);
        let mut fired = Vec::new();
        let mut now = 0;
        let mut last_tick = 0;
        while wheel.armed() > 0 {
            now += tick;
            let before = fired.len();
            wheel.advance(now, &mut fired);
            for (_, word) in &fired[before..] {
                let (deadline_tick, _) = expected[usize::try_from(*word).unwrap()];
                assert!(deadline_tick <= now / tick, "timer {word} fired early");
                assert!(
                    now / tick - deadline_tick <= 1,
                    "timer {word} fired {} ticks late",
                    now / tick - deadline_tick
                );
                assert!(deadline_tick >= last_tick, "out of order");
            }
            if fired.len() > before {
                last_tick = now / tick;
            }
        }
        assert_eq!(fired.len(), 10_000);
    }

    #[test]
    fn disarm_removes_a_timer_and_refuses_one_that_is_not_armed() {
        let mut wheel = Wheel::new(10, 8, 0);
        wheel.arm(0, 50, 1).unwrap();
        wheel.arm(1, 50, 2).unwrap();
        assert!(matches!(
            wheel.arm(1, 70, 9),
            Err(RtError::TimerState { armed: true, .. })
        ));
        wheel.disarm(0).unwrap();
        assert!(wheel.disarm(0).is_err());
        assert_eq!(wheel.next_deadline_ns(), Some(50));
        let mut fired = Vec::new();
        wheel.advance(60, &mut fired);
        assert_eq!(fired, vec![(1, 2)]);
        assert!(wheel.disarm(1).is_err(), "a fired timer is no longer armed");
        assert_eq!(wheel.next_deadline_ns(), None);
        assert!(wheel.arm(8, 50, 1).is_err(), "past the capacity");
    }

    /// slates' regression (a stale cancel orphaned the timer that reused its slot, hanging a SWIM probe's
    /// deadline): here a timer fires, its id is armed again by another timer, and a late disarm of that id
    /// removes the new timer only when its owner asks — the wheel refuses a disarm of a fired id, so a
    /// stale disarm arriving between the fire and the reuse cannot orphan anything. Generations that tell a
    /// stale holder from the new one are the desk's (docs/runtime.md §3.4).
    #[test]
    fn a_disarm_after_a_fire_is_refused_and_the_reuse_still_fires() {
        let mut wheel = Wheel::new(10, 4, 0);
        wheel.arm(0, 50, 1).unwrap();
        let mut fired = Vec::new();
        wheel.advance(60, &mut fired);
        assert_eq!(words(&fired), vec![1]);
        assert!(wheel.disarm(0).is_err(), "the stale disarm is refused");
        fired.clear();
        wheel.arm(0, 100, 2).unwrap();
        assert_eq!(wheel.next_deadline_ns(), Some(100));
        wheel.advance(110, &mut fired);
        assert_eq!(words(&fired), vec![2]);
    }

    #[test]
    fn skipping_idle_ticks_never_misses_a_cascade() {
        let mut wheel = Wheel::new(1, 64, 0);
        let deadlines = [63, 64, 65, 127, 128, 4095, 4096, 4097, 262_144, 262_145];
        for (i, d) in deadlines.iter().enumerate() {
            wheel
                .arm(u32::try_from(i).unwrap(), *d, u64::try_from(i).unwrap())
                .unwrap();
        }
        let mut fired = Vec::new();
        let mut now = 0;
        let mut order = Vec::new();
        while wheel.armed() > 0 {
            let next = wheel.next_deadline_ns().unwrap();
            assert!(next > now, "the next event is ahead of the clock");
            now = next;
            let before = fired.len();
            wheel.advance(now, &mut fired);
            for (_, w) in &fired[before..] {
                let d = deadlines[usize::try_from(*w).unwrap()];
                assert_eq!(d, now, "timer {w} fired at {now}, deadline {d}");
                order.push(d);
            }
        }
        assert_eq!(order, deadlines.to_vec());
    }

    #[test]
    fn a_far_deadline_cascades_down_the_levels_and_fires_on_time() {
        let mut wheel = Wheel::new(1, 8, 0);
        let far = 64 * 64 * 3 + 7;
        wheel.arm(0, far, 42).unwrap();
        let mut fired = Vec::new();
        wheel.advance(far - 1, &mut fired);
        assert!(fired.is_empty());
        wheel.advance(far, &mut fired);
        assert_eq!(words(&fired), vec![42]);
    }

    /// §4.3 (a shard's wake cost must not grow with its idle time): advancing across a long idle stretch to
    /// the next timer examines a number of ticks bounded by the wheel's levels, not one per tick.
    #[test]
    fn advancing_across_an_idle_stretch_costs_per_event_not_per_tick() {
        let mut wheel = Wheel::new(1, 16, 0);
        let mut fired = Vec::new();
        wheel.arm(0, 5, 1).unwrap();
        wheel.advance(5, &mut fired);
        assert_eq!(words(&fired), vec![1], "the first timer fired");
        let far = 66_600;
        wheel.arm(1, far, 2).unwrap();
        let before = wheel.visits();
        wheel.advance(far, &mut fired);
        assert_eq!(words(&fired), vec![1, 2], "the far timer fired on time");
        let visited = wheel.visits() - before;
        assert!(
            visited <= 2 * LEVELS as u64,
            "{visited} ticks examined to reach one timer {far} ticks away"
        );
    }

    /// The next event is found from the occupancy words, never by visiting entries: with every timer armed at
    /// the far end of the wheel and one near, the next deadline is the near one, and disarming it moves the
    /// next deadline to the far timers' cascade.
    #[test]
    fn the_next_deadline_follows_arms_and_disarms_without_a_rescan() {
        let mut wheel = Wheel::new(1, 1_000, 0);
        for id in 1..1_000u32 {
            wheel.arm(id, 1_000_000, u64::from(id)).unwrap();
        }
        wheel.arm(0, 10, 0).unwrap();
        assert_eq!(wheel.next_deadline_ns(), Some(10));
        wheel.disarm(0).unwrap();
        let next = wheel.next_deadline_ns().unwrap();
        assert!(next > 10 && next <= 1_000_000, "{next}");
    }
}
