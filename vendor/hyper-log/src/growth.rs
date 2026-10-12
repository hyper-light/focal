//! The log's file grows only as its owner admits (`docs/durable.md` §6).
//!
//! The file grows a segment at a time, as frames open slots past its end, up to
//! `Config::max_segments`. Nothing else stopped it: a volume that filled first failed the write
//! that grew the file, and the log fenced. An owner that shares the volume with other writers
//! (focal's disk envelope, `focal_memory::DiskBudget`) states what the file may take through a
//! [`Growth`] it hands the log at create or open, and the log asks it before a frame opens a slot
//! past the file's end. A refusal reads exactly as `max_segments` reached: the frame that needed
//! the slot is answered `Full`, groups compact, sweeps run, and nothing is fenced.
//!
//! Reservations are whole segments: a slot admitted is one the file will fill, so a frame that
//! fills it later meets no volume it was not admitted on. A slot written whole with zeros before its
//! frames (`docs/durable.md` §6.3, `BlockFile::fills_new_space`) takes exactly the segment admitted
//! for it: the zeros commit no disk the gate did not admit. The file never shrinks and freed slots
//! are reused, so nothing is released when a slot is freed by compaction: a reservation ends only
//! when its slot is durable (committed) or when it will never be (released).
//!
//! The gate narrows the volume's running out, and does not close it: a write of another owner of
//! the volume, outside the gate, can still fill it, and the log's own write then fails and fences
//! as before.
use crate::state::State;

/// The owner's admission for the log's file to grow, asked on the log's owner thread.
pub trait Growth: Send {
    /// The file already takes `bytes` (its persist area and every slot, each a whole segment),
    /// told once at open before any admission, so a restart neither counts them twice nor admits
    /// past the volume.
    fn held(&mut self, bytes: u64);
    /// Whether the file may grow by `bytes`, reserving them if so.
    fn admit(&mut self, bytes: u64) -> bool;
    /// Bytes admitted that the file now holds durably: the slot they were admitted for opened,
    /// its header flushed.
    fn commit(&mut self, bytes: u64);
    /// Bytes admitted that the file will not hold: the write that would have grown it failed, or
    /// the log ended before using them.
    fn release(&mut self, bytes: u64);
}

/// Slots the log keeps admitted ahead of need. The writer's decisions read only whether no
/// segment, one, or two or more can be used (`writer::target`, `writer::sweepable`), so with two
/// admitted ahead every decision is the one `max_segments` would make with the bound where the
/// owner's admission stopped.
const AHEAD: u32 = 2;

/// The log's side of a [`Growth`]: what is admitted and not yet durable.
///
/// The [`Growth`] is the owner's code, run on the log's owner thread: each call into it is inside
/// an unwind boundary (CLAUDE.md §1). One that unwinds is never called again, its state unknown:
/// from then on every admission reads as a refusal, so the file grows no further and a frame that
/// needs a slot is answered `Full`, as at `max_segments`.
pub(crate) struct Gate {
    /// The owner's admission, until a call into it unwinds.
    growth: Option<Box<dyn Growth>>,
    /// Bytes of one slot.
    segment: u64,
    /// Slots admitted and not yet durable, all past the file's last.
    pending: u32,
    /// Admissions the owner refused (`LogStats::growth_refused`).
    pub(crate) refused: u64,
}

impl Gate {
    /// The gate over `growth`, the file taking `held` bytes already.
    pub(crate) fn new(growth: Box<dyn Growth>, segment: u64, held: u64) -> Self {
        let mut gate = Self {
            growth: Some(growth),
            segment,
            pending: 0,
            refused: 0,
        };
        gate.call((), |growth| growth.held(held));
        gate
    }

    /// Runs `op` on the owner's admission inside an unwind boundary: `refused` once the admission
    /// has unwound, now or before. One that unwinds is dropped, inside a boundary too.
    fn call<T>(&mut self, refused: T, op: impl FnOnce(&mut dyn Growth) -> T) -> T {
        let Some(growth) = self.growth.as_mut() else {
            return refused;
        };
        match crate::unwind::contained(|| op(growth.as_mut())) {
            Some(answer) => answer,
            None => {
                self.drop_growth();
                refused
            }
        }
    }

    /// Drops the owner's admission, whose drop is its code too.
    fn drop_growth(&mut self) {
        if let Some(growth) = self.growth.take() {
            let _ = crate::unwind::contained(move || drop(growth));
        }
    }

    /// Admits `bytes` the file is about to take outside a slot's opening: a new log's persist area
    /// and first slot.
    pub(crate) fn take(&mut self, bytes: u64) -> bool {
        let admitted = self.call(false, |growth| growth.admit(bytes));
        if !admitted {
            self.refused = self.refused.saturating_add(1);
        }
        admitted
    }

    /// Bytes [`Gate::take`] admitted are durable in the file.
    pub(crate) fn hold(&mut self, bytes: u64) {
        self.call((), |growth| growth.commit(bytes));
    }

    /// Bytes [`Gate::take`] admitted that the file will not hold.
    pub(crate) fn give(&mut self, bytes: u64) {
        self.call((), |growth| growth.release(bytes));
    }

    /// Admits slots until [`AHEAD`] are usable past the file's last, or `max_segments` is
    /// reached, or the owner refuses; the log's bound is then where admission stopped
    /// (`State::ceiling`).
    pub(crate) fn refill(&mut self, state: &mut State, max_segments: u32) {
        let slots = u32::try_from(state.segments.incarnation.len()).unwrap_or(u32::MAX);
        let segment = self.segment;
        while state.ceiling < max_segments && state.ceiling.saturating_sub(slots) < AHEAD {
            if !self.call(false, |growth| growth.admit(segment)) {
                self.refused = self.refused.saturating_add(1);
                return;
            }
            state.ceiling = state.ceiling.saturating_add(1);
            self.pending = self.pending.saturating_add(1);
        }
    }

    /// `slots` slots past the file's former last became durable: their bytes are held.
    pub(crate) fn grew(&mut self, slots: u32) {
        let taken = slots.min(self.pending);
        self.pending = self.pending.saturating_sub(taken);
        if taken > 0 {
            let bytes = self.segment.saturating_mul(u64::from(taken));
            self.call((), |growth| growth.commit(bytes));
        }
    }

    /// Nothing more will be written: what was admitted and is not durable goes back, and the
    /// bound falls back to the file's last slot.
    pub(crate) fn release_pending(&mut self, state: &mut State) {
        state.ceiling = state.ceiling.saturating_sub(self.pending);
        self.give_back();
    }

    fn give_back(&mut self) {
        if self.pending > 0 {
            let bytes = self.segment.saturating_mul(u64::from(self.pending));
            self.call((), |growth| growth.release(bytes));
            self.pending = 0;
        }
    }
}

impl Drop for Gate {
    /// The log ended: what it admitted and never used goes back, and the owner's admission is
    /// dropped inside an unwind boundary.
    fn drop(&mut self) {
        self.give_back();
        self.drop_growth();
    }
}
