//! Forward erasure correction of a connection's packets: RFC 8681's sliding-window random linear
//! code over GF(2^8), its coefficients drawn from TinyMT32 (RFC 8682).
//!
//! A source symbol is the frames of one 1-RTT packet as it was sent, and a repair symbol a linear
//! combination of the sources sent last, its coefficients drawn from the repair's key (RFC 8681
//! §3.6). Every symbol of a combination is taken at the combination's length, a shorter one zero
//! filled (RFC 8681 §3.2 pads its symbols with zeros), and a zero is a PADDING frame (RFC 9000
//! §19.1), so a source rebuilt at a repair's length is its packet's frames as sent and PADDING
//! after them, read as any packet's payload is.
//!
//! A receiver keeps the sources it holds, and the repairs that cover sources it does not, each
//! reduced by the sources held to an equation over the ones missing (RFC 8681 §6.2: "a linear
//! system whose variables are the received and lost source symbols"). The equations are kept in
//! reduced row echelon form as they come, by Gaussian elimination, so a source is rebuilt as soon
//! as the equations covering it are of full rank, and a source that arrives late is substituted
//! into them.
//!
//! The coding only ever adds: a source or a repair either end cannot keep is forgotten, and what
//! it would have rebuilt is repaired as QUIC repairs any loss, by retransmission. So each end
//! holds what it can afford, up to its stated bound, and no more.

mod field;
mod prng;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, VecDeque};

use crate::Instant;
use field::{add_scaled, inverse, scale};
use prng::coefficients;

/// What a repair covers: `count` sources from `first`, combined with the coefficients of `key`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Span {
    pub(crate) first: u64,
    pub(crate) count: u64,
    pub(crate) key: u16,
}

/// A source sent: its number, the packet that carried it and when, whether that packet was
/// acknowledged, and its frames.
struct Sent {
    id: u64,
    packet: u64,
    at: Instant,
    acked: bool,
    payload: Vec<u8>,
}

/// The sending side: the sources sent last, which the next repairs combine.
pub(crate) struct Encoder {
    /// Consecutive by number, oldest first; at most `capacity`.
    window: VecDeque<Sent>,
    /// Buffers of sources forgotten, kept for the next sources so that sending one allocates only
    /// while the window grows to its bound; at most `capacity`.
    spare: Vec<Vec<u8>>,
    next_id: u64,
    next_key: u16,
    capacity: usize,
    coefficients: Vec<u8>,
}

impl Encoder {
    /// An encoder that holds at most `capacity` sources.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            window: VecDeque::new(),
            spare: Vec::new(),
            next_id: 0,
            next_key: 0,
            capacity: capacity.max(1),
            coefficients: Vec::new(),
        }
    }

    /// The number the next source takes.
    pub(crate) fn next_id(&self) -> u64 {
        self.next_id
    }

    /// The sources held.
    #[cfg(test)]
    pub(crate) fn held(&self) -> usize {
        self.window.len()
    }

    /// The bytes held for the sources and their spare buffers.
    pub(crate) fn held_bytes(&self) -> usize {
        let window = self.window.iter().fold(0usize, |sum, sent| {
            sum.saturating_add(sent.payload.capacity())
        });
        self.spare
            .iter()
            .fold(window, |sum, spare| sum.saturating_add(spare.capacity()))
    }

    /// The source numbered [`Self::next_id`] was sent with `payload` in packet `packet` at `at`:
    /// kept for the repairs after it, the oldest forgotten past the bound. False, and nothing
    /// kept, when its memory could not be had; the number is used either way, so a repair never
    /// names a source that was not sent as that number.
    pub(crate) fn sent(&mut self, payload: &[u8], packet: u64, at: Instant) -> bool {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        if self
            .window
            .back()
            .is_some_and(|last| last.id.checked_add(1) != Some(id))
        {
            // A number was skipped (a source not kept): the window restarts at this one, so its
            // numbers stay consecutive.
            self.forget_all();
        }
        if self.window.len() >= self.capacity
            && let Some(oldest) = self.window.pop_front()
        {
            self.recycle(oldest.payload);
        }
        let mut buffer = self.spare.pop().unwrap_or_default();
        buffer.clear();
        if buffer.try_reserve_exact(payload.len()).is_err() || self.window.try_reserve(1).is_err() {
            return false;
        }
        buffer.extend_from_slice(payload);
        self.window.push_back(Sent {
            id,
            packet,
            at,
            acked: false,
            payload: buffer,
        });
        true
    }

    /// Packet `packet` was acknowledged: a source it carried is held by the receiver, and a
    /// repair need not cover it.
    pub(crate) fn acked(&mut self, packet: u64) {
        // Packets are numbered in the order they leave, and sources in the order of their packets
        if let Ok(at) = self
            .window
            .binary_search_by(|sent| sent.packet.cmp(&packet))
            && let Some(sent) = self.window.get_mut(at)
        {
            sent.acked = true;
        }
    }

    /// Forgets the oldest sources, while each is acknowledged or was sent before `before`: past
    /// it, a repair would rebuild the source no sooner than the retransmission of its loss.
    pub(crate) fn prune(&mut self, before: Instant) {
        while let Some(oldest) = self.window.front() {
            if !oldest.acked && oldest.at >= before {
                break;
            }
            if let Some(oldest) = self.window.pop_front() {
                self.recycle(oldest.payload);
            }
        }
    }

    /// Forgets every source held.
    pub(crate) fn forget_all(&mut self) {
        while let Some(oldest) = self.window.pop_front() {
            self.recycle(oldest.payload);
        }
    }

    fn recycle(&mut self, buffer: Vec<u8>) {
        if self.spare.len() < self.capacity {
            self.spare.push(buffer);
        }
    }

    /// A repair of the last `span` sources held, at most, written over `into` at the length of
    /// the longest of them; `None` when none is held or its memory could not be had.
    pub(crate) fn repair(&mut self, span: usize, into: &mut Vec<u8>) -> Option<Span> {
        let count = span.min(self.window.len());
        let skip = self.window.len().saturating_sub(count);
        let first = self.window.get(skip)?.id;
        let length = self
            .window
            .iter()
            .skip(skip)
            .map(|sent| sent.payload.len())
            .max()?;
        into.clear();
        if into.try_reserve_exact(length).is_err() {
            return None;
        }
        into.resize(length, 0);
        let key = self.next_key;
        if !coefficients(key, count, &mut self.coefficients) {
            return None;
        }
        for (sent, coefficient) in self.window.iter().skip(skip).zip(&self.coefficients) {
            add_scaled(into, &sent.payload, *coefficient);
        }
        self.next_key = self.next_key.wrapping_add(1);
        Some(Span {
            first,
            count: u64::try_from(count).ok()?,
            key,
        })
    }
}

/// One repair's equation, reduced by the sources held: a combination of sources not held, with
/// what it equals.
struct Row {
    /// Each source's coefficient, none zero, by number; the first is the row's pivot, and is one.
    terms: BTreeMap<u64, u8>,
    payload: Vec<u8>,
}

impl Row {
    fn pivot(&self) -> Option<u64> {
        self.terms.keys().next().copied()
    }

    /// `self += factor · other`, terms and payload; false when the payload's memory could not be
    /// had.
    fn add(&mut self, other: &Self, factor: u8) -> bool {
        if other.payload.len() > self.payload.len() {
            let extra = other.payload.len().saturating_sub(self.payload.len());
            if self.payload.try_reserve_exact(extra).is_err() {
                return false;
            }
            self.payload.resize(other.payload.len(), 0);
        }
        add_scaled(&mut self.payload, &other.payload, factor);
        for (id, coefficient) in &other.terms {
            let sum = self.terms.get(id).copied().unwrap_or(0) ^ field::mul(factor, *coefficient);
            if sum == 0 {
                self.terms.remove(id);
            } else {
                self.terms.insert(*id, sum);
            }
        }
        true
    }

    /// The row scaled so that its pivot is one.
    fn normalize(&mut self) {
        let Some(coefficient) = self.terms.values().next().copied() else {
            return;
        };
        let factor = inverse(coefficient);
        scale(&mut self.payload, factor);
        for value in self.terms.values_mut() {
            *value = field::mul(factor, *value);
        }
    }
}

/// The receiving side: the sources held and the repairs that cover the ones missing.
pub(crate) struct Decoder {
    /// Every source held, received or rebuilt, by number; none below `floor`.
    known: BTreeMap<u64, Vec<u8>>,
    /// The equations, in reduced row echelon form: each row's pivot is one, and no other row has
    /// a term there. Each row has a pivot of its own, so they are no more than the sources
    /// missing within the bound.
    rows: Vec<Row>,
    /// Sources numbered below this are forgotten.
    floor: u64,
    /// The lowest and the highest numbers a source or a repair named.
    first: Option<u64>,
    high: Option<u64>,
    /// The sources received and rebuilt.
    received: u64,
    rebuilt: u64,
    capacity: usize,
    coefficients: Vec<u8>,
}

impl Decoder {
    /// A decoder that holds the sources of at most `capacity` consecutive numbers.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            known: BTreeMap::new(),
            rows: Vec::new(),
            floor: 0,
            first: None,
            high: None,
            received: 0,
            rebuilt: 0,
            capacity: capacity.max(1),
            coefficients: Vec::new(),
        }
    }

    /// The sources named and neither received nor rebuilt: those the code left, or has yet to
    /// rebuild, for retransmission.
    pub(crate) fn forgotten(&self) -> u64 {
        let (Some(first), Some(high)) = (self.first, self.high) else {
            return 0;
        };
        high.saturating_sub(first)
            .saturating_add(1)
            .saturating_sub(self.received)
            .saturating_sub(self.rebuilt)
    }

    /// The bytes held for sources and equations.
    pub(crate) fn held_bytes(&self) -> usize {
        let known = self.known.values().fold(0usize, |sum, payload| {
            sum.saturating_add(payload.capacity())
        });
        self.rows
            .iter()
            .fold(known, |sum, row| sum.saturating_add(row.payload.capacity()))
    }

    /// The equations waiting on sources missing.
    #[cfg(test)]
    pub(crate) fn pending(&self) -> usize {
        self.rows.len()
    }

    /// Moves the bound up to hold `id`: what falls below it is forgotten, and an equation over a
    /// source forgotten can no longer be solved, so it goes too.
    fn reach(&mut self, id: u64) {
        let Some(span) = id
            .checked_sub(self.floor)
            .and_then(|above| above.checked_add(1))
        else {
            return;
        };
        let capacity = u64::try_from(self.capacity).unwrap_or(u64::MAX);
        if span > capacity {
            self.raise_floor(id.saturating_sub(capacity.saturating_sub(1)));
        }
        self.first = Some(self.first.map_or(id, |first| first.min(id)));
        self.high = Some(self.high.map_or(id, |high| high.max(id)));
    }

    /// Forgets every source and equation below `floor`: the sources missing below it are given
    /// up to retransmission.
    fn raise_floor(&mut self, floor: u64) {
        if floor <= self.floor {
            return;
        }
        self.floor = floor;
        self.known = self.known.split_off(&floor);
        self.rows
            .retain(|row| row.terms.keys().next().is_none_or(|first| *first >= floor));
    }

    /// Forgets everything held: the peer codes no longer, or the memory is wanted. A source it
    /// names later starts the window again.
    pub(crate) fn forget_all(&mut self) {
        if let Some(high) = self.high {
            self.raise_floor(high.saturating_add(1));
        }
        self.known.clear();
        self.rows.clear();
    }

    /// The source numbered `id` arrived with `payload`: held, substituted into the equations,
    /// and every source that rebuilds is appended to `rebuilt`.
    pub(crate) fn received(&mut self, id: u64, payload: &[u8], rebuilt: &mut Vec<Vec<u8>>) {
        if id < self.floor || self.known.contains_key(&id) {
            return;
        }
        self.reach(id);
        let mut copy = Vec::new();
        if copy.try_reserve_exact(payload.len()).is_err() {
            return;
        }
        copy.extend_from_slice(payload);
        self.received = self.received.saturating_add(1);
        self.substitute(id, &copy, rebuilt);
        self.known.insert(id, copy);
    }

    /// A repair arrived: its equation, reduced by the sources held, joins the system, and every
    /// source that rebuilds is appended to `rebuilt`. A repair over a source forgotten is of no
    /// use and is dropped, as is one over only sources held.
    pub(crate) fn repair(&mut self, span: Span, payload: &[u8], rebuilt: &mut Vec<Vec<u8>>) {
        let Ok(count) = usize::try_from(span.count) else {
            return;
        };
        let Some(last) = span
            .first
            .checked_add(span.count)
            .and_then(|end| end.checked_sub(1))
        else {
            return;
        };
        if count == 0 || count > self.capacity || span.first < self.floor {
            return;
        }
        self.reach(last);
        if span.first < self.floor {
            return;
        }
        self.first = Some(self.first.map_or(span.first, |first| first.min(span.first)));
        // The sender's window only moves forward, so no later repair names a source below this
        // one's first: the sources held below it serve nothing more. The equations over sources
        // missing below it stay, as they may yet solve the ones above.
        self.known = self.known.split_off(&span.first);
        if !coefficients(span.key, count, &mut self.coefficients) {
            return;
        }
        let mut row = Row {
            terms: BTreeMap::new(),
            payload: Vec::new(),
        };
        if row.payload.try_reserve_exact(payload.len()).is_err() {
            return;
        }
        row.payload.extend_from_slice(payload);
        for (id, coefficient) in (span.first..=last).zip(&self.coefficients) {
            match self.known.get(&id) {
                Some(source) => add_scaled(&mut row.payload, source, *coefficient),
                None => {
                    row.terms.insert(id, *coefficient);
                }
            }
        }
        self.insert(row, rebuilt);
    }

    /// `row` joins the system: reduced by every row's pivot, normalized at its own, and
    /// eliminated from every other row; then the rows left with one term are solved.
    fn insert(&mut self, mut row: Row, rebuilt: &mut Vec<Vec<u8>>) {
        for other in &self.rows {
            let Some(pivot) = other.pivot() else {
                continue;
            };
            if let Some(factor) = row.terms.get(&pivot).copied()
                && !row.add(other, factor)
            {
                return;
            }
        }
        let Some(pivot) = row.pivot() else {
            // Every term eliminated: the repair said nothing new.
            return;
        };
        row.normalize();
        for other in &mut self.rows {
            if let Some(factor) = other.terms.get(&pivot).copied()
                && !other.add(&row, factor)
            {
                // A row that cannot take the elimination is no longer reduced; it goes.
                other.terms.clear();
            }
        }
        self.rows.retain(|other| !other.terms.is_empty());
        if self.rows.try_reserve(1).is_err() {
            return;
        }
        self.rows.push(row);
        self.solve(rebuilt);
    }

    /// The source numbered `id`, now held as `payload`, leaves the equations: each term of it is
    /// moved to the payload's side. A row whose pivot it was is reduced again from what is left.
    fn substitute(&mut self, id: u64, payload: &[u8], rebuilt: &mut Vec<Vec<u8>>) {
        let mut repivot = Vec::new();
        let mut index = 0;
        while index < self.rows.len() {
            let Some(row) = self.rows.get_mut(index) else {
                break;
            };
            if let Some(coefficient) = row.terms.remove(&id) {
                if row.payload.len() < payload.len() {
                    let extra = payload.len().saturating_sub(row.payload.len());
                    if row.payload.try_reserve_exact(extra).is_err() {
                        row.terms.clear();
                    } else {
                        row.payload.resize(payload.len(), 0);
                    }
                }
                add_scaled(&mut row.payload, payload, coefficient);
                if row.pivot().is_none_or(|pivot| pivot > id) {
                    // The term removed was the row's pivot, its least: the row's next term
                    // becomes a pivot only once it is eliminated from the others again.
                    let row = self.rows.swap_remove(index);
                    if repivot.try_reserve(1).is_ok() {
                        repivot.push(row);
                    }
                    continue;
                }
            }
            index = index.saturating_add(1);
        }
        self.rows.retain(|row| !row.terms.is_empty());
        for row in repivot {
            if !row.terms.is_empty() {
                self.insert(row, rebuilt);
            }
        }
        self.solve(rebuilt);
    }

    /// Every row left with its pivot alone is a source rebuilt: held, and appended to `rebuilt`.
    /// In reduced form no other row has a term at a pivot, so nothing else changes.
    fn solve(&mut self, rebuilt: &mut Vec<Vec<u8>>) {
        let mut index = 0;
        while index < self.rows.len() {
            let solved = self.rows.get(index).is_some_and(|row| row.terms.len() == 1);
            if !solved {
                index = index.saturating_add(1);
                continue;
            }
            let row = self.rows.swap_remove(index);
            let Some(id) = row.pivot() else {
                continue;
            };
            let mut copy = Vec::new();
            if copy.try_reserve_exact(row.payload.len()).is_ok() && rebuilt.try_reserve(1).is_ok() {
                copy.extend_from_slice(&row.payload);
                rebuilt.push(copy);
            }
            self.rebuilt = self.rebuilt.saturating_add(1);
            self.known.insert(id, row.payload);
        }
    }
}
