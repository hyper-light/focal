//! The bytes a node may hold for the transport, reserved through a trait each project implements
//! over its own accountant (mantle note 32 §3.1, "Budgets by trait"): focal's `MemoryBudget`,
//! slates' `mem::budget`, mantle's admission authorities. A reservation is a value its holder gives
//! back; nothing is reference counted.

use crate::Refusal;

/// What a reservation is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane<K> {
    /// A connection's receive window: the bytes QUIC may buffer for the peer before the
    /// application reads them (quinn: "upper bound proportional to receive_window", node.md §3.3);
    /// and the connections' own state beside it (docs/transport.md §4x). Accounting only;
    /// such a reservation carries no buffer.
    Window,
    /// A message of this class: its head, a piece of its body, or a lane's frame.
    Class(K),
}

/// Bytes the node may hold for the transport, implemented over each project's accountant.
pub trait Budget<K> {
    /// `bytes` for `lane`, or the refusal the accountant gives.
    fn reserve(&mut self, bytes: u64, lane: Lane<K>) -> Result<Reservation, Refusal>;
    /// A reservation given back, its bytes free again.
    fn release(&mut self, reservation: Reservation);
}

/// Bytes granted by a [`Budget`], with the buffer they are read into when they hold data.
#[derive(Debug, Default)]
pub struct Reservation {
    granted: u64,
    buffer: Vec<u8>,
    /// The accountant's own mark, given back with the reservation: what it needs to release the
    /// bytes where it took them (focal funds the most urgent class from a reserve apart from the
    /// rest). The endpoint never reads it.
    tag: u8,
}

impl Reservation {
    /// `granted` bytes, read into `buffer` (cleared here); an accountant that recycles buffers
    /// hands back one whose capacity already holds `granted`, so that reading into it allocates
    /// nothing.
    pub fn new(granted: u64, mut buffer: Vec<u8>) -> Self {
        buffer.clear();
        Self {
            granted,
            buffer,
            tag: 0,
        }
    }
    /// [`Reservation::new`], marked with the accountant's `tag` ([`Reservation::tag`]).
    pub fn tagged(granted: u64, buffer: Vec<u8>, tag: u8) -> Self {
        Self {
            tag,
            ..Self::new(granted, buffer)
        }
    }
    /// The accountant's mark: zero unless it was made [`Reservation::tagged`].
    pub fn tag(&self) -> u8 {
        self.tag
    }
    /// The bytes granted.
    pub fn granted(&self) -> u64 {
        self.granted
    }
    /// The bytes read into the reservation so far.
    pub fn bytes(&self) -> &[u8] {
        &self.buffer
    }
    /// How many more bytes the reservation takes.
    pub fn room(&self) -> u64 {
        let filled = u64::try_from(self.buffer.len()).unwrap_or(u64::MAX);
        self.granted.saturating_sub(filled)
    }
    /// Empty the reservation, keeping its grant, to read into it again.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
    /// `other`'s grant taken into this one, to be given back with it: how an owner that charges a
    /// sum as one reservation (an endpoint's connections' state, docs/transport.md §4x) grows it.
    /// Both must be of one accountant's mark and hold no buffer (accounting only); one that is not
    /// so, or whose grant the sum cannot hold, is handed back as it was.
    pub fn absorb(&mut self, other: Reservation) -> Result<(), Reservation> {
        if other.tag != self.tag || other.buffer.capacity() > 0 || self.buffer.capacity() > 0 {
            return Err(other);
        }
        match self.granted.checked_add(other.granted) {
            Some(granted) => {
                self.granted = granted;
                Ok(())
            }
            None => Err(other),
        }
    }
    /// `bytes` of this grant split off as a reservation of their own, of the same mark, to be given
    /// back apart; none past the grant, or of a reservation holding a buffer. An accountant gives
    /// back what each release carries, so a reservation split and absorbed gives back, in all, what
    /// was granted.
    pub fn split(&mut self, bytes: u64) -> Option<Reservation> {
        if self.buffer.capacity() > 0 {
            return None;
        }
        self.granted = self.granted.checked_sub(bytes)?;
        Some(Self {
            granted: bytes,
            buffer: Vec::new(),
            tag: self.tag,
        })
    }
    /// The buffer, for an accountant that recycles it.
    pub fn into_buffer(self) -> Vec<u8> {
        self.buffer
    }
    pub(crate) fn fill(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }
}

/// A budget of a fixed number of bytes, recycling the buffers it hands out: the accountant the
/// tests, the benchmarks and a single process use. Reservations past `capacity` are refused.
#[derive(Debug)]
pub struct Fixed {
    capacity: u64,
    used: u64,
    spare: Vec<Vec<u8>>,
    max_spare: usize,
    refused: u64,
}

impl Fixed {
    /// A budget of `capacity` bytes that keeps at most `max_spare` buffers for reuse.
    pub fn new(capacity: u64, max_spare: usize) -> Self {
        Self {
            capacity,
            used: 0,
            spare: Vec::with_capacity(max_spare),
            max_spare,
            refused: 0,
        }
    }
    /// The bytes reserved and not yet given back.
    pub fn used(&self) -> u64 {
        self.used
    }
    /// The reservations refused so far.
    pub fn refused(&self) -> u64 {
        self.refused
    }
}

impl<K> Budget<K> for Fixed {
    fn reserve(&mut self, bytes: u64, lane: Lane<K>) -> Result<Reservation, Refusal> {
        let used = self
            .used
            .checked_add(bytes)
            .filter(|used| *used <= self.capacity);
        let wanted = usize::try_from(bytes).ok();
        let (Some(used), Some(wanted)) = (used, wanted) else {
            self.refused = self.refused.saturating_add(1);
            return Err(Refusal::Budget);
        };
        self.used = used;
        let buffer = match lane {
            Lane::Window => Vec::new(),
            Lane::Class(_) => {
                // The smallest spare that holds the grant, or else the largest: a buffer that
                // already holds it is never grown.
                let fits = (0..self.spare.len())
                    .filter(|at| {
                        self.spare
                            .get(*at)
                            .is_some_and(|spare| spare.capacity() >= wanted)
                    })
                    .min_by_key(|at| self.spare.get(*at).map_or(usize::MAX, Vec::capacity));
                let largest = || {
                    (0..self.spare.len())
                        .max_by_key(|at| self.spare.get(*at).map_or(0, Vec::capacity))
                };
                let mut buffer = fits
                    .or_else(largest)
                    .map(|at| self.spare.swap_remove(at))
                    .unwrap_or_default();
                buffer.clear();
                buffer.reserve_exact(wanted);
                buffer
            }
        };
        Ok(Reservation::new(bytes, buffer))
    }

    fn release(&mut self, reservation: Reservation) {
        self.used = self.used.saturating_sub(reservation.granted);
        let buffer = reservation.into_buffer();
        if buffer.capacity() > 0 && self.spare.len() < self.max_spare {
            self.spare.push(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fixed_budget_refuses_past_its_capacity_and_recycles_buffers() {
        let mut budget = Fixed::new(100, 1);
        let first = Budget::<()>::reserve(&mut budget, 60, Lane::Class(())).unwrap();
        assert_eq!(
            Budget::<()>::reserve(&mut budget, 41, Lane::Class(())).err(),
            Some(Refusal::Budget)
        );
        assert_eq!((budget.used(), budget.refused()), (60, 1));
        let pointer = first.bytes().as_ptr();
        Budget::<()>::release(&mut budget, first);
        assert_eq!(budget.used(), 0);
        let second = Budget::<()>::reserve(&mut budget, 40, Lane::Class(())).unwrap();
        assert_eq!(second.bytes().as_ptr(), pointer, "the buffer was reused");
        let window = Budget::<()>::reserve(&mut budget, 60, Lane::Window).unwrap();
        assert_eq!((window.granted(), window.room()), (60, 60));
    }

    #[test]
    fn a_tag_comes_back_with_its_reservation_and_survives_the_fill() {
        let mut tagged = Reservation::tagged(8, vec![9; 4], 3);
        assert_eq!(
            (tagged.tag(), tagged.granted(), tagged.bytes()),
            (3, 8, &[][..])
        );
        tagged.fill(&[1, 2]);
        tagged.clear();
        assert_eq!((tagged.tag(), tagged.room()), (3, 8));
        assert_eq!(Reservation::new(8, Vec::new()).tag(), 0);
    }
}
