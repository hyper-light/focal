use std::ops::Range;

use crate::{ConnectionId, ResetToken, frame::NewConnectionId};

/// DataType stored in CidQueue buffer
type CidData = (ConnectionId, Option<ResetToken>);

/// Sliding window of active Connection IDs
///
/// May contain gaps due to packet loss or reordering
#[derive(Debug)]
pub(crate) struct CidQueue {
    /// Ring buffer indexed by `self.cursor`
    buffer: [Option<CidData>; Self::LEN],
    /// Index at which circular buffer addressing is based
    cursor: usize,
    /// Sequence number of `self.buffer[cursor]`
    ///
    /// The sequence number of the active CID; must be the smallest among CIDs in `buffer`.
    offset: u64,
    /// The active CID, the one `buffer[cursor]` holds: kept apart so that reading it needs no
    /// fallible lookup (upstream unwrapped the slot).
    active: CidData,
}

impl CidQueue {
    pub(crate) fn new(cid: ConnectionId) -> Self {
        let mut buffer = [None; Self::LEN];
        let [first, ..] = &mut buffer;
        *first = Some((cid, None));
        Self {
            buffer,
            cursor: 0,
            offset: 0,
            active: (cid, None),
        }
    }

    /// Handle a `NEW_CONNECTION_ID` frame
    ///
    /// Returns a non-empty range of retired sequence numbers and the reset token of the new active
    /// CID iff any CIDs were retired.
    pub(crate) fn insert(
        &mut self,
        cid: NewConnectionId,
    ) -> Result<Option<(Range<u64>, ResetToken)>, InsertError> {
        // Position of new CID wrt. the current active CID
        let index = match cid.sequence.checked_sub(self.offset) {
            None => return Err(InsertError::Retired),
            Some(x) => x,
        };

        let retired_count = cid.retire_prior_to.saturating_sub(self.offset);
        // Both are below 2^62 (variable-length integers), so the sum does not saturate.
        if index >= Self::LEN_U64.saturating_add(retired_count) {
            return Err(InsertError::ExceedsLimit);
        }

        // Discard retired CIDs, if any
        for step in 0..Self::LEN.min(usize::try_from(retired_count).unwrap_or(Self::LEN)) {
            if let Some(slot) = self.slot_mut(step) {
                *slot = None;
            }
        }

        // Record the new CID
        let data = (cid.id, Some(cid.reset_token));
        if let Some(slot) = self.slot_mut(Self::ring_step(index)) {
            *slot = Some(data);
        }
        if index == 0 {
            self.active = data;
        }

        if retired_count == 0 {
            return Ok(None);
        }

        // The active CID was retired. Find the first known CID with sequence number of at least
        // retire_prior_to, and inform the caller that all prior CIDs have been retired, and of
        // the new CID's reset token.
        self.cursor = self.position(Self::ring_step(retired_count));
        // The frame's sequence is at least its retire_prior_to (checked when it was decoded), so
        // the CID just recorded is past the retired ones and is found here, with its token.
        let Some((i, (id, Some(token)))) = self.iter().next() else {
            return Err(InsertError::RetiresAll);
        };
        self.cursor = self.position(i);
        self.active = (id, Some(token));
        let orig_offset = self.offset;
        // `i` is below LEN.
        self.offset = cid.retire_prior_to.saturating_add(Self::step_u64(i));
        // We don't immediately retire CIDs in the range (orig_offset +
        // Self::LEN)..self.offset. These are CIDs that we haven't yet received from a
        // NEW_CONNECTION_ID frame, since having previously received them would violate the
        // connection ID limit we specified based on Self::LEN. If we do receive a such a frame
        // in the future, e.g. due to reordering, we'll retire it then. This ensures we can't be
        // made to buffer an arbitrarily large number of RETIRE_CONNECTION_ID frames.
        Ok(Some((
            orig_offset..self.offset.min(orig_offset.saturating_add(Self::LEN_U64)),
            token,
        )))
    }

    /// Switch to next active CID if possible, return
    /// 1) the corresponding ResetToken and 2) a non-empty range preceding it to retire
    pub(crate) fn next(&mut self) -> Option<(ResetToken, Range<u64>)> {
        let (i, cid_data) = self.iter().nth(1)?;
        // Every CID after the initial one came with a reset token.
        let token = cid_data.1?;
        if let Some(slot) = self.slot_mut(0) {
            *slot = None;
        }

        let orig_offset = self.offset;
        // `i` is below LEN.
        self.offset = self.offset.saturating_add(Self::step_u64(i));
        self.cursor = self.position(i);
        self.active = cid_data;
        Some((token, orig_offset..self.offset))
    }

    /// Iterate CIDs in CidQueue that are not `None`, including the active CID
    fn iter(&self) -> impl Iterator<Item = (usize, CidData)> + '_ {
        (0..Self::LEN).filter_map(move |step| {
            self.buffer
                .get(self.position(step))
                .copied()
                .flatten()
                .map(|cid_data| (step, cid_data))
        })
    }

    /// Replace the initial CID
    pub(crate) fn update_initial_cid(&mut self, cid: ConnectionId) {
        // Called before any NEW_CONNECTION_ID, while the offset is 0 (upstream asserted that in
        // debug builds).
        if let Some(slot) = self.slot_mut(0) {
            *slot = Some((cid, None));
        }
        self.active = (cid, None);
    }

    /// Return active remote CID itself
    pub(crate) fn active(&self) -> ConnectionId {
        self.active.0
    }

    /// Return the sequence number of active remote CID
    pub(crate) fn active_seq(&self) -> u64 {
        self.offset
    }

    /// The ring buffer's slot `step` places after the cursor.
    fn slot_mut(&mut self, step: usize) -> Option<&mut Option<CidData>> {
        let position = self.position(step);
        self.buffer.get_mut(position)
    }

    /// The buffer index `step` places after the cursor: both are below `LEN`.
    fn position(&self, step: usize) -> usize {
        self.cursor.wrapping_add(step % Self::LEN) % Self::LEN
    }

    /// `index` places after the cursor, reduced to the ring: below `LEN`.
    fn ring_step(index: u64) -> usize {
        usize::try_from(index % Self::LEN_U64).unwrap_or(0)
    }

    /// A step below `LEN`, as a sequence-number distance.
    fn step_u64(step: usize) -> u64 {
        u64::try_from(step).unwrap_or(Self::LEN_U64)
    }

    /// The window of remote CIDs this endpoint accepts: upstream's value, which bounds the
    /// active_connection_id_limit it advertises.
    pub(crate) const LEN: usize = 5;
    /// [`Self::LEN`] as a sequence-number count.
    const LEN_U64: u64 = Self::LEN as u64;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum InsertError {
    /// CID was already retired
    Retired,
    /// Sequence number violates the leading edge of the window
    ExceedsLimit,
    /// The frame retires every CID it does not supply, its own included: the frame decoder
    /// refuses a retire_prior_to past the sequence number, so this is not reached (upstream's
    /// `expect`).
    RetiresAll,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cid(sequence: u64, retire_prior_to: u64) -> NewConnectionId {
        NewConnectionId {
            sequence,
            id: ConnectionId::new(&[0xAB; 8]),
            reset_token: ResetToken::from([0xCD; crate::RESET_TOKEN_SIZE]),
            retire_prior_to,
        }
    }

    fn initial_cid() -> ConnectionId {
        ConnectionId::new(&[0xFF; 8])
    }

    #[test]
    fn next_dense() {
        let mut q = CidQueue::new(initial_cid());
        assert!(q.next().is_none());
        assert!(q.next().is_none());

        for i in 1..CidQueue::LEN as u64 {
            q.insert(cid(i, 0)).unwrap();
        }
        for i in 1..CidQueue::LEN as u64 {
            let (_, retire) = q.next().unwrap();
            assert_eq!(q.active_seq(), i);
            assert_eq!(retire.end - retire.start, 1);
        }
        assert!(q.next().is_none());
    }
    #[test]
    fn next_sparse() {
        let mut q = CidQueue::new(initial_cid());
        let seqs = (1..CidQueue::LEN as u64).filter(|x| x % 2 == 0);
        for i in seqs.clone() {
            q.insert(cid(i, 0)).unwrap();
        }
        for i in seqs {
            let (_, retire) = q.next().unwrap();
            dbg!(&retire);
            assert_eq!(q.active_seq(), i);
            assert_eq!(retire, (q.active_seq().saturating_sub(2))..q.active_seq());
        }
        assert!(q.next().is_none());
    }

    #[test]
    fn wrap() {
        let mut q = CidQueue::new(initial_cid());

        for i in 1..CidQueue::LEN as u64 {
            q.insert(cid(i, 0)).unwrap();
        }
        for _ in 1..(CidQueue::LEN as u64 - 1) {
            q.next().unwrap();
        }
        for i in CidQueue::LEN as u64..(CidQueue::LEN as u64 + 3) {
            q.insert(cid(i, 0)).unwrap();
        }
        for i in (CidQueue::LEN as u64 - 1)..(CidQueue::LEN as u64 + 3) {
            q.next().unwrap();
            assert_eq!(q.active_seq(), i);
        }
        assert!(q.next().is_none());
    }

    #[test]
    fn retire_dense() {
        let mut q = CidQueue::new(initial_cid());

        for i in 1..CidQueue::LEN as u64 {
            q.insert(cid(i, 0)).unwrap();
        }
        assert_eq!(q.active_seq(), 0);

        assert_eq!(q.insert(cid(4, 2)).unwrap().unwrap().0, 0..2);
        assert_eq!(q.active_seq(), 2);
        assert_eq!(q.insert(cid(4, 2)), Ok(None));

        for i in 2..(CidQueue::LEN as u64 - 1) {
            let _ = q.next().unwrap();
            assert_eq!(q.active_seq(), i + 1);
            assert_eq!(q.insert(cid(i + 1, i + 1)), Ok(None));
        }

        assert!(q.next().is_none());
    }

    #[test]
    fn retire_sparse() {
        // Retiring CID 0 when CID 1 is not known should retire CID 1 as we move to CID 2
        let mut q = CidQueue::new(initial_cid());
        q.insert(cid(2, 0)).unwrap();
        assert_eq!(q.insert(cid(3, 1)).unwrap().unwrap().0, 0..2,);
        assert_eq!(q.active_seq(), 2);
    }

    #[test]
    fn retire_many() {
        let mut q = CidQueue::new(initial_cid());
        q.insert(cid(2, 0)).unwrap();
        assert_eq!(
            q.insert(cid(1_000_000, 1_000_000)).unwrap().unwrap().0,
            0..CidQueue::LEN as u64,
        );
        assert_eq!(q.active_seq(), 1_000_000);
    }

    #[test]
    fn insert_limit() {
        let mut q = CidQueue::new(initial_cid());
        assert_eq!(q.insert(cid(CidQueue::LEN as u64 - 1, 0)), Ok(None));
        assert_eq!(
            q.insert(cid(CidQueue::LEN as u64, 0)),
            Err(InsertError::ExceedsLimit)
        );
    }

    #[test]
    fn insert_duplicate() {
        let mut q = CidQueue::new(initial_cid());
        q.insert(cid(0, 0)).unwrap();
        q.insert(cid(0, 0)).unwrap();
    }

    #[test]
    fn insert_retired() {
        let mut q = CidQueue::new(initial_cid());
        assert_eq!(
            q.insert(cid(0, 0)),
            Ok(None),
            "reinserting active CID succeeds"
        );
        assert!(q.next().is_none(), "active CID isn't requeued");
        q.insert(cid(1, 0)).unwrap();
        q.next().unwrap();
        assert_eq!(
            q.insert(cid(0, 0)),
            Err(InsertError::Retired),
            "previous active CID is already retired"
        );
    }

    #[test]
    fn retire_then_insert_next() {
        let mut q = CidQueue::new(initial_cid());
        for i in 1..CidQueue::LEN as u64 {
            q.insert(cid(i, 0)).unwrap();
        }
        q.next().unwrap();
        q.insert(cid(CidQueue::LEN as u64, 0)).unwrap();
        assert_eq!(
            q.insert(cid(CidQueue::LEN as u64 + 1, 0)),
            Err(InsertError::ExceedsLimit)
        );
    }

    #[test]
    fn always_valid() {
        let mut q = CidQueue::new(initial_cid());
        assert!(q.next().is_none());
        assert_eq!(q.active(), initial_cid());
        assert_eq!(q.active_seq(), 0);
    }
}
