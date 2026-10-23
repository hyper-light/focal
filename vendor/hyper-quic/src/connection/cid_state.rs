//! Maintain the state of local connection IDs
use std::collections::VecDeque;

use rustc_hash::FxHashSet;
use tracing::{debug, trace};

use crate::{Duration, Instant, TransportError, shared::IssuedCid};

/// Local connection ID management
pub(super) struct CidState {
    /// Timestamp when issued cids should be retired
    retire_timestamp: VecDeque<CidTimestamp>,
    /// Number of local connection IDs that have been issued in NEW_CONNECTION_ID frames.
    issued: u64,
    /// Sequence numbers of local connection IDs not yet retired by the peer
    active_seq: FxHashSet<u64>,
    /// Sequence number the peer has already retired all CIDs below at our request via `retire_prior_to`
    prev_retire_seq: u64,
    /// Sequence number to set in retire_prior_to field in NEW_CONNECTION_ID frame
    retire_seq: u64,
    /// cid length used to decode short packet
    cid_len: usize,
    //// cid lifetime
    cid_lifetime: Option<Duration>,
    /// Connection IDs the peer retired that still route to the connection, oldest retired first,
    /// each with when it stops (`Connection::on_retire_connection_id`)
    retiring: VecDeque<(u64, Instant)>,
}

impl CidState {
    pub(crate) fn new(
        cid_len: usize,
        cid_lifetime: Option<Duration>,
        now: Instant,
        issued: u64,
    ) -> Self {
        let mut active_seq = FxHashSet::default();
        // Add sequence number of CIDs used in handshaking into tracking set
        for seq in 0..issued {
            active_seq.insert(seq);
        }
        let mut this = Self {
            retire_timestamp: VecDeque::new(),
            issued,
            active_seq,
            prev_retire_seq: 0,
            retire_seq: 0,
            cid_len,
            cid_lifetime,
            retiring: VecDeque::new(),
        };
        // Track lifetime of CIDs used in handshaking
        for seq in 0..issued {
            this.track_lifetime(seq, now);
        }
        this
    }

    /// Find the next timestamp when previously issued CID should be retired
    pub(crate) fn next_timeout(&mut self) -> Option<Instant> {
        self.retire_timestamp.front().map(|nc| {
            trace!("CID {} will expire at {:?}", nc.sequence, nc.timestamp);
            nc.timestamp
        })
    }

    /// Track the lifetime of issued cids in `retire_timestamp`
    fn track_lifetime(&mut self, new_cid_seq: u64, now: Instant) {
        let lifetime = match self.cid_lifetime {
            Some(lifetime) => lifetime,
            None => return,
        };

        let expire_timestamp = now.checked_add(lifetime);
        let expire_at = match expire_timestamp {
            Some(expire_at) => expire_at,
            None => return,
        };

        let last_record = self.retire_timestamp.back_mut();
        if let Some(last) = last_record {
            // Compare the timestamp with the last inserted record
            // Combine into a single batch if timestamp of current cid is same as the last record
            if expire_at == last.timestamp {
                last.sequence = new_cid_seq;
                return;
            }
        }

        self.retire_timestamp.push_back(CidTimestamp {
            sequence: new_cid_seq,
            timestamp: expire_at,
        });
    }

    /// Update local CID state when previously issued CID is retired
    ///
    /// Return whether a new CID needs to be pushed that notifies remote peer to respond `RETIRE_CONNECTION_ID`
    pub(crate) fn on_cid_timeout(&mut self) -> bool {
        // Whether the peer hasn't retired all the CIDs we asked it to yet
        let unretired_ids_found =
            (self.prev_retire_seq..self.retire_seq).any(|seq| self.active_seq.contains(&seq));

        let current_retire_prior_to = self.retire_seq;
        let next_retire_sequence = self
            .retire_timestamp
            .pop_front()
            // Sequence numbers are varints, below 2^62
            .map(|seq| seq.sequence.saturating_add(1));

        // According to RFC:
        // Endpoints SHOULD NOT issue updates of the Retire Prior To field
        // before receiving RETIRE_CONNECTION_ID frames that retire all
        // connection IDs indicated by the previous Retire Prior To value.
        // https://tools.ietf.org/html/draft-ietf-quic-transport-29#section-5.1.2
        if !unretired_ids_found {
            // All Cids are retired, `prev_retire_cid_seq` can be assigned to `retire_cid_seq`
            self.prev_retire_seq = self.retire_seq;
            // Advance `retire_seq` if next cid that needs to be retired exists
            if let Some(next_retire_prior_to) = next_retire_sequence {
                self.retire_seq = next_retire_prior_to;
            }
        }

        // Check if retirement of all CIDs that reach their lifetime is still needed
        // According to RFC:
        // An endpoint MUST NOT
        // provide more connection IDs than the peer's limit.  An endpoint MAY
        // send connection IDs that temporarily exceed a peer's limit if the
        // NEW_CONNECTION_ID frame also requires the retirement of any excess,
        // by including a sufficiently large value in the Retire Prior To field.
        //
        // If yes (return true), a new CID must be pushed with updated `retire_prior_to` field to remote peer.
        // If no (return false), it means CIDs that reach the end of lifetime have been retired already. Do not push a new CID in order to avoid violating above RFC.
        (current_retire_prior_to..self.retire_seq).any(|seq| self.active_seq.contains(&seq))
    }

    /// Update cid state when `NewIdentifiers` event is received
    pub(crate) fn new_cids(&mut self, ids: &[IssuedCid], now: Instant) {
        // `ids` could be `None` once active_connection_id_limit is set to 1 by peer
        let last_cid = match ids.last() {
            Some(cid) => cid,
            None => return,
        };
        // A count of CIDs issued, below 2^62
        self.issued = self.issued.saturating_add(ids.len() as u64);
        // Record the timestamp of CID with the largest seq number
        let sequence = last_cid.sequence;
        ids.iter().for_each(|frame| {
            self.active_seq.insert(frame.sequence);
        });
        self.track_lifetime(sequence, now);
    }

    /// Update CidState for receipt of a `RETIRE_CONNECTION_ID` frame
    ///
    /// Returns `None` for a frame naming an ID already retired, a frame repeated (its packet sent
    /// again, both arriving), which retires nothing and asks for no new CID; else whether a new CID
    /// can be issued; or an error if the frame was illegal.
    pub(crate) fn on_cid_retirement(
        &mut self,
        sequence: u64,
        limit: u64,
    ) -> Result<Option<bool>, TransportError> {
        if self.cid_len == 0 {
            return Err(TransportError::PROTOCOL_VIOLATION(
                "RETIRE_CONNECTION_ID when CIDs aren't in use",
            ));
        }
        if sequence > self.issued {
            debug!(
                sequence,
                "got RETIRE_CONNECTION_ID for unissued sequence number"
            );
            return Err(TransportError::PROTOCOL_VIOLATION(
                "RETIRE_CONNECTION_ID for unissued sequence number",
            ));
        }
        if !self.active_seq.remove(&sequence) {
            return Ok(None);
        }
        // Consider a scenario where peer A has active remote cid 0,1,2.
        // Peer B first send a NEW_CONNECTION_ID with cid 3 and retire_prior_to set to 1.
        // Peer A processes this NEW_CONNECTION_ID frame; update remote cid to 1,2,3
        // and meanwhile send a RETIRE_CONNECTION_ID to retire cid 0 to peer B.
        // If peer B doesn't check the cid limit here and send a new cid again, peer A will then face CONNECTION_ID_LIMIT_ERROR
        Ok(Some(limit > self.active_seq.len() as u64))
    }

    /// Keeps the connection ID `sequence`, which the peer retired, routing to the connection until
    /// `until`, at most `cap` of them; past that, returns the oldest, to stop routing now
    pub(crate) fn retire_later(
        &mut self,
        sequence: u64,
        until: Instant,
        cap: usize,
    ) -> Option<u64> {
        self.retiring.push_back((sequence, until));
        if self.retiring.len() > cap.max(1) {
            return self.retiring.pop_front().map(|(sequence, _)| sequence);
        }
        None
    }

    /// A retired connection ID whose routing ends by `now`, taken
    pub(crate) fn retired_due(&mut self, now: Instant) -> Option<u64> {
        let at = self.retiring.iter().position(|&(_, until)| until <= now)?;
        self.retiring.remove(at).map(|(sequence, _)| sequence)
    }

    /// When the next retired connection ID stops routing to the connection
    pub(crate) fn next_retired(&self) -> Option<Instant> {
        self.retiring.iter().map(|&(_, until)| until).min()
    }

    /// Length of local Connection IDs
    pub(crate) fn cid_len(&self) -> usize {
        self.cid_len
    }

    /// The value for `retire_prior_to` field in `NEW_CONNECTION_ID` frame
    pub(crate) fn retire_prior_to(&self) -> u64 {
        self.retire_seq
    }

    #[cfg(test)]
    pub(crate) fn active_seq(&self) -> (u64, u64) {
        let mut min = u64::MAX;
        let mut max = u64::MIN;
        for n in self.active_seq.iter() {
            if n < &min {
                min = *n;
            }
            if n > &max {
                max = *n;
            }
        }
        (min, max)
    }

    #[cfg(test)]
    pub(crate) fn assign_retire_seq(&mut self, v: u64) -> u64 {
        // Cannot retire more CIDs than what have been issued
        debug_assert!(v <= *self.active_seq.iter().max().unwrap() + 1);
        let n = v.checked_sub(self.retire_seq).unwrap();
        self.retire_seq = v;
        n
    }
}

/// Data structure that records when issued cids should be retired
#[derive(Copy, Clone, Eq, PartialEq)]
struct CidTimestamp {
    /// Highest cid sequence number created in a batch
    sequence: u64,
    /// Timestamp when cid needs to be retired
    timestamp: Instant,
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    /// A connection ID the peer retired routes until its time, the earliest first whatever the
    /// order retired, and no more are kept than the cap: past it the oldest retired stops at once.
    #[test]
    fn a_retired_id_routes_until_its_time_and_no_more_are_kept_than_the_cap() {
        let start = Instant::now();
        let mut state = CidState::new(8, None, start, 4);
        assert_eq!(state.retire_later(0, start + MS * 30, 2), None);
        assert_eq!(state.retire_later(1, start + MS * 20, 2), None);
        assert_eq!(state.next_retired(), Some(start + MS * 20));
        assert_eq!(state.retired_due(start + MS * 19), None);
        assert_eq!(state.retired_due(start + MS * 20), Some(1));
        assert_eq!(state.next_retired(), Some(start + MS * 30));
        assert_eq!(state.retire_later(2, start + MS * 40, 2), None);
        assert_eq!(
            state.retire_later(3, start + MS * 50, 2),
            Some(0),
            "past the cap the oldest retired stops routing at once"
        );
        assert_eq!(state.retired_due(start + MS * 50), Some(2));
        assert_eq!(state.retired_due(start + MS * 50), Some(3));
        assert_eq!(state.retired_due(start + MS * 50), None);
        assert_eq!(state.next_retired(), None);
    }

    /// A RETIRE_CONNECTION_ID repeated, its packet sent again and both arriving, retires nothing and
    /// asks for no new connection ID: a second one for the same retired ID would pass the peer's
    /// limit, as an upstream server's CONNECTION_ID_LIMIT_ERROR showed in hyper-quic's e2e.
    #[test]
    fn a_repeated_retirement_retires_nothing_and_asks_for_nothing() {
        let start = Instant::now();
        let mut state = CidState::new(8, None, start, 2);
        assert_eq!(state.on_cid_retirement(0, 2), Ok(Some(true)));
        assert_eq!(state.on_cid_retirement(0, 2), Ok(None));
    }
}
