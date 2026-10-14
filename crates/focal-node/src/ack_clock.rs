//! How late each peer answers this replica's messages (27 §8.4), timed from the answers Raft
//! itself sends: a leader's append is answered once its peer holds the entries it carried,
//! persisted, and a heartbeat by the peer's answer to it. What a commit waits on among the
//! voters is these answers, and the leader steps every one of them; a peer whose owner stalls
//! answers late, which the leader's own periods do not show.
//!
//! When every message was an exchange, its transport answered it at the peer's Ready fence and
//! the pool timed that answer: a second answer, beside Raft's, for every message. A message on
//! a lane is answered by nothing but Raft's own answer, so the answer is timed where it is
//! stepped.
//!
//! One message to a peer is timed at a time, the first to leave while none is, as a transport
//! times one packet a round trip, and only an answer of its term answers it: an append by the
//! first answer that takes the log to its last entry or past it, a heartbeat by the next answer
//! to a heartbeat. A timed message whose answer never comes (lost, or refused and sent again)
//! is answered by the first that does; until then its age is the peer's lateness, so a peer is
//! seen late while it stalls and not only once the stall ends.
use focal_consensus::{Message, MessageType, Stepped};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// Peers a clock is kept for at most: every member a configuration may name
/// (`crate::fleet::LOST_PEERS`).
const PEERS: usize = crate::fleet::LOST_PEERS;

/// Each peer's answers to this replica's messages: what they took, and the message timed now.
#[derive(Default)]
pub(crate) struct AckClock {
    peers: BTreeMap<u64, PeerClock>,
    /// The voters' lateness, gathered each time the quorum's is asked for: kept, so asking
    /// allocates only when the voters outgrow it.
    scratch: Vec<Duration>,
}

#[derive(Default)]
struct PeerClock {
    timed: Option<Timed>,
    answers: focal_timing::ExchangeRtt,
}

/// A message timed: what answers it, the term it left in, and when.
#[derive(Clone, Copy)]
struct Timed {
    awaits: Awaits,
    term: u64,
    sent: Instant,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Awaits {
    /// An append: answered once the peer holds the log to this index.
    Append(u64),
    /// A heartbeat: answered by the peer's answer to one.
    Heartbeat,
}

impl AckClock {
    /// `message` left for its peer at `now`: timed, unless a message of its term to the peer
    /// is timed already. Only appends and heartbeats are answered so that a leader waits on
    /// the answer; any other message is not timed.
    pub(crate) fn sent(&mut self, message: &Message, now: Instant) {
        let awaits = match message.msg_type {
            MessageType::MsgAppend => Awaits::Append(
                message
                    .entries
                    .last()
                    .map_or(message.index, |entry| entry.index),
            ),
            MessageType::MsgHeartbeat => Awaits::Heartbeat,
            _ => return,
        };
        if !self.peers.contains_key(&message.to) && self.peers.len() >= PEERS {
            return;
        }
        let clock = self.peers.entry(message.to).or_default();
        // A message of an older term is never answered in this one: the newer takes its place.
        if clock.timed.is_none_or(|timed| timed.term != message.term) {
            clock.timed = Some(Timed {
                awaits,
                term: message.term,
                sent: now,
            });
        }
    }

    /// A peer's message was stepped at `now`: the answer to the message timed, if it is one,
    /// is a sample of how late its peer answers.
    pub(crate) fn stepped(&mut self, stepped: Stepped, now: Instant) {
        let Some(clock) = self.peers.get_mut(&stepped.from) else {
            return;
        };
        let Some(timed) = clock.timed else {
            return;
        };
        let answers = stepped.term == timed.term
            && match (timed.awaits, stepped.msg_type) {
                (Awaits::Append(index), MessageType::MsgAppendResponse) => {
                    !stepped.reject && stepped.index >= index
                }
                (Awaits::Heartbeat, MessageType::MsgHeartbeatResponse) => true,
                _ => false,
            };
        if answers {
            clock.timed = None;
            let taken = now.saturating_duration_since(timed.sent);
            clock
                .answers
                .on_sample(u64::try_from(taken.as_nanos()).unwrap_or(u64::MAX));
        }
    }

    /// How late `peer` answers at least: the tail of its answers, or the age of the message of
    /// `term` timed and not yet answered where that is more. `None` while nothing was measured
    /// or is out.
    pub(crate) fn lateness(&self, peer: u64, term: u64, now: Instant) -> Option<Duration> {
        let clock = self.peers.get(&peer)?;
        let answered = clock.answers.tail_ns().map(Duration::from_nanos);
        let age = clock
            .timed
            .filter(|timed| timed.term == term)
            .map(|timed| now.saturating_duration_since(timed.sent));
        match (answered, age) {
            (Some(answered), Some(age)) => Some(answered.max(age)),
            (answered, age) => answered.or(age),
        }
    }

    /// The lateness a commit of `term` waits on among `voters`, `local` among them
    /// ([`crate::pace::quorum_tail`]).
    pub(crate) fn quorum_tail(
        &mut self,
        voters: &[u64],
        local: u64,
        term: u64,
        now: Instant,
    ) -> Option<Duration> {
        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();
        for voter in voters.iter().filter(|voter| **voter != local) {
            if let Some(late) = self.lateness(*voter, term, now) {
                // A voter there is no room to gather for is one not measured.
                if scratch.try_reserve(1).is_ok() {
                    scratch.push(late);
                }
            }
        }
        let tail = crate::pace::quorum_tail(&mut scratch, voters.len());
        self.scratch = scratch;
        tail
    }

    /// Keeps only the peers `member` names: a peer that left the configuration is forgotten.
    pub(crate) fn retain(&mut self, member: impl Fn(u64) -> bool) {
        self.peers.retain(|peer, _| member(*peer));
    }
}

#[cfg(test)]
#[path = "ack_clock_tests.rs"]
mod tests;
