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
//! is answered by the first that does.
//!
//! Only answers measure. A peer that stalls is measured late once it answers, its answer as old
//! as the stall; a message still unanswered measures nothing. The patience a replica takes from
//! these answers (before it asks whether a quorum heard it, and beyond a request's time) is
//! what its voters' answers have taken, and grows only as late answers come: silence never
//! renews it, so a leader its quorum no longer answers (cut off, or its peers gone) stands down
//! within an election timeout and that patience, where taking the unanswered message's age as
//! lateness moved the check on at every tick, up to its cap.
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
    /// diag (not for merge): each append's last index and when it first left to any peer, until a
    /// peer's answer covers it: the commit's replication, as hyper-quic's replication test times it.
    firsts: std::collections::VecDeque<(u64, Instant)>,
    first_sent: u64,
    first_hist: Vec<u64>,
    /// The voters' lateness, gathered each time the quorum's is asked for: kept, so asking
    /// allocates only when the voters outgrow it.
    scratch: Vec<Duration>,
}

#[derive(Default)]
struct PeerClock {
    timed: Option<Timed>,
    answers: focal_timing::ExchangeRtt,
    /// diag (not for merge): every append in flight, its last index and when it left.
    flight: std::collections::VecDeque<(u64, Instant)>,
    /// diag: append-to-answer times since the last report, in log2 buckets of microseconds.
    hist: Vec<u64>,
    appends: u64,
    answered_appends: u64,
    max_us: u64,
}

/// diag (not for merge): each peer's appends and their answer times since the last call, as a
/// line; the counts reset.
pub(crate) fn diag_report(clock: &mut AckClock) -> String {
    let mut out = String::new();
    {
        let total: u64 = clock.first_hist.iter().sum();
        let quantile = |q: f64| -> u64 {
            if total == 0 {
                return 0;
            }
            let want = ((total as f64) * q).ceil() as u64;
            let mut seen = 0u64;
            for (bucket, count) in clock.first_hist.iter().enumerate() {
                seen += count;
                if seen >= want {
                    return (bucket as u64 + 1) * 10;
                }
            }
            0
        };
        out.push_str(&format!(
            " first: n={} p10_ms<{} p50_ms<{} p90_ms<{} p99_ms<{}",
            total,
            quantile(0.1),
            quantile(0.5),
            quantile(0.9),
            quantile(0.99),
        ));
        clock.first_hist.iter_mut().for_each(|count| *count = 0);
    }
    for (peer, c) in clock.peers.iter_mut() {
        let total: u64 = c.hist.iter().sum();
        let quantile = |q: f64| -> u64 {
            if total == 0 {
                return 0;
            }
            let want = ((total as f64) * q).ceil() as u64;
            let mut seen = 0u64;
            for (bucket, count) in c.hist.iter().enumerate() {
                seen += count;
                if seen >= want {
                    return (bucket as u64 + 1) * 10_000;
                }
            }
            0
        };
        out.push_str(&format!(
            " peer={peer} appends={} answered={} p50_us<{} p90_us<{} p99_us<{} max_us={} inflight={}",
            c.appends,
            c.answered_appends,
            quantile(0.5),
            quantile(0.9),
            quantile(0.99),
            c.max_us,
            c.flight.len()
        ));
        c.hist.iter_mut().for_each(|count| *count = 0);
        c.appends = 0;
        c.answered_appends = 0;
        c.max_us = 0;
    }
    out
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
        // diag: an append's first leaving, to any peer
        if let Awaits::Append(last) = awaits
            && !message.entries.is_empty()
            && last > self.first_sent
        {
            self.first_sent = last;
            if self.firsts.len() < 4096 {
                self.firsts.push_back((last, now));
            }
        }
        let clock = self.peers.entry(message.to).or_default();
        // diag: every append in flight.
        if let Awaits::Append(last) = awaits
            && !message.entries.is_empty()
        {
            clock.appends += 1;
            if clock.flight.len() < 4096 {
                clock.flight.push_back((last, now));
            }
        }
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
        // diag: the first answer covering an append is its replication's end
        if stepped.msg_type == MessageType::MsgAppendResponse && !stepped.reject {
            while let Some(&(last, sent)) = self.firsts.front() {
                if last > stepped.index {
                    break;
                }
                self.firsts.pop_front();
                let ms = now.saturating_duration_since(sent).as_millis() as usize;
                if self.first_hist.is_empty() {
                    self.first_hist.resize(256, 0);
                }
                self.first_hist[(ms / 10).min(255)] += 1;
            }
        }
        let Some(clock) = self.peers.get_mut(&stepped.from) else {
            return;
        };
        // diag: every append the answer covers.
        if stepped.msg_type == MessageType::MsgAppendResponse && !stepped.reject {
            while let Some(&(last, sent)) = clock.flight.front() {
                if last > stepped.index {
                    break;
                }
                clock.flight.pop_front();
                let us = now.saturating_duration_since(sent).as_micros() as u64;
                if clock.hist.is_empty() {
                    clock.hist.resize(256, 0);
                }
                clock.hist[(us as usize / 10_000).min(255)] += 1;
                clock.answered_appends += 1;
                clock.max_us = clock.max_us.max(us);
            }
        }
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

    /// How late `peer` answers: the tail of its answers, `None` while it has answered nothing.
    /// A message of it still unanswered makes it no later.
    pub(crate) fn answered(&self, peer: u64) -> Option<Duration> {
        let clock = self.peers.get(&peer)?;
        clock.answers.tail_ns().map(Duration::from_nanos)
    }

    /// The age of the message of `term` timed to `peer` and not yet answered, if one is.
    #[cfg(test)]
    pub(crate) fn outstanding(&self, peer: u64, term: u64, now: Instant) -> Option<Duration> {
        let clock = self.peers.get(&peer)?;
        clock
            .timed
            .filter(|timed| timed.term == term)
            .map(|timed| now.saturating_duration_since(timed.sent))
    }

    /// How late a commit's quorum answers among `voters`, `local` among them
    /// ([`crate::pace::quorum_tail`]): of their answers alone, so never later for their silence.
    pub(crate) fn quorum_tail(&mut self, voters: &[u64], local: u64) -> Option<Duration> {
        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();
        for voter in voters.iter().filter(|voter| **voter != local) {
            if let Some(late) = self.answered(*voter) {
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
