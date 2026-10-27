//! The barriers a copy asks its leader for its linearizable reads (27 §5), at most its share of
//! the reads its leader may hold at once. A leader holds the reads that wait for their quorum up to
//! its core's bound (`DurableNode::pending_reads`) and refuses the next, unanswered: a copy asked
//! more reads at once than its leader can hold loses them, and their askers wait out their time
//! (compare CI's work mode at 100 claims a second, every agent's reads linearizable at its home: the
//! 1-CPU leader refused 984 and 1,165 of its peers' frames where none were refused with no read
//! barriers, and 63 and 140 lifecycles a round were abandoned on a read; run 40584468171). Each copy
//! keeps at most the bound divided among the members that may ask (`window`).
//!
//! Short of its share a copy asks a barrier for each read as it is asked, the leader's rounds
//! sharing what each drain carries (27 §10), so no read waits for another's barrier. At its share a
//! read waits, and every read that waited is asked for by one barrier once one comes back: a
//! barrier asked after a read was held confirms it, as one round of heartbeats confirms every read
//! asked before it was sent (Ongaro's thesis §6.4), and as etcd's members ask one read index for
//! every read that waited while the last was out (server/etcdserver/read/read.go,
//! `LinearizableReadLoop`). A leader whose answers slow down is so asked fewer barriers, each for
//! more reads.
//!
//! A barrier's answer can be lost within a term (a connection to the leader closed with it in
//! flight): one out is asked again once it has waited the core's election timeout in ticks, the
//! period after which the copy would take its leader for gone, and given up with its reads after a
//! second, which frees its place for the reads waiting. A new term ends every barrier, and the
//! reads waiting on one are given up with the term they were asked in.
//!
//! The barriers a copy holds, out or answered ahead of what it has applied, are bounded as its
//! parked barriers were, by the core's bound: one at it is too far behind its leader to take another
//! read. The reads themselves, which cost the leader nothing past their barrier's, are held in room
//! grown as they come and charged to the copy's memory before it is taken (`room`).

use super::ReadCorrelation;
use std::collections::VecDeque;

/// A barrier asked and the reads it confirms: `reads` of those held, after the earlier rounds'.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Round {
    /// The barrier's correlation: that of the first read it confirms.
    barrier: ReadCorrelation,
    /// The reads it confirms.
    reads: usize,
    /// The index its answer named, once it came.
    answer: Option<u64>,
    /// The ticks it has waited unanswered.
    waited: usize,
}

/// The reads a copy holds and the barriers asked for them.
#[derive(Debug, Default)]
pub(crate) struct ReadRounds {
    /// Every read held, in rounds in the order they were asked, then those waiting for a barrier.
    reads: VecDeque<ReadCorrelation>,
    /// The barriers asked whose reads are not yet served, in the order asked.
    rounds: VecDeque<Round>,
    /// The term the reads were asked in.
    term: u64,
}

impl ReadRounds {
    /// The reads held: waiting on a barrier, or answered and waiting to be applied here.
    pub(crate) fn held(&self) -> usize {
        self.reads.len()
    }
    /// The barriers held: out, or answered and waiting for this copy to apply their index.
    pub(crate) fn rounds(&self) -> usize {
        self.rounds.len()
    }
    /// The reads room is held for.
    pub(crate) fn room(&self) -> usize {
        self.reads.capacity().min(self.rounds.capacity())
    }
    /// The barriers out: asked and not answered.
    fn out(&self) -> usize {
        self.rounds
            .iter()
            .filter(|round| round.answer.is_none())
            .count()
    }
    /// The reads in rounds; those after them wait for a barrier.
    fn in_rounds(&self) -> usize {
        self.rounds
            .iter()
            .fold(0usize, |sum, round| sum.saturating_add(round.reads))
    }
    /// The bytes room for `room` reads holds, to charge.
    pub(crate) fn bytes_for(room: usize) -> usize {
        room.saturating_mul(
            std::mem::size_of::<ReadCorrelation>().saturating_add(std::mem::size_of::<Round>()),
        )
    }
    /// Room for `room` reads, taken before the first is held so that holding one never
    /// allocates; `false` when the memory cannot be had.
    pub(crate) fn reserve(&mut self, room: usize) -> bool {
        let reads = room.saturating_sub(self.reads.len());
        let rounds = room.saturating_sub(self.rounds.len());
        self.reads.try_reserve_exact(reads).is_ok() && self.rounds.try_reserve_exact(rounds).is_ok()
    }
    /// Holds `read`: asked for by a barrier of its own (`ask`) while fewer than `window` are
    /// out and none waits, behind the reads waiting otherwise. A barrier refused holds nothing.
    pub(crate) fn hold<E>(
        &mut self,
        read: ReadCorrelation,
        window: usize,
        ask: impl FnOnce(ReadCorrelation) -> Result<(), E>,
    ) -> Result<(), E> {
        if self.out() >= window.max(1) || self.in_rounds() < self.reads.len() {
            self.reads.push_back(read);
            return Ok(());
        }
        ask(read)?;
        self.reads.push_back(read);
        self.rounds.push_back(Round {
            barrier: read,
            reads: 1,
            answer: None,
            waited: 0,
        });
        Ok(())
    }
    /// The barrier `barrier`, answered at `index`: its round is served once the copy has applied
    /// `index`, and the reads waiting are asked for. An answer to a barrier not out (one asked
    /// again and answered twice, or one given up) serves nothing. The reads given up because
    /// their barrier was refused.
    pub(crate) fn answered<E>(
        &mut self,
        barrier: ReadCorrelation,
        index: u64,
        window: usize,
        ask: impl FnOnce(ReadCorrelation) -> Result<(), E>,
    ) -> usize {
        if let Some(round) = self
            .rounds
            .iter_mut()
            .find(|round| round.answer.is_none() && round.barrier == barrier)
        {
            round.answer = Some(index);
        }
        self.ask_waiting(window, ask)
    }
    /// The reads waiting, asked for by one barrier (`ask`) when fewer than `window` are out, and
    /// given up if it is refused: how many were given up.
    fn ask_waiting<E>(
        &mut self,
        window: usize,
        ask: impl FnOnce(ReadCorrelation) -> Result<(), E>,
    ) -> usize {
        let in_rounds = self.in_rounds();
        let waiting = self.reads.len().saturating_sub(in_rounds);
        let Some(&first) = self.reads.get(in_rounds) else {
            return 0;
        };
        if waiting == 0 || self.out() >= window.max(1) {
            return 0;
        }
        if ask(first).is_err() {
            self.reads.truncate(in_rounds);
            return waiting;
        }
        self.rounds.push_back(Round {
            barrier: first,
            reads: waiting,
            answer: None,
            waited: 0,
        });
        0
    }
    /// Up to `room` of the reads whose barriers were answered at or below `applied`, each with
    /// its round's answer: the copy has applied what they must see. A round served in part keeps
    /// the rest for the next call.
    pub(crate) fn take_served(
        &mut self,
        applied: u64,
        mut room: usize,
        mut serve: impl FnMut(ReadCorrelation, u64),
    ) {
        // The reads of the rounds before `position` start the deque, `start` of them.
        let mut position = 0;
        let mut start = 0usize;
        while room > 0
            && let Some(round) = self.rounds.get_mut(position)
        {
            let Some(index) = round.answer.filter(|index| *index <= applied) else {
                start = start.saturating_add(round.reads);
                position = position.saturating_add(1);
                continue;
            };
            while room > 0 && round.reads > 0 {
                let Some(read) = self.reads.remove(start) else {
                    round.reads = 0;
                    break;
                };
                round.reads = round.reads.saturating_sub(1);
                room = room.saturating_sub(1);
                serve(read, index);
            }
            if round.reads == 0 {
                self.rounds.remove(position);
            }
        }
    }
    /// One tick of the copy's clock: each barrier out asked again (`ask`) once it has waited
    /// `patience` ticks unanswered, and given up with its reads once it has waited twice that,
    /// which frees its place for the reads waiting. The reads given up.
    pub(crate) fn tick<E>(
        &mut self,
        patience: usize,
        window: usize,
        mut ask: impl FnMut(ReadCorrelation) -> Result<(), E>,
    ) -> usize {
        let patience = patience.max(1);
        let mut given = 0usize;
        let mut position = 0;
        let mut start = 0usize;
        while let Some(round) = self.rounds.get_mut(position) {
            if round.answer.is_some() {
                start = start.saturating_add(round.reads);
                position = position.saturating_add(1);
                continue;
            }
            round.waited = round.waited.saturating_add(1);
            if round.waited == patience {
                // Refused again, it waits on and is given up at the second end.
                let _ = ask(round.barrier);
            }
            if round.waited < patience.saturating_mul(2) {
                start = start.saturating_add(round.reads);
                position = position.saturating_add(1);
                continue;
            }
            let reads = round.reads;
            self.rounds.remove(position);
            for _ in 0..reads {
                if self.reads.remove(start).is_none() {
                    break;
                }
            }
            given = given.saturating_add(reads);
        }
        given.saturating_add(self.ask_waiting(window, ask))
    }
    /// The reads held for `term`: those of an earlier term are given up with it, however the copy
    /// learned it (a read asked, a tick or a delivery). How many were given up.
    pub(crate) fn begin_term(&mut self, term: u64) -> usize {
        if term == self.term {
            return 0;
        }
        let given = self.reads.len();
        self.reads.clear();
        self.rounds.clear();
        self.term = term;
        given
    }
}

#[cfg(test)]
#[path = "native_read_rounds_tests.rs"]
mod tests;
