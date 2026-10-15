//! One round of requests to several peers, asked at once (mantle note 32 T45; focal `round.rs`,
//! made sans-io over hyper-timing's [`RoundWait`]).
//!
//! The round ends when it has what it needs, when every peer has reported, or when its wait says so:
//! stalled past the lookahead of its deadline, or extended as far as it may be. A dead peer costs
//! the round nothing the live ones do not need, and a round whose replies are still arriving is not
//! cut off at a fixed time. The owner opens the round's exchanges, reports each answer or refusal
//! as its event arrives, and asks [`Round::judge`] at [`Round::next_judgement`]. focal bounded a
//! round at 1,024 peers because it held a future per peer; this round holds counts only, so it has
//! no table to bound.

use std::time::{Duration, Instant};

use hyper_timing::{RoundBudget, RoundWait};

/// How a round ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoundEnd {
    /// The caller's test held.
    Enough,
    /// Every peer reported, and that was not enough.
    AllReported,
    /// The wait ended the round with peers outstanding.
    Expired,
}

/// One round's state.
#[derive(Clone, Copy, Debug)]
pub struct Round {
    wait: RoundWait,
    began: Instant,
    asked: usize,
    reported: usize,
    answered: usize,
    end: Option<RoundEnd>,
}

fn nanos(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

impl Round {
    /// A round under `budget` that asked `asked` peers at `now`.
    pub fn begin(budget: &RoundBudget, asked: usize, now: Instant) -> Self {
        Self {
            wait: RoundWait::begin(budget, 0),
            began: now,
            asked,
            reported: 0,
            answered: 0,
            end: (asked == 0).then_some(RoundEnd::AllReported),
        }
    }
    /// A peer reported at `now`, with an answer or without; `enough` is the caller's test of what
    /// the round has gathered with it. Returns the round's end once it has one.
    pub fn report(&mut self, now: Instant, answered: bool, enough: bool) -> Option<RoundEnd> {
        if self.end.is_some() {
            return self.end;
        }
        self.reported = self.reported.saturating_add(1);
        if answered {
            self.answered = self.answered.saturating_add(1);
            let gathered = u64::try_from(self.answered).unwrap_or(u64::MAX);
            let _ = self.wait.judge(gathered, self.elapsed(now));
            if enough {
                self.end = Some(RoundEnd::Enough);
                return self.end;
            }
        }
        if self.reported >= self.asked {
            self.end = Some(RoundEnd::AllReported);
        }
        self.end
    }
    /// Judge the round at `now`: a reply in hand is reported before the round is judged.
    pub fn judge(&mut self, now: Instant) -> Option<RoundEnd> {
        if self.end.is_none() && now >= self.next_judgement() {
            let gathered = u64::try_from(self.answered).unwrap_or(u64::MAX);
            if !self.wait.judge(gathered, self.elapsed(now)) {
                self.end = Some(RoundEnd::Expired);
            }
        }
        self.end
    }
    /// When the round is next judged.
    pub fn next_judgement(&self) -> Instant {
        let at = Duration::from_nanos(self.wait.next_judgement_ns());
        self.began.checked_add(at).unwrap_or(self.began)
    }
    /// The round's end, once it has one.
    pub fn end(&self) -> Option<RoundEnd> {
        self.end
    }
    /// Peers asked, peers that reported, and peers that answered.
    pub fn counts(&self) -> (usize, usize, usize) {
        (self.asked, self.reported, self.answered)
    }
    fn elapsed(&self, now: Instant) -> u64 {
        nanos(now.saturating_duration_since(self.began))
    }
}

#[cfg(test)]
mod tests {
    //! focal `round.rs`'s tests, on the caller's clock instead of tokio's paused one: each peer
    //! reports at a time of its own or never, and the driver below judges the round at its next
    //! judgement or delivers the next report, whichever comes first, a report first on a tie
    //! (focal's `biased` select).
    use super::*;
    use hyper_timing::RoundAnchors;

    const NEVER: u64 = u64::MAX;
    const MS: u64 = 1_000_000;
    fn ms(value: u64) -> Duration {
        Duration::from_millis(value)
    }
    /// focal's near group: opens for 100 ms, judged at 75 ms of the deadline in force once
    /// something has arrived, extended 100 ms at a time, stalled after 200 ms without an answer.
    fn budget() -> RoundBudget {
        let anchors = RoundAnchors {
            heartbeat_ns: 100 * MS,
            stall_periods: 2,
            polls_per_period: 10,
            lookahead: (3, 4),
        };
        RoundBudget::derive(&anchors, Some(10 * MS), 5_000 * MS)
    }
    /// Runs a round over `peers` (when each reports, and whether with an answer) until it ends;
    /// returns the round, the peers whose answers it took, in order, and when it ended.
    fn run(
        peers: &[(u64, bool)],
        budget: RoundBudget,
        need: usize,
    ) -> (Round, Vec<usize>, Duration) {
        let start = hyper_sim::Anchor::new().instant(0).unwrap();
        let mut round = Round::begin(&budget, peers.len(), start);
        let mut order: Vec<usize> = (0..peers.len())
            .filter(|&peer| peers[peer].0 != NEVER)
            .collect();
        order.sort_by_key(|&peer| peers[peer].0);
        let mut answers = Vec::new();
        let mut next = order.into_iter().peekable();
        let mut now = start;
        while round.end().is_none() {
            let judged = round.next_judgement();
            let report = next.peek().map(|&peer| start + ms(peers[peer].0));
            match report {
                Some(at) if at <= judged => {
                    now = at;
                    let peer = next.next().unwrap();
                    let answered = peers[peer].1;
                    if answered {
                        answers.push(peer + 1);
                    }
                    round.report(now, answered, answers.len() >= need);
                }
                _ => {
                    now = judged;
                    round.judge(now);
                }
            }
        }
        (round, answers, now - start)
    }

    #[test]
    fn a_dead_peer_costs_the_round_nothing() {
        let (round, answers, took) = run(&[(10, true), (NEVER, true), (20, true)], budget(), 2);
        assert_eq!(
            (round.end(), round.counts()),
            (Some(RoundEnd::Enough), (3, 2, 2))
        );
        assert_eq!(answers, vec![1, 3]);
        assert_eq!(took, ms(20));
    }

    #[test]
    fn a_round_ends_when_every_peer_has_reported() {
        let (round, answers, took) = run(&[(10, true), (30, false), (20, false)], budget(), 2);
        assert_eq!(
            (round.end(), round.counts()),
            (Some(RoundEnd::AllReported), (3, 3, 1))
        );
        assert_eq!(answers, vec![1]);
        assert_eq!(took, ms(30));
        let (round, _, took) = run(&[], budget(), 1);
        assert_eq!(
            (round.end(), round.counts().0, took),
            (Some(RoundEnd::AllReported), 0, ms(0))
        );
    }

    #[test]
    fn a_round_with_no_answer_ends_at_its_deadline() {
        let (round, _, took) = run(&[(NEVER, true), (NEVER, true)], budget(), 1);
        assert_eq!(
            (round.end(), round.counts().1),
            (Some(RoundEnd::Expired), 0)
        );
        assert_eq!(took, ms(100));
        // A refusal is a report and no answer: it does not extend the round.
        let (round, _, took) = run(&[(5, false), (NEVER, true)], budget(), 1);
        assert_eq!(
            (round.end(), round.counts().1),
            (Some(RoundEnd::Expired), 1)
        );
        assert_eq!(took, ms(100));
    }

    #[test]
    fn an_answer_in_the_last_quarter_of_the_deadline_is_collected() {
        let (round, answers, took) = run(&[(90, true), (NEVER, true)], budget(), 1);
        assert_eq!((round.end(), round.counts().2), (Some(RoundEnd::Enough), 1));
        assert_eq!(answers, vec![1]);
        assert_eq!(took, ms(90));
    }

    #[test]
    fn a_round_whose_answers_keep_arriving_is_extended_past_its_deadline() {
        let peers: Vec<(u64, bool)> = (1..=10).map(|peer| (peer * 60, true)).collect();
        let (round, answers, took) = run(&peers, budget(), 10);
        assert_eq!(
            (round.end(), round.counts().2),
            (Some(RoundEnd::Enough), 10)
        );
        assert_eq!(answers.len(), 10);
        assert_eq!(took, ms(600));
        assert!(took > Duration::from_nanos(budget().deadline_ns));
    }

    #[test]
    fn a_round_that_stalls_ends_with_its_peers_outstanding() {
        // Two answers early, then nothing: judged at 75 and 150 ms with an answer inside the stall
        // window, and at 225 ms without.
        let (round, answers, took) = run(
            &[(10, true), (20, true), (NEVER, true), (NEVER, true)],
            budget(),
            3,
        );
        assert_eq!(
            (round.end(), round.counts()),
            (Some(RoundEnd::Expired), (4, 2, 2))
        );
        assert_eq!(answers, vec![1, 2]);
        assert_eq!(took, ms(225));
    }

    #[test]
    fn extensions_end() {
        // An answer every 50 ms for ever, and never enough.
        let peers: Vec<(u64, bool)> = (1..=200).map(|peer| (peer * 50, true)).collect();
        let (round, _, took) = run(&peers, budget(), usize::MAX);
        assert_eq!(round.end(), Some(RoundEnd::Expired));
        assert!(
            took <= Duration::from_nanos(budget().max_deadline_ns()),
            "{took:?}"
        );
        assert!(took >= ms(800), "{took:?}");
    }

    #[test]
    fn an_answer_that_arrives_with_the_judgement_is_read_first() {
        let (round, answers, took) = run(
            &[(10, true), (20, true), (225, true), (NEVER, true)],
            budget(),
            3,
        );
        assert_eq!((round.end(), round.counts().2), (Some(RoundEnd::Enough), 3));
        assert_eq!(answers, vec![1, 2, 3]);
        assert_eq!(took, ms(225));
    }

    #[test]
    fn an_unmeasured_peer_opens_the_round_to_the_ceiling() {
        let anchors = RoundAnchors {
            heartbeat_ns: 100 * MS,
            stall_periods: 2,
            polls_per_period: 10,
            lookahead: (3, 4),
        };
        let budget = RoundBudget::derive(&anchors, None, 5_000 * MS);
        let (round, _, took) = run(&[(4_000, true), (NEVER, true)], budget, 1);
        assert_eq!((round.end(), took), (Some(RoundEnd::Enough), ms(4_000)));
        let (round, _, took) = run(&[(NEVER, true)], budget, 1);
        assert_eq!((round.end(), took), (Some(RoundEnd::Expired), ms(5_000)));
    }
}
