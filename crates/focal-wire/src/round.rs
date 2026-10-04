//! One round of requests to several peers, asked at once (27 §3.1 P1).
//!
//! The round ends when it has what it needs, when every peer has reported,
//! or when its wait says so ([`focal_timing::RoundWait`]): stalled past the
//! lookahead of its deadline, or extended as far as it may be. A peer that
//! is dead costs the round nothing the live ones do not need, and a round
//! whose replies are still arriving is not cut off at a fixed time.
//!
//! The exchanges still in flight when the round ends are dropped with it.
//! The pool counts each as given up on, which lengthens what that peer is
//! expected to take ([`crate::PeerConnectionPool::exchange_tail`]), so an
//! estimate that ended a round too early corrects itself.
use focal_timing::{RoundBudget, RoundWait};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::future::Future;

/// The most peers one round asks.
pub const MAX_ROUND_PEERS: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundEnd {
    /// The caller's test held.
    Enough,
    /// Every peer reported, and that was not enough.
    AllReported,
    /// The wait ended the round with peers outstanding.
    Expired,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Round {
    pub end: RoundEnd,
    pub asked: usize,
    /// Peers that reported, with an answer or without.
    pub reported: usize,
    /// Peers that answered.
    pub answered: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RoundError {
    #[error("a round asks at most {MAX_ROUND_PEERS} peers")]
    Capacity,
    #[error("a round needs a runtime with a timer")]
    Runtime,
}

/// Ask every peer at once. `asks` are the exchanges, each yielding its
/// peer's answer or none; `enough` is given each answer as it arrives and
/// says whether the round has what it needs.
pub async fn gather<T, F>(
    asks: impl IntoIterator<Item = (u64, F)>,
    budget: RoundBudget,
    mut enough: impl FnMut(u64, T) -> bool,
) -> Result<Round, RoundError>
where
    F: Future<Output = Option<T>>,
{
    let mut exchanges = FuturesUnordered::new();
    for (peer, ask) in asks {
        if exchanges.len() >= MAX_ROUND_PEERS {
            return Err(RoundError::Capacity);
        }
        exchanges.push(async move { (peer, ask.await) });
    }
    let asked = exchanges.len();
    let mut round = Round {
        end: RoundEnd::AllReported,
        asked,
        reported: 0,
        answered: 0,
    };
    if tokio::runtime::Handle::try_current().is_err() {
        return Err(RoundError::Runtime);
    }
    let started = tokio::time::Instant::now();
    let elapsed = |started: tokio::time::Instant| {
        u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
    };
    let mut wait = RoundWait::begin(&budget, 0);
    let count = |value: usize| u64::try_from(value).unwrap_or(u64::MAX);
    loop {
        let judged = started
            .checked_add(std::time::Duration::from_nanos(wait.next_judgement_ns()))
            .ok_or(RoundError::Runtime)?;
        tokio::select! {
            // A reply in hand is read before the wait is judged.
            biased;
            reported = exchanges.next() => {
                let Some((peer, answer)) = reported else {
                    return Ok(round);
                };
                round.reported = round.reported.saturating_add(1);
                if let Some(answer) = answer {
                    round.answered = round.answered.saturating_add(1);
                    let _ = wait.judge(count(round.answered), elapsed(started));
                    if enough(peer, answer) {
                        round.end = RoundEnd::Enough;
                        return Ok(round);
                    }
                }
            }
            () = tokio::time::sleep_until(judged) => {
                if !wait.judge(count(round.answered), elapsed(started)) {
                    round.end = RoundEnd::Expired;
                    return Ok(round);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const NEVER: u64 = u64::MAX;
    fn ms(value: u64) -> Duration {
        Duration::from_millis(value)
    }
    /// A near group: opens for 100 ms, given all of it while nothing has
    /// arrived, judged at 75 ms of the deadline in force once something
    /// has, extended 100 ms at a time, stalled after 200 ms without an
    /// answer.
    fn budget() -> RoundBudget {
        RoundBudget::derive(ms(100), Some(ms(10)), ms(5_000))
    }
    /// A peer that answers after `after` milliseconds, with an answer or
    /// without one, or never.
    async fn peer(after: u64, answers: bool) -> Option<u64> {
        if after == NEVER {
            std::future::pending::<()>().await;
        }
        tokio::time::sleep(ms(after)).await;
        answers.then_some(after)
    }
    async fn run(
        peers: &[(u64, bool)],
        budget: RoundBudget,
        need: usize,
    ) -> (Round, Vec<u64>, Duration) {
        let started = tokio::time::Instant::now();
        let mut answers = Vec::new();
        let asks = peers
            .iter()
            .enumerate()
            .map(|(index, (after, answers))| (index as u64 + 1, peer(*after, *answers)));
        let round = gather(asks, budget, |peer, _| {
            answers.push(peer);
            answers.len() >= need
        })
        .await
        .unwrap();
        (round, answers, started.elapsed())
    }

    #[tokio::test(start_paused = true)]
    async fn a_dead_peer_costs_the_round_nothing() {
        let (round, answers, took) =
            run(&[(10, true), (NEVER, true), (20, true)], budget(), 2).await;
        assert_eq!(
            round,
            Round {
                end: RoundEnd::Enough,
                asked: 3,
                reported: 2,
                answered: 2
            }
        );
        assert_eq!(answers, vec![1, 3]);
        assert_eq!(took, ms(20));
    }
    #[tokio::test(start_paused = true)]
    async fn a_round_ends_when_every_peer_has_reported() {
        let (round, answers, took) =
            run(&[(10, true), (30, false), (20, false)], budget(), 2).await;
        assert_eq!(
            round,
            Round {
                end: RoundEnd::AllReported,
                asked: 3,
                reported: 3,
                answered: 1
            }
        );
        assert_eq!(answers, vec![1]);
        assert_eq!(took, ms(30));
        let (round, _, took) = run(&[], budget(), 1).await;
        assert_eq!(
            (round.end, round.asked, took),
            (RoundEnd::AllReported, 0, ms(0))
        );
    }
    #[tokio::test(start_paused = true)]
    async fn a_round_with_no_answer_ends_at_its_deadline() {
        let (round, _, took) = run(&[(NEVER, true), (NEVER, true)], budget(), 1).await;
        assert_eq!((round.end, round.reported), (RoundEnd::Expired, 0));
        assert_eq!(took, ms(100));
        // A refusal is a report and no answer: it does not extend the round.
        let (round, _, took) = run(&[(5, false), (NEVER, true)], budget(), 1).await;
        assert_eq!((round.end, round.reported), (RoundEnd::Expired, 1));
        assert_eq!(took, ms(100));
    }
    #[tokio::test(start_paused = true)]
    async fn an_answer_in_the_last_quarter_of_the_deadline_is_collected() {
        // The deadline is the tail of these peers: an answer at 90 ms of a
        // 100 ms round is the answer the round opened for.
        let (round, answers, took) = run(&[(90, true), (NEVER, true)], budget(), 1).await;
        assert_eq!((round.end, round.answered), (RoundEnd::Enough, 1));
        assert_eq!(answers, vec![1]);
        assert_eq!(took, ms(90));
    }
    #[tokio::test(start_paused = true)]
    async fn a_round_whose_answers_keep_arriving_is_extended_past_its_deadline() {
        let peers: Vec<(u64, bool)> = (1..=10).map(|peer| (peer * 60, true)).collect();
        let (round, answers, took) = run(&peers, budget(), 10).await;
        assert_eq!((round.end, round.answered), (RoundEnd::Enough, 10));
        assert_eq!(answers.len(), 10);
        assert_eq!(took, ms(600));
        assert!(took > Duration::from_nanos(budget().deadline_ns));
    }
    #[tokio::test(start_paused = true)]
    async fn a_round_that_stalls_ends_with_its_peers_outstanding() {
        // Two answers early, then nothing: judged at 75 and 150 ms with an
        // answer inside the stall window, and at 225 ms without.
        let (round, answers, took) = run(
            &[(10, true), (20, true), (NEVER, true), (NEVER, true)],
            budget(),
            3,
        )
        .await;
        assert_eq!(
            round,
            Round {
                end: RoundEnd::Expired,
                asked: 4,
                reported: 2,
                answered: 2
            }
        );
        assert_eq!(answers, vec![1, 2]);
        assert_eq!(took, ms(225));
    }
    #[tokio::test(start_paused = true)]
    async fn extensions_end() {
        // An answer every 50 ms for ever, and never enough.
        let peers: Vec<(u64, bool)> = (1..=200).map(|peer| (peer * 50, true)).collect();
        let (round, _, took) = run(&peers, budget(), usize::MAX).await;
        assert_eq!(round.end, RoundEnd::Expired);
        assert!(
            took <= Duration::from_nanos(budget().max_deadline_ns()),
            "{took:?}"
        );
        assert!(took >= ms(800), "{took:?}");
    }
    #[tokio::test(start_paused = true)]
    async fn an_answer_that_arrives_with_the_judgement_is_read_first() {
        // The third answer arrives at 225 ms, the instant the round of the
        // stalled test ended.
        let (round, answers, took) = run(
            &[(10, true), (20, true), (225, true), (NEVER, true)],
            budget(),
            3,
        )
        .await;
        assert_eq!((round.end, round.answered), (RoundEnd::Enough, 3));
        assert_eq!(answers, vec![1, 2, 3]);
        assert_eq!(took, ms(225));
    }
    #[tokio::test(start_paused = true)]
    async fn an_unmeasured_peer_opens_the_round_to_the_ceiling() {
        let budget = RoundBudget::derive(ms(100), None, ms(5_000));
        let (round, _, took) = run(&[(4_000, true), (NEVER, true)], budget, 1).await;
        assert_eq!((round.end, took), (RoundEnd::Enough, ms(4_000)));
        let (round, _, took) = run(&[(NEVER, true)], budget, 1).await;
        assert_eq!((round.end, took), (RoundEnd::Expired, ms(5_000)));
    }
    #[tokio::test(start_paused = true)]
    async fn a_round_asks_a_bounded_number_of_peers() {
        let asks = (0..=MAX_ROUND_PEERS as u64).map(|peer| (peer, peer_answer()));
        assert_eq!(
            gather(asks, budget(), |_, _: u64| false).await,
            Err(RoundError::Capacity)
        );
        let asks = (0..MAX_ROUND_PEERS as u64).map(|peer| (peer, peer_answer()));
        let round = gather(asks, budget(), |_, _: u64| false).await.unwrap();
        assert_eq!(
            (round.end, round.answered),
            (RoundEnd::AllReported, MAX_ROUND_PEERS)
        );
    }
    async fn peer_answer() -> Option<u64> {
        Some(1)
    }
}
