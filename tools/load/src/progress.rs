//! What a run has done so far, said on stderr as it goes: once at each elapsed time that doubles
//! from the run's start (1 s, 2 s, 4 s, …), so a run of any length says where it stands in a line
//! a doubling, and one that stalls says where, the last line before its bound naming the steps its
//! lifecycles wait at. A run killed at its bound (a comparison's `run.sh`) leaves these lines where
//! its report would have been.

use std::convert::Infallible;
use std::io::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A lifecycle's steps, in the order a `deliver` lifecycle takes them: what one waits at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stage {
    /// The claim's creation.
    Create,
    /// The issuer's post of it.
    Post,
    /// The consumer's wait for the post in its events.
    WaitPosted,
    /// The consumer's acquire of the receipt.
    Acquire,
    /// The consumer's testament.
    Submit,
    /// The consumer's post of its testament.
    PostTestament,
    /// The issuer's wait for the testament in its events.
    WaitAnswered,
    /// The issuer's receipt of the testament.
    Receive,
    /// The issuer's entry of the whole-work validation.
    Enter,
    /// The issuer's release of the claim's scope.
    Release,
    /// A work claim's worker: its work artifact for the claim's slot.
    Artifact,
    /// A work claim's evaluator: its wait for the evaluation due in its events.
    WaitEvaluation,
    /// The evaluator's beginning of the evaluation.
    Begin,
    /// The evaluator's report.
    Report,
    /// A work claim's issuer: its wait for the claim settled in its events.
    WaitSettled,
}

impl Stage {
    const ALL: [Stage; 15] = [
        Stage::Create,
        Stage::Post,
        Stage::WaitPosted,
        Stage::Acquire,
        Stage::Submit,
        Stage::PostTestament,
        Stage::WaitAnswered,
        Stage::Receive,
        Stage::Enter,
        Stage::Release,
        Stage::Artifact,
        Stage::WaitEvaluation,
        Stage::Begin,
        Stage::Report,
        Stage::WaitSettled,
    ];
    fn index(self) -> usize {
        match self {
            Stage::Create => 0,
            Stage::Post => 1,
            Stage::WaitPosted => 2,
            Stage::Acquire => 3,
            Stage::Submit => 4,
            Stage::PostTestament => 5,
            Stage::WaitAnswered => 6,
            Stage::Receive => 7,
            Stage::Enter => 8,
            Stage::Release => 9,
            Stage::Artifact => 10,
            Stage::WaitEvaluation => 11,
            Stage::Begin => 12,
            Stage::Report => 13,
            Stage::WaitSettled => 14,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Stage::Create => "create",
            Stage::Post => "post",
            Stage::WaitPosted => "wait-posted",
            Stage::Acquire => "acquire",
            Stage::Submit => "submit",
            Stage::PostTestament => "post-testament",
            Stage::WaitAnswered => "wait-answered",
            Stage::Receive => "receive",
            Stage::Enter => "enter",
            Stage::Release => "release",
            Stage::Artifact => "artifact",
            Stage::WaitEvaluation => "wait-evaluation",
            Stage::Begin => "begin",
            Stage::Report => "report",
            Stage::WaitSettled => "wait-settled",
        }
    }
}

/// The run's counts, kept as its callers go (one process, one run; the callers share a shard, and
/// a count is a word another thread may read, so each is atomic).
pub(crate) struct Progress {
    agents: AtomicU64,
    agents_opened: AtomicU64,
    started: AtomicU64,
    lifecycles: AtomicU64,
    undelivered: AtomicU64,
    committed: AtomicU64,
    refused: AtomicU64,
    unknown: AtomicU64,
    at: [AtomicU64; 15],
    tail_polls: AtomicU64,
    tail_waits: AtomicU64,
    tail_events: AtomicU64,
    tail_failed: AtomicU64,
}

/// This process's run.
pub(crate) static PROGRESS: Progress = Progress {
    agents: AtomicU64::new(0),
    agents_opened: AtomicU64::new(0),
    started: AtomicU64::new(0),
    lifecycles: AtomicU64::new(0),
    undelivered: AtomicU64::new(0),
    committed: AtomicU64::new(0),
    refused: AtomicU64::new(0),
    unknown: AtomicU64::new(0),
    at: [const { AtomicU64::new(0) }; 15],
    tail_polls: AtomicU64::new(0),
    tail_waits: AtomicU64::new(0),
    tail_events: AtomicU64::new(0),
    tail_failed: AtomicU64::new(0),
};

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

impl Progress {
    /// The run opens `count` agents' connections.
    pub(crate) fn agents(&self, count: u64) {
        self.agents.store(count, Ordering::Relaxed);
    }
    /// One more agent's connection opened.
    pub(crate) fn agent_opened(&self) {
        bump(&self.agents_opened);
    }
    /// A lifecycle began.
    pub(crate) fn started(&self) {
        bump(&self.started);
    }
    /// A lifecycle ended whole.
    pub(crate) fn lifecycle(&self) {
        bump(&self.lifecycles);
    }
    /// A message's other side never showed its step.
    pub(crate) fn undelivered(&self) {
        bump(&self.undelivered);
    }
    /// A write committed.
    pub(crate) fn committed(&self) {
        bump(&self.committed);
    }
    /// A write refused.
    pub(crate) fn refused(&self) {
        bump(&self.refused);
    }
    /// A write whose outcome is unknown.
    pub(crate) fn unknown(&self) {
        bump(&self.unknown);
    }
    /// A tail asked for a page of its events.
    pub(crate) fn tail_poll(&self) {
        bump(&self.tail_polls);
    }
    /// A tail's page was held at the node until a delivery came or its wait ended.
    pub(crate) fn tail_wait(&self) {
        bump(&self.tail_waits);
    }
    /// A tail read `events` events.
    pub(crate) fn tail_events(&self, events: u64) {
        self.tail_events.fetch_add(events, Ordering::Relaxed);
    }
    /// A tail's page was refused or failed.
    pub(crate) fn tail_failed(&self) {
        bump(&self.tail_failed);
    }
    /// A lifecycle is at `stage` while what this returns is held.
    pub(crate) fn at(&self, stage: Stage) -> At<'_> {
        if let Some(count) = self.at.get(stage.index()) {
            bump(count);
        }
        At {
            progress: self,
            stage,
        }
    }

    /// The counts as one line, `elapsed` into the run.
    fn line(&self, elapsed: Duration) -> String {
        let read = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        let mut stages = String::new();
        for stage in Stage::ALL {
            let waiting = self.at.get(stage.index()).map_or(0, read);
            if waiting > 0 {
                if !stages.is_empty() {
                    stages.push_str(", ");
                }
                stages.push_str(&format!("{} {waiting}", stage.name()));
            }
        }
        format!(
            "focal-load +{}s: agents {}/{}; lifecycles {} started, {} whole, {} undelivered; \
             writes {} committed, {} refused, {} unknown; waiting at [{stages}]; \
             tails {} pages ({} held at the node, {} failed), {} events",
            elapsed.as_secs(),
            read(&self.agents_opened),
            read(&self.agents),
            read(&self.started),
            read(&self.lifecycles),
            read(&self.undelivered),
            read(&self.committed),
            read(&self.refused),
            read(&self.unknown),
            read(&self.tail_polls),
            read(&self.tail_waits),
            read(&self.tail_failed),
            read(&self.tail_events),
        )
    }
}

/// A lifecycle held at a stage: let go, it is there no more.
pub(crate) struct At<'a> {
    progress: &'a Progress,
    stage: Stage,
}

impl Drop for At<'_> {
    fn drop(&mut self) {
        if let Some(count) = self.progress.at.get(self.stage.index()) {
            count.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// Says the run's progress on stderr at each elapsed time that doubles from one second after
/// `began`, until it is dropped: raced beside the run's own work (`hyper_rt::combine::race2`), never
/// ending of itself. A shard with no timer leaves the run unreported.
pub(crate) async fn report(began: Instant) -> Infallible {
    let mut next = Duration::from_secs(1);
    loop {
        if focal_rt::time::sleep(next.saturating_sub(began.elapsed()))
            .await
            .is_err()
        {
            return std::future::pending::<Infallible>().await;
        }
        let line = PROGRESS.line(began.elapsed());
        // A closed stderr leaves nothing to say it to; the run goes on.
        let _ = writeln!(std::io::stderr().lock(), "{line}");
        next = next.saturating_mul(2);
    }
}
