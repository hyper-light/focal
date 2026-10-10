//! The bounds every loom model in the workspace explores under (Part 6, "Concurrency": loom on the
//! ring and handle cores; AC-0.7; T-0.3). One definition, so the memory crate's ring and handle
//! models and the runtime's parking model explore the same space in CI and on a laptop, and a
//! run's numbers (`docs/wip/concurrency.md`) mean the same thing everywhere.
//!
//! loom explores thread interleavings exhaustively up to a bound on preemptions (involuntary
//! thread switches) per execution, after Musuvathi and Qadeer's iterative context bounding
//! [A: "Iterative context bounding for systematic testing of multithreaded programs", PLDI 2007]:
//! every bug in their corpus surfaced within two preemptions, and loom's documentation recommends
//! two or three. Two is also what the parking protocol's store-buffering race needs (each side
//! preempted once, between its write and its read), so it is the smallest bound that can still
//! find a lost wake. An execution that runs past the branch cap is a livelock (a spin loop whose
//! condition never comes); loom turns it into a failure instead of a hang.
//!
//! Measured (2026-09-13, `RUSTFLAGS="--cfg loom" cargo test -p slates-mem --release --lib loom
//! -- --nocapture`, Apple M5 Max, load average 6–7 with two other builds running): under the
//! bound, the one-producer ring explores 157 interleavings, the two contending producers 3,865
//! and the lapping producer 26, in 0.14 s together. With the bound lifted
//! (`LOOM_MAX_PREEMPTIONS=255`, loom's widest, which no execution here reaches, so it is the
//! exhaustive run) the one-producer ring explores 6,096 — the same count the unbounded run at
//! c80b6f9 gave — and the lapping producer 192; the contending producers did not finish the
//! exhaustive run inside a 600 s box, which is what the bound is for. Every model's numbers are
//! recorded in `docs/wip/concurrency.md` with the command that produced them.
//!
//! loom's own environment variables stay loom's debugging interface: `LOOM_MAX_PREEMPTIONS` and
//! `LOOM_MAX_BRANCHES` override the constants here for an investigation (a wider run, or the
//! exhaustive one), and `LOOM_CHECKPOINT_FILE` and `LOOM_LOCATION` isolate a failing iteration
//! as loom documents. Nothing narrows what CI runs: the lane sets no variable and gets the
//! constants.

use std::sync::atomic::{AtomicU64, Ordering};

/// Derived: the preemption bound, from iterative context bounding (two preemptions surface every
/// bug in the PLDI'07 corpus; loom recommends two or three) and the parking protocol's race
/// (one preemption per side between its write and its read); the smallest bound that finds both.
pub const PREEMPTION_BOUND: usize = 2;

/// Derived: twice the longest honest execution across the workspace's models, so no legitimate
/// schedule trips the cap and a livelock is cut within twice the longest honest run. Measured
/// 2026-09-13 by bisecting `LOOM_MAX_BRANCHES` (`docs/wip/concurrency.md`): the widest model,
/// the two contending producers, passes at 112 and fails at 108; the others under 90.
pub const BRANCH_CAP: usize = 2 * 112;

/// A model builder carrying the bounds; every loom model checks through it. The constants apply
/// where the environment says nothing, so a documented loom variable still steers an
/// investigation.
pub fn builder() -> loom::model::Builder {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound.get_or_insert(PREEMPTION_BOUND);
    if std::env::var_os("LOOM_MAX_BRANCHES").is_none() {
        builder.max_branches = BRANCH_CAP;
    }
    builder
}

/// Checks `model` under the bounds and returns how many executions loom explored, printing the
/// count under the model's `name` so a run's record does not depend on loom's own logging. The
/// counter is a leaked word the model closure bumps once per execution (loom calls the closure
/// once per explored interleaving). A failing model reports the count reached before the
/// failing interleaving, then fails as loom reported it.
///
/// # Panics
///
/// When the model explored no interleaving at all (a vacuous model is a failed model), and, as
/// loom itself does, when an interleaving violates the model's assertions, deadlocks, or runs
/// past the branch cap.
pub fn explore(name: &str, model: impl Fn() + Send + Sync + 'static) -> u64 {
    let explored: &'static AtomicU64 = Box::leak(Box::new(AtomicU64::new(0)));
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        builder().check(move || {
            explored.fetch_add(1, Ordering::Relaxed);
            model();
        });
    }));
    let count = explored.load(Ordering::Relaxed);
    if let Err(failure) = outcome {
        eprintln!(
            "loom: {name}: failed at interleaving {count} (preemption bound {PREEMPTION_BOUND})"
        );
        std::panic::resume_unwind(failure);
    }
    eprintln!("loom: {name}: explored {count} interleavings (preemption bound {PREEMPTION_BOUND})");
    assert!(count > 0, "loom: {name}: explored no interleaving");
    count
}
