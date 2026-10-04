//! Waits on in-process replica owners, charged to the periods they run
//! and not to the wall clock
//! ([27](../../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md) §3.1 P8).
//!
//! The allowance is what the wait takes at most on an idle machine, counted
//! in the owners' configured tick; under load the owners run fewer periods a
//! second and the wait stretches with them. An owner that runs no period at
//! all for [`FROZEN`] is wedged, and the wait is over.
#![allow(dead_code)]
use focal_node::fleet::ReplicaHost;
use std::{future::Future, time::Duration};

/// How long the slowest owner may run no period before it is called wedged.
pub const FROZEN: Duration = Duration::from_secs(60);

/// The periods each of `hosts`' owners has run. One stopped runs no more,
/// and while another runs it is charged none: the wait is charged to the
/// others' periods as they run.
pub fn periods(hosts: &[&ReplicaHost]) -> Vec<u64> {
    let running = hosts.iter().any(|host| !host.progress().stopped);
    hosts
        .iter()
        .map(|host| {
            if running && host.progress().stopped {
                u64::MAX
            } else {
                host.periods()
            }
        })
        .collect()
}

/// `what`'s output, or why the wait is over: the owners of `hosts` running
/// when it began ran the periods `allowance` holds at `tick` without it, or
/// ran none for [`FROZEN`].
pub async fn within<F: Future>(
    hosts: &[&ReplicaHost],
    allowance: Duration,
    tick: Duration,
    what: F,
) -> Result<F::Output, focal_timing::Spent> {
    let live: Vec<&ReplicaHost> = hosts
        .iter()
        .copied()
        .filter(|host| !host.progress().stopped)
        .collect();
    charged(|| periods(&live), allowance, tick, what).await
}

/// `what`'s output, or why the wait is over: the owners whose period counts
/// `counters` reads ran the periods `allowance` holds at `tick` without it,
/// or ran none for [`FROZEN`].
pub async fn charged<F: Future>(
    counters: impl Fn() -> Vec<u64>,
    allowance: Duration,
    tick: Duration,
    what: F,
) -> Result<F::Output, focal_timing::Spent> {
    let budget = focal_timing::ProgressDeadline::periods(allowance, tick);
    counted(counters, budget, tick, what).await
}

/// `what`'s output, or why the wait is over: the counts `counters` reads,
/// every `poll`, advanced by `budget` without it, or not at all for
/// [`FROZEN`].
pub async fn counted<F: Future>(
    counters: impl Fn() -> Vec<u64>,
    budget: u64,
    poll: Duration,
    what: F,
) -> Result<F::Output, focal_timing::Spent> {
    let mut wait = focal_timing::ProgressDeadline::begin(&counters(), budget, FROZEN);
    let mut what = std::pin::pin!(what);
    loop {
        tokio::select! {
            output = &mut what => return Ok(output),
            () = tokio::time::sleep(poll) => wait.check(&counters())?,
        }
    }
}
