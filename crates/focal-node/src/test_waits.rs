//! Waits in the crate's tests on in-process owners, charged to the periods
//! they run and not to the wall clock (27 §3.1 P8), as
//! `tests/support/owners.rs` charges the integration tests'. The allowance
//! is what the wait takes at most on an idle machine, counted in the owners'
//! configured tick; under load the owners run fewer periods a second and the
//! wait stretches with them. An owner that runs no period for [`FROZEN`] is
//! wedged, and the wait is over; where no owner is observed, that window is
//! the wait's only bound.
use crate::fleet::ReplicaHost;
use std::{future::Future, time::Duration};

/// How long the slowest owner may run no period before it is called wedged.
pub(crate) const FROZEN: Duration = Duration::from_secs(60);
/// The root and partition control owners' configured period
/// (`ControlHostConfig`'s default).
pub(crate) const CONTROL_TICK: Duration = Duration::from_millis(100);
/// An owner answers what reaches it in the period it arrives in or the
/// next: a turn takes what is queued up to the clients the owner admits at
/// once (`ReplicaConfig::pending_clients`), more than these tests send. The
/// periods one step of a wait on it takes at most.
pub(crate) const STEP: u32 = 2;

/// The periods each of `hosts`' owners has run. One stopped runs no more,
/// and while another runs it is charged none: the wait is charged to the
/// others' periods as they run.
pub(crate) fn periods(hosts: &[&ReplicaHost]) -> Vec<u64> {
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
pub(crate) async fn within<F: Future>(
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
pub(crate) async fn charged<F: Future>(
    counters: impl Fn() -> Vec<u64>,
    allowance: Duration,
    tick: Duration,
    what: F,
) -> Result<F::Output, focal_timing::Spent> {
    let mut wait = focal_timing::ProgressDeadline::begin(
        &counters(),
        focal_timing::ProgressDeadline::periods(allowance, tick),
        FROZEN,
    );
    let mut what = std::pin::pin!(what);
    loop {
        tokio::select! {
            output = &mut what => return Ok(output),
            () = tokio::time::sleep(tick) => wait.check(&counters())?,
        }
    }
}
