//! A wait on real `focal` processes, charged to the periods their root
//! owners run and not to the wall clock
//! ([27](../../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md) §3.1 P8).
//!
//! The allowance is what the wait takes at most on an idle machine; under
//! load the processes run fewer periods a second and the wait stretches with
//! them. A process that does not answer (stopped, paused, killed, not yet
//! started) has no progress to charge and is left out while another
//! answers; while none answers the allowance is a wall-clock one.
#![allow(dead_code)]
use std::time::Duration;

/// The owners' configured period: what an allowance in seconds is counted
/// in (the `ControlHostConfig` default).
pub const PERIOD: Duration = Duration::from_millis(100);
/// How long the slowest process may run no period before it is called
/// wedged, at least.
const FROZEN: Duration = Duration::from_secs(60);

/// The periods in a node's metrics text.
pub fn periods_in(metrics: &str) -> Option<u64> {
    metrics
        .lines()
        .find_map(|line| line.strip_prefix("focal_root_periods_total"))
        .and_then(|rest| rest.rsplit(' ').next()?.trim().parse().ok())
}

pub type Counter<'a> = Box<dyn Fn() -> Option<u64> + 'a>;

struct Observed<'a> {
    counter: Counter<'a>,
    /// The count last read, and the periods charged before it: a process
    /// that restarts counts from zero again.
    last: Option<u64>,
    charged: u64,
}
pub struct Progress<'a> {
    observed: Vec<Observed<'a>>,
    wait: focal_timing::ProgressDeadline,
}
impl<'a> Progress<'a> {
    pub fn begin(counters: Vec<Counter<'a>>, allowance: Duration) -> Self {
        let observed: Vec<_> = counters
            .into_iter()
            .map(|counter| {
                let last = counter();
                Observed {
                    counter,
                    last,
                    charged: 0,
                }
            })
            .collect();
        let wait = focal_timing::ProgressDeadline::begin(
            &vec![0; observed.len()],
            focal_timing::ProgressDeadline::periods(allowance, PERIOD),
            allowance.max(FROZEN),
        );
        Self { observed, wait }
    }
    /// Why the wait is over, once it is.
    pub fn spent(&mut self) -> Option<focal_timing::Spent> {
        let mut answered = false;
        let mut charges = Vec::with_capacity(self.observed.len());
        for observed in &mut self.observed {
            let now = (observed.counter)();
            if let Some(now) = now {
                answered = true;
                let advance = match observed.last {
                    Some(last) if now >= last => now - last,
                    // Restarted, or first seen: what it has run since.
                    Some(_) => now,
                    None => 0,
                };
                observed.charged = observed.charged.saturating_add(advance);
                observed.last = Some(now);
            }
            charges.push((now.is_some(), observed.charged));
        }
        let charges: Vec<u64> = charges
            .into_iter()
            .map(|(answers, charged)| {
                if answers || !answered {
                    charged
                } else {
                    u64::MAX
                }
            })
            .collect();
        self.wait.check(&charges).err()
    }
    /// Whether the wait may go on.
    pub fn open(&mut self) -> bool {
        self.spent().is_none()
    }
}
