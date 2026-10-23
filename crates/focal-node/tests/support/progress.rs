//! A wait on real `focal` processes, charged to the periods their root
//! owners run and not to the wall clock
//! ([27](../../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md) §3.1 P8).
//!
//! The allowance is what the wait takes at most on an idle machine; under
//! load the processes run fewer periods a second and the wait stretches with
//! them. A process that does not answer (stopped, paused, killed, not yet
//! started) has no progress to charge and is left out while another
//! answers; while none answers the allowance is a wall-clock one.
//!
//! A process just started is waited for the same way until it reports
//! ([`reported`]), once its executable has run: an operating system may hold
//! an executable's first instruction while it assesses the file. On macOS a
//! launch of focal's 209 MB debug binary waited 3.5 to 6.8 s with no CPU of
//! its own while syspolicyd and XprotectService ran 1.8 s and 1.1 s of
//! theirs, again whenever the binary had gone unrun for minutes, and past the
//! 25 s a start was given under twelve busy loops, which starve those
//! daemons: a start's wait counted the OS's assessment as the node's.
//! [`admit`] runs the executable to its exit before a start is timed, a wait
//! on the fact itself.
#![allow(dead_code)]
use std::{
    path::Path,
    process::{Child, Command, ExitStatus},
    sync::mpsc::{Receiver, RecvTimeoutError},
    time::Duration,
};

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
                    Some(last) if now >= last => now.saturating_sub(last),
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

/// Runs the `focal` executable once to its exit, so that whatever the OS takes to admit it is
/// spent before the start that follows is timed.
pub fn admit() {
    let _ = Command::new(env!("CARGO_BIN_EXE_focal"))
        .arg("--version")
        .output();
}

/// The periods the root owner of the process on `root` has run, from its metrics; none while it
/// does not answer.
pub fn root_periods(root: &Path) -> Option<u64> {
    let output = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str()?, "inspect", "node", "--metrics"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    periods_in(&String::from_utf8_lossy(&output.stdout))
}

/// Why a started process gave no report.
#[derive(Debug)]
pub enum NotReported {
    /// It exited first.
    Exited(ExitStatus),
    /// Its output ended first, while it ran.
    Ended,
    /// Its wait was spent.
    Spent(focal_timing::Spent),
}
impl std::fmt::Display for NotReported {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exited(status) => write!(formatter, "it exited first ({status})"),
            Self::Ended => formatter.write_str("its output ended first, while it ran"),
            Self::Spent(spent) => write!(formatter, "{spent}"),
        }
    }
}

/// The first report `receive` gives of `child`, the process started on `root`, waited for as
/// every wait on `focal` processes is: charged to the periods its root owner runs, `allowance`'s
/// worth, and while it does not answer yet the frozen window of the wall clock; over at once if it
/// exits.
pub fn reported<T>(
    receive: &Receiver<T>,
    child: &mut Child,
    root: &Path,
    allowance: Duration,
) -> Result<T, NotReported> {
    let mut wait = Progress::begin(vec![Box::new(move || root_periods(root))], allowance);
    loop {
        match receive.recv_timeout(PERIOD) {
            Ok(report) => return Ok(report),
            Err(RecvTimeoutError::Disconnected) => {
                return Err(match child.try_wait() {
                    Ok(Some(status)) => NotReported::Exited(status),
                    _ => NotReported::Ended,
                });
            }
            Err(RecvTimeoutError::Timeout) => {}
        }
        if let Ok(Some(status)) = child.try_wait() {
            // A report it gave as it exited is still its report.
            return receive.try_recv().map_err(|_| NotReported::Exited(status));
        }
        if let Some(spent) = wait.spent() {
            return Err(NotReported::Spent(spent));
        }
    }
}
