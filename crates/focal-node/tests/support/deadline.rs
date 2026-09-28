//! A deadline for a test of real `focal` processes that start by data
//! directory, charged to the periods those processes run and not to the wall
//! clock ([27](../../../../docs/archictecutre/27-consensus-roadmap-and-slates-port.md) §3.1 P8).
//!
//! A test names the data directories it starts ([`observe`]); a
//! [`Deadline`] then lasts as many periods as its allowance holds on an idle
//! machine, however long the machine takes to run them. While no observed
//! process answers, the allowance is a wall-clock one.
#![allow(dead_code)]
use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

#[path = "progress.rs"]
pub mod progress;

/// The most data directories one test observes.
const MAX_OBSERVED: usize = 64;

thread_local! {
    /// The data directories this test started processes on.
    static OBSERVED: RefCell<Vec<PathBuf>> = const { RefCell::new(Vec::new()) };
}
/// A process is started on `root`: deadlines begun on this thread from now
/// on are charged to it.
pub fn observe(root: &Path) {
    OBSERVED.with(|observed| {
        let mut observed = observed.borrow_mut();
        if observed.len() < MAX_OBSERVED && !observed.iter().any(|known| known == root) {
            observed.push(root.to_path_buf());
        }
    });
}
/// The periods the root owner of the process on `root` has run; none while
/// it does not answer.
pub fn periods(root: &Path) -> Option<u64> {
    let output = Command::new(env!("CARGO_BIN_EXE_focal"))
        .args(["--data-dir", root.to_str()?, "cluster", "node", "metrics"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    progress::periods_in(&String::from_utf8_lossy(&output.stdout))
}
pub struct Deadline {
    wait: progress::Progress<'static>,
    allowance: Duration,
}
impl Deadline {
    /// `allowance` is what the wait takes at most on an idle machine.
    pub fn after(allowance: Duration) -> Self {
        let roots = OBSERVED.with(|observed| observed.borrow().clone());
        Self {
            wait: progress::Progress::begin(
                roots
                    .into_iter()
                    .map(|root| Box::new(move || periods(&root)) as progress::Counter<'static>)
                    .collect(),
                allowance,
            ),
            allowance,
        }
    }
    /// Whether the wait may go on.
    pub fn open(&mut self) -> bool {
        self.wait.open()
    }
    pub fn allowance(&self) -> Duration {
        self.allowance
    }
}
