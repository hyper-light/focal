//! What the live work reserves of its checkpoint's bound so that it can always finish (rule 2,
//! its liveness): every claim not yet released keeps room for the rows and bytes its release
//! adds, and every open claim for those its terminal transition adds as well, whatever ends it.
//!
//! Finishing a claim grows the root before anything shrinks it: its terminal transition adds an
//! outcome, its events and the cut it ends at, a monitor still active is cancelled before the
//! release with an outcome and an event of its own, the release adds another outcome and event,
//! and only a released family retires. A root at its
//! bound with a fixed reserve for finishing work wedged: cancels took the reserve before any
//! release came, every release was then refused, no family could retire, and the ledger could
//! finish nothing more. Every claim's finishing is now reserved where the claim is admitted, as
//! a transaction reserves its log space before it changes anything (XFS, jbd2) and as doc 22's
//! completion promises reserve a report's rows before it arrives: the root with its reservations
//! never passes the bound, a transition that ends a claim or releases one takes no more than it
//! gives back, and so one is never refused by it.
use super::*;

/// Rows and encoded bytes of a checkpoint.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Finishing {
    pub rows: usize,
    pub bytes: usize,
}

impl Finishing {
    pub(super) const NONE: Self = Self { rows: 0, bytes: 0 };
    pub(super) fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            rows: self.rows.checked_add(other.rows)?,
            bytes: self.bytes.checked_add(other.bytes)?,
        })
    }
    pub(super) fn checked_sub(self, other: Self) -> Option<Self> {
        Some(Self {
            rows: self.rows.checked_sub(other.rows)?,
            bytes: self.bytes.checked_sub(other.bytes)?,
        })
    }
    pub(super) fn saturating_add(self, other: Self) -> Self {
        Self {
            rows: self.rows.saturating_add(other.rows),
            bytes: self.bytes.saturating_add(other.bytes),
        }
    }
    pub(super) fn saturating_sub(self, other: Self) -> Self {
        Self {
            rows: self.rows.saturating_sub(other.rows),
            bytes: self.bytes.saturating_sub(other.bytes),
        }
    }
    fn checked_mul(self, times: usize) -> Option<Self> {
        Some(Self {
            rows: self.rows.checked_mul(times)?,
            bytes: self.bytes.checked_mul(times)?,
        })
    }
}

/// What a claim's terminal transition adds: 3 rows (its event, its status event and the outcome of
/// the invocation that ends it) and the cut its row ends at. For a claim on nothing and with no
/// monitor, ended by its issuer's cancel or its deadline: the event 258 bytes at most (a
/// deadline's; a cancel's is 254), the status event 160, the outcome 209 (a request's or a claim
/// deadline's) and the cut 57. Its status index row is replaced, the same size. What a terminal
/// transition deletes (a timer it consumes) is not counted on: a reservation never relies on a
/// row being there to take. Measured on every path by `finishing_growth_tests`; a report that
/// ends a claim adds its own artifacts beside this, as work on what was taken.
const TERMINAL: Finishing = Finishing {
    rows: 3,
    bytes: 57 + 258 + 160 + 209,
};
/// What a claim with dependencies may add beyond [`TERMINAL`]: a dependency's failure ends it at a
/// graph cut, 164 bytes against a cancel's 57.
const GRAPH_CUT: usize = 164 - 57;
/// What a claim with a monitor active may add beyond [`TERMINAL`]: the monitor's deadline ends it
/// with an event of 274 bytes (against 258), a status event of 176 (against 160) and the outcome
/// of a timer's invocation, 241 (against 209).
const MONITOR_DEADLINE: usize = (274 - 258) + (176 - 160) + (241 - 209);
/// What a claim's release adds: 2 rows, its event (254 bytes) and its outcome (209, a request's),
/// and its row grows by the cut it is released at (40).
const RELEASE: Finishing = Finishing {
    rows: 2,
    bytes: 40 + 254 + 209,
};
/// What cancelling a monitor adds, as a claim with a monitor active is released only once the
/// monitor is cancelled or released: 2 rows, its event (319 bytes) and its outcome (209), and the
/// claim's row grows by the monitor's disposition (49). Its index rows shrink and its timer goes;
/// neither is counted on.
const MONITOR: Finishing = Finishing {
    rows: 2,
    bytes: 49 + 319 + 209,
};
/// What fencing a live evaluation adds as its claim ends: 2 rows, its two events, and its row
/// grows by the fence. Begun, the events carry its attempt: 429 bytes each and 65. One that never
/// began: 328 each and 97. Its timer, where it has one, goes; that is not counted on.
const FENCE_BEGUN: Finishing = Finishing {
    rows: 2,
    bytes: 65 + 429 + 429,
};
const FENCE_WAITING: Finishing = Finishing {
    rows: 2,
    bytes: 97 + 328 + 328,
};

/// What the row `row` at `key` reserves for its finishing, by the shape it has: a claim not yet
/// released its release and a monitor's cancel for each monitor it has active, and an open claim
/// its terminal transition as well, by what can end it (a dependency's failure where it has
/// dependencies, a monitor's deadline where it has one active); a live evaluation its fence, as
/// begun or not. A spilled claim waits to be delivered, on nothing and in nothing
/// (`OwnedClaim::spillable`), so it reserves what an open claim of no dependency and no monitor
/// does, unread.
pub(super) fn of(key: &Key, row: &Row) -> Result<Finishing, NativeError> {
    let capacity = || NativeError::Capacity("finishing reservation");
    match (key, row) {
        (Key::Claim(_), Row::Claim(claim)) => match claim.claim() {
            None => TERMINAL.checked_add(RELEASE).ok_or_else(capacity),
            Some(state) if state.released() => Ok(Finishing::NONE),
            Some(state) => {
                let monitors = state.scopes().iter().filter(|scope| scope.active()).count();
                let release = MONITOR
                    .checked_mul(monitors)
                    .and_then(|monitors| monitors.checked_add(RELEASE))
                    .ok_or_else(capacity)?;
                if state.is_terminal() {
                    return Ok(release);
                }
                let graph = if state.graph().obligations().is_empty() {
                    0
                } else {
                    GRAPH_CUT
                };
                let deadline = if monitors == 0 { 0 } else { MONITOR_DEADLINE };
                let terminal = Finishing {
                    rows: TERMINAL.rows,
                    bytes: TERMINAL
                        .bytes
                        .checked_add(graph)
                        .and_then(|bytes| bytes.checked_add(deadline))
                        .ok_or_else(capacity)?,
                };
                release.checked_add(terminal).ok_or_else(capacity)
            }
        },
        (Key::Evaluation(_), Row::Evaluation(evaluation)) => Ok(match evaluation.get() {
            Some(state) if !state.state().is_terminal() && state.fence().is_none() => {
                if state.has_begun() {
                    FENCE_BEGUN
                } else {
                    FENCE_WAITING
                }
            }
            _ => Finishing::NONE,
        }),
        _ => Ok(Finishing::NONE),
    }
}

/// What the rows `changes` put reserve for their finishing.
pub(super) fn added(
    mut changes: impl Iterator<Item = impl ranges::ChangeRead>,
) -> Result<Finishing, NativeError> {
    changes.try_fold(Finishing::NONE, |sum, change| match change.row() {
        Some(row) => sum
            .checked_add(of(&change.changed(), row)?)
            .ok_or(NativeError::Capacity("finishing reservation")),
        None => Ok(sum),
    })
}

/// What `rows` reserve for their finishing, summed row by row: for a root built whole (a
/// checkpoint restored, an archive read), whose rows were never published one at a time.
pub(super) fn of_rows(rows: &ranges::NativeRanges) -> Result<Finishing, NativeError> {
    rows.entries().try_fold(Finishing::NONE, |sum, entry| {
        sum.checked_add(of(&entry.key, entry.value)?)
            .ok_or(NativeError::Capacity("finishing reservation"))
    })
}
