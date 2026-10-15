//! What the world refuses, typed: a harness bug must not look like a system bug (`docs/sim.md` §2).

use std::fmt;

/// A refusal of the world.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimError {
    /// A stated bound was reached: `what` holds `bound` already (`docs/sim.md` §7).
    Full {
        /// The resource: `"events"`, `"nodes"`, `"streams"` or `"trace"`.
        what: &'static str,
        /// The bound the run stated.
        bound: usize,
    },
    /// An event was scheduled before the world's present.
    InThePast {
        /// The time asked for, in virtual nanoseconds.
        at: u64,
        /// The world's present.
        now: u64,
    },
    /// The clock was asked to jump past something due: under the ordered discipline nothing is
    /// skipped.
    Skips {
        /// When the earliest pending event or timer is due.
        due: u64,
        /// Where the jump was to go.
        to: u64,
    },
    /// A node the world does not hold.
    UnknownNode(u32),
    /// A stream the world does not hold.
    UnknownStream(u32),
    /// A stream was named twice: two sources drawing from one sequence would make each depend on
    /// the other's draws.
    DuplicateStream(&'static str),
    /// A clock whose rate would stop or reverse it: the rate must exceed −10⁶ ppm.
    Rate(i32),
    /// Virtual or node time past `u64` nanoseconds (584 years), or a wall clock stepped before
    /// its epoch.
    TimeOverflow,
    /// A replayed trace holds, at decision `at`, a value not below the bound the run asks for: the
    /// run is not the one that recorded the trace.
    Diverged {
        /// The decision's position in the trace, in words.
        at: usize,
    },
    /// A replayed trace ended at word `at` while the run still asked for decisions.
    TraceEnded {
        /// Where it ended.
        at: usize,
    },
    /// A measured table that is no distribution: its grid not from zero to a million rising, its
    /// values falling, or the two of different lengths (`net::Measured`).
    NotADistribution,
    /// A loss process in time whose mean burst or mean gap is zero (`net::Loss::bursty_in_time`).
    NotALossProcess,
    /// A strategy picked a candidate that is not there.
    Pick {
        /// What the strategy picked.
        picked: usize,
        /// How many candidates there were.
        candidates: usize,
    },
}

impl fmt::Display for SimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full { what, bound } => write!(f, "the world's {what} are full at {bound}"),
            Self::InThePast { at, now } => {
                write!(
                    f,
                    "an event scheduled at {at} ns, before the present {now} ns"
                )
            }
            Self::Skips { due, to } => {
                write!(f, "a jump to {to} ns skips what is due at {due} ns")
            }
            Self::UnknownNode(node) => write!(f, "no node {node}"),
            Self::UnknownStream(stream) => write!(f, "no stream {stream}"),
            Self::DuplicateStream(label) => write!(f, "the stream {label:?} is named twice"),
            Self::Rate(ppm) => write!(f, "a clock rate of {ppm} ppm stops or reverses the clock"),
            Self::TimeOverflow => write!(f, "time past the range of u64 nanoseconds"),
            Self::Diverged { at } => {
                write!(f, "the run diverged from its trace at word {at}")
            }
            Self::TraceEnded { at } => write!(f, "the trace ended at word {at}"),
            Self::NotADistribution => f.write_str("a measured table that is no distribution"),
            Self::NotALossProcess => {
                f.write_str("a loss process in time with a zero mean burst or gap")
            }
            Self::Pick { picked, candidates } => {
                write!(f, "a strategy picked candidate {picked} of {candidates}")
            }
        }
    }
}

impl std::error::Error for SimError {}
