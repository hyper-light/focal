//! Determinism, enforced (`docs/sim.md` §3.9): a simulation's first seed runs twice from its seed
//! and once from the trace of the first run, and all three must give the same digest — the
//! generalization of mantle's `a_seed_runs_the_same_every_time`, made one call.

use std::fmt;

use crate::trace::Digest;
use crate::world::{Record, Source};

/// Why a run is not deterministic, or failed.
#[derive(Debug)]
pub enum Twice<X> {
    /// A run failed on its own terms.
    Run(X),
    /// The second run from the seed differs from the first: something outside the world (a host
    /// clock, entropy, an unordered iteration, state kept between runs) entered a decision.
    Seed {
        /// The first run's digest.
        first: Digest,
        /// The second's.
        second: Digest,
        /// The first word at which their traces differ, if they do.
        trace_word: Option<usize>,
    },
    /// The replay of the first run's trace differs from it: a draw was made outside the world.
    Replay {
        /// The first run's digest.
        first: Digest,
        /// The replay's.
        replay: Digest,
    },
}

impl<X: fmt::Display> fmt::Display for Twice<X> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Run(error) => write!(f, "the run failed: {error}"),
            Self::Seed {
                first,
                second,
                trace_word,
            } => write!(
                f,
                "one seed ran twice to digests {:#018x} and {:#018x} (traces differ at word {trace_word:?})",
                first.0, second.0
            ),
            Self::Replay { first, replay } => write!(
                f,
                "the trace replayed to {:#018x}, the run to {:#018x}",
                replay.0, first.0
            ),
        }
    }
}

impl<X: fmt::Debug + fmt::Display> std::error::Error for Twice<X> {}

/// `run` from `seed` twice and from the first run's trace once; the first run's record if all
/// three agree. `run` builds its world from the [`Source`] it is given.
pub fn twice<X, F>(seed: u64, mut run: F) -> Result<Record, Twice<X>>
where
    F: FnMut(Source) -> Result<Record, X>,
{
    let first = run(Source::Seed(seed)).map_err(Twice::Run)?;
    let second = run(Source::Seed(seed)).map_err(Twice::Run)?;
    if first.digest != second.digest || first.trace != second.trace {
        return Err(Twice::Seed {
            first: first.digest,
            second: second.digest,
            trace_word: first.trace.first_difference(&second.trace),
        });
    }
    let replay = run(Source::Trace(first.trace.clone())).map_err(Twice::Run)?;
    if replay.digest != first.digest {
        return Err(Twice::Replay {
            first: first.digest,
            replay: replay.digest,
        });
    }
    Ok(first)
}
