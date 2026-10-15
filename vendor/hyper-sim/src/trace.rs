//! The decision trace and the run's digest (`docs/sim.md` §3.1, §3.9).
//!
//! Every choice the world makes is drawn through one [`Chooser`]: in a recorded run from the named
//! streams, each value appended to the trace; in a replay from the trace alone, which needs no
//! seed. A value below 2³² is one word, a wider one two, so the trace is four bytes a decision as
//! `docs/sim.md` §7 bounds it, and a draw with a bound of 0 or 1 is no decision and takes no word.

use crate::error::SimError;
use crate::rng::{GOLDEN_GAMMA, Seeded, mix64, stream_seed};

/// The words of a run's decisions, in the order the run made them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Trace {
    words: Vec<u32>,
}

impl Trace {
    /// A trace of `words`, as a recorded run gave them.
    pub fn from_words(words: Vec<u32>) -> Self {
        Self { words }
    }
    /// The words.
    pub fn words(&self) -> &[u32] {
        &self.words
    }
    /// How many words.
    pub fn len(&self) -> usize {
        self.words.len()
    }
    /// Whether the run made no decision.
    pub fn is_empty(&self) -> bool {
        self.words.is_empty()
    }
    /// The first word at which two traces differ, or the length of the shorter if one is a prefix
    /// of the other; `None` if they are equal.
    pub fn first_difference(&self, other: &Self) -> Option<usize> {
        let at = self
            .words
            .iter()
            .zip(&other.words)
            .position(|(a, b)| a != b);
        match at {
            Some(at) => Some(at),
            None if self.words.len() == other.words.len() => None,
            None => Some(self.words.len().min(other.words.len())),
        }
    }
}

/// A digest of every event the world ran, every observation the harness made and, when the run
/// ends, every decision: [`mix64`] folded over their words. Two runs that agree on all of them agree on it; two that
/// differ collide with probability about 2⁻⁶⁴.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Digest(pub u64);

impl Digest {
    /// The digest of nothing.
    pub const EMPTY: Self = Self(GOLDEN_GAMMA);

    /// `word` folded in.
    pub fn fold(&mut self, word: u64) {
        self.0 = mix64(self.0 ^ word);
    }
}

/// Whether draws come from the streams, recorded, or from a trace.
#[derive(Clone, Debug)]
enum Mode {
    Record,
    Replay { at: usize },
}

/// Where a world's decisions come from, and where they go: its streams, its trace and its digest.
#[derive(Clone, Debug)]
pub(crate) struct Chooser {
    seed: u64,
    streams: Vec<Seeded>,
    /// Each stream's first state, by which a name given twice is found.
    firsts: Vec<u64>,
    max_streams: usize,
    trace: Trace,
    max_words: usize,
    mode: Mode,
    pub(crate) digest: Digest,
}

/// A length as a word: exact on every target this repository builds for.
fn word_of(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

/// The largest bound whose draws fit one word.
const ONE_WORD: u64 = 1 << 32;

impl Chooser {
    /// Draws from the streams of `seed`, recorded in at most `max_words` words. The trace's room
    /// is taken now, once: a run's decisions then never allocate, and a bound the host cannot
    /// hold is refused before the run starts rather than partway.
    pub(crate) fn recording(
        seed: u64,
        max_streams: usize,
        max_words: usize,
    ) -> Result<Self, SimError> {
        let mut words = Vec::new();
        words
            .try_reserve_exact(max_words)
            .map_err(|_| SimError::Full {
                what: "trace",
                bound: max_words,
            })?;
        Ok(Self {
            seed,
            streams: Vec::new(),
            firsts: Vec::new(),
            max_streams,
            trace: Trace { words },
            max_words,
            mode: Mode::Record,
            digest: Digest::EMPTY,
        })
    }

    /// Draws from `trace`.
    pub(crate) fn replaying(trace: Trace, max_streams: usize) -> Self {
        let max_words = trace.len();
        Self {
            seed: 0,
            streams: Vec::new(),
            firsts: Vec::new(),
            max_streams,
            trace,
            max_words,
            mode: Mode::Replay { at: 0 },
            digest: Digest::EMPTY,
        }
    }

    /// A new stream, named by `label` and `parts`.
    pub(crate) fn stream(&mut self, label: &'static str, parts: &[u64]) -> Result<u32, SimError> {
        let first = stream_seed(self.seed, label, parts);
        if self.firsts.contains(&first) {
            return Err(SimError::DuplicateStream(label));
        }
        if self.streams.len() >= self.max_streams {
            return Err(SimError::Full {
                what: "streams",
                bound: self.max_streams,
            });
        }
        let id = u32::try_from(self.streams.len()).map_err(|_| SimError::Full {
            what: "streams",
            bound: self.max_streams,
        })?;
        self.streams.push(Seeded::new(first));
        self.firsts.push(first);
        Ok(id)
    }

    /// The words of trace recorded, or replayed, so far.
    pub(crate) fn words(&self) -> usize {
        match self.mode {
            Mode::Record => self.trace.words.len(),
            Mode::Replay { at } => at,
        }
    }

    /// A value uniform in `[0, bound)` from `stream`, or the trace's next.
    pub(crate) fn below(&mut self, stream: u32, bound: u64) -> Result<u64, SimError> {
        let index = usize::try_from(stream).map_err(|_| SimError::UnknownStream(stream))?;
        let generator = self
            .streams
            .get_mut(index)
            .ok_or(SimError::UnknownStream(stream))?;
        let value = match &mut self.mode {
            Mode::Record => {
                let value = generator.below(bound);
                if bound > 1 {
                    record(&mut self.trace.words, self.max_words, bound, value)?;
                }
                value
            }
            Mode::Replay { at } => {
                if bound <= 1 {
                    return Ok(0);
                }
                let start = *at;
                let value = replay(&self.trace.words, at, bound)?;
                if value >= bound {
                    return Err(SimError::Diverged { at: start });
                }
                value
            }
        };
        Ok(value)
    }

    /// The trace recorded, or as much of the one replayed as the run used, and the digest with
    /// every word of it folded in. The decisions are folded here, once, rather than as they are
    /// made: a fold is a third of a decision's cost, and the trace holds every one in order.
    pub(crate) fn finish(self) -> (Trace, Digest) {
        let used = match self.mode {
            Mode::Record => self.trace.words.len(),
            Mode::Replay { at } => at,
        };
        let mut digest = self.digest;
        let words = self.trace.words.get(..used).unwrap_or(&[]);
        digest.fold(word_of(words.len()));
        for word in words {
            digest.fold(u64::from(*word));
        }
        (self.trace, digest)
    }
}

/// `value` appended to `words`: one word below [`ONE_WORD`], else its high and low halves.
fn record(words: &mut Vec<u32>, max: usize, bound: u64, value: u64) -> Result<(), SimError> {
    let need = if bound <= ONE_WORD { 1 } else { 2 };
    if words.len().saturating_add(need) > max {
        return Err(SimError::Full {
            what: "trace",
            bound: max,
        });
    }
    let low = u32::try_from(value & u64::from(u32::MAX)).map_err(|_| SimError::TimeOverflow)?;
    if need == 2 {
        let high = u32::try_from(value >> 32).map_err(|_| SimError::TimeOverflow)?;
        words.push(high);
    }
    words.push(low);
    Ok(())
}

/// The value at `at` in `words`, advancing `at`, read as [`record`] wrote it for `bound`.
fn replay(words: &[u32], at: &mut usize, bound: u64) -> Result<u64, SimError> {
    let mut next = || -> Result<u64, SimError> {
        let word = words.get(*at).ok_or(SimError::TraceEnded { at: *at })?;
        *at = at.saturating_add(1);
        Ok(u64::from(*word))
    };
    if bound <= ONE_WORD {
        next()
    } else {
        let high = next()?;
        let low = next()?;
        Ok((high << 32) | low)
    }
}
