//! The decision tape: every word a run's schedule drew, grouped by the step that drew it, so a run
//! is replayed, mutated and shrunk by its tape (`docs/sim.md` §3.1, "The decision trace"; §4.5,
//! coverage-guided mutation and shrinking). A harness plays one through a [`Player`] of its own, or
//! through the world (`Source::Tape`), whose every decision then reads the tape (`docs/sim.md` §18).
//!
//! A harness draws through a [`Player`]: each word comes from the tape being played while its step
//! has words left, and from a filler stream (SplitMix64 from a stated seed) once it has none; every
//! word the run took is recorded, step by step, into the tape it gives back ([`Player::finish`]).
//! A step's words are read as the harness reads any draw, reduced to the bound it asks for, so
//! **any tape is a feasible schedule**: a word moved, dropped or redrawn is read against whatever
//! the state then offers. That is what Gulcan et al. (§3.2, "feasible mutation") get by naming
//! buffers rather than messages; here it holds of every decision, not only deliveries. The tape a
//! run gives back replays it exactly: each step reads the words it took, no more, no fewer.
//!
//! Each step carries a kind the harness names (a delivery, a crash, any other operation), which
//! the mutations of [`Tape::mutate`] respect, and its length is bounded ([`Bounds`]): a tape that
//! would pass its bound is refused ([`TapeError::Full`]).

use crate::rng::Seeded;

/// What a tape may hold, the run's stated bound (`docs/sim.md` §7, "Trace").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bounds {
    /// Steps.
    pub steps: usize,
    /// Words over all steps.
    pub words: usize,
}

/// Why a tape took no more.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TapeError {
    /// A bound was reached.
    Full {
        /// What was bounded.
        what: &'static str,
        /// The bound.
        bound: usize,
    },
    /// The host refused an allocation within the bound.
    Memory,
}

/// A run's decisions: words, and per step how many it drew and its kind.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tape {
    words: Vec<u64>,
    steps: Vec<(u32, u8)>,
}

impl Tape {
    /// Steps recorded.
    pub fn len(&self) -> usize {
        self.steps.len()
    }

    /// Whether no step is recorded.
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// Words recorded.
    pub fn words(&self) -> usize {
        self.words.len()
    }

    /// The kind of each step, in order.
    pub fn kinds(&self) -> impl Iterator<Item = u8> + '_ {
        self.steps.iter().map(|(_, kind)| *kind)
    }

    /// Step `step`'s words.
    fn segment(&self, step: usize) -> Option<&[u64]> {
        let start = self
            .steps
            .get(..step)?
            .iter()
            .map(|(count, _)| *count as usize)
            .fold(0usize, usize::saturating_add);
        let (count, _) = *self.steps.get(step)?;
        self.words.get(start..start.saturating_add(count as usize))
    }

    /// The tape with steps chosen by `keep` (each step's words and kind kept whole).
    pub fn filtered(&self, mut keep: impl FnMut(usize) -> bool) -> Self {
        let mut out = Self::default();
        let mut at = 0usize;
        for (step, (count, kind)) in self.steps.iter().enumerate() {
            let end = at.saturating_add(*count as usize);
            if keep(step)
                && let Some(words) = self.words.get(at..end)
            {
                out.words.extend_from_slice(words);
                out.steps.push((*count, *kind));
            }
            at = end;
        }
        out
    }

    /// One mutation of this tape (Gulcan et al. §3.2's three, over steps of the harness's kinds):
    /// a step's words redrawn (which message, member or fate it chose: SwapBuffers and
    /// SwapCrashProcesses); two steps of one kind exchanged; or a step of a kind dropped or
    /// repeated (one fewer or one more delivery: SwapMaxMessages). `draws` chooses which, where,
    /// and the redrawn words.
    pub fn mutate(&self, draws: &mut Seeded) -> Self {
        let len = self.steps.len();
        if len == 0 {
            return self.clone();
        }
        let below = |draws: &mut Seeded, bound: usize| {
            usize::try_from(draws.below(u64::try_from(bound).unwrap_or(u64::MAX))).unwrap_or(0)
        };
        let step = below(draws, len);
        match draws.below(4) {
            0 => {
                let mut out = self.clone();
                let start = self
                    .steps
                    .get(..step)
                    .map_or(0, |before| before.iter().map(|(c, _)| *c as usize).sum());
                let count = self.steps.get(step).map_or(0, |(c, _)| *c as usize);
                for word in out
                    .words
                    .get_mut(start..start.saturating_add(count))
                    .into_iter()
                    .flatten()
                {
                    *word = draws.next_u64();
                }
                out
            }
            1 => {
                let kind = self.steps.get(step).map_or(0, |(_, k)| *k);
                let alike: Vec<usize> = self
                    .steps
                    .iter()
                    .enumerate()
                    .filter(|(_, (_, k))| *k == kind)
                    .map(|(at, _)| at)
                    .collect();
                let other = alike
                    .get(below(draws, alike.len()))
                    .copied()
                    .unwrap_or(step);
                let (a, b) = (step.min(other), step.max(other));
                let mut order: Vec<usize> = (0..len).collect();
                order.swap(a, b);
                self.reordered(&order)
            }
            2 => self.filtered(|at| at != step),
            _ => {
                let mut order: Vec<usize> = (0..len).collect();
                order.insert(step, step);
                self.reordered(&order)
            }
        }
    }

    /// The tape with its steps in `order` (a step may appear twice or not at all).
    fn reordered(&self, order: &[usize]) -> Self {
        let mut out = Self::default();
        for step in order {
            if let (Some(words), Some(entry)) = (self.segment(*step), self.steps.get(*step)) {
                out.words.extend_from_slice(words);
                out.steps.push(*entry);
            }
        }
        out
    }
}

/// Plays a tape to a harness, or records a fresh run, and records what the run took.
#[derive(Clone, Debug)]
pub struct Player {
    source: Tape,
    /// The step of the source being played, and the word within it.
    step: usize,
    word: usize,
    /// Where the words past a step's own come from.
    filler: Seeded,
    out: Tape,
    taken: u32,
    bounds: Bounds,
    replaying: bool,
}

impl Player {
    /// A fresh run: every word from SplitMix64 seeded with `seed`, recorded.
    pub fn record(seed: u64, bounds: Bounds) -> Self {
        Self {
            source: Tape::default(),
            step: 0,
            word: 0,
            filler: Seeded::new(seed),
            out: Tape::default(),
            taken: 0,
            bounds,
            replaying: false,
        }
    }

    /// `tape` played: each step reads its words, then the filler's (seeded with `filler`) if it
    /// asks for more.
    pub fn replay(tape: &Tape, filler: u64, bounds: Bounds) -> Self {
        Self {
            source: tape.clone(),
            replaying: true,
            ..Self::record(filler, bounds)
        }
    }

    /// Steps of the played tape not yet run; `None` for a fresh run, whose length is its
    /// harness's.
    pub fn left(&self) -> Option<usize> {
        self.replaying
            .then(|| self.source.len().saturating_sub(self.step))
    }

    /// The next word of the step being drawn.
    pub fn word(&mut self) -> Result<u64, TapeError> {
        if self.out.words.len() >= self.bounds.words {
            return Err(TapeError::Full {
                what: "words",
                bound: self.bounds.words,
            });
        }
        let own = self
            .source
            .segment(self.step)
            .and_then(|words| words.get(self.word))
            .copied();
        let word = match own {
            Some(word) => {
                self.word = self.word.saturating_add(1);
                word
            }
            None => self.filler.next_u64(),
        };
        self.out
            .words
            .try_reserve(1)
            .map_err(|_| TapeError::Memory)?;
        self.out.words.push(word);
        self.taken = self.taken.saturating_add(1);
        Ok(word)
    }

    /// Uniform in `[0, bound)` from the next word, by the high half of its product with `bound`
    /// (Lemire's multiply-shift without the rejection, the harness's `below`, so a played word is
    /// read as the harness's own draw would have been).
    pub fn below(&mut self, bound: u64) -> Result<u64, TapeError> {
        let word = self.word()?;
        let product = u128::from(word).checked_mul(u128::from(bound)).unwrap_or(0);
        Ok(u64::try_from(product >> 64).unwrap_or(0))
    }

    /// The step drawn is over: its words are closed under `kind`, and the next step of the played
    /// tape is next.
    pub fn end_step(&mut self, kind: u8) -> Result<(), TapeError> {
        if self.out.steps.len() >= self.bounds.steps {
            return Err(TapeError::Full {
                what: "steps",
                bound: self.bounds.steps,
            });
        }
        self.out
            .steps
            .try_reserve(1)
            .map_err(|_| TapeError::Memory)?;
        self.out.steps.push((self.taken, kind));
        self.taken = 0;
        self.step = self.step.saturating_add(1);
        self.word = 0;
        Ok(())
    }

    /// What the run took, step by step: it replays the run exactly.
    pub fn finish(self) -> Tape {
        self.out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOUNDS: Bounds = Bounds {
        steps: 1_000,
        words: 10_000,
    };

    /// A run that draws a step's words by what it has seen: as a harness does.
    fn run(player: &mut Player, steps: usize) -> Vec<u64> {
        let mut seen = Vec::new();
        for _ in 0..steps {
            let first = player.below(5).unwrap();
            for _ in 0..first {
                seen.push(player.below(100).unwrap());
            }
            seen.push(first);
            player.end_step(u8::try_from(first % 2).unwrap()).unwrap();
        }
        seen
    }

    #[test]
    fn a_recorded_tape_replays_its_run_exactly() {
        let mut fresh = Player::record(7, BOUNDS);
        let seen = run(&mut fresh, 40);
        let tape = fresh.finish();
        let mut again = Player::replay(&tape, 99, BOUNDS);
        assert_eq!(run(&mut again, 40), seen);
        assert_eq!(again.finish(), tape);
    }

    #[test]
    fn every_mutation_is_a_feasible_run_that_replays() {
        let mut fresh = Player::record(3, BOUNDS);
        run(&mut fresh, 30);
        let tape = fresh.finish();
        let mut draws = Seeded::new(11);
        // Under Miri a tenth of the mutations, the same code over fewer runs.
        for _ in 0..if cfg!(miri) { 20 } else { 200 } {
            let mutated = tape.mutate(&mut draws);
            let mut player = Player::replay(&mutated, 5, BOUNDS);
            let steps = player.left().unwrap();
            let seen = run(&mut player, steps);
            let taken = player.finish();
            let mut again = Player::replay(&taken, 6, BOUNDS);
            assert_eq!(run(&mut again, steps), seen);
        }
    }

    #[test]
    fn a_tape_past_its_bound_is_refused() {
        let mut player = Player::record(1, Bounds { steps: 2, words: 3 });
        for _ in 0..3 {
            player.word().unwrap();
        }
        assert_eq!(
            player.word(),
            Err(TapeError::Full {
                what: "words",
                bound: 3
            })
        );
    }
}
