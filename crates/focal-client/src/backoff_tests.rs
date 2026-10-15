//! The audit's F64: the pause between a caller's attempts is the exponential
//! step spread by full jitter, so callers that failed together do not
//! return together; the attempt and elapsed budgets bound it as before.
use crate::client::jittered;
use crate::*;
use std::time::Duration;

/// SplitMix64: a seeded draw for the simulation, so a run is repeatable.
struct Draws(u64);
impl Draws {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[test]
fn a_step_is_spread_over_itself_and_never_past_the_budget() {
    let policy = RetryPolicy::default();
    for backoffs in 0..8u32 {
        let step = Duration::from_millis(20 * (1u64 << backoffs)).min(Duration::from_millis(500));
        assert_eq!(
            jittered(&policy, backoffs, Duration::MAX, 0),
            Duration::ZERO
        );
        let whole = jittered(&policy, backoffs, Duration::MAX, u64::MAX);
        assert!(
            whole <= step && whole >= step - Duration::from_nanos(1),
            "{whole:?} {step:?}"
        );
        let middle = jittered(&policy, backoffs, Duration::MAX, u64::MAX / 2);
        assert!(
            middle <= step / 2 && middle >= step / 2 - Duration::from_nanos(1),
            "{middle:?}"
        );
    }
    // What remains of the clock caps every draw.
    assert_eq!(
        jittered(&policy, 5, Duration::from_millis(3), u64::MAX),
        Duration::from_millis(3)
    );
    assert_eq!(
        jittered(&policy, 5, Duration::ZERO, u64::MAX),
        Duration::ZERO
    );
}

/// A wave: a thousand callers refused together by a service that serves
/// fifty a millisecond, each retrying under the default policy. The whole
/// step returns the wave intact at every step; the spread thins it.
fn wave(spread: bool, seed: u64) -> (u64, u64, usize) {
    const CALLERS: usize = 1000;
    const CAPACITY: usize = 50;
    let policy = RetryPolicy::default();
    let mut draws = Draws(seed);
    // Each caller's next attempt in milliseconds, and its refusals so far.
    let mut callers: Vec<Option<(u64, u32)>> = vec![Some((0, 0)); CALLERS];
    let mut attempts = 0u64;
    let mut tallest = 0usize;
    let mut now = 0u64;
    while callers.iter().any(Option::is_some) {
        let mut due: Vec<usize> = callers
            .iter()
            .enumerate()
            .filter_map(|(index, caller)| caller.filter(|(at, _)| *at <= now).map(|_| index))
            .collect();
        if now > 0 {
            tallest = tallest.max(due.len());
        }
        attempts += due.len() as u64;
        let served = due.len().min(CAPACITY);
        for index in due.drain(..served) {
            callers[index] = None;
        }
        for index in due {
            let Some((_, refusals)) = callers[index] else {
                continue;
            };
            let random = if spread { draws.next() } else { u64::MAX };
            let pause = jittered(&policy, refusals, Duration::MAX, random);
            callers[index] = Some((now + pause.as_millis() as u64, refusals + 1));
        }
        now += 1;
        assert!(now < 60_000, "the wave never drained");
    }
    (attempts, now, tallest)
}

#[test]
fn a_wave_of_refused_callers_thins_over_its_spread_and_takes_fewer_calls() {
    let (whole_attempts, whole_ticks, whole_tallest) = wave(false, 1);
    // Every step returns the wave whole: nineteen more waves of what was
    // left, each fifty shorter.
    assert_eq!(whole_tallest, 950);
    assert_eq!(whole_attempts, 10_500);
    eprintln!(
        "F64 whole step: {whole_attempts} calls, tallest wave {whole_tallest}, {whole_ticks} ms"
    );
    for seed in 1..=3u64 {
        let (attempts, ticks, tallest) = wave(true, seed);
        eprintln!("F64 spread seed {seed}: {attempts} calls, tallest wave {tallest}, {ticks} ms");
        assert!(
            attempts < whole_attempts / 2,
            "seed {seed}: {attempts} of {whole_attempts}"
        );
        assert!(
            tallest < whole_tallest / 4,
            "seed {seed}: {tallest} of {whole_tallest}"
        );
        assert!(
            ticks <= whole_ticks,
            "seed {seed}: {ticks} ticks against {whole_ticks}"
        );
    }
}
