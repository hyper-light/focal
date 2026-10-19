//! The open-loop schedule's arithmetic, shared by the generators: one record due each `interval`
//! from the run's start, none at or past its end, every moment an offset from the start.

use crate::CompareError;
use std::time::Duration;

/// What the schedule had due through a moment the window was full: counted refused at once, rather
/// than a turn of the generator's loop each, which at a rate far past a system's ceiling were the
/// generator's own work.
#[derive(Debug, PartialEq, Eq)]
pub struct Overdue {
    /// Of the records due, those after the warm-up: the refusals a report counts.
    pub measured: u64,
    /// When the first record after them is due.
    pub next: Duration,
}

/// The records due from `next` through `elapsed`, one each `interval`, none at or past `total`:
/// how many of them fell at or after `warmup`, and when the next one is due.
pub fn overdue(
    next: Duration,
    elapsed: Duration,
    interval: Duration,
    warmup: Duration,
    total: Duration,
) -> Result<Overdue, CompareError> {
    let step = interval.as_nanos();
    if step == 0 {
        return Err(CompareError::Argument(
            "a rate past one record a nanosecond",
        ));
    }
    let one = Duration::from_nanos(1);
    // The records due from `next` through `through`, both included.
    let due = |through: Duration| -> Option<u128> {
        match through.as_nanos().checked_sub(next.as_nanos()) {
            None => Some(0),
            Some(span) => span.checked_div(step)?.checked_add(1),
        }
    };
    let last = elapsed.min(total.saturating_sub(one));
    let all = due(last).ok_or(CompareError::Argument("schedule"))?;
    let warming = if warmup.is_zero() {
        0
    } else {
        due(last.min(warmup.saturating_sub(one))).ok_or(CompareError::Argument("schedule"))?
    };
    let measured = all
        .checked_sub(warming)
        .and_then(|measured| u64::try_from(measured).ok())
        .ok_or(CompareError::Argument("schedule"))?;
    let next = all
        .checked_mul(step)
        .and_then(|span| span.checked_add(next.as_nanos()))
        .and_then(|next| u64::try_from(next).ok())
        .map(Duration::from_nanos)
        .ok_or(CompareError::Argument("schedule"))?;
    Ok(Overdue { measured, next })
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn every_record_due_through_the_moment_is_counted_once_and_the_next_is_past_it() {
        // Due at 10, 11, ... 15 ms: six records, none warming, the next at 16 ms.
        let skipped = overdue(10 * MS, 15 * MS, MS, Duration::ZERO, 100 * MS).unwrap();
        assert_eq!(
            skipped,
            Overdue {
                measured: 6,
                next: 16 * MS
            }
        );
        // A moment between two records takes the earlier one only.
        let skipped = overdue(
            10 * MS,
            15 * MS + Duration::from_micros(999),
            MS,
            Duration::ZERO,
            100 * MS,
        )
        .unwrap();
        assert_eq!(skipped.measured, 6);
        assert_eq!(skipped.next, 16 * MS);
    }

    #[test]
    fn the_records_due_in_the_warm_up_are_not_counted() {
        // Due at 8, 9 (warming), 10, 11, 12 ms: three counted.
        let skipped = overdue(8 * MS, 12 * MS, MS, 10 * MS, 100 * MS).unwrap();
        assert_eq!(skipped.measured, 3);
        assert_eq!(skipped.next, 13 * MS);
        // All of them warming.
        let skipped = overdue(2 * MS, 6 * MS, MS, 10 * MS, 100 * MS).unwrap();
        assert_eq!(skipped.measured, 0);
        assert_eq!(skipped.next, 7 * MS);
    }

    #[test]
    fn nothing_at_or_past_the_end_is_due() {
        // Due at 97, 98, 99 ms; 100 ms is the end.
        let skipped = overdue(97 * MS, 140 * MS, MS, Duration::ZERO, 100 * MS).unwrap();
        assert_eq!(skipped.measured, 3);
        assert_eq!(skipped.next, 100 * MS);
        // A schedule already at its end has nothing due and stays there.
        let skipped = overdue(100 * MS, 140 * MS, MS, Duration::ZERO, 100 * MS).unwrap();
        assert_eq!(skipped.measured, 0);
        assert_eq!(skipped.next, 100 * MS);
    }

    #[test]
    fn a_moment_before_the_next_record_has_nothing_due() {
        let skipped = overdue(10 * MS, 9 * MS, MS, Duration::ZERO, 100 * MS).unwrap();
        assert_eq!(skipped.measured, 0);
        assert_eq!(skipped.next, 10 * MS);
    }

    #[test]
    fn the_count_matches_a_record_at_a_time() {
        // Against the loop it replaces, on an interval that does not divide a millisecond.
        let interval = Duration::from_nanos(333_333);
        let (warmup, total) = (7 * MS, 20 * MS);
        // `next` is on the schedule, as the generators keep it: 0, just before the warm-up's
        // end, and two records before the end.
        for (first, elapsed) in [
            (0, 25_000_000),
            (20, 7_000_001),
            (58, 30_000_000),
            (3, 900_000),
        ] {
            let next = interval * first;
            let elapsed = Duration::from_nanos(elapsed);
            let (mut at, mut measured, mut after) = (next, 0_u64, next);
            // The schedule's records from `next`, as the loop met them.
            while at < total && at <= elapsed {
                if at >= warmup {
                    measured += 1;
                }
                at += interval;
                after = at;
            }
            let skipped = overdue(next, elapsed, interval, warmup, total).unwrap();
            assert_eq!(
                skipped.measured, measured,
                "from {next:?} through {elapsed:?}"
            );
            assert_eq!(skipped.next, after, "from {next:?} through {elapsed:?}");
        }
    }

    #[test]
    fn a_zero_interval_is_refused() {
        assert!(overdue(MS, MS, Duration::ZERO, MS, MS).is_err());
    }
}
