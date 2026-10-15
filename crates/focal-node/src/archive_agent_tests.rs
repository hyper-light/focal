use super::*;

const INTERVAL: Duration = Duration::from_secs(5);

fn walked(retired: usize, full: bool, partial: bool) -> Walked {
    Walked {
        retired,
        deferred: 0,
        full,
        partial,
    }
}

/// A walk that filled a batch, or stopped short having found families, is followed at once:
/// more are waiting. One that found none goes on through its pass at a scan's pace, then waits
/// the interval. One that found fewer than a batch
/// waits as long as a batch's worth took to arrive at the rate these did: at 300 families a
/// second a batch of 64 is some 213 ms, so batches stay full and retirement keeps pace, where a
/// walk once waited the whole interval after every partial batch (doc 31 M2: at 300
/// lifecycles a second, retirement stalled five seconds at a time and fell thousands behind).
#[test]
fn a_walk_comes_again_as_soon_as_a_batch_is_likely_waiting() {
    let at_once = BACKLOG_INTERVAL;
    assert_eq!(
        retire_pace(walked(64, true, false), INTERVAL, INTERVAL),
        at_once
    );
    // A full batch none of which could retire (every candidate's room taken): no spin.
    assert_eq!(
        retire_pace(walked(0, true, false), INTERVAL, INTERVAL),
        INTERVAL
    );
    // Found and deferred, all waiting on a pending candidate: soon, at the rate they were found,
    // neither at once nor after the interval.
    let waiting = Walked {
        deferred: 64,
        ..walked(0, true, true)
    };
    assert_eq!(
        retire_pace(waiting, Duration::from_millis(200), INTERVAL),
        Duration::from_millis(200)
    );
    assert_eq!(
        retire_pace(walked(3, false, true), INTERVAL, INTERVAL),
        at_once
    );
    // Nothing found yet with further to go: on through the pass at a scan's pace; the pass
    // done, the interval.
    assert_eq!(
        retire_pace(walked(0, false, true), INTERVAL, INTERVAL),
        SCAN_INTERVAL
    );
    assert_eq!(
        retire_pace(walked(0, false, false), INTERVAL, INTERVAL),
        INTERVAL
    );
    // 300 a second: 60 found a fifth of a second after the walk before.
    let pace = retire_pace(
        walked(60, false, false),
        Duration::from_millis(200),
        INTERVAL,
    );
    assert!(
        pace > Duration::from_millis(200) && pace < Duration::from_millis(220),
        "{pace:?}"
    );
    // One a second: a batch would take a minute, so the interval bounds it.
    assert_eq!(
        retire_pace(walked(5, false, false), Duration::from_secs(5), INTERVAL),
        INTERVAL
    );
    // Faster than a batch an interval of the backlog's: never sooner than it.
    assert_eq!(
        retire_pace(walked(63, false, false), Duration::from_millis(1), INTERVAL),
        at_once
    );
    // An interval below the backlog's is raised to it.
    assert_eq!(
        retire_pace(walked(0, false, false), INTERVAL, Duration::from_millis(1)),
        at_once
    );
    // The widest inputs saturate within the bounds.
    assert_eq!(
        retire_pace(walked(1, false, false), Duration::MAX, INTERVAL),
        INTERVAL
    );
    assert_eq!(
        retire_pace(walked(usize::MAX, false, false), INTERVAL, INTERVAL),
        at_once
    );
}

/// A walk refused or failed is asked again soon, each failure in a row doubling the wait, never
/// later than the interval.
#[test]
fn a_failed_walk_is_asked_again_sooner_than_the_interval_while_its_failures_are_few() {
    let interval = Duration::from_secs(5);
    assert_eq!(failed_pace(1, interval), BACKLOG_INTERVAL);
    assert_eq!(failed_pace(2, interval), BACKLOG_INTERVAL * 2);
    assert_eq!(failed_pace(3, interval), BACKLOG_INTERVAL * 4);
    assert_eq!(failed_pace(9, interval), interval);
    assert_eq!(failed_pace(u32::MAX, interval), interval);
    assert_eq!(failed_pace(1, Duration::from_millis(1)), BACKLOG_INTERVAL);
}
