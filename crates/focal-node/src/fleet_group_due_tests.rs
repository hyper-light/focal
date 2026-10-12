//! When a grouped owner runs a session next (`GroupDue::at`): at once only
//! when running it can make progress.
use super::*;

fn due() -> GroupDue {
    GroupDue {
        pressed: false,
        persistence_pending: false,
        delivery_retained: false,
        drain_owed: false,
        has_ready: false,
        beats: false,
    }
}

/// `now`, the next tick and the next beat, apart.
fn times() -> (Instant, Instant, Instant) {
    let now = Instant::now();
    (
        now,
        now + Duration::from_millis(100),
        now + Duration::from_millis(40),
    )
}

#[test]
fn a_session_with_something_to_drain_is_run_at_once() {
    let (now, tick, beat) = times();
    let ready = GroupDue {
        has_ready: true,
        ..due()
    };
    assert_eq!(ready.at(now, tick, beat), Some(now));
    let owed = GroupDue {
        drain_owed: true,
        ..due()
    };
    assert_eq!(owed.at(now, tick, beat), Some(now));
}

/// A delivery retained for what it lacks makes no progress until that comes:
/// a session holding one, with nothing else owed, waits for its tick — and
/// for its beat while it leads at a stretched period — where it was run again
/// at once, round after round (a follower short of memory drained 54 times a
/// period, each refused). The drain a batch owes is due at once all the same.
#[test]
fn a_retained_delivery_is_tried_again_at_the_tick_not_at_once() {
    let (now, tick, beat) = times();
    let retained = GroupDue {
        delivery_retained: true,
        has_ready: true,
        ..due()
    };
    assert_eq!(retained.at(now, tick, beat), Some(tick));
    let leading = GroupDue {
        beats: true,
        ..retained
    };
    assert_eq!(leading.at(now, tick, beat), Some(beat));
    let owed = GroupDue {
        drain_owed: true,
        ..retained
    };
    assert_eq!(owed.at(now, tick, beat), Some(now));
}

#[test]
fn a_session_waiting_for_its_write_is_woken_by_its_log_or_its_tick() {
    let (now, tick, beat) = times();
    let waiting = GroupDue {
        persistence_pending: true,
        has_ready: true,
        drain_owed: true,
        beats: true,
        ..due()
    };
    assert_eq!(waiting.at(now, tick, beat), Some(tick));
}

#[test]
fn an_idle_session_waits_for_its_tick_or_its_beat_and_a_pressed_one_a_millisecond() {
    let (now, tick, beat) = times();
    assert_eq!(due().at(now, tick, beat), Some(tick));
    let leading = GroupDue {
        beats: true,
        ..due()
    };
    assert_eq!(leading.at(now, tick, beat), Some(beat));
    let pressed = GroupDue {
        pressed: true,
        persistence_pending: true,
        ..due()
    };
    assert_eq!(
        pressed.at(now, tick, beat),
        Some(now + Duration::from_millis(1))
    );
}
