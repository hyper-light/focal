use super::*;

fn budget(bytes: usize) -> MemoryBudget {
    MemoryBudget::new(bytes, 0).unwrap()
}

fn participant(n: u128) -> ParticipantId {
    ParticipantId::from_u128(n)
}

/// Each recipient once, sorted, whatever the order and the repeats its deliveries came in; the
/// take leaves nothing noted, and the notes' room is charged while they hold it and given back
/// with them.
#[test]
fn the_notes_are_each_recipient_once_and_their_room_is_charged_while_held() {
    let budget = budget(1 << 20);
    let mut notes = Notes::new(64);
    for n in [5, 3, 5, 9, 3, 1, 9, 9] {
        notes.note(participant(n), &budget);
    }
    assert!(budget.stats().used > 0, "the notes' room is charged");
    let delivered = notes.take();
    assert!(!delivered.all);
    assert_eq!(delivered.recipients, [1, 3, 5, 9].map(participant).to_vec());
    assert!(delivered.reaches(participant(3)));
    assert!(!delivered.reaches(participant(4)));
    assert!(notes.take().is_empty(), "taken, nothing is left noted");
    drop(delivered);
    assert_eq!(budget.stats().used, 0, "the room goes with the notes");
}

/// Past the bound, or with their room refused, the notes cannot be kept exactly: the take says
/// every inbox may have changed, and holds no room.
#[test]
fn notes_that_cannot_be_kept_exactly_say_every_inbox_may_have_changed() {
    let budget = budget(1 << 20);
    let mut notes = Notes::new(3);
    for n in 1..=4 {
        notes.note(participant(n), &budget);
    }
    let delivered = notes.take();
    assert!(delivered.all && delivered.recipients.is_empty());
    assert!(delivered.reaches(participant(99)));
    drop(delivered);
    assert_eq!(budget.stats().used, 0);
    // A budget with no room for the notes.
    let tight = MemoryBudget::new(1, 0).unwrap();
    let mut notes = Notes::new(64);
    notes.note(participant(1), &tight);
    assert!(notes.take().all);
    // And once taken, the next notes are exact again.
    notes.note(participant(2), &budget);
    let delivered = notes.take();
    assert_eq!(
        (delivered.all, delivered.recipients),
        (false, vec![participant(2)])
    );
}
