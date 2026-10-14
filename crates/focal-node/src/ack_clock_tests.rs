use super::*;
use focal_consensus::Entry;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn append(to: u64, term: u64, index: u64, last: Option<u64>) -> Message {
    Message {
        msg_type: MessageType::MsgAppend,
        to,
        from: 1,
        term,
        index,
        entries: last
            .map(|index| {
                vec![Entry {
                    index,
                    term,
                    ..Entry::default()
                }]
            })
            .unwrap_or_default(),
        ..Message::default()
    }
}

fn heartbeat(to: u64, term: u64) -> Message {
    Message {
        msg_type: MessageType::MsgHeartbeat,
        to,
        from: 1,
        term,
        ..Message::default()
    }
}

fn answer(from: u64, term: u64, msg_type: MessageType, index: u64, reject: bool) -> Stepped {
    Stepped {
        msg_type,
        from,
        term,
        index,
        reject,
    }
}

/// An append is answered once its peer holds its last entry: an answer short of it, or one
/// that refuses, answers nothing, and the peer is as late as the append is old meanwhile.
#[test]
fn an_append_is_answered_once_its_peer_holds_its_last_entry() {
    let start = Instant::now();
    let mut clock = AckClock::default();
    assert_eq!(clock.lateness(2, 3, start), None, "nothing measured");
    clock.sent(&append(2, 3, 10, Some(12)), start);
    clock.stepped(
        answer(2, 3, MessageType::MsgAppendResponse, 11, false),
        start + ms(5),
    );
    clock.stepped(
        answer(2, 3, MessageType::MsgAppendResponse, 12, true),
        start + ms(6),
    );
    assert_eq!(
        clock.lateness(2, 3, start + ms(7)),
        Some(ms(7)),
        "unanswered, the peer is as late as the append is old"
    );
    clock.stepped(
        answer(2, 3, MessageType::MsgAppendResponse, 12, false),
        start + ms(8),
    );
    let late = clock.lateness(2, 3, start + ms(500)).unwrap();
    assert!(late >= ms(8), "{late:?}");
    assert!(
        late < ms(500),
        "answered, the append's age is no longer the peer's lateness: {late:?}"
    );
}

/// An append that carries no entry is answered once its peer holds the entry it names.
#[test]
fn an_empty_append_is_answered_at_the_entry_it_names() {
    let start = Instant::now();
    let mut clock = AckClock::default();
    clock.sent(&append(2, 3, 40, None), start);
    clock.stepped(
        answer(2, 3, MessageType::MsgAppendResponse, 40, false),
        start + ms(3),
    );
    let late = clock.lateness(2, 3, start + ms(3)).unwrap();
    assert!(late >= ms(3) && late < ms(100), "{late:?}");
}

/// One message to a peer is timed at a time: the first to leave, whatever leaves after it
/// before its answer comes.
#[test]
fn one_message_is_timed_at_a_time() {
    let start = Instant::now();
    let mut clock = AckClock::default();
    clock.sent(&append(2, 3, 10, Some(11)), start);
    clock.sent(&append(2, 3, 11, Some(12)), start + ms(20));
    clock.sent(&heartbeat(2, 3), start + ms(25));
    // The second append's answer answers the first, timed from when the first left.
    clock.stepped(
        answer(2, 3, MessageType::MsgAppendResponse, 12, false),
        start + ms(30),
    );
    let late = clock.lateness(2, 3, start + ms(30)).unwrap();
    assert!(late >= ms(30), "{late:?}");
    // Nothing is timed now: the next to leave is.
    clock.sent(&heartbeat(2, 3), start + ms(40));
    assert!(clock.lateness(2, 3, start + ms(41)).unwrap() >= late);
}

/// A heartbeat is answered by an answer to a heartbeat, and by nothing else.
#[test]
fn a_heartbeat_is_answered_by_a_heartbeats_answer() {
    let start = Instant::now();
    let mut clock = AckClock::default();
    clock.sent(&heartbeat(3, 4), start);
    clock.stepped(
        answer(3, 4, MessageType::MsgAppendResponse, 99, false),
        start + ms(1),
    );
    assert_eq!(clock.lateness(3, 4, start + ms(2)), Some(ms(2)));
    clock.stepped(
        answer(3, 4, MessageType::MsgHeartbeatResponse, 0, false),
        start + ms(4),
    );
    let late = clock.lateness(3, 4, start + ms(1000)).unwrap();
    assert!(late >= ms(4) && late < ms(1000), "{late:?}");
}

/// Only an answer of the timed message's term answers it; a message of a newer term takes the
/// place of one timed in an older term; and the age of a message of an older term is no
/// lateness in the term the replica is in.
#[test]
fn a_message_is_timed_and_answered_within_its_term() {
    let start = Instant::now();
    let mut clock = AckClock::default();
    clock.sent(&append(2, 3, 10, Some(11)), start);
    clock.stepped(
        answer(2, 2, MessageType::MsgAppendResponse, 11, false),
        start + ms(1),
    );
    assert_eq!(
        clock.lateness(2, 3, start + ms(2)),
        Some(ms(2)),
        "an older term's answer"
    );
    assert_eq!(
        clock.lateness(2, 5, start + ms(2)),
        None,
        "a term the replica left"
    );
    clock.sent(&heartbeat(2, 5), start + ms(10));
    assert_eq!(clock.lateness(2, 5, start + ms(12)), Some(ms(2)));
    clock.stepped(
        answer(2, 5, MessageType::MsgHeartbeatResponse, 0, false),
        start + ms(13),
    );
    let late = clock.lateness(2, 5, start + ms(13)).unwrap();
    assert!(late >= ms(3) && late < ms(13), "{late:?}");
}

/// A message other than an append or a heartbeat is not timed: nothing a leader waits on
/// answers it.
#[test]
fn only_appends_and_heartbeats_are_timed() {
    let start = Instant::now();
    let mut clock = AckClock::default();
    for msg_type in [
        MessageType::MsgRequestVote,
        MessageType::MsgSnapshot,
        MessageType::MsgTimeoutNow,
        MessageType::MsgAppendResponse,
    ] {
        clock.sent(
            &Message {
                msg_type,
                to: 2,
                term: 3,
                ..Message::default()
            },
            start,
        );
    }
    assert_eq!(clock.lateness(2, 3, start + ms(50)), None);
}

/// A commit waits on the voters that answer first: of three voters, the sooner of the other
/// two; of five, the second soonest of the other four. Voters nothing is measured of are not
/// counted, and with too few measured nothing is known.
#[test]
fn the_quorum_waits_on_the_voters_that_answer_first() {
    let start = Instant::now();
    let mut clock = AckClock::default();
    for (peer, after) in [(2, 5), (3, 30), (4, 9), (5, 700)] {
        clock.sent(&heartbeat(peer, 1), start);
        clock.stepped(
            answer(peer, 1, MessageType::MsgHeartbeatResponse, 0, false),
            start + ms(after),
        );
    }
    let at = start + ms(1000);
    let late = |clock: &AckClock, peer| clock.lateness(peer, 1, at).unwrap();
    assert_eq!(
        clock.quorum_tail(&[1, 2, 3], 1, 1, at),
        Some(late(&clock, 2))
    );
    assert_eq!(
        clock.quorum_tail(&[1, 2, 3, 4, 5], 1, 1, at),
        Some(late(&clock, 4))
    );
    assert_eq!(clock.quorum_tail(&[1, 6, 7], 1, 1, at), None);
    assert_eq!(
        clock.quorum_tail(&[1], 1, 1, at),
        None,
        "a voter alone waits on none"
    );
}

/// A peer that left the configuration is forgotten; and no more peers are timed than a
/// configuration may name.
#[test]
fn peers_are_bounded_and_forgotten_as_they_leave() {
    let start = Instant::now();
    let mut clock = AckClock::default();
    clock.sent(&heartbeat(2, 1), start);
    clock.sent(&heartbeat(3, 1), start);
    clock.retain(|peer| peer == 3);
    assert_eq!(clock.lateness(2, 1, start + ms(1)), None);
    assert_eq!(clock.lateness(3, 1, start + ms(1)), Some(ms(1)));
    for peer in 10..10 + PEERS as u64 {
        clock.sent(&heartbeat(peer, 1), start);
    }
    assert_eq!(clock.peers.len(), PEERS);
    assert_eq!(clock.lateness(9 + PEERS as u64, 1, start + ms(1)), None);
}
