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
/// that refuses, answers nothing, and the peer has answered nothing meanwhile.
#[test]
fn an_append_is_answered_once_its_peer_holds_its_last_entry() {
    let start = Instant::now();
    let mut clock = AckClock::default();
    assert_eq!(clock.answered(2), None, "nothing measured");
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
        clock.answered(2),
        None,
        "short of it, or refused: no answer"
    );
    assert_eq!(clock.outstanding(2, 3, start + ms(7)), Some(ms(7)));
    clock.stepped(
        answer(2, 3, MessageType::MsgAppendResponse, 12, false),
        start + ms(8),
    );
    let late = clock.answered(2).unwrap();
    assert!(late >= ms(8) && late < ms(500), "{late:?}");
    assert_eq!(clock.outstanding(2, 3, start + ms(500)), None);
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
    let late = clock.answered(2).unwrap();
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
    let late = clock.answered(2).unwrap();
    assert!(late >= ms(30), "{late:?}");
    // Nothing is timed now: the next to leave is.
    clock.sent(&heartbeat(2, 3), start + ms(40));
    assert_eq!(clock.outstanding(2, 3, start + ms(41)), Some(ms(1)));
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
    assert_eq!(clock.answered(3), None);
    assert_eq!(clock.outstanding(3, 4, start + ms(2)), Some(ms(2)));
    clock.stepped(
        answer(3, 4, MessageType::MsgHeartbeatResponse, 0, false),
        start + ms(4),
    );
    let late = clock.answered(3).unwrap();
    assert!(late >= ms(4) && late < ms(1000), "{late:?}");
}

/// Only an answer of the timed message's term answers it, and a message of a newer term takes
/// the place of one timed in an older term.
#[test]
fn a_message_is_timed_and_answered_within_its_term() {
    let start = Instant::now();
    let mut clock = AckClock::default();
    clock.sent(&append(2, 3, 10, Some(11)), start);
    clock.stepped(
        answer(2, 2, MessageType::MsgAppendResponse, 11, false),
        start + ms(1),
    );
    assert_eq!(clock.answered(2), None, "an older term's answer");
    assert_eq!(clock.outstanding(2, 3, start + ms(2)), Some(ms(2)));
    assert_eq!(
        clock.outstanding(2, 5, start + ms(2)),
        None,
        "a term it left"
    );
    clock.sent(&heartbeat(2, 5), start + ms(10));
    assert_eq!(clock.outstanding(2, 5, start + ms(12)), Some(ms(2)));
    clock.stepped(
        answer(2, 5, MessageType::MsgHeartbeatResponse, 0, false),
        start + ms(13),
    );
    let late = clock.answered(2).unwrap();
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
    assert_eq!(clock.outstanding(2, 3, start + ms(50)), None);
    assert_eq!(clock.answered(2), None);
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
    let late = |clock: &AckClock, peer| clock.answered(peer).unwrap();
    assert_eq!(clock.quorum_tail(&[1, 2, 3], 1), Some(late(&clock, 2)));
    assert_eq!(
        clock.quorum_tail(&[1, 2, 3, 4, 5], 1),
        Some(late(&clock, 4))
    );
    assert_eq!(clock.quorum_tail(&[1, 6, 7], 1), None);
    assert_eq!(
        clock.quorum_tail(&[1], 1),
        None,
        "a voter alone waits on none"
    );
}

/// Silence never makes a peer later: a quorum that stops answering (cut off, or gone) leaves
/// the tail its answers took, however long its messages go unanswered, so a deadline taken
/// from it is not moved on by the silence it judges.
#[test]
fn silence_never_makes_a_quorum_later() {
    let start = Instant::now();
    let mut clock = AckClock::default();
    for peer in [2, 3] {
        clock.sent(&heartbeat(peer, 1), start);
        clock.stepped(
            answer(peer, 1, MessageType::MsgHeartbeatResponse, 0, false),
            start + ms(10),
        );
    }
    let before = clock.quorum_tail(&[1, 2, 3], 1).unwrap();
    // Both are sent to and never answer again.
    for at in [20, 30] {
        clock.sent(&heartbeat(2, 1), start + ms(at));
        clock.sent(&append(3, 1, 5, Some(6)), start + ms(at));
    }
    assert_eq!(
        clock.outstanding(2, 1, start + ms(60_000)),
        Some(ms(59_980))
    );
    assert_eq!(clock.quorum_tail(&[1, 2, 3], 1), Some(before));
    assert!(before < ms(100), "{before:?}");
}

/// A late answer makes its peer later: a peer that stalls is measured late once it answers,
/// its answer as old as the stall, and the quorum's tail grows only when both answered late.
#[test]
fn a_stalled_peer_is_measured_late_once_it_answers() {
    let start = Instant::now();
    let mut clock = AckClock::default();
    for peer in [2, 3] {
        clock.sent(&heartbeat(peer, 1), start);
        clock.stepped(
            answer(peer, 1, MessageType::MsgHeartbeatResponse, 0, false),
            start + ms(10),
        );
    }
    let quick = clock.quorum_tail(&[1, 2, 3], 1).unwrap();
    // Peer 2 stalls nine hundred milliseconds, then answers.
    clock.sent(&heartbeat(2, 1), start + ms(100));
    clock.stepped(
        answer(2, 1, MessageType::MsgHeartbeatResponse, 0, false),
        start + ms(1_000),
    );
    assert!(clock.answered(2).unwrap() >= ms(900));
    // The commit still waits on the sooner of the two: unchanged.
    assert_eq!(clock.quorum_tail(&[1, 2, 3], 1), Some(quick));
    // Peer 3 stalls as long: now the quorum is that late.
    clock.sent(&heartbeat(3, 1), start + ms(1_100));
    clock.stepped(
        answer(3, 1, MessageType::MsgHeartbeatResponse, 0, false),
        start + ms(2_000),
    );
    assert!(clock.quorum_tail(&[1, 2, 3], 1).unwrap() >= ms(900));
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
    assert_eq!(clock.outstanding(2, 1, start + ms(1)), None);
    assert_eq!(clock.outstanding(3, 1, start + ms(1)), Some(ms(1)));
    for peer in 10..10 + PEERS as u64 {
        clock.sent(&heartbeat(peer, 1), start);
    }
    assert_eq!(clock.peers.len(), PEERS);
    assert_eq!(clock.outstanding(9 + PEERS as u64, 1, start + ms(1)), None);
}
