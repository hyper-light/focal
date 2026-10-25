//! A client's path degrading (VENDORED.md §45): nothing it sent acknowledged for four probe
//! timeouts, quiche's rule.

use super::*;

/// The path events `conn` has for the application, counted: degrading, then progress; the rest
/// are taken.
fn path_events(conn: &mut Connection) -> (u32, u32) {
    let (mut degrading, mut progress) = (0, 0);
    while let Some(event) = conn.poll() {
        match event {
            Event::PathDegrading => degrading += 1,
            Event::ForwardProgress => progress += 1,
            _ => {}
        }
    }
    (degrading, progress)
}

/// Runs the client alone, as if its server were gone: what it sends is lost and time moves to its
/// next timer, at most `turns` times, until `done` holds of the path events the client's
/// connection gives and the time they came.
fn client_alone(
    pair: &mut Pair,
    ch: ConnectionHandle,
    turns: usize,
    mut done: impl FnMut((u32, u32), Instant) -> bool,
) {
    for _ in 0..turns {
        pair.drive_client();
        pair.server.inbound.clear();
        let events = path_events(pair.client_conn_mut(ch));
        if done(events, pair.time) {
            return;
        }
        let next = pair.client.next_wakeup().expect("the client holds a timer");
        pair.time = pair.time.max(next);
    }
    panic!("the condition never held in {turns} turns");
}

/// A client whose server stops answering is told its path is degrading four probe timeouts after
/// the first packet nothing acknowledged, once however long the silence lasts, and that it makes
/// progress once the server answers again; the connection stays open throughout, and what the
/// client sent arrives.
#[test]
fn a_client_whose_server_stops_answering_is_told_its_path_degrading_then_progressing() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    pair.latency = Duration::from_millis(10);
    let (client_ch, server_ch) = pair.connect();
    pair.drive();
    path_events(pair.client_conn_mut(client_ch));
    path_events(pair.server_conn_mut(server_ch));

    let s = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
    pair.client_send(client_ch, s).write(b"hello").unwrap();
    let pto = pair.client_conn_mut(client_ch).data_pto();
    let sent = pair.time;
    let mut told = None;
    client_alone(&mut pair, client_ch, 1_000, |(degrading, progress), now| {
        assert_eq!(progress, 0, "progress with nothing acknowledged");
        if degrading > 0 {
            assert_eq!(degrading, 1);
            told = Some(now);
        }
        told.is_some()
    });
    assert_eq!(told.unwrap() - sent, pto * PATH_DEGRADING_PROBE_TIMEOUTS);
    // Silent through as long again and more: told nothing more, and the connection is open.
    let until = pair.time + pto * PATH_DEGRADING_PROBE_TIMEOUTS * 4;
    client_alone(&mut pair, client_ch, 1_000, |events, now| {
        assert_eq!(events, (0, 0), "told again while still degrading");
        now >= until
    });
    assert!(!pair.client_conn_mut(client_ch).is_closed());
    assert_eq!(
        pair.client_conn_mut(client_ch).stats().path.path_degradings,
        1
    );

    // The server answers again: the client is told it makes progress, once, and the data arrives.
    pair.drive();
    assert_eq!(path_events(pair.client_conn_mut(client_ch)), (0, 1));
    assert_eq!(stream_chunks(pair.server_recv(server_ch, s)), b"hello");
    // The server, the side that cannot go elsewhere, was told nothing.
    assert_eq!(path_events(pair.server_conn_mut(server_ch)), (0, 0));

    // A second silence is a second degrading path.
    let s = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
    pair.client_send(client_ch, s).write(b"again").unwrap();
    client_alone(&mut pair, client_ch, 1_000, |(degrading, _), _| {
        degrading == 1
    });
    assert_eq!(
        pair.client_conn_mut(client_ch).stats().path.path_degradings,
        2
    );
}

/// A client whose server answers is never told its path is degrading, over a path whose round trip
/// is many times the timer's granularity, sending a stream after each acknowledgement and idle
/// between rounds.
#[test]
fn a_client_whose_server_answers_is_never_told_its_path_degrading() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    pair.latency = Duration::from_millis(50);
    let (client_ch, server_ch) = pair.connect();
    pair.drive();
    for round in 0..50_u8 {
        let s = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
        pair.client_send(client_ch, s)
            .write(&[round; 1_000])
            .unwrap();
        pair.client_send(client_ch, s).finish().unwrap();
        pair.drive();
        assert_eq!(stream_chunks(pair.server_recv(server_ch, s)).len(), 1_000);
        assert_eq!(
            path_events(pair.client_conn_mut(client_ch)),
            (0, 0),
            "round {round}"
        );
        pair.time += Duration::from_secs(1);
    }
    assert_eq!(
        pair.client_conn_mut(client_ch).stats().path.path_degradings,
        0
    );
}

/// A server whose client stops answering is told nothing of its path: only the side that dialed,
/// which can go elsewhere, detects it (quiche's `ShouldDetectPathDegrading`).
#[test]
fn a_server_whose_client_stops_answering_is_told_nothing_of_its_path() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    pair.latency = Duration::from_millis(10);
    let (client_ch, server_ch) = pair.connect();
    pair.drive();
    path_events(pair.server_conn_mut(server_ch));
    let s = pair.server_streams(server_ch).open(Dir::Uni).unwrap();
    pair.server_send(server_ch, s).write(b"hello").unwrap();
    let pto = pair.server_conn_mut(server_ch).data_pto();
    let until = pair.time + pto * PATH_DEGRADING_PROBE_TIMEOUTS * 4;
    for _ in 0..1_000 {
        pair.drive_server();
        pair.client.inbound.clear();
        assert_eq!(path_events(pair.server_conn_mut(server_ch)), (0, 0));
        if pair.time >= until {
            break;
        }
        let next = pair.server.next_wakeup().expect("the server holds a timer");
        pair.time = pair.time.max(next);
    }
    assert!(pair.time >= until, "the silence never lasted");
    assert_eq!(path_events(pair.client_conn_mut(client_ch)), (0, 0));
}

/// A packet declared lost by time, once a later one was acknowledged, leaves nothing in flight, and
/// the detection stops there (quiche stops it in quiescence): a client whose server answered what
/// came after is told no degrading path, however long the connection then stays quiet. Without the
/// stop the detection ran on from the lost packet's send and told one four probe timeouts later
/// (found by `a_retired_cid_routes_for_three_probe_timeouts`, whose drive it kept from idling).
#[test]
fn a_packet_lost_by_time_with_nothing_else_in_flight_stops_the_detection() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let (client_ch, _) = pair.connect();
    pair.drive();
    path_events(pair.client_conn_mut(client_ch));
    // A ping the network holds, then a stream it carries
    pair.client_conn_mut(client_ch).ping();
    pair.client.drive(pair.time, pair.server.addr);
    pair.client.delay_outbound();
    let s = pair.client_streams(client_ch).open(Dir::Uni).unwrap();
    pair.client_send(client_ch, s).write(b"after").unwrap();
    pair.drive();
    assert_eq!(
        pair.client_conn_mut(client_ch).pto_state().1,
        0,
        "the ping is lost"
    );
    assert_eq!(path_events(pair.client_conn_mut(client_ch)), (0, 0));
    let pto = pair.client_conn_mut(client_ch).data_pto();
    let until = pair.time + pto * PATH_DEGRADING_PROBE_TIMEOUTS * 4;
    client_alone(&mut pair, client_ch, 1_000, |events, now| {
        assert_eq!(events, (0, 0), "told with nothing in flight");
        now >= until
    });
    assert_eq!(
        pair.client_conn_mut(client_ch).stats().path.path_degradings,
        0
    );
}
