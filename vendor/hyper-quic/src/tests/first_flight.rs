//! A client's first flight when its ClientHello spans more than one Initial datagram, as a
//! post-quantum key share makes it: the default client prefers SecP384r1MLKEM1024, whose 1,665-byte
//! key share (draft-ietf-tls-ecdhe-mlkem: a 97-byte P-384 point and a 1,568-byte ML-KEM-1024
//! encapsulation key) does not fit beside the rest of a ClientHello in one 1,200-byte Initial.

use std::cmp;

use rand::RngExt;

use super::*;

/// RFC 9002 §6.2.1 with no RTT sample (§5.3), from the server's configured initial RTT: the
/// endpoint drops held Initials three of these after they arrive
fn initial_pto(server: &ServerConfig) -> Duration {
    let rtt = server.transport.initial_rtt;
    rtt + cmp::max(4 * (rtt / 2), TIMER_GRANULARITY)
}

#[test]
fn post_quantum_key_exchange_is_negotiated() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let (client_ch, _) = pair.connect();
    let data = pair
        .client_conn_mut(client_ch)
        .crypto_session()
        .handshake_data()
        .unwrap()
        .downcast::<crate::crypto::rustls::HandshakeData>()
        .unwrap();
    assert_eq!(
        data.negotiated_key_exchange_group,
        Some(rustls::NamedGroup::secp384r1MLKEM1024)
    );
}

#[test]
fn the_default_client_hello_spans_two_initial_datagrams() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    pair.begin_connect(single_flights(client_config()));
    pair.drive_client();
    assert_eq!(pair.server.inbound.len(), 2);
    // By default each goes twice (`TransportConfig::handshake_copies`), the copies τ·ln(PTO/τ)
    // behind: 35 ms·ln(999 ms/35 ms) = 117.3 ms at the first probe timeout, from kInitialRtt
    let mut pair = Pair::default();
    pair.begin_connect(client_config());
    pair.drive_client();
    assert_eq!(pair.server.inbound.len(), 2);
    let sent = pair.time;
    let due = pair.client.next_wakeup().unwrap();
    let spacing = due - sent;
    assert!(
        (Duration::from_micros(117_200)..Duration::from_micros(117_400)).contains(&spacing),
        "{spacing:?}"
    );
    pair.time = due;
    pair.drive_client();
    assert_eq!(pair.server.inbound.len(), 4);
}

/// Upstream surfaced the first flight's second datagram, arriving after the server answered the
/// first with Retry, as a second connection attempt. It is held instead, and expires.
#[test]
fn a_retried_attempt_surfaces_once() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    pair.server.handle_incoming = Box::new({
        let mut attempts = 0;
        move |incoming| {
            attempts += 1;
            match attempts {
                1 => IncomingConnectionBehavior::Retry,
                2 => {
                    assert!(incoming.remote_address_validated());
                    IncomingConnectionBehavior::Accept
                }
                _ => panic!("attempt {attempts} surfaced; the client made one"),
            }
        }
    });
    let client_ch = pair.begin_connect(single_flights(client_config()));
    pair.drive();
    let server_ch = pair.server.assert_accept();
    assert_matches!(
        pair.client_conn_mut(client_ch).poll(),
        Some(Event::HandshakeDataReady)
    );
    assert_matches!(
        pair.client_conn_mut(client_ch).poll(),
        Some(Event::Connected)
    );
    assert_matches!(
        pair.server_conn_mut(server_ch).poll(),
        Some(Event::HandshakeDataReady)
    );

    // The straggler is held until three probe timeouts after it arrived; the next datagram after
    // that drops it.
    assert_eq!(pair.server.endpoint.held_initials(), 1);
    pair.time += 3 * initial_pto(&server_config()) + TIMER_GRANULARITY;
    pair.client_conn_mut(client_ch).ping();
    pair.drive();
    assert_eq!(pair.server.endpoint.held_initials(), 0);
}

/// The second datagram of a ClientHello arriving before the first is held, not surfaced and not
/// lost: when the first arrives the attempt surfaces with both, and the handshake proceeds
/// without waiting for the client to probe.
#[test]
fn a_client_hello_out_of_order_surfaces_once_without_a_probe() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let client_ch = pair.begin_connect(single_flights(client_config()));
    pair.drive_client();
    assert_eq!(pair.server.inbound.len(), 2);
    pair.server.inbound.swap(0, 1);

    let before = pair.time;
    pair.drive_server();
    let server_ch = pair.server.assert_accept();
    assert_eq!(pair.server.endpoint.held_initials(), 0);
    assert_eq!(pair.time, before, "the server waited for time to pass");

    pair.drive();
    assert_matches!(
        pair.client_conn_mut(client_ch).poll(),
        Some(Event::HandshakeDataReady)
    );
    assert_matches!(
        pair.client_conn_mut(client_ch).poll(),
        Some(Event::Connected)
    );
    assert_matches!(
        pair.server_conn_mut(server_ch).poll(),
        Some(Event::HandshakeDataReady)
    );
    assert_eq!(pair.client_conn_mut(client_ch).stats().path.lost_packets, 0);
}

/// An Initial whose header is intact but whose payload fails authentication is not a connection
/// attempt: upstream surfaced it and authenticated only in `accept`.
#[test]
fn an_initial_that_fails_authentication_surfaces_nothing() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    pair.begin_connect(client_config());
    pair.drive_client();
    for (_, _, datagram) in pair.server.inbound.iter_mut() {
        // The last byte of each datagram is in its packet's AEAD tag
        let last = datagram.len() - 1;
        datagram[last] ^= 1;
    }
    pair.drive_server();
    assert!(pair.server.accepted.is_none());
    assert!(pair.server.waiting_incoming.is_empty());
    assert_eq!(pair.server.endpoint.held_initials(), 0);
    assert_eq!(pair.server.endpoint.incoming_buffer_bytes(), 0);
}

/// A certificate whose names are random, so RFC 8879 compression cannot shrink it under the
/// anti-amplification budget the way it shrinks `big_cert_and_key`'s repeated names
fn incompressible_cert_and_key() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let mut rng = rand::rng();
    let names = Some("localhost".to_owned())
        .into_iter()
        .chain((0..1000).map(|_| format!("{:032x}.example", rng.random::<u128>())))
        .collect::<Vec<_>>();
    let cert = rcgen::generate_simple_self_signed(names).unwrap();
    (
        cert.cert.into(),
        PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into()),
    )
}

/// RFC 9000 §8.1: before validating the client's address a server sends at most three times the
/// bytes it has received. With a two-datagram ClientHello and a certificate too large for that
/// budget even compressed, the server's first flight fills the budget and stops at it.
#[test]
fn the_first_flight_fills_but_never_exceeds_three_times_what_arrived() {
    let _guard = subscribe();
    let (cert, key) = incompressible_cert_and_key();
    let server = server_config_with_cert(cert.clone(), key);
    let client = single_flights(client_config_with_certs(vec![cert]));
    let mut pair = Pair::new(Default::default(), server);

    pair.begin_connect(client);
    pair.drive_client();
    let received: usize = pair.server.inbound.iter().map(|(_, _, d)| d.len()).sum();
    pair.drive_server();
    let sent: usize = pair.client.inbound.iter().map(|(_, _, d)| d.len()).sum();
    assert!(sent <= 3 * received, "sent {sent} for {received} received");
    assert!(
        sent + usize::from(MIN_INITIAL_SIZE) > 3 * received,
        "sent {sent} for {received} received: a whole datagram more would have fit"
    );
}

/// RFC 9002 §6.2.2 and Appendix A.11 (`OnPacketNumberSpaceDiscarded`: `pto_count = 0`): dropping
/// keys is forward progress and resets the probe backoff. A client whose first flight was lost
/// probed once; when it then discards its Initial keys, on sending its first Handshake packet, its
/// backoff is reset, so a lost Finished is probed one PTO later, not two.
#[test]
fn discarding_initial_keys_resets_the_probe_backoff() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let client_ch = pair.begin_connect(single_flights(client_config()));
    pair.drive_client();
    // The first flight is lost, and the client's PTO fires.
    pair.server.inbound.clear();
    pair.time = pair.client.next_wakeup().unwrap();
    pair.drive_client();
    assert_eq!(pair.client_conn_mut(client_ch).pto_state().0, 1);
    // The probe reaches the server, whose flight reaches the client; the client answers with its
    // first Handshake packet, discarding its Initial keys, before anything acknowledges it.
    pair.drive_server();
    pair.drive_client();
    assert_eq!(pair.client_conn_mut(client_ch).pto_state().0, 0);
}

/// RFC 9000 §8.1 on a path the client migrates to: until the server validates the new address it
/// sends at most three times what it has received there, its MTU probes included, which wait
/// for the validation (upstream let a full datagram go for a single byte of allowance, and sent
/// its probes past the limit).
#[test]
fn a_migrated_path_stays_within_the_amplification_limit() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let (client_ch, server_ch) = pair.connect();
    pair.drive();
    // Any other address: the pair carries datagrams in memory
    let moved = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 2).into(), 4433);
    pair.client.addr = moved;
    pair.client_conn_mut(client_ch).ping();
    let mut validated = false;
    for _ in 0..1_000 {
        let moving = pair.step();
        let server = pair.server_conn_mut(server_ch);
        let (sent, now_validated) = server.amplification_state();
        if server.remote_address() == moved && !now_validated {
            assert!(sent <= 3 * server.total_recvd(), "{sent} sent");
        }
        validated |= now_validated && server.remote_address() == moved;
        if !moving {
            break;
        }
    }
    assert!(validated);
}
