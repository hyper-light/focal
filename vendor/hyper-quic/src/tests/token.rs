//! Tests specifically for tokens

use super::*;

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
use wasm_bindgen_test::wasm_bindgen_test as test;

#[test]
fn stateless_retry() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    pair.server.handle_incoming = Box::new(validate_incoming);
    let (client_ch, _server_ch) = pair.connect();
    pair.client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);
}

#[test]
fn retry_token_expired() {
    let _guard = subscribe();

    let retry_token_lifetime = Duration::from_secs(1);

    let mut pair = Pair::default();
    pair.server.handle_incoming = Box::new(validate_incoming);

    let mut config = server_config();
    config
        .time_source(Box::new(FixedTime(START)))
        .retry_token_lifetime(retry_token_lifetime);
    pair.server.set_server_config(Some(config)).unwrap();

    let client_ch = pair.begin_connect(client_config());
    pair.drive_client();
    pair.drive_server();
    pair.drive_client();

    // to expire retry token
    advance_server_clock(&mut pair, retry_token_lifetime + Duration::from_millis(1));

    pair.drive();
    assert_matches!(
        pair.client_conn_mut(client_ch).poll(),
        Some(Event::ConnectionLost { reason: ConnectionError::ConnectionClosed(err) })
        if err.error_code == TransportErrorCode::INVALID_TOKEN
    );

    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);
}

#[test]
fn use_token() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let client_config = pair.add_client_config(client_config());
    let (client_ch, _server_ch) = pair.connect_with_shared(client_config);
    pair.client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);

    pair.server.handle_incoming = Box::new(|incoming| {
        assert!(incoming.remote_address_validated());
        assert!(incoming.may_retry());
        IncomingConnectionBehavior::Accept
    });
    let (client_ch_2, _server_ch_2) = pair.connect_with_shared(client_config);
    pair.client
        .connections
        .get_mut(&client_ch_2)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);
}

#[test]
fn retry_then_use_token() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let client_config = pair.add_client_config(client_config());
    pair.server.handle_incoming = Box::new(validate_incoming);
    let (client_ch, _server_ch) = pair.connect_with_shared(client_config);
    pair.client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);

    pair.server.handle_incoming = Box::new(|incoming| {
        assert!(incoming.remote_address_validated());
        assert!(incoming.may_retry());
        IncomingConnectionBehavior::Accept
    });
    let (client_ch_2, _server_ch_2) = pair.connect_with_shared(client_config);
    pair.client
        .connections
        .get_mut(&client_ch_2)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);
}

#[test]
fn use_token_then_retry() {
    let _guard = subscribe();
    let mut pair = Pair::default();
    let client_config = pair.add_client_config(single_flights(client_config()));
    let (client_ch, _server_ch) = pair.connect_with_shared(client_config);
    pair.client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);

    pair.server.handle_incoming = Box::new({
        let mut i = 0;
        move |incoming| {
            if i == 0 {
                assert!(incoming.remote_address_validated());
                assert!(incoming.may_retry());
                i += 1;
                IncomingConnectionBehavior::Retry
            } else if i == 1 {
                assert!(incoming.remote_address_validated());
                assert!(!incoming.may_retry());
                i += 1;
                IncomingConnectionBehavior::Accept
            } else {
                panic!("too many handle_incoming iterations")
            }
        }
    });
    let (client_ch_2, _server_ch_2) = pair.connect_with_shared(client_config);
    pair.client
        .connections
        .get_mut(&client_ch_2)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);
}

#[test]
fn use_same_token_twice() {
    #[derive(Default)]
    struct EvilTokenStore(Bytes);

    impl TokenStore for EvilTokenStore {
        fn insert(&mut self, _server_name: &str, token: Bytes) {
            if self.0.is_empty() {
                self.0 = token;
            }
        }

        fn take(&mut self, _server_name: &str) -> Option<Bytes> {
            if self.0.is_empty() {
                None
            } else {
                Some(self.0.clone())
            }
        }
    }

    let _guard = subscribe();
    let mut pair = Pair::default();
    let client_config = pair.add_client_config(client_config());
    pair.client
        .endpoint
        .set_token_store(Box::new(EvilTokenStore::default()));
    let (client_ch, _server_ch) = pair.connect_with_shared(client_config);
    pair.client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);

    pair.server.handle_incoming = Box::new(|incoming| {
        assert!(incoming.remote_address_validated());
        assert!(incoming.may_retry());
        IncomingConnectionBehavior::Accept
    });
    let (client_ch_2, _server_ch_2) = pair.connect_with_shared(client_config);
    pair.client
        .connections
        .get_mut(&client_ch_2)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);

    pair.server.handle_incoming = Box::new(|incoming| {
        assert!(!incoming.remote_address_validated());
        assert!(incoming.may_retry());
        IncomingConnectionBehavior::Accept
    });
    let (client_ch_3, _server_ch_3) = pair.connect_with_shared(client_config);
    pair.client
        .connections
        .get_mut(&client_ch_3)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);
}

#[test]
fn use_token_expired() {
    let _guard = subscribe();
    let lifetime = Duration::from_secs(10000);
    let mut server_config = server_config();
    server_config
        .time_source(Box::new(FixedTime(START)))
        .validation_token
        .lifetime(lifetime);
    let mut pair = Pair::new(Default::default(), server_config);
    let client_config = pair.add_client_config(client_config());
    let (client_ch, _server_ch) = pair.connect_with_shared(client_config);
    pair.client
        .connections
        .get_mut(&client_ch)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);

    pair.server.handle_incoming = Box::new(|incoming| {
        assert!(incoming.remote_address_validated());
        assert!(incoming.may_retry());
        IncomingConnectionBehavior::Accept
    });
    let (client_ch_2, _server_ch_2) = pair.connect_with_shared(client_config);
    pair.client
        .connections
        .get_mut(&client_ch_2)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);

    advance_server_clock(&mut pair, lifetime + Duration::from_secs(1));

    pair.server.handle_incoming = Box::new(|incoming| {
        assert!(!incoming.remote_address_validated());
        assert!(incoming.may_retry());
        IncomingConnectionBehavior::Accept
    });
    let (client_ch_3, _server_ch_3) = pair.connect_with_shared(client_config);
    pair.client
        .connections
        .get_mut(&client_ch_3)
        .unwrap()
        .close(pair.time, VarInt(42), Bytes::new());
    pair.drive();
    assert_eq!(pair.client.known_connections(), 0);
    assert_eq!(pair.client.known_cids(), 0);
    assert_eq!(pair.server.known_connections(), 0);
    assert_eq!(pair.server.known_cids(), 0);
}

/// The wall-clock time the token tests start from: fixed, so no test reads the host's clock.
const START: SystemTime = SystemTime::UNIX_EPOCH;

/// A wall clock stopped at one time. The server's configuration owns it; a test moves the
/// server's time on by replacing it there (`advance_server_clock`), where upstream shared one clock
/// between the test and the configuration behind a lock.
pub(super) struct FixedTime(pub(super) SystemTime);

impl TimeSource for FixedTime {
    fn now(&self) -> SystemTime {
        self.0
    }
}

/// Moves the clock of the server's current configuration `by` on, keeping its keys.
fn advance_server_clock(pair: &mut Pair, by: Duration) {
    let config = pair.server.endpoint.server_config_mut().unwrap();
    let now = config.time_source.now();
    config.time_source(Box::new(FixedTime(now + by)));
}
