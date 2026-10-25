# hyper-quic: quinn-proto 0.11.18, conformed

Source: `vendor/quinn-proto`, the published crates.io archive. See its `VENDORED.md` for the
checksum and upstream commit. Every change from those sources is listed here, in order.
Upstream's own tests are the oracle that conformance changed no behaviour.

## 1. One build path (2026-10-01)

- **Package.** Renamed to `hyper-quic`. The manifest is upstream's `Cargo.toml.orig` with the
  workspace inheritance replaced by explicit versions.
- **Crypto.** rustls on AWS-LC is always on: the provider mantle and focal vendor, and the one
  slates has queued.
  - Removed: the `ring` provider, the `rustls-ring` default, `platform-verifier`, the FIPS
    features, the `__rustls-post-quantum-test` feature, and the wasm target dependencies. Their
    source paths are deleted, not compiled out.
  - `ClientConfig::with_platform_verifier` and `try_with_platform_verifier` are gone; a consumer
    supplies roots through `with_root_certificates`.
- **Address-validation tokens.** `bloom` is always on. NEW_TOKEN tokens are issued (`sent: 2`) and
  their reuse is detected by `BloomTokenLog`. RFC 9000 §8.1.4 says a server SHOULD protect
  against replay of these tokens; with bloom off, quinn sends none.
- **qlog.** On by default (the `qlog` feature, §20). Whether a connection emits it is runtime
  configuration (`QlogConfig`).
- **`arbitrary`.** Kept as a feature: fuzzing derives, never in a shipped build.
- **Oracle.** 303 unit tests and 3 doctests pass. That is upstream's suite under this one
  configuration; the ring default ran 296 unit tests because the bloom-gated token tests were
  off.

## 2. Configuration without `Arc` (docs/transport.md §3.1)

1. **Congestion control is the closed enum `congestion::Congestion`, carried by value.**
   - `ControllerFactory` and `TransportConfig::congestion_controller_factory` are gone; use
     `TransportConfig::congestion`.
   - Each controller owns its config.
2. **A connection owns its `TransportConfig`**, which is now `Clone` plain data.
   - `ServerConfig::transport` and `ClientConfig`'s transport hold it by value.
   - A connection keeps only `grease_quic_bit` from the endpoint's configuration.
3. **qlog belongs to the connection.**
   - `QlogStream` owns its streamer, with no `Arc<Mutex<_>>`.
   - It is passed to `Endpoint::connect` and `Endpoint::accept`; `TransportConfig::qlog_stream`
     is gone.
   - `QlogConfig::into_stream` returns `Result<QlogStream, QlogError>`. A missing writer or a
     failed header write used to be logged and dropped.
4. **The endpoint owns its `EndpointConfig` by value.**
   - The reset key is AWS-LC's `hmac::Key`; the provider is no longer selectable.
   - The CID-generator factory (`Arc<dyn Fn>`) is replaced by a generator value. Each endpoint
     takes a copy through the new required method `ConnectionIdGenerator::clone_box`.
   - One behavioural difference: two endpoints built from one configuration with the default
     `HashedConnectionIdGenerator` now share its hash key, where upstream's factory drew a key per
     endpoint. The key only lets an endpoint cheaply reject CIDs it did not issue, and upstream
     already documents sharing it (`from_key`) to keep CIDs valid across restarts.
5. **Address-validation tokens have single owners.**
   - The endpoint owns the server's `TokenLog` (`Endpoint::set_token_log`; default
     `BloomTokenLog`) and the client's `TokenStore` (`Endpoint::set_token_store`; default
     `TokenMemoryCache`).
   - Both traits take `&mut self`, and both default implementations lose their `Mutex`.
   - `ValidationTokenConfig::log` and `ClientConfig::token_store` are gone.
   - A client connection that receives NEW_TOKEN reports it as an endpoint event
     (`EndpointEventInner::NewToken`), and the endpoint stores it. A connection's initial token is
     taken from the store at `connect`.
   - Two upstream test helpers were changed to match: `server_config_with_cert` no longer installs
     a test log, which existed only for builds without `bloom`; `use_same_token_twice` installs its
     store on the client endpoint.
6. **TLS is `hyper-tls`.** `rustls` is now a Cargo rename of `hyper-tls` (`crates/hyper-tls`), so the
   source still refers to `rustls::`. `HandshakeData::negotiated_key_exchange_group` is always
   present, as an `Option`; upstream gated it behind a test feature and filled it with `.expect`.
7. **The crypto provider is borrowed for `'static`.** Every TLS configuration uses
   `rustls::crypto::aws_lc_rs::DEFAULT_PROVIDER`, where upstream built a fresh
   `Arc<CryptoProvider>` per configuration. The classical-key-exchange test client keeps its
   provider in a `LazyLock`.

8. **TLS configuration lives in the endpoint's `Configs` slab** (`config/configs.rs`).
   - The endpoint owns every server and client configuration in a slab of
     `EndpointConfig::config_slots` slots (default 4: one current configuration per side and its
     replacement while it is replaced; §19 states what holds a replaced one).
     `ServerConfig` holds its crypto configuration, handshake-token key and time source as
     `Box`es, and `ClientConfig` its crypto configuration and initial-CID provider; neither is
     `Clone` any more.
   - A connection and a pending `Incoming` hold a generation-checked key. The endpoint counts
     them per slot: up when an attempt is surfaced or a connection created, down when the attempt
     is accepted, refused, retried or ignored and when a connection's `Drained` event arrives. A
     superseded configuration is dropped when its count reaches zero; a slot whose generation
     counter is exhausted is never reused. A full slab refuses the new configuration with
     `ConfigsFull`.
   - Endpoint API: `Endpoint::new` takes `Option<ServerConfig>` by value;
     `set_server_config(Option<ServerConfig>) -> Result<(), ConfigsFull>` supersedes the current
     one; `insert_client_config`/`retire_client_config` and
     `insert_server_config`/`retire_server_config` manage the others; `connect` takes a
     `ClientConfigHandle` (connections made with one handle share its TLS session cache, so a
     later connection resumes); `accept` takes an optional `ServerConfigHandle`; `configs()` and
     `configs_mut()` lend the slab. `ConnectError::UnknownConfig` is new.
   - Connection API: `handle_event(event, &mut Configs)` and
     `poll_transmit(now, max_datagrams, buf, &Configs)`. A datagram can carry CRYPTO data, and a
     TLS read can change the configuration's stores (session cache, ticketer, key log), so that
     call borrows mutably; this deviates from §3.1's `&Configs`. A transmit can carry NEW_TOKEN,
     which reads the token key and time source. `handle_timeout` needs no configuration.
   - The values a connection read from its `Arc<ServerConfig>` without a TLS step (`migration`,
     whether a preferred address was sent, `ValidationTokenConfig::sent`) are copied into the
     connection at accept.
   - The crypto traits borrow: `crypto::ClientConfig::start_session(&mut self, …)`,
     `crypto::ServerConfig::start_session(&self, …)`, and
     `Session::read_handshake(&mut self, SessionConfig<'_>, buf)`, where `SessionConfig` lends
     the client's or server's crypto configuration for the call. Both configuration traits are
     `Any`, so the rustls session recovers its own `QuicClientConfig` or `QuicServerConfig`; a
     configuration of the wrong side or implementation is a typed `INTERNAL_ERROR`, unreachable
     through the endpoint. `QuicClientConfig` and `QuicServerConfig` own their rustls
     configuration; `with_initial` and `TryFrom` take it by value.
9. **`TokenMemoryCache` owns each server name once.** Upstream shared it between the lookup map
   and the LRU entry through `Arc<str>`. The map now owns it (`Box<str>`); evicting the least
   recently used name finds its map entry by slot, a scan bounded by `max_server_names` and paid
   only when a new name evicts. Storing a token for a known name no longer allocates a name.
10. **Tests.** Only the API changed in them; every assertion is unchanged.
    - The harness's `connect_with`/`begin_connect` insert a configuration per connection and
      retire it at once; `add_client_config` with `connect_with_shared`/`begin_connect_shared`
      serve the tests that reused one configuration through `clone` (0-RTT, tokens).
      `zero_rtt_rejection` changes ALPN through `Configs::client_config_mut` where upstream used
      `Arc::get_mut`.
    - `pending_incoming_can_retry_after_disabling_server` reused one `ServerConfig` through
      `clone`, sharing its token key. It now builds two configurations around one token key, so
      the second still validates the first's retry token.
    - The fake time source is a clonable handle (`Arc<Mutex<SystemTime>>`, test code).
    - New: four unit tests of the slab (refusal when full, reclamation after the last user,
      immediate reclamation when unused, a current configuration kept without users), and
      `tests/handshake.rs`: two endpoints driven through the public API complete a handshake and
      exchange data both ways, and allocations per handshake are counted.
    - Oracle: 313 unit tests (309 and the 4 new) and 3 doctests pass, and `tests/handshake.rs`'s
      2 tests.
11. **Wire behaviour and allocations.** `tests/handshake.rs` with each datagram's size printed,
    run at 526c2cc (adapted to its `Arc` API) and here: the same 63 datagrams with the same sizes
    in the same order, across a warm-up, a full and a resumed handshake. Allocations per
    handshake, client and server together, macOS aarch64 development machine, debug profile,
    three runs each (reallocations vary by ±3 in both):

    | Handshake | 526c2cc | Now |
    |---|---|---|
    | Full | 505 allocs, ~174,300 B | 494 allocs, ~174,100 B |
    | Resumed | 511 allocs, ~129,880 B | 501 allocs, ~129,530 B |

## 3. Connection attempts (2026-10-01)

hyper-tls prefers X25519MLKEM768, as Chromium and Firefox now do. Its 1,184-byte key share
(draft-ietf-tls-ecdhe-mlkem) spreads a ClientHello over two Initial datagrams, and running upstream's
suite on it exposed two defects in quinn-proto 0.11.18's server:

- **An attempt reached the application before its payload was authenticated.** `handle_first_packet`
  removed header protection and surfaced an `Incoming`; only `accept` decrypted the payload. A
  forged Initial with a garbage payload therefore became a connection attempt, and its token was
  spent in the token log first.
- **A straggler became a second attempt.** When the server answered a two-datagram ClientHello's
  first datagram with Retry, the second datagram arrived afterwards for a CID with no state. It was
  surfaced as a new `Incoming` with half a ClientHello and an already-spent token. One client
  attempt produced two decisions, and possibly a second Retry. Upstream's own `use_token_then_retry`
  fails on this.

The fix:

- `handle_first_packet` authenticates the payload before anything else acts on the packet. `accept`
  no longer decrypts again.
- An attempt surfaces only with the first byte of its ClientHello: CRYPTO data at offset 0
  (RFC 9001 §4.1.3).
- An authenticated Initial carrying more of a ClientHello is held per initial CID (`Endpoint::held`).
  When the start arrives, the held datagrams join the attempt's buffered datagrams. A straggler
  after Retry is never joined; it expires three probe timeouts after arrival. That is RFC 9000
  §10.2's allowance for stray packets after state ends, with the PTO computed by RFC 9002 §6.2.1
  from the server's configured initial RTT.
- Held entries count toward `max_incoming` and the incoming byte limits.
- An Initial carrying only a CONNECTION_CLOSE, or no valid frames, before any ClientHello is
  dropped, because no handshake can follow it.

Tests:

- `tests::first_flight` (new) covers: post-quantum negotiation; the two-datagram ClientHello; a
  retried attempt surfacing once, with the straggler expiring; a reversed ClientHello surfacing once
  with no time passing and no loss; a forged payload surfacing nothing and buffering nothing; and a
  first flight that fills but never exceeds three times the bytes received, using a certificate
  whose random names compression cannot shrink.
- Upstream tests changed:
  - `server_can_send_3_inital_packets` and the two `zero_rtt_incoming_buffer_size` tests use a
    classical-key-exchange client (`client_config_classical`). Their expected counts derive from a
    one-datagram ClientHello. The post-quantum layout is covered by the new tests.
  - `instant_close_1` now asserts that the server surfaces and keeps nothing. Upstream surfaced the
    close as an attempt and then lost the connection.
  - `known_connections`' bookkeeping assertion excludes held Initials, which share the
    initial-CID routing table but are not connections.

Result: 309 unit tests (upstream's 303 and 6 new) and 3 doctests pass.

## 4. No panics in shipped code (2026-10-01)

The crate takes the workspace's lint table (`[lints] workspace = true`; its own `[lints.rust]`
and the crate-root `allow(clippy::cognitive_complexity)` are gone): no `unwrap`, `expect`,
`panic!`, `unreachable!` or assert family, no indexing or slicing that can go out of bounds, no
overflowing arithmetic, no narrowing `as`, `cognitive_complexity` at most 10, every `const`
documented. No item or crate in shipped code allows any of them; test code opts out at the crate
root (`#![cfg_attr(test, allow(...))]`, the same list as hyper-tls) and at `tests/handshake.rs`'s.

### Classes of change

1. **Lookups that cannot miss, by construction.** The packet number spaces are a `Spaces`
   struct reached by `SpaceId` (`get`/`get_mut`), not `[PacketSpace; 3]` indexed by `space as
   usize`; the per-direction stream state is `PerDir<T>`; the timer table is destructured by
   `Timer`; BBR's gain cycle by its offset. The endpoint's `Index<ConnectionHandle> for
   Slab<ConnectionMeta>` is gone: each lookup is `get`/`get_mut`/`try_remove`, and an event for a
   connection the endpoint no longer knows is ignored.
2. **Fallible signatures where a failure has a caller** (each an API change):
   - `Endpoint::new` returns `Result<Self, RngUnavailable>`: the system RNG failing to seed it
     was an `expect`.
   - `Endpoint::refuse` returns `Option<Transmit>`, `None` where the close cannot be protected.
   - `ClientConfig::with_root_certificates` returns `ClientConfigError` (the verifier's error, or
     the TLS configuration's: no TLS 1.3, no initial cipher suite), `QuicClientConfig::new` and
     `QuicServerConfig::new` return the `rustls::Error`; upstream `expect`ed both.
   - `ConnectError::InvalidTlsConfig` is new: a client TLS configuration that cannot start a QUIC
     session (one without TLS 1.3, which `TryFrom<rustls::ClientConfig>` does not check) was an
     `unwrap` in `start_session`.
   - The crypto traits: `crypto::ServerConfig::start_session` returns
     `Result<Box<dyn Session>, TransportError>` and `retry_tag` `Result<[u8; 16], CryptoError>`;
     `HeaderKey::encrypt`/`decrypt` and `PacketKey::encrypt` return `Result<(), CryptoError>`;
     `HandshakeTokenKey::aead_from_hkdf` returns `Result<Box<dyn AeadKey>, CryptoError>`.
   - `StreamId::new` returns `Option`: an index at `MAX_STREAM_COUNT` or past it has no ID.
   - Internal: `PacketNumber::new` and `PacketSpace::get_tx_number` return `Option`;
     `PacketBuilder::finish` returns `Option`; `PartialEncode::finish` returns `Result`;
     `Connection::write_crypto`, `upgrade_crypto`, `update_keys` and `set_peer_params` return
     `Result<_, TransportError>`; `Token::encode` returns `Option`;
     `AckFrequencyState::next_sequence_number` returns `Option`.
3. **The connection's own invariants, as typed errors.** Where the connection finds its own
   state inconsistent (no keys for a space it is sending in, the 1-RTT keys without the next
   phase's, keys handed over after the last space, the 0-RTT outcome unknown after the
   handshake) it returns `TransportError::INTERNAL_ERROR`, or kills the connection with it where
   the caller cannot return one (`PacketBuilder`). A packet being built when this happens is
   taken back out of the buffer: nothing unprotected is sent.
4. **Checked arithmetic and `get`.** Every offset and length the peer influences is checked:
   packet and header decoding (the payload length, the token's position, the packet number),
   frame decoding (ACK ranges, lengths, CIDs), transport parameters (lengths, the reserved
   parameter), CRYPTO data's end (RFC 9000 §19.6: past 2^62 - 1 is a FRAME_ENCODING_ERROR),
   header protection's sample, Retry integrity, stateless reset tokens.
5. **Saturation where it is the stated meaning**, each with a comment: statistics counters
   (`ConnectionStats`, ECN counts); byte counts and offsets bounded by an in-memory buffer;
   required sizes, which can only over-state and so write less (`fits`, `min_size`,
   `NewToken::size`); congestion windows, byte totals and BBR's bandwidth samples, which saturate
   at a size no connection reaches; durations (PTO, RTT estimates, delays), which can only
   lengthen. A deadline past what `Instant` represents is never reached: the timer is not armed
   (idle, keep-alive, key discard, path validation, MTU reactivation), except the close timer,
   which then fires at once so that a closing connection cannot be held forever.
6. **Float to integer without `as`**: `float::saturating_u64`/`saturating_u32` give `as`'s
   meaning (fraction dropped, saturating, NaN to zero), and `float::scale_duration` gives
   `Duration::mul_f32`'s without its panic; tests check them against `as` and `mul_f32`.
7. **Debug-only assertions removed** where they checked an internal precondition the callers
   hold (packet builder sizes, GSO alignment, ACK-only writes, `discard_space`'s space, upgrade
   order, `handle_first_packet`'s side, CID sequence ordering, MTU discovery's phase). Release
   builds never evaluated them, so no behaviour changes; `SentFrames::is_ack_only`, used only by
   one of them, is gone.
8. **Restructured so the type system proves the invariant**: `set_key_discard_timer` starts
   from `now`, which every caller had made the previous keys' end time (upstream `expect`ed
   both the previous keys and their end); `PacketNumber::decode` takes the header's first byte,
   whose two low bits are the length, instead of a length upstream matched with
   `unreachable!`; `LongHeaderType::from_byte` matches the two type bits exhaustively;
   `TransportParameterId::try_from` finds the ID among `SUPPORTED`; `get_or_insert_recv`
   re-opens a freed stream by construction; the `IncomingImproperDropWarner` is dismissed by a
   flag instead of `mem::forget`.
9. **Split for `cognitive_complexity`** into named steps, behaviour unchanged:
   `poll_transmit` (`close_pending`, `queue_ack_frequency`, `fill_spaces`, `fill_step`,
   `start_datagram`, `congestion_blocks`, `finish_for_next_datagram`, `allocate_datagram`,
   `write_close`, `encode_close`, `send_off_path_response`, `finish_last_packet`,
   `write_mtu_probe`, over a `TransmitState`), `populate_packet` (`write_signal_frames`,
   `write_ack_frequency`, `write_path_frames`, `write_crypto_frames`, `write_cid_frames`,
   `write_datagram_frames`, `write_new_tokens`), `handle_packet` (`packet_result`,
   `on_authentication_failure`, `on_decrypted_packet`, `drops_authenticated`, `after_packet`,
   `state_after_error`), `process_decrypted_packet` (`process_established`,
   `process_while_closed`, `process_retry`, `process_handshake_packet`,
   `on_client_handshake_complete`, `process_initial_packet`, `process_version_negotiate`),
   `process_payload` (`process_data_frame`, `process_stream_frame`, one handler per frame
   kind, `after_payload`), `on_ack_received` (`record_largest_acked`, `on_packets_acked`,
   `update_rtt`, `on_ack_ecn`), `detect_lost_packets` (`find_lost_packets`, `on_packets_lost`),
   `handle_timeout` (`on_timer`), `Endpoint::handle` (`negotiate_version`, `buffer_incoming`,
   `handle_unknown`), `handle_first_packet` (`admit_first_packet`,
   `authenticate_first_packet`), `TransportParameters::write` (`write_param` and one writer per
   kind), `StreamsState::write_control_frames` and `write_stream_frames`, `RecoveryMetrics`.
10. **Bounded loops**: `Endpoint::new_cid` draws at most 155 CIDs, `(3/4)^155 < 2^-64` given
    `cids_exhausted` keeps a quarter of short CID spaces free; a generator that keeps giving
    used CIDs refuses the connection (`CidsExhausted`) instead of spinning. The STREAM copy loop
    stops on an empty read instead of spinning.
11. **Every `const` in `src/` carries a `///`** with its derivation or citation. Upstream's
    tunables keep upstream's values and say so. `scripts/check-contracts.py` now treats a file
    a `#[cfg(test)] mod name;` declares as test code, as it treated an inline `#[cfg(test)] mod`,
    and no longer stops scanning the declaring file at that line.
13. **No `allow(missing_docs)`**: each `FrameStats` field names its frame type, where upstream
    allowed the struct undocumented.
12. **Tests**: `tests/handshake.rs`'s `unsafe` counting allocator is replaced by
    `hyper_measure::alloc::Counting`; upstream tests changed only where the API did
    (`.unwrap()` on the new `Result`s and `Option`s) and in one MTU discovery test (below).

### Behaviour changes: former panics and what they are now

| Reached by | Upstream | Now |
|---|---|---|
| **Local configuration and data**: an initial MTU above 16 KiB with 0-RTT data (any long header packet larger than its two-byte length field) | `assert!` in `PartialEncode::finish`, active in release builds | the packet builder caps each long header packet at what the field holds (`PartialEncode::length_limit`); the data goes in the next packet (`zero_rtt_long_header_packets_fit_their_length_field`, which panics on upstream's code) |
| The API: `recv_stream` on a stream this side opened unidirectionally, `send_stream` on one the peer did | `assert!` | every operation fails with `ClosedStream` (`stream_used_against_its_direction_is_closed`) |
| The API: `poll_transmit` with `max_datagrams == 0` | `assert!` | `None`, nothing sent (`poll_transmit_with_no_datagrams_sends_nothing`) |
| The API: `Endpoint::new` with the system RNG unavailable | `expect` | `RngUnavailable` |
| The API: an `Incoming` from another endpoint passed to `accept`/`refuse`/`retry`/`ignore` | slab `remove` panicked | `accept` refuses it (`INTERNAL_ERROR`), `retry` hands it back, `refuse`/`ignore` release no buffer or configuration of this endpoint's |
| The API: a client TLS configuration without TLS 1.3 | `unwrap` at `connect` | `ConnectError::InvalidTlsConfig` |
| The API: a CID generator that keeps giving used CIDs | an endless loop | the connection is refused (`CidsExhausted`) |
| The API: a 4-byte CID on a 32-bit target | `cids_exhausted` overflowed `usize` | counted in `u64` |
| The API: `mtu_discovery_config` interval or black-hole cooldown, `keep_alive_interval` or `max_idle_timeout` past `Instant`'s range | `Instant + Duration` panicked | never reached: not armed |
| The API: `time_threshold` negative or not finite | `Duration::mul_f32` panicked | a loss delay of the timer granularity, the floor upstream applies (`float::scale_duration` gives zero) |
| A peer, after 2^31 unacknowledged packets of a space | `panic!("packet number too large to encode")` | the connection is killed (`INTERNAL_ERROR`), nothing more sent (`packet_number_beyond_any_encoding_is_refused`) |
| The connection's own packet numbers reaching 2^62 | `assert!` | killed without sending anything further, as RFC 9000 §12.3 requires |
| A peer's Initial token or payload length past `usize` (32-bit targets) | truncated by `as` | `InvalidHeader`, the packet dropped |
| The peer's parameters arriving during an MTU search | `debug_assert!` (debug builds only) | recorded; upstream's `should_panic` test now asserts that (`mtu_discovery_with_peer_max_udp_payload_size_during_search_records_it`) |
| A forged or unexpected Retry with no ClientHello kept | `unwrap` | the Retry is discarded |
| The connection's own state inconsistent (class 3) | `unwrap`, `expect`, `unreachable!` | `INTERNAL_ERROR`, unreachable by construction |
| Every other site | `unwrap`, `expect`, assert, indexing, overflow | a typed refusal, unreachable by construction |

No panic in upstream's receive path was found reachable from the wire on a 64-bit target: each
peer-influenced quantity upstream did not check is a varint below 2^62 added to another below
2^16, or was checked before the site (the header protection sample length, the ACK ranges by
`scan_ack_blocks`, the transport parameters' bounds). Those sites are checked now regardless.

### Oracle

319 unit tests pass: upstream's 309 (§3), `float`'s 2, and the new
`zero_rtt_long_header_packets_fit_their_length_field`, `stream_used_against_its_direction_is_closed`,
`poll_transmit_with_no_datagrams_sends_nothing` and `packet_number_beyond_any_encoding_is_refused`,
with upstream's `mtu_discovery_with_peer_max_udp_payload_size_during_search_panics` turned into
the test of what now happens instead. `tests/handshake.rs` 2 and the 3 doctests pass.

### Allocations per handshake

`tests/handshake.rs`, client and server together, macOS aarch64 development machine, debug
profile, three runs each; the counts before are the same test at 3fa28ed (this section's base)
with its own counting allocator, after with `hyper_measure::alloc`, which counts the same calls:

| Handshake | Before | Now |
|---|---|---|
| Full | 494 allocations, 55–58 reallocations | 494 allocations, 56–58 reallocations |
| Resumed | 501 allocations, 54–58 reallocations | 501 allocations, 53–56 reallocations |

No path gained an allocation. A first draft built `TransportError`s, whose reason is a `String`,
eagerly in `ok_or(...)` on the success path, which cost 9–10 allocations a handshake; they are
built only on failure (`ok_or_else`).

## 5. What the application layer reads (2026-10-01)

Three items made public for hyper-transport (mantle note 32 §3.4); no behaviour changes.

- **Patch Q5: the assembler's span limit.** `MAX_CHUNKS` (1,024, `connection/assembler.rs`), past
  which a stream's out-of-order spans close the connection ("too many gaps in stream buffer"), is
  public and re-exported as `hyper_quic::MAX_STREAM_CHUNKS`, so that a stream window is derived from
  it instead of from a literal (node.md §3.3: a window of at most `2 · 1,024 · d` cannot reach it).
- **`Streams::write_limit`.** The stream bytes the connection may still take from the application
  now: the peer's connection credit not yet spent and room in the send window, whichever is less
  (the existing `StreamsState::write_limit`). quinn keeps one connection window, so an application
  that keeps a credit reserve for its more urgent classes (note 32 T16) needs to see it.
- **`Connection::peer_max_ack_delay`.** The peer's `max_ack_delay` transport parameter (RFC 9000
  §18.2; the default before its parameters arrive), which the connection already computed for its
  own timers. hyper-transport judges an exchange by whether its period heard the peer, so it
  refuses a period no longer than this (`Endpoint::open`, `Refusal::Configuration`).

## 6. The host clock, stated (2026-10-02)

The workspace denies `Instant::now`, `SystemTime::now`, thread spawns and environment reads
(`clippy.toml`, `docs/sim.md` §3.9). Upstream reads the host clock in two shipped places, each kept
and allowed at the site with its reason; no behaviour changes:
- `config/mod.rs`, `StdSystemTime::now`: the default `TimeSource` of address-validation tokens,
  which an owner replaces through `ServerConfig`.
- `config/transport.rs`, `QlogConfig::default`: qlog's start time, taken when a qlog configuration
  is made (the `qlog` feature).

## 7. Allocations after hyper-tls lends its TLS 1.2 sessions (2026-10-03)

No change here. hyper-tls's session store now moves a server name in where it cloned it, and a
spent TLS 1.3 ticket's certificate chain moves into the connection where it was copied
(`crates/hyper-tls/VENDORED.md` §5). `tests/handshake.rs`, debug profile, this machine: 492
allocations a full handshake (494 before), 497 a resumed one (501 before), against 526c2cc's 505 and
511.

## 8. End to end, against upstream (2026-10-03)

`tests/e2e.rs` (`harness = false`): real processes over real UDP sockets on loopback (CLAUDE.md
§1a). The test binary is the client and spawns itself as the server, as a relay and, for one
scenario, as the client, each a process that shares nothing with it but the kernel's sockets.
Every scenario runs three ways: hyper-quic on both sides, a hyper-quic client against an upstream
quinn-proto 0.11.18 server, and an upstream client against a hyper-quic server (the
`upstream-quinn-proto` dev-dependency, unmodified, on rustls 0.23.45 with AWS-LC; slates'
`docs/wip/transport-quic.md` §4, stage 5). A killed peer runs where hyper-quic is the survivor.
The scenarios follow the QUIC interop runner's cases (quic-interop-runner `testcases_quic.py` at
`740c05a`):

| Scenario | What it does and checks |
|---|---|
| `handshake` | a full handshake with a mebibyte each way, then a resumed one whose stream takes its bytes before the handshake ends: the client's `accepted_0rtt` and the server's 0-RTT keys both say the early data was taken (`resumption`, `zerortt`) |
| `streams` | three bidirectional streams at once, 2, 3 and 5 MiB each way (`transfer`'s files) |
| `lossy` | 2 MiB each way through a relay process that drops one full-size datagram in fifty in each direction (`transferloss`'s 2 %) and holds one in fifty until the next in its direction has passed; the relay reports its drops and holds, and both senders report packets lost and recovered |
| `migration` | 2 MiB each way while the client moves to a new socket mid-transfer and tells its connection (RFC 9000 §9.5: a new connection ID on the new path); the server sends PATH_CHALLENGE on the new path and follows the client to it, the client answers and retires a connection ID (`connectionmigration`) |
| `rebinding` | the same with the client's socket replaced unannounced, as a NAT rebinding does (RFC 9000 §9.3, `rebind-port`) |
| `killed-server` | the server process is killed with SIGKILL mid-upload; the client's connection ends `TimedOut` no sooner than its idle timeout after the server's last datagram (RFC 9000 §10.1); a new server process answers: the client resumes the killed server's session and opens its stream on 0-RTT keys, the new process, holding none of the killed one's sessions, rejects the early data, and the client opens the stream again on 1-RTT keys (RFC 9001 §4.6.2) |
| `killed-client` | the client process is killed mid-upload; the server's connection ends the same way, and the server serves the next client |

Every stream carries a pattern no lost, duplicated or misplaced byte can match, checked byte by
byte on both sides. A process is killed mid-stream on a fact: the server once a stream has brought
it a mebibyte, the client once flow control (RFC 9000 §4.1) shows the server has taken a stream
window of its upload.

Every wait is for a fact: a datagram, a timer the connection set, or an event it reports. A wait
ends when its fact holds or when the connection reports its end; the quiet rule (as
`hyper_raft_e2e::quiet`, in the protocol's terms) fails it once no stream byte has moved for the
connection's idle timeout of listening, 30 s (`TransportConfig`'s default; RFC 9308 §3.2 finds
shorter timeouts make transient interruptions harder to survive): QUIC itself ends a connection
that hears nothing that long, so one that hears its peer and moves nothing that long is stuck.
Only time the driver spent listening on its socket counts, so a starved process does not count its
starvation as the peer's silence. A wait for a killed peer's end is the connection's own idle
timer; its failure guard is a second idle timeout of listening.

Results, debug profile, this machine, four runs of all 19 at load 21–46 (other sessions building
and testing), every one passing: a full and a 0-RTT handshake with a mebibyte each way in 0.33–0.56
s; 10 MiB each way on three streams in 0.47–1.30 s; through the relay 0.28–0.56 s, the relay
dropping 30 and holding 30 of about 1,515 full-size datagrams each way and the senders recovering
30 losses each (31 once); a migration or rebinding in 0.23–0.31 s, the server sending two
PATH_CHALLENGE frames (three twice) and the client answering, and retiring a connection ID after an
announced move; a killed server timed out 30.050–30.062 s after its last datagram, a killed client
30.001–30.029 s after its own. Earlier runs found four defects in the harness itself, each fixed
before these: a connection forgotten as it drained before its end was read (the killed scenarios
waited for an end already gone), a server that took its client's stream mark for the end of its
connection, and two hooks that read a stream before the server's limits let it open (one run in
four at load 58).

## 9. The handshake at a geographic distance (2026-10-04)

slates reported a two-datagram X25519MLKEM768 ClientHello that failed at 500 ms one way.
`tests/geo.rs` runs the handshake on hyper-sim's network at that delay, then again with 5% loss and
±100 ms of reordering over 32 seeds, each checked exactly (`docs/transport.md` §4e). Six departures
from upstream came out of it, each a defect with a test that failed before its fix:

1. **The attempt bound counted one attempt twice** (`Endpoint::admit_first_packet`). A ClientHello
   whose first datagram is lost has its second held. The retransmitted flight's first datagram then
   found the held entry counted against `max_incoming`, so with room for one attempt the attempt
   was refused until the held entry expired, three PTOs later. A held entry for the same
   destination CID is now this attempt's own.
2. **The anti-amplification limit is exact** (RFC 9000 §8.1). Upstream let a full datagram go
   whenever one byte of allowance was left (quinn-rs/quinn#1082), so its first flight was 7,248
   bytes against a 7,200-byte limit. A datagram is now cut to the allowance. A datagram that could
   not hold its packet is not begun: for an ack-eliciting Initial that is 1,200 bytes (§14.1),
   otherwise the smallest packet. A padded PATH_CHALLENGE datagram is padded only as far as the
   allowance permits (§8.2.1). MTU probes wait for the path's validation; upstream sent them past
   the limit after a migration.
3. **A PTO probes every handshake space with data** (RFC 9002 §6.2.4, "the peer might only have
   receive keys for one of the two packet number spaces"). Upstream probed only the space whose
   timer fired. A server whose ServerHello was lost held Handshake packets in flight that the client
   could not decrypt. It probed those until the idle timeout while the lost Initial waited,
   congestion-blocked, behind them. Three of 32 lossy seeds never connected.
4. **One packet past the window on entering recovery** (RFC 9002 §7 and §7.3.2). The
   retransmission of a lost packet goes without waiting for in-flight bytes the peer may be unable
   to acknowledge.
5. **Packets ahead of their keys are held, not dropped** (RFC 9001 §4.1.4: an endpoint "SHOULD
   buffer received packets if they might be processed using keys that are not yet available").
   Upstream dropped a Handshake packet that overtook the Initial carrying the ServerHello. The
   server then resent it after loss detection, a round trip or more later: about 2.9 s where 2 s
   suffices with reordering and no loss. Held bytes are bounded by `crypto_buffer_size`, and past
   the bound a packet is dropped as before.
6. **Discarding keys resets the PTO backoff** (RFC 9002 §6.2.2, Appendix A.11's `pto_count = 0`).
   Upstream carried the count past the discard, so a client whose Initial PTO had fired probed a
   lost Finished after twice the PTO.

## 10. TLS between nodes: hybrid post-quantum groups and 256-bit suites only (2026-10-04)

The owner approved restricting the TLS hyper-quic builds to post-quantum key exchange and 256-bit
suites. `crypto::rustls::node_provider()` is aws-lc-rs's provider with two lists restricted. Every
configuration hyper-quic builds uses it, and so does hyper-transport's mutual TLS:

- **Key exchange.** X25519MLKEM768 first, whose share a ClientHello carries, then
  SecP256r1MLKEM768 (draft-ietf-tls-ecdhe-mlkem). Upstream also offered X25519, P-256 and P-384.
- **Cipher suites.** TLS13_AES_256_GCM_SHA384, then TLS13_CHACHA20_POLY1305_SHA256. Both have
  256-bit keys. TLS13_AES_128_GCM_SHA256 is no longer offered.
- **Initial packets.** These keep AEAD_AES_128_GCM, as RFC 9001 §5.2 requires. Their suite is
  taken from aws-lc-rs's suite table (`initial_suite`), not from the suites a configuration offers.
  Upstream refused a configuration whose provider lacked AES-128-GCM.
- **What a peer sees.** A classical-only peer, or one offering only AES-128-GCM, shares nothing
  with a node. It is refused with a TLS handshake_failure alert (CRYPTO_ERROR 0x128, RFC 9001
  §4.8), in either role (`src/tests/node_tls.rs`).
- **What is now exposed.** `HandshakeData::negotiated_cipher_suite` reports the suite.
- **Upstream's tests.** The packet-counting tests need a one-datagram ClientHello, so they pair
  their classical client with a test-only server that accepts classical groups.
- **Interoperation.** Upstream quinn-proto, on rustls with `prefer-post-quantum`, offers
  X25519MLKEM768 and AES-256-GCM. The end-to-end interoperation suite passes unchanged.

## 11. Recovery at a geographic distance (2026-10-04)

Measured at 500 ms one way (`docs/benchmarks.md`, "Probe timeouts, tickets and Careful Resume";
sources in `docs/research/quic-overhead.md` §2). Each departure has a test that failed before it.

1. **What a probe carries** (`PacketSpace::maybe_queue_probe`). A probe with no new data to send
   retransmits the oldest in-flight packet's frames, its STREAM frames included, as Chromium,
   msquic and Cloudflare quiche do (RFC 9002 §6.2.4: "the sender MAY retransmit unacknowledged
   data"). Upstream took only the control frames, so a lost reply went again only after the probe's
   own acknowledgement declared it lost, a round trip later. The STREAM frames move off the old
   packet as its control frames do, so one sent copy of each range is tracked
   (`a_lost_reply_goes_again_in_the_first_probe_within_two_round_trips`). The black-hole test
   now sends four payloads: each arrives in its first probe, clamped to the base MTU, and each such
   loss is one suspicious burst, where upstream's one payload was lost in full-size
   retransmissions until the black hole was declared.
2. **The probe timer's variation weight is 2** (`RttEstimator::probe_base`), Chromium's
   `kPtoRttvarMultiplier`, where RFC 9002 §6.2.1 has 4. A PTO expiry declares nothing lost and
   leaves the window alone (§6.2), so an early probe costs one or two packets; at the first sample
   (rttvar = sample / 2, §5.3) it waits two smoothed RTTs, RACK-TLP's PTO (RFC 8985 §7.2), where 4
   waits three. Before any sample the timer is RFC 9002's from kInitialRtt. Persistent congestion,
   key discard and draining keep the weight of 4, so no congestion response or lifetime changes.
3. **A PTO also probes the Data space** (`on_loss_detection_timeout`), once the path has an RTT
   sample, as RFC 9002 §6.2.4 asks ("other packet number spaces with in-flight data"). A client's
   lost Finished and the request coalesced with it go again in one datagram
   (`a_lost_finished_and_request_go_again_in_one_probe`). Before a sample the timer from
   kInitialRtt fires at 999 ms against a 1 s round trip, so the server's Data probe would repeat
   0.5-RTT data that is not lost; measured on real sockets, it held a packet in flight through the
   client's first burst and raised the open loop's worst latency from 291–294 ms to 343–354 ms.
4. **A server arms its probe timer when its handshake completes**. It armed it while discarding its
   Handshake keys, still handshaking, when the Data space is skipped (RFC 9002 §6.2.1: "An endpoint
   MUST NOT set its PTO timer for the Application Data packet number space until the handshake is
   confirmed", which for a server is the handshake's completion, RFC 9001 §4.1.2). A server whose
   window was full of 0.5-RTT data, with the client's acknowledgements of it dropped before the
   handshake completed (RFC 9001 §5.7), sent nothing more and both sides idled out
   (`a_server_whose_handshake_completes_late_probes_its_full_window`).
5. **The max_ack_delay timer and the ACK bundling deadline are the Data space's**. Upstream reset
   both on an ACK sent in any space, so the Initial ACK in a server's first flight stopped the timer
   for the client's 0-RTT packet and pushed its bundled ACK a round trip out: the 0-RTT packet was
   acknowledged after the handshake, a round trip past max_ack_delay (RFC 9000 §13.2.1). On real
   sockets this alone cut the open loop's initial-window stall from 291 to 242 ms
   (`the_first_flight_acknowledges_0rtt`).
6. **Tests hold no locks.** The unit tests' address counters (`SERVER_PORTS`, `CLIENT_PORTS`, each a
   `Mutex` shared by every test) became each `Pair`'s own: its network is in memory, so its addresses
   need be distinct only from one another (`Pair::fresh_client_addr`). The token tests' clock, an
   `Arc<Mutex<SystemTime>>` shared between the test and the server's configuration, became a
   stopped clock the configuration owns and the test replaces (`advance_server_clock`, through a
   test-only `Endpoint::server_config_mut`). The capture sockets and key log the tests opened when
   `SSLKEYLOGFILE` was set went with them: they read the environment and bound real ports, the
   reason the counters were process-wide.

## 12. Careful Resume (RFC 9959, 2026-10-04)

`connection/resume.rs`; the decision and its sources are `docs/research/quic-overhead.md` §1.
Each endpoint keeps, per remote IP address, what its closed connections delivered a round trip
and their minimum RTT (`CongestionMemory`, at most `CarefulResumeConfig::remembered` addresses,
the oldest replaced; a measurement below four initial windows is not kept, §3.1). A connection
reports its measurement as it closes (`EndpointEventInner::Observed`), not when it drains three
PTOs later, so a connection made at once can use it. The next connection to that address takes it,
so no second connection starts from it at once (§3.2), and returns it unused at its close unless it
retreated. On that connection:

- **Reconnaissance**: the initial window under normal congestion control until every ack-eliciting
  Data packet sent before the first acknowledgement is acknowledged without congestion;
- **the jump**, deferred to the first time the window blocks a datagram (§3.2): the window becomes
  half the measurement (and at most `max_jump`), if that is more than it is and the minimum RTT is
  above half the saved RTT and the smoothed RTT within ten times it (§4.2.1);
- **Unvalidated**: the window held, paced at one window a smoothed RTT (§3.3), until the flight
  fills it, an acknowledgement covers its first packet, or a round trip passes; then the window
  becomes the PipeSize and Careful Resume ends, or the flight size and Validating follows;
- **Validating**: normal congestion control until the last Unvalidated packet is acknowledged;
- **Safe Retreat** on congestion after the jump: the measurement deleted, the window at most half
  the PipeSize and not grown, and on leaving ssthresh half the PipeSize (§3.5).

`Controller::set_window` and `set_ssthresh` carry the window changes; CUBIC and NewReno implement
them, and BBR and Copa, whose windows come from their models, keep the initial window. On by
default (`EndpointConfig::careful_resume`): lifetime one hour, Chromium's bound on the bandwidth
estimate it resumes from; 256 addresses, the server names `TokenMemoryCache` keeps tokens for.

Tests: the phases on NewReno (`connection::resume::tests`), a resumed mebibyte at least a round trip
sooner (`a_resumed_connection_starts_from_half_what_the_last_delivered`), and a jump onto the lossy
condition over its 32 seeds, where 7 seeds jump and all 7 retreat and every transfer completes
(`a_jump_onto_a_lossy_path_retreats_and_every_transfer_completes`).

### The upstream peer's driver order in `tests/e2e.rs` (2026-10-04)

The end-to-end loop took every datagram, fired the timers due, then sent. Upstream quinn-proto
0.11.18's `migrate` keeps the loss-detection timer armed for the old path, and fired before anything
is sent on the new path it reaches `debug_assert!(!self.peer_completed_address_validation())` in
`pto_time_and_space`: its server panicked there after a migration on the macOS Intel runner (CI run
37257976598). quinn's main at d45b513 still does not re-arm the timer in `migrate`. quinn's own
`ConnectionDriver::poll` transmits before it drives its timer, and its first send re-arms the
timer, so the upstream side now runs in that order (`Quic::TRANSMIT_BEFORE_TIMERS`), its asserts on.
hyper-quic re-arms the timer in `migrate` (17c9964, `a_rebinding_leaves_no_stale_probe_timeout`), and
its side keeps the harder order.

## 13. The handshake's flights twice, and three defects at 500 ms one way (2026-10-05)

Measured at 500 ms one way (`docs/benchmarks.md`, "hyper-quic at 500 ms one way: the handshake's
flights twice"; sources in `docs/research/quic-overhead.md` §4). Each change has a test that failed
before it.

1. **The handshake's flights go twice** (`TransportConfig::handshake_copies`, on by default;
   `PacketSpace::queue_copies`). Every Initial and Handshake packet, the 0-RTT and 0.5-RTT data sent
   while the handshake runs, and on a connection without 0-RTT the data beside the client's Finished
   are copied once, in packets of their own numbers, when their space has nothing new to send (the
   Data space's streams included, so a request is sent whole before any copy of it,
   `a_request_goes_whole_before_its_copies`), under
   the congestion window, pacing and the anti-amplification limit. A lost original is still declared
   lost and answered (RFC 9265). On the lossy condition the first reply's p90 fell from 1,899 to
   180 ms fresh and from 1,177 to 124 ms resumed, for 73% more bytes there
   (`a_lost_first_datagram_costs_no_probe_timeout_with_its_copy`; the probe-schedule tests run with
   single flights through `Scenario::copies`, and the unit tests of one flight's wire layout or of
   Retry's attempts through `single_flights`).
2. **Acknowledged stream data is never sent again** (`SendBuffer::retransmit`, `Send::retransmit`).
   A range acknowledged through one packet that carried it is not queued for another's loss, and a
   queued retransmission whose range is then acknowledged is dropped; a FIN acknowledged is not sent
   again (`acknowledged_data_is_never_sent_again`). Upstream re-sent such ranges whenever a probe's
   copy and the original were both in flight.
3. **0-RTT packets that arrive before the whole ClientHello are held** (`hold_undecryptable`,
   `awaits_0rtt_keys`): RFC 9001 §4.1.4 ("SHOULD buffer received packets if they might be processed
   using keys that are not yet available") and §5.7. Upstream dropped them, and the request went
   again a round trip later in 1-RTT
   (`a_0rtt_packet_before_the_whole_client_hello_is_held_until_its_keys`).
4. **The window counts as used for the round trip after it blocked** (`Connection::window_limited`).
   RFC 9002 §7.8 grows only a used window; upstream judged each acknowledgement by its last
   transmission alone, so a sender that filled its window, emptied its backlog and went idle took the
   rest of that round trip's acknowledgements for an unused window, and the stall came again each
   round trip. As Linux's `tcp_cwnd_validate`, the window counts as used until a packet sent after
   the block is acknowledged (`a_window_used_up_grows_by_all_the_round_trip_after_acknowledges`).
5. **The initial window follows the datagram size** (`congestion::initial_window`; NewReno and
   CUBIC). RFC 9002 §7.2: "If the maximum datagram size changes during the connection, the initial
   congestion window SHOULD be recalculated with the new size." A window still at the initial one
   with no congestion met becomes the new initial window; a configured initial window stays fixed
   (`the_initial_window_follows_the_datagram_size_until_the_window_moves`).

The geo harness's certificate is the same every run (an Ed25519 key from a fixed seed, a fixed
serial): rustls compresses certificates (RFC 8879), and a random key and serial moved the server's
flight by bytes run to run, and with it every later datagram's draw from the network.

## 14. Copies spaced past a burst, and 1-RTT packets held until the handshake completes (2026-10-05)

Measured on hyper-sim's burst-loss model in time (`docs/research/burst-loss.md`; `docs/benchmarks.md`,
"hyper-quic at 500 ms one way under bursts"). Each change has a test that failed before it.

1. **A copy of the handshake's flights waits `τ·ln(C/τ)` behind its original**
   (`connection::copies::copy_spacing`, `PacketSpace::queue_copies`, `Timer::Copies`;
   `TransportConfig::handshake_copy_burst`, 35 ms by default). `C` is the probe timer's interval
   before backoff; the spacing minimises the flight's expected delay under bursts of correlation time
   `τ` (§5 of the note), 117.3 ms at the first probe timeout and zero where the probe timeout is no
   longer than `τ`. One spacing serves every space, so a datagram's coalesced packets are copied
   together. A copy whose original is acknowledged or declared lost before it is due is never sent.
   Under the measured burst condition the fresh first reply's p90 fell from 2,749 to 394 ms
   (`under_bursts_a_spaced_copy_clears_the_burst_its_original_met`,
   `the_default_client_hello_spans_two_initial_datagrams`); under independent loss it rose from 136
   to 317 ms. The logarithm is in fixed point (Turner, IEEE SPM 2010), so a schedule is the same on
   every host.
2. **1-RTT packets that arrive while the handshake runs are held** (`hold_undecryptable`,
   `keys_ready`) and used once it completes, within the bound of the other held packets: RFC 9001
   §5.7, "Received packets protected with 1-RTT keys MAY be stored and later decrypted and used once
   the handshake is complete". Upstream discarded them (its TODO: "SHOULD buffer these to improve
   reordering tolerance"), and a request that overtook the client's Finished went again after a
   probe timeout (`a_1rtt_packet_before_the_finished_is_held_until_the_handshake_completes`;
   upstream's `handshake_1rtt_handling` now finds nothing lost).

## 15. The path's warm-up for Careful Resume (2026-10-05)

`connection/resume.rs`, `Connection::warm_up`; the decision and its sources are
`docs/research/quic-overhead.md` §5.

1. **An idle connection measures its path** (`CarefulResumeConfig::warm_up`, on by default). The
   endpoint's memory now gives a new connection a `Grant`: a measurement to resume from, the right
   to measure the path (no measurement held and no other connection measuring it), or neither. A
   connection given the right sends, once its handshake is confirmed and while idle in both
   directions (nothing waiting in its streams, no ack-eliciting packet received in the last
   smoothed round trip), PING frames padded to the path's MTU under the window and the pacer, until
   four initial windows are acknowledged in one round trip, congestion is met, or sixteen initial
   windows are spent. Its measurement goes to the endpoint as it is made
   (`EndpointEventInner::Resume`). A warm-up that fails holds its remote for the lifetime; a
   connection that closes before or during its warm-up releases it. Tests:
   `an_idle_connection_warms_its_path_up_and_a_later_burst_needs_no_second_round_trip` (geo, exact:
   a 24 kB reply on a kept connection 2,000 ms without, 1,259 ms with, the floor 1,000 ms; each
   side's warm-up within its budget), and the memory's and the warm-up's rules in
   `connection::resume::tests`.
2. **A round trip's delivery is counted by packet number** (`Observer`): from an acknowledgement to
   the first acknowledgement of a packet sent after it, as BBR counts its rounds
   (draft-ietf-ccwg-bbr-06 §5.5.1). Counted on the clock by the smoothed RTT, a path whose round
   trip was below the gap between acknowledgements never saw a window's worth delivered
   (`a_round_trip_is_counted_by_packet_numbers_whatever_the_clock`).
3. **The jump is taken only where it gains** (`CarefulResume::on_window_blocked`): in slow start
   only if it is at least twice the window or the bytes in flight and waiting in the streams fit
   in it. A jump between the two held the window a round trip where slow start doubled it, and a
   resumed mebibyte came 185 ms later
   (`a_jump_below_twice_the_window_is_taken_only_for_a_burst_that_fits_it`).
4. **The warm-up waits for the handshake's confirmation.** Sent from the client's 1-RTT keys, its
   packets went beside the client's Finished, where a server not yet holding the Finished may drop
   them (RFC 9001 §5.7); `handshake_1rtt_handling` caught it.

The unit tests that pin another mechanism's exact traffic (ACK frequency, window growth after an
unused window) run with the warm-up off (`endpoint_config_without_warm_up`): the in-memory pair's
clock moves only to the next timer, so its pacer never refills and a warm-up there spends its
budget. The MTU discovery test keeps it on and counts its PINGs apart.

## 16. Freed stream credit announced when the peer is short of it (2026-10-05)

Upstream announces a raised MAX_STREAMS only once more than an eighth of the concurrency window
has been freed since the last announcement (`queue_max_stream_id`), to save frames. A peer that
holds most of its streams open never frees an eighth: with 1,000 of a 1,024 window held, the 24
others were used once each and freed, never announced, and the peer could open no 1,025th stream
although it had 1,001 open, below the limit (hyper-transport `benches/lookup.rs`). RFC 9000 §4.6
asks the maximum to rise as streams close "to keep the number of streams available to peers
roughly consistent".

The announcement is now also made when anything is unannounced and the peer has an eighth of the
window or less left to open (what it was last told, less the streams it has opened). The eighth
is upstream's threshold, inherited and uncited; only the "peer short of credit" trigger is new
(RFC 9000 §4.6), and it reuses that line rather than adding one. Deriving the eighth is owed. Test:
`freed_stream_credit_is_announced_when_the_peer_is_short_of_it` (its second half fails on
upstream's rule; its first half holds the saving).

## 17. The copies' spacing learned per path, copies aligned, the first reply copied (2026-10-05)

`docs/research/burst-loss.md` §7–§9. Each change has a test that failed before it.

1. **The spacing learned per path.** A connection weighs what its own losses say of the path's
   bursts (`connection/losses.rs`), and its endpoint keeps the evidence per remote IP address
   (`LossMemory`, `EndpointConfig::loss_memory`: 256 addresses for an hour, as Careful Resume's
   memory, in a structure of its own). Once one hypothesis is 19 times as likely as the configured
   35 ms (Wald's ratio at errors of 5%), it replaces `TransportConfig::handshake_copy_burst`;
   independent loss sends the copies at once.
   - *Evidence.* Each lost ack-eliciting packet sent after the handshake's flights, paired with
     its space's next ack-eliciting packet (`SentPacket::next`).
   - *When a loss counts.* Once a packet of the same space, sent more than four mean deviations of
     the delay later, is acknowledged while the lost one is not. One acknowledged after its loss was
     declared counts as delivered.
   - *Bounds.* At most ten losses wait to be counted (the initial window's packets). A neighbour's
     loss is never taken as likelier than Bolot's 0.60 at 8 ms.
   - Tests: `four_neighbours_sent_with_lost_packets_and_delivered_are_independent_loss`,
     `a_packet_acknowledged_after_its_loss_was_declared_is_delivered`,
     `a_loss_counts_once_a_later_send_arrives_and_its_neighbour_is_known`,
     `a_path_learned_independent_sends_its_copies_at_once_and_one_bursty_spaces_them`.
2. **A copy is a packet of its original's frames alone** (`PacketSpace::queue_copies` copies one
   original at a time). Queued together, the retransmission queue merged their frames and packed
   them anew: the ClientHello's copies went as `[1141, 1928)` with `[0, 349)`, then `[349, 1141)`,
   so one lost copy left both halves without theirs
   (`each_copy_carries_its_originals_frames_alone`).
3. **A message's end in the application's first round trip is copied**: a packet that finishes a
   stream, sent from the handshake's completion (`application_from`) until the peer acknowledges a
   packet of its space. The server's first reply goes then; lost and uncopied, it waited a probe
   timeout, the most frequent wait at the fresh dial's p90 (`a_lost_first_reply_comes_by_its_copy`).
   A bulk transfer's first window ends no stream and is not copied.
   `a_lost_first_datagram_costs_no_probe_timeout_with_its_copy` now finds the lost original
   undeclared: the client's Initial keys go before an acknowledgement meets a threshold for it
   (RFC 9002 §6.4).

Over 128 seeds, independent loss, two dials before: the first reply's p90 went from 658 / 296 ms
(fresh / resumed) at 35 ms to 207 / 105 ms learned. Under bursts the learned rows equal the 35 ms
rows.

## 18. What the peer acknowledged (2026-10-10)

- **Patch Q6: `PathStats::acked_bytes`.** The bytes of the path's packets the peer acknowledged,
  counted where an acknowledged packet leaves flight (`Connection::on_packet_acked`). hyper-transport
  judges a request sent whole by what reaches the peer: a bottleneck's queue holds the last window
  for seconds after it leaves on a slow path, and bytes sent said nothing of when the peer had them
  (focal's audit F38). No behaviour of the connection changes.
- **Patch Q7: `Connection::probe_timeout`.** The application data space's probe timeout before
  backoff (RFC 9002 §6.2.1: smoothed RTT, four times its variation, the peer's acknowledgement
  delay), which the connection already computed for its own timer. hyper-transport judges no
  exchange's window sooner: on a path whose round trip under load is longer than an exchange's
  period, nothing the peer acknowledges or answers can come within the period. No behaviour of the
  connection changes.
- **Patch Q8: `SendStream::sent_acknowledged` and `SendStream::written`.** Whether every byte sent on
  a stream has been acknowledged, none waiting to be sent again (the send buffer's unacknowledged
  length equal to its unsent bytes, no retransmissions queued), and the bytes the application wrote
  to it. Out of the peer's credit as well, the stream's bytes are all in the peer's buffer and the
  peer is not reading them: hyper-transport ends such an exchange (focal's audit F38). No behaviour
  of the connection changes.

## 19. A server connection holds its configuration through its handshake (2026-10-10)

- **Patch Q9: a replaced server configuration lives no longer than its handshakes.** Upstream held
  an `Arc<ServerConfig>` in every connection for the connection's life. The slab of §2.8 copied
  that as a count released only by `Drained`.
  - Every connection a server accepted pinned the configuration it was accepted under for as long
    as it lived. Peers hold their connections across a node's certificate renewals, so each
    replacement left one more superseded configuration in a slot.
  - After two or three replacements `set_server_config` was refused (`ConfigsFull`), and the node
    never presented its renewed certificate. focal's founder kept presenting a bootstrap server
    certificate its joiners had already unpinned, and their renewals failed until their
    credentials expired.
  - Now a server connection releases its count when its handshake completes, with a new endpoint
    event `ConfigReleased`. Nothing after that point needs the configuration it was accepted
    under:
    - its tickets were made as it read the client's Finished;
    - a QUIC client sends no TLS message after its Finished (RFC 9001 §4.4, §6), and CRYPTO data
      arriving after it is refused as TLS would refuse it: crypto error 10, unexpected_message;
    - NEW_TOKEN is sealed under the endpoint's current server configuration (`Configs` keeps which
      one is current, `Configs::current_server_config`). That is the configuration that validates
      the token when the client returns. Upstream sealed it under the connection's own, so a token
      issued after a replacement was never valid.
  - A client connection still holds its configuration until it drains: its server may send
    tickets for that configuration's cache at any time after the handshake (RFC 8446 §4.6.1).
- **Test harness.** `TestEndpoint::drive_outgoing` handed every queued connection event to the
  first connection it visited, and kept one timer for all connections: the last visited
  connection's. With two connections, the map's order decided which connection heard the other's
  datagrams, and the other's timers were lost (handshakes timed out, or `drive` never idled). Each
  connection now takes its own events and timer. Upstream's helper has the same loop; its tests
  hold one connection per pair.
- **Tests.**
  - `server_config_replacements_outlast_the_connections_accepted_under_them`: eight replacements
    while every earlier connection stays up. The original code refused the fourth with
    `ConfigsFull`.
  - `a_token_issued_after_a_replacement_validates_under_it`: a replacement mid-handshake. The
    original code sealed the token under the replaced key, and the returning client was not
    validated.

## 20. qlog behind a feature again (2026-10-10)

- **The `qlog` feature is restored.** It is on by default, so this workspace and every consumer
  that leaves default features on build as before.
  - `qlog` 0.15 turns on serde_json's `preserve_order`. Cargo unifies features, so every
    serde_json map in a build that holds hyper-quic iterated in insertion order rather than
    sorted. That includes code that never touches QUIC.
  - focal's schemas, output and canonical documents were written against sorted maps. Its suite
    found the change: a native-verb test read an object's first key and got another one. A
    document carrying free-form JSON would have encoded its keys in input order, so two equal
    documents could encode differently.
  - Built without the feature, the qlog pieces change as follows:
    - `QlogStream` is a type with no values, so no connection is given one;
    - `QlogSink` emits nothing;
    - `QlogConfig`, `QlogError` and the paths' recovery-metrics snapshot that only qlog reads are
      not compiled.
  - Upstream quinn-proto gates the same pieces behind its own `qlog` feature.
- **Checked.** Clippy -D warnings and the 392 unit tests pass in both builds, with the feature and
  without it.

## 21. BBR starts at RFC 9002's initial window (2026-10-11)

- **Patch Q10: `BbrConfig`'s initial window is RFC 9002 §7.2's unless one is set.**
  - Upstream's default was 200 datagrams of 1,200 bytes, 240,000 bytes. That number is Chromium's
    `kMaxInitialCongestionWindow`, its bound on a window resumed from a cached bandwidth, not a
    start. RFC 9002 §7.2 starts a sender at ten datagrams, at most 14,720 bytes; CUBIC, NewReno and
    Copa here already did, by the path's datagram size.
  - The path's warm-up (§15) measures to four initial windows a round trip and spends at most
    sixteen, by the law's own initial window. Under BBR those were 960 kB and 3.84 MB a connection,
    for every remote a connection measured, once a lifetime.
  - Found by focal. Its three metadata owners in one process, their endpoints on one thread, spent
    that thread on warm-up padding: one connection sent 2,546 warm-up packets (3.7 MB), where
    RFC 9002's window makes the budget 160. Their votes and appends waited behind it, and the
    quorum test failed 9 runs of 10 under BBR. With RFC 9002's window it passed 10 of 10.
  - `BbrConfig::initial_window` is now `None` unless set. Unset, it is RFC 9002's for the path's
    current datagram size, recalculated as that changes; a value set is kept, as CUBIC's is.
- **Tests.**
  - `congestion::bbr::tests::the_initial_window_is_rfc_9002s_by_the_datagram_size_unless_set`.
  - `every_law_warms_its_path_up_within_sixteen_initial_windows` (`tests/geo.rs`): each law's
    warm-up, counted from each side's `PathStats::sent_warm_up_packets`, stays within sixteen
    initial windows. Before the patch BBR's client sent 3,200 warm-up packets where at most 193
    fit.
  - The geo harness's scenarios name the law both endpoints run: CUBIC, their default, unless a
    check is of the laws.
- **Checked.** Clippy -D warnings with and without default features; the 393 unit tests and the
  integration suites pass.

## 22. A run of PADDING is one frame (2026-10-11)

- **Patch Q11: `frame::Iter` takes a run of PADDING whole.**
  - Each PADDING frame is one zero byte of no meaning (RFC 9000 §19.1). Upstream decoded each
    as a frame of its own: a frame type read, a match, a `Frame::Padding` that every reader
    then passes over (`process_payload`, `process_while_closed`, the frame statistics).
  - A datagram padded to the path's MTU therefore cost its receiver about 1,450 frames. A path's
    warm-up (§15) and its MTU probes send such datagrams. So does a padded Initial. A server
    that five hundred fresh connections warm up receives up to sixteen initial windows of them
    from each: a benchmark of 512 connections had to wait seconds for the warm-ups before its
    timing meant anything (hyper-rt-transport's `benches/connections.rs`).
  - The iterator now scans the run to its first non-zero byte and yields one `Frame::Padding`.
    Every reader already skips it, so nothing else changes.
- **Measured** (`frame::test::measure_a_padded_datagram`, release, a PING padded to 1,452 bytes):
  4,019 ns and 1,452 frames before, 472 ns and 2 frames after, before the per-frame work of the
  connection's readers, which the patch also spares.
- **Tests.** `frame::test::a_run_of_padding_is_one_frame`: 100 bytes of padding, a PING, 50
  more, decode as three frames (151 before). The 394 unit tests and the integration suites pass.

## 23. The warm-up is background traffic (2026-10-11)

- **Patch Q12: the path's warm-up (§15) ends at its own queue, as LEDBAT++'s slow start does.**
  - A warm-up measures to four initial windows a round trip. Over a path that carries far less,
    its packets queued, and where the queue held them all no loss ended it: it ran to its budget
    of sixteen initial windows, on a 128 kbit/s path some twelve seconds of padding that every
    other packet waited behind. focal's 128 kbit/s crossing test (a relay of 32 datagrams each way)
    found no leader within ten seconds in 4 of 6 runs on Linux CI with the warm-up on, its election
    votes queued behind it, and in none of 6 with it off (focal's diag run 38171360767).
  - RFC 9406's delay increase (HyStart++), tried first, compares each round trip's least RTT with
    the last's. A warm-up's round trip begins with an empty queue, so its least sample stays at the
    base however fast the others rise, and on a slow path a round trip lasts as long as the queue:
    on the pair below it ended only in its third round trip.
  - A warm-up is traffic of use to no one but the next connection, LEDBAT's case (RFC 6817). It now
    runs as LEDBAT++'s slow start (draft-irtf-iccrg-ledbat-plus-plus §4.1, §4.5):
    - it begins with two packets ("LEDBAT++ sender limits the initial window to 2 packets"), sent
      once nothing of the connection is in flight, so its first sample is its own packets' alone;
    - its window grows by what each acknowledgement acknowledges;
    - it ends once its queueing delay is larger than three quarters of the 60 ms target: the least
      of its four most recent RTT samples, none older than a round trip (RFC 6817 §2.5), past the
      least of all its samples.
  - Only its own packets judge it (`SentPacket::warm_up`). A smaller packet's RTT is shorter by the
    time the path takes to carry the difference (91 ms of a 1,452-byte datagram at 128 kbit/s), and
    a packet sent before the warm-up began met another flight's queue (the handshake's, at its end).
  - **Departure from LEDBAT++.** The window grows by all an acknowledgement acknowledges, not by
    LEDBAT++'s GAIN of it (§4.2: as little as 1/16 on a short path). The GAIN shares congestion
    avoidance among background flows, which a warm-up never enters; and the warm-up's budget is
    slow start's geometric sum, which growth at a sixteenth would spend before the target on a
    short path.
  - `PathStats::warm_up_end` says why a warm-up ended (`WarmUpEnd`: measured, queued, congestion,
    budget, path changed).
- **Measured** (the pair, 128 kbit/s each way, 10 ms one way, a queue without bound):
  - before the patch: 160 and 163 warm-up packets, each side ending at its budget, the smoothed RTT
    7.3 s;
  - with RFC 9406's exit alone: 41 and 35, the smoothed RTT 1.9 s and 1.3 s, the pair idle after
    8.4 s;
  - with the patch: 6 a side, each ending queued at its second sample, the smoothed RTT 172 ms and
    192 ms, the pair idle after 1.9 s.
  - Without a rate on the path, the warm-up still measures.
- **Tests.**
  - `tests::a_warm_up_over_a_slow_path_ends_once_its_packets_queue_and_over_a_fast_one_measures_it`
    fails before the patch (the warm-up ended at its budget). It runs on the pair's new link rate,
    `Pair::rate`: each direction carries so many bytes a second, with a queue without bound behind
    it.
  - `resume::tests::a_queue_is_the_least_recent_sample_past_the_least_by_three_quarters_of_60_ms`,
    `a_warm_up_begins_with_two_packets_and_grows_by_what_is_acknowledged` and
    `a_warm_up_whose_packets_queue_ends_as_an_attempt`; the existing warm-up tests check each end's
    cause.
  - The 398 unit tests and the integration suites pass, and hyper-transport's suites with them.

## 24. A warm-up that cannot meet its target stops, and one that can keeps what it measured (2026-10-11)

- **Patch Q13: the warm-up's end at its budget, a plateau, and its own byte count.**
  Measured on focal's three-node fleet:
  - **Where it ran.** Loopback paths of about 0.1–1 ms and relay paths of 5 ms at 100 Mbit/s,
    every warm-up's start, rounds and end printed.
  - **How the warm-ups ended.** Most ran to their budget of 160 packets (232 KB each), measuring
    nothing.
  - **Why, on loopback.** BBR's window sat at its floor of four datagrams (the bandwidth-delay
    product of an 86 µs path is a few kilobytes). Every round trip after that delivered three
    datagrams, while the warm-up's own window kept doubling.
  - **What it broke.** On Linux CI, padding from a warm-up that ran in an idle gap was counted as
    bytes of a copy: `evidence_quic`'s copy test failed 2 of 3 runs with the warm-up on and none of
    3 with it off.
  - **The budget ended the warm-up as it sent.** The round trip in flight then, the one that met
    the target, was acknowledged to an ended warm-up, and the measurement was lost. Now the budget
    stops the sending: the warm-up ends once its last packet is acknowledged, short of the target,
    and a round trip that met it is published.
  - **A plateau ends it.** BBR's full-pipe test (draft-ietf-ccwg-bbr §4.3.1.2): three round trips
    that each deliver less than a quarter more than the last one that grew, short of the target,
    end the warm-up as an attempt (`WarmUpEnd::Plateau`).
    - Only the warm-up's own acknowledged bytes are judged (`SentPacket::warm_up`). A reply the
      application had in flight as the warm-up began set the bar its slow start from two packets
      had to clear. On the 500 ms geo path, 12,647 bytes of a reply, then the warm-up's 1,200,
      3,600 and 9,600 were counted as no growth, and the warm-up ended short of the measurement
      that path's test needs.
    - Only a round trip in which a window held the warm-up back is judged, its own or the
      connection's (with its pacer). A round trip spent waiting for a quiet connection says
      nothing of the path, as BBR judges no application-limited round trip.
  - **Its own bytes are counted.** `PathStats::sent_warm_up_bytes` counts the warm-up's datagrams,
    each its own. hyper-transport's `PathFacts::warm_up_bytes` carries the count, so a count of
    what crossed a path can take the warm-up's padding out.
  - **Whether a warm-up is still to come.** `PathStats::warm_up_pending` (hyper-transport's
    `PathFacts::warming_up`) says whether the warm-up is yet to end: measuring, or waiting for an
    idle connection. A connection warms its path up at most once, so a count of what crosses a
    path can wait until no warm-up is pending and see the application's bytes alone.
    - Taking the padding out by the sender's count does not work. The sender counts a datagram as
      it sends it; a relay counts it once it has carried it, or never if it drops it.
    - On Linux CI, in 1 run of 6, a copy then counted 27,218 bytes for a chunk of 65,536.
- **Tests.**
  - `resume::tests::a_warm_up_whose_round_trips_stop_growing_ends_as_an_attempt`: a plateau short
    of the target ends it, a round trip no window held is not judged, and a plateau past the target
    publishes.
  - The budget test now checks that the warm-up ends once its last packet is acknowledged, and
    that a last round trip meeting the target is published.
  - `tests::a_warm_up_keeps_what_its_last_round_trip_measures`: under BBR at 100 Mbit/s and 40 µs
    one way. It fails with the old budget (ended at the budget).
  - The slow-path test checks the byte count.
  - The 400 unit tests and the congestion, e2e, geo and handshake suites pass, with
    hyper-transport's.

## 25. Acknowledgments alone are not padded to the MTU (2026-10-12)

- **Patch Q14: `pad_to_mtu` pads ack-eliciting packets only.**
  - Upstream pads every 1-RTT datagram when `pad_to_mtu` is on, a packet of acknowledgments alone
    included. A padded packet is in flight and counts against the congestion window: RFC 9002 §2
    ("in flight when they are ack-eliciting or contain a PADDING frame") and §3. But a packet of
    acknowledgments is never checked against the window (`start_datagram` checks ack-eliciting
    packets), so padded it went past the window, which RFC 9002 §7 forbids ("MUST NOT send a
    packet if it would cause bytes_in_flight ... to be larger than the congestion window").
  - Nothing frees it either. A peer acknowledges a non-ack-eliciting packet only along with an
    ack-eliciting packet of its own (RFC 9000 §13.2.1: "An endpoint that is only sending ACK frames
    will not receive acknowledgments from its peer"). A client that only acknowledges a server's
    flight holds the window its padded acknowledgments took until a later exchange.
  - Found on windows-2025 CI, `tests::pad_to_mtu` (run 38316593172): the client sent nothing,
    "blocked by congestion control". The server's warm-up (§15) ran to its budget of about 160
    padded PINGs, and the client acknowledged them in twelve packets padded to 1,333 bytes (its
    packet numbers 3 to 14), three of them drawn at once by packet numbers the server skipped
    (RFC 9000 §21.4, §13.2.1), none checked against the window. How many the skips add is random,
    so the test passed in most runs.
  - The padding hides the size of what the application sends, the option's stated purpose. A
    packet of acknowledgments carries none of it. The flag is now set once a packet is certain to
    be sent, and only for an ack-eliciting one, so a congestion check that held a packet back
    sets nothing either.
- **Tests.** `tests::padding_to_the_mtu_leaves_acknowledgments_unpadded_and_out_of_flight`: a client
  padding to its MTU acknowledges more server pings than its window holds padded datagrams; every
  acknowledgment is an ACK frame alone and nothing of the client is left in flight. It fails before
  the patch, 1,333 bytes in flight. `tests::pad_to_mtu` and the unit and integration suites pass.

## 26. A stream holds the spans its window pays for (2026-10-13)

- **Patch Q15: the assembler's span bound follows the peer's window** (`connection/assembler.rs`,
  `max_spans`).
  - Upstream bounds the spans a stream holds out of order at a fixed 1,024
    (`MAX_CHUNKS`: "ngtcp2 uses a threshold of 4000 -- try to be a little more conservative?"),
    and closes the connection with INTERNAL_ERROR, "too many gaps in stream buffer", past it. A
    legitimate sender reaches it: a window of frames of `d` bytes with every other frame lost
    holds `window / (2d)` spans, so any stream window past `2 · 1,024 · d`, 2.3 MB at the smallest
    datagram, can be closed by its own sender's losses. slates' congestion bake-off found it on
    hyper-quic's harness (`docs/benchmarks.md`, "slates' congestion bake-off on hyper-quic"):
    at 100 Mbit/s and 100 ms with no random loss, a slow start overshot the drop-tail queue, which
    then lost every other datagram of the round trip, and the receiver refused its 1,025th span
    1.36 s in, 2.4 MB buffered in a 4.8 MB reach. NewReno and CUBIC carried nothing after that at
    100 and 300 ms, BBR nothing at 300 ms. focal met the same limit with a 10 MiB window, and
    hyper-transport capped its stream window at `2 · 1,024 · d` for it (patch Q5). Upstream's
    issue #2809 is its contiguous form, closed by PR #2814, which this crate carries.
  - The bound is now the spans whose own memory, a `Buffer` each, fits in the window the peer
    holds at their most (twice the bound, the compaction's slack), and never under the 1,024
    upstream kept. The window is what the peer may send past what the application has read: the
    stream's `sent_max_stream_data` for a stream, the crypto buffer for CRYPTO frames (where the
    floor stays the bound: 16 KiB pays for under 1,024). So what a peer makes the receiver hold
    for disorder stays within the memory the window already grants, Linux's budget for a receive
    buffer's overhead (`tcp_adv_win_scale` = 1, Documentation/networking/ip-sysctl.rst), and a
    sender whose frames carry at least a `Buffer`'s size is never refused for its losses. A peer
    of one-byte frames past gaps is refused once its spans pass the bound, as before.
  - `MAX_STREAM_CHUNKS` stays exported, now the floor: hyper-transport's stream window ceiling,
    derived from it, is still never refused, and is no longer what keeps the connection open.
- **Tests.**
  - `assembler::test::a_window_with_every_other_frame_lost_is_held`: 10 MiB of 1,134-byte frames,
    every other one lost, held and read back byte for byte once the lost ones come. It fails
    under the fixed bound at its 1,025th span.
  - `assembler::test::spans_past_what_the_window_pays_for_are_refused`: one-byte frames past
    gaps are refused once past the bound, the spans held never costing more than the window.
  - `tests/congestion.rs`, `a_slow_starts_overshoot_at_a_large_window_keeps_its_connection`:
    NewReno at 100 Mbit/s and 100 ms for 2 s with a window of four times the path and its queue
    keeps its connection; under the fixed bound the server closed it at 1.36 s.
  - The assembler's earlier tests keep upstream's bound through a limit at the floor.

## 27. The copies' scan looks at the handshake's flights alone (2026-10-13)

Measured by `benches/vs_upstream.rs` (hyper-quic against upstream quinn-proto 0.11.18 in one process,
the same TLS on both sides; `docs/benchmarks.md`, "hyper-quic against upstream quinn-proto"). With its
defaults, hyper-quic took 1.58 times upstream's instructions a mebibyte of bulk transfer and 1.30
times an exchange of 64 bytes each way; with the handshake's copies and Careful Resume off, 1.07 and
1.18. A profile of the bulk transfer (`sample`) put 41% of hyper-quic's samples in
`Connection::poll_transmit`'s own code against 8% with both off, and turning the features off one at a
time put the cost on the copies.

- **`PacketSpace::queue_copies` visited every packet in flight at every transmission.** Only packets
  of the handshake's flights are ever copied (§13, §17), all sent while the handshake runs or in the
  application's first round trip, but the scan walked the whole `sent_packets` map: a window of
  thousands of packets was walked for each datagram sent. A space now keeps `flights_end`, one past
  the largest packet number it sent as one of the flights, and the scan walks `..flights_end`.
  `Connection::queue_copies` returns before working out the copies' spacing (the burst estimate and
  its fixed-point logarithm) when no space holds a flight in flight (`PacketSpace::holds_flights`).
- **`Connection::after_fill` read every space's pending frames at every transmission**, to stop a
  space sending copies once its queue had emptied. It now reads them only for a space that is sending
  copies.
- After the change (three rounds at load 10–12), instructions against upstream's: a mebibyte 1.068
  (copies and Careful Resume off, 1.059), an exchange of 64 bytes each way 1.121 (1.089), of 4 KiB
  1.072, of 64 KiB 1.060; a full handshake 0.805, a resumed one 0.738. What remains is spread over the
  patches' per-packet work (`finish_and_track`, the receive side's reads, `initial_window`), no one
  site above a few percent of the samples.
- **Tests.** `tests::the_copies_look_at_the_handshakes_flights_alone`: after the handshake, a client
  with a window of packets of a bulk stream in flight has no flight left to scan. The copies' own
  tests (`tests/geo.rs`, `connection::copies`) pass unchanged.

## 28. STREAMS_BLOCKED sent when the peer's stream limit stops an open, and kept when heard (2026-10-13)

Upstream never sent STREAMS_BLOCKED. `Streams::open` returned `None` at the peer's limit and left a
`TODO: Queue STREAM_ID_BLOCKED if this fails`. A received STREAMS_BLOCKED was logged and dropped.
RFC 9000 §4.6 says a sender SHOULD send STREAMS_BLOCKED when it wishes to open a stream but cannot
because of the peer's maximum stream limit. A peer that grants streams as they are wanted, holding
back what it cannot take (hyper-transport's stream pool, admission credit as Breakwater's, Cho et
al., OSDI 2020), has no other word that this side wants more.

- **Sent.** An open refused at the peer's limit queues the frame once for that limit
  (`StreamsState::streams_blocked_limit`), as DATA_BLOCKED is queued once for each connection
  limit. It is written while the limit stands (`can_send_streams_blocked`), with the limit the
  peer last sent, and retransmitted on loss like any control frame (`Retransmits::streams_blocked`)
  unless the peer has raised the limit since. `Streams` now carries the data space's pending
  frames, as `SendStream` does, so `open` can queue it.
- **Heard.** The limit the peer reports is kept per direction. `Connection::peer_streams_blocked`
  says whether the peer is blocked with no more streams on their way: it reported the limit it was
  last sent, and no greater limit is sent or waiting to be (a raise not yet sent is on its way, so a
  side granting streams does not grant twice for one report).
- **What is left to open.** `Streams::remaining` says the streams this side may still open in a
  direction, its peer's limit less those opened: with the streams it holds, what its peer grants
  it, by which an asker bounds what it queues for streams (hyper-transport's `AskQueue::Credit`).
- **Test.** `an_open_at_the_peers_limit_says_so_once_for_each_limit`: one frame for each limit, none
  written once the limit is raised, the frame again at the new limit, and the receiving side
  blocked only at the limit it last sent with none greater on its way.

## 29. A finished reply gives its stream's credit back with it (2026-10-13)

A peer-initiated bidirectional stream gave its credit back (`stream_freed`: its count among the
allocated taken out, so `ensure_remote_streams` raises the limit within the window) only once both
halves were freed: its request read whole, and its reply's last frames acknowledged. The raised
MAX_STREAMS then went out alone, a round trip after the reply, since a server has nothing more
for that peer once its reply is acknowledged. hyper-transport's pooled credit
(docs/transport.md §4g) grants a pooled peer streams one at a time, so every exchange cost the
server that datagram, and the peer the wait for it.

A stream's credit now goes back at the first of: its request read whole with its reply finished
(`StreamsState::return_credit_early`, from `SendStream::finish` and `stream_recv_freed`), or its
freeing as before. The stream's send half records that it did (`Send::credit_returned`), so its
freeing, by the reply's acknowledgement or by a reset's, gives none again. Once returned at the
finish, the raised limit is queued at once (`queue_max_stream_id`), and it goes out in the reply's
datagrams.

RFC 9000 §4.6 lets an endpoint raise MAX_STREAMS at any time; it must be prepared for what it
allows. The streams over the window at once are those whose reply waits for its acknowledgement,
which their state already holds: Breakwater's overcommit of credit issued in its responses (Cho et
al., OSDI 2020). An application that manages the window (`set_max_concurrent_streams`) sets
it before it finishes a reply, so the credit goes back within what it decided.

Tests: `a_finished_response_gives_its_streams_credit_back_once_before_its_acknowledgement` (it fails
without the change, at "announced with the response"); hyper-transport's
`a_pooled_servers_stream_goes_back_with_the_reply` (20 exchanges through a pool of one: 40
datagrams of each side, where they took 59) and `a_pool_sized_to_nothing_holds_its_peer_until_sized_again`
(the pool settled before the finish).

## 30. BBR's bandwidth from delivery-rate samples, and three divergences from Chromium (2026-10-15)

Found while measuring focal's event bus, whose publisher's congestion window reached 408,890,985
bytes on a path of 72 µs, and whose lossless lane, then bound to the window, held seconds of
messages behind QUIC. That window was CUBIC's, the bus's law, which on a path that loses nothing has
no bound (hyper-transport's lossless lane is now bound otherwise, `docs/transport.md` §4). Reading
BBR, focal's law elsewhere, for the same growth found the port diverging from Chromium's
`bbr_sender.cc` in four places, the first the same unbounded growth in an app-limited startup.

- **Patch Q16: startup grows the window to its target and no further.** Chromium's
  `CalculateCongestionWindow` grows a window in startup by what is acknowledged while
  `congestion_window_ < target_window`. The port compared the gain (2.885) with the target in bytes,
  which is true of any target. A sender that never leaves startup grew its window by every byte
  acknowledged, without end: one held back by its peer's flow control, or by its application, never
  sees full bandwidth. RFC 9002 §7.8 says a window not used to its size is not grown.
- **Patch Q17: the bandwidth filter's rule.** A sample updates the windowed maximum when it was not
  app-limited, or when it is above the maximum. That is Chromium's `UpdateBandwidthAndMinRtt`: an
  app-limited rate is a floor of the path's. The port took only samples that were both not
  app-limited and larger, so a sender that was always app-limited kept an estimate of zero, and an
  old high sample never aged out.
- **Patch Q18: the minimum round trip expires ten seconds after it was sampled.** This is Chromium's
  `min_rtt_timestamp_`.
  - The port timed the expiry from when ProbeRTT last began, and called it expired before any had.
    So every connection entered ProbeRTT (a window of four datagrams, for 200 ms) at its first
    acknowledgement that was not app-limited. §24's "BBR's window sat at its floor of four datagrams"
    on loopback and 5 ms paths was this.
  - The minimum started at zero, and `min_rtt > sample` is never true from zero, so only that expiry
    ever set it. On expiry the port took the connection's lifetime minimum; Chromium takes the
    latest sample.
  - A sample from a phase the application limited does not expire it. Such a sender keeps no queue of
    its own to drain; this is Chromium's option `probe_rtt_disabled_if_app_limited_`.
- **Patch Q19: delivery-rate samples** (draft-cheng-iccrg-delivery-rate-estimation-02 §3; Linux's
  `net/ipv4/tcp_rate.c`; Chromium's `BandwidthSampler`).
  - **What the port measured.** It divided each acknowledged packet's bytes by the time since the
    previous acknowledgement, and the latest packet sent by the time since the one before, whichever
    packets those were. On a pair at 100 Mbit/s and 40 µs one way it estimated 357 MB/s. BBR's
    window grew to 147 kB, 65 times the path's bandwidth-delay product, and the queue it kept
    lengthened the round trip from 143 µs to 5 ms.
  - **The per-packet snapshot.** Each path now keeps what it has delivered (`connection::Delivery`).
    Each packet in flight carries what had been delivered when it was sent, and when (`Snapshot`).
  - **The sample.** An acknowledgement's newest packet gives the sample. Its rate is the bytes
    delivered since that packet's snapshot, over the longer of its flight's send phase and its
    acknowledgement phase. ACKs that come together shorten only the second phase; packets sent
    together shorten only the first. An interval shorter than the path's least round trip is no
    sample, since no flight is delivered faster than it crosses the path.
  - **App-limited phases.** A `poll_transmit` that finds nothing to send, with neither window nor
    pacer holding anything back, marks a phase the application limits. It lasts until what is in
    flight is delivered (§3.4).
  - **Size.** The snapshot is sixteen bytes in the sent packet's record: its times are nanoseconds
    before the send, and a flight older than 4.29 s gives no rate. The app-limited flag takes a byte
    the record's other flags leave free. `largest_acked` is now eight bytes, held as one more than
    the packet number in a `NonZeroU64`. So the record stays within the 128 bytes
    `sent_packet_size` holds it to.
- **The order of an acknowledgement.** The round trip is sampled before the controller hears that the
  acknowledgement is done, as in Chromium. The controller takes the delivery sample through
  `Controller::on_delivered` and folds it into its model in `on_end_acks`, in Chromium's
  `OnCongestionEvent` order: the round, then bandwidth and minimum round trip, then recovery, ack
  aggregation and the modes. Its full-bandwidth check reads the sample's app-limited flag, not the
  connection's at the time of the acknowledgement.
- **Measured.** slates' bake-off (`docs/benchmarks.md`), the branch before and after:
  - NewReno, CUBIC and Copa wrote the same 495 rows byte for byte.
  - BBR's goodput shortfall went from 1.075 to 1.020. Its share of the link rose on most paths:
    64 kbit/s at 300 ms from 62% to 91%, and 100 Mbit/s at 300 ms with 1% loss from 21% to 143%. A
    share past 100% counts what the path held when the warm-up ended.
  - Its ping p99 went from 1.467 to 1.497 of the best law's, its worst from 2.77 to 3.95 (a buffer of
    four products: 206 → 430 ms). Its Jain index at 20 and 100 ms went from 0.714 to 0.515.
  - BBR states no pacing rate, so the connection paces it at five quarters of its window a round trip.
    Drain's gain (0.35) and ProbeBW's cycle therefore move nothing. A sender the window holds keeps
    its window in flight, about a product of queue standing, and its ping p99 is twice the round
    trip on most paths. Chromium paces at the model's rate. That is the next change, with how two
    warm-ups at once yield to each other: paced, the pair at 5 ms ended at its budget in 17 of 40
    runs with both sides warming up, and 3 of 40 unpaced.
- **Tests.**
  - `connection::delivery::tests`: a burst through a bottleneck measures the bottleneck; ACKs that
    come together measure no faster than the packets were sent; an interval below the least round
    trip is no rate; an app-limited phase ends once its flight is delivered; an idle gap is in
    neither phase; a packet sent after a stall gives no rate.
  - `congestion::bbr::tests::a_bulk_sender_measures_its_bottleneck_and_keeps_a_window_of_a_few_bandwidth_delay_products`
    drives the controller and a sampler over a 100 Mbit/s bottleneck: its estimate within 10% of
    the bottleneck, its window within four products, its round trips within four times the path's.
  - `an_app_limited_sender_in_startup_grows_its_window_to_the_target_and_no_further`, now driven
    through the sampler; without Q16 the window reached 9,612,000 bytes.
  - `tests::a_warm_up_keeps_what_its_last_round_trip_measures` runs at 5 ms one way, with the client
    alone warming up. At 40 µs, the path it ran on, the warm-up now ends at its plateau
    (`a_warm_up_on_a_path_shorter_than_its_target_ends_at_a_plateau`). Each passed 40 runs of 40.

## 31. What a receiver's socket is charged for: the window consumed, the packets a peer may acknowledge (2026-10-16)

Patch Q20 adds two getters and changes no behaviour. A driver learns what its socket's queue is
charged for each stream byte a peer sends (hyper-transport's receive charge, `docs/transport.md`
§4i), so it needs two counts:
- what each message it read carried against the connection's window;
- what the peer may send unprompted by any window.

- `Connection::stream_bytes_received` counts the bytes of every STREAM frame received, each as it
  arrived, repeats and frames past a gap included: what the datagrams carried. It is a counter
  beside the frame's processing and changes nothing else. The first version returned flow
  control's `data_recvd`, every receive stream's end offset summed. That moves by a gap's bytes when
  a frame lands past one, so a datagram read alone after a loss seemed to carry more stream bytes
  than its whole payload, and the room told from it passed its socket's buffer (`docs/transport.md`
  §4i).
- `Connection::ack_eliciting_in_flight` is the current path's `in_flight.ack_eliciting`: the packets
  the peer may acknowledge, one acknowledgement each at the most (RFC 9000 §13.2).

## 32. A packet space's sent packets in a deque (2026-10-16)

Patch Q21 holds a packet space's sent packets in `SentPackets`, a `VecDeque` of `(number, packet)` in
packet number order, where upstream holds a `BTreeMap<u64, SentPacket>`. Packet numbers only grow
(RFC 9000 §12.3), so each packet sent is pushed at the back, and acknowledgements come oldest
first, so most packets leave from the front in constant time. The map moved the rest of a leaf's
packets on each take: a `SentPacket` is 128 bytes and a leaf holds eleven. The take was the receive
path's largest copy after its own data (`BTreeMap::remove` and its `memmove`, about 1.5 % of a bus
node's busy samples at 100,000 messages a second, macOS `sample`, 2026-10-16). Chromium holds its
unacknowledged packets the same way, in a deque from the least unacknowledged
(`QuicUnackedPacketMap`).

A take from the middle (an acknowledgement past a loss, or a forgotten acknowledgement-only packet)
moves the shorter side. A lookup is a binary search that asks the newest packet first. The
methods are the map's, so every use reads as it did. Test:
`spaces::test::sent_packets_answer_as_the_map_they_replaced` runs the deque against the map over
20,000 of a space's own operations from a seed: sends, takes from either end and the middle,
lookups, and every range shape the uses ask.

## 33. Acknowledgements decimated as Google QUIC's (2026-10-16)

Patch Q22 lets a sender ask its peer, through the acknowledgement frequency extension it already
carried (draft-ietf-quic-ack-frequency), to acknowledge as Google QUIC's receivers do (quiche,
`quic_constants.h`): an ACK for every 10 ack-eliciting packets (`kMaxRetransmittablePacketsBeforeAck`),
else a quarter of the round trip after the first one waiting (`kPeerAckDecimationDelay`), from the
100th packet on (`kMinReceivedBeforeAckDecimation`, "to avoid the beginning of slow start, when
CWNDs may be rapidly increasing"). `AckFrequencyConfig::decimation(packet_threshold)` builds the
request and `TransportConfig::decimate_acks` asks it with the configuration's own packet threshold.
Upstream's request is static, a fixed delay or the peer's own (25 ms on most paths), sent at once:
- the delay asked is the smoothed round trip over four (`AckFrequencyConfig::rtt_divisor`), the
  smoothed round trip and not the least (draft-ietf-quic-ack-frequency-14 §8.1; quiche takes the
  least), never below the peer's `min_ack_delay`, which hyper-quic advertises as its 1 ms timer
  granularity (RFC 9002 §6.1.2): on a path under 4 ms the delay is 1 ms;
- the first request waits until the application data space has numbered 100 packets
  (`AckFrequencyConfig::after_packets`), so slow start's packets are acknowledged as RFC 9000
  §13.2.2 asks; a request is sent again as before, once the delay it would ask differs by a fifth;
- the reordering threshold is the loss detector's packet threshold (§9), so a reordering elicits an
  immediate ACK only where it would let the sender declare a loss.

RFC 9000 §13.2.2's default, an ACK for every other ack-eliciting packet, cost each delivery of a
one-packet message half a packet back, with its system calls and the sender's wake: at 100,000 bus
messages a second a node handled 3.4 datagrams a message, a third of them acknowledgements
(`docs/transport.md` §4k).

Test: `tests::decimated_acks_come_from_the_hundredth_packet_every_tenth_or_a_quarter_round_trip_on`
(no request before the 100th packet; then a lone ping is acknowledged a quarter of a 20 ms round
trip later, 5 ms and not 4, and the tenth of ten pings at once).

## 34. A probe timeout that follows an unanswered one repeats no stream data (2026-10-17)

Patch Q23 narrows §11's first departure. A probe with no new data to send repeats its oldest
packet's frames, STREAM frames included, as Chromium's does. That stays so for the first probe
timeout after an acknowledgement, where a lost reply goes again with its probe. A timeout that
follows an unanswered one (`pto_count` past one) repeats only the oldest packet's control frames in
the application data space, as upstream's probes did, and no stream data
(`Connection::poll_transmit`, `PacketSpace::maybe_queue_probe`'s `streams_too`). RFC 9002 §6.2.4
lets a sender choose: previously sent data "MAY" be sent when no new data can be.

The reason is the peer's socket. Every probe datagram is clamped to the smallest datagram
(upstream's rule, `allocate_datagram`), but the frames a probe moves are its oldest packet's, and
what its two datagrams cannot hold went beside them as the congestion window allowed. So each probe
timeout could send up to a packet of the path's MTU again. A peer that has stopped reading holds
all of it in its socket's queue, and may time out fifteen times before its idle timeout (ten
seconds). hyper-transport reserves room in that queue for what a peer sends unprompted, and at the
path's MTU (65,507 bytes on loopback, §4q of `docs/transport.md`) that took 262 KB to 2 MB of the
319 KB Linux grants by default. A timeout that follows an unanswered one says the peer is not
acknowledging: it has stopped reading, and the data waits in its queue, or the path drops it, and
the probe's acknowledgement will declare it lost. So a peer that has stopped reading is now sent at
most one packet of stream data again (`docs/transport.md` §4s). The handshake spaces repeat whole:
their packets fit the smallest datagram, and the handshake cannot go on without them.

Test: `tests::a_probe_timeout_after_an_unanswered_one_repeats_no_stream_data` (every datagram
dropped through three timeouts: the first's two probes repeat stream data, the next two timeouts'
probes carry none, only their IMMEDIATE_ACKs, and every probe is within the smallest datagram). §11's
`geo::a_lost_reply_goes_again_in_the_first_probe_within_two_round_trips` and
`tests::blackhole_after_mtu_change_repairs_itself` pass unchanged.

## 35. What a shrunk receive window still owes, read (2026-10-17)

Patch Q24 adds a getter and changes no behaviour. `Connection::set_receive_window` shrinks a window
as upstream does: the credit already given stands (RFC 9000 §4.1: "it is not an error to advertise
a smaller limit, but the smaller limit has no effect"), so the difference is held as
`receive_window_shrink_debt`, which the application's reads pay before `local_max_data` moves
again. `Connection::receive_window_debt` returns that debt. hyper-transport sets an idle
connection's grown window back to its first and gives back what the growth held as the debt is
paid (`docs/transport.md` §4t): the peer may still send at most the window and the debt beyond what
was read, so what is held never falls below that.

## 36. A probe of the path's MTU waits for the window and the pacer (2026-10-17)

Patch Q25 departs from upstream's MTU discovery, which sent a probe whenever the connection had
nothing else to send, past the congestion window and the pacer. Upstream's search stops at 1,452
bytes by default, where a probe costs a path little. hyper-transport searches up to the path's MTU
as its kernel holds it (`docs/transport.md` §4q): 65,527 bytes on a loopback. Behind a relay of 128
kbit/s in front of such a loopback, the search's first step, about 33 KB, held the link two seconds
and every packet behind it waited: three focal nodes never heard each other's votes and elected no
leader (`evidence_quic::a_megabyte_chunk_reaches_its_copy_across_128_kbit_per_second`, every run on
Linux once focal took the search to the path's MTU; a clamp of the ceiling to 1,452 bytes brought the
leader back). RFC 8899
§3, item 7 lets a probe be regulated so: "A PL MAY use a congestion controller to decide when to send
a probe packet", its probes then "delayed or suspended during congestion", and a PL "could need
transmission of probe packets to be regulated by" its pacing.

A probe is now sent only when it fits the congestion window and the pacer lets it go
(`write_mtu_probe`), the pacing timer set for when it may. A window of one round trip of the path
never affords a probe that would take the path longer, so a slow path keeps its MTU and a fast one
probes as before. The search plans its next size once (`MtuDiscovery::probe_due`, `SearchState::
planned`) and takes it as sent when its packet exists, under its own number (`probe_sent`): choosing
a size reads the last probe's fate, so a step held back is not chosen again, and a builder that
refused no longer left the search waiting on a number the next packet took. A step planned past a
ceiling lowered meanwhile is planned again from the bounds as they stand.

Test: `tests::an_mtu_probe_waits_for_the_window_on_a_slow_path` (a 128 kbit/s path whose MTU is
65,527, the search allowed to it: no probe sent, the MTU where it began; without the rule the client
sent 12 probes, reached 65,527, and its smoothed round trip grew to 4.8 s against 48.5 ms, the
warm-up's packets of the new MTU growing its window to 393 KB). `tests::mtud_probes_include_immediate_ack`
and every MTU test pass unchanged.

## 37. A probe the window and the pacer can carry goes before new data (2026-10-20)

Patch Q26 narrows Q25 (§36) for a connection that keeps sending. Q25 holds a probe back as any
packet is held, by the congestion window and the pacer, and the probe is still sent only in the room
data leaves (`send_when_idle`, as upstream sends it). A connection that kept sending left it none:
data filled the window, and the pacer's bucket, as fast as acknowledgements and time freed them, so
the room a probe needs never came, and the search stayed where it began until the sending stopped.
On loopback a connection with a window of 130 KB sent no probe in a minute and carried every byte in
1,200-byte datagrams of the path's 65,507
(`hyper-rt-transport::peer_sockets::a_connections_mtu_search_ends_at_its_paths_mtu_as_the_kernel_holds_it`,
7 runs of 40 on arm64 Linux, two arm64 CI runs; the faster x86 runners left gaps between bursts).

Now a probe the search has due that the window could carry with nothing in flight and the pacer's
bucket could carry full goes before new data (`Connection::probe_goes_first`): new data in the 1-RTT
space waits, as an application-limited connection's does, until what was in flight drains and the
bucket refills by the probe's size, and the probe takes the room; acknowledgements, loss probes and a
PATH_RESPONSE do not wait, nor does the warm-up take the room meanwhile. TCP's probe takes its place
among its window's segments the same way (Linux, `tcp_mtu_probe`). A probe larger than the window
or the bucket waits as §36 has it, and data does not wait for it: on a slow path it would hold the
link past a round trip, which is what Q25 keeps from it.

Test: `tests::an_mtu_search_runs_on_a_connection_that_keeps_sending` (a fast path whose MTU is
65,527, the search allowed to it, no warm-up and the flow-control windows out of the way, 16 MiB sent
the moment the idle connection's first window has carried no probe: 318 datagrams, the search run
during the transfer and ended at 65,527; without Q26, 14,377, its one probe starved by the pacer).
`tests::an_mtu_probe_waits_for_the_window_on_a_slow_path` and every MTU test pass unchanged; on arm64
Linux the peer_sockets test passed 40 runs of 40.

## 38. Loss thresholds that learn a path's reordering, and a probe timeout a loss time held back (2026-10-22)

Patch Q27 departs from upstream's fixed loss thresholds, RFC 9002's three packets
(`TransportConfig::packet_threshold`) and 9/8 of the round trip (`time_threshold`), which stay the
floor. RFC 9002 §6.1.1 names RACK's raised threshold as expected to be "at least as useful in QUIC",
and §6.1.2 lets the time threshold adapt. `connection/reordering.rs` holds one connection's
tolerance, learned in its Data space from spurious losses, packets declared lost that the peer
acknowledges afterwards (`PathStats::spurious_losses`; `docs/transport.md` §4ac gives the rules and
their sources):

- the packet threshold rises to a spurious loss's distance plus one;
- the time threshold gains a reordering window, a quarter of the least round trip for each round
  trip with a spurious loss (RACK's `reo_wnd_mult`, the round trip in packet numbers), bounded by
  the longer of the smoothed round trip and the longest a spurious loss took;
- sixteen recovery periods begun with no spurious loss return both to the floor (RACK's
  `reo_wnd_persist`);
- a declaration is remembered for a probe timeout, no more of them than a window holds for each
  round trip in it.

The ACK frame's ranges are checked against the remembered declarations before the frame's newly
acknowledged packets are taken, so an acknowledgement that names only packets already declared
lost still teaches. A congestion event begins a recovery period as RFC 9002 §7.3.2 has it, a lost
packet sent after the current period began (`Reordering::on_congestion_event`, beside the
controller's own); an ECN mark is no loss recovery and begins none.

Patch Q28 changes `on_loss_detection_timeout`. Upstream returns after a time-threshold detection,
having armed the probe timeout if no loss time remains; a loss time holds the probe timeout back
(RFC 9002 Appendix A.8), so the probe timeout can then be due already, and the connection asked to be
woken at the time it was woken, its probe sent only at that second wake. With a reordering window a
loss time ends after the newest ack-eliciting packet's probe timeout often: the congestion harness's
`Run::fire` refused a server's wake at ±40 ms of jitter. The probe timeout now fires in the same
wake.

Measured (`congestion::print_the_reordering_table`, `docs/benchmarks.md`, "hyper-quic: a path that
reorders"): BBR at 10 Mbit/s and 100 ms carried 69.7%, 44.1%, 21.2% and 6.0% of the link with ±5,
±10, ±20 and ±40 ms of jitter each way, and now 97.2%, 96.7%, 98.4% and 96.2%, as the same jitter
kept in order carries (97.1%, 94.5%, 98.1%, 97.4%). slates' bake-off against the parent: 660 of
672 rows the same byte for byte, every path that does not reorder; its reordering ±10 ms trial
carried 45.5–46.5% for BBR and 3.9–10.6% for the others, now 93.5–94.9% and 24.2–34.2%.

Tests: `connection::reordering::tests` (a spurious loss widens the tolerance once a round trip, the
tolerance is bounded by what the path showed, a quiet path returns to the floor with its memory
bounded, an acknowledgement of nothing declared changes nothing);
`congestion::a_reordering_path_is_carried_as_its_jitter_kept_in_order` (BBR at ±20 and ±40 ms keeps
nine tenths of its in-order twin; without Q27, 21.2% and 6.0%);
`congestion::every_wake_on_a_path_that_reorders_does_what_is_due_at_its_time` (BBR at 10 Mbit/s and
20 ms with ±10 ms; without Q28 a node woke at 19.65 s and asked to wake then again). The unit tests,
congestion, e2e, geo and handshake suites pass.

## 39. A connection ID the peer retired routes for three probe timeouts more (2026-10-22)

Patch Q29 departs from upstream's endpoint, which stopped routing a connection ID the moment the
peer's RETIRE_CONNECTION_ID arrived. The peer sends that frame after its last packet with the ID, but
a path that reorders delivers such packets after it: the endpoint dropped each, its data to be sent
again, and answered it with a stateless reset. A server meets this on every connection over such a
path: at the client's first NEW_CONNECTION_ID it moves off the client's first connection ID, which
carries no reset token, and retires it (upstream's `on_new_connection_id`), its packets with that ID
still in flight. In the congestion harness, jitter of ±10 to ±80 ms each way on paths of 20, 100 and
300 ms, every law, 14 of 84 runs met a stateless reset (`Run::arrive` refuses one), on `25cf889` as
after Q27 and Q28.

A connection now keeps a retired ID routing for three probe timeouts (`CidState::retire_later`,
`Timer::RetiredCid`): Chromium's rule (`QuicSelfIssuedConnectionIdManager::OnRetireConnectionIdFrame`,
`3 * pto_delay`), and the span RFC 9001 §6.5 keeps old read keys for packets still in flight. RFC 9000
§5.1.1 has an issuer accept an ID until its peer retires it and leaves what comes after to the issuer.
The connection asks its endpoint for the replacement at once (`NeedIdentifiers`), as upstream did beside
the unrouting; `EndpointEventInner::RetireConnectionId` now names only the ID to stop routing. No more
are kept routing than the connection issues (`TransportParameters::issue_cids_limit`, at most
`LOC_CID_COUNT`); past that the oldest retired stops at once, which costs only what was still on its
way with it.

A RETIRE_CONNECTION_ID repeated (its packet sent again and both arriving) retires nothing and asks for
nothing (`CidState::on_cid_retirement` answers `None`). Upstream's endpoint had made a repeat harmless
by finding the ID already gone and issuing none; asked for at the retirement instead, a repeat that
came before the first replacement was counted asked for a second, past the peer's limit. Twelve e2e
handshakes at a time against an upstream server, four rounds: 13 of 48 closed with
CONNECTION_ID_LIMIT_ERROR without the check, every one a run that took a repeat (26 repeats); none of
48 with it, 52 repeats taken.

Tests: `connection::cid_state::tests::a_retired_id_routes_until_its_time_and_no_more_are_kept_than_the_cap`
and `a_repeated_retirement_retires_nothing_and_asks_for_nothing`;
`tests::a_retired_cid_routes_for_three_probe_timeouts` (a packet held behind its RETIRE_CONNECTION_ID
reaches its connection, and the ID stops routing after; without Q29 the packet never reached it);
`congestion::a_path_whose_jitter_passes_its_delay_keeps_what_was_sent_with_a_retired_id` (BBR at ±20 ms
each way on 20 ms; without Q29 a stateless reset). The 84 runs: none meets a stateless reset.

slates' bake-off against `cd90e5b`: 33 of its 672 rows move, on paths that do not reorder, every law's
somewhere, the shares in both directions (BBR at 100 Mbit/s and 300 ms the most, 93.4 → 82.4% in one
seed and 100.2 → 150.6% in another, past 100% the warm-up's, §30). With `Timer::RetiredCid` never
armed every row is as `cd90e5b`'s: the timer's wakes, one each a retirement, shift each run's events.
What a wake changes, traced to the event (loss 0%, 1 Mbit/s and 20 ms, seed 3, BBR): a pacer short
of tokens sleeps until its bucket holds a whole burst (`Pacer::wait`, upstream's rule, to batch its
wakes), but a connection woken sooner for any reason sends what the bucket holds then. At 462.755 ms
the client's retirement timer woke it and it sent 6 datagrams that, without the timer, went with a
seventh at the pacer's own wake, 470.027 ms. A paced sender's bursts follow its wakes: what it sends
is the same, when the bucket lets it go is not, and each run's later events move with it. Not an
app-limited mark (§30): with a wake's polls marking none, the same 33 rows move. The bake-off's
summary is as `cd90e5b`'s within its third decimal (BBR's ping p99 geomean 1.498 → 1.490).

## 40. The time threshold covers the longest a spurious loss took (2026-10-23)

Patch Q30 narrows §38's time threshold. §38 added RACK's reordering window to RFC 9002's threshold,
9/8 of the larger of the smoothed and latest round trips, in steps of a quarter of the least round
trip. Where a share of the packets skips the path's delay, as netem's `reorder` sends them (the
reordering focal's comparison runs apply), both terms rest on the packets that overtook: QUIC samples
a round trip only from the largest packet acknowledged (RFC 9002 §5.1), and there that is the one
that skipped, so at 10 Mbit/s and 80 ms with 5% skipping each way the least round trip read 1 ms and
the smoothed one about 64 ms while the rest took 80 and more. Each later packet acknowledged declared
the rest lost, and the window, a quarter of a millisecond a round trip, never caught up: BBR declared
921 packets lost, every one spurious.

The threshold is now RFC 9002's or the longest a spurious loss took, whichever is longer, with the
reordering window on top (`Reordering::time_threshold`). A packet as late as one that was declared
lost and arrived is not declared lost again: Chromium's adaptive time threshold raises its fraction
of the round trip until each spurious loss would not have been declared
(`GeneralLossAlgorithm::SpuriousLossDetected`); here the time a spurious loss took is the bound
itself, as QUIC's round trip under reordering is the overtaking packets'. Nothing changes until a
spurious loss: slates' bake-off against `ea7d228` changes only its reordering trial's rows.

Measured (`congestion::print_the_skipping_table`, `docs/benchmarks.md`): with 5% skipping, declared
losses BBR 921 → 21, NewReno 120 → 53, CUBIC 170 → 51, every one spurious, and the shares carried
NewReno 12.1 → 35.6%, CUBIC 13.2 → 51.7%, Copa 11.1 → 22.2%; with 25%, CUBIC 6.0 → 96.7%. BBR stays
near a quarter (21.8 → 24.0%): its window is twice its bandwidth times the least round trip, a
millisecond (`docs/STATUS.md`, open defects). Over jitter that reorders, ±40 and ±60 ms each way on
100 ms, NewReno 19.6 → 26.2% and 3.8 → 18.9%, CUBIC 30.7 → 43.8% and 7.2 → 50.5%; BBR 96.0–100.1%
where it carried 96.2–98.4% up to ±40 ms. Two cells fell: NewReno at ±20 ms, 41.0 → 34.6%, still cut
by each of its 16 spurious losses (§38's open item), and Copa at ±40 ms, 20.4 → 1.7%, at its
two-datagram floor, the jitter read as queue, as its twin kept in order sits at 19.4%.

Tests: `connection::reordering::tests::the_time_threshold_covers_the_longest_a_spurious_loss_took`;
`tests::a_packet_as_late_as_a_spurious_loss_is_not_declared_lost_again` (a PING held 50 ms is
declared lost and acknowledged after; one held 40 ms then is not declared; without Q30 it is, two
spurious losses where one is). hyper-sim's `Path::overtaking` lets a measured path's draws overtake,
for the table's skipping paths.

## 41. A pacer credits the same bytes however often it is asked (2026-10-23)

Patch Q31 departs from upstream's pacer (`connection/pacing.rs`). Asked whether a datagram may go and
short of tokens, it refilled its bucket for the time since it last did, truncated the credit to whole
bytes and moved its clock to the call: what a fraction of a byte took was lost at every call. A
connection asked again and again while it waited lost up to a byte each time, a share of its rate
that grows with how often it is asked: at 10 Mbit/s and 100 ms, its window's rule refilling 1.5625
bytes a microsecond, asked every microsecond for a millisecond it credited 1,000 bytes where 1,562
were bought (a rate a controller states, 1.25 a microsecond: 1,000 where 1,250). A busy endpoint asks
its connections often, and a connection woken while it waits for the pacer asks it each time.

The bucket now credits the whole bytes the time bought and moves its clock on by just the time they
took, rounded up; what a fraction of a byte took waits for the next refill (`Pacer::refill`). A bucket
that fills keeps nothing past its capacity, as before. A wait is the deficit's time from that clock,
rounded up, so it is never due before a whole byte more is bought (`Pacer::wait`): the window's rule,
five quarters of the window a smoothed round trip, and a stated rate share both, in integers, where the
window's rule computed in floating point.

slates' bake-off against Q30 (`cf9849b`): most rows move, every law's pacing credited exactly; the
summary is as before or better for every law. Ping p99 against the best, geometric mean: NewReno 1.408
→ 1.386, CUBIC 1.353 → 1.343, BBR 1.490 → 1.484, Copa 1.035 → 1.036; goodput shortfall NewReno 3.142 →
3.144, CUBIC 2.780 → 2.780, BBR 1.020 → 1.019, Copa 1.284 → 1.270; Copa's worst p99 3.12 → 2.48;
CUBIC stalled in 10 runs, where 11.

Tests: `connection::pacing::tests::the_credit_is_the_same_however_often_the_pacer_is_asked` (fails on
upstream's: 1,000 bytes for 1,562), `a_wait_is_never_due_when_asked`; every pacing test passes as it
was.

## 42. BBR draws its gain cycle's phases from the connection's seed (2026-10-23)

Patch Q32. BBR drew the phase its ProbeBW cycle starts in from the operating system's generator
(upstream's `Pcg32::from_rng(&mut rand::rng())`): BBR picks it at random so flows sharing a bottleneck
cycle apart (the BBR draft's randomized ProbeBW phase). Window-held, BBR's cycle moved nothing (§30),
so no run showed it; paced at its model's rate (§43), the phase moves what it sends, and a run the
simulation seeded whole no longer replayed: the same path, 1% of its packets skipping their delay,
carried 90.7, 90.0 and 89.3% in three runs. A path's controller now draws from the connection's seed,
its first eight bytes with the path's count added (`Congestion::build`'s `seed`, `Bbr::seeded`), not
from the connection's generator: a draw from it would move every draw after it. In production the
connection's seed is the operating system's, so flows still start their cycles apart; `Bbr::new` keeps
its random draw. The same path now carries 92.5% in every run.

## 43. BBR paced at its model's rate, its least round trip from packets in order (2026-10-23)

Patch Q33, in two parts measured together: each alone was worse than neither.

**BBR states its pacing rate** (`Bbr::pacing_rate`, Chromium's `BbrSender::PacingRate`: the gain of
its phase times its bandwidth, or, before it has one, the startup gain times the initial window a
least round trip). The port computed the rate and never gave it to the pacer, which sent five
quarters of the window a round trip (§30's open item): Drain's gain and ProbeBW's cycle moved
nothing, and BBR, held by its window of twice its product, kept about a product queued. At a
drop-tail queue of one product, 10 Mbit/s and 100 ms, it lost 4,978 of 25,476 packets where NewReno
lost 215, and 2,634 once §41 credited its pacer exactly. Paced, a bulk sender's longest round trip is 1.30 the path's at 1 ms each way where
window-held it is 2.98 (`a_paced_bulk_sender_keeps_about_its_bandwidth_delay_product_in_flight`, from
the attempt of 2026-10-15 that did not land for its warm-ups: here the warm-up tests pass 200 runs
of 200 each, the warm-up's rules changed since).

**A packet acknowledged while one sent before it is not gives BBR no round trip**
(`RateSample::rtt` zero, its delivery rate standing). On a path that reorders that packet is one that
overtook the rest, its round trip the fastest route's: with 5% of the packets skipping the delay, as
netem's `reorder` sends them, BBR's least round trip was a millisecond where the rest took 80, and
paced it carried 14.6% of the link (window-held, 27.0%). TCP samples the segment that fills a gap,
the late one, for the same reason (RFC 7323 §4.3). On a path that does not reorder, the packet before
is lost, and BBR's ten-second filter has the samples around it; the connection's own round trip, RFC
9002's, is sampled as before, so no other law sees a change. Gated on the tolerance of reordering
(§38) it was weaker: where reordering comes seldom, as with 1% skipping, the tolerance returns to the
RFC's between events and the next overtaking sample set the least round trip again (43.8% in a run
before BBR's draws were seeded, against 92.5%).

Measured, BBR (`congestion::print_the_reordering_table`, `print_the_skipping_table`; `docs/benchmarks.md`):

| Path | before (§41) | paced | paced, round trips in order |
|---|---|---|---|
| 10 Mbit/s, 100 ms, no jitter | 96.6% (2,634 lost) | 96.8% (195) | 96.8% (195) |
| ±20 ms each way | 98.5% (916) | 93.4% (55) | 97.1% (341) |
| ±40 ms each way | 96.0% (37) | 75.8% (43) | 96.2% (2,619) |
| ±60 ms each way | 11.2% | 10.3% | 95.0% |
| 80 ms, 1% skipping | 64.7% | 14.1% | 92.5% |
| 80 ms, 5% skipping | 27.0% | 14.6% | 96.3% |
| 80 ms, 25% skipping | 28.8% | 25.6% | 97.3% |

slates' bake-off against §41, every BBR row moved and no other law's: BBR's ping p99 against the best,
geometric mean, 1.484 → 1.125, its worst 4.00 → 1.81; its goodput shortfall 1.019 → 1.013; Jain's
index at 20 and 100 ms 0.533 → 0.829, still under slates' floor of 0.9.

Open: at ±40 ms BBR declares 2,619 packets lost where, its least round trip the fast tail's, it
declared 37. Its least round trip is now near the path's propagation, and BBR v1's window of twice
its product overflows a queue of one product when jitter bunches its acknowledgements; BBRv2's bound
on what is in flight after loss (`inflight_hi`, draft-ietf-ccwg-bbr) is the answer.

Tests: `congestion::bbr_at_a_queue_of_one_product_loses_no_more_than_newreno` (195 against 215; 2,634
before), `congestion::a_path_where_packets_skip_its_delay_is_carried_by_bbr_as_its_delays_kept_in_order`
(1% and 5% keep nine tenths of the same delays kept in order; 14.1% and 14.6% paced without the
filter), `congestion::bbr::tests::a_paced_bulk_sender_keeps_about_its_bandwidth_delay_product_in_flight`.

## 44. BBR reports the rate it paces at (2026-10-23)

`congestion/bbr/mod.rs`: `Controller::metrics` gave the model's pacing rate as it stood, zero until
its model had a bandwidth, where `Controller::pacing_rate`, what the connection paces by (§43), is
the start-up's until then (its gain times its first window a round trip) and nothing before a round
trip. The connection's statistics, and hyper-transport's `PathFacts::pacing_rate` through them, said
a BBR connection paced at zero bits a second while it paced at its start-up's rate. `metrics` now
gives `pacing_rate`'s. Test: `congestion::bbr::tests::the_rate_reported_is_the_rate_paced_at` (before:
`(None, Some(0))` where `(None, None)`).

## 45. A client told when its path is degrading (2026-10-25, found by focal)

`connection/mod.rs`, `connection/timer.rs`, `connection/packet_builder.rs`: quinn-proto tells its
application nothing between a peer that answers and a connection that idles out, so a client whose
server went without a word (a killed process, a host down, a path cut) learned it only at the idle
timeout: two minutes at focal's bus, whose client kept sending into a leader killed with kill -9
while the group had elected another. Chromium's QUIC tells its session (quiche,
`QuicNetworkBlackholeDetector`): a client's path is degrading once nothing it sent has been
acknowledged for four probe timeouts (`kNumProbeTimeoutsForPathDegradingDelay`, "kPDE4 is the
default. We have the experiments with kPDE4."; `GetPathDegradingDelay()`,
`num_ptos_for_path_degrading_ * GetPtoDelay()`), and the session may move to another path
(`OnPathDegrading`); it is told when the path makes progress again
(`OnForwardProgressMadeAfterPathDegrading`), and the connection is not closed. TCP has the same
step: past R1, at least three retransmissions, "TCP SHOULD inform the application of the delivery
problem ... when R1 is reached and before R2", the close (RFC 1122 §4.2.3.5).

The connection now runs quiche's detection on a client once its handshake is confirmed (its
Handshake keys discarded on HANDSHAKE_DONE): `Timer::PathDegrading` is armed when an ack-eliciting
packet is sent and the timer is not running, at four times the application space's probe timeout at
its current backoff; any packet newly acknowledged restarts it while ack-eliciting packets are in
flight, and quiescence stops it, where an acknowledgement leaves none in flight and where packets
declared lost by time leave none (`set_loss_detection_timer`, quiche's `StopDetection`). On expiry
the application is told `Event::PathDegrading` and no detection runs until the peer acknowledges a
packet, which tells `Event::ForwardProgress`. `PathStats::path_degradings` counts them. A server is
never told, as quiche's is not: it cannot go elsewhere. Tests (`tests/path_degrading.rs`): told
exactly four probe timeouts after the first packet nothing acknowledged, once however long the
silence, progress once the server answers and the data delivered, and again on a second silence;
never over a 100 ms round trip with a server that answers; never on the server; and nothing told
after a packet lost by time left nothing in flight (without the quiescence stop it told one four
probe timeouts after the lost packet's send, `(1, 0)`, found by
`a_retired_cid_routes_for_three_probe_timeouts`, whose drive the running timer kept from idling).
