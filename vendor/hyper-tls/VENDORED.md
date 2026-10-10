# hyper-tls: rustls 0.23.45, conformed

Source: `vendor/rustls`, the published crates.io archive. See its `VENDORED.md` for the checksum
and upstream commit.

The oracle is upstream's own test suite from the rustls repository at that commit (`2976d90`), which
the archive omits:
- `tests/`, upstream's `rustls/tests`;
- `tests/support/rustls-test`, upstream's `rustls-test` crate pointed at this one;
- `test-ca/`;
- `tests/data/rfc-9180-test-vectors.json`, from upstream's `rustls-provider-test`;
- `tests/data/fuzz-corpus/message`, from upstream's `fuzz/corpus/message`.

Every change from the archive is listed here, in order.

## 1. One build path (2026-10-01)

- **Package and library.** Renamed to `hyper-tls`; the library is `hyper_tls`. Tests and doc examples
  refer to `hyper_tls::`. The edition stays 2021 for now, because rustls's `impl Trait` returns
  borrow under 2021's capture rules; moving to 2024 is a later step.
- **Features collapsed.** `std`, TLS 1.2, aws-lc-rs, `prefer-post-quantum`, `logging`, and RFC 8879
  certificate compression with both brotli and zlib are always built.
  - Removed: the *ring* provider, FIPS, `no_std` (`hashbrown`, the no-std lock and ticketer
    paths), `custom-provider`, `read_buf`, and the nightly `#[bench]` modules and `verifybench`.
    Their code is deleted, not compiled out.
  - The four files the aws-lc-rs provider shared with *ring* through `#[path]` (`hash`, `hmac`,
    `kx`, `quic`) now live in `crypto/aws_lc_rs/`.
  - TLS 1.2 stays because S3 clients reach mantle's HTTP/1.1 listener over TLS 1.2 as well as 1.3.
  - brotli stays alongside zlib for two reasons: Chromium implements only brotli for RFC 8879, and
    upstream's test of the compression cache's LRU eviction needs both algorithms.
  - The manual's FIPS chapter and the crate docs' feature list are replaced by one paragraph
    stating the single build.
- **Tests that ran only with *ring* now run with aws-lc-rs:** the TLS 1.2 PRF and the TLS 1.3 HKDF
  known-answer tests. Their note that aws-lc-rs "doesn't provide hmac" was stale.
- **Tests dropped:** `webpki_supported_algorithms_is_debug`, which asserts the Debug text of *ring*'s
  algorithm table, and `default_suites_are_fips`.
- **`process_provider`** keeps its one case: the implicit default is aws-lc-rs.
- **Fixture paths** are fixed to `CARGO_MANIFEST_DIR`.
- **Oracle, measured on the result:**

  | Suite | hyper-tls | Upstream default build at `2976d90` |
  |---|---|---|
  | Unit | 246 | 235 |
  | api | 227 | 224 |
  | api_ffdhe | 5 | 5 |
  | client_cert_verifier | 4 | 4 |
  | ech | 2 | 2 |
  | key_log_file_env | 2 | 2 |
  | process_provider | 1 | 1 |
  | server_cert_verifier | 6 | 6 |
  | unbuffered | 27 | 27 |
  | Doctests | 15 (3 ignored) | 15 (5 ignored) |

  All pass. The extra unit and api tests are the ported known-answer tests and the
  compression-dependent tests, which upstream's default build leaves off.

## 2. Configuration without `Arc` (docs/transport.md §3.2)

1. **The crypto provider is `&'static CryptoProvider`.** A provider is tables of algorithm
   references; aws-lc-rs's is static data.
   - `ClientConfig::builder_with_provider`, `ServerConfig::builder_with_provider`, their
     `builder_with_details`, and `ConfigBuilder`, `ClientConfig` and `ServerConfig` hold and
     return `&'static CryptoProvider`.
   - The process default is a `OnceLock<&'static CryptoProvider>`:
     `CryptoProvider::install_default(&'static self) -> Result<(), &'static Self>` and
     `get_default() -> Option<&'static Self>`. The implicit default is
     `crypto::aws_lc_rs::DEFAULT_PROVIDER`, a `LazyLock` over `default_provider()`.
   - The webpki verifier builders read only the provider's signature algorithms, so they borrow
     it for the call: `builder_with_provider(roots, &CryptoProvider)`.
   - Tests build providers at runtime; they leak each one to get `'static`
     (`rustls_test::static_provider`, `crypto::static_provider` under `cfg(test)`). A program
     keeps its provider in a `static`.
2. **A connection holds no configuration.** The caller owns the configuration and lends it to
   each call that can advance a handshake.
   - `ConnectionCommon::process_new_packets(&mut self, config)` and `complete_io(io, config)`
     take `&mut ClientConfig` or `&mut ServerConfig`. `UnbufferedConnectionCommon` takes it in
     `process_tls_records(config, incoming)`; the kernel connection in
     `handle_new_session_ticket(config, payload)`; QUIC in `read_hs(config, plaintext)`.
   - `Stream` borrows the configuration beside the connection and socket:
     `Stream::new(conn, config, sock)`. `StreamOwned<C, T, F>` owns or borrows it through
     `F: BorrowMut<Config>`, and `into_parts` returns all three.
   - Constructors: `ClientConnection::new(&mut ClientConfig, name)` (starting a session takes a
     resumption ticket and the key-exchange hint from the stores), `ServerConnection::new(&ServerConfig)`,
     `Accepted::into_connection(&mut ServerConfig)`, and the QUIC and unbuffered equivalents.
   - The state machine's `Context` carries `config: &Settings` and the stores, `&mut` for the
     call; no handshake state has a `config` field.
   - The side-neutral `Connection` enum loses `process_new_packets` and `complete_io`: each needs
     its own side's configuration.
   - Deviation from §3.2: QUIC's `write_hs` takes no configuration. It only drains the flight a
     read already produced; it consults no setting and no store.
3. **Settings and stores are separated.** `ClientConfig` is `ClientSettings` (read-only, reached
   through `Deref`/`DerefMut`) plus its stores as public fields: `resumption`, `key_log`
   (`Box<dyn KeyLog>`) and `cert_compression_cache`. `ServerConfig` is `ServerSettings` plus
   `session_storage`, `ticketer`, `key_log` and `cert_compression_cache`. A call splits the
   configuration into `&Settings` and `&mut` stores, so a borrowed certificate key and a mutable
   store coexist in one call.
   - Every mutating store method takes `&mut self`: `StoresServerSessions::put`/`take`,
     `ProducesTickets::encrypt`/`decrypt`, `KeyLog::log`, and `ClientSessionStore`'s setters and
     takers. Lookups stay `&self`.
   - Their `Mutex`es are gone: `KeyLogFile`, `ServerSessionMemoryCache`,
     `ClientSessionMemoryCache`, the ticket rotator (rotation is `maybe_roll(&mut self, now)`),
     and the compression cache (`compression_for(&mut self)` returns a cached entry by reference
     or a fresh one by value). `src/lock.rs` and the `sync` alias module are deleted.
4. **Certificate keys are borrowed for the signing call.** `CertifiedKey` owns its
   `Box<dyn SigningKey>` and is no longer `Clone`; resolvers return `Option<&CertifiedKey>`;
   `SigningKey::choose_scheme` returns a `Box<dyn Signer + '_>` that borrows the key, and the
   aws-lc-rs key types own their key pairs. `KeyProvider::load_private_key` returns a `Box`.
   - §3.2's claim, checked against the state machine. Server, TLS 1.3: the certificate is
     selected and CertificateVerify signed while handling ClientHello. Server, TLS 1.2:
     selection and ServerKeyExchange, likewise. Both true.
   - Client: false. TLS 1.3 receives CertificateRequest in one flight and signs CertificateVerify
     in the call that handles the server's Finished; TLS 1.2 receives CertificateRequest and
     signs at ServerHelloDone. The smallest correct alternative: the client keeps the request
     (`ClientAuthRequest`: context, root hint subjects, signature schemes) and resolves its
     credentials in the signing call. Messages and their order are unchanged. Observable
     difference: the client's resolver now runs after the server's certificate is verified
     instead of on receipt of CertificateRequest, and not at all if the handshake fails between.
5. **Verifiers, resolvers and root stores are owned by their configuration**: `Box<dyn …>` for
   each verifier and resolver, `RootCertStore` by value (`with_root_certificates(RootCertStore)`,
   verifier builders take the store). `WebPkiServerVerifier`'s builder returns
   `Box<WebPkiServerVerifier>`; `WebPkiClientVerifier`'s, and `no_client_auth`, return
   `Box<dyn ClientCertVerifier>`. `ClientConfig`, `ServerConfig`, `ConfigBuilder`,
   `CertifiedKey` and `Resumption` are no longer `Clone`; an endpoint that needs a configuration
   builds its own.
6. **Session compatibility uses identities, not pointers.** Upstream refused to resume a session
   under a different verifier or client-certificate resolver by comparing `Arc` pointers (the
   session kept `Weak`s). Each installation now draws an `Identity` from a process-wide
   `AtomicU64` (relaxed; only uniqueness matters, and 2^64 installations do not occur), and a
   stored session records the two identities it was made under. `client_auth_cert_resolver` is
   private, set through `ClientConfig::set_client_auth_cert_resolver`;
   `dangerous().set_certificate_verifier` replaces the verifier. Both mint a new identity.
7. **`OtherError` owns its source**: `OtherError(pub Box<dyn ClonableError>)`, built with
   `OtherError::new`. `Error` is `Clone`, so the source is clonable (`clone_box`) instead of
   shared by `Arc`.
8. **Stored sessions own their data.** A client session's ticket and the server's certificate
   chain are owned values. A TLS 1.3 ticket moves out of the store, as before. A TLS 1.2 session
   stays in the store for reuse, so retrieving it copies the ticket and chain, where upstream
   bumped two reference counts: measured below, +2 allocations per resumed TLS 1.2 handshake.
9. **Tests.** Only the API changed in them; every assertion is unchanged.
   - `rustls-test` pairs each connection with its configuration (`TestClient`, `TestServer`,
     `TestUnbufferedClient`, `TestUnbufferedServer`, holding a `Shared` configuration,
     `Rc<RefCell<_>>`) and drives handshakes through them. Test doubles that the test inspects
     while a configuration owns them (session stores, key logs, verifiers) share their state
     through `Arc` in test code only.
   - One test's meaning changed: `client_only_attempts_resumption_with_compatible_security`.
     Its "allowed case, using `clone`" has no counterpart, because configurations are not
     `Clone`; it now resumes with the same configuration. Its two disallowed cases replace the
     resolver and the verifier on that configuration through the setters, and expect the same
     full handshakes and the same trace messages.
   - New: `tests/alloc_per_handshake.rs`, counting allocations per handshake.
   - Oracle after this change: unit 246, api 227, api_ffdhe 5, client_cert_verifier 4, ech 2,
     key_log_file_env 2, process_provider 1, server_cert_verifier 6, unbuffered 27, doctests 15
     (3 ignored), all passing, plus `alloc_per_handshake`.
10. **Allocations per handshake**, client and server together, counted by
    `tests/alloc_per_handshake.rs` on the macOS aarch64 development machine, debug profile,
    three runs each, against the same test adapted to 526c2cc's `Arc` API. Reallocations vary
    by ±2 between runs in both.

    | Handshake | 526c2cc | Now |
    |---|---|---|
    | TLS 1.3 full | 296 allocs, 34,590,450 B | 286 allocs, 34,590,126 B |
    | TLS 1.3 resumed | 219 allocs, 47,234 B | 213 allocs, 46,994 B |
    | TLS 1.2 full | 144 allocs, 29,543 B | 140 allocs, 29,556 B |
    | TLS 1.2 resumed | 99 allocs, 19,586 B | 101 allocs, 20,974 B |

    The full handshakes' bytes are dominated by the brotli compressor's tables. The TLS 1.2
    resumed rise is item 8's copy of the ticket and chain out of the store.

## 3. No panics in shipped code (2026-10-01)

The crate takes the workspace's lint table (`[lints] workspace = true`): no `unwrap`, `expect`,
`panic!`, `unreachable!`, `unimplemented!`, `todo!` or assert family, no indexing or slicing that
can go out of bounds, no overflowing arithmetic, no narrowing `as`, `cognitive_complexity` at most
10, every `const` documented. No item or crate in shipped code allows any of them; test code opts
out at the crate root (`#![cfg_attr(test, allow(...))]`, and each integration test's root).

### Classes of change

1. **`Error::Internal(&'static str)`** is new: an invariant of this implementation did not hold.
   It is never something a peer caused. Each site upstream panicked at for such an invariant now
   returns it: early-data state transitions, a PSK offer without exactly one identity and binder,
   a missing key share after TLS 1.3 was negotiated, an ECH config of the wrong version, a key
   exchange group claiming DHE without FFDHE parameters, deframer ranges outside their buffer,
   `handle_new_session_ticket` on a server, `complete_hybrid_component` without
   `hybrid_component`, and the like.
2. **Fallible signatures where a failure has a caller.**
   - `ClientConfig::builder`, `builder_with_protocol_versions` and the `ServerConfig` pair return
     `Result<ConfigBuilder<_, WantsVerifier>, Error>`; upstream unwrapped
     `with_protocol_versions`.
   - The TLS 1.3 key schedule (`KeySchedule*`, `derive_traffic_key`, `derive_traffic_iv`,
     `hkdf_expand_label*`), `HkdfExpander::expand_block`, `crypto::tls13::expand`,
     `tls12::Prf::for_secret`, `ConnectionSecrets` (key block, verify data, exporter),
     `CommonState::start_encryption_tls12` and `enqueue_key_update_notification`,
     `RecordLayer::encrypt_outgoing`, HPKE's labeled expansion and key schedule return their
     failure. `From<OutputLengthError> for Error` maps HKDF's.
   - `InboundOpaqueMessage::into_plain_message_range` and `Accepted::client_hello` return
     `Option`; `Iv::copy` returns `Option` for a slice that is not `NONCE_LEN` bytes.
   - `ServerConnection::set_resumption_data` and both `reject_early_data`s return
     `Result<(), Error>`.
3. **A refusing value where the trait has no error path.**
   - An AEAD key the provider rejects (unreachable: the key schedule derives `key_len()` bytes)
     gives `cipher::KeyRejected`, an encrypter and decrypter whose every record is
     `Error::Internal`; upstream unwrapped the key's construction.
   - A QUIC key whose derivation or construction fails gives `quic::RefusedKey`, whose every use
     is `Error::Internal`; `Secrets` keeps a `refused` flag so later key updates refuse too.
   - A record that fails to encrypt on a send path that cannot return is kept as the connection's
     encrypt failure and returned by the next `process_new_packets` (or unbuffered
     `process_tls_records`), which also poisons the state.
4. **Checked arithmetic and `get`.** Every offset and length the peer influences is checked:
   the deframer, `Reader`, the GCM and ChaCha20-Poly1305 record layers, HKDF, the TLS 1.2 key
   block (`KeyBlock::split`), `ChunkVecBuffer`. Counters that limit the peer (`TemperCounters`,
   the early-data allowance, `skip_data_left`) use `checked_sub`.
5. **Saturation where it is the stated meaning**, each with a comment: a required size
   (`encrypted_payload_len`, `check_required_size`, `encoded_len`), which can only over-state;
   capacity hints; byte counts and offsets bounded by an in-memory buffer; the RFC 9149 ticket
   count hint (uint8); a ticket lifetime hint; ages and the test-only epoch rewind.
6. **Length fields of encodings** (`LengthPrefixedBuffer`, `PayloadU8/16/24`, `SessionId`, SNI,
   the record header) take the low-order bytes of the length, as upstream's `as` casts did in a
   release build; the encoders' own limits keep every length within its field. `codec::low_u8`,
   `low_u16` and `low_u24` state this once.
7. **Fixed-capacity values** (`hmac::Tag`, `hash::Output`, `OkmBlock`) hold at most their
   `MAX_LEN` (SHA-512's 64 bytes) and keep that many bytes of a longer slice, where upstream
   panicked; no supported hash is longer. HKDF's zero salt and IKM come from one static
   (`zero_hash_len`).
8. **Debug-only assertions removed** where they checked an internal precondition the callers
   hold (record-layer direction states, early-traffic flags, the fatal-alert flag, key exchange
   state, the protocol side in the key schedule). Release builds never evaluated them, so no
   behaviour changes.
9. **Restructured so the type system proves the invariant**: `LoggedSecret` (only secrets with a
   key-log label can be logged), PSK identities and binders zipped after their lengths are
   checked, the revocation options built only from a non-empty CRL list, `RsaSigner::new` and
   the ECDSA public key returning `None` for a scheme they do not serve, array destructuring for
   nonces and AAD.
10. **Split for `cognitive_complexity`** into named steps, behaviour unchanged:
    `emit_client_hello_for_retry` (`offered_versions`, `client_hello_extensions`,
    `offer_key_shares`, `offer_tls13_extensions`, `offered_cipher_suites`, `apply_ech`,
    `derive_early_secret`), `ExpectServerHello::handle` (`server_hello_version`),
    `EchState::encode_inner_hello` (`inner_hello_from`, `pad_inner_hello`), server
    `handle_client_hello` (`check_client_hello`, `check_second_hello`, `retry_for_key_share`,
    `choose_psk`, `settle_resumption`, `next_state`, `emit_server_authentication`,
    `install_handshake_decrypter`), and `process_tls_records_common` (`process_next_message`,
    `idle_state`).
11. **Every `const` in `src/` carries a `///`** with its derivation or citation
    (`scripts/check-contracts.py`). Upstream's tunables keep upstream's values and say so.
13. **No `allow(missing_docs)`**: upstream allowed it on `enums`, `msgs`, `internal`,
    `PeerMisbehaved` and `PeerIncompatible`. `enum_builder!` now documents each variant with its
    registered name and wire value, `Unknown` and its two methods; each `PeerMisbehaved` and
    `PeerIncompatible` variant states its name as a sentence (upstream does not say more about
    them, by design); the `msgs` items `internal` re-exports carry their own docs.
12. **Tests**: the in-crate `unsafe` in `alloc_per_handshake` is replaced by
    `hyper_measure::alloc::Counting` (a dev-dependency); `key_log_file_env` calls `env::set_var`
    without `unsafe` (edition 2021); the `read_buf` attributes of the removed feature are gone.
    `key_log_file_env` writes its key log to a file in the test's own target directory, under its
    process id, removed when the test lets it go: upstream's `./sslkeylogfile.txt` was left in the
    crate's directory by every run.

### Behaviour changes: former panics and what they are now

| Reached by | Upstream | Now |
|---|---|---|
| **A peer**: a server's HelloRetryRequest selecting a TLS 1.2 cipher suite to a client offering ECH | `unreachable!` in `handle_hello_retry_request` | `illegal_parameter` alert, `PeerMisbehaved::SelectedUnusableCipherSuiteForVersion` (`client::test::test_ech_client_rejects_hrr_selecting_tls12_suite`, which panics on upstream's code) |
| The API: `ClientConfig::builder()`/`ServerConfig::builder()` with a process provider that cannot serve the versions | `unwrap` | the `Error` from `with_protocol_versions` |
| The API: a TLS 1.2 exporter context of 2^16 bytes or more | `assert!` | `Error::General` (`test_tls12_exporter_refuses_context_longer_than_uint16`) |
| The API: a TLS 1.3 exporter label longer than 249 bytes | the label's length byte was truncated: a wrong key, silently | `Error::General` (`test_tls13_exporter_refuses_label_longer_than_249`) |
| The API: `set_resumption_data` with 2^15 bytes or more | `assert!` | `Error::General` |
| The API: `reject_early_data` after the handshake | `assert!` | `Error::General` |
| The API: `BufRead::consume` past what `fill_buf` returned | `assert!` | consumes what it returned, as std's `BufReader` does |
| A provider: `Hmac`/`Hkdf` with a tag longer than 64 bytes, a zero-length tag, an AEAD key or IV of the wrong length | slicing, `chunks_mut(0)`, `unwrap` | `Error::Internal`/`OutputLengthError`, or the refusing cipher or key |
| HPKE: the u32 sequence number at its end | overflow (debug) or wrap, reusing a nonce (release) | the context refuses |
| Every other site | `unwrap`, `expect`, `unreachable!`, assert, indexing | `Error::Internal`, unreachable by construction |

### Oracle

Unit 247 (246 and the ECH HelloRetryRequest test), api 229 (227 and the two exporter tests),
api_ffdhe 5, client_cert_verifier 4, ech 2, key_log_file_env 2, process_provider 1,
server_cert_verifier 6, unbuffered 27, alloc_per_handshake 1, doctests 15 (3 ignored): all pass.
Upstream's assertions are unchanged; the tests changed only where the API did (`.unwrap()` on
the new `Result`s and `Option`s).

### Allocations per handshake

`tests/alloc_per_handshake.rs`, now counting with `hyper_measure::alloc`, macOS aarch64
development machine, debug profile, three runs (reallocations vary by ±1, bytes by ±300, since
`hyper_measure` also counts what each reallocation grew):

| Handshake | §2 | Now |
|---|---|---|
| TLS 1.3 full | 286 allocs | 286 allocs, 45 reallocs, 34,602,141 B |
| TLS 1.3 resumed | 213 allocs | 213 allocs, 41 reallocs, 55,706 B |
| TLS 1.2 full | 140 allocs | 140 allocs, 32 reallocs, 32,832 B |
| TLS 1.2 resumed | 101 allocs | 101 allocs, 17 reallocs, 21,766 B |

No hot path gained an allocation. The TLS 1.2 resumed +2 against 526c2cc (§2 item 8) is still
owed (closed in §5). Its cause: `ClientSessionStore::tls12_session` hands out an owned
`Tls12ClientSessionValue`, so the ticket and the server's certificate chain are copied out of a
store that keeps them for the next connection, where upstream bumped two reference counts. The
ticket copy could go if every ClientHello borrowed it from the store in the call that sends it,
but a store shared by connections may replace it between a ClientHello and its retry, which RFC
8446 §4.1.2 forbids changing. The chain has two owners, the store and `peer_certificates`;
without shared ownership one of them copies, unless `peer_certificates` borrows the
configuration's store, an API change left for the consumers to ask for.

## 4. The environment, stated (2026-10-02)

The workspace denies `Instant::now`, `SystemTime::now`, thread spawns and environment reads
(`clippy.toml`, `docs/sim.md` §3.9). Upstream reads `SSLKEYLOGFILE` in `key_log_file.rs`, only when
an owner installs `KeyLogFile`; it is kept and allowed at the site with its reason. No behaviour
changes.

## 5. TLS 1.2 sessions lent by their store (2026-10-03)

§3's owed count, closed at its cause. `ClientSessionStore::tls12_session` handed out an owned
`Tls12ClientSessionValue`, so every TLS 1.2 resumption copied the session out of the store: its
ticket, its master secret, and the server's certificate chain (the vector and each certificate),
where upstream bumped two reference counts. A resumed handshake then stored a new value made of
the same chain, copied again from `peer_certificates`.

1. **The store lends.** `tls12_session(&mut self, &ServerName<'static>) ->
   Option<&Tls12ClientSessionValue>`. In the call that sends the ClientHello
   (`ClientConnection::new`), the client reads the session where it is: its session ID, or a random
   one when it offers the ticket (RFC 5077 §3.4), and the ticket, copied into the ClientHello's
   extension as upstream copied it. The connection keeps only the session's `SessionStamp`, an
   `Identity` drawn for each session.
2. **A retry repeats the first ClientHello's ticket.** RFC 8446 §4.1.2 lets a ClientHello sent
   after a HelloRetryRequest change only its key shares, cookie, early data, PSK and padding. The
   first ClientHello's session ticket extension is moved out of the message once its bytes are
   encoded and kept in the connection's `ClientHelloInput`, and the retry offers it again, whatever
   the store did meanwhile, without a copy.
3. **The server's answer finds the session by its stamp** (`lent_tls12_session`), in the call that
   takes the ServerHello, and copies the chain into `peer_certificates`: the one copy upstream made
   too.
4. **The store keeps a session it displaced after lending it.** Between the ClientHello and the
   ServerHello the owner may drive other connections with the same configuration, which save
   another session for the server, remove it (a resumption that failed to decrypt), or evict the
   server from the cache. `ClientSessionMemoryCache` marks a session lent when it lends it, and
   moves a lent session it displaces into a queue it searches by stamp: at most as many sessions as
   its bound on servers (`size` / 8, rounded up), the oldest pushed out first, the queue's memory
   reserved when the cache is made. An insertion into `LimitedCache` now returns the entry it
   evicted, so an eviction displaces like a replacement or a removal. A resumption whose session was
   pushed out before its server answered fails with the new `Error::ResumedSessionLost`, sending an
   `internal_error` alert (RFC 5246 §7.2.2): a stated bound, typed at its edge, where a store of
   values shared by reference counts had no bound to state. A store of another implementation that
   keeps nothing it displaced gives that error whenever its session is displaced before the answer.
5. **A resumed session is renewed where the store keeps it** (`current_tls12_session`,
   `Tls12ClientSessionValue::renew`): the session ID the server echoed, the ticket it issued if it
   issued one, and the lifetime it gave, from now. Its chain, master secret, suite, and the verifier
   and resolver identities it was made under stay as they were. Upstream stored a new value,
   copying the chain from `peer_certificates` and stamping the identities the configuration had at
   that moment. A session the store displaced meanwhile is saved anew as upstream saved it, with the
   ticket the ClientHello offered if the server issued none; it then displaces the newer one, as
   upstream's last save did.
6. **Only the states a resumption reaches carry the session**, as a `Resumed { stamp,
   offered_ticket }`. Every state of a full handshake carried an empty
   `Option<Tls12ClientSessionValue>` before, about 150 bytes in each boxed state.
7. **Two copies the cache made of server names, gone**: `set_tls12_session` and
   `insert_tls13_ticket` cloned the name they were given before inserting it, an allocation for
   every session or ticket kept under an owned name; it is moved in. A TLS 1.3 ticket, which moves
   out of its store and is spent by the handshake that offers it, gives its chain to
   `peer_certificates` by move, where upstream copied it out of the shared session.

API changes: `ClientSessionStore::tls12_session` lends, through `&mut self` (to mark the lend) and
a `'static` name, as `take_tls13_ticket` and `remove_tls12_session` already take; the trait gains
`lent_tls12_session` and `current_tls12_session`; `Tls12ClientSessionValue` is no longer `Clone`
and has `stamp()`; `SessionStamp` and `Error::ResumedSessionLost` are new. A store behind a lock
cannot lend a reference, so `tests/api.rs`'s `ClientStorage` keeps TLS 1.2 sessions per instance and
shares only its operation log, key exchange hints and TLS 1.3 tickets between clones; no assertion
changed.

### The test

`tests/alloc_per_handshake.rs` now runs upstream rustls 0.23.45, unmodified (the dev-dependency
`upstream-rustls`, whose checksum in `Cargo.lock` is the archive's SHA-256 that
`vendor/rustls/VENDORED.md` records, built with this crate's one build path: `aws_lc_rs`,
`brotli`, `logging`, `prefer-post-quantum`, `std`, `tls12`, `zlib`), beside hyper-tls in one
process, with one counter,
over eleven handshake shapes with ECDSA P-256 and with Ed25519 credentials. Both providers draw
their random bytes from one SplitMix64 stream reseeded for each run. A shape's allocations are the
same on every run, so they are held to upstream's exactly: no shape may allocate more than
upstream's does. With Ed25519 every size in a handshake is fixed, so reallocations and bytes are
the same on every run too (five runs, identical), and they are held to upstream's as well. With
ECDSA they vary with the signatures' DER lengths and are recorded below. Before this change the
test failed: TLS 1.2 resumed 101 allocations against 99, TLS 1.2 resumed by ticket 117 against 115,
declined 144 against 143, and bytes over upstream's in six Ed25519 shapes, from 8 B (TLS 1.2 full,
the states' empty sessions) to 1,298 B (TLS 1.2 resumed by ticket). It passes now.

New tests besides: `tls12_ticket_is_offered_again_after_a_retry_though_its_store_replaced_it`,
`a_tls12_session_displaced_after_it_was_lent_still_resumes` (which fails with the queue taken out),
`a_tls12_session_pushed_out_of_its_store_fails_the_resumption_typed`, and the cache's
`test_lent_tls12_session_outlives_its_servers_eviction`,
`test_unlent_tls12_session_is_not_kept_once_displaced` and
`test_current_tls12_session_is_only_the_current_stamp`.

### Allocations per handshake

Client and server together, debug profile, macOS aarch64 development machine; allocations the same
in every run of either credential, reallocations and bytes for Ed25519, whose runs are identical:

| Shape | Upstream 0.23.45 | Before | Now | Now, reallocations and bytes (upstream's) |
|---|---|---|---|---|
| TLS 1.3 full | 296 | 286 | 286 | 44, 34,578,758 B (44, 34,579,070 B) |
| TLS 1.3 resumed | 219 | 213 | 209 | 43, 54,194 B (43, 55,636 B) |
| TLS 1.3 with a HelloRetryRequest | 325 | 315 | 315 | 60, 34,561,398 B (60, 34,561,718 B) |
| TLS 1.3, both sides authenticated | 423 | 411 | 411 | 72, 69,126,558 B (72, 69,127,038 B) |
| TLS 1.2 full | 144 | 140 | 140 | 31, 30,215 B (31, 31,415 B) |
| TLS 1.2 resumed by session ID | 99 | 101 | 91 | 17, 18,332 B (17, 19,942 B) |
| TLS 1.2 full, the server issuing tickets | 156 | 151 | 151 | 38, 31,864 B (38, 33,224 B) |
| TLS 1.2 resumed by ticket | 115 | 117 | 106 | 25, 21,295 B (25, 23,065 B) |
| TLS 1.2 resumption declined | 143 | 144 | 139 | 31, 29,159 B (31, 30,471 B) |
| TLS 1.2, both sides authenticated | 184 | 180 | 180 | 52, 47,991 B (52, 49,495 B) |
| A TLS 1.2 ticket offered, then a TLS 1.3 retry | 328 | 324 | 317 | 61, 34,562,062 B (61, 34,562,654 B) |

The full handshakes' bytes are the brotli compressor's tables. hyper-quic's handshakes, on this
crate, went from 494 to 492 allocations (full) and from 501 to 497 (resumed): its client's
tickets are stored under an owned name, and its resumed ticket's chain moves.

### Oracle

Unit 250 (247 and the cache's three), api 232 (229 and the three above), api_ffdhe 5,
client_cert_verifier 4, ech 2, key_log_file_env 2, process_provider 1, server_cert_verifier 6,
unbuffered 27, alloc_per_handshake 1, doctests 15 (3 ignored): all pass.

## 6. End to end, against upstream (2026-10-03)

`tests/e2e.rs` (`harness = false`): real processes over real TCP sockets on loopback (CLAUDE.md
§1a). The test binary is the client and spawns itself as each scenario's server, a process that
shares nothing with it but the kernel's sockets; the server serves the scenario's connections in
turn and reports each on its standard output. Every scenario runs three ways: hyper-tls on both
sides, a hyper-tls client against an upstream rustls 0.23.45 server (the `upstream-rustls`
dev-dependency), and an upstream client against a hyper-tls server.

| Scenario | Connections |
|---|---|
| `tls13`, `tls12`, `tls12-tickets` | a full handshake, then a resumed one: by ticket under TLS 1.3, by the server's session cache or by ticket under TLS 1.2 |
| `mutual13`, `mutual12` | the same with a client certificate the server requires; the server reports the certificate it verified, in the resumed handshake too |
| `untrusted-server13`, `untrusted-server12` | the server's chain from an authority the client does not trust: `UnknownIssuer` at the client, `AlertReceived(UnknownCA)` at the server |
| `untrusted-client13`, `untrusted-client12` | the client's chain from an authority the server does not trust: `UnknownIssuer` at the server, `AlertReceived(UnknownCA)` at the client |
| `missing-client13` | no client certificate where one is required: `NoCertificatesPresented` at the server, `AlertReceived(CertificateRequired)` at the client |
| `wrong-name13` | the server's certificate does not name the server asked for: `NotValidForName` at the client, `AlertReceived(BadCertificate)` at the server |

A connection that completes moves a mebibyte each way, every byte checked on both sides, and both
sides agree on its kind (`Full` or `Resumed`) and version. A refused one is refused on both sides
with each implementation's typed error, the alert the refusing side sent being the error the other
reports. Neither side closes its socket with bytes unread, which would reset the other's stream and
could lose the alert in it: each ends its writing and reads to the end of the other's.

Every wait is for a fact: bytes from the peer, the end of its stream, or its process ending. The
one wall-clock bound is a failure guard on each socket read (120 s), far past what a handshake on
loopback needs, so that a wedged peer fails the test instead of hanging it.

All 33 runs pass, in 2.8 s for the binary (debug profile, this machine), and 20 repetitions at load
23–24 passed every one: a full TLS 1.3 handshake with a mebibyte each way in 36–74 ms, a resumed
one in 1.7–2.8 ms.

## 7. Tickets with the server's Finished over QUIC (2026-10-04)

RFC 8446 §4.6.1: "a server which does not request client authentication MAY compute the remainder
of the transcript independently and then send a NewSessionTicket immediately upon sending its
Finished rather than waiting for the client Finished." Over QUIC a client sends no EndOfEarlyData
(RFC 9001 §8.3), so on a handshake that does not request a client certificate its second flight is
its Finished alone. `server::tls13` now computes that Finished (`client_finish_ahead`) and the
resumption secret it gives (`resumption_ahead`), and writes the tickets after its own Finished, to
go under the 1-RTT keys (`Quic::one_rtt_flight`, released by `write_hs` once those keys are passed
on, since RFC 9001 §4 puts NewSessionTicket in 1-RTT packets). The transcript the client's Finished
is checked against is left as it was, and a client whose Finished differs is refused as before.

The reason is measured (`docs/benchmarks.md`): a resumed dial that closed at its first reply, one
round trip in, closed half a round trip before tickets sent after the client's Finished arrived, so
with the two tickets of the first dial spent the fourth dial fell back to 1-RTT. More tickets per
full handshake only defer that, since each such dial spends one and gets none; tickets in the first
flight replace the one each dial spends. A handshake that requests a client certificate, and TLS
over TCP, send them after the client's Finished as upstream does; `ExpectFinished` writes no flight
when none are left to send.

The cost is one allocation a handshake, the fork of the transcript's hash the resumption secret is
derived from; the predicted Finished is hashed in place. hyper-quic's
`allocations_per_handshake` (client and server together, two runs each): 490 to 491 allocations for a
full handshake and 495 to 496 for a resumed one, reallocations 55–57 before and 54–55 after. Upstream's suite passes unchanged;
`crates/hyper-quic/tests/geo.rs`'s `every_resumed_dial_closed_at_its_reply_leaves_a_ticket_for_the_next`
failed before it.

## 8. SecP384r1MLKEM1024, checked against OpenSSL (2026-10-05)

`kx_group::SECP384R1MLKEM1024` (codepoint 0x11ed, draft-ietf-tls-ecdhe-mlkem): ML-KEM-1024 with
ECDH over P-384, the classical share and secret first, as SecP256r1MLKEM768. hyper-quic prefers it
between nodes (hyper-raft docs/seal.md §10). `tests/openssl_interop.rs` checks it against OpenSSL
3.6.4 (Homebrew, macOS, aarch64), each side offering that group alone, over TCP, both ways: a hyper-tls
client against `openssl s_server`, and `openssl s_client` against a hyper-tls server, each asserting
the group negotiated and the chain verified. Both pass. CI's runners carry OpenSSL 3.0, which has no
ML-KEM, so the tests are `#[ignore]` and run by hand: `cargo test -p hyper-tls --test
openssl_interop -- --ignored`. slates ships the same group, checked against OpenSSL 3.5.7 both ways.
