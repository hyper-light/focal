# hyper-datagram: origin and design

The sealed datagram plane of mantle's `docs/design/node.md` §3.4 and note 32 §3.5 (step P-1).
Built new, from three sources:
- slates' control-plane seal, acceptance and key schedule (`crates/transport/src/{seal,accept,schedule}.rs`
  at slates `c4e2c52`);
- RFC 4303's anti-replay window;
- RFC 8085's rules for UDP applications.

## What it keeps from slates

- AES-256-GCM, with the cleartext prologue bound as associated data.
- Nonces that are a counter, never random, refused past the key's limit.
- The window's edge moving only after the tag verifies.
- Keys per direction from HKDF-Expand-Label.
- A cheap-first acceptance order.
- RFC 9001 §6.6's confidentiality (2^23) and integrity (2^52) limits for AEAD_AES_256_GCM.

## What it changes, and why

- **Key source.** An epoch's secret is exported from the TLS session of the QUIC connection to
  the peer (RFC 8446 §7.5), not an enrolled control secret. Every connection brings new keys, so a
  restarted node never reuses a nonce under an old key. This closes the hazard of slates'
  counter-zero `Enrollment::sealer` (mantle audit §11.8) and the secret distribution slates left
  owed.
- **Replay.** RFC 4303's sliding window replaces slates' drop-older high-water, which slates noted
  as owed.
  - It starts at 64 and widens only on authenticated late arrivals, up to
    `PlaneLimits::window_limit`.
  - The counter travels in the authenticated prologue.
- **Fencing.** The owner's `Fence` is consulted before any cryptography (slates: owed).
- **Integrity.** The sealed body carries a CRC-32C of its messages (mantle CLAUDE.md §6). A
  message whose tag verifies but whose checksum fails is `Refusal::Corrupt`, a fault to report, not
  a forgery.
- **Packing and rate.** Messages queue per peer and are sealed into at most one datagram per
  `flush`. No datagram exceeds the path size the transport reports, or 576 bytes before it does
  (RFC 8085 §3.2).
- **Crypto.** AWS-LC (`aws-lc-rs`) in place of RustCrypto's `aes-gcm` and `hkdf`: the provider
  shared with hyper-tls.
- **The wire is version 1** and is not slates' wire.
  - Its golden vector (`a_sealed_datagram_matches_its_golden_vector`) was verified against an
    independent implementation.
  - slates' golden vector, which note 32 named as P-1's gate, pins a format this plane replaces,
    so it no longer applies.

## Allocation

- Opening decrypts in place in the caller's buffer and allocates nothing.
- Sealing reuses one scratch buffer per plane.
- A peer's queue is reserved once, to its datagram size.

## Tests

- 19 unit tests:
  - the acceptance order;
  - replay, reorder and window widening;
  - forged and tampered datagrams;
  - epochs (restart under new keys, overlap, retirement, the bound);
  - peer and datagram bounds;
  - the confidentiality limit;
  - corruption versus forgery;
  - the golden vector.
- `tests/udp.rs`, over real UDP sockets on loopback:
  - two real processes (the test binary re-runs itself as the peer) exchange 50 sealed datagrams
    of four messages each, echoed and checked;
  - a third socket replays a captured datagram and forges a counter, both are refused, and honest
    traffic continues.

## Owed

- Allocation, reallocation and page-fault counts per seal and open, measured against slates'
  `Sealer` and `Opener` with the measurement crate under CLAUDE.md §1a.
- Each consumer wiring its exporter secret: slates on hyper-quic, focal, and mantle's node.
