# SPEC: wire security — the seal-once pipeline, mechanics and implementation

Status: ACCEPTED 2026-08-18 (D-10(a) settled through the six-round grilling
arc; mechanics verified against primary sources — dossier in GRILLING.md;
spec presented in-message and accepted). Amended 2026-08-20: §6 registers the
QUEUE/FANOUT queue + topic stream classes and the CACHE / pub-sub ephemeral notify
flow — the latter on the existing `DATAGRAM_SUPERSEDE` frame class (new flow
identities only, no new frame class). Companions: PROTOCOL.md (envelope,
classes, §1.3 order), PODS.md (devices, warden), CONSENSUS.md §7
(externalization fencing). Owns: the egress staging device, the warden seal
pipeline, flow keys, the sealed-payload hop classes, the guest receive path,
the boot classifier.

## 1. The laws (settled, restated once)

One payload seal at the origin authority, one payload unseal at the
destination; hosts touch integrity only. The warden seals what it inspected.
Every verdict is metadata-decidable (payload plaintext may tighten, never be
required). The five-layer default applies to every connection on both
stacks; an unclassified transport path fails boot.

## 2. The egress staging device (`hecate-egress`)

**Precedent**: this device is structurally **vhost-net's TX path with
"inspect + seal" as the network stack** — the backend reads guest-authored
frames from a virtqueue via its mapping of guest RAM, exactly as every vhost
backend does (OASIS virtio 1.2: descriptors carry guest-physical addresses;
libkrun: `GuestMemoryMmap` + the MMIO `BusDevice` trait its eight in-tree
devices already implement, virtio-fs among them — a strictly more complex
device than this one). Shipping a project-private device is precedented
(virtio-wl ran on millions of Chromebooks unstandardized); owning both the
fork and the guest image removes even that friction.

- **Transport**: virtio-MMIO in the libkrun fork. **Two virtqueue pairs**
  (control/claims, bulk-envelope) so bulk posting never head-of-line-blocks
  claims posts. **Split virtqueues, no indirect descriptors** — the only
  configuration the TDX hardening effort audited and fuzzed; adopted for
  both directions.
- **Slot protocol**: guest writes one frame image per descriptor chain —
  `{slot_hdr: {frame_len: u32, class: u8, flow_dst: u64, reserved},
  envelope_image, payload}` — and advances the available ring; kick via
  doorbell with **VIRTIO_F_EVENT_IDX suppression** (spec-standard
  bidirectional kick suppression; the warden batch-polls under load,
  re-arms when idle). After posting, the slot is protocol-frozen (guest
  must not write; violation is harmless — see the invariant).
- **The copy-once invariant (compiler-proof, spec law — XSA-155's
  lesson)**: the warden reads `frame_len` **exactly once** via volatile
  load, caps it against the ring's derived frame budget, then moves exactly
  that many bytes into private host memory via raw-pointer volatile copy
  (`ptr::read_volatile` / `copy_nonoverlapping`). **Creating any
  `&T`/`&[u8]` borrow into guest-mapped memory is lint-banned** —
  XSA-155's second fetch was *compiler-introduced* through exactly such an
  alias, and it yielded arbitrary code execution on the management domain
  (CVE-2015-8550; the canonical fix, Xen's `RING_COPY_REQUEST`, is this
  invariant). All parsing, inspection, and sealing operate on the private
  copy; a guest mutating the slot post-copy mutates nothing the warden
  uses.
- **The warden pipeline (one pass per frame)**: copy → parse (§1.3 order:
  length caps → envelope → fencing — `src_pod` must equal the ring's
  owning pod, physics-checked) → policy verdict (compiled local policy;
  metadata-complete by law) → on ALLOW, **seal the private copy** with the
  flow key (AEAD over envelope‖payload, per-flow counter nonce) and hand
  to the host router; on DENY, used-ring completion with typed reason; on
  HOLD, escalation ticket (hold-and-escalate, unchanged). **Sealing runs
  off the device event loop** (libkrun's EventManager is single-threaded;
  AEAD work on it would head-of-line-block other devices) — the warden's
  seal stage is its own hecate-rt task per the task-lifecycle law.
- **Throughput reality (receipted)**: copy ≈ memory bandwidth (~10 GB/s
  conservative single-core — derived bound, flagged THIN); seal = AES-GCM
  at 0.64 cpb Skylake-class → 4–5 GB/s/core, toward 10+ with VAES (Gueron,
  eprint 2018/392). Neither is a bottleneck at claims-frame rates; both
  are ratchet-measured at first light, never assumed.

## 3. Flow keys

- **Derivation (RFC 5869 / SP 800-108 — the KDK pattern TLS 1.3 and SigV4
  run at planetary scale)**:
  - At summon, pod Q's host mints root `R_Q`. **Per-workload roots (amended
    2026-08-22, the five-site flow-key landing — supersedes the guest-global
    root)**: `recv_root_{Q,w} = HKDF(R_Q, "hecate/flow/recv-root" ‖
    encode(workload_id_w))` for each of Q's four workloads {primary, Scribe,
    sensor, init}; **hecate-init installs each root ONLY into that
    workload's container fd table at spawn** (custody separation — no
    guest-global receive root exists; the primary cannot derive the
    Scribe's receive keys). Attribution is thereby **re-derivable from the
    mint record alone** — the derivation path IS the identity; no mutable
    table sits on the attribution chain.
  - Receive key, derived *by the owning workload itself, no key delivery*:
    `K(P_w→Q_v) = HKDF(recv_root_{Q,v}, encode(P_uid) ‖ encode(w) ‖
    encode(epoch_P) ‖ encode(epoch_Q))` — **all context fields
    fixed-width-encoded** (SP 800-108's explicit failure mode: ambiguous
    context concatenation; our encoding is hecate-wire canonical,
    unambiguous by construction).
  - Seal key, host-side: host(P) obtains `K(P_w→Q_v)` from host(Q) via
    `FlowKeyRequest{P_uid, w, Q_uid, v, epochs} → FlowKeyGrant{key,
    grant_epoch}` over the host↔host session — cached, invalidated on any
    epoch bump (one `key_epoch` per pod; all four workload keys re-derive
    together at rotation; an in-pod respawn keeps workload id and epoch —
    custody unchanged, no rotation).
  - **The key-hint law**: the cleartext prologue's `key_hint` names the
    per-workload key — workload-granular **by construction**, since that
    is what finds the key. **Hints are never coarsened to pod granularity**
    — that "optimization" would erase the attribution the derivation
    carries (a permanent prohibition, tested).
  - **The host authentication point binds frame → workload** and stamps
    actor attribution host-side (`HostObserved` — the lie-detector's
    actor-attribution row gets its host veto here): identity is only ever
    proven by key possession, never asserted in data.
    **Epochs are explicit counters carried in the grant protocol, never
    wall-clock** — deleting Kerberos's clock-skew failure mode instead of
    inheriting it.
- **The broker argument (carried in-spec because deployed precedent is
  THIN — one 2025 arXiv paper and patents)**: the grant broker is the
  destination's host, which already minted `R_Q`, maps pod Q's entire RAM,
  and runs its warden. Kerberos's golden-ticket catastrophe is a KDC
  *gaining* authority over endpoints it didn't otherwise control; here the
  broker's compromise grants **zero marginal authority** — a compromised
  host already owns every flow touching its pods. The failure analysis,
  not a citation, is the justification, stated as such.
- **Nonces**: seal-side is single-sealer per flow (the warden) — 64-bit
  monotone counter per (flow, epoch); **counter state is never persisted
  or resumed — any restart mints a new epoch and thus new keys** (the
  crash-safety rule that makes reuse structurally impossible). Guest-side
  replay window = sliding window over the counter, per flow.

## 4. The hop layer (private QUIC version — frame classes, per-lane integrity)

- `CTRL` (transport control: acks, credits, grants): full packet
  protection.
- `SEALED_FRAME/claims`: payload = warden-sealed frame; hop adds **GMAC
  over the payload** (NIST SP 800-38D authentication-only GCM —
  standardized, ~half of full-GCM cost, GHASH at PCLMUL speeds) so wire
  corruption is transport-detected and retransmitted, preserving
  never-shed semantics. **The GMAC nonce law (the one crypto landmine —
  Joux's forbidden attack: a single nonce reuse under one hop key =
  unlimited forgery)**: nonces bind to the QUIC packet number space —
  monotone by construction, crash-safe because a new connection = new
  keys, rotated within the NIST invocation limits the 800-38D revision is
  tightening. No random nonces, no resumed counters, anywhere.
- `SEALED_FRAME/bulk`: payload exempt from the hop MAC — **with the guard
  as law** (MASQUE forwarded-mode's security considerations as the
  checklist): name-verification precedes any parse or use of chunk bytes,
  and the *envelope layer* detects truncation and reordering of chunk
  streams (offsets/lengths under the hop-authenticated envelope). SRTP's
  discipline is the shape: intermediaries never handle unauthenticated
  *metadata* even when payload protection is end-to-end (RFC 3711); IPsec
  AH (RFC 4302) is the auth-without-encrypt standard this lane
  instantiates.
- `DATAGRAM_SUPERSEDE`: RFC 9221-shaped, CC-governed, no retransmit —
  supersession lanes.
- The bare-UDP control plane is untouched (already at standard).

## 5. The guest receive path

Host side: hop MAC verify → §1.3 order → envelope admission (ingress
warden, metadata-complete) → sealed frame into the pod's `hecate-ingress`
ring (same device discipline as §2: split queues, no indirect descriptors,
EVENT_IDX).
Guest side: a **purpose-built parser, not a general driver stack** (VIA,
ACSAC 2021: 50 bugs in 22 general-purpose Linux drivers under
malicious-device fuzzing — the receipt for keeping this path small):
length caps → envelope parse → per-flow replay window → unseal (`K(P→Q)`,
guest-derived) → tag verify → decode → dispatch. Failures are typed,
counted, and reported host-ward on the egress ring (a guest silently
swallowing failures is visible in sensor telemetry deltas). The TDX
guest-hardening posture ("all virtio input from the host must be
considered untrusted") is adopted verbatim as defense-in-depth symmetry, a
strictly weaker use than its CC origin.

## 6. The boot classifier (`hecate-transport-registry`)

Sockets, sessions, stream classes, and flows are constructible **only**
through `TransportRegistry::open_*` APIs that require the classification
tuple `(hop_kind, lane, enforcement_point, flow_identity)`; direct socket
construction is lint-banned (the same wall as `Arc`). Boot walks the
registry against the declared table; an unclassified path fails startup.
This is the chokepoint-coverage law applied to transport, as settled.
**`flow_identity` carries the workload dimension** (amended 2026-08-22):
a flow is identified `(pod_uid, workload_id, …)`, matching the §3
per-workload derivation; the declared table gains the **Scribe-flow** rows
(authority-in, alert/handoff-request out, Archivalist-flush bulk) and
confirms the sensor row — each with its own enforcement point.

**Sibling-primitive registrations (QUEUE, FANOUT, CACHE pub-sub — 2026-08-20; no new
frame class):**

- **Queue delivery** — `(SEALED_FRAME/claims, claims, host-parser + terminal-client,
  flow=queue.deliver)`: at-least-once durable delivery to the non-recoverable
  hand-off boundary (`QUEUE.md`) rides the retransmit-preserved claims lane; a large
  body rides the §4 bulk lane by `ContentRef`, unchanged — not a new class.
- **Topic delivery** — `(SEALED_FRAME/claims, claims, host-parser,
  flow=topic.deliver)`: a durable subscription *is* a queue partition (`FANOUT.md`
  §7), inheriting the queue delivery classification above — registered once, never
  double-classified.
- **Pub-sub ephemeral notify** — `(DATAGRAM_SUPERSEDE, supersede, host-parser +
  guest-unseal, flow=pubsub.notify)`: ephemeral at-most-once fire-and-forget (CACHE
  pub-sub §7; FANOUT ephemeral subscriptions; cross-node cache invalidation §4.1)
  rides the **existing** `DATAGRAM_SUPERSEDE` frame class — **no new frame class**,
  only new flow identities (`pubsub.notify`, `cache.invalidate`).

## 7. Implementation plan (crates, phases, gates)

Crates: `hecate-noise` (handshake), `hecate-flowkey` (derivation +
grants), `hecate-egress`/`hecate-ingress` (fork devices + guest drivers in
hecate-init), `hecate-warden-seal` (copy-inspect-seal pipeline),
`hecate-transport-registry`. The transport state machine crate is
D-10(b)'s decision.

| Phase | Delivers | Gate (all seed-replayable in SIM) |
|---|---|---|
| P-a1 | Noise-IKpsk2 core in CRYPTO frames (nQUIC shape), SIM-driven | bit-reproducible handshakes; replay-rule + Retry conformance vectors; refute-vectors red |
| P-a2 | flow-key module: derivation vectors, grant exchange over stub session, epoch invalidation | derivation test vectors (incl. encoding-ambiguity negatives); epoch bump invalidates every cache; restart ⇒ new epoch (nonce crash-safety) |
| P-a3 | staging/ingress devices in the fork + guest drivers + warden pipeline | **the XSA-155 test**: guest mutates slot at every point post-copy ⇒ sealed bytes ≡ inspected copy, always; borrow-into-guest-memory lint red; one-seal-one-unseal structural audit; copy+seal throughput baseline ratchet |
| P-a4 | private-version hop integration (per D-10(b)) with the four frame classes | no-double-encryption structural test; GMAC nonce-law verification (packet-number binding, no reuse across 2³²-class sweep); per-lane MAC coverage exact |
| P-a5 | full five-layer matrix under cluster-SIM | forgery/replay/decryption-DoS/corruption-per-lane/divergence suites green across **every** endpoint class (host parser, guest unseal, terminal, bare-UDP); boot-classifier coverage check (no endpoint × attack cell unstated) |

Walking-skeleton consumption: P0 wire takes P-a1/P-a2; the pod leg takes
P-a3; nothing ships bulk before P-a4's lane guards.

## 8. Acceptance criteria

1. Copy-once is compiler-proof: the borrow-into-guest-memory lint and the
   volatile-copy discipline are CI-fatal from the first device commit (the
   XSA-155 test permanent).
2. One payload seal + one payload unseal per frame, structurally audited;
   hosts perform no payload encryption anywhere.
3. GMAC nonces are packet-number-bound, never resumed; restart ⇒ new epoch
   ⇒ new keys (tested, not asserted).
4. Epochs are protocol counters; no wall-clock enters any key or grant
   decision.
5. The bulk exemption guard holds: no chunk byte is parsed or used before
   name verification; envelope-layer truncation/reorder detection tested
   per lane.
6. Guest receive paths are split-virtqueue/no-indirect and purpose-built;
   the general-driver surface is zero.
7. Every transport path is registry-classified at boot; unclassified fails
   startup.
8. The broker's zero-marginal-authority argument appears in this spec
   (§3); C3's THIN precedent status is recorded, not hidden.
