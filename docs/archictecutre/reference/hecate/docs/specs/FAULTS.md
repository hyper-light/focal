# SPEC: the fault model — scope, dispositions, simulation

Status: ACCEPTED 2026-08-17 (decision 5 + amendment 5a of the ratified
Branch-20 direction; accepted with the split-brain and cross-region delta
sets). Governs: the consensus core, WAL, both storage planes, the protocol
layer — every subsystem names its obligations against this one fault
taxonomy. Companion: `CONSENSUS.md`. Amended 2026-08-20 (CACHE/QUEUE/FANOUT
acceptance): §5 adds the three shared-substrate primitive rows (queue
dup-is-expected + N=1-loss-priced + rebuild-from-quorum; cache/pub-sub loss
Masked-or-Degraded, never work-loss; fan-out ephemeral-loss Degraded,
confined to the ephemeral class).

## 1. The fault scope (what we defend, stated closed)

**In scope**: crash-recover (kill at any instruction, recover from durable
state); process pause/resume of any duration (the GC/scheduler stand-in —
defeated by fencing, never by timing assumptions); network partition in
every shape (clean splits, majorities-ring, **per-link per-direction
asymmetric omission** — the Cloudflare class — and region-scale partitions);
message loss, duplication, reordering; clock strobe/jump (only telemetry may
notice — no correctness path reads wall-clock); storage faults with
detection (torn writes, misdirected writes, detected corruption,
disk-swap-on-reboot, stalled-device gray failure); **region-scale events**
(WAN partition, correlated loss of a full failure-domain subtree, WAN
latency inflation); **internal-machinery misbehavior of liveness-only
components** (the node-liveness fabric emitting contradictory/stale/withheld
claims — in scope precisely because CONSENSUS §3b's fabric law promises
safety is untouched by it).

**Out of scope, explicitly**: Byzantine participants (the pod/warden
boundary and **per-workload flow keys** are the authenticity mechanism —
amended 2026-08-22, the five-site flow-key landing: **a compromised
workload lies only as itself** — at Bar A (an ordinarily-compromised
agent, guest kernel intact) the primary cannot forge as its Scribe, since
each container holds only its own keys; at Bar B (guest-kernel compromise)
interior custody collapses but the mint root is pod-scoped, so the pod
still lies only as that pod, contained host-side. **The two-bar adversary
model is recorded here as fault-model law**: Bar A degradation is
HANDOFF's domain; Bar B forges no consensus traffic and gains no
authority beyond the pod's own. Neither bar can forge consensus traffic); undetected
corruption past the checksum layer (BLAKE3-everywhere makes the undetected
residue the hash-collision probability, stated, not defended further).

**Scoped exception — the IAM authority plane (amendment 2026-08-18,
`IAM.md` §12 T4).** For the IAM store *only*, one Byzantine class is
admitted in scope: a **compromised host, replica, or group leader
attempting to inject or mutate authority records**. Authority records
(bindings, mandates, grants, roles, policies) and the IAM-root/checkpoint
carry the management service's signature (Ed25519; custody/rotation in
Branch 25's key hierarchy), verified at **apply on every replica, at
run-load, and on snapshot install**. A forged or foreign-signed authority
record is refused and alarmed — not applied — so a single compromised
node (leader included) cannot mint authority; blast radius stays its
resident sessions and replicas (T6). This is a **deliberate, documented
expansion of the fault scope for this subsystem, entered here per the §7
acceptance rule** ("new fault classes enter by amending §1/§3, never by an
ad-hoc test"); it does **not** admit Byzantine consensus traffic, forged
messages past the key layer (F5 stands), or Byzantine behavior for any
other subsystem. The cost is apply-path CPU (~25–40 µs/verify,
ed25519-dalek), off the read path.

## 2. Corruption dispositions (protocol-aware recovery, FAST'18 CTRL)

The one law: **never silent truncation** — truncating a corrupt log region
without protocol awareness can un-commit globally-committed entries.
Dispositions, typed per artifact:

- **Torn tail** (fails its own frame checksum at the recovery frontier):
  discard — it was never acked (the witness discipline makes this safe by
  construction).
- **Detected body corruption** (checksum fails behind the frontier): typed
  disposition by artifact class — consensus log entry ⇒
  **rebuild-from-quorum** (fetch the committed entry from peers; refuse to
  serve until repaired); N=1 or quorum-unavailable ⇒ **refuse loudly**
  (typed, names the entry, operator-surfaced — never guess); content chunk
  ⇒ re-fetch by hash (self-healing; OBJECT_TIER scrub/repair path); derived
  state ⇒ discard and re-derive (always legal; watermark-recovery
  invariant).
- Every disposition is a counted, categorized event; a disposition with no
  signal is a bug.

## 3. The nemesis matrix (the closed fault-injection vocabulary)

`kill` (any instruction) · `pause` (unbounded, resume) · `partition`
(clean, majorities-ring, partial) · `omit` (per-link, per-direction,
probabilistic and total) · `dup/reorder` · `clock` (strobe, jump, skew) ·
`torn-write` · `misdirected-write` · `corrupt` (targeted byte flips, data
and metadata) · `disk-swap` (node reboots with a peer's or stale disk) ·
`disk-stall` (device alive, unbounded latency) · `fabric-fault`
(contradictory/stale/withheld/forged-within-scope support claims) ·
`region-partition` (WAN cut, incl. asymmetric) · `region-loss` (correlated
kill of a full subtree) · `wan-inflate` (WAN latency ×10–100).

Every class has at least one CI scenario; composite scenarios
(pause + partition + clock jump; region-partition + fabric-fault) are
seed-generated, not hand-enumerated. The omit/dup/reorder/wan-inflate
classes exercise **owned hecate-quic loss recovery through the transport**
(D-10(b)) — for the first time in any QUIC implementation, recovery itself
is seed-replayable under the nemesis matrix, not just the layers above it. New fault classes enter by amending
this section, never by an ad-hoc test.

## 4. The deterministic whole-cluster simulation (amendment 5a)

- **One simulation, cluster-scoped**: RUNTIME.md's SIM driver extended to
  host N simulated nodes across a simulated failure-domain tree — simulated
  network (the §3 vocabulary as injectable edges, WAN edges with derived
  latency distributions), simulated storage (torn/corrupt/stall
  injectable), logical time. The consensus core's IO-as-data shape
  (CONSENSUS §2) means the cluster runs unmodified inside it — the two
  decisions are one design.
- **Seeded and replayable**: every run is a seed; every failure is a
  seed + step count, replayable to the instruction. A failing seed becomes
  a permanent regression seed.
- **Biased search, BUGGIFY-style**: injection sites carry seed-driven bias
  hooks (prefer the rare branch, the full buffer, the expiring lease, the
  healing partition) — the FDB lesson that unbiased random rarely finds
  the interesting interleavings.
- **Budget as a ratchet, not a number**: simulation CPU-hours per release
  ratchet up from the first CI baseline; the FDB ~trillion-CPU-hour and
  TigerBeetle 24/7-VOPR postures are the reference points; our floor
  derives from fleet size and release cadence at the definition site.
- **The N=1 gate** (CONSENSUS §8): the crash-injection suite runs at N=1
  as a named, separate CI job — the configuration where the v3.5 class
  hides.

## 5. The failure×obligation matrix

The spec's core artifact: rows = §3 fault classes, columns = subsystem
obligations. Each cell is one of **Masked** (no observable effect),
**Degraded** (typed, bounded, surfaced), or **Refused** (loud stop, never
guess) — and each non-trivial cell names its test. The matrix ships in this
document and is **boot-validated for coverage**: every subsystem × fault
class has a stated cell — an uncovered cell fails CI, not review. Headline
rows:

- `pause` ⇒ Masked everywhere by fencing (CN8).
- `disk-stall` ⇒ Degraded via support-lapse election (CN3).
- `corrupt(log body)` ⇒ Degraded via rebuild-from-quorum; Refused at N=1.
- asymmetric `omit` ⇒ Masked for safety, Degraded-bounded for liveness
  (CS8).
- `fabric-fault` ⇒ Masked for safety unconditionally, Degraded for
  election latency only (CN11 — the executable form of the fabric law).
- `region-partition` ⇒ Masked for everything region-local; Degraded for
  the closed root-operation stall list, which may never intersect a
  session path (CN13).
- `region-loss` ⇒ Degraded per the priced loss class: unlanded work within
  the replication lag forfeits; sealed work intact; sessions re-summon
  (CN14).

**IAM subsystem rows (amendment 2026-08-18, `IAM.md` §3).** The IAM store,
its compaction, and its reachability index take the standard §2
dispositions by artifact class; the cells:

- **IAM store** — reads: `region-partition` ⇒ **Masked** for
  session/user/org scopes (the owning group is region-local; ReadIndex is
  region-local, and the decision path reads compiled residuals, not the
  store); `region-loss` ⇒ **Degraded** (re-replicate from quorum, refetch
  run chunks by hash). writes: `region-partition` ⇒ **Degraded** for
  away-scope writes (they wait); `pause` ⇒ **Masked** by epoch fencing.
  A corrupt IAM-root or authority record ⇒ **Degraded** via
  rebuild-from-quorum, **Refused** at N=1 (the consensus-log disposition);
  a corrupt run chunk ⇒ **Masked** by re-fetch-by-hash.
- **IAM compaction** — any fault ⇒ **Masked**: compaction is a background,
  restartable, content-addressed merge; a crash mid-compaction discards the
  half-written run (never referenced by the IAM-root until the atomic swap)
  and re-runs. It holds no correctness obligation — the pre-compaction runs
  remain the authority until the root swaps.
- **IAM reachability index** — any fault ⇒ **Degraded then Masked**:
  discard-and-re-derive from the log (it is a derived read-optimization,
  never authority; queries fall back to the authoritative merge-read of
  runs while it rebuilds).

The T4 apply-time-signature exception (§1) adds one row: an
**inject-authority attempt** (forged/foreign-signed record at any replica)
⇒ **Refused** and alarmed — never applied (F8).

**Primitive-subsystem rows (amendment 2026-08-20, `CACHE.md`/`QUEUE.md`/
`FANOUT.md` acceptance).** The three shared-substrate primitives (CACHE §0)
take these cells:

- **Queue** — `dup/reorder` (crash re-leases leased-but-unacked records) ⇒
  **not a fault**: at-least-once redelivery is EXPECTED, absorbed by the
  consumer's dedup window (`QUEUE.md` §13), never counted as loss; a
  power-cut acked write is never lost (WAL fsync). `region-loss`/node-death
  at the opt-in lossy fast tier ⇒ **Degraded**, priced, never silent.
  `corrupt(partition log)` ⇒ **Degraded** via rebuild-from-quorum where
  replicated, **Refused** at N=1 (the §2 consensus-log disposition).
- **Cache / pub-sub** — cache `region-loss`/node-loss/eviction ⇒ **Masked**
  (re-fill from the authority — the cache is never a source of truth) or
  **Degraded** (cold, slower), **never work-loss** (`CACHE.md` §13);
  pub-sub delivery loss ⇒ a **counted at-most-once drop**, never silent.
- **Fan-out** — `partition`-induced ephemeral-subscription loss ⇒
  **Degraded**, confined to the ephemeral class, never work-loss
  (`FANOUT.md` §11); durable subscriptions inherit the Queue cells above
  (they *are* queue partitions — not double-classified).

Every drop is counted; an unknown drop is a bug (the §2 no-silent-signal
law applied to the primitives).

## 6. Test matrix

| # | Test | Catches |
|---|---|---|
| F1 | Full nemesis × linearizability sweep (CN1's harness) at N=1/3/5 across a multi-region simulated tree, seeds ratcheted | safety under every in-scope fault |
| F2 | Every §2 disposition exercised by targeted corruption injection; zero silent truncations (asserted structurally) | the un-commit class |
| F3 | Replay determinism: any failing seed replays byte-identically | debuggability of everything above |
| F4 | Obligation-matrix coverage check: no subsystem × fault cell unstated | silent scope holes |
| F5 | Byzantine non-goal boundary: a forged-message attempt dies at the key layer, never reaches the core (negative test) | scope confusion |
| F6 | Disk-swap-on-reboot: node with stale/foreign disk is detected (epoch/identity mismatch) and refuses to vote | the FAST'18 disk-swap class |
| F7 | Region-heal fuzz: partition a region (not kill), let both sides run, heal ⇒ safety holds under the full §7 protocol: (a) inside the lease-shadow window the root refuses re-grant/re-summon-with-materialization while the cut side may still legally materialize (CN15's window, exercised from the fault side); (b) after dead-declaration the region epoch is terminal — on heal the region rejoins under a new epoch, no pre-partition epoch resumes authority or renews a lease; (c) zombie sessions' unlanded work ingests as fork branches only, never continuations — overlapping descendants of one lineage node surface as parallel workstreams carrying conflict values; (d) zombie externalization attempts during and after the partition are refused at the fenced egress chokepoints (CN16 from the fault side) | zombie-region resurrection; the lease shadow; un-fenced externalization |
| F8 | IAM inject-authority (the §1 scoped exception): a forged or foreign-signed authority record / IAM-root injected at any replica — leader included — is refused at apply, run-load, and snapshot install, and alarmed; blast radius stays the compromised node's resident sessions and replicas (no accepted mutation elsewhere) | authority injection via node/leader compromise |

## 7. Acceptance criteria

1. The fault scope is closed: new fault classes enter by amending §1/§3,
   never by an ad-hoc test. The one scoped Byzantine exception — IAM
   inject-authority (§1, `IAM.md` §12 T4) — is entered here by that rule,
   bounded to the IAM store, and tested by F8; it widens no other
   subsystem and does not disturb F5.
2. Zero code paths truncate a log region without a typed §2 disposition
   (architecture test).
3. Simulation runs in CI from the first consensus commit; the seed corpus
   and CPU-hour floor only ratchet.
4. The obligation matrix is complete and boot-validated (F4 permanent).
5. Every incident-derived scenario (Cloudflare asymmetric partition, v3.5
   watermark race, TiKV #10017, disk-swap, region-heal) exists as a named
   regression seed.
6. The simulated failure-domain tree covers depth one (laptop) through
   multi-region in the same harness; no scenario is laptop-only or
   fleet-only by construction.
