# SPEC: the object tier — two planes over one substrate

Status: presented for acceptance (grilling Branch 24; two-planes verdict
ratified in-session 2026-08-17). Amended 2026-08-20 (CACHE/QUEUE/FANOUT
acceptance): the `queue` storage class registered (§1/OT11); §5a — the cache
role is a two-layout Navy-style engine (BlockCache + BigHash/Kangaroo), the
one-pack-volume-engine substrate law (AC-1) scoped to the durable
origin/staging plane, "zero invalidation" scoped to the content-by-hash face.
Research on file (GRILLING.md): Tectonic
FAST'21 read in full (LEAD REFERENCE, user direction 2026-08-17) + Haystack
OSDI'10 + f4 OSDI'14 lineage; ShardStore SOSP'21; BlueStore SOSP'19; CacheLib
OSDI'20; Tectonic-Shift ATC'23; EdenFS tier mechanics (repo docs, read
directly); Colossus (published posts); copysets ATC'13 + Tiered Replication
ATC'15; LRC ATC'12 + Clay FAST'18; LSE SIGMETRICS'07 + corruption FAST'08;
Venti FAST'02 / OCI / Ambry (any-copy-valid-by-hash); Tail at Scale CACM'13;
memcache leases NSDI'13. Companion to `VFS.md` (the store) and `SERVING.md`
(the serving machine); this spec is the storage system beneath both.

## 1. The two planes (the factoring law)

One **substrate**, two **fleet disciplines**. The anti-Sylk law lives at the
identity layer, below placement: one content identity (BLAKE3), one CDC
chunking, one manifest encoding, one pack-volume engine (§2), one wire verb
set (§8). Dedup works across planes by construction. Above that layer, the
planes are distinct systems:

- **The serving plane** — session work volumes, green chains, tool mounts,
  the cache hierarchy, DAX. Placement = weighted HRW per `SERVING.md` §6,
  **unchanged**: zero-lookup on the turn path, state-follows-compute
  locality, minimal movement under churn. Exposure is hours-to-days; blast
  radius is session-bounded. Copyset exposure is **accepted with its
  derivation written down** (§3).
- **The durable plane** — registry content, knowledge/vector generations,
  lineage baselines, archives, sealed session proofs. Placement =
  **assignment-based copyset placement as published** (Cidon / Tiered
  Replication) over a small consensus-owned placement map (§4). Legitimate
  because nothing in this plane is on the turn path: registry resolution
  happens at summon/staging, generation fetch is background cache-fill,
  archival is background. The zero-lookup requirement that forced
  placement-as-computation never applied here.

The copysets×HRW composition is **withdrawn as a research gap** and recorded
as a design lesson: no coherent system needs both disciplines for the same
data; the prior tension was a symptom of artificial unification. Factored,
each plane uses its own literature's proven construction.

**Storage class is the router**: a property of the content's root, recorded
in the manifest/descriptor at write time (the registry storage-class hook),
boot-validated per the chokepoint law — never a runtime heuristic, never a
second authority. Class transitions ride existing lifecycle boundaries only
(§6). Registered classes include the durable content classes and **`queue`**
(`QUEUE.md` — queue-payload bodies above the inline budget, placed in the
origin role per the queue's durability tier); an undeclared class fails boot
(OT11). The **`telemetry`** class registered 2026-08-22 (COLLECTOR acceptance):
the collector's cold blocks — sealed interval event/span blocks, sealed trace
rows, cooled System partitions — content-addressed, GC-rooted by op-log
reference (§7).

## 2. The substrate: pack volumes (one on-node engine, three roles)

The workload is Haystack's verbatim — written once, read often, never
modified, rarely deleted — minus mutation entirely. One on-disk format
serves the cache, origin, and staging roles; roles differ by policy, never
by format (the Tectonic chunk store's obliviousness; the laptop degenerate's
enabler).

**The staging role** (added with `TRANSFER.md`'s acceptance, 2026-08-17):
the ingest state machine's landing zone for unaddressed bytes. Same volume
format, same append + batched-flush + scan-as-truth recovery — `PartAck`
watermarks are re-derived by staging-pack scan after a crash, which is what
makes transfer resume truthful by construction (`TRANSFER.md` §5 rung 1).
Policy differences, exactly three: staged extents are **lease-reclaimed**
(the transfer's derived lease expires ⇒ scan reclaims, counted); staged
content is **never placement-eligible** (it has no identity yet — placement
and replication verbs refuse the staging role structurally); and staged
bytes convert to addressed chunks only through `TRANSFER.md` §6's single
commit gate.

- **Volume**: one preallocated large file per volume, held-open fd,
  preallocated extents sized so blockmaps stay RAM-resident (Haystack's
  1 GB-extent / stripe-alignment shape; our constants derived from measured
  device anchors at the definition site). Superblock, then framed chunks:
  `{header magic, blake3 (32 B), len, payload, footer magic}`, 8-byte
  aligned. **The BLAKE3 address IS the checksum** — no cookie, no separate
  CRC.
- **Index**: in-RAM `blake3 → (volume, offset, len)`. **Rebuild-by-scan is
  truth**; the async index-file sidecar is the optimization, recovered by
  Haystack's orphan-tail replay (last sidecar record marks the last
  non-orphan chunk; scan the tail, append). Content addressing makes the
  sidecar trustless: every entry re-verifiable against data.
- **Crash story**: append + batched flush using `WAL.md`'s measured
  durability primitives — **not** the ledger WAL machinery (content never
  enters the WAL, by law). A torn tail chunk fails its own hash and is
  dropped at scan. **No fsck exists**; recovery is scan.
- **Index-RAM formula** (the one condition that flips the design, derived at
  the definition site): `chunks_per_node = capacity ÷ cdc_avg_chunk`; full
  32 B keys cost ~44–56 B/chunk of RAM. If the derived count crosses the
  node RAM budget, the published escape hatch applies: 8 B key-hash index
  in RAM, full key on media, validate after read (CacheLib LOC; collision
  < 10⁻⁶ at 536 M objects). Below tens of millions of chunks per node, full
  keys win on auditability.
- **Compaction**: copy-forward live chunks into a new volume, atomic swap —
  needed only for GC reclaim (§7), never for updates.
- **Rejected alternatives, with receipts**: LSM (ShardStore) earns its
  complexity only for mutable keys and heat-driven re-placement — it paid
  with soft-updates dependency DAGs and an institutional formal-methods
  program; even ShardStore keeps bulk bytes outside the tree. Raw-block
  (BlueStore) answers overwrite/enumeration pathologies absent from
  append-only packs; the measured filesystem tax is per-object-fsync-shaped
  and batched appends do not pay it (Haystack: 85% of raw device throughput
  on plain XFS). File-per-chunk is the anti-pattern both lineages measured
  to death (10 disk ops/read worst case; 536 B inodes vs 10 B/object).

## 3. The serving plane (owned elsewhere; the acceptance derivation here)

`SERVING.md` §6 owns the serving plane's topology verbatim. This spec adds
only the **work-class acceptance derivation**, written down as required:

```
E[work-class loss] = correlated_event_rate × exposure_window(seal→land)
                   × copyset_coverage(pure HRW at fleet N, R_eff)
```

evaluated against the class loss cost (one session's unlanded work,
user-visible, re-derivable by agent effort — bounded, never existential).
The derivation lives at the placement function's definition site and is
re-evaluated when fleet parameters change; SIM sweeps it (§11 OT5). Pure
HRW is accepted for this class because the exposure term keeps the integral
small — not because the coverage term is small.

## 4. The durable plane

- **The placement map**: a consensus-owned document
  `{epoch, copyset table, exception table}` — no new authority class
  (consensus already owns the inventory map and refs). Entries are durable
  chunk-groups; the map is tiny and is **the one honest piece of placement
  metadata in the system**. Construction is the published one: permutation-
  derived copysets under a chosen scatter width `S`; `S` derived from
  repair-bandwidth anchors against the derived exposure-window bound
  (repair after a node loss must complete inside it); copysets minimized
  subject to `S`; failure-domain constraints enforced in construction
  (`R_eff = min(R_target, distinct domains)`, loud when degenerate);
  upmap-style exceptions for residual skew and pins; deterministic
  promotion order.
- **Writes**: the writing daemon acts as the Tectonic-style client —
  parallel, unordered, **idempotent** PUTs to the map's R targets. Any copy
  is valid by hash (Venti/OCI/Ambry); no chain, no ordering protocol for
  chunk data — ordering machinery survives only where mutability survives
  (refs, maps). Tail control: hedged reservations ahead of data to a few
  more nodes than R, write to the first R responders; reservations double
  as admission checks (Tectonic §5.1, ~20% p99 receipt). Ack = R_eff
  durable acks.
- **The four-rung durability ladder** (order is law):
  1. **Witnessed** — journal group commit (serving plane, `SERVING.md` §2);
  2. **Sealed** — CDC → BLAKE3 → arena → local pack volume;
  3. **Placed** — R_eff acks from the plane's targets;
  4. **Referenced** — manifest commit, then ref flip (`set_ref_if` /
     merge-gate chain append).
  **Placed strictly precedes referenced**: nothing reachable can be
  under-placed; a crash between rungs 3 and 4 leaves orphan chunks for GC,
  never dangling references.
- **Reads**: the serving plane cache-fills FROM the durable plane by hash
  — one direction, never reverse. Hedged read to a second replica at a
  derived p95 trigger (Tail at Scale: 1,800 ms → 74 ms p999 at +2% load).
  BLAKE3 verify on every fill.
- **Repair**: driven by the map; per-node **reverse index**
  (device → resident chunk-groups) — scan-derivable, sidecar-cached, same
  trust model as the pack index; under-replicated-first priority.
- **Scrub**: verify-on-read everywhere (intrinsic) **plus** scheduled
  full-read scrub of cold content — >60% of latent sector errors are found
  only by scrub (SIGMETRICS'07; 3.45% of disks over 32 months; nearline
  8.5%). Cadence derived from LSE/AFR rates × fleet size × repair
  bandwidth (Ceph's weekly deep-scrub as the production anchor, never the
  constant).
- **EC cold tail**: seal-then-encode (WAS/f4/Tectonic pattern). Code chosen
  at a derived crossover — replication may dominate at small fleets; LRC
  where repair fan-in dominates, Clay/MSR where repair network bytes
  dominate; RS(10,4) is never adopted by imitation. Encode/decode carries
  Tectonic's inverse-transform checksum verification (in-memory corruption
  is a regular occurrence at scale; corrupt parity must never propagate
  into reconstruction).
- **Durability number**: instantiated from Cidon's formula with our
  weights, domains, and fleet sizes, at a definition site — no borrowed
  nines (no published Tectonic durability model exists).

## 5. Tiering and cache governance (the interop mechanics)

```
T0  guest page cache + DAX windows           (SERVING.md §7)
T1  node chunk arena (RAM)                   (VFS.md §1 — the store's RAM tier)
T2  node pack-volume store (NVMe)            (§2 — cache role / origin role)
T3  HRW peer / shield                        (single-flight, SERVING.md FS11)
T4  plane origin                             (serving: HRW group; durable: map targets)
T5  EC cold tail                             (§4)
Mutable side (journals, ledger WAL): node-local per WAL.md — not in this
hierarchy; content enters the tier only at seal.
```

- **The arena and the pack store are the RAM and NVMe tiers of one store.**
  In the **origin/store and staging roles**, movement between them is
  explicit lifecycle (flush-at-seal, fill-on-demand), never spill;
  exhaustion stays a typed error at each tier (`VFS.md` §1 reconciliation);
  and there **exactly one on-disk store format exists** — EdenFS's own
  admission (two overlapping disk caches, "local store eviction is an
  unsolved problem", one being retired) is the binding counter-receipt.
  The **cache role** is the one sanctioned eviction-driven cross-tier flow
  (§5a): a DRAM eviction feeds the flash **admission gate**, most items
  rejected — governed flow, not spill.
- **Cache-role governance** (CacheLib import): eviction is whole-volume
  FIFO (region-granularity; sequential writes cut device write
  amplification 1.5× → 1.05×), never per-chunk free-space tracking.
  **Admission is the endurance governor**: NVMe write budget derived from
  the device anchor (TBW ÷ target lifetime); admission probability servo'd
  to it (44%-fewer-flash-bytes-at-equal-hit-ratio receipt); reject-first
  for scan traffic. **Declared-future admission**: summon claims and
  template eager-sets are the declared working set — consumed the way
  Tectonic-Shift consumes training-job dataset specs.
- **§5a — the cache role is a two-layout Navy-style engine** (`CACHE.md`
  §8; environment-derived — robustly provisioned at scale, DRAM-only on the
  laptop degenerate). It hosts a **log-structured region cache** (BlockCache:
  large objects and the content-by-hash face) **and** a **set-associative
  small-object store** (BigHash/Kangaroo: sub-KB general-KV, where a
  per-object RAM index would exceed the object). **The content-by-hash face
  is sealed-immutable and carries zero invalidation logic** (Shift's
  premise) — CDC's minimum chunk size floors its object size, so it needs no
  set-associative tier. **The general-KV face is mutable**: the store itself
  stays immutable (a value change is evict + reinsert), and coherence /
  invalidation lives at the cache layer (`CACHE.md` §4), never in the store.
  This second on-disk layout is admitted **only in the cache role**, which is
  a rebuildable copy and never a source of truth (AC-1 scoping).

## 6. Lifecycle boundaries (the seam)

Content crosses planes only at events the architecture already owns:

- **Seal** is a serving-plane write (work class).
- **Landing, archival, generation publication, registry provisioning** are
  the durable-plane writes: the same immutable chunks — identity unchanged
  — placed per the copyset map, then the ref flips (the f4
  lock-then-migrate / Tectonic seal-then-reencode sibling). No new state
  machine.
- Reads flow one direction (§4). No durable-plane write path exists
  outside these boundaries (structural, §11 OT10).
- **Co-located fleets** (the common case) get TrafficClass-style isolation
  between repair/scrub IOPS and serving IOPS (Tectonic Gold/Silver/Bronze
  receipt); floors derived. On a laptop the question is moot.

## 7. GC and capacity

- **Liveness roots**: green manifest chains + registry refs +
  ledger-referenced seal manifests + generation pointers + **live IAM
  records** (`IAM.md` §3/§11 — an IAM record pins the policy text, compiled
  artifacts, and sorted-run chunks it references; the IAM-root/checkpoint
  is a root like any generation pointer) + **op-log-referenced telemetry
  blocks** (a sealed interval's `sealed_refs` on the operational log pin its
  blocks until the referencing log records retire — `COLLECTOR.md` §7/§16;
  amended 2026-08-22, COLLECTOR acceptance). **Mark-and-sweep
  from roots** — never cross-node refcounts (mutable distributed state;
  mark-from-roots is derivable and restartable over immutable manifests).
  Sweep granularity = pack-volume copy-forward compaction (§2).
- Per-session/tenant quotas and accounting from summon budgets (`VFS.md`
  §4 pattern).
- f4-style **crypto-erase** (delete = discard a scope key) composes with
  §9 if per-scope keys land — right-to-forget without touching packs.

## 8. API surface

`put / get / batch_exists / ranged_get / list / scan` on hecate-wire,
extending the `SERVING.md` topology verbs; `scan` feeds scrub and GC.
**No append verb** — chunks are whole immutable objects; streaming large
objects (generation segments, packs) is ranged reads over manifest chunk
lists. Wire additions ride hecate-wire's append-only evolution rules;
`WIRE_FORMAT.md` precedes codec implementation.

## 9. Encryption × dedup (SETTLED 2026-08-17 — D-3 ratified; AC-6 lifted)

**Scope-salted convergent encryption, validated against the full attack
literature** (Perttula 2008; BKR13's MLE formalization; DupLESS;
Harnik–Pinkas–Shulman-Peleg side channels; eprint 2025/558). Chunk key =
`f(content, dedup-domain salt)`; domain = the legitimate sharing scope
(lineage / user / global) = the isolation line. Dedup survives within the
domain exactly (same content ⇒ same key ⇒ same ciphertext); the salt
defeats cross-domain confirmation; crypto-erase per domain is preserved
(§7's f4-style composition, now live). The construction survives
structurally: **every attack class requires either an attacker who can
evaluate the key derivation, or mutually untrusting co-writers inside one
dedup domain — and no Hecate domain has either.** Lineage scope:
co-writers mutually trusting by construction. User scope: one principal
(the Borg/restic/Tarsnap per-repo pole, isomorphic to scope-salting).
Global scope: vendor content is public — confirmation reveals nothing.
Tahoe-LAFS has run this exact construction ("convergence secret" per
dedup domain) in production since 2008.

**Four hardenings, as law:**

1. **Salt = 256-bit random key, never name-derived.** Chunk identity in
   private scopes = **BLAKE3 keyed mode** (`keyed_hash(domain_salt,
   content)`), never plain hash — a plain hash gives a compromised
   operator the O(|dictionary|) attack BKR13 proves unavoidable for
   predictable content, and code files are maximally predictable.
   (`WIRE_FORMAT.md` §3b's keyed-mode `ContentRef.root` clause is this
   hardening's wire face.)
2. **The global domain is write-closed**: membership by vendor-signed
   chunk-ID manifest, never by content-that-happens-to-match — otherwise
   the shared-salt domain becomes a fleet-wide existence oracle salting
   cannot fix. **An ID is never a bearer capability** (the
   Dropbox/Dropship lesson): every ID-based claim carries possession-proof
   semantics (`TRANSFER.md` §2's bao root-only proofs are the mechanism).
3. **Chunk-size hygiene**: randomized chunk→pack placement (restic
   0.18.0's mitigation) and/or padding buckets, derived per class. Any
   chunker seed is *obfuscation, not a security boundary* — eprint
   2025/558 broke the keyed chunkers of all five shipped systems (Borg,
   Bupstash, Duplicacy, restic, Tarsnap).
4. **Crypto-erase hierarchy**: the per-scope master key wraps both the
   salt *and* chunk-key material — scope deletion destroys decryptability
   **and derivability** (kill only chunk keys and an adversary holding
   content copies re-derives them). Erasure granularity is the scope;
   per-object erase requires ref-counted rewrap and is built only if
   compliance demands it (Branch 25 inherits this wrap structure).

**Tripwire option, recorded not built**: RCE-style split (random
per-chunk data key + deterministic keyed dedup *tag*, data key wrapped for
domain members) upgrades "salt stolen ⇒ full within-domain CE exposure"
to "salt stolen ⇒ confirmation ability, not decryption," at key-wrap
plumbing cost. Adopt if a dedup domain with weaker internal trust ever
appears; lineage domains' mutual trust makes it v1-unnecessary.

**Rejected with receipts**: DupLESS-style key service (defends
untrusted-co-writer predictable content — a problem no current domain
has; online key service violates local-first; zero production adoption in
13 years). Storj-style no-dedup (forfeits the core cross-session dedup
goal against attackers absent from this trust model).

**Thin spots, carried honestly**: Tahoe-LAFS is the only production
deployment of tenant-scoped CE (the middle path is otherwise
patents-only; the per-repo-key pole has more mileage); the Dropbox
dedup-shutdown date is secondary-sourced.

**Tests**: OT16 — cross-domain dedup isolation: identical content written
in two domains produces distinct ciphertext/identity; within one domain,
identical ciphertext/identity (both directions property-fuzzed). OT17 —
crypto-erase completeness: after scope-key destruction, neither
decryption nor key re-derivation succeeds given full pack access + known
plaintext. OT18 — global-domain closure: a non-manifest chunk matching
vendor content is refused membership; an ID presented without possession
proof is refused (negative vectors). OT19 — placement-randomization
effectiveness: chunk-size-sequence fingerprinting across packs stays at
chance under the 2025/558 attack model (measured bound, ratcheted).

## 10. Laptop degenerate

Inventory of one: both planes collapse onto the same local pack volumes
(the format-role unification of §2 is what makes this clean); the placement
map is trivial; `R_eff = 1` loudly; scrub still runs on its derived
cadence; hedged reads degenerate to no-ops; class labels persist inert, so
a laptop corpus later joining a fleet re-places correctly from recorded
classes. Same formulas, no modes.

## 11. Test matrix (failure each catches)

| # | Test | Catches |
|---|---|---|
| OT1 | Pack power-fail fuzz: cut at every byte offset; scan-recovery drops exactly the torn tail; every flushed chunk intact and hash-verified | acked-loss; scan-truth drift |
| OT2 | Sidecar honesty: stale/absent/corrupt sidecar ⇒ orphan-tail replay reproduces the scan-derived index byte-identically | sidecar promoted to truth |
| OT3 | Index-RAM formula honored: derived chunk count vs budget decides full-key vs key-hash index; collision handling exercised under the hash mode | silent RAM blowup; unverified collisions |
| OT4 | Placement determinism: same (map epoch, class, group) ⇒ identical placement on every node, cross-platform | split-brain placement |
| OT5 | Correlated-failure sweep (SIM): kill a failure domain per seed sweep; each plane's measured loss events ≤ its derived bar; repair completes inside the exposure-window bound | the loss integral being fiction |
| OT6 | Movement bound: map/inventory churn moves only the designed groups per plane (serving: w/W; durable: repair-paced re-placement only) | placement churn; serving-path invalidation from durable churn |
| OT7 | Scrub effectiveness: injected latent corruption in cold chunks detected within one derived cadence; verify-on-read catches hot corruption immediately, typed, naming the chunk | silent rot in the cold tail |
| OT8 | Repair priority: under-replicated groups repaired before mirrored convenience copies; reverse index consistent with scan ground truth | repair scheduler blindness |
| OT9 | GC safety fuzz: random crash points across the four-rung ladder ⇒ no reachable chunk ever collected; all orphans eventually collected | dangling references; leak-forever orphans |
| OT10 | Seam structural: no durable-plane write path outside lifecycle boundaries; serving never serves as durable origin (reads one-directional) | plane bleed; a second placement authority |
| OT11 | Class-at-root: undeclared storage class fails boot (chokepoint law); class transition outside a lifecycle boundary is unrepresentable | runtime-heuristic placement |
| OT12 | EC inverse-verify: corrupted input to encode/decode detected before parity/reconstruction propagates | poisoning the reconstruction path |
| OT13 | Endurance servo: sustained cache pressure keeps NVMe write rate ≤ derived budget while hit ratio stays within ratchet | flash burn; admission theater |
| OT14 | Ladder ordering: no ref observable before R_eff placement acks (SIM assertion at every crash point) | reachable-but-underplaced content |
| OT15 | Laptop first-light: N=1 boot-to-serve within derived budget; identical formulas verified against fleet mode outputs | mode creep |

## 12. Acceptance criteria

1. **The substrate law** (architecture test): one content identity, one
   CDC, one manifest encoding, one wire verb set — a second of any is
   unrepresentable; the two planes share the substrate and never share a
   placement authority. The **one-pack-volume-engine** clause binds the
   **durable (origin/staging) plane**: the source-of-truth store has exactly
   one on-disk format. The **cache role** — a rebuildable copy, never a
   source of truth — is permitted the two-layout Navy-style engine of §5a
   (BlockCache + BigHash/Kangaroo); if its layouts ever tangle, the cache is
   rebuilt from the durable store and nothing is lost.
2. **No fsck exists** anywhere in the tier; recovery is scan/replay only.
3. **Placed-before-referenced** is invariant (OT14 permanent CI).
4. Every constant (extent sizes, index mode threshold, scatter width,
   scrub cadence, endurance budget, EC crossover, TTLs) is derived at its
   definition site from measured/physical anchors; zero literals.
5. The durability number ships with its own Cidon-instantiated derivation;
   no figure is borrowed from another system's cluster.
6. §9 SETTLED 2026-08-17 (D-3): the gate is lifted — the durable plane may
   accept user content once the four §9 hardenings are implemented and
   OT16–OT19 are green; hardenings ship wired or the gate stays practical.
7. OT1, OT5, OT9, OT10, OT11, OT14 are permanent CI gates.
8. External cloud storage appears only as an optional, registry-declared
   import source behind Guardian staging — never a tier either plane
   depends on (the no-cloud-pairing law, restated).
