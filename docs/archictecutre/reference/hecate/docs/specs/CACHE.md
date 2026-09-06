# SPEC: CACHE + PUB-SUB — general mutable KV cache with a coherence spine

Status: ACCEPTED 2026-08-20 (grilling finalization). Folds: the ValKey source
study (git `8.0.8-238-g3f0d910c8`), the CacheLib + distributed-hybrid research,
Lane-B (at-most-once-notify), and two user reframes settled in-session — (a)
**general mutable KV is the required core**; content-addressed immutability is
an **opt-in specialization**, not the default (IAM permission/token caches +
observability aggregates + general infra all need general mutable KV in front
of an authoritative source); (b) the **opt-in-optimization principle** — the
default is OSS-compatible, safe, general-purpose behavior; architecture-native
optimizations are opt-in, each gated on a **declared workload property** that
makes it safe, never a mode flag. The **flash tier is environment-derived**
with a robust distributed end (DRAM-only laptop ↔ full provisioned hybrid at
scale), exactly parallel to the QUEUE durability derivation. Sibling of QUEUE
and FANOUT on the shared substrate (§0). References: ValKey source; CacheLib
(Navy / BlockCache / BigHash / Kangaroo; W-TinyLFU); "Scaling Memcache at
Facebook" (NSDI 2013); TAO (ATC 2013); End-to-End Arguments (TOCS 1984).

## 0. Shared substrate (with QUEUE, FANOUT)

- The **durable log** (WAL logical-log + CONSENSUS; replica count = the
  durability knob) is the source of truth for anything that has one. The cache
  is NOT a source of truth (§1).
- Payload → N-reader fan-out is a RUNTIME §4 **arena** `acquire`/`release-count`
  — bytes live in the owning arena, visible in replay, never a smart pointer
  (Arc/Rc banned).
- **One single-owner task per shard** (thread-per-core): shard state is touched
  only by its owner ⇒ lock-free, race-free; a key maps to exactly one shard;
  cross-shard traffic is move-only over **bounded** channels; typed outcomes,
  never panics; tracked tasks only.
- **Placement** = weighted HRW (rendezvous) over the fenced host-inventory map
  (SERVING §6). Concurrency sharding (partition-for-lock-freedom) is orthogonal
  and node-local.
- **N=1 collapse**: a depth-one failure-domain tree derives HRW ring cardinality
  = replica = shard = 1; distributed edges are absent because the targets are
  absent; in-process channels replace RPCs; identical binary and code path
  laptop ↔ fleet. Never a mode flag.
- **Determinism**: every hash (index, shard-assignment, frequency sketch) and
  the admission coin seed from `Driver::rng`; time reads `Driver::now()`; no std
  `HashMap` in shard state; SIM is bit-reproducible.
- **Chokepoint law**: every durable writer is boot-classified in the CONSENSUS
  §6 roster, or startup fails. The cache holds no durable writer — it is
  registered **writer-less by absence** (§10).

## 1. Role & framing

A general-purpose **mutable key→value cache** (ValKey-shape) that sits in front
of authoritative sources — IAM (compiled permission/decision caches, token and
session lookups), observability (metric aggregates, dedup windows, query-result
caches, the collector hot-ring), and general infrastructure (resolver caches,
rate-limit counters, session state). It is **not a source of truth**: a miss
recomputes or refetches from the authority.

- **Default** = general mutable KV with full coherence (§4), DRAM tier,
  environment-derived provisioning.
- **Opt-in** (a declared property of the workload that makes the optimization
  safe): **content-addressed** (key = hash of value ⇒ immutable ⇒ coherence is a
  no-op, §4.4); **flash-backed** locally (§8); the log-backed read optimizations.
- Nothing is a hardcoded number: every size/limit derives from a physical anchor
  (RUNTIME AC#7).

## 2. Data model

```rust
// key -> shard is stable_hash(key) % shard_count (seeded)
struct CacheShard {
    arena:    SlabArena,                    // DRAM tier; (slab:u32,off:u32,gen:u32) = handle
    index:    SwissTable<KeyHash, Handle>,  // open-addressing + two-table incremental rehash
    sketch:   CountMin4bit,                 // exact W-TinyLFU frequency + doorkeeper
    policy:   EvictionPolicy,               // WTinyLfu (default) | Lru2Q | Sieve, per pool
    ttl:      TimingWheel,                  // ordered TTLs; pop O(due), not sampled
    holders:  HolderIndex,                  // key -> interested holders (coherence, §4)
    channels: DetHashMap<ChannelId, SubRing>, // pub-sub; co-shards with keys; bounded ring
    flash:    Option<NavyCache>,            // env-derived; None on laptop (§8)
    budget:   PoolBudget,                   // bytes + fragmentation + metadata, typed exhaustion
}
struct Handle { slab: u32, offset: u32, gen: u32 } // stale gen => typed error; no refcount
```

Slab-class boundaries are **derived from the live object-size histogram**; the
pool budget counts fragmentation and per-entry metadata, so exhaustion is a
typed error, not a reactive OOM shrink. A pool is a budget-charged isolation
domain bound to a session/dedup line (VFS §4).

## 3. Hot paths

```rust
fn get(&mut self, k: KeyHash) -> Lookup {
    self.sketch.increment(k);
    if self.ttl.is_expired(k) { self.evict_now(k); return Lookup::Miss; } // lazy TTL = staleness floor
    if let Some(h) = self.index.get(k) { self.policy.on_hit(h); return Lookup::Hit(self.arena.borrow(h)); }
    if let Some(f) = &self.flash { if let Some(b) = f.get(k) { return Lookup::Hit(self.admit_to_dram(k,b)); } }
    Lookup::Miss // caller single-flights the authority (recompute/refetch), or content-fetch if content-addressed
}
fn put(&mut self, k: KeyHash, v: &[u8]) -> Result<(), Exhausted> {
    while !self.arena.can_fit(v.len()) {                 // make room synchronously
        let victim = self.policy.pick_victim();
        if self.sketch.duel(k, victim.key) {             // W-TinyLFU admission duel (ValKey has none)
            let evicted = self.arena.free(victim);
            if let Some(f) = &mut self.flash { f.maybe_admit(evicted, Driver::rng()); } // endurance-servo'd
        } else { return Err(Exhausted); }
    }
    let h = self.arena.alloc(v)?; self.index.insert(k, h); Ok(())
}
```

Eviction, TTL expiry, incremental rehash, and holder-bound enforcement all run
**synchronously and amortized on the shard tick** — a background maintenance
thread would be a banned spawn (§6).

## 4. Coherence — the spine

General mutable KV requires coherence: when the authoritative value changes,
cached copies must be invalidated. There are two genuinely different patterns.

### 4.1 Read-through with invalidation (cached copy of an authoritative value)
IAM tokens/sessions, resolver caches. Two scopes:
- **Within a node** — the **holder index**: ValKey CLIENT TRACKING generalized.
  `key → local holders`, populated on read, and on a change the holders are
  invalidated and the set cleared (holders re-read to re-register). Co-sharded
  (lives on the key's owning shard ⇒ lock-free), bound **derived from arena
  bytes** (not ValKey's fixed 1M-key cap), evicting the **coldest** interest
  (not ValKey's random-walk — we already hold the frequency sketch), one-shot
  cleared on write.
- **Across nodes** — the key's **HRW owner holds the cross-node holder set**
  (which nodes cache it). On an authoritative change (a delta), the owner fans
  it out as an invalidation to exactly those nodes via FANOUT → co-sharded
  pub-sub. This is memcache's mcsqueal (invalidations replayed from the
  authoritative log) + TAO's versioning, on our transport. Delivery is
  **at-most-once notify + version backstop**: the invalidation carries the
  value's version, a holder already past it ignores it, and a *missed*
  invalidation is caught by a version-check on the next read or a slow reconcile
  — staleness is bounded by that interval, never indefinite.

### 4.2 In-cache mutable working set (the cache holds the value)
Observability aggregates, rate-limit counters. There is no external source to
invalidate from; the value is *owned* by one shard whose single owner serializes
every update. No lost updates, no cross-node invalidation (the value is not
cached elsewhere).

### 4.3 Freshness / the critical-read escape (TAO)
Reads are region-local and bounded-stale by default. A **critical read bypasses
the cache to the authority** when freshness is non-negotiable. IAM staleness is
a **security parameter**: a revoked permit or token must not be served stale, so
sensitive checks use a critical-read or a tight derived-staleness bound plus
invalidation. (IAM permission *decisions* are computed from a **local compiled
artifact** the delta stream refreshes on policy change — fresh-by-local-compile,
barely touching the distributed cache; the distributed cache mostly holds
token/session lookups, exactly the bounded-stale + invalidation + critical-read-
on-revocation case.)

### 4.4 Content-by-hash specialization (opt-in)
Declaring a pool **content-addressed** (key = hash of value) makes the value
immutable by construction, so the entire coherence machinery above becomes a
**verified no-op**: no holder index, no invalidation, stale-set unrepresentable.
This is the only place the "emergent distributed cache" story (HRW ×
immutability × single-flight, no cross-region invalidation) legitimately holds.
It is an opt-in specialization declared by a workload property, not the default.

## 5. Eviction & admission

- **Admission** = W-TinyLFU (a 4-bit Count-Min frequency sketch + doorkeeper +
  aging + a candidate/victim **duel**), scan-resistant at the gate. ValKey has
  *no* admission (it inserts everything and samples 5 keys to evict) — our exact
  sketch + duel is the improvement. Aging is deterministic (halving at a
  sample-count threshold), not ValKey's wall-clock decay; the flash-admission
  coin is `Driver::rng`.
- **Eviction** is pluggable per pool (CacheLib's lesson: no universal winner,
  2Q was its biggest DRAM gain): **W-TinyLFU-main** default, **LRU-2Q**
  first-class, **SIEVE** where a pool is provably scan-free.

## 6. Memory & maintenance

- **DRAM** = a slab-classed arena (one size-class per slab); the compressed
  `(slab, offset, gen)` **is** the RUNTIME §4 generational handle; a stale gen is
  a typed error. No per-object refcount (ValKey's `refcount:29` is the Arc/Rc we
  ban); no `val_ptr` indirection.
- **Incremental rehash**: the index resizes via a second table and drains it one
  bucket-chain per operation (ValKey `hashtable.c`), on the shard tick — dropping
  all of ValKey's fork/COW resize policy (it serves a persistence model we lack)
  and all process-global statics (shard-local, seeded).
- **One maintenance doctrine**: incremental rehash, eviction, TTL expiry, and
  holder-bound enforcement are all a bounded, time-boxed, cursor-resumable slice
  of work on the shard executor. **No background maintenance thread.**

## 7. Pub-sub

Ephemeral **at-most-once** fire-and-forget to currently-connected subscribers;
channels co-shard with keys (ValKey shard-channel hashes with the *same* slot
function as keys), and are auto-deleted at zero subscribers. A lagging
subscriber gets a **bounded ring + a counted drop** (never ValKey's silent
output-buffer-overflow disconnect); fan-out order is Driver-deterministic.
Cache-invalidation, TTL-expiry, and eviction all publish through **one** event
path (ValKey's "mutation is a pub/sub event"), emitted at the **structural
mutation chokepoint** (not ValKey's 117 hand-placed call sites) as **typed enum
events**. Delivery is the ephemeral delivery class (PROTOCOL §3).

## 8. Flash tier — environment-derived, robust distributed

DRAM everywhere by default. The flash tier's provisioning is
**environment-derived**, one parameter, no mode flag, with a **robust
distributed end**:
- **Laptop** derives to **DRAM-only** — negligible working set ⇒ no evictions ⇒
  no flash region, zero SSD writes (a dev SSD's endurance is not spent on a cache
  that raises no hit rate).
- **Distributed / at scale** the default **is** the full robust hybrid: a
  deliberately **provisioned, first-class DRAM+flash capacity tier** — one
  **Navy-style cache engine** hosting a **log-structured BlockCache** (large
  objects) and a **set-associative BigHash/Kangaroo** (small mutable KV) — with
  the admission gate + endurance servo (NVMe write budget = device TBW ÷ target
  lifetime; reject-first for scan traffic), sized from available flash × the
  working-set ratio.
- The two ends are DRAM-only ↔ full robust hybrid, never DRAM-only ↔ reluctant
  spillover. Flash-backing is additionally an **opt-in** for the rare large
  **local** working set with expensive recompute.

This is the sole sanctioned eviction-driven cross-tier flow (OBJECT_TIER §5:
DRAM eviction → flash admission gate; most items rejected, endurance-servo'd —
not uncontrolled spill). **AC-1 resolution**: at scale that one Navy-style
*cache* engine hosts both on-disk structures; OBJECT_TIER/VFS "one immutable-pack
engine, a second is unrepresentable" is **scoped to the origin/store role**, so
the robust cache tier is representable.

## 9. Replication & placement

No durable writer, so nothing to replicate: content is *placed* on multiple
nodes by HRW with popularity mirroring, and re-filled from the authoritative
source on node loss (zero data loss — it was never the truth). Cross-node
coherence is the §4.1 HRW-owner holder set + FANOUT invalidation. For a
content-addressed pool the distributed cache is emergent (§4.4).

## 10. Boot & chokepoint

**Writer-less by absence** — positively registered as "no durable writer" so the
chokepoint-coverage check has a cell for it (an unregistered participant fails
boot; the cache introduces *no* CONSENSUS roster entry). Pools bind to their
session/dedup budget lines at boot. The IAM capability actions
`cache_read`/`cache_write`/`channel_publish`/`channel_subscribe` compile to PEP
at boot.

## 11. Laptop / N=1

One shard, DRAM arena, no flash region (derives to zero), in-process channels.
Identical `get`/`put`/`publish` code; ring cardinality 1.

## 12. Opt-in optimizations (declared-property table)

| Declared property | Optimization unlocked |
|---|---|
| (none — default) | general mutable KV, full coherence, DRAM, env-derived provisioning |
| content-addressed (key = hash of value) | immutable ⇒ zero-invalidation, emergent distributed cache (§4.4) |
| flash-backed (large local working set, expensive recompute) | provision a local flash region (§8) |
| log-cursor consumer | cache-invalidation as best-effort notify; consumer recovers by cursor |

## 13. Failure handling (FAULTS §5)

Cache loss = **Masked** (re-fill from authority) or **Degraded** (cold, slower)
— never work-loss. Pub-sub loss = a **counted at-most-once drop** (never silent).
Every drop is counted; an unknown drop is a bug.

## 14. Acceptance criteria

1. General mutable KV (arbitrary key → mutable value, TTL, eviction) is the
   default and serves IAM/observability workloads.
2. A source change invalidates all holders (within-node and cross-node) within
   the derived staleness bound; invalidation carries the version; a missed
   invalidation is caught by version-check or reconcile (never indefinitely
   stale); a **critical read returns authoritative**.
3. A **revoked IAM permit/token stops being served within the derived bound**
   (or immediately via critical-read).
4. A content-addressed pool executes **zero** invalidation ops.
5. **DRAM-only on a laptop** (no flash region, zero SSD writes); flash is a
   robustly **provisioned** tier at scale, fully exercised in the distributed
   SIM (capacity + admission + endurance under load); the SIM sweeps both ends;
   the endurance servo holds device wear ≤ target; one cache engine (AC-1 scoped
   to origin).
6. Hit-rate ≥ the recency+frequency baseline on the trace sweep.
7. The holder-index bound is **derived from arena bytes**, one-shot cleared on
   write, coldest-evicted under pressure.
8. TTL expiry is **O(due)** (ordered timing wheel), never a sampled scan.
9. **No maintenance thread** appears in the SIM spawn census (rehash/evict/expiry
   all on the shard tick); the shard is refcount-free (generational handles
   only); SIM is bit-reproducible.
10. Every size constant traces to a physical anchor; the cache introduces no
    CONSENSUS roster entry and boot confirms its writer-less cell.

**Amendment (2026-08-22, TRACING/MONITORING acceptance):** this subsystem's
chokepoints emit execution spans per `TRACING.md` §3; its chokepoint registry
entries are the span roster (boot-validated; an unregistered emitter fails
startup).
