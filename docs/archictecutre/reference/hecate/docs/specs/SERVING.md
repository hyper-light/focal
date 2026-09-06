# SPEC: the serving machine

Status: ACCEPTED 2026-08-16 (grilling Branch 21, decisions (a)–(g) ratified
individually, spec accepted whole). Research on file: EdenFS internals,
CitC/Piper, virtio-fs/DAX/virtiofsd/libkrun, sharded CAS + placement functions.
Companion to `VFS.md` (chunk store, manifests, volume roles) — this spec is the
machine that serves those volumes into pods and witnesses what comes back.
Amended 2026-08-18: §0 (the single-surface law) added; green-chain extension
gains the placed-before-referenced requirement (MERGE §5). Amended
2026-08-20 (CACHE/QUEUE/FANOUT acceptance): §4 names the FANOUT router as
the green version-advance push transport (re-bind stays pod-initiated; not a
new authority).

## 0. The single-surface law

**No API in the system names a machine.** Volumes, content, peers, sessions,
and services are addressed by identity — version, hash, participant UID,
lineage node. **A session spans nodes**: its pods place on any machines the
scheduler chooses; its home services (ledger core, merge service, frontier —
the colocation unit) sit together on one node with their log replicated
across the session group; its volumes mount from any node. Any pod on any
node may attach any volume version — locality is a property of caching,
never of availability. The fleet presents as one runtime with one namespace;
the laptop is that same surface with one machine under it. (Receipts: the
K8s image-by-digest model for immutable content; EdenFS's lazy local
projection, its own four-tier read order — kernel cache → in-memory LRU →
local store → network — adopted verbatim here; Nix/OSTree
pull-by-name-verify-by-hash. Coherence machinery is absent because every
shared object is immutable; the mutable work volume is single-pod,
single-node by law — our RWO — and is never shared.)

## 1. The two-representation law

Exactly two representations exist, and the set is closed:

- **Mutable** — the per-pod **work-volume overlay**: one logical WAL journal +
  one extent index per volume. The journal is the *only* place dirty bytes live
  before sealing. The index of volume V can only reference records of V's log —
  cross-volume reads are unrepresentable in the type, not forbidden by policy.
- **Immutable** — **manifests-over-CAS**: baseline, green chain, seals, tools.

Any state that is neither is a spec violation (architecture test, AC-1).

## 2. The write/witness path

- Mutable inodes are non-DAX; every guest write arrives as `FUSE_WRITE`. The
  daemon appends a content-bearing record to the volume's logical log — `{inode,
  offset, len, bytes, prev_version}` — through WAL.md group commit, then replies.
  **The reply is the witness**: acked ⇒ durable (power-fail-safe, chained-CRC,
  torn/corrupt discriminated). Unacked guest page cache is work-bearing memory
  and dies with the pod, by doctrine.
- **Scratch scope**: template/registry-declared redirect paths (`target/`,
  `node_modules/`, caches) mount a pod-local **scratch volume** — unwitnessed,
  unjournaled, unmergeable, freed at pod teardown. The witnessed overlay holds
  source-tree mutations only (VFS.md role addition).

## 3. The overlay (log-structured)

- Extent index: `(file, version) → [segment extents]`, arena-allocated, owned by
  the volume's serving instance. Reads: guest page cache first; `FUSE_READ`
  resolves extents and copies from page-cached segments zero-copy into the
  descriptor chain (common case: one contiguous whole-file record).
- Crash recovery = index rebuild by segment header walk; content is never
  copied. Rebuild time is bounded by seal cadence; **no index checkpoint exists**
  — if measured rebuild breaches the pod-resume budget, that tripwire reopens
  the decision (recorded in GRILLING.md).
- **Seal (at increment submission)**: drain guest writeback (`FUSE_FSYNC` sweep)
  → freeze epoch → derive per-file edit ops by diff against `prev_version`
  (pure, version-pinned deriver — the landing engine's op-log input, run off the
  hot path) → CDC-chunk → BLAKE3 → CAS → manifest update → drop extents →
  recycle segments per WAL.md. What seals is exactly what the agent fsync'd.
- DAX for dirty reads is **rejected permanently**: group commit interleaves
  volumes in physical segments; mapping segment pages into a guest would leak
  foreign volumes' bytes. Dirty reads are memcpy-class `FUSE_READ`s.

## 4. Green (the manifest chain)

- Green is a chain of immutable manifests over CAS content — **the session's
  shared CoW staging volume**, attachable from any node (§0). A merge-gate
  commit appends `{version, manifest_hash, increment_refs}` — green's only
  WAL touch; content is already CAS-resident from seal. Merkle manifests
  share unchanged tree nodes: chain cost is O(changed paths).
- **A version is placed before it is referenced** (amended 2026-08-18, MERGE
  §5): the chain record does not commit until the version's new blobs are
  replicated to the session-group members and acked — the OT14 ladder
  applied to green, so a promoted merge-service applier holds log, state,
  and bytes, and the chain can never point at content nobody has.
- The merge gate **extends, never writes**; nothing mutates in place (MERGE.md's
  fix-forward, made physical). Green serving instances are compiled with no
  write path: `FUSE_WRITE` and WRITE-flagged `SETUPMAPPING` answer `EROFS`.
- Green is all-DAX read-only: every pod in the session maps the same host
  page-cache pages for shared chunks. Pods pin `green@version` (the lease
  basis); re-bind happens pod-initiated at increment boundaries — manifest diff
  → targeted invalidations for changed paths only. The version-advance
  notification (MERGE §0 step 8's "green is now vN") is pushed to the
  session's pods over the **FANOUT router** (`FANOUT.md`) — the
  version-advance push transport; re-bind stays pod-initiated, and the
  fanout carries only the delta notification, never a new write authority
  (green's sole writer remains the merge gate).

## 5. The metadata model

- **Inode identity law**: inode = `(volume, path-entry)`, allocated
  monotonically at first lookup, stable for the *volume's* lifetime; the table
  serializes and re-binds with the volume (handoff invisible to `(dev,inode)`
  tooling). Content identity = manifest-entry hash, swapped under stable inodes
  at re-bind. Generation numbers guard reuse. Single-parent, no hard links.
- Manifest entries carry `(type, mode, size, blake3)`: `stat`/`readdir`/ENOENT
  answered zero-fetch from the projection; tree nodes are CAS chunks loaded
  lazily on first `readdir`. Content fetch happens at `open`, batched through
  the topology verbs.
- **Digest xattr contract**: BLAKE3 exposed as an xattr iff clean; absent while
  dirty; restored at seal. Consumers (build tools, scheduler memoization) never
  see a stale digest.
- TTL posture: green/tools = infinite entry/attr TTLs + explicit invalidation;
  work volume = writeback cache mode (sole-writer coherence).
- Prefetch: template eager-sets walk the manifest at bind; sampled access logs
  derive per-template glob profiles (observe-mode first). Crawls degrade to
  bounded cache-fill (SES7).

## 6. The topology (the serving plane)

This section owns the **serving plane** of the two-planes factoring
(`OBJECT_TIER.md` §1, ratified 2026-08-17): session-hot work content —
work volumes, green chains, tools, the cache hierarchy. The **durable
plane** (registry content, knowledge/vector generations, lineage baselines,
archives) is its own system with assignment-based copyset placement —
`OBJECT_TIER.md` §4. Both planes share one substrate (identity, CDC,
manifests, pack-volume engine, wire verbs) and never share a placement
authority. Content crosses planes only at lifecycle boundaries (seal =
serving-plane write; landing/archival/generation-publication/registry-
provisioning = durable-plane writes); the serving plane cache-fills FROM
the durable plane by hash, never the reverse.

- **One placement function per plane** — this plane's: weighted rendezvous
  hashing over a versioned, fenced host-inventory map (consensus-owned —
  consumes Branch 20's API). Domains: chunk-group ID (immutable), session
  UID (mutable replicas). Work-class copyset exposure is accepted with its
  derivation written down (`OBJECT_TIER.md` §3).
- Chunk-groups are the placement/repair unit: `group_count = devices ×
  target_groups_per_device` (≈100–200, Ceph anchor); the map carries an explicit
  exception table (upmap pattern) for residual skew and pins.
- Replication: `R_eff = min(R_target, distinct_failure_domains)`, stated loudly
  when degenerate. Erasure coding only in the Archivalist's cold tail.
- Cache hierarchy: guest DAX → host pack-volume store (append-only volumes +
  in-RAM index) → HRW peer → shield/origin. Single-flight at every layer;
  popularity-triggered mirroring; failure absorbed by spare capacity, never
  load-rehash.
- **The durable object tier is Hecate's own — never a cloud provider's**
  (user directive 2026-08-17): the chunk store IS the object storage. The
  origin behind every shield tier is Hecate-authoritative, not
  S3/GCS/anything external — for work-class content, the HRW placement
  group of this section; for lineage-class content (the registry's
  documents, index generations, sealed snapshots, archives), the durable
  plane's copyset-map targets (`OBJECT_TIER.md` §4, which owns the
  S3-level machinery: copyset placement, erasure cold tail, scrub,
  map-driven repair). External cloud storage may only ever appear as an
  optional, registry-declared external source behind Guardian staging — a
  place content can be *imported from*, never a tier Hecate depends on.
  (The Nix/OCI receipts are receipts for the stateless-over-blob *pattern*;
  their delegation of the blob tier to a provider is exactly what this
  rule forbids.)
- Mutable side: **state-follows-compute** — primary lives at the scheduler's
  colocation host; HRW gives the replica set (journal-ship to top-(R−1)
  successors) and the deterministic promotion order. HRW enters scheduler
  locality scoring as a preference.
- hecate-wire gains batched-existence and group-granular fetch/repair verbs.
- **Laptop**: inventory of one — same formulas, `R_eff = 1` loudly, empty
  exception table, all tiers collapse local. No modes.

## 7. Device + DAX policy

- Own FUSE-over-virtio protocol layer + backend trait, native to hecate-rt
  (`!Send` tasks, arenas, io_uring, SIM-drivable), in-process in the libkrun
  fork. Multiqueue advertised (Linux ≥ 6.10 guests; 5.5× receipt). Borrowed
  structure, not code: virtiofsd dispatch shape + zero-copy seam, libkrun's
  three-platform mapping paths, EdenFS inode discipline.
- **Mapping engine = one trait, two modes**: `splice` (mmap chunk pages over
  the window) and `managed` (boot-mapped window, daemon copies; zero runtime
  hypervisor calls). KVM ships splice; HVF ships splice behind a boot
  capability probe with managed fallback; WHP ships managed until splice is
  proven. Guest-invisible either way.
- Window size = `2 MiB × derived_peak_hot_ranges + reclaim_headroom(20 ranges)`,
  ceilinged by the ~1.6% guest-RAM metadata tax — every constant anchored in
  kernel source at the definition site.
- Per-inode DAX split (`dax=inode` + `FUSE_ATTR_DAX`); WRITE-flagged mappings
  on immutable inodes answer `EROFS`; `open(O_WRONLY/O_RDWR)` on a clean file
  materializes a mutable overlay inode (new nodeid, non-DAX) — copy-up in
  protocol terms.
- **Single-protocol advantage (recorded)**: one guest protocol (FUSE-over-virtio
  into Linux guests) on all three host OSes. EdenFS's three-protocol matrix —
  and its chmod-hack invalidation, fsck-every-boot, and un-veto-able writes —
  is structurally absent from this design.

## 8. Implementation sketch

```rust
// Witness record — the journal's unit (hecate-wire framed, CRC-chained)
struct WitnessRecord<'a> {
    inode: InodeNo, offset: u64, prev: VersionNo,
    bytes: WireBytes<'a>,                      // zero-copy from descriptor chain
}

// Per-volume overlay — single authority, arena-owned
struct Overlay {
    log: LogicalLogHandle,                     // this volume's WAL stream only
    index: ExtentIndex,                        // (FileId, VersionNo) → SmallVec<Extent>
    inodes: InodeTable,                        // serializable; re-binds with volume
}

impl Overlay {
    async fn write(&mut self, w: WitnessRecord<'_>) -> Reply {
        let pos = self.log.append(&w).group_commit().await?; // ack AFTER durability
        self.index.splice(w.inode, w.offset, w.bytes.len(), pos);
        Reply::ok(w.bytes.len())
    }
    fn read(&self, ino: InodeNo, off: u64, sink: &mut dyn ZeroCopyWriter) -> Reply {
        for ext in self.index.resolve(ino, off, sink.remaining()) {
            sink.copy_from(self.log.mapped(ext));  // page-cached segment, one copy
        }
        Reply::data()
    }
}

// Platform seam — the only per-hypervisor code in the FS
trait MappingEngine {
    fn map_chunk(&mut self, win_off: u64, chunk: ChunkRef, prot: Prot) -> Result<()>;
    fn unmap(&mut self, win_off: u64, len: u64) -> Result<()>;
}
// impls: KvmSplice, HvfSplice (probe-gated), ManagedWindow (portable, copies)
```

## 9. Test matrix

| # | Test | Catches |
|---|---|---|
| FS1 | Witness durability: SIM power-fail mid-write-storm — every acked write recovered, no unacked write half-present (torn discrimination) | the ack-before-durable lie |
| FS2 | Replay equivalence: overlay state post-recovery ≡ pre-crash acked state (property, SIM fuzz) | derived-state drift |
| FS3 | Isolation structural: extent index cannot name a foreign log (type-level) + runtime guard fuzz | cross-volume leak |
| FS4 | DAX bypass: guest `mmap(PROT_WRITE)` on clean inode → EROFS; no unwitnessed write path exists (protocol sweep) | silent witness bypass |
| FS5 | Green immutability: writes + WRITE-mappings → EROFS; chain extends only via merge-gate committer | green authority erosion |
| FS6 | Re-bind exactness: invalidations = changed paths exactly; unchanged `(dev,inode)` stable across re-bind AND pod handoff | dcache breakage; build-tool identity breaks |
| FS7 | Seal barrier: sealed manifest ≡ guest's fsync'd view (no straggler writeback) | agent-submitted ≠ sealed |
| FS8 | Op-log purity: (ancestor, epoch, deriver version) → byte-identical splice ops | landing-engine input drift |
| FS9 | Mapping equivalence: splice vs managed → byte-identical guest reads, per platform | platform semantic fork |
| FS10 | Topology movement: node add/remove moves only w/W of groups; N=1 all-local | placement churn; laptop mode creep |
| FS11 | Single-flight: K concurrent misses on one group → one origin fetch | thundering herd |
| FS12 | Crawl degradation: full-tree walk → bounded cache-fill throughput | the virtual-FS cliff (SES7 twin) |
| FS13 | Volume re-bind: pod kill → successor binds same volume — same inodes, no guest-visible discontinuity | handoff visibility |
| FS14 | Window pressure: reclaim at threshold — bounded round-trips, no stall/deadlock | reclaim storms |
| FS15 | Xattr honesty: digest absent while dirty, correct post-seal | stale-digest consumers |

## 10. Acceptance criteria

1. Two representations, closed set — new mutable state outside the overlay
   fails the architecture test.
2. No `FUSE_WRITE` reply precedes group commit (SIM assertion); reply-latency
   budget derived from WAL group-commit anchors.
3. **No fsck exists.** Recovery is replay only; any repair pass is a defect.
4. No cross-pod path to a live overlay (structural + grep gate); non-pod access
   is Guardian-granted read-only user views served by the owning instance.
5. Every constant (window, group counts, seal cadence, R, TTLs) derived at its
   definition site; zero literals.
6. Mapping-engine differential (FS9) green on a platform before that platform
   ships splice.
7. FS1, FS3, FS4, FS5, FS8 are permanent CI gates.
8. The journal is FS-service state: the ledger references sealed manifest
   hashes only — no journal position, segment, or epoch ever appears in a
   claim, testament, validation, or artifact.
9. Laptop first-light: N=1 bind-and-serve within derived budget (ties SES11).
10. Wire additions (existence/fetch/repair verbs) enter via hecate-wire's
    append-only evolution rules; WIRE_FORMAT.md precedes codec implementation.

**Amendment (2026-08-22, TRACING/MONITORING acceptance):** this subsystem's
chokepoints emit execution spans per `TRACING.md` §3; its chokepoint registry
entries are the span roster (boot-validated; an unregistered emitter fails
startup).

**Amended 2026-08-22 (MONITORING acceptance):** container-upper writes are guest
tmpfs and never reach the machine's serving plane — strengthening AC-1's
write-authority claim for the two-container interior (VFS amendment of the same
date).
