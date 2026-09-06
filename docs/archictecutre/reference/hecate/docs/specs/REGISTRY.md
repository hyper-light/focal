# SPEC: the registry — the slow plane and the extension surface

Status: ACCEPTED 2026-08-16 (v2 — rewritten after the syllium agentregistry
implementation examination, findings on file in GRILLING.md; amended under
maximal audit with exact implementations — three corners closed: universal
canonical encoding via the `Document` type, tenancy as scope-in-key with typed
handles, revision-floor reads). Amended 2026-08-20: §2b records the QUEUE/FANOUT
topic/queue/subscription/filter descriptors as config-registered kinds (written via
the §5 `set_ref_if` CAS-first primitive), like every other config descriptor.
Ratified context: open roster with offices;
custom agents/skills/tools/MCP servers/packages as first-class staged entries;
defaults as catalog entries; no Postgres/Redis dependency; laptop-through-Meta
on one storage contract.

## 1. The envelope (adopted: RawObject, both halves opaque)

One generic envelope for every kind: `{kind, metadata, spec: bytes, status: bytes}`.
The generic layer never parses spec **or** status — per-kind codecs are the only
typed boundary (the syllium property that let one store and one handler serve nine
kinds with zero DTOs). In Hecate the spec bytes are hecate-wire canonical, so the
**content hash is the address**: syllium's `content_hash` column collapses into
identity itself.

Metadata: `namespace/name/tag` (user), `labels` (indexed, equality-subset queries
only — syllium proved nobody needs a selector language), `annotations` (narrative,
never indexed), plus server-minted fields callers cannot supply: `uid` (instance
identity — content addressing gives *content* identity, but "same name after
delete+recreate" still needs a server-minted UID), `generation`, timestamps.

**Writer disjointness is two typed operations** — `upsert(spec)` cannot touch
status; `patch_status(mutator)` cannot touch spec — a compile-time property in Rust,
not a documented convention. Multi-writer status uses key-ownership: each writer
(resolver, Guardian, validator) owns a top-level key in `status.details`, merged
without coordination. Conditions with reasons; no phase roll-ups.

## 2. Kinds and registration

Built-in kinds: `AgentRole` (offices, rank archetype, custom domains), `Prompt`,
`Skill`, `MCPServer`, `Recipe`, `ModelConfig`, `GuestImage`, `Bundle`.

**`AgentRole` vs the authority plane's Role (amendment 2026-08-18, `IAM.md`
§7.9).** These are distinct and each owns its half: `AgentRole` (registry) is
the **agent→office binding + bundle refs** — the composition identity resolved
at summon; the IAM Role pack is **what a principal of that office may do** — the
policy statements. There is no double-store: at summon, resolution reads the
registry's `AgentRole` office binding and the IAM plane compiles the matching
role pack into the pod's residual (per-principal rows are compilation *output*,
never a second writable binding store). The registry stays a non-runtime-
authority (it cannot route, gate, or author claims); role-pack *content* lives
in the IAM store, root-anchored. Scope-authored roles that ship *through* the
registry ride the existing Guardian-staged publication (a claim — legal, it is
registry work, not IAM management); the commit **into** the IAM store is a
management-surface apply triggered *after* approval, never the claim itself, and
decision-time reads never touch the registry (G5's hot-path isolation holds).

Registration is a **descriptor**: `{kind, spec schema, storage class, plural}` —
one registration call for compiled kinds, and (beyond syllium) **the descriptor
carries the spec's schema, so a kind can be registered from a config document
without recompiling** — the mechanism that makes custom agents and TS/Py skill
declarations first-class rather than privileged. Shipped and custom entries differ
by publisher and staging provenance only.

### 2b. Canonical encoding for schema-registered kinds (amendment — exact)

The §1 law "spec bytes are hecate-wire canonical" holds **universally** because
schema-registered kinds encode through one compiled generic type:

```rust
#[derive(Wire)]                       // canonical-or-reject like every wire type
enum DocValue {
    Null,
    Bool(bool),
    Int(i128),                        // NO floats — non-integer JSON/YAML numbers
                                      // are a typed apply error ("use a decimal
                                      // string"); derived quantities are formulas
                                      // in code, never stored floats
    Str(String),
    Bytes(Vec<u8>),
    List(Vec<DocValue>),
    Map(BTreeMap<String, DocValue>),  // sorted-unique keys BY CONSTRUCTION;
                                      // decode rejects unsorted/duplicate keys
}
```

- **Ingestion**: author JSON/YAML → parse → `DocValue` (duplicate keys, floats,
  non-UTF-8 ⇒ typed errors) → validate against the kind's schema → encode via the
  `DocValue` wire codec. **Those bytes are the spec bytes; BLAKE3 over them is
  the identity.** Key order, whitespace, and serialization dialect can never
  fork the hash (G11 extension).
- **Descriptors are also the SDK-generation source**: the TS/Python authoring
  bindings (`SKILLS_API.md` §2) generate their typed APIs from kind
  descriptors — compiled reflection for compiled kinds, the registered schema
  for config kinds — so every kind, including config-registered ones, gets a
  friendly authoring surface whose output is always the canonical document.
- **The schema language is a closed subset**, enforced at descriptor
  registration: `type, properties, required, additionalProperties, items, enum,
  const, pattern, minimum/maximum, minLength/maxLength, minItems/maxItems,
  $defs + in-document $ref`. Anything else — remote `$ref`, `format`,
  conditionals — **rejects the registration loudly**; JSON Schema's
  silent-unknown-keyword default is inverted.
- **Schema evolution is append-only**: a descriptor is
  `(kind, schema_version) → schema document (itself a canonical `DocValue`,
  content-addressed)`; entries record the `schema_version` they validated
  under; changing a schema is publishing the next version, never mutating —
  the hecate-wire evolution discipline applied to config kinds.

**Sibling-primitive descriptors are config-registered kinds (amendment 2026-08-20).**
The QUEUE / FANOUT **topic**, **queue**, **subscription**, and **filter** descriptors
register through exactly this §2b mechanism — schema documents, no recompile — and are
written via the §5 `set_ref_if` CAS-first primitive (the CAS-first classification),
identical to how every other config descriptor is registered. They carry no runtime
authority (§8): the registry stores and serves them; the primitives' own single-owner
tasks run the routing and delivery.

Syllium's `Deployment`/`Runtime` half — controllers that create workloads,
finalizers, soft-delete GC, discovery reconcilers — is **deliberately dropped**: it
was the source of roughly half their store's complexity and it is runtime authority,
our standing non-goal. Hecate keeps only the tagged-artifact path.

## 3. Apply semantics (adopted: three-state upsert)

- Content hash over `{labels, annotations, spec}` — excluding status, timestamps,
  UID, generation — yields `created | unchanged | configured`; **`unchanged` writes
  nothing at all** (re-apply is free; CI loops are idempotent; no `updated_at`/event
  churn).
- Identity per tagged entry is `(namespace, name, tag)`; tags are mutable pointers;
  the hash is the identity. Batch apply is per-document (failures reported, never
  short-circuiting), stage-tagged errors mapping cleanly to results.
- **Intent/observation split, adopted wholesale**: spec records the user's pointer
  (a git branch, an OCI tag); resolution pins the concrete commit/digest into
  **status** (`resolvedSource`) — so re-resolving a moved ref never dirties the
  spec hash (identical intent stays `unchanged`), while the pin **does** enter the
  bundle fingerprint. Status-only writes emit no events, with the resolvedSource
  carve-out as the one allowlisted exception — the exact filter that prevents
  controller self-wakeup loops while letting pins propagate.

## 4. Bundles (adopted: closure fingerprint + evidence)

At summon, resolution walks the full transitive closure — role, prompts, skills,
model config, image, tool pins — and produces:
- the **Bundle fingerprint**: a Merkle root over content addresses (strictly
  stronger than syllium's payload hash: verifiable, not just comparable), versioned
  for hash-schema evolution, with resolved pins as material;
- the **dependency snapshot**: the human-readable evidence list of exactly what
  went in — a lockfile, a cache key, and an audit record in one structure.
Re-summon with an unchanged fingerprint is a no-op; a `force` token exists for
operator-forced rebuilds. Handoff re-resolves; nothing else ever does.

**Revision-floor reads (amendment — exact)**: every resolution read carries
`min_revision`. The requester's floor is its **session registry cursor** (the
session's registry projector already maintains a watch cursor; summon
resolution passes it). Replica behavior:

```rust
async fn serve_at_floor(&self, min_rev: Revision, deadline: Tick) -> Result<View> {
    if self.applied_revision() >= min_rev { return Ok(self.view()); }
    // wait for local apply up to the derived staleness budget
    // (p99 replication lag × stated factor), then forward:
    match self.wait_applied(min_rev, deadline).await {
        Ok(()) => Ok(self.view()),
        Err(Lagging) => self.forward_to_leader(min_rev).await, // leader always qualifies
    }
}
```

A replica **never serves below the floor** — resolution is read-your-writes
for everything the session has observed, and cross-session freshness is
bounded by watch propagation. The dependency snapshot records
`resolved_at_revision`, so every bundle names the exact registry state that
produced it. Laptop: single instance is always at head; the same code path
never waits.

## 4b. From catalog to running pod (why the Deployment half stays dropped)

The materialization path, end to end — the registry answers questions; it deploys
nothing:

1. **Publishers**: the shipped catalog (default entries embedded in the harness
   binary as content-addressed documents, seeded into the store at first boot —
   upgrades publish new hashes and never touch pinned running bundles);
   user-applied entries (through Guardian staging); external sources (specs point
   at git/OCI — the registry hosts nothing; the resolver pins, and the
   **provisioner** fetches content through the Guardian gates into the store, at
   staging/provision time, never at summon).
2. **At summon**: pure reference resolution — refs → content hashes → Bundle
   (fingerprint + snapshot). No downloads: content is local chunks, or cache-fills
   by hash with intrinsic verification on fleet nodes.
3. **Materialization** is the **summon executor's** job (harness machinery driven
   by the summon claim): image manifest → rootfs projection; tool manifests → RO
   mounts; prompts/skills/ModelConfig → delivered at assignment over the control
   channel; keys minted; warden policy compiled from the bundle. The registry is
   not on this path (G5).
4. **Lifecycle/teardown** belong to the ledger's machinery — summon claims,
   fast-forward teardown, wardens. Syllium's Deployment half (adapters, finalizers,
   soft-delete GC, discovery controllers) is not dropped as unwanted; it is
   **replaced by stronger, already-specified machinery** — and Hecate has no
   externally-created deployments to discover, because nothing exists that the
   summon executor didn't create.

## 5. Storage: one semantic contract, two implementations

The syllium lesson inverted: their storage "abstraction" was `Pool() *pgxpool.Pool`
— Postgres structurally welded in, a mandatory container to store a dozen YAML
files. Hecate defines the contract as **semantics**, provided by a file-backed
single-process implementation (laptop) and the replicated one (fleet) identically:

1. **CAS put/get** — the unified chunk store (`VFS.md`); registry documents are
   content-addressed like everything else.
2. **Ref compare-and-swap** — `set_ref_if(expected, new)` per `(kind, ns, name,
   tag)`. This one primitive re-provides syllium's advisory locks, row locks, no-op
   detection, and approval-staging conflict checks at once.
3. **Ordered ref scan** — lexicographic `(namespace, name, tag)` keyset pagination
   (immutable sort key — better than their `updated_at` cursor — and with a page
   cap, which they never enforced).
4. **Label index** — `(key, value) → refs`, maintained with the ref write;
   equality-subset scope only.
5. **Ref index, forward and reverse** — maintained at write time, so invalidation
   is O(dependents), never their O(everything) full scan (they built
   `FindReferrers` and never wired it; we wire it from day one).
6. **Event log** — three operations: `append`, `list_after(rev, limit)`,
   `oldest_revision()`. Identity-only events.

**Unbypassable emission** (their trigger property, re-provided structurally): the
ref-CAS is the *only* mutation primitive — module privacy makes raw writes
unreachable — and it appends the event in the same operation. No write path exists
that can skip an event, including tooling.

**Revision**: the registry service is a single-owner task, so the monotonic `u64`
is trivial locally; distributed, the registry rides the meta consensus group —
revision = log index, same gap arithmetic (`CONSENSUS.md`).

### 5b. Tenancy: scope in the key, authority in the handle (amendment — exact)

- **Scope is the leading component of the ref key**, not metadata:

  ```rust
  enum Scope { Shipped, Org(OrgId), User(UserId) }
  struct RefKey { scope: Scope, kind: Kind, ns: Namespace, name: Name, tag: Tag }
  ```

  Cross-scope collision is unrepresentable, and every scan is a scope-prefixed
  range — visibility filtering costs zero (range restriction, not row checks).
  **Relation to the authority plane's scope ladder (amendment 2026-08-18,
  `IAM.md` §2).** This `Scope` enum is a **projection of the IAM scope nodes**,
  not a second tenancy authority: `Shipped` ≙ the root/harness scope, `Org` and
  `User` ≙ those rungs of the ladder (`root → org → user → project → session`).
  The registry keeps its own key-prefix mechanics (they cost zero and predate
  the plane); the plane owns the tenancy *taxonomy* and answers the standing
  question. Registry refs remain **grant-ineligible across scopes** — an IAM
  grant never creates a cross-scope road; publication (below) stays the only
  one (G13), so the plane composes with §5b, it does not dissolve it.
- **Write authority lives in the handle type, not per-call checks**: a registry
  handle is constructed at authentication with its scope baked in —
  `UserRegistryHandle(UserId)` can only *construct* `RefKey`s in its own
  `User` scope (the constructor is the chokepoint; foreign-scope keys cannot
  be expressed). `Shipped` is writable only by the harness-boot publisher
  (first-boot seeding, upgrades); `Org` refs are written only by the staging
  executor carrying a Guardian approval bound to that org. Principals arrive
  from the protocol layer's key identity (per-pod HKDF chain → principal).
- **Reads/watches take the principal's visibility set**, computed once at
  handle construction: `{Shipped, Org(own), User(self)}` — scans iterate only
  those prefixes; watch events are scope-filtered by the same set.
- **Publication is a staging transition between scopes**: a Guardian-gated
  claim whose executor copies nothing content-wise (hashes are already in the
  store) — it CASes new refs in the target scope with provenance in status
  (`publishedFrom: {scope, uid, approval_hash}`); the source entry is
  untouched; un-publication is retirement of the target ref (custody
  transfer). There is no other road between scopes.
- **Laptop degenerate**: one principal ⇒ visibility `{Shipped, User(local)}`,
  `Org` prefixes empty — identical code, empty ranges.

## 6. Watch (adopted internals + the API they never shipped)

Syllium built checkpoint replay + gap detection + resync correctly and exposed none
of it — external consumers poll. Hecate ships the **external resumable watch**:

- Events carry identity only: `{revision, kind, ns, name, tag, uid, generation,
  op}` — payload is a hint at most; consumers re-read canonical state.
- Subscribe with `since = revision`; a cursor below `oldest_revision` gets a typed
  `RESYNC_REQUIRED` (their 410 pattern, our protocol's existing semantics — this is
  just the delta-stream discipline applied to registry events).
- Resync order is law: capture the high-water mark **first**, full-read, then
  replay from the mark — the race they documented and got right.
- Coalescing wakeups (N notifications → 1 pending) + a periodic bound tick, so
  notification loss is a latency bug, never a correctness bug.
- Pruning in bounded batches; retention derived; gap detection assumed always-on.

## 7. Guardian staging (adopted: inventory-from-bytes, hardened)

What a reviewer approves is **computed from bytes, never declared**:
- Canonicalize the staged tree: skip symlinks and `.git`, reject `..`/absolute/
  non-clean paths, bound file count and total bytes *before reading* — syllium's
  hardening list, adopted verbatim.
- Parse the author manifest **losslessly** (unknown keys preserved) for fidelity —
  but derive the **inventory** by scanning actual files: skills, sub-agents,
  commands, hooks, executables, MCP server declarations — sorted, deduped,
  deterministic. Executables and hooks are flagged as arbitrary-code surface.
- The approval binds to the content hash of exactly those bytes, forever
  (content-bound approval — also the SEP-2640 rule). Approval verdicts replay
  through the normal apply path so validation and audit re-run; a production entry
  changed since staging is a typed conflict (their `base_content_hash` check —
  which is just our ref-CAS again).

## 8. Non-goals, re-confirmed by fossil evidence

No live-endpoint registration. No routing role. No runtime authority (the dropped
Deployment half). And **no semantic/capability search** — the examination recovered
the fossil record of syllium building embeddings-based search (pgvector, gated
migrations, conditional-migration machinery) and deleting all of it (#462, #505):
the infrastructure blast radius exceeded the value, and the deterministic
inventory became the search surface. Our catalog queries are exact: labels, refs,
kinds, publishers.

**Loud one-way doors**: any registry format change that cannot downgrade refuses to
boot on an older binary — never silent stale reads (their bridge's documented worst
hazard, inverted into a rule).

## 9. Tests and acceptance

| # | Test | Catches |
|---|---|---|
| G1 | Writer disjointness: status ops cannot reach spec and vice versa (compile-time + runtime probes) | envelope corruption |
| G2 | Three-state upsert: `unchanged` performs zero writes, zero events (storage op counts) | re-apply churn |
| G3 | Staging wall: unstaged entries unusable in bundles; approval bound to content hash; changed-since-staging ⇒ typed conflict | ungoverned capability; stale approvals |
| G4 | Fingerprint: Merkle root stable across identical resolution; moved pin changes it; snapshot lists every input | drift invisibility; hollow lockfile |
| G5 | Non-authority: registry down ⇒ hot paths untouched; only summon/handoff/staging blocked, loudly | slow plane leaking into fast plane |
| G6 | Watch resume sweep: kill/reconnect at arbitrary revisions ⇒ no gap, no dup; below-retention ⇒ RESYNC path exercised; resync order verified (high-water before scan) | the backwards-resync race; silent gaps |
| G7 | Emission unbypassability: no code path mutates a ref without an event (architecture test — ref-CAS is the only primitive) | the trigger property lost in translation |
| G8 | Inventory determinism + hardening: same tree ⇒ same inventory on all platforms; symlink/`..`/oversize trees rejected before read | governance surface drift; traversal |
| G9 | Reverse-dep invalidation: one entry's change wakes O(dependents), measured, never a full scan | their acknowledged scaling scar |
| G10 | File-backed ≡ replicated: the same contract test suite passes both storage implementations | laptop/fleet divergence |
| G11 | Config-registered kind: a kind added via schema document (no recompile) round-trips apply/list/watch/staging; **same logical document via permuted key orders / YAML vs JSON ⇒ identical hash; duplicate keys and floats ⇒ typed errors; unknown schema keywords reject registration** | extension surface being compiled-only; hash forks from serialization dialect |
| G12 | One-way door: newer-format store + older binary ⇒ refusal to boot with a typed reason | silent stale reads |
| G13 | Tenancy: foreign-scope `RefKey` construction unrepresentable (compile probe); forged-ref CAS refused at runtime; visibility sweep — user A never observes user B's refs in list/watch under key fuzz; publication transition is the only cross-scope path (architecture test) | tenant leakage; ungoverned publication |
| G14 | Revision floor: injected replica lag, resolution at floor F on a replica at F−k ⇒ waits or forwards, never serves stale; `resolved_at_revision ≥ F` in every snapshot | silently stale bundles |

Acceptance: adding a kind is a descriptor (compiled) or a schema document (config) —
never a migration; G5/G6/G7/G10/G13 permanent; both storage implementations ship
together from first light; page caps enforced; every retention/derivation constant
carries its derivation; spec bytes are hecate-wire canonical for every kind
(compiled types or `DocValue` — no third encoding ever); scope is a key
component, never a filter; no resolution read path lacks a floor.

**Amendment (2026-08-22, TRACING/MONITORING acceptance):** this subsystem's
chokepoints emit execution spans per `TRACING.md` §3; its chokepoint registry
entries are the span roster (boot-validated; an unregistered emitter fails
startup).
