# SPEC: IAM — the authority plane

Status: ACCEPTED (grilling Branch 44, 2026-08-18). Amended 2026-08-20 (CACHE/QUEUE/FANOUT acceptance): new resource types queue + topic with their actions; cache/channel capability atoms (cache_read/cache_write/channel_publish/channel_subscribe); GAP-3 subscribe_deltas-vs-topic.subscribe clarified; each new action compiles to PEP, scope-fenced to the session/dedup line. The single control plane for
roles, policies, and permissions across every Hecate system. Companion amendments
land in the same change (§16). Sources (verbatim receipts in GRILLING.md research
index): Zanzibar ATC'19; SpiceDB/OpenFGA source; AWS IAM/STS/Organizations; Cedar
(arXiv 2403.04651) + cedar-policy crate; Vault source; OPA/Envoy-xDS/Istio/K8s/
SPIFFE; Spanner OSDI'12; CockroachDB/Pebble, etcd/bbolt, TiKV; Anderson
ESD-TR-73-51; Saltzer–Schroeder; Firecracker NSDI'20; gVisor; Goguen–Meseguer;
Denning; Flume; TUF; Google BSRS; seL4; Ed25519.

Hecate before this spec had five enforcement points with placeholder inputs and
zero management surface. This builds the plane behind them. Every prior enforcement
mechanism (Rank, SafetyPolicy, Guardian gates, Biscuit grants, claim affordances)
becomes a CONSUMER governed by this plane; none is a piece of it.

## §0 The separation law

The ledger drives and proves agent **work**. No IAM object — role, policy, binding,
mandate, grant — lives in it; no IAM management operation is a claim. Work facts (an
active claim on a subject, session membership) enter decisions ONLY as
request-context attributes read at evaluation time — the OpenFGA contextual-tuple
shape ("treated as if they were actual stored tuples during the evaluation of that
request… not persisted"). The IAM plane has its own store (§3), own APIs (§13), own
audit (§11).

## §0b The enforcement layers

```
L5  JUDGMENT    Guardian/Arbiter/Inspector verdicts — agentic; never folds into policy
L4  POLICY      roles · policies · bindings — the discretionary layer (§5 decision tree)
L3  CEILINGS    SafetyPolicy ∧ Guardian hard-block classes ∧ mandate scope-downs — intersection-only caps
L2  STRUCTURE   population walls · sensor tighten-floor · writer laws · no-authority-transfer
L1  SESSION     the envelope: cross-session = NONEXISTENCE at resolution. One door: the brokered Grant
L0  METAL       microVM boundary · host-side plane · attestation-born identity
```

Composition is intersection downward, non-bypass upward (SELinux MAC-over-DAC: "the
subject cannot decide to bypass the policy rules being enforced by the MAC policy").
Sessions are the mandatory layer (Goguen–Meseguer noninterference: "what the first
group of users does… has no effect on what the second group of users can see"); L4
is discretionary authority INSIDE the envelope.

## §1 The object model

Seven object kinds, all hecate-wire records in the authority store; policy text and
compiled artifacts are CAS content referenced by hash.

| Object | Shape | Note |
|---|---|---|
| **Principal** | immutable UID + kind (`agent_pod` \| `system_service` \| `user` \| `external` \| `node`) + attestation binding | A Principal is a Participant viewed by the authority plane — 1:1 map (`agent_pod`→agent, `system_service`→service/system, `user`/`external`→external, `node`→system). **Wire never branches on kind; kind is a read field** (CONTEXT.md Participant law). Identity minted elsewhere (summon mint / node enrollment); IAM stores no keys. **Two agent principals may share one pod** (amended 2026-08-22 — the primary and its Scribe are distinct principals; each is bound to its own per-workload flow identity, so speaking-as is proven by key custody, per container, never by pod membership). |
| **Resource** | typed ref `(resource_type, scope_id, name)` | Types are a closed schema-versioned taxonomy (§4). Instances live in their owning system; IAM only names them. |
| **Role** | named versioned bundle: policy statements + assumption trust statement + optional ceiling | Shipped role packs per office + system roles; scope-authored roles. |
| **Policy** | owned Cedar-dialect statements; text in CAS, hash + attachment metadata in the record | Mandatory scope structure (Cedar indexability). |
| **Binding** | `(principal-or-set, role, scope, caveats?, expiry?)` | The ReBAC row — SpiceDB caveat-name + JSON context + optional expiration. |
| **Ceiling** | a policy whose statements only cap, never grant | SafetyPolicy compiles to the user's; Guardian hard-blocks are a shipped one; mandate scope-downs are ceilings frozen at assumption (AWS: "the resulting permissions are the intersection… cannot grant more"). |
| **Mandate** | derived principal `(base, role, scope, scope-down ceiling hash, expiry, provenance chain, tags)` | The assumed-role object (§9). Named Mandate to avoid collision with CONTEXT.md's Session. |
| **Grant** | narrow evaluated epoch-stamped durable decision `(principal, action, resource, caveats, mint epoch, expiry, revocation id)` | Biscuit token = the portable projection (Branch 25 owns serialization); the store row is the authority. |

Plus the **Schema**: resource-type + action taxonomy + attribute types, versioned,
content-hashed — the small config plane, fully replicated (Zanzibar ratio: ~1,500
configs vs 2T instance rows).

## §2 Scopes and tenancy — the blend

Flat-store + namespace-control + no-Vault-limits is the shipped architecture of AWS
Organizations, GCP Resource Manager, and K8s HNC (HashiCorp's overflow guidance
concedes it: "the entire list of namespaces must fit into a single storage entry…
use namespaces sparingly"). Three separated planes:

- **Instances, flat, self-routing.** A scope is an immutable fixed-size ID; every
  reference embeds it — the ARN pattern: the reference IS the route. Reparenting
  never changes identity (GCP: "Project ID and number stays the same… Direct IAM
  roles stays the same"; only the inherited overlay recomputes).
- **Hierarchy as relation rows.** The **scope ladder** (policy inheritance) is
  `root → org → user → project → session` — depth-bounded ≤ 5 (AWS ≤5 OU levels, GCP
  ≤10 folders; Vault's ~160 is the residual-byte-budget outlier). The lineage **fork
  tree** is NOT the ladder — forks of any depth share their project's chain unless a
  policy is explicitly attached. On a laptop the org rung is degenerate.
- **Separate from the failure-domain tree** (`node < AZ < region`), which governs
  authority PLACEMENT, not inheritance. Each scope's records place on the group
  owning its **epoch scope** by holder-containment (CONSENSUS §6): session→session
  group, user/org→home-region group, root + lineage (holders anywhere)→root group.
  Caveat: a globally-mobile principal's holders force root scope.

**Visibility is resolution-layer nonexistence** (GitHub: "GitHub uses a 404 Not
Found response instead of a 403 Forbidden response to avoid confirming the existence
of private repositories"). A principal resolves exactly its direct chain ∪
explicitly granted refs; everything else is unresolvable — no exists-vs-denied
oracle, uniform error shape and timing. Delegated administration is a binding —
"admin of scope S" grants IAM-management actions on S's subtree, structurally
contained (records under S's keyspace; non-interference invariant: no admin(S)
action changes any decision outside S's subtree). Zero per-scope machinery (Hecate
has one identity mint by construction).

## §3 The authority store — a global authorization store

Grounded by the storage dossiers (NO-PRECEDENT: no global authz plane uses an
in-memory arena as authority; all use a replicated on-disk engine with derived
in-memory indexes). An **owned log-structured MVCC engine (LSM)** assembled from
Hecate primitives — not a new filesystem, not the ledger arena.

**Record**: `IAMRecord { key: (scope, kind, id), revision: LogPos, body: Live(hecate-wire) | Tombstone }`,
sorted `(key asc, revision desc)`. Update = higher-revision record; delete =
tombstone; point-in-time = read at revision ≤ R (**per-scope** — LogPos is
per-group; the committed/applied index, never a raw append position).

**Layout, composing existing machinery:**

| LSM concept | Hecate primitive |
|---|---|
| Write-ahead log | consensus WAL — `IAMRecord` as a **hecate-wire schema kind inside raft entries** (not a new `kind:u8`) |
| Memtable | in-RAM sorted map (single-owner task) |
| Sorted run | a **content-addressed manifest object** — hecate-wire sorted records + in-run key→offset index block + bloom, CDC-chunked into ordinary pack chunks (NOT a single blob, NOT a second on-disk format) |
| Store root ("IAM root") | the group's **checkpoint** (WAL §6 floor API) naming current run-hashes per level + min/max key + min/max revision for zero-fetch skip |
| Compaction | **new record-merge logic** (drop superseded + sub-floor tombstones → new run); pack copy-forward only reclaims the retired runs' dead chunks. OBJECT_TIER §2 endorses the consumer: "LSM earns its complexity only for mutable keys… keeps bulk bytes outside the tree" |

**Write**: mutation → hecate-wire IAMRecord → consensus append through the owning
scope's group → quorum-ack (durable) → apply to memtable, advancing the applied
watermark **in the same transaction** (CONSENSUS §2). **Flush**: full memtable →
sorted run → pack chunks → IAM-root/checkpoint advance → covered log prefix retires.
**Read**: memtable then runs newest-first with bloom/key-range/min-rev skip; below
the GC floor ⇒ typed "revision expired" (etcd "required revision has been compacted"
shape). **The hot path never reads the store** — PEPs decide from compiled residuals;
store reads happen at compile time.

**Replication (dual, both existing):** the log via consensus/Raft across the scope
group's replicas (quorum, ordering, authoritative); run chunks by hash via the
content plane (TRANSFER already-addressed path, `batch_exists` dedup free). Bootstrap
= fetch IAM-root@floor → fetch run chunks → tail log from floor (WAL snapshot+tail;
CONSENSUS §5 snapshot-via-transfer-plane).

**Global read-locality:** reads use **ReadIndex** (CONSENSUS §3 — "Lease reads do not
exist"); for session/user/org scopes the owning group is region-local, so ReadIndex
is region-local — no cross-region RTT (Zanzibar: Safe requests "served within the
region most of time," two orders above Recent). Root/lineage reads ride the
compiled-residual escape. Genuine local-follower reads would be a CONSENSUS
amendment, never a flag.

**GC floor** = max(grant TTL, mandate TTL) + freshness window T + audit-replay margin
(derived), advanced by a **node-level amortized sweep or event** — never a per-group
timer (CN2). **Reachability index** (WhoCan/WhatCan) = a derived in-RAM Leopard
set-index (offline + Watch-fed incremental), under a **derived RAM budget**
(OBJECT_TIER §2 index formula: items × per-entry cost vs node anchor + escape hatch),
scope-partitioned, evictable — never authority (authority = merge-read of runs at a
revision). **Availability**: region partition ⇒ local replica serves reads under
last floor (Masked); writes to away-scopes wait (Degraded) — KMS shape: "all data
and authorization information required to execute an API request is available on all
regional hosts."

**FAULTS cells** (required, boot-validated) for `{IAM store, compaction, reachability
index} × {every fault class}` ride existing dispositions: run chunks =
re-fetch-by-hash; IAM-root = rebuild-from-quorum / N=1 refuse-loudly; index =
discard-and-re-derive.

## §4 The capability taxonomy

Closed per-resource-type action enums, schema-versioned. **Existence law**
(generalizing SKILLS_API's `compile_to_warden`): an action exists only with a
registered `compile_to_pep()` mapping; the boot classifier fails startup on an
unmapped action or unclassified PEP (Saltzer–Schroeder complete mediation: "every
access to every object must be checked for authority").

| Resource type | Actions | PEP |
|---|---|---|
| `claim_plane` | `issue_claim(kind, target_office, subject_class)`, `receive_work(kind)`, `testify`, `consult(domain)`, `challenge(domain)`, `route_work(subject_class)`, `read_claim(kind)`, `read_testament(kind)`, `read_artifact(type)`, `attach_artifact(type)`, `evaluate_validation(kind)`, `subscribe_deltas(class)`, `supersede` | ledger affordance guard (write/execute) + ledger serving edge (read) |
| `peer_channel` | `open_flow(peer_class)`, `send(lane_class)` | transport (FlowKeyGrant) + warden lanes |
| `session` | `create`, `fork`, `dispose`, `attach_lineage`, `read_archive` | Guardian admission + Sibyl |
| `summon` | `summon(workload_class)`, `scale(class)` | Guardian admission (scheduler executes) |
| `volume` | `claim(role, access_mode)`, `attach`, `read`, `write`, `seal`, `snapshot_read` | serving boundary + warden mount scopes |
| `green` | `read(version)`, `propose_increment`, `approve_disk_commit` | merge gate |
| `lineage` | `fork`, `land(head)`, `materialize(target)`, `push`, `fetch`, `protect_ref`, `set_required_validation` | landing engine + lineage service |
| `secret` | `reference(scope)`, `request_materialize`, `grant`, `revoke`, `rotate`, `erase(scope)` | vault resolver + warden mount-write fence |
| `registry` | `publish(kind)`, `stage_approve`, `install`, `provision(recipe)` | Guardian staging + provisioner |
| `skill_tool` | `invoke(capability_bits)`, `load(bundle)` | warden atoms + Guardian skill gate |
| `egress` | `connect(destination_class)`, `resolve(name_class)` | provider gateway + Guardian hard-block |
| `observability` | `read_series(sel)`, `read_events(class)`, `read_trace(target)`, `join_work(claim)`, `read_health` | the collector query serving edge (`COLLECTOR.md` §9) + the health-plane edge — row amended 2026-08-22 (COLLECTOR acceptance); `join_work` additionally requires the caller's ledger-read capability per plane (no bridge) |
| `knowledge` | `query(forest)`, `contribute(class)`, `read_index`, `import_corpus` | field/index serving edge — `contribute` targets only knowledge-graph/documents organs, NEVER the Forest field (no emission path exists) |
| `queue` | `create`, `enqueue`, `lease`, `ack`, `nack`, `purge`, `dead_letter_read`, `snapshot_read` | queue service serving edge (partition sequencer) — scope-fenced to the session/dedup line |
| `topic` | `publish`, `create_subscription`, `subscribe`, `set_filter_policy`, `delete_subscription`, `redrive` | topic router serving edge — scope-fenced to the session/dedup line |
| `iam` | `create_role`, `put_policy`, `bind`, `unbind`, `assume(role)`, `revoke_mandate`, `mint_grant`, `revoke_grant`, `audit_read(scope)`, `put_schema` | the management surface (§13) — `principal_kinds: [user, system_service]` only |
| `node_plane` | `enroll`, `claim_support`, `reconfigure(group)`, `place(unit)` | meta-tree admission — system principals only |

No stringly actions; adding an action is a schema-version publish that re-runs boot
classification.

The cache/pub-sub **channel capability atoms** `cache_read`, `cache_write`,
`channel_publish`, `channel_subscribe` (CACHE §10) are **warden capability atoms** —
node-local pod-boundary capabilities the warden already gates (the
`skill_tool.invoke(capability_bits)` class), **not resource types**. Each compiles to
PEP via `compile_to_warden` (the existence-law generalization above) and, like
`queue.*`/`topic.*`, is scope-fenced to the session/dedup budget line (CACHE §2 pool =
session/dedup isolation domain; VFS §4).

**GAP-3** (delta subscription ≠ general pub-sub): ledger deltas KEEP
`claim_plane.subscribe_deltas` (ledger serving edge); general pub-sub subscription
uses the new `topic.subscribe` (topic router serving edge). The two are **distinct
actions with distinct PEPs** — `subscribe_deltas` supplies the **precedent shape
only**, never the same action (no double-mapping).

## §5 The decision procedure — the decision tree

One deterministic owned Rust evaluator, a Cedar-dialect (cedar-policy crate + Lean
model as a dev-only conformance oracle). Cedar's skip-on-error is replaced by **typed
per-effect fail-closed** — Cedar's own docs name the hazard ("if a policy's
evaluation returns error… it is skipped" → an erroring forbid silently stops
forbidding). `decide(P, A, R, S, C, floor) → Verdict`, evaluated closed-world over
the compiled artifact + request context slice (no I/O mid-decision):

```
0 SCHEMA CHECK      unknown type/action or artifact schema-hash mismatch → REFUSE_UNKNOWN
1 FRESHNESS FENCE   local applied epoch ≥ floor (grant|token|request); else REFUSE_STALE
                    (sealed grant bridges its own mint race — KMS grant-token shape)
2 STRUCTURE         population wall · sensor tighten-floor · isolation · writer laws → DENY_STRUCTURAL
                    (never overridable by any permit)
3 EXPLICIT FORBID   any forbid on the direct chain (depth ≤5) + ceilings → DENY_FORBID
                    eval error in a forbid → DENY_ERROR (fail closed)
4 CEILING ∩         SafetyPolicy ∧ hard-blocks ∧ mandate scope-down ∧ every ancestor's scope-down
                    any ceiling lacks an allow → DENY_CEILING   (AWS: ceilings INTERSECT)
5 PERMIT ∪          union of permits from bindings + grants on the direct chain
                    none → DENY_DEFAULT (+ nearest-miss diagnostics)
→ ALLOW {determining policies, epoch, obligations: TTL / caveats / narrowing / user-approval}
```

Receipts: deny-first + default-deny (AWS enforcement flow; Cedar "no request is
authorized unless there is a specific permit… by default, the decision is Deny");
forbid-overrides (Cedar verified property; Vault most-specific-deny); ceiling
intersection (AWS "intersection of the two categories"); permit union (GCP
inheritance). **Ceilings always bind** — the AWS resource-policy-names-session-
principal bypass is unrepresentable.

**Verdict → affordance mapping** (honoring CONTEXT.md "refusal is reserved for
structural invariants"): `DENY_DEFAULT`, `DENY_FORBID`, and `DENY_CEILING` on agent
tool calls surface as **inform or yield** (the park-to-grant claim — the secrets §5
flow generalized; the claim drives the user decision, the authority lands in the IAM
store). **Refusal** is reserved for `DENY_STRUCTURAL`. `REFUSE_STALE` is availability,
not authorization — retry/park, never proceed. Every verdict carries provenance
(determining policy IDs + epoch — Vault `GrantingPolicies` / AVP `determiningPolicies`).

## §6 Compile-and-distribute (networking + caching)

**Amended 2026-08-22 (the five-site flow-key landing):** distribution is
**per-workload**: a pod receives one residual per container (the primary's and
the Scribe's compile separately — different principals, different capabilities),
installed by hecate-init pre-spawn and pinned; each artifact is addressed to its
workload's flow identity, so a residual can never be delivered to, or claimed
by, the pod's other workload.

Per-scope compilers (owned tasks, derived caps, `Cancelled`-handling; classified
CAS-first at boot) tail the store watch and emit **per-PEP artifacts** — compiled
indexed matchers, not policy text (Vault radix-tree + capability-bitmap; nobody scans
text per request). Per-pod warden profiles are **residual policies** (cedar typed-
partial-evaluation: principal/scope/bindings fixed, action/resource/context free) —
a warden verdict is a µs lookup.

**Artifact contract** (composite; each element receipted): content-addressed, signed,
roots-scoped (OPA bundles), stamped `(epoch, schema hash)`. **Networking**: pointer/
activation records ride directed class-3 (never-shed) or supersession control
datagrams; watch rides the demand-driven ordered-log subscribe/cursor/RESYNC
(PROTOCOL §4); artifact bytes ride the content plane by hash (TRANSFER). Host↔host
plane traffic uses hecate-quic sessions, Noise-IKpsk2 identity, TransportRegistry-
classified `(hop_kind, lane, enforcement_point, flow_identity)`. **Caching**: residual
artifacts cached at each PEP (content-addressed — safe; new epoch = new hash = new
fetch); decision memoization at serving-edge PEPs keyed on `(subproblem,
artifact-epoch)` — the Zanzibar quantization shape with the epoch as the quantum. NOT
cached: the core affordance guard (fresh from core-local state — already O(1)
local); context slices (per-request). Revocation = epoch bump + push; a feed silent
beyond T fails the PEP closed for NEW effects; stated bound: revocation bites within
min(push, T), TTL backstop.

## §7 Per-system intercepts

Uniform frame: intercept moment → request → PEP + artifact → lifecycle → dispositions.

- **7.1 Transport / agent↔agent.** `FlowKeyRequest{P,Q}` issues a key ONLY if the
  policy edge exists at current epoch — the mesh default-deny moment (policy names
  principals, not addresses; SPIFFE/Istio). `send(lane_class)` enforced per-frame by
  the warden. **Revocation bites through the warden's per-frame residual, not the
  key-epoch machinery** (that bumps only at summon/handoff/restart). Handoff mints
  successor edges before custody transfer.
- **7.2 Ledger.** The existing affordance guard consumes the compiled session
  artifact **core-locally** (the artifact arrives as a WAL-logged input record, the
  score-snapshot pattern — zero cross-plane RTT; L3/L13 preserved). IAM decides
  **standing** (may issue this kind); claims keep work-order + scope. Rank composes
  as a compiled projection of RANK's single source (freshness-exempt; K1–K8 intact;
  clarification challenges exempt from the permit layer). caused_by is
  principal-carried, verified against core-known turn context at commit.
- **7.3 Pods/warden.** Warden inputs = (SafetyPolicy ceiling, residual artifact,
  capability atoms, claim-scope context) — the ratified compile-source list with real
  inputs (PODS §6 amended in the same change). Sensor stays tighten-only, a DENY layer
  above the evaluator (not a `decide()` step). Hold-and-escalate to Guardian unchanged;
  verdicts compile back as policy with provenance.
- **7.4 Summon.** Guardian admission evaluates `summon(workload_class)` and
  **pre-validates the requested bindings** — confer semantics (PassRole): a
  confer-permit lets you confer a role without holding its capabilities (the Guide
  summons write-pods holding no write caps). Bindings commit before the pod's first
  turn.
- **7.5 Volumes.** Authorization at **attachment creation** (VFS §3b — "no claim, no
  attachment, no mount"); the attachment's warden scope entries are compiled from the
  volume-claim bindings; per-IO is warden-local. Re-bind re-evaluates.
- **7.6 Merge gate.** `propose_increment` (submitter holds the work — context slice
  carries the claim-on-subject); `approve_disk_commit` (Arbiter office permit;
  Architect joins per MERGE, not minted in IAM; user approval rides SafetyPolicy
  **`disk_write_mode`**). Validation judgment stays agentic.
- **7.7 Sessions/landing/materialization.** `materialize` = lease-holder ∧ IAM permit
  ∧ zero conflict values ∧ **user-validation review gate** (SESSIONS §6). Sibyl judges
  cross-fence sharing (user-plane claims drive it — the Sibyl mints them as itself);
  the Grant is the authority object, Biscuit-projected, manifest-ref-scoped,
  epoch-caveated.
- **7.8 Secrets.** `reference` = scope visibility (population wall structural at step
  2); `request_materialize` parks to grant on DENY_DEFAULT; the mount-write fence is
  one instance of §6's freshness fence; the vault index keeps envelope-root/lease
  **projections** pinned to grant epochs (IAM store owns grant authority — SECRETS
  §4/§7 reworded when SECRETS.md is written).
- **7.9 Registry.** `publish` by publisher principals; `stage_approve` = a
  **fail-closed conjunction** (REGISTRY owns content-bound approval; IAM owns
  standing); role-pack bindings derive from AgentRole; registry resources are
  **grant-ineligible cross-scope** (publication stays the only road — G13). Skill
  `load`: bundle capability bits ⊄ pod chain ⇒ refuse naming missing atoms.
- **7.10 Egress.** System egress (provider gateway, boot-resolved permits); workload
  `connect(destination_class)` at the Guardian-policed virtual-network edge. Guardian
  mid-stream severance stands above (SUMMONING §4 sever-existing preserved — IAM
  composes, never weakens). External inbound = external principals with
  ExternalId-shaped trust caveats (confused-deputy cure).
- **7.11 Repo (Branch 35).** `protect_ref` = an explicit forbid (deny-beats-allow
  gives branch protection teeth); CODEOWNERS = path-relation policies;
  `set_required_validation` attaches merge-gate obligations at land.
- **7.12 Observability/knowledge.** Reads at the health/field serving edges (Scribe:
  its primary only; Guardian broad; user per SafetyPolicy). Forest is session-fenced
  by design; same-user cross-session **recall is up-chain scope visibility** (archive
  at user scope), not a cross-fence reach. Ingestion is a registered emission surface;
  knowledge items carry contributor identity + epoch; influence stays advisory.
- **7.13 IAM self-governance.** `bind` requires the binder hold the role's
  confer-permit at that scope; `put_policy` needs scope-admin; `put_schema` is
  root-only/system/harness-release-gated. **Root anchors** (root role pack + root
  bindings) ship versioned in the signed harness release (TUF: "root of trust must
  not rely on external PKI"); user supremacy is a root anchor no agent-authored policy
  can forbid.
- **7.14 Node plane / boot.** Node principals minted at enrollment (attestation);
  `reconfigure`/`place` are system-only, decided from consensus-logged state (SCH1
  determinism). Boot: node identity → root anchors → IAM partitions recover →
  compilers emit → boot classifier verifies total coverage → workloads admit.

## §7b Ledger governance matrix + anti-hijack

Granular see/write/execute, with the **metadata/content split at the serving edge**
(not in the core, not by redacting deltas — the core stays identity-blind and emits
the whole byte-identical stream; the per-pod ledger serving edge filters per recipient
via subscription classes + traverse filtering; default within-session posture stays
broad, restriction is optional tightening):

| Object | See | Write | Execute |
|---|---|---|---|
| Claim metadata | session-visible (coordination substrate); cross-session nonexistent | `issue_claim` — edge-checked **both ends** (H1) | activation = **receipt-holder only** (structural) ∧ `receive_work(kind)` |
| Claim content | parties + `read_claim(kind)` | — | — |
| Testament | parties + `read_testament(kind)` | `testify` = receipt-holder only (structural) | per kind-specific receipt authority |
| Artifact | `read_artifact(type)` ∩ source-scope visibility | `attach_artifact(type)` on own testament | — |
| Validation | rides claim visibility | issuer-**owned**, multi-**contributed** (Guardian validators enter w/ contributed_by) | `evaluate_validation` = designated evaluator |
| Delta | `subscribe_deltas(class)`; cross-session nonexistent | none (system-emitted, post-commit) | — |

**Anti-hijack laws** (OWASP Excessive Agency: "implement authorization in downstream
systems rather than relying on an LLM to decide if an action is allowed"): **H1**
double-entry edges (issuer + receiver both checked). **H2 — no authority transfer**
(Hardy confused-deputy + ocap Property-D no-ambient-authority): executing another's
claim confers the issuer's authority NEVER; the executor acts with its own authority
∩ an explicitly attached, caveated, TTL-bound Grant. **H3** unforgeable provenance
(core-stamped kind + caused_by; user-plane-kind claims unrepresentable for agents).
**H4** typed claims (satisfaction judged by the designated evaluator, not free text).
**H5** content ≠ authority.

**Derived-data law** (Denning lattice ⊕ = least-upper-bound; Flume label
propagation): derived data (knowledge, embeddings, narration, summaries, artifacts)
inherits the **most-restrictive** scope set of its sources; visibility =
chain-visibility of all sources. **Governed scope-lifts** are the explicit up-chain
transitions: promotion→archive (user scope), publication→published scope, merge-gate
landing→lineage scope, vector re-baseline→generation. Ingestion into any derived
store is a registered emission surface.

## §8 Boundary-complete enforcement

The pod boundary IS the complete effect surface (every effect crosses a host
chokepoint by construction: VFS server-side, lanes warden-terminated, egress
gatewayed, claims through the core). Industry line: Firecracker jailer + 24-syscall
seccomp profile at the VMM boundary; gVisor "the application's direct interactions
with the host System API are intercepted by the Sentry"; virtio-fs "the virtiofs
daemon runs on the host"; K8s "authorization of API requests takes place within the
API server" and nothing authorizes processes inside a container. So: **full
decision-tree evaluations at boundary events** (attach/load/grant/edge/summon/submit
— rare); **compiled residual checks per boundary operation** (constant-time; seccomp
29–106 ns per filter is the analogue); **in-guest computation is free** (interior
objects were authorized in and can't leave except through checked operations); the
sensor watches the interior tighten-only. We never rely on the VM for anything
outside it; §12 bounds escape consequences.

## §9 Assignment, assumption (Mandate), chaining

**Assignment** — three doors: summon manifest (admission-validated), session template
(default office bindings stamped at birth — never bare), management surface (user
pane / scope admins; user decisions arrive as answered park-to-grant claims).
**Assumption**: `assume(role, scope)` mints a **Mandate**; effective authority = base
∩ role ∩ scope-down (intersection-only; AWS "cannot grant more"). **Chaining**
re-intersects each hop (monotonic narrowing), depth-capped (derived), chained TTL ≤
min(parent remaining, derived cap) — the AWS 1-hour-chain anti-laundering bound.
**Provenance**: a set-once immutable origin (AWS SourceIdentity: "cannot be changed
during the role session… persists when a role is used to assume another role") in
every decision context and audit record. **Revocation**: Mandates are store rows —
epoch bump kills within min(push, T) (Vault late-binding + SPIFFE short TTLs
backstop).

## §10 Worked examples

**Dialect** (Cedar-shaped): `permit|forbid (principal, action, resource) when { <conditions over context> };`
— `in` walks the scope chain; entity refs and context attributes are the request slice.

Engineer role pack (shipped):
```
permit (principal in Office::"engineer", action == Action::"volume.write", resource)
  when { resource.scope in principal.session && resource.role == "work" };
permit (principal in Office::"engineer", action in [Action::"green.propose_increment",
        Action::"claim_plane.testify", Action::"claim_plane.consult", Action::"claim_plane.challenge"],
        resource) when { resource.scope in principal.session };
permit (principal in Office::"engineer", action == Action::"skill_tool.invoke", resource)
  when { resource.capability_bits <= principal.chain_bits };
forbid (principal in Office::"engineer", action == Action::"green.approve_disk_commit", resource);
```

Guide role pack (zero write capability; confers via summon):
```
permit (principal in Office::"guide", action == Action::"summon.summon", resource)
  when { resource.workload_class in principal.confer_set };   // confer-permit, not the caps
permit (principal in Office::"guide", action in [Action::"claim_plane.issue_claim",
        Action::"claim_plane.route_work"], resource) when { resource.scope in principal.session };
forbid (principal in Office::"guide", action == Action::"volume.write", resource);
```

SafetyPolicy ceiling (user-owned, compiled from config — caps, never grants):
```
forbid (principal, action == Action::"egress.connect", resource)
  when { context.safety.network_egress == "deny" };
forbid (principal, action == Action::"green.approve_disk_commit", resource)
  when { context.safety.disk_write_mode == "user" && !context.user_approved };
```

System role — autoscaler: `permit (principal == System::"autoscaler", action == Action::"summon.scale", resource) when { resource.class in principal.managed_classes };`.
The Guardian-unchallengeable rule and the rank pack are shipped structural/projected
forms (§7.2, §12); the remaining offices follow the same shape, each binding its §4
actions, the cannot-fold trio staying agentic.

## §11 Storage / networking / caching / audit — consolidated

| Data class | Home | Mechanism |
|---|---|---|
| Bindings, mandates, grants, roles (records) | owning scope group's LSM (§3) | MVCC records, consensus log + pack-chunked runs |
| Policy text, compiled artifacts, schema docs | CAS (BLAKE3), pinned by live IAM records (GC roots) | content-addressed; OBJECT_TIER §7 root list amended |
| Residual artifacts | PEP-local RAM, backed by CAS | content-addressed cache; epoch-invalidated |
| Reachability index | in-RAM, derived-budget, scope-partitioned | Leopard; discard-and-re-derive |
| Audit — decisions | the one scope-tagged operational log, `op-critical` lane (`COLLECTOR.md` §4 — "session/operational stream" are the two scope-classes of the one log; amended 2026-08-22, COLLECTOR acceptance) | epoch-stamped facts `{principal, action, resource, verdict, determining-policies, epoch}`, valueless, fail-closed (OPA decision-log shape) |
| Audit — mutations | the IAM log itself | free (Zanzibar changelog-in-the-txn) |

Decision facts may ride the ledger/operational log (the secrets precedent); authority
objects never do (§0).

## §12 Tamperproof — "near physically impossible"

Anderson's triad (ESD-TR-73-51: "tamper proof / always invoked / small enough to be
subject to analysis"), as eight structural facts:

- **T1** every IAM component is a host-side task; **zero IAM code in any guest image**.
- **T2** management endpoints are **nonexistent** on pod lanes (TransportRegistry
  classifier fails boot if a pod-facing lane could address them) — for an agent the
  IAM API is absent, not forbidden.
- **T3** `iam`/`node_plane` actions are schema-marked `principal_kinds: [user,
  system_service]`; an agent binding is **unrepresentable** (refused at encode via
  distinct principal types, and at apply) — the K8s NodeRestriction precedent.
- **T4** every IAMRecord and IAM-root carries the management-service Ed25519 signature
  (custody/rotation in Branch 25's key hierarchy; RUNTIME §4 FFI/lint allowlist),
  **verified at apply on every replica, at run-load, and on snapshot install** — a
  compromised node or leader cannot inject authority (TUF thresholds). ~25–40 µs/verify
  (ed25519-dalek), apply-path, off the read path. **Requires the scoped FAULTS §1/§3
  amendment** admitting a compromised-host adversary for the authority plane —
  deliberate, documented.
- **T5** PEPs activate only signature-valid, epoch-monotonic artifacts.
- **T6** bounded blast radius (BSRS "compartmentalize the impact"): a fully
  compromised node yields resident sessions' state, its replicas, nothing more — no
  store mutation (no signing key), no foreign-session reads (L1), no artifact forgery
  elsewhere. The colocation law is also the compromise-containment law.
- **T7** root/region mutations carry a multi-party user-approval obligation
  (Saltzer–Schroeder separation-of-privilege; BSRS MPA).
- **T8** in-guest probing of management endpoints is a high-weight sensor signal →
  hard block + quarantine.

Verifiable = the owned evaluator with the Lean conformance oracle + cluster-SIM (seL4:
"confidentiality: seL4 will not allow an entity to read data without having been
explicitly given read access"). Agents touch authority through exactly one aperture:
asking in the work plane (a park-to-grant claim the user answers), written by
user/system principals through a surface agents cannot see or forge into.

## §13 The management surface

Management actions are `iam`-family schema actions decided by the same tree (§5).
Write verbs: `create_role`, `put_policy`, `bind`/`unbind` (with MUST_MATCH/
MUST_NOT_MATCH preconditions — SpiceDB shape), `assume`, `revoke_mandate`,
`mint_grant`, `revoke_grant`, `put_schema` (root-only). Query verbs: `WhatCan(P)` /
`WhoCan(R)` (Zanzibar Read/Expand shape; bounded — direct-chain walk ≤5 + binding
fan-in via the reachability index), `Simulate` (decide against a hypothetical
artifact — Envoy shadow-rule / OPA precedent), `Watch` (the store feed). Audit query:
`audit_read(scope)`. All are IAM-plane APIs with IAM-plane audit — never claims.

## §14 Scale walks

**Meta, cross-region** (10⁵ users × 10² sessions × regions): a pod tool call → warden
residual, node-local, zero RTT. A flow key → session-scope artifact,
session-group-local. A binding change at user scope → the user's region-partition
commit (region-local Raft), pushed to that user's artifacts; root is touched only by
rare role-pack/schema publications (Spanner singleton lesson: nothing global on any
request path). Store growth shards by scope ID; no artifact scales with tenant count
(KIP-500 counter-receipt). Existence proof three orders beyond need: Zanzibar, 10M+
QPS, p95 <10ms, 99.999%/3yr on exactly this shape. **Laptop, N=1**: the failure-domain
tree collapses to one node — root ≡ org ≡ region ≡ one group hosting all partitions;
one replica; single-member self-ack consensus; memtable + local pack tier. Same
records, MVCC, LSM, evaluator, epochs, T. **Zero modes** — the laptop is the fleet with
N=1 in every derivation.

## §15 Test matrix

| # | Test | Catches |
|---|---|---|
| IAM1 | Boot coverage: unmapped action or unclassified PEP ⇒ startup failure | unenforced authority |
| IAM2 | Decision determinism: `decide()` pure over (request, artifact, slice); differential replay, zero divergence | REAL≠SIM |
| IAM3 | Typed fail-closed: error-in-permit grants nothing; error-in-forbid fails the decision closed | Cedar skip-on-error hazard |
| IAM4 | Conformance oracle: differential vs the Cedar model on the shared subset | semantic drift |
| IAM5 | Default deny: empty policy set ⇒ non-structural requests deny with diagnostics | ambient authority |
| IAM6 | Forbid supremacy fuzz: permits never overturn a matching forbid; protected-ref forbids hold | deny-beats-allow regression |
| IAM7 | Ceiling conjunction: allowed only when every ceiling allows; session-principal bypass unrepresentable | the receipted precedence hole |
| IAM8 | Chain monotonicity: authority never grows along a chain; provenance immutable; chained TTL ≤ min(parent, cap) | privilege re-amplification |
| IAM9 | Freshness fence: feed silent > T refuses new effects; grant bridges its mint race; revocation ≤ min(push,T) | unbounded stale-allow |
| IAM10 | Epoch fence both directions | new-enemy problem |
| IAM11 | Flow-key default-deny: no edge ⇒ no key; unbind ⇒ next-frame residual kills the flow | mesh bypass |
| IAM12 | Ledger-guard core-locality: standing check from core-local state, zero cross-plane RTT (measured) | authority checks leaving the core |
| IAM13 | Summon confer: cannot bind authority you lack a confer-permit for; escalation paths refused | confused deputy at the source |
| IAM14 | Warden residual ≡ full evaluator over the pod's request space | compile/eval divergence |
| IAM15 | Skill-load meet: bundle bits ⊄ chain ⇒ refuse naming missing atoms | capability smuggling |
| IAM16 | Structural walls unrepresentable for agents (secret://system, node_plane, sensor-widen) | wall breach via policy |
| IAM17 | MVCC point-in-time reproduces historical verdicts within the window; outside ⇒ typed expiry | broken audit replay |
| IAM18 | Partition split/move under load: no lost/duplicated authority; lazy repair; decisions uninterrupted | the registry cliff |
| IAM19 | No object scales with total scope/tenant count (architecture test + measurement) | the Vault/KIP-500 anatomy |
| IAM20 | Depth-bound cost: f(ladder ≤5, chain bindings), constant in total scope count | hidden O(tenants) walks |
| IAM21 | Laptop ≡ fleet: every test byte-identical at N=1 | mode divergence |
| IAM22 | Management self-governance: each iam verb needs its own permit; root anchors immutable at runtime | the plane exempting itself |
| IAM23 | Audit totality: every decision + mutation carries provenance + epoch; unwritable audit fails the op; no value material | silent decisions; audit leaks |
| IAM24 | Affordance mapping: DENY_DEFAULT/FORBID/CEILING → inform/yield; refusal only for structural | the language law regressing |
| IAM25 | Effect-path completeness: every guest-originated effect terminates in a classified chokepoint | a sixth path |
| IAM26 | Interior freedom: in-guest 10⁴-file walk shows zero decision-tree evaluations; overhead ≤ derived bound | per-IO policy creep |
| IAM27 | Ledger matrix: see/write/execute enforced; cross-session unresolvable, not denied | coordination loss / overexposure |
| IAM28 | No-authority-transfer: executing P's claim never yields P's authority; attached Grant widens exactly its caveats | hijack elevation |
| IAM29 | Provenance forgery: forged caused_by / user-plane-kind records unrepresentable at encode + refused at apply | forged directives |
| IAM30 | Derived-scope: mixed-source items carry the most-restrictive set; read-via-index laundering fails | information-flow laundering |
| IAM31 | Visibility oracle: probing foreign scopes returns uniform nonexistence (shape + timing) | the confirmation oracle |
| IAM32 | Reparent atomicity: decisions wholly-old or wholly-new chain; identity + direct bindings stable | mixed-chain authority |
| IAM33 | Non-interference: admin(S) changes zero decisions outside S's subtree | delegated-admin escape |
| IAM34 | Unbindability: iam/node_plane bindings to agents unrepresentable | plane takeover via policy |
| IAM35 | Apply-time signatures: unsigned/foreign-signed records injected at any replica (leader included) refused + alarmed | store injection via node compromise |
| IAM36 | Blast radius (adversarial cluster-SIM): full node compromise ⇒ zero accepted store mutations, zero foreign-session reads | unbounded compromise |
| IAMS1 | One on-disk format: a run is pack-chunked content; no `.sst` format exists (architecture test) | a second store format |
| IAMS2 | Record-merge correctness: superseded + sub-floor tombstones dropped; a record spanning a chunk boundary reassembles | broken compaction |
| IAMS3 | CN2: an idle scope's LSM ticks nothing — no per-group timer | idle-cost creep |
| IAMS4 | Reachability-index budget: bounded RAM; evicts + re-derives under pressure | unbounded growth |
| IAMS5 | ReadIndex locality: session/user reads incur no cross-region RTT; below-floor reads fail typed | WAN on the decision path |
| IAMS6 | FAULTS cells complete for the three subsystems (boot-validated) | uncovered fault cell |

## §16 Acceptance criteria

1. One decision procedure — every authorization reaches §5 via a compiled artifact; no
   second evaluator survives.
2. Complete mediation at boot: unmapped action or unclassified PEP ⇒ startup failure.
3. The separation law is architectural: no IAM object in the ledger, no management op
   as a claim, work facts context-only.
4. Typed fail-closed per effect; forbid-eval errors fail the decision.
5. Ceilings always bind — the session-principal bypass unrepresentable.
6. Chains only narrow; provenance set-once; caps derived.
7. Plane-wide freshness contract, bound min(push, T); T + GC window derived at
   definition sites.
8. Store: owned LSM, one on-disk format, MVCC, per-scope; no SQL, no external DB, no
   second store format.
9. Scale laws: no global artifact, depth ≤5 walks, per-scope sharding — proven in
   cluster-SIM before any multi-node claim.
10. Laptop degenerate holds with zero modes.
11. Rank/SafetyPolicy/Biscuit/affordances consume the plane; the cannot-fold trio
    stays agentic/structural.
12. Root anchors ship versioned with the harness; user supremacy structural.
13. Every derived constant (T, chain cap, GC window, bloom FP, flush threshold, index
    budget) carries its derivation at its definition site.
14. Audit total, valueless, epoch-stamped, fail-closed per population.
15. Tamperproof T1–T8 with the scoped FAULTS amendment; blast radius quantified.
16. FAULTS cells complete for the store, compaction, and reachability index.

## §17 Companion amendments (this change)

CONSENSUS §3 (region-local ReadIndex note) + a deferred splittable-keyspace/
cross-region-reparent rider; OBJECT_TIER §7 (IAM manifests in the root list); FAULTS
§1/§3 (scoped authority-plane adversary) + §5 (the three subsystem cells); PODS §6 +
CONTEXT.md Warden (compile-source = residual); CONTEXT.md Affordance (authorization
axis), SafetyPolicy (compiled-ceiling); LEDGER.md (standing-vs-work-order split;
ledger serving edge); RANK.md (pack = compiled projection); SECRETS §4/§7 (grant
authority in IAM, index keeps projections — when SECRETS.md is written); REGISTRY
(Scope as projection of IAM scope nodes; AgentRole↔Role split); FOREST/VECTOR_INDEX
(governed scope-lift + emission-surface registration + item provenance stamps). The
splittable-keyspace/cross-region-reparent rider is genuine new consensus machinery,
flagged and deferred to its own exchange (GAPS.md).

**Amendment (2026-08-22, TRACING/MONITORING acceptance):** this subsystem's
chokepoints emit execution spans per `TRACING.md` §3; its chokepoint registry
entries are the span roster (boot-validated; an unregistered emitter fails
startup).
