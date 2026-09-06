# Summoning, Pods, and Isolation

How work comes into existence in Hecate: the Guide summons; the Guardian admits; the
harness allocates microVM pods, volumes, permissions, and network; agents run inside a
hard boundary and reach everything — ledger, work volumes, tools, providers — over the pod
network.

Terminology follows `../../CONTEXT.md`.

## 1. The summon

A summon is a **claim**: an orchestrating agent (the Guide; the autoscaler as a
system participant) requests a workload; **the scheduler executes it** and
testifies the allocation; the issuer monitors and evaluates. In the spirit of
creating a Kubernetes Deployment:

1. **Compose** — the Guide chooses the agent set for the intent (an architect pod for a
   design conversation; inspector + engineer for a bug fix; a lone engineer for simple
   work; several engineers for parallelizable work), informed where useful by consulting
   the agents themselves.
2. **Admit** — the Guardian soft-gates the summon (§7).
3. **Allocate** — pods (one agent per pod, possibly many pods per summon), VFS volumes,
   permissions, and network endpoints/policies.
4. **Assign** — each agent binds to its pod with its role's guest image, skill set, and
   model configuration, resolved and pinned at summon time. Configuration changes reach
   live work only via handoff — never by mutating a running pod.
5. **Validate** — health and correctness: the pod boots, the agent is reachable, its
   mounts and network policy are live. A summon that cannot validate fails loudly.

Summons persist as **claims** on the ledger — the summon is directed work with
validations (Guardian admission among them), its testament records what was allocated,
and teardown is its terminal lifecycle. Everything else about a summon (resource
telemetry, scheduling detail) is logs, not ledger.

## 2. The isolation stack

**A pod is a microVM running one agent.** The VM boundary is the isolation guarantee —
hardware virtualization, a guest kernel, and a device surface the host polices. This is
the hard boundary Sylk's process-level discipline clumsily approximated.

Decision (ADR-0001): Hecate builds on the **libkrun family, forked and owned in-tree**:

- **Linux**: KVM backend (upstream libkrun).
- **macOS**: Hypervisor.framework backend (upstream libkrun; Apple Silicon).
- **Windows**: Windows Hypervisor Platform backend, adopted from the 2026 WHP lineage
  (A3S Box / SmolVM) and co-maintained by us.

We fork and maintain all of it. We do not fear the complexity, and where we can improve
the stack we do. libkrun is a *library* VMM, which is what makes the single-binary
product shape possible: `hecate` embeds the runtime; there is no daemon requirement.

**The boundary contract is VMM-agnostic** so any leg can be swapped for a native
platform VM if a backend stalls:

- Linux guest, one dual-arch image family (x86_64 + aarch64).
- **virtio-fs** for work-volume and projection mounts.
- **vsock** for control (agent runtime ↔ harness).
- **All network egress terminates in a host user-space network stack**
  (gvproxy/TSI-style). There is no guest path to the network that does not traverse a
  host process — this is where the Guardian's network hard-block physically lives.

The **full agent loop runs in-guest** — LLM client included. All egress (provider APIs,
ledger protocol, MCP) crosses the virtual network where the Guardian polices it; no
host-side agent code exists that could bypass the boundary.

OS-level sandboxes (bubblewrap / Seatbelt / AppContainer) exist only as an explicitly
labeled **degraded mode**, never the default boundary, and a run under them is marked
non-hermetic to its consumers.

## 3. Guest images

- One minimal Linux image family, dual-arch, with Hyper-V enlightenments compiled in
  (harmless elsewhere) so the identical image boots on all three hosts.
- **Role-specific images**: the Engineer's carries toolchains, test runners, and LSPs;
  the Inspector's carries analyzers only; the Archivalist's carries debuggers,
  profilers, and tracing tools; the Designer's carries media tooling; the TS/Python
  skill runtimes ship in the images of agents whose skills need them.
- Images are content-addressed and pinned into the summon. "Did this agent's world
  change" is a hash comparison.

## 4. Networking

Entirely network-driven inter-agent and inter-pod communication. Two protocols ride the
virtual network (ADR-0002, `LEDGER.md` §7):

- **The claims plane** — ledger operations, delta streaming, summon control, health —
  speaks Hecate's own ground-up dual-stack UDP/TCP protocol.
- **The tool plane** — skills and tools, internal and external — speaks MCP
  (`SKILLS.md`).

Per-pod network identity and policy are allocated at summon time. Default posture is
deny: a pod reaches the harness endpoints it was granted and nothing else; external
egress is a Guardian-policed grant. The Guardian may sever any flow mid-stream; policy
revocation tears down affected parked turns with a reason (signals carry reasons —
`LEDGER.md` §5).

## 5. VFS volumes and the guest mount

The host-side VFS remains the single write authority; guests see **views**.

- Each pod's volume projects into the guest as a **POSIX mount** (virtio-fs), served
  host-side from the pod's overlay. Toolchains just work: compilers, test runners, and
  linters operate on ordinary files.
- Writes land in the overlay **server-side**, where budgets, capture, and Guardian
  policy live. The guest never holds work-volume bytes the host didn't serve it.
- The serving cut is the path-based, stateless namespace interface Sylk already proved
  (Stat / ListDir / ReadFile / Write / handle layer) — now served over virtio-fs instead
  of FUSE-in-process.
- The guest mount **deletes Sylk's compensation machinery**: shebang rewriting at
  extraction time, dynamic-linker interposers, and heuristic argv/env/shell path
  translation existed only because processes saw host paths. In-guest, the canonical
  paths are simply real.
- VFS is in-RAM with **no disk spill**, bounded by derived budgets (§8). Layers: disk
  (committed baseline) → green (increment-validated, `LEDGER.md` §6) → per-pod overlays.
  The Designer's volumes sit outside this versioned chain entirely.

## 6. The tool substrate

Hecate adopts Sylk's Tool VFS design — the part that was designed right and never wired
— as the way external tooling (packages, toolchains, libraries) exists:

- **Content-addressed store**: BLAKE3-keyed blobs, manifests as path→blob indexes,
  install-once/share-many. Recipes are content; the runtime is universal — the substrate
  knows how to fetch, verify, extract, build, sandbox, project, cache, and dedupe, and
  does not know what any artifact is for. Adding an ecosystem is writing recipes and a
  resolver adapter, not teaching the runtime a language.
- **Deterministic builds**: recipe identity is a content hash; recipe → manifest is a
  function under the determinism profile; hermetic build sandboxes are deny-first (no
  network, clamped env), keyed by target rather than host.
- **Lockfile**: per-session, WAL-durable, append-only witnesses per pod; no silent
  upgrades; side-by-side versions are nearly free under dedup.
- **Projection**: a pod's resolved tool set composes into its guest mount alongside the
  work-volume view; substrate paths are immutable (a "write" to content-addressed bytes is
  meaningless and is refused).
- **The three Guardian gates**, now real code, not doc prose:
  1. **Provision gate** — before any bytes are fetched: resolved package, source,
     hashes, license, full transitive closure, provenance/attestations. The Guardian
     inventories the closure, never trusts the author's manifest.
  2. **Sandbox capability gate** — every sandbox summon: proposed mounts, network policy,
     resource caps, purpose; verdicts include APPROVED_WITH_CAVEATS (capability
     downgrade).
  3. **Disk fallback gate** — consented, hash-pinned host-tool use, permanently tagged,
     never promotable to substrate-canonical.
- Supply-chain violations are loud: hash mismatch → refusal plus a durable violation
  event. Verification happens on every fetch, not first fetch.

Sylk's implementation lessons are binding here (`PLATFORM.md` §8): no half-wired gates,
no advisory caches nobody consults, no "registered but returns not-implemented"
extractors. A capability ships wired or it does not ship.

## 7. Guardian admission

The summon soft gate, in the spirit of a Kubernetes admission controller:

- **Known agent type** — the type must be registry-known (shipped, or Guardian-staged custom); a summon naming an unregistered type is refused
  structurally.
- **Resource sanity** — pod count, volume budgets, and rate are checked against derived
  ceilings; a malicious or runaway summoner cannot DoS the system by spinning up pods.
- **Capability tier** — read-only/advisory pods admit freely; write- or exec-capable
  pods carry a Guardian validation on the summon claim, and the Guardian's judgment
  (per SafetyPolicy) decides whether the user is also asked.
- Soft-gate rules are bounded and declared: the Guardian may deny within them or request
  more evidence a bounded number of times; it cannot stall a summon indefinitely.

After admission, the Scribes watch: each agent's Scribe reports to the Guardian, which
analyzes conduct continuously. Skill and tool invocation gating (per-call, risk-based,
auto-approvable) is the Guardian's ongoing authority, distinct from admission.

## 8. Budgets

Every ceiling is **derived from physical anchors** — system memory, volume allocation,
pod count — never a literal constant. Volume budgets derive from summon-time allocation;
the Designer's disk-overflow threshold derives from its volume budget; provisioning
concurrency derives from substrate-global caps. Exhaustion produces durable, typed
signals (budget_exhausted is a counted outcome, not an error string), and the Guardian
is the primary consumer of pressure telemetry.

## 9. Health validation and lifecycle

- Summon-time: boot, reachability, mount and network liveness — validated before the
  summon claim's admission validations can pass.
- Runtime: the health plane (`PLATFORM.md` §5) feeds each agent's Scribe; handoffs
  (context: unilateral scribe-initiated; performance: Guardian-approved) replace agent
  instances without losing work — parked turns and claims survive by construction
  because they are ledger state, not process state.
- Teardown: never before the work is committed — pipeline lifecycle is independent of
  disk commit, and rejection/correction are not terminal for a pod's volumes.
  Suspend/resume captures durable state only; a resumed pod re-resolves secrets and
  nothing else.

## 10. Topology

**Single binary** (ADR-0004): `hecate` is the terminal client and the runtime in one
process — libkrun-as-library makes this real, not a daemon hiding in a trench coat.

Internally the client and runtime keep a **seam** that speaks the real wire protocol
over three bindings — in-memory (default), unix socket, remote — carrying the same
frames, delta streams, and cursors in all three (ADR-0004). Sylk's hardest retrofit —
a bus welded inside an agent package, a UI holding 25 live pointers into the runtime —
is the cost of not having this seam; Hecate pays the near-zero price up front.

## 11. Local is distributed, degenerate

Hecate is a high-velocity harness that runs local-same-as-distributed. The rules that
make that true rather than aspirational:

- **One machinery, degenerate locally.** The seam's in-memory binding, the ledger's
  single-replica consensus group (`LEDGER.md` §8), and single-node placement are the
  *same code* as their distributed forms with smaller parameters. No local-only
  shortcuts, no distributed-only modules bolted on later.
- **Colocation units.** The session truth plane — ledger home, merge proposer,
  green-head authority, disk flusher — is one colocation unit, placed whole on one
  node **for locality, not for existence**: its logs replicate across the session
  group, warm applier instances run at every group member, green versions are
  placed to the group before they are referenced, and failover is promotion, not
  reconstruction (MERGE §§2/5, 2026-08-18). Each pod colocates
  with its own volume server. Pods otherwise place anywhere: cross-pod interaction is
  already exclusively network protocol (claims plane + MCP), and the streaming merge
  gate is distribution-native by construction — increments travel as ledger artifacts
  regardless of which node produced them. Summoning gains a placement step that is
  trivial on one node and real on many; the summon contract does not change.
- **Identity and credentials.** Every message carries a composite principal
  (`user/session/agent/pod`) — the on-behalf-of chain lives in the identity, stamped
  where the turn is minted. Agents never hold user credentials: pods receive scoped,
  short-lived tokens minted at the runtime boundary, locally and remotely alike.
  Provider gateways sit behind the same host chokepoint in both modes, so token
  accounting sees every request either way.
- **Velocity is a requirement, not a hope.** MicroVM summons must not cost seconds:
  warm pod pools per role, VM snapshot/restore for near-instant resume, and
  content-addressed image caching keep summon-to-ready inside a derived budget that is
  measured continuously — a summon that misses its budget is a health-plane signal,
  not an accepted cost. Failing a budget locally is the same defect as failing it on a
  cluster.
