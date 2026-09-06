# Skills

How capability is defined, validated, published, and invoked in Hecate. A skill is a
**typed, code-defined capability** — never raw markdown at the source — published over
MCP. Agents hold a small handful of well-worded skills each, backed by a wide capability
surface underneath.

Terminology follows `../../CONTEXT.md`.

## 1. The skill model

A skill is authored against the harness's typed API and carries, statically:

- **Identity**: name, description, version, owning role(s).
- **Tools**: one or more typed operations, each with input and output schemas.
- **Contract**: the protocol artifacts it produces and consumes (so the dispatcher can
  check claim/testament flows structurally).
- **Instructional content**: the procedural guidance an agent needs to use the skill
  well — authored as part of the typed definition, rendered out as content (see §2).
- **Declared capabilities**: what the skill's handlers may touch (network, work-volume
  views, ledger operations, budgets). This declaration is what the Guardian validates —
  inventory-from-declaration, statically, before any handler runs.

Everything about a skill that can be known without running it is a static artifact.

## 2. Publication: two layers over MCP

Decision (Q17): Hecate skills are **typed at the source, MCP on the wire** — compatible
with the Skills-over-MCP working group direction (SEP-2640) without inheriting its
"skills are untrusted markdown" posture for our own code:

- Each skill's **tools** publish as ordinary MCP tools (typed, schema'd, invocable).
- Each skill's **instructional content** publishes as a `skill://` resource per
  SEP-2640 (SKILL.md + supporting files, content-digested).
- The typed Rust definition is the single authoring surface; the MCP projection is
  generated. Internal and external tooling therefore speak one wire protocol — which
  the network-driven pod model needs anyway.

SEP-2640's security MUSTs are adopted as Guardian duties for **external** servers:
skill content is untrusted model input, origin-tagged per server, namespaced per origin
against shadowing, and approval is **content-bound** — a digest change to the resource
set revokes prior approval. Digests confirm consistency, not trustworthiness; trust is
the Guardian's call.

## 3. Built-ins

Built-in skills are **compiled Rust**, shipped in the harness and in the role guest
images. There is no dynamic native code loading — that would be the one supply-chain
hole the Guardian could not inspect its way out of.

## 4. User skills: TypeScript and Python declarations

Users extend Hecate by declaring skills in TS or Python against **generated,
schema-first SDKs**:

- The declaration (schemas, metadata, contract, declared capabilities) is a static
  artifact the **Guardian validates before load** — the same
  inventory-from-declaration discipline as the substrate's provision gate. No code runs
  to be evaluated.
- The handler body executes **in-pod**: inside the invoking agent's microVM, within its
  resource budget and network policy. A malicious skill gains nothing the agent didn't
  already have; there is no host-side interpreter path.
- The TS/Python runtimes ship in the guest images of roles whose skills need them.
- The harness enforces the typed contract at the boundary — inputs validated against
  the declared schema on the way in, outputs on the way out.

External MCP servers are the second extension path: Guardian-staged (§2), reached only
across the pod network, never granted more than their declared and approved surface.

## 5. Tool-surface discipline

The Sylk number to beat: its architect surfaced **31 tool definitions in turn one**, its
archivalist about 41. Hecate's rule:

- Each agent's default surface is a **small handful**: the shared façades plus
  its role façades. A façade is one well-worded skill with an action parameter, not ten
  sibling verbs.
- The long tail exists behind **progressive disclosure**: a search-and-activate
  mechanism over the agent's full manifest, so capability breadth costs nothing per
  turn.
- **Omission is the strongest gate.** A capability an agent must never hold (writes for
  the Inspector, code tools for the Designer, anything for the Scribe beyond narration
  and claims) is not registered in its pod at all — not hidden, absent. Rank and role
  drive registration; the model cannot invoke what does not exist in its manifest.
- Activation state must be able to shrink as well as grow (Sylk's was monotonic —
  a bug, not a feature).

## 6. The shared façade surface

Every agent gets the same core façades (behavior per `LEDGER.md`):

- **Claims**: post, query/traverse, progress, testaments, validation evaluation,
  carry-forward/recall — as a compact façade set, not a dozen verbs.
- **Peers**: consult, challenge (evidence-bearing, rank-checked), track — with
  parking/yield semantics so consulting never blocks a replica.
- **History**: query a peer Scribe's window; query the Archivalist beyond it.
- **Self**: health/diagnostic reporting.

Role façades ride on top: the Engineer's work-volume/test/toolchain skills, the
Archivalist's investigation instruments, the Guardian's gates and scanners, the
Designer's media and A/B skills — each a handful, each schema'd, each declared.
