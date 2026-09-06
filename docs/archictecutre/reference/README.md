# Frozen Hecate architecture reference

Captured 2026-09-05 from `../hecate` at commit `103c0785d2623c19d0c02a450e94677bbfc70359`. The 37 imported files match their tracked HEAD contents. Unrelated untracked local configuration was not copied. [manifest.json](hecate/manifest.json) records the source path, snapshot path, SHA-256, byte count, and whether each captured file differed from HEAD.

These are upstream source documents, not Focal implementation status. The numbered documents one directory above define the proposed Focal adaptation. Preserve this snapshot; record future imports with a new provenance manifest and explicit reconciliation.

## Primary reading

| Source | Relevance |
|---|---|
| [CONTEXT](hecate/CONTEXT.md) | Original domain glossary |
| [LEDGER](hecate/docs/architecture/LEDGER.md) | Claims, testaments, validations, artifacts, relations, lifecycle, graph, coordination |
| [LEDGER_CORE](hecate/docs/specs/LEDGER_CORE.md) | Sequencer, effective state, monitoring, replay, retirement |
| [LEDGER_SUBSTRATE](hecate/docs/specs/LEDGER_SUBSTRATE.md) | Commit, materialization, serving, and distribution composition |
| [MATERIALIZER](hecate/docs/specs/MATERIALIZER.md) | Proposed order-preserving parallel and partitioned apply |
| [STORE](hecate/docs/specs/STORE.md) | Range ownership, read visibility, materialization, sharding and recovery |
| [WAL](hecate/docs/specs/WAL.md) | Logical logs, group commit, disk durability and recovery |
| [CONSENSUS](hecate/docs/specs/CONSENSUS.md) | Failure-domain hierarchy, Raft ordering, fencing, retention |
| [ARCHIVE](hecate/docs/specs/ARCHIVE.md) | Proof custody, retirement, query continuation |
| [OBJECT_TIER](hecate/docs/specs/OBJECT_TIER.md) | Immutable evidence/checkpoint storage and garbage-collection roots |

## Supporting contracts

| Group | Imported sources | Purpose |
|---|---|---|
| Delivery and reads | [CACHE](hecate/docs/specs/CACHE.md), [FANOUT](hecate/docs/specs/FANOUT.md), [QUEUE](hecate/docs/specs/QUEUE.md), [SERVING](hecate/docs/specs/SERVING.md) | Projection coherence, credits, subscriptions and serving ownership |
| Wire | [PROTOCOL](hecate/docs/specs/PROTOCOL.md), [WIRE_FORMAT](hecate/docs/specs/WIRE_FORMAT.md), [WIRE_SECURITY](hecate/docs/specs/WIRE_SECURITY.md) | Typed contracts, serialization, transport and peer security |
| Runtime and topology | [RUNTIME](hecate/docs/specs/RUNTIME.md), [SESSIONS](hecate/docs/specs/SESSIONS.md), [TRANSFER](hecate/docs/specs/TRANSFER.md), [AUTOSCALING](hecate/docs/specs/AUTOSCALING.md) | Ownership, session sharding, movement and capacity adjustment |
| Identity and policy | [RANK](hecate/docs/specs/RANK.md), [IAM](hecate/docs/specs/IAM.md), [REGISTRY](hecate/docs/specs/REGISTRY.md) | Participant identity, evaluator authority, admission inputs |
| Runtime integration | [AGENTS_RUNTIME](hecate/docs/specs/AGENTS_RUNTIME.md), [SKILLS_API](hecate/docs/specs/SKILLS_API.md) | Parking, execution, accumulation, host interfaces |
| Verification and operations | [FAULTS](hecate/docs/specs/FAULTS.md), [HEALTH](hecate/docs/specs/HEALTH.md), [TRACING](hecate/docs/specs/TRACING.md), [GAPS](hecate/docs/GAPS.md) | Failure model, acceptance obligations, operational contracts |
| Architecture context | [PLATFORM](hecate/docs/architecture/PLATFORM.md), [AGENT_MODEL](hecate/docs/architecture/AGENT_MODEL.md), [SKILLS](hecate/docs/architecture/SKILLS.md), [SUMMONING](hecate/docs/architecture/SUMMONING.md) | Why agent/service work and policy use claims; host integration context |
| Decisions | [Protocol](hecate/docs/adr/0002-custom-dual-stack-claims-protocol.md), [Streaming gate](hecate/docs/adr/0003-streaming-merge-gate.md), [Client/runtime seam](hecate/docs/adr/0004-single-binary-with-client-runtime-seam.md) | Historical rationale and interface context |

`docs/architecture/AGENTS.md` was copied byte-for-byte as `AGENT_MODEL.md` so an architecture description is not installed as repository-agent instructions. The manifest records the rename. All other relative source paths are preserved.

The source documents cite the wider Hecate platform, research, and files outside this selected snapshot. Those historical references remain unchanged; this is a curated import, not a self-contained copy of the whole Hecate repository. Focal's authored documents link directly to the imported files they rely on. Unimported VFS/merge/scheduler/collector details are integration context outside the initial ledger implementation.

Sylk is examined in [the source audit](../01-source-audit.md), at commit `50154e6159c7ed590728b82423dde3e7fc977c26`. Its source code is not vendored. Sibling-repository links in that audit require the original workspace layout.
