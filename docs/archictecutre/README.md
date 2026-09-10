# Focal architecture and implementation plan

Status: architecture baseline with implementation in progress, 2026-09-05. This directory uses the path requested for this project: `docs/archictecutre`.

Focal is an inter-agent, single-node or massively distributed communication protocol and event-driven ledger that facilitates robust, predictable, efficient, and scalable coordination and communication between swarms of agents. It is implemented in Rust. Its target is one architecture that works on a single laptop and across multiple regions at Meta scale: custom RAM-resident working state, disk-backed durable logs, sharded ledger execution, and independent session ordering. The latter scale is an objective to qualify through measurement, not a demonstrated property of either reference repository.

The user confirmed that RAM is primary and disk provides durability, analogous to Kafka's durable log. This does not require Kafka, Kafka's storage engine, or Kafka's consistency semantics. Focal owns its memory store and log storage adapter.

The user's 2026-09-06 clarification makes execution participant-owned: Focal does
not launch agents or validation workers. Issuers invoke tools, skills or code in
their own environments and record authorized evidence and results. Claims,
testaments, artifacts and validations have separate coordinated lifecycles.
[16](16-peer-validation-contract.md) records this correction, the current model
gaps and the required migration; it supersedes earlier wording that implied daemon
workflow execution or one shared object lifecycle.
[17](17-lifecycle-state-and-authority.md) fixes the target transition/authority
tables and source conflicts; [18](18-lifecycle-storage-upgrade.md) defines the
storage-upgrade prerequisites. Written contracts do not imply those new lifecycle
states or mutations are already implemented.

## Read in this order

| Document | What it settles |
|---|---|
| [00 — Target architecture](00-target-architecture.md) | Scope, authority, topology, consistency, failure domains, scale assumptions |
| [01 — Source audit](01-source-audit.md) | Hecate's design versus Sylk's implemented behavior; evidence and port hazards |
| [02 — Domain and lifecycle](02-domain-and-lifecycle.md) | Claims, testaments, validations, artifacts, commands, transitions, satisfaction |
| [03 — Rust workspace and interfaces](03-rust-workspace-and-interfaces.md) | Planned crates, ownership, types, request contracts, dependency choices |
| [04 — Storage and distribution](04-storage-and-distribution.md) | RAM layout, WAL, replication, apply, sharding, snapshots, recovery, region failure |
| [05 — Implementation plan](05-implementation-plan.md) | Dependency-ordered work packages, file targets, deliverables, acceptance gates |
| [06 — Verification and operations](06-verification-and-operations.md) | Correctness suites, fault injection, scale qualification, recovery, observability |
| [07 — Decisions and traceability](07-decisions-and-traceability.md) | Source conflicts, deliberate adaptations, requirements mapped to work and tests |
| [08 — Stepped complexity and deployment](08-stepped-complexity-and-deployment.md) | Minimal configuration and concepts through laptop, VMs, Kubernetes, zones, regions, and global deployment |
| [09 — Implementation status](09-implementation-status.md) | Executable components, verification evidence, remaining acceptance work, and implementation decisions |
| [10 — Ownership and failure policy](10-ownership-and-failure-policy.md) | Enforced no-panic rules, owned state, retained concurrent sharing, and dependency boundaries |
| [11 — CLI source research](11-cli-spec-research.md) | Primary-source operation inventory, manual command surface, input and query semantics |
| [12 — Agent tools and workflows](12-agent-tools-and-workflows.md) | Skills/MCP mapping, challenge and consultation semantics, source conflicts and authority boundaries |
| [13 — CLI and agent implementation extension](13-cli-and-agent-implementation-plan.md) | Actionable P17–P20 work for shared operations, the complete CLI, MCP, skills and proof-bearing workflows |
| [14 — MCP protocol research](14-mcp-protocol-research.md) | Verified protocol revisions, bounded Rust adapter decision, compatibility, durable tool identity and conformance gates |
| [15 — Managed request streams](15-managed-request-streams.md) | Concurrent client ownership, durable ordinal allocation, safe receipt retirement, versioned operation IDs and decoder activation |
| [16 — Peer validation and independent lifecycles](16-peer-validation-contract.md) | Participant-invoked tools/skills/code, four coordinated object state machines, narrow peer evaluation APIs and migration gates |
| [17 — Lifecycle state and authority](17-lifecycle-state-and-authority.md) | Exact target transitions, writers, response alternatives, short-circuit/late-result rules, asymmetric audit evidence and historical distinctions |
| [18 — Lifecycle storage upgrade and decoder transition](18-lifecycle-storage-upgrade.md) | Existing format/reducer audit, explicit successor decoder floor and activation, immutable historical replay and migration qualification |
| [19 — CLI and MCP implementation contracts](19-cli-mcp-implementation.md) | Peer operation admission, durable retry, external validation, named authenticated contexts, and explicit remaining lifecycle and deployment work |
| [20 — Native binary distribution](20-binary-distribution.md) | One prebuilt server/client/MCP executable, native release matrix and integrity gates, Windows implementation and installation qualification |
| [21 — Native input format](21-native-input-format.md) | Complete dormant command/timer byte grammar, allocation-free structural inspection, aggregate limits and the remaining semantic/durable activation gates |
| [22 — Native recorded mutations](22-native-record-format.md) | Complete mutation encoding, structural integrity, dependency-phased restoration and remaining row decoding, recovery and WAL activation |
| [23 — Native activation and import](23-native-activation-and-import.md) | One Session with two domain engines, the persisted field matrix, the replicated activation protocol, retained deliveries, and the fixed design of legacy import |
| [26 — Custody, archive, retention and restore](26-custody-archive-retention-and-restore.md) | Custody receipts per verified copy and the obligation a validation phase reads, then archive and retirement, retention floors, garbage collection, backup and restore |
| [25 — Parallel materialization, ranges and movement](25-parallel-materialization-and-ranges.md) | Deterministic parallel materialization of committed records (staged in dependency waves, read-traced, barrier-checked, installed in order), then ranges and safe movement |
| [24 — Placement execution and fleet control](24-placement-execution-and-fleet-control.md) | Committed assignment progress, plan phases derived from signed readiness, retiring copies, bounded refusals, the measured guarantee report, and the remaining controller, agent, admission and operator batches |
| [Hecate source snapshot](reference/README.md) | Imported architecture and supporting specs, original provenance and hashes |

## Authority and current status

The user's requirements take precedence. The numbered Focal documents are a proposed, mutually consistent implementation contract derived from the references. Explicit Focal decisions resolve conflicting upstream passages; importing an upstream `ACCEPTED` label does not make it implemented or automatically accepted for Focal. The frozen reference files retain their original text, including superseded passages.

Hecate is a design repository. Sylk is a Go implementation reference. Focal started with only a README, license, and gitignore. The Rust implementation is now underway; [09](09-implementation-status.md) records concrete components and remaining work. The architecture's commands, interfaces, test matrices, milestones and benchmarks remain acceptance targets unless verified in that implementation record.

The implementation plan is executable without inventing a distributed consistency model. Deployment-specific workload sizes, capacity budgets, geographic placement, and recovery objectives remain measured inputs. Each has a named qualification step rather than an invented universal number.

## Implementation stages

P00–P05 in the [implementation plan](05-implementation-plan.md) define the original durable laptop baseline. The current V1 service persists and recovers its ledger and exposes CLI/MCP operations. The native RAM owner implements the corrected independent lifecycles and full authored bodies; its durable recovery, complete wire decoding and live activation remain the next boundary described in [18](18-lifecycle-storage-upgrade.md) and [21](21-native-input-format.md).

The production objective requires all distributed, multi-region, memory-bound, security, operational, and stepped-complexity gates through P16. The user's added CLI, skills, MCP, challenge and consultation scope extends the active implementation goal through P20 in [13](13-cli-and-agent-implementation-plan.md). All list filters are optional, and flags/JSON/YAML must compile to the same typed requests. A working laptop demo or command parser is an intermediate milestone.

The [network startup guide](../network-startup.md) documents the implemented founder, invitation, join, and restart commands. Joined nodes currently replicate root metadata as learners; application placement and stronger durability remain required work.

The [manual CLI guide](../manual-cli.md) documents the implemented local submit/get/list commands, validation results, artifact downloads and durable operation recovery. The CLI uses shared typed Rust builders and the service's authenticated mutation/read boundary.

The [MCP guide and agent skills](../mcp.md) document `focal mcp serve`, its implemented operation catalog, named local/authenticated remote contexts and exact retry contract. Native lifecycle activation, complete peer challenge/consult workflows and full deployment journeys remain required. Participants retain responsibility for executing tools and authoring any follow-up claims.
