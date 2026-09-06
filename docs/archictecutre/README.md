# Focal architecture and implementation plan

Status: architecture baseline with implementation in progress, 2026-09-05. This directory uses the path requested for this project: `docs/archictecutre`.

Focal is a Rust protocol and claims-ledger platform. Its target is one architecture that works on a single laptop and across multiple regions at Meta scale: custom RAM-resident working state, disk-backed durable logs, sharded state and execution, and independent session ordering. The latter scale is an objective to qualify through measurement, not a demonstrated property of either reference repository.

The user confirmed that RAM is primary and disk provides durability, analogous to Kafka's durable log. This does not require Kafka, Kafka's storage engine, or Kafka's consistency semantics. Focal owns its memory store and log storage adapter.

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
| [Hecate source snapshot](reference/README.md) | Imported architecture and supporting specs, original provenance and hashes |

## Authority and current status

The user's requirements take precedence. The numbered Focal documents are a proposed, mutually consistent implementation contract derived from the references. Explicit Focal decisions resolve conflicting upstream passages; importing an upstream `ACCEPTED` label does not make it implemented or automatically accepted for Focal. The frozen reference files retain their original text, including superseded passages.

Hecate is a design repository. Sylk is a Go implementation reference. Focal started with only a README, license, and gitignore. The Rust implementation is now underway; [09](09-implementation-status.md) records concrete components and remaining work. The architecture's commands, interfaces, test matrices, milestones and benchmarks remain acceptance targets unless verified in that implementation record.

The implementation plan is executable without inventing a distributed consistency model. Deployment-specific workload sizes, capacity budgets, geographic placement, and recovery objectives remain measured inputs. Each has a named qualification step rather than an invented universal number.

## First executable slice

Complete P00–P04 in the [implementation plan](05-implementation-plan.md) for a durable laptop ledger: establish the Rust workspace and model; implement the deterministic serial ledger; persist its log; restart and recover. P05 adds the complete generated → posted → received → testament → validation → satisfied demonstration with immutable evidence. Preserve the final command and read interfaces so replication and partitioning extend this slice.

The production objective requires all distributed, multi-region, memory-bound, security, operational, and stepped-complexity gates through P16. The user's added CLI, skills, MCP, challenge and consultation scope extends the active implementation goal through P20 in [13](13-cli-and-agent-implementation-plan.md). All list filters are optional, and flags/JSON/YAML must compile to the same typed requests. A working laptop demo or command parser is an intermediate milestone.

The [network startup guide](../network-startup.md) documents the implemented founder, invitation, join, and restart commands. Joined nodes currently replicate root metadata as learners; application placement and stronger durability remain required work.

The [manual CLI guide](../manual-cli.md) documents the implemented local submit/get/list commands, validation results, artifact downloads and durable operation recovery. The CLI uses shared typed Rust builders and the service's authenticated mutation/read boundary.

The [MCP guide and agent skills](../mcp.md) document `focal mcp serve`, its released operation catalog and exact retry contract. Remote contexts, autonomous challenge/consult policy and full deployment journeys remain required.
