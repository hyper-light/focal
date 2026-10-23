# Vendored crates

The crates in this directory are built into every focal binary in place of their
crates.io copies (`[patch.crates-io]` in the workspace `Cargo.toml`). Each is the
crates.io archive of the named version, unpacked byte for byte and unchanged;
the archive's SHA-256 was checked against the checksum crates.io publishes for
that version before it was unpacked. Keeping the sources here means a release is
built from what was reviewed, can be built without the registry, and can carry
an upstream fix before a release of the crate does, with the difference visible
in this tree.

| Crate | Version | crates.io archive SHA-256 | Taken | Why it is here |
|---|---|---|---|---|
| `aws-lc-rs` | 1.18.1 | `b281d307588d634de920874890732659e2e7672f72b5e10e81badc1a8a83621e` | 2026-09-29 | The cryptographic provider behind rustls, quinn and rcgen, and the ECDSA verification of enrollment statements ([07](../docs/archictecutre/07-decisions-and-traceability.md)) |
| `aws-lc-sys` | 0.45.0 | `9bff6c3b54fad79a2e60b8102caf565819711497c1f5f092f49508e2f5c31b27` | 2026-09-29 | AWS-LC 5.7.0 itself (C and assembly, with the pre-generated bindings for every target focal ships), built by `aws-lc-rs` |

## Shared crates

The `hyper-*` crates are not crates.io archives. They come from the repository focal shares with
mantle and slates (<https://github.com/hyper-light/hyper-raft>), taken from it whole at one
revision, which `SNAPSHOT` in each names (since 2026-10-23 all sixteen at c8c3cb6 of the shared
repository, whose hyper-quic learns a reordering path's loss thresholds from its spurious losses, its
packet threshold their farthest distance and its time threshold at least the longest one took, fires
a probe timeout a loss time held back in the wake that ends it, keeps a connection ID its peer retired
routing for three probe timeouts, and credits its pacer the same bytes however often it is asked; since
2026-10-22 at d4e8447, whose stream pool parks an exchange that waits on its owner's event, holding none of the
pool's credit; since 0e89f97, where its two lines met: main's hyper-liveness judge whose own flush proof stalls blames
no peer for it, hyper-raft's leader hand-over rules, every dial ending in an event its owner is told,
GroupStore's page read, the kernel's receive stamps in its own ticks and the transport on the
simulation runtime, with fate-commit's work; and a driver's receive ring holding what its drains
needed, from one buffer up, its growth charged as it grows; since 64acd86, which charges a
connection's state what it holds where its endpoint runs, read from a heap of its own (the new
hyper-heap), grows an identity's admission list with what it holds,
answers an unproven handshake with a Retry once the handshakes hold what their owner allows them,
and gives a certificate's Brotli compression the least window holding it; since 1af65f7, which
wipes a sealed file's plaintext from its writer's and reader's buffer when they are let go, and
since 8b36a88 sums CRC-32C with crc-fast, writes a frame on an idle lane
straight into its stream, finds a stream's unacknowledged data by binary search, searches each
path's MTU up to the path's MTU as its kernel holds it, charges a socket's room for what a peer can
send unprompted at any MTU (a probe timeout after an unanswered one repeats no stream data),
reserves for the admission places handshakes hold, shares the port of a socket handed to a driver
bound with its port shared, sets back a window its peer stopped using when another wants its room,
keeps what a departed peer's socket did in the driver's counts, parks the driver at the cost of what
fired (a readiness group in hyper-rt), not of every peer socket held, binds a shared port before
sharing it, so a port-0 bind is never given a port another shared socket holds, sends a probe of the
path's MTU only as the congestion window and the pacer afford it, refuses a stream as unread only
when it has written nothing for a window, gives a request smaller than a datagram, acknowledged
whole, its peer's period to answer, and commits on entries held in memory across the fates an owner
states (core step R-8: a follower's answer under its durable promise, a leader counting its own disk,
a restarted member's mark narrowed by its leader, and what may leave or be applied given without a
write), and makes a write refused for room again with what the refused writes held and nothing past
it, so a compaction never passes an entry it writes again, and judges no peer's flush-proved
heartbeat late while its own log has made nothing durable since it took the heartbeat before
(Lifeguard's local health against the flush proof), and takes what a compaction's start lets go of
out of the compaction rule's count by what it counted of each entry as it applied, reading nothing
back from the log, and queues an endpoint's own responses as they come, never laid out at their
bound, so an owner states what one receive hands the endpoint and a burst of dials is answered,
and lets no ClientHello its client sent again under the ID a retry answered, nor a copy of an
attempt it refused, hold a handshake place, and finds whether a log segment is live by a binary
search of the live slots, and has btrfs write a log file opened empty in place rather than copy it
on write, and forgets a freed segment's sealing sessions from the log's tail after each frame
rather than asking of every session, and lets an owner restate its quiet period as it moves, and counts a full log's sweeps, and makes a frame's confirmation durable the cheaper of a FUA write and a write and flush as the device measured them, and counts the zeros it writes over the slots its file grows by, and writes them only where its file system journals its allocation in place (ext2 to ext4, XFS), and sends a probe of the path's MTU that the window and the pacer can carry before new data, so a connection that keeps sending still searches, and writes those zeros ahead of a slot's frames, a piece no larger than a frame at a time, only while its device has nothing else to do, a frame that opens a slot writing none, and charges what a connection holds beside its windows (its state and the exchange tables its streams size) as it is held, its endpoint's tables growing with what it holds; the event bus's transport, merged at a8f5941 with batched sends and
receives, the measured receive room, precise timed waits on Linux, macOS and Windows, one CRC pass
a frame, the sent-packet deque and decimated acknowledgements, and since 76eaa33 a socket's
account read at an epoch's doublings, a held lane's frame waiting for its owner and an idle turn's
ask left to its park):
- focal's consensus core and its election law, which moved there
  ([27](../docs/archictecutre/27-consensus-roadmap-and-slates-port.md) §14);
- the durable shell around the core, with the log, the block layer and the liveness stream it
  builds on (27 §15). Each is that revision's `crates/<name>/src`, `ORIGIN.md` (and `README.md`) and the
repository's `LICENSE`, unchanged, with one file of focal's: a `Cargo.toml` of its own that states
what the shared repository's workspace gave it (version, edition, dependencies) and makes it a
workspace root of its own, outside focal's, and that sets `rust-version` to focal's toolchain, which
the snapshot builds on. Its suites, benchmarks and lint wall run in the shared repository on all six
targets; a change to it is made there first and taken here by a new snapshot, never edited here.

| Crate | Version | Revision | Taken | Why it is here |
|---|---|---|---|---|
| `hyper-block` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | Block I/O for the log: aligned direct I/O, each platform's full flush, group commit's wait |
| `hyper-datagram` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | The sealed datagram plane: consensus and membership messages on a UDP socket of their own beside QUIC, so a vote never waits behind a transfer, keyed from each connection's TLS exporter |
| `hyper-durable` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | The durable shell a group's replica becomes (27 §15) |
| `hyper-heap` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | A node's endpoints run on heaps of their own (mimalloc's first-class heaps), whose bytes in use their endpoints observe as what their connections hold (hyper-raft docs/transport.md §4x) |
| `hyper-liveness` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | The node-pair liveness stream, which the shell's owner wires once focal elects by suspicion |
| `hyper-log` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | The log every group of a data directory writes, one flush for all of them (27 §15.3) |
| `hyper-measure` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | Measurement only, a dev-dependency of the event bus's benchmarks: the counting allocator, the OS's page-fault, CPU and footprint accounts, the open-loop schedule and quantile intervals |
| `hyper-quic` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | QUIC (quinn-proto conformed: configuration owned, not shared, no panics), every node's and client's transport |
| `hyper-raft` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | The consensus core every group runs |
| `hyper-rt` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | The thread-per-core runtime focal runs on: shards, timers, sockets, signals, the blocking pool (it replaced tokio) |
| `hyper-rt-transport` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | hyper-transport's endpoint and hyper-datagram's plane driven on a hyper-rt shard, over its batched UDP socket |
| `hyper-seal` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | Sealing at rest: the log's per-session keys, MACs and tags (hyper-log's sealed log), whole files sealed by STREAM (`sealed_file`), keys wrapped by AES-256-KW and sent to another machine by ML-KEM-1024 |
| `hyper-sim` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | The deterministic world (seeded streams, virtual time, the simulated block device) hyper-block's `sim` feature builds on, and the bus's simulations will |
| `hyper-timing` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | The election law's draw the core takes its delays from |
| `hyper-tls` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | TLS for QUIC and TCP (rustls conformed: configuration owned and lent per call, hybrid post-quantum groups between nodes) |
| `hyper-transport` | 0.1.0 | `c8c3cb67661fdec2d3b3b705997d002b653f18ea` | 2026-10-23 | Exchanges over QUIC: admission by identity, lanes, credit and the peer's measured timing (it replaced quinn); a lane's `LaneRoom` event, which focal's replication lanes wait on |

A new snapshot: `git archive <revision> crates/<name>/src crates/<name>/ORIGIN.md
crates/<name>/README.md LICENSE` from the shared repository into `vendor/<name>`, `SNAPSHOT` set to
`<name> <revision> crates/<name>`, the manifest kept, and the table above, `Cargo.lock` and
`docs/dependencies/inventory.tsv` (the revision is the source it lists) brought up to date.

These directories hold third-party code under their own licenses (`LICENSE`
in each); the release notices and SBOM list them from `Cargo.lock` like any
other dependency, with the archive checksum above as their provenance
(`scripts/release/notices.py`). They are not workspace members: focal's lints
and the production panic policy apply to `crates/` and `tools/`, and these
crates keep their own.

## Refreshing a vendored crate

1. Download the new version's archive and verify it:
   `curl -sL https://static.crates.io/crates/<name>/<name>-<version>.crate -o <name>-<version>.crate`,
   then compare `shasum -a 256` with the `checksum` field of
   `https://crates.io/api/v1/crates/<name>/<version>`.
2. Replace the directory with the unpacked archive (`tar xzf`, then move
   `<name>-<version>` to `vendor/<name>`; drop `.cargo-ok` if present). Do not
   edit the sources; a change focal needs before upstream releases it is a
   separate, documented patch commit on top of the verbatim import.
3. Update the table above, `Cargo.lock`, `docs/dependencies/inventory.tsv`
   (`python3 scripts/release/notices.py <scratch directory>`, which refuses a stale roster), and record the reason in
   `docs/archictecutre/09-implementation-status.md`.
4. Run the six gates; the dependency audit (`cargo deny`) covers the vendored
   crates' licenses through `Cargo.lock`.
