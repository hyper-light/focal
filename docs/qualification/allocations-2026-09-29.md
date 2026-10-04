<!-- Qualification record: allocation, reallocation and page-fault audit, 2026-09-29, macOS arm64 (Apple M5 Max, 18 CPUs, 128 GiB), focal at 6b03c4a with the slates-port batch in the tree. Written by the session's subagent; file:line citations are of that tree. -->

# Focal heap-allocation audit — hot paths, counts, attribution, ranked reductions

Date 2026-09-29, tree `r10-r11-windows-ci` at 8c112da (plus the bench-only instrumentation listed in §8). macOS arm64, Rust 1.94.1, release profile (`lto = "thin"`, `codegen-units = 1`). No production code was changed; every number below is a **count** (allocations, reallocations, bytes requested, peak live bytes, page faults), because the machine was loaded (a Docker build and another agent's cargo jobs ran throughout) and counts are stable under load while wall-clock is not. Wall-clock figures quoted from `docs/qualification/performance/2026-09-12-macos-arm64.md` are used only to weigh the reductions.

Raw outputs, the audit notes per crate, the shapes and scripts are under `scratchpad/program/allocs/` (`run4-*.txt` are the final bench outputs; `run-e2e-10k.txt`, `time-*.err`, `server-*.time` the long runs; `audit-*.md` the static-audit notes with verified line numbers).

## 1. Method and instrumentation

**Counting allocator** (`crates/focal-memory/benches/support/alloc_count.rs`, bench-only, `#[path]`-included by six `allocs` benches; the same shape as the adversarial tests' allocator, extended):

- `#[global_allocator]` wrapping `System`; counts allocations, deallocations, **reallocations separately** (a realloc is a growth the caller failed to size), bytes requested, bytes moved by realloc, live and peak live bytes, and a request-size histogram (≤16, ≤64, ≤256, ≤1 KiB, ≤4 KiB, ≤64 KiB, larger).
- A process-wide **gate**: counts accrue only while a `Meter` window is open, so fixture construction (request envelopes, WAL record batches, reduce inputs) is excluded; live/peak tracking is unconditional so peaks stay consistent.
- **Sampled attribution**: every Nth counted allocation (N = 31–101 per bench) and every Mth reallocation (M = 1 everywhere, so realloc counts per site are exact) captures `std::backtrace::Backtrace::force_capture()`, renders it, drops the allocator's and the std allocation plumbing's frames, keeps the next 16 frames, and folds them into a site key (FNV over names+file:line). The table is bounded at 4,096 distinct sites (more are counted as `dropped`), sampling has a per-process budget, and everything the sampler allocates is counted as `overhead` and excluded from the totals (a thread-local re-entry flag; the site table is only touched under it; the backtrace lock and the table lock are never held together).
- Benches were built with `CARGO_PROFILE_RELEASE_DEBUG=line-tables-only` so inlined frames and `file:line` resolve (release codegen is otherwise unchanged; the artefacts are separate hashes and did not disturb the other agent's release build). Without line tables the same binaries attribute by outlined symbol only.
- `share_by_innermost` classifies each sampled site by the innermost frame that belongs to a named group (a crate path or module prefix), separately for allocation samples and reallocation samples (they are sampled at different rates), so a crate is charged for what it asked the allocator for, not for every caller above it.

**Benches** (all `harness = false`, dependency-free, same fixtures as the existing `ns/op` benches, which were left untouched so the qualification numbers stay comparable):

| bench | what is measured |
|---|---|
| `crates/focal-memory/benches/allocs.rs` | `MemoryBudget::reserve/commit/drop`, refusal, `child`, `funded_child`, `Arena::insert` |
| `crates/focal-wire/benches/allocs.rs` | `encode_payload`/`decode_payload` (0 B/256 B/4 KiB/64 KiB native frames) and one `write_frame`+`read_frame` round trip over `tokio::io::duplex` |
| `crates/focal-log/benches/allocs.rs` | `Wal::open`; `Wal::append` for batch×payload ∈ {1×64 B, 16×64 B, 256×64 B, 1×4 KiB, 16×4 KiB} (records built before the gate opens) |
| `crates/focal-core/benches/allocs.rs` | `Core::prepare` on the base state; `Core::apply` with its `prepare` outside the gate, first and second half of 2,000 claims |
| `crates/focal-raft/benches/allocs.rs` | the replicate harness (`tests/support`) for raft-rs (`Old`) and focal-raft (`New`), newest-first delivery as `benches/replicate.rs`, and `FOCAL_RAFT_FIFO=1` oldest-first; core vs harness share |
| `tools/load/benches/allocs.rs` | the `focal-load` driver path (same `focal_client::Client` over `EmbeddedTransport` against an in-process `EmbeddedNode`/`LocalHost`) with the gate around node open, each committed native Create, each linearizable claim read, and shutdown; per-crate shares |

**Process level**: `/usr/bin/time -l` on the unmodified `target/release/focal-load` (shapes 1k, 2k, 10k claims with as many reads) and on `target/release/focal start` serving CLI `submit claim` / `get claim` over its Unix socket (200 and 400 claims). "page reclaims" are minor faults, "page faults" major.

**Static audit**: eight read-only sweeps (wire, log, raft, consensus, core, ledger, node, client) with every line number re-verified; condensed in §5 and in `audit-*.md`.

Caveats: (1) sampled shares are estimates (±5% at these sample counts); realloc counts per site are exact. (2) The raft harness (`tests/support`) clones every message and committed entry it records; its share is separated out but the per-entry totals include it. (3) `tokio::io::duplex` in the wire round trip adds one `BytesMut` realloc and one lazily-created pthread mutex per op that are the pipe's, not focal's. (4) One product discrepancy found on the way: `focal schema example claim.submit` emits `evidence_schemas`, which `submit claim` rejects (`unknown field`), and with it removed `deadline` is missing; the server runs use a hand-written self-targeted handoff document instead.

## 2. Per-path counts

Columns: allocs/op, reallocs/op, bytes/op (requested by `alloc` + new sizes requested by `realloc`), peak growth over the phase (bytes), histogram of allocation sizes.

### 2.1 Memory budget (`focal-memory`) — confirmed zero-alloc

| path | ops | allocs/op | reallocs/op | bytes/op | peak growth |
|---|---|---|---|---|---|
| `reserve` + `commit` + drop, 4 KiB | 50,000 | **0.00** | 0 | 0 | 0 |
| `reserve` + drop (uncommitted) | 50,000 | **0.00** | 0 | 0 | 0 |
| refusal (over limit) | 50,000 | **0.00** | 0 | 0 | 0 |
| `child` + nested reserve/commit | 5,000 | 1.00 | 0 | 216 | 216 |
| `funded_child` + reserve/commit | 5,000 | 1.00 | 0 | 216 | 216 |
| `Arena::insert` u64 (page 128 slots) | 50,000 | 0.02 | 0 | 137.6 | 3,240,560 |

The admission path allocates nothing: a permit is a plain struct over an `Arc` refcount bump. A child budget is exactly one 216 B `Arc<Counters>` (`budget.rs:242`/`:299`). The arena costs two allocations per 128 inserts: the 7,168 B slot page and a **rebuilt page directory** (`arena.rs:309-319` creates a new `pages` Vec of `len+1` per page; over an arena's life that is 32 B × pages²/2 of transient bytes — small, but a `try_reserve_exact(1)` on the existing Vec is the same bound with no rebuild).

### 2.2 Wire codec (`focal-wire`)

| path | ops | allocs/op | reallocs/op | bytes/op | peak growth |
|---|---|---|---|---|---|
| encode native / 0 B (53 B wire) | 20,000 | 1.00 | 0 | 53 | 23 |
| decode native / 0 B | 20,000 | 0.00 | 0 | 0 | 0 |
| encode native / 256 B | 20,000 | 1.00 | 0 | 310 | 280 |
| decode native / 256 B | 20,000 | 1.00 | 0 | 256 | 226 |
| encode native / 4 KiB | 4,000 | 1.00 | 0 | 4,150 | 4,120 |
| decode native / 4 KiB | 4,000 | 1.00 | 0 | 4,096 | 4,066 |
| encode native / 64 KiB | 1,000 | 1.00 | 0 | 65,591 | 65,561 |
| decode native / 64 KiB | 1,000 | 1.00 | 0 | 65,536 | 65,506 |
| `write_frame`+`read_frame` / 0 B | 20,000 | 4.00 | 1.00 | 255 | 359 |
| `write_frame`+`read_frame` / 256 B | 20,000 | 5.00 | 1.00 | 1,282 | 1,118 |
| `write_frame`+`read_frame` / 4 KiB | 4,000 | 5.00 | 1.00 | 16,642 | 12,638 |
| `write_frame`+`read_frame` / 64 KiB | 1,000 | 5.00 | 1.00 | 262,405 | 196,960 |
| control: `block_on(async {})` | 20,000 | 0.00 | 0 | 0 | 64 |

Sites: encode is one exact buffer (`frame.rs:65` `payload_buffer` via `try_reserve_exact`, then `resize(len, 0)` zero-fill, then `postcard::to_slice`) — sized once, never grown. Decode is one allocation for the frame: `serde_core visit_seq<u8>` → `Vec::with_capacity` → then a **per-byte** `next_element` loop, because `Operation::Native { frame: Vec<u8> }` (`message.rs:181-183`) and the other 19 + 4 `Vec<u8>` fields are serialized as sequences of `u8` (no `serde_bytes` in the tree). That per-byte loop, not the copy, is why the qualification doc shows decode at 1,964 ns for 4 KiB vs 262 ns for encode (a 4 KiB memcpy is ~100 ns). The framed round trip adds the header write, the read-side exact buffer (`frame.rs:138`) and the decode; of the 5 allocations, 3 are focal's (encode buffer, read buffer, decoded frame) and 2 + the realloc are `tokio::io::duplex`'s (`BytesMut::extend_from_slice` at `tokio/io/util/mem.rs:288`; a lazily boxed pthread mutex). A frame is therefore materialized three times on a local hop (encode buffer → pipe → read buffer → decoded `Vec`).

### 2.3 Durable WAL append (`focal-log`, path A `Wal::append`)

| path | appends | allocs/append | reallocs/append | bytes/append | peak growth | per record |
|---|---|---|---|---|---|---|
| `Wal::open` (fresh directory) | 1 | 24 | 21 | 2,816 | 563 | — |
| batch=1, 64 B | 20 | 5.00 | 8.00 | 682 | 374 | 5.0 allocs, 8.0 reallocs |
| batch=16, 64 B | 20 | 20.00 | 68.00 | 4,762 | 2,654 | 1.25 / 4.25 |
| batch=256, 64 B | 20 | 260.00 | 1,028.00 | 70,042 | 39,134 | 1.02 / 4.02 |
| batch=1, 4 KiB | 20 | 5.00 | 14.00 | 16,810 | 8,438 | 5.0 / 14.0 |
| batch=16, 4 KiB | 20 | 20.00 | 164.00 | 262,810 | 131,678 | 1.25 / 10.25 |

Top sites (realloc counts exact):

1. `Wal::encode_batch` → `postcard::to_stdvec(record)` (`lib.rs:417`): one `Vec` per record grown from empty by per-byte `try_push` (`postcard ser/flavors.rs:474` → `serialize_u8`): **4 reallocs per 64 B record, 10 per 4 KiB record** (0→8→…→8192), bytes requested ≈ 2× the record. Path B (`writer.rs:620-630`, the production `SharedWal`) sizes each record exactly but still zero-fills and serializes per byte.
2. `install_fence` (`lib.rs:603-625`) per append: `to_stdvec(&Fence)` (1 alloc + 3 reallocs: the `[u8;16]` identity is pushed a byte at a time, `lib.rs:41`), `directory.join("CURRENT.tmp")` and `join("CURRENT")` (2 × `to_vec` + 2 × `extend_from_slice` realloc), i.e. **4 allocs + 5 reallocs per append**, plus open/write×3/`F_FULLFSYNC`/rename/directory `F_FULLFSYNC`.
3. Per record `self.active.write_all(&header)?; self.active.write_all(data)?;` (`lib.rs:503-504`): two `write` syscalls per record on a raw `File`, no batch buffer.

So a durable append is 3 `F_FULLFSYNC` (segment, `CURRENT.tmp`, directory), `2n + ~11` syscalls and `n + 4` allocations + `~4n + 5` reallocations; the 12–15 ms per append in the qualification table is three fsyncs, not one.

### 2.4 Domain reduce (`focal-core`, classic `Core::prepare`/`apply`, 2,000 GenerateClaims)

| path | ops | allocs/op | reallocs/op | bytes/op | peak growth |
|---|---|---|---|---|---|
| `prepare` (base state, 0 claims) | 2,000 | 34.00 | 21.00 | 17,698 | 13,446 |
| `apply`, first 1,000 (mean 500 resident) | 1,000 | **1,109.8** | 21.00 | 139,646 | 2,706,396 |
| `apply`, next 1,000 (mean 1,500 resident) | 1,000 | **3,276.3** | 21.00 | 382,905 | 2,718,852 |

The per-op allocation count grows by **2.17 allocations per resident claim** — the O(session) ceiling in the qualification doc (179 → 645 µs/op) is allocation-shaped, and it is not a map clone: 66% of apply allocations in the second half are three lines of `check_acyclic` (`graph.rs:83` `let mut stack = vec![(*start, false)]` — one Vec per resident claim; `graph.rs:108` `path.insert(id)` — one BTreeSet leaf per claim; `graph.rs:95` `done.insert(id)` — the set of all ids rebuilt), called on every generate (`reduce.rs:451`), and `propagate` (`reduce.rs:692-758`) walks all claims four more times through the overlay's seek iterator (`overlay.rs:113`+`:137`, two B-tree descents per row per pass). `prepare` repeats the same walk against the same state (`pending.rs:445-454`). The fixed part is ~34 allocs + 21 reallocs: the command hash via `postcard::to_allocvec` (`canonical.rs:322`, per-byte growth) and four `CanonicalEncoder` hashes that each grow from `Vec::new()` (`canonical.rs:29`, `:46`); the claim content is hashed twice (`reduce.rs:199` and `:392`).

### 2.5 Raft replication (`focal-raft` vs raft-rs, per entry committed by every member)

Newest-first delivery (as `benches/replicate.rs`) — includes the harness:

| group | raft-rs allocs/entry | focal-raft allocs/entry | bytes/entry (focal) |
|---|---|---|---|
| 3 members, 1 at a time, 64 B | 112.2 | 122.2 | 57,281 |
| 3 members, 16 at a time, 64 B | 64.4 | 66.9 | 24,880 |
| 3 members, 1 at a time, 4 KiB | 112.2 | 122.2 | 178,241 |
| 5 members, 1 at a time, 64 B | 203.2 | 220.2 | 109,034 |
| 5 members, 16 at a time, 1 KiB | 112.9 | 115.9 | 99,094 |

Oldest-first delivery (`FOCAL_RAFT_FIFO=1`, the order an in-order transport gives):

| group | raft-rs | focal-raft | bytes/entry (focal) |
|---|---|---|---|
| 3 members, 1 at a time, 64 B | 88.2 | 97.2 | 39,090 |
| 3 members, 16 at a time, 64 B | 87.2 | 96.2 | 38,743 |
| 3 members, 1 at a time, 4 KiB | 88.2 | 97.2 | 151,986 |
| 5 members, 1 at a time, 64 B | 154.2 | 169.2 | 71,499 |
| 5 members, 16 at a time, 1 KiB | 153.2 | 168.2 | 117,001 |

Two findings about the bench itself: (a) newest-first delivery reorders a follower's messages, so it sees the commit-only append before the entry-carrying one, rejects it, and the leader re-probes and re-sends from storage (`raft.rs:1566-1572`, `progress.rs:91-95` frees the inflights ring, `log.rs:537-540` re-reads) — the existing `replicate.rs` measures that path, which is why 1-at-a-time costs +24 allocs/entry over in-order and why 16-at-a-time looks cheaper than 1-at-a-time under LIFO but not under FIFO; (b) the harness records every message and committed payload by clone (`cluster.rs:202`, `:176-186`, `mod.rs:887`, `:891`, `:744`) and keeps `chosen` for the whole timed run, so most of the per-entry allocations are its own. Innermost-frame attribution (judged by source path; `run5-raft-*.txt`): under in-order delivery focal-raft's core performs **≈26 allocations per entry per 3-member group** (26.0 at 1 and at 16 per round, 26.3 at 4 KiB) against raft-rs's ≈19 (19.0/19.0/19.1); the harness accounts for ≈70 in both. Under newest-first delivery the re-probe path raises focal-raft to 31 (raft-rs 24) at 1 per round and lowers both at 16 per round (18 vs 15) because a rejected batch is re-sent from storage as one message. The static count (§5.3, ~32 core allocations and 11 payload copies per entry for N=3 against one payload allocation by the application) is the upper bound of this; the difference to raft-rs is the staging copy into the unstable log, the `Ready.entries` copy and the `msgs` regrowth (R7).

Core sites (focal-raft, in-order): `push` at `raft.rs:362` (`msgs.try_reserve(1)` regrowth from capacity 0 after every `mem::take`, node.rs:391), `Store::entries` → `slice` at `log.rs:540` (committed entries re-read from storage into a fresh Vec per Ready, `node.rs:380`), `copy_entries` at `log.rs:124-125` (staging copy into the unstable log though the caller owns the batch, `raft.rs:970`), `node.rs:448` (`Ready.entries` copied, unstable originals dropped at `log.rs:394`), and `log.rs:548` (one payload copy per follower per `MsgAppend`).

### 2.6 End to end (`focal-load` path: `Client::request` → `EmbeddedTransport` → `LocalHost` → session → consensus → WAL → native engine)

1,000 claims + 1,000 linearizable reads (`run4-e2e-1k.txt`; the two earlier 1k runs, seeds 21 and 31, agree to ±0.2 allocs/op):

| phase | ops | allocs/op | reallocs/op | bytes/op | peak growth | live growth |
|---|---|---|---|---|---|---|
| open: activate + `EmbeddedNode::open` + `LocalHost::spawn` + `Client::new` | 1 | 807 | 266 | 550,170 | 141,319 | 137,678 |
| **claim**: native Create committed | 1,000 | **356.9** | **12.0** | **574,068** | 9,871,480 | 9,439,584 |
| **read**: NativeRead claim, linearizable | 1,000 | **27.0** | **1.0** | 10,955 | 8,131 | 0 |
| shutdown: drop client + host, `owner.join` | 1 | 10 | 0 | 640 | 104 | −9,544,031 |

10,000 claims + 10,000 reads (`run-e2e-10k.txt`):

| phase | ops | allocs/op | reallocs/op | bytes/op | peak growth |
|---|---|---|---|---|---|
| claim | 10,000 | **376.2** | **12.0** | **1,052,819** | 95,408,768 |
| read | 10,000 | 27.0 | 1.0 | 10,956 | 14,236 |

Fixed vs marginal: the open phase is 807 allocations once; the per-claim allocation count is nearly flat between 1k and 10k (356.8 → 376.2; the +19 are the deeper range directory, O(log₁₂₈ n) `Arc<Node>` per rebuilt page) and the per-read figure is identical (27.0 / 1.0 / 10,956 B), but **bytes per claim nearly double (573,848 → 1,052,819)**: the whole difference is the RamLog realloc pair (§2.6.2 site 5), whose moved size grows linearly with retained entries. Live heap grows 9.4 KB per committed claim (RamLog entries, range pages, WAL index chunks) until a checkpoint compacts.

#### 2.6.1 Per-crate shares (innermost frame; allocation and reallocation samples separately)

Claim (per committed Create; `run6-e2e-1k.txt`, seed 71; allocation samples 1:101, reallocation samples 1:1; a frame is judged by its source path, std/tokio only when no project frame is on the stack):

| crate (innermost project frame) | allocs per claim | reallocs per claim | what it is |
|---|---|---|---|
| focal-core | 178.2 | 0 | native rows: the copy-on-write neighbour copies (`OwnedEvent`, `Declaration`, `ClaimRow` singletons via `prepare.rs:79-154`), the 5 worst-case scratch Vecs, extras, index rows, the record buffer |
| focal-memory | 84.5 | 0 | range page rebuilds: `bounded_vec` merged/entries, `Arc<Page>`, directory `Arc<Node>` leaves and branches (`range.rs:913-945`, `range_directory.rs:454-511`) |
| focal-log | 27.4 | 10.0 | two group commits: `sizes`/`encoded`/frame/`IndexChunk`/channels per append (`writer.rs:601-632`, `:791`, `:830-832`), writer scratch (`:909-916`, `:1020-1023`), and the fence's `to_stdvec` + 2 `PathBuf::join` (`lib.rs:608-623`; the 10 reallocs) |
| focal-model | 27.4 | 0 | row content copies inside the COW copies (`ClaimState::try_copy`, `RegistrationSet`, `DeclaredObligation` reserve, `lifecycle/memory.rs:34`) |
| focal-consensus | 16.5 | 2.0 | records (`persistence.rs:161-211`), `Box<ReadyPhase>`/`Box<LightPhase>`, RamLog `prepare_with` clones and the **two `reserve_slots` reallocs** (`storage.rs:123-124`), `NodeEvents`, `status()` clones |
| focal-wire | 7.4 | 0 | request/response encode buffers, decoded frame, peer clone, page decode |
| focal-raft | 6.1 | 0 | proposal Vec, unstable staging copy, `Ready.entries`, `msgs` regrowth, committed read-back |
| focal-ledger | 3.5 | 0 | delivery result Vecs, `NativeOutput` |
| focal-node | 3.4 | 0 | `Box::pin`, `Box<VerifiedRequest>`, `oneshot`, discarded reply encode |
| focal-client | 1.1 | 0 | boxed transport future |
| std / tokio (no project frame) | 1.3 | 0 | lazily created pthread mutexes, thread parking |
| **total** | **356.8** | **12.0** | |

Read (per linearizable claim read):

| crate | allocs per read | reallocs per read | what it is |
|---|---|---|---|
| focal-consensus | 8.0 | 0 | `status()` Vec clones ×5, `drain_progress` records, read-index answer |
| focal-wire | 7.0 | 0 | request encode + decode, reply encode ×2 and decode (`objects` Vec + `Box<NativeClaim>`), peer clone |
| focal-node | 5.0 | 0 | `Box::pin`, `Box<VerifiedRequest>`, `oneshot`, page `vec!`, discarded reply encode (`host.rs:659`) |
| focal-ledger | 4.0 | 1.0 | `readiness_context` (`session.rs:1511-1512`; the realloc), context copy (`native_session_apply.rs:1043`), delivery Vecs |
| focal-raft | 2.0 | 0 | `answer_read` read-state, `msgs` |
| focal-client | 1.0 | 0 | boxed transport future |
| **total** | **27.0** | **1.0** | |

#### 2.6.2 Top sites, claim path (reallocation counts exact, allocation counts ×101 sampled)

1. **WAL fence, `[u8;16]` identity pushed per byte** — `postcard try_push` → `serialize_u8` → `focal_log::lib.rs:41` (`WalIdentity` in `Fence`) via `install_fence` `lib.rs:608`: **4 reallocs per claim** (2 per fence ×2 fences).
2. **WAL fence `PathBuf::join`** — `Vec::extend_from_slice` realloc under `directory.join("CURRENT.tmp")`/`join("CURRENT")` (`lib.rs:613`, `:623`): **4 reallocs per claim**.
3. **WAL fence `DurablePosition` varint** (`lib.rs:100`, `:109`): 2 reallocs per claim. Sites 1–3 together prove **two durable group commits per committed claim**: the entry append and the Light-phase `HardState` append (`focal-consensus/src/persistence.rs:317-381`, every cycle on a single voter), i.e. **6 `F_FULLFSYNC` per claim** on macOS.
4. **Copy-on-write page neighbours** — `focal_core::native::owned::singleton` under `OwnedEvent::copy` (`owned.rs:404`/`:415`) called from the row copier `prepare.rs:124` inside `focal_memory::range.rs:937` (`value: copy(&entry.value)` for every retained row of every touched page): **≈106 allocations of 504 B per claim**; the same copier for `Declaration` rows (`owned.rs:240`/`:292`, `prepare.rs:129`): ≈39 × 464 B; `ClaimRow` (`owned.rs:175`/`:228`, `prepare.rs:151`): ≈19 × 1,176 B; `DeclaredObligation` (`focal-model lifecycle/memory.rs:34`): ≈20 × 136 B. Cause: `Key::Meta` is rewritten by every mutation (`original_plan.rs:353`) and shares page class 0 and the CONTROL affinity with every `Key::Event` (`layout.rs:195`, `native.rs:1075-1100`), so page 0 (Meta + the 127 oldest events) and the newest-event page are recopied on every Create.
5. **RamLog `reserve_slots`** (`focal-consensus/src/storage.rs:123` and `:124`): **2 reallocs per claim of 36,108 B and 16,048 B on average at 1k claims** — the two `VecDeque`s are grown with `try_reserve_exact(additional)` although the budget is charged for `next_power_of_two`, so once full they reallocate (and memmove) on **every** append; the moved size grows linearly with retained entries (at 10k: **360,108 B and 160,048 B per claim**, i.e. 72 B + 32 B per retained entry per commit, moved on every append until a checkpoint compacts).
6. **Page rebuild structure** — `bounded_vec` for the merged entries (`range.rs:913`, ≈1.4 KB) and the page entries (`range.rs:932`, ≈19.9 KB), `Arc<Page>` (`range.rs:945`), directory leaf/branch nodes (`range_directory.rs:454`, `:470`, `:487`, `:511`): ≈80 allocations per claim over ~8 touched pages.
7. `focal_log::writer::SharedWal::batch` (`writer.rs:601-632`: `sizes`, `encoded`, per-record exact frame, `IndexChunk`) ≈ 6 per claim across the two appends; the `IndexChunk` is retained until checkpoint.

Read path (27 allocs, 1 realloc per read): `readiness_context` (`focal-ledger/src/session.rs:1511-1512`: `to_vec` + `extend_from_slice`) is **the** realloc plus 1 alloc per read, compared against the barrier context at `:1392` on every linearizable read; `status()` clones `voters`/`learners` (`focal-consensus/src/lib.rs:1190-1191`) five times per read (`native_session_apply.rs:1056`, `:929`, `session.rs:628` ×2, `:1089`); the boxed transport future (`focal-client/src/transport.rs:35`, 2,216 B), the request encode/decode round trip (`transport.rs:55-58`), `AuthenticatedPeer::clone` (`auth.rs:42`/`:53`, a `BTreeSet` node), `Box<VerifiedRequest>` into the host channel (`host.rs:301`, 656 B), the `oneshot` (`host.rs:298`, 984 B), the discarded reply `encode_payload` (`host.rs:659`, 215 B), the page `Vec` + `Box<NativeClaim>` (`native_reads.rs:391`, 1,040 B) and its decode on the client (`NativeObject` `with_capacity`), `answer_read` (`raft.rs:1517`), the read-index context copy (`native_session_apply.rs:1043`), `drain_progress` records (`persistence.rs:166`).

## 3. Process level (page faults, RSS)

`/usr/bin/time -l target/release/focal-load --shape …` (unmodified binary; one process holds the node and the client):

| run | wall (loaded machine) | minor faults ("page reclaims") | major | max RSS | peak footprint |
|---|---|---|---|---|---|
| 1k claims + 1k reads | 59.9 s | 1,589 | 0 | 22.9 MB | 16.7 MB |
| 2k claims + 2k reads | 138.8 s | 2,256 | 0 | 33.8 MB | 25.5 MB |
| 10k claims + 10k reads | 436.9 s | 7,580 | 0 | 121.0 MB | 111.7 MB |

First-touch vs steady state (difference method): 2k − 1k = **667 minor faults per 1,000 claims ≈ 0.67 faults (2.7 MB of fresh pages) per committed claim**, which is the retained state (9.4 KB tracked live growth per claim, 10.9 KB of RSS growth per claim with allocator slack). The intercept, ≈920 faults ≈ 3.7 MB, is first touch: binary/runtime pages, the 550 KB node open, tokio/thread stacks. So at 1k claims ~58% of minor faults are first-touch and ~42% steady-state; at 10k the same slope holds ((7,580 − 1,589) / 9,000 = 0.666 faults per claim; RSS +10.9 MB per 1,000 claims), so ~12% of the 10k run's minor faults are first-touch and ~88% are the retained state of committed claims. Major faults are zero throughout; nothing is paged. Peak footprint stays far below the 64 MiB host budget (`host.rs:45`).

`focal start` serving CLI submits and reads over the Unix socket (`server-faults.sh`; each CLI call is a separate process, so the server sees a fresh connection, a Hello handshake and one request per call):

| run | server wall | minor faults | major | max RSS |
|---|---|---|---|---|
| 200 submits + 200 reads (200 committed, 200 hits) | 68.3 s | 1,846 | 0 | 26.3 MB (peak footprint 11.6 MB) |
| 400 submits + 400 reads (256 committed, 400 hits) | 43.3 s | 1,912 | 0 | 27.5 MB (peak footprint 13.1 MB) |

The server's faults are almost all first-touch (~1,800 ≈ 7 MB: binary, runtime, listener, node open); the marginal cost of 56 more committed claims and 200 more reads over fresh Unix connections was 66 minor faults (~1 per claim including its retained state, ~0 per read), and RSS grew by 1.2 MB. Submits 257–400 returned no reply from the CLI: a client-side bound was reached after 256 committed operations from one data directory (`{"code":"capacity","exit_code":6,"message":"native operation store capacity exceeded"}` — the CLI's native operation store is bounded at 256 operations and refuses with a typed error, as the rules require), so the second run is effectively 256 claims; the in-process runs above do not go through that journal.

## 4. What the counts say (summary of findings)

1. **Admission is zero-alloc** (§2.1). The budget/permit design is sound; nothing to do.
2. **The per-claim cost is 357 allocations, 12 reallocations and ~574 KB requested, of which ~260 allocations (≈73%) are the copy-on-write page rebuild of the native range store** (neighbour rows deep-copied because Meta shares the events' page class) and ~52 KB (growing with retained entries) is one RamLog realloc pair per commit.
3. **Two durable group commits per claim, three `F_FULLFSYNC` each.** Not an allocation, but the counts prove it (§2.6.2 sites 1–3) and it is the wall-clock floor of the write path (6 × ~4–5 ms).
4. **Byte payloads are serialized and deserialized one byte at a time** everywhere (`Vec<u8>` fields in `focal-wire` messages, `Record.payload` in `focal-log`, ids in the classic canonical hash), which is why decode is 7× slower than encode and why WAL records cost 4–10 reallocs each on path A.
5. **The classic reduce allocates 2.2× resident claims per apply** (cycle check + propagation over the whole graph per op) — the qualification doc's O(session) ceiling; legacy path only after native activation.
6. **Replication copies each entry payload F+3 times on the leader and 3 on each follower** (unstable, Ready, RamLog, storage read-back, per-follower message); focal-raft's core is ~34 allocations per entry per 3-member group, comparable to raft-rs.
7. The per-request floor outside the engine is ~11 allocations (boxed transport future, encode/decode round trip, peer `BTreeSet` clone, host clone + `Box::pin` + `Box<VerifiedRequest>` + `oneshot`, a discarded reply encode) and 27 per linearizable read.
8. Page faults are small and almost all first-touch or retained-state growth; there is no steady-state fault churn.

## 5. Static audit (condensed; every line verified; full notes in `audit-*.md`)

### 5.1 `focal-wire` (`audit-wire.md`)
- `frame.rs:63-71` exact buffer + zero-fill per frame, never reused; `read_frame_into`/`read_frame_payload_into` (`:180-190`, `:231-239`) exist and are unused in production. `frame.rs:93-99` two serde passes per encode; `:108-110` owned decode (`DeserializeOwned`) copies every heap field out of the frame buffer that is freed right after. `frame.rs:128-129` two `write_all` per frame: on `quinn::SendStream` each is copied again into a `Bytes` (`quinn-proto send.rs:225`), on `UnixStream` two syscalls; `SendStream::write_chunk(Bytes::from(vec))` would move.
- `message.rs` 19 `Vec<u8>` fields (`:98,:114,:122,:149,:182,:191,:198,:206,:215,:225,:503,:540,:545,:591,:595,:599,:606,:693,:711`) and `native.rs` 4 (`:375,:922-931,:1122,:1263`) serialized per byte; decode preallocates `min(len, 1 MiB)` then doubles (up to 4 reallocs at the 16 MiB cap).
- `auth.rs:45` `PeerGrant.tenants: BTreeSet<TenantId>` cloned per QUIC stream (`transport.rs:488` → `auth.rs:143-149`), per local connection (`unix.rs:94`) and per embedded request (`focal-client transport.rs:57`, `focal-node host.rs:172`).
- Size passes: response 3 + 1 serialize (`handler.rs:225`, `transport.rs:500`, `frame.rs:93-94`), request 3 + 1 (`peers.rs:756`, `transport.rs:760`, `frame.rs:93-94`), ingress +1 (`auth.rs:549`).
- Local transport per request: `symlink_metadata` + fresh `UnixStream::connect` (`unix.rs:161`, `:165`), Hello round trip repeated (`local/mod.rs:32-60`, `:95-101`), ~10 frame/handshake allocations before the handler runs.
- `handler.rs:956-958` `blake3::hash(b"focal.builtin.receipt.v1")` recomputed per receipt attempt record. `round.rs:57/:62` `FuturesUnordered` + `Arc<Task>` per peer per quorum round (≤1024). Good: `payload_len` (`frame.rs:78-88`), header on the stack, limits before allocation, `validate_response` allocation-free, `verify_request` consumes.

### 5.2 `focal-log` (`audit-log.md`)
- Path A `Wal::append`: `lib.rs:412` intermediate `Vec<Vec<u8>>`; `:417` `to_stdvec` per-byte growth; `:503-504` two writes per record; `:519-521` `sync_all` + `install_fence`; `:603-625` fence = `to_stdvec` + 2 `PathBuf::join` + open + 3 writes + `F_FULLFSYNC` + rename + directory `F_FULLFSYNC`. Path A charges no budget.
- Path B (`SharedWal`): `writer.rs:601` `sizes` (exact), `:667` per-byte `serialized_size` (done twice: `validate_append` `:761` and `validate_batch` `:602`), `:620-630` n+1 exact allocations with zero-fill and per-byte `to_slice`, `:632` `IndexChunk` retained per batch (48 B/record + 256 B) until checkpoint, `:791` `sync_channel(1)` per blocking append (~3 allocs), `:830-832` async path oneshot + `sync_channel(1)`; writer thread `:909-911` `batches.try_reserve_exact(64)` per group commit even for k=1, `:1020-1023` `prepared`/`completed` per group commit, `:1031-1035` O(k²) count; group commit = one `finish_append` for up to 64 batches (good).
- Recovery: `lib.rs:745` zeroed `vec![0u8; len]` per record + `:795-805` second allocation and memcpy; `writer.rs:347-349` a 1-element `IndexChunk` per replayed record (+304 B charge); `:1223` `segment_path` `format!` + `PathBuf` per replayed record even with a cached handle; `:1231-1249` 3 syscalls per record; `:1204` thread handoff per record over `sync_channel(1)`.
- `Record.payload: Vec<u8>` (`lib.rs:78`) is the per-byte cause.

### 5.3 `focal-raft` (`audit-raft.md`) and `focal-consensus` (`audit-consensus.md`)
- Copies of one proposal on the leader: caller Vec → `Entry.data` (move, `node.rs:251`) → unstable log copy (`raft.rs:970` → `log.rs:124-125`, staging Vec + payload) → `Ready.entries` copy (`node.rs:448`; originals dropped at `log.rs:394`) → `Record.payload` (`persistence.rs:184` → `lib.rs:1475` prost `encode_to_vec`) → WAL frame Vec (`writer.rs:625-629`, per byte) → RamLog clone (`storage.rs:238`, from the Ready; `Ready::take_entries` exists but `prepare_with` borrows, `persistence.rs:213-217`) → apply clone out of RamLog (`storage.rs:393`, then moved `lib.rs:1300`) + (N−1) per-follower `MsgAppend` copies (`log.rs:548`). 6 + (N−1) user-space copies (7 with `propose_borrowed_in`, `lib.rs:714`); follower 7.
- `raft.rs:362` `msgs` regrows from capacity 0 every Ready (`node.rs:391`); `log.rs:394` unstable buffer released every persist; `progress.rs:91-95` inflights ring released on every Probe transition; pinned by `src/tests.rs:278-280` ("release capacity when idle").
- `log.rs:523-551` `slice` copies the whole unstable tail then trims (`:550`, `raft.rs:422`) — O(tail × messages) for a lagging follower (`raft.rs:464-473` `append_all`).
- `storage.rs:107-124` `reserve_slots` charges `next_power_of_two` but reserves `additional` → realloc per append once full (measured, §2.6.2 site 5) and O(n²) on replay (`lib.rs:1582` → `:315-318`).
- `storage.rs:375-380` `Storage::entries` reserves `high − low` for a lagging peer regardless of `max_bytes` while the core accounts messages by capacity (`raft.rs:690-693`) — unconfirmed risk of failing the leader's staging reservation (`lib.rs:1402-1406`); no test covers a catch-up large enough to be trimmed.
- `persistence.rs:212` `validate_append` walks every payload byte, then `writer.rs:602` sizes and `:629` encodes it again (three per-byte passes). `:218` `Box<ReadyPhase>` per cycle (deliberate), `:294/:298/:391` `events.messages.extend` into a fresh `NodeEvents` per drain, `lib.rs:1300` `events.committed.push` without reserve, `:317-381` Light phase = second append + fsync + fence per cycle when the commit index moves (every cycle on one voter). `lib.rs:1181-1193` `status()` clones two Vecs per call; `:1354-1417` `guarded_in` walks queued messages + unstable entries twice per operation (O(B²) between drains, CPU). No byte cap on a follower Ready (65,536 entries by count only) while the WAL refuses > 64 MiB fatally.

### 5.4 `focal-core` (`audit-core.md`)
- Classic: `graph.rs:71-122` (`check_acyclic`: `done` set + per-claim `stack` Vec + `path` set), `reduce.rs:451-459`, `:692-758` (`propagate` scans), `overlay.rs:113`/`:137` (seek iteration), `pending.rs:445-454` (prepare repeats), `:479` input deep clone; `canonical.rs:322` `to_allocvec`, `:29`/`:46` encoder growth, `reduce.rs:199`/`:392` double hash, `:398`/`:413`/`:419` content clones + `vec!`, `lib.rs:270` receipt clone.
- Native Create: decode exact and verified (`legacy_creation.rs:205-214`, `legacy_creation_plan.rs:324`), frame parsed 3×, fingerprint 3×, chain check 2×, `check_build` 2× (CPU); prepare ~18 exact allocations of which **5 are worst-case 256-capacity scratch Vecs regardless of claim count** (`model/lifecycle/creation.rs:400-402` ×3, `native/graph_effects.rs:174-175` ×2, `plan_nodes` 256 `native.rs:211`); `Extras::push` manual doubling from `Vec::new()` (`prepare.rs:291-326`) though `extras_count` is known; one change buffer per transaction (`original_plan.rs:253-256`); rows are singleton Vecs (`owned.rs:81-93`); keys are `Copy` (`native.rs:968`), no `format!`, no per-row postcard; record encode one exact zero-filled buffer per transaction (`record_codec/buffer.rs:163-170`), rows serialized 4× across passes; publish is a root swap (`native.rs:1672-1696`).
- **COW dominant** (measured): `range.rs:905-950` copies every retained neighbour of every touched page; `layout.rs:195` puts `Meta` and `Event` in the same class/affinity; `native.rs:1075-1100` `page_partition` class 0 for both; Meta rewritten per mutation (`original_plan.rs:353`).

### 5.5 `focal-ledger` (`audit-ledger.md`)
- Native path allocates little directly: frame borrowed (`native_hosting.rs:885`), `propose_borrowed_in` (`native_session_engine.rs:779`), pending chain sized once (`:121-132`), outputs funded before apply (`native_session_apply.rs:18-33`). Per request: 2 `status()` clones; per delivery: `session.rs:1118-1125`/`:1165-1175` three legacy result Vecs reserved for native commits, `native_session_apply.rs:29` `NativeOutput.committed` allocated + charged with no production reader (`session.rs:1430`); per read: `session.rs:1511-1512` `readiness_context` heap (the standalone engine uses a stack `[u8;16]`, `native_session_apply.rs:58-67`), `:1043-1045` a 24 B context copy forced by `read_index(Vec<u8>)`, 3 `status()` clones; `:584` full blake3 `inspect` on apply though the leader holds `pending.hash` (batch path twice `:761`, `:828`); `native_hosting.rs:888` decode limits re-derived per frame.
- Legacy/managed (refused after native activation): `session.rs:844-845` magic `to_vec` + `extend`, `:1043`/`:920` pending `VecDeque` freed when drained and re-reserved (128 slots) per request at low concurrency, `cursor_session.rs:362-365` full `BTreeMap` clone per cursor command (not native-gated), `apply_epoch.rs:255-262` decode + `plan_epoch(inputs.clone())`, `managed_session.rs:209-217` input clone + Box per request, `request_streams.rs:460`/`:512-514` slot clone then immediate realloc.

### 5.6 `focal-node` host (`audit-node.md`) and `focal-client` (`audit-client.md`)
- `host.rs:172` `request.clone()` (BTreeSet + frame memcpy) before every admission check; `:173` `Box::pin`; `:174-189` two error envelopes built eagerly; `:298` `oneshot`; `:301` `Box<VerifiedRequest>` into `sync_channel(32)`; `:659` **`encode_payload` to check the reply size** (allocates, zero-fills, serializes, drops; up to 1 MiB for a read page; `payload_len` exists); `reads.rs:134-136` same per object; `native_ingress.rs:182-184`/`native_reads.rs:135-145` up to 8 `poll()` calls returning fresh `SessionEvents` (~9 Vecs each) that are dropped; `native_reads.rs:476/512/552/573` `Vec::new()` + push; `:389…865` one-element page `vec!`; `Box` per `NativeObject` (wire enum); `native_documents.rs` deep copies per object; `streams.rs:183-191` 4 allocations per stream request; `native_lists.rs:61` `to_stdvec(filter)` per list; `fleet.rs:1195` same clone + Box pattern, `:2692` `locations()` Vec only iterated; metrics are sampled every 5 s, never per request.
- Client: `transport.rs:35` `Box::pin` per attempt; `:55-58` encode → decode → encode → decode (deliberate parity, comment `:54`): 3 allocations + 2 frame copies + 3 extra serde passes; `:57` peer clone; `client.rs:91` `RouteHint` (2 Strings) per request once a route is cached; `:579-583` route lock + lookup even for the route-agnostic embedded transport; QUIC `transport.rs:133` two `String` clones per attempt for the cache key; Unix transport reconnects + re-handshakes per request; the trace sink is free when absent; journals are per durable step, not per request.

## 6. Ranked reductions

Ordered by expected wall-clock gain, memory second. Each states the site, the mechanism, the effect, the correctness argument, the risk, the proving test, and whether it needs a chosen constant (house rule: constants derive from measurement or research and scale from a laptop to a fleet; none proposed here introduces an arbitrary one). "(D)" marks items that need a design decision on durability or format before they are engineering work.

### R1 (D) — One durable group commit per committed proposal, not two
- **Site**: `crates/focal-consensus/src/persistence.rs:317-381` (Light phase: `HardState` record → second `append_async_in` → second fsync + fence), reached on every cycle where the commit index moves during `advance_append` (every cycle on a single voter; measured as two fences per claim, §2.6.2 sites 1–3).
- **Mechanism**: do not give the commit-index-only `HardState` its own group commit. Raft safety needs term, vote and entries durable; `commit` is volatile state in the algorithm and is re-derived after restart from the log and the next quorum (a single voter re-commits its whole persisted log on restart). Either carry the commit index in the *next* entry append's `HardState` record (write-behind), or write it to the WAL without a fence when nothing else is pending. Application delivery (`finish_light` → `apply_entries`) then follows persistence of the entry, not a second fence.
- **Effect**: −1 group commit = −3 `F_FULLFSYNC` and −~9 allocations/reallocations per committed claim on a single voter (and on any leader whose commit advances at `advance_append`); on this APFS host the write path drops from 6 to 3 `F_FULLFSYNC` per claim (roughly −40% of the measured 54 ms p50). The biggest wall-clock item found.
- **Correctness**: durable acknowledgment still waits for the entry's fence; recovery replays entries ≥ the checkpoint's applied index either way (`delivered_index` is checkpointed separately). Bounds unchanged; nothing new allocated; no panics.
- **Risk**: doc 27 / the checkpoint protocol may rely on `HardState.commit` being durable before delivery is reported (e.g. for the "applied index never exceeds durable commit" invariant on restart). Needs the consensus roadmap's ruling — hence (D).
- **Test**: the crash-cut harness (R11 §2) with a kill between the entry fence and the (removed) commit fence: on restart the entry must be re-committed and delivered exactly once; the counting allocator in `tools/load/benches/allocs.rs` asserts one fence encode per claim (site 1 reallocs = 2/claim).

### R2 (D) — One `F_FULLFSYNC` per group commit instead of three
- **Site**: `crates/focal-log/src/lib.rs:519-521` (`sync_all` then `install_fence`) and `:603-625` (fence = new file + fsync + rename + directory fsync).
- **Mechanism**: keep the fence inside an existing, preallocated file rewritten in place (two alternating slots, each `magic | payload | crc32 | monotonic sequence`; the reader takes the highest valid sequence), so a fence is one `pwrite` + one `sync_all` and no rename/directory fsync (an overwrite of an existing inode needs no directory sync). That is 2 `F_FULLFSYNC` per group commit. To reach 1, terminate each batch with a commit frame in the segment itself (the frames already chain `previous` checksums, `lib.rs:485-493`) so the durable position is the last chained, CRC-valid commit frame and the segment fsync covers both data and position; the `CURRENT` file then only names the generation/segment and is rewritten at rollover.
- **Effect**: −1 to −2 `F_FULLFSYNC` and −4 allocations −5 reallocations per group commit (no `to_stdvec(&Fence)`, no `PathBuf::join`s); with R1, a claim goes from 6 fsyncs to 1–2.
- **Correctness**: a torn in-place slot is detected by CRC + sequence and the previous slot is used (same guarantee as rename: the durable position never moves backward past a fenced batch); the commit-frame variant relies on the existing per-frame CRC chain, which recovery already verifies. Bounds: fence ≤ 1 KiB (`lib.rs:36`) unchanged; the fence buffer becomes a stack array (`Fence` is 1 + 16 + 3×8 bytes).
- **Risk**: on-disk format change (a new WAL generation format with migration, `docs/…/22-native-record-format.md` and the release notices); Windows write-through semantics (`focal-windows-fs-semantics` memory) must be re-verified for in-place overwrite. Hence (D).
- **Test**: the existing fault-injection points (`FaultPoint`, `lib.rs:161`) extended with "torn slot"; recovery must return the last fenced position for every injected cut; the append bench's `reallocs/append` for the fence drops to 0.

### R3 — RamLog: grow to the budgeted power of two, not by `additional`
- **Site**: `crates/focal-consensus/src/storage.rs:107-124` (`reserve_slots`: charges `needed.checked_next_power_of_two()` but calls `try_reserve_exact(additional)` on both `VecDeque`s).
- **Mechanism**: reserve `capacity − len` (the power of two already charged) so growth is amortised O(1); on replay the same.
- **Effect**: measured 2 reallocs per commit moving 36 KB + 16 KB at 1k retained entries (at 10k retained entries: 360 KB + 160 KB moved per commit) → ~0.001 reallocs per commit; on replay O(n²) → O(n). Wall-clock: a per-commit memmove that reaches hundreds of KB before a checkpoint, plus the fresh-page faults each larger block incurs.
- **Correctness**: the memory budget already charges the power of two, so accounting is unchanged and the bound is identical; `try_reserve_exact` stays fallible; no constant is chosen (the growth rule is the one the charge already encodes).
- **Risk**: none; a 2-line change.
- **Test**: append 4,096 entries through `RamLog::prepare_with`/`publish` under the counting allocator (or assert `entries.capacity()` only changes at powers of two); the e2e bench's site-5 realloc count per claim → 0.

### R4 — Stop recopying the events page on every native mutation
- **Site**: `crates/focal-core/src/native/layout.rs:195` (`Key::Meta | Key::Event(..) | Key::DueTimer(..) => CONTROL`), `crates/focal-core/src/native.rs:1075-1100` (`page_partition`: Meta and Event both class 0), `crates/focal-core/src/native/original_plan.rs:353` (Meta rewritten per mutation), `crates/focal-memory/src/range.rs:932-945` (page rebuild deep-copies every retained neighbour via `prepare.rs:79-154`).
- **Mechanism**: give `Key::Meta` its own page partition class (or place it in the newest-event page's successor) so the Meta rewrite no longer forces a copy of the 127 oldest events; and keep events append-only at the tail so only the newest page is rebuilt per mutation. Optionally, in `range.rs:937`, share unchanged rows between page versions by reference count (`Arc<Row>` per entry is a justified shared-immutable case: doc 10 lists "COW pages, immutable content" as shared by design), making a neighbour copy a refcount bump instead of a deep copy.
- **Effect**: −106 `OwnedEvent` copies (504 B each), and with the tail-only rule most of the −39 `Declaration`, −19 `ClaimRow`, −20 obligation copies: about **−150 to −190 of the 357 allocations per claim and −60–100 KB per claim**; `Arc<Row>` sharing removes the rest of the neighbour copies (bounded by page size, so the gain is per mutation regardless of session size). Wall-clock: this is the majority of the CPU allocation work per claim; also fewer pages touched → fewer first-touch faults and a smaller peak.
- **Correctness**: the copy-on-write invariant (readers holding the old root keep the old pages; `publish` is a root swap) is untouched by a layout class change; the layout order is an owner-local storage layout, not a wire or placement choice (comment at `native.rs:1072-1074`), but it is part of the frozen record format only if page boundaries are persisted — verify against doc 22 before changing the class table. Budgets: `page_charge` per page unchanged; `Arc<Row>` would charge per row once instead of per copy.
- **Risk**: layout-class changes interact with range splits (`range_layout.rs:73-79`) and with the hydration/import order; `Arc<Row>` touches the "no Arc unless justified" rule and every row copier.
- **Test**: the e2e bench's neighbour-copy sites per claim (target: ≤ 2 pages touched by a plain Create; `OwnedEvent::copy` samples ≈ 0 per claim); the existing range split/merge and record replay tests.

### R5 — Byte payloads as bytes, not sequences (no new dependency)
- **Site**: `crates/focal-wire/src/message.rs:181-183` and the other `Vec<u8>` fields (§5.1), `crates/focal-log/src/lib.rs:78` (`Record.payload`), `crates/focal-model/src/durable_v1/leaves.rs:49` (`[u8;16]` ids per byte in the classic command hash).
- **Mechanism**: a hand-written `Serialize`/`Deserialize` for a `Bytes`-like newtype (or `#[serde(with = "…")]` on the fields) that calls `serialize_bytes` / `deserialize_byte_buf`. Under postcard both forms are `varint(len) ‖ raw bytes`, so the wire and durable bytes are **byte-identical**; `serde_bytes` is not needed (and would be a new crate).
- **Effect**: decode of a frame becomes one exact allocation + memcpy instead of a per-byte visitor (the qualification numbers imply decode at 4 KiB from ~1.96 µs to ~0.3 µs and 64 KiB from ~30 µs to ~3 µs); every `serialized_size`/`validate_append`/`validate_batch` pass over a payload becomes O(1) in the payload (three passes per WAL record, three per response); WAL path A records lose their 4–10 reallocs; encode passes become memcpy. Allocation counts barely change; this is a CPU/wall-clock item on every request, reply, record and replay.
- **Correctness**: postcard's `deserialize_byte_buf` bounds-checks the length against the remaining input before allocating (`postcard de/deserializer.rs:403-410`), which is a tighter adversarial bound than today's `visit_seq` (preallocates `min(len, 1 MiB)`); the frozen-format tests and the adversarial peak-growth tests cover it. No constants.
- **Risk**: low; the type change ripples through pattern matches on the fields.
- **Test**: a corpus test asserting `encode(old) == encode(new)` and round trip for every message with a `Vec<u8>` field; the codec bench (ns/op) and `allocs` bench (unchanged counts, no reallocs on path A).

### R6 — Frame a WAL batch into one buffer, one write
- **Site**: `crates/focal-log/src/writer.rs:620-630` (n exact per-record frames), `crates/focal-log/src/lib.rs:412-432` (path A `Vec<Vec<u8>>` of `to_stdvec`), `lib.rs:503-504` (two `write_all` per record).
- **Mechanism**: `sizes` and `bytes` are already known before encoding (`writer.rs:601-602`); `try_reserve_exact(bytes)` one buffer per batch (bounded by `max_batch_bytes`), write each 20 B header + record contiguously (`to_slice` into the sub-slice; with R5 a memcpy), then one `write_all` (or `writev` of header/payload pairs without copying). The frame layout on disk is unchanged. Keep the writer's `batches`/`prepared`/`completed` Vecs as scratch on the `Writer` across group commits (`writer.rs:909-916`, `:1020-1023`).
- **Effect**: n + 1 → 1 allocation per batch (plus 3 per group commit → 0 after warm-up); 2n → 1 syscalls per batch (a 256-record batch saves 511 `write` calls, ~1–2 ms); zero-fill of n buffers → none (the buffer is fully overwritten; use `try_reserve_exact` + `spare_capacity_mut`/`resize` once).
- **Correctness**: byte stream identical; CRC per frame unchanged; the Pending charge already covers `bytes` (+20 B/record); `max_batch_bytes` bounds the buffer; all reservations fallible.
- **Risk**: low; recovery is untouched.
- **Test**: existing round-trip/replay tests; the append `allocs` bench (allocs/append → ~1 + fence, reallocs → 0); a fault-injection cut inside the single write (recovery truncates at the last valid CRC as today).

### R7 — Move entries instead of copying them through consensus
- **Sites**: `crates/focal-raft/src/raft.rs:970` (`self.log.append(&entries)` on an owned `Vec<Entry>`) → `log.rs:108-143` (`truncate_and_append(&[Entry])` staging copy); follower `raft.rs:1834-1844`; `node.rs:448` (`Ready.entries` copied, originals dropped `log.rs:394`); `crates/focal-consensus/src/persistence.rs:213-217` (`prepare_with(ready.entries(), …)` borrows → `storage.rs:238` `entry.clone()` although `Ready::take_entries` exists, `node.rs:114-116`, and `advance_append` does not read `ready.entries`); `raft.rs:362`/`node.rs:391` (`msgs` regrown from 0 each Ready); `storage.rs:375-380` (`Storage::entries` copies committed entries back out for apply).
- **Mechanism**: `Log::append(Vec<Entry>)` consuming the batch (leader and follower own it); `prepare_with` takes `ready.take_entries()` and moves the entries into RamLog once the records are encoded; keep `msgs` capacity across cycles (update the "release capacity when idle" test `src/tests.rs:278-280` to measure payload bytes, not spine capacity, or release only above a high-water mark); hand committed entries to apply by reference where the application only reads them.
- **Effect**: −2 payload copies and −3 allocations per entry per member (leader: 11 → ~8 payload copies per entry for N=3; core allocations ~34 → ~25). Wall-clock scales with entry size: at the 4 MiB entry cap each removed copy is ~4 MB of memcpy (~0.3–0.5 ms) per member; for 64 B entries it is malloc-call count only.
- **Correctness**: the moved entries are exactly the persisted ones (`stable_entries` already checks index/term, `log.rs:388-392`); RamLog's Payload charge per entry is unchanged; bounds (`unstable_entries`, `max_uncommitted_size`) unchanged; `try_reserve` stays fallible.
- **Risk**: the differential harness (`tests/support`) borrows `ready.entries()` after `prepare_with`; adapt it to `take_entries`. The capacity-retention change conflicts with a pinned design (`tests.rs:278-280`) and needs that decision recorded.
- **Test**: the differential tests (`tests/differential.rs`) must stay equal to raft-rs on every schedule; the raft `allocs` bench core share per entry; a counting-allocator test in focal-consensus that a 3-voter propose/commit cycle performs ≤ 1 payload copy per member beyond the record encode.

### R8 — Check the reply size without encoding it
- **Site**: `crates/focal-node/src/host.rs:659` (`encode_payload(&response, …).is_err()`), `crates/focal-node/src/reads.rs:134-136`.
- **Mechanism**: `focal_wire::payload_len` (`frame.rs:78-88`), which applies the same limit without allocating.
- **Effect**: −1 allocation, −1 zero-fill and −1 serialization per request; for a read page up to `max_frame_bytes` (1 MiB default, `message.rs:776`) of avoided allocation + memset per read.
- **Correctness**: identical decision (`payload_len` and `encode_payload` check the same `limit` against the same `serialized_size`). No risk. **Test**: the node's existing response-limit test plus the read `allocs` figure (27 → 26).

### R9 — Per-request fixed cost on the host/transport seam
- **Sites**: `crates/focal-wire/src/auth.rs:45`/`:53` (`PeerGrant.tenants: BTreeSet` cloned per stream/connection/request: `transport.rs:488`, `unix.rs:94`, `focal-client transport.rs:57`, `focal-node host.rs:172`); `host.rs:172` deep clone before admission (frame memcpy); `host.rs:301` `Box<VerifiedRequest>` into `sync_channel(32)`; `host.rs:298` `oneshot` created before `try_send`; `focal-client transport.rs:35` `Box::pin`; `transport.rs:55-58` codec round trip.
- **Mechanism**: (a) `AuthenticatedPeer { grant: Arc<PeerGrant> }` — the registry hands one immutable grant to every stream; doc 10 already lists transport grants as shared state, so the `Arc` is justified and it also removes the clone at `host.rs:172`'s peer half; (b) store `Work::Request` inline in the channel (32 slots × `size_of::<VerifiedRequest>()`, charged once at spawn) instead of a Box per request; (c) do the size/lane/reserve checks before cloning so refused requests pay nothing, and create the `oneshot` after `try_send` admission; (d) an owned-handoff handler API (`VerifiedRequest` given back with the reply) removes the frame memcpy entirely — API change, do last; (e) the embedded transport's encode→decode parity round trip stays if the codec tests do not already prove parity; if they do, `payload_len` + move saves 2 allocations and 2 frame copies per request.
- **Effect**: −3 to −6 allocations and −1 to −2 frame copies per request (of ~11 fixed); reads 27 → ~21.
- **Correctness**: (a) immutable after registration, revocation replaces the map entry (readers holding an old `Arc` finish their in-flight request with the grant they were admitted under, as today with the clone); (b) the channel bound is the same 32; (c) admission order unchanged for accepted requests. **Risk**: low. **Test**: the `allocs` e2e read figure; existing revocation tests (`network_controller_admission_tests`).

### R10 — Read-barrier and status allocations in the ledger
- **Sites**: `crates/focal-ledger/src/session.rs:1511-1512` (`readiness_context` heap Vec, compared at `:1392` on every linearizable read); `crates/focal-consensus/src/lib.rs:1190-1191` (`status()` clones `voters`/`learners`), called 3× per read, 2× per request, 2× per poll (`native_hosting.rs:1149`/`:1143`, `native_session_apply.rs:1056`/`:929`, `session.rs:628`/`:1089`, `native_session_engine.rs:600`); `native_session_apply.rs:1043-1045` (24 B context copied into a `Vec` because `read_index` takes `Vec<u8>`).
- **Mechanism**: a stack `[u8; 27]` (or the standalone engine's `readiness(term) -> [u8;16]`, `native_session_apply.rs:58-67`) and compare slices; `NodeStatus` with `leader_id()/term()/role()` accessors for the callers that need only those, or `voters: &[u64]` borrowed; `read_index` accepting a small fixed context.
- **Effect**: −1 alloc −1 realloc (the read path's only realloc) and −5 allocs per read; −2 per request; −2 per poll. **Correctness**: trivial; no bounds change. **Test**: read `allocs` figure (→ ~20 with R8/R9).

### R11 — Delivery bookkeeping the native path never reads
- **Sites**: `crates/focal-ledger/src/session.rs:1118-1125`, `:1165-1175` (three legacy result Vecs reserved per delivery, sized by committed entries), `crates/focal-ledger/src/native_session_apply.rs:29` + `session.rs:1430` (`NativeOutput.committed` allocated and budget-charged with no production consumer; `focal-node fleet.rs:3081-3082` resolves waiters via `native_outcome`), `crates/focal-consensus/src/persistence.rs:294/:298/:391` (`extend` into a fresh `NodeEvents` per drain), `lib.rs:1300` (`committed.push` without reserve).
- **Mechanism**: reserve the legacy Vecs only for legacy sessions; drop `NativeOutput.committed` or make it lazy; swap the `messages` Vec into `NodeEvents` when empty; `try_reserve_exact(entries.len())` before the push loop.
- **Effect**: −4 to −6 allocations per commit/delivery and a smaller Pending charge. **Correctness**: nothing reads the removed data (verified by grep); bounds unchanged. **Test**: ledger delivery tests; e2e claim figure.

### R12 — Classic reduce: incremental cycle check and propagation (legacy path)
- **Sites**: `crates/focal-core/src/graph.rs:71-122`, `crates/focal-core/src/reduce.rs:451-459`, `:692-758`, `crates/focal-core/src/overlay.rs:108-140`, `pending.rs:445-454`.
- **Mechanism**: only the new claim's lineage edges can close a cycle, so run the DFS from the new claim's dependencies with one `stack`/`path` scratch allocated per call (not per resident claim) and a `done` set bounded by what it reaches; start `propagate` from the touched claims; give the overlay a merged iterator instead of a seek per row. Applies to `prepare` too.
- **Effect**: per-op allocations from 45 + 2.2·n to ~45; apply from 645 µs (2,000 claims) to tens of µs; removes the O(session) ceiling on the legacy path. **Correctness**: acyclicity is a property of edges; a graph without a cycle stays acyclic unless a new edge closes one, and a freshly generated claim cannot change other claims' satisfied/released state — the same invariants the full scan checks. **Risk**: the differential oracle `apply_serial` exists to prove equivalence (`lib.rs:249`). **Test**: `apply_serial` vs `apply` on the existing corpora; the `allocs` bench second-half figure equals the first-half figure. Ranked here because the path is refused after native activation.

### R13 — Classic canonical hashing without growth
- **Sites**: `crates/focal-model/src/canonical.rs:322` (`to_allocvec`), `:29`/`:46` (`CanonicalEncoder` from `Vec::new()`), `crates/focal-core/src/reduce.rs:199` + `:392` (same content hashed twice).
- **Mechanism**: `serialized_size` + `try_reserve_exact`, or stream into blake3 as `epoch.rs:686-690` already does; hash the claim content once and reuse. **Effect**: −21 reallocs and ~−10 allocs per prepare and per apply. **Correctness**: identical bytes → identical hashes (frozen canonical form; `durable_v1` tests). Legacy path.

### R14 — Pre-size the known-count `Vec::new()` + push loops
- **Sites**: `crates/focal-core/src/native/prepare.rs:291-326` (`Extras` manual doubling though `extras_count` is known), `crates/focal-node/src/native_reads.rs:476/512/552/573`, `reads.rs:123`, `streams.rs:489/559`, `crates/focal-core/src/reduce.rs:91`/`:836`, `crates/focal-memory/src/arena.rs:309-319` (directory rebuilt per page), `crates/focal-log/src/writer.rs:1037`→`:300` (chunk list amortised), `crates/focal-wire/src/admission.rs:166-174` (`VecDeque::new()` then push per new identity). **Mechanism**: `try_reserve_exact(count)` from the bound each loop already has. **Effect**: the residual reallocations on those paths (small). **Correctness**: bounds unchanged, all fallible.

### R15 — Worst-case scratch sized by `plan_nodes` for a one-claim Create
- **Sites**: `crates/focal-model/src/lifecycle/creation.rs:400-402` (3 × `reserve_accounted(limits.nodes)`), `crates/focal-core/src/native/graph_effects.rs:174-175` (2 × `scratch.reserve(limits.plan_nodes)`), `plan_nodes` = 256 (`native.rs:211`).
- **Mechanism**: size from the transaction's actual node count (known after decode, bounded by `plan_nodes`) or keep the scratch on the owner across transactions (charged once). **Effect**: −5 allocations and −(5 × 256 × elem) bytes per Create (memory, not wall-clock). **Correctness**: the same bound applies; the reservation stays pre-charged. No constant.

### R16 — Lagging-peer catch-up: copy what fits, then send
- **Sites**: `crates/focal-raft/src/log.rs:523-551` (`slice` copies the entire unstable range then trims), `raft.rs:464-473` (`append_all` re-slices per message), `crates/focal-consensus/src/storage.rs:375-380` (`Storage::entries` reserves `high − low` regardless of `max_bytes`).
- **Mechanism**: trim to the byte/entry limits before copying; reserve `min(high − low, entries_per_message)` in `Storage::entries`. **Effect**: O(tail × messages) → O(sent) copies during catch-up; removes the unconfirmed staging-reservation failure mode (`lib.rs:1402-1406`) for a peer far behind. **Correctness**: message contents unchanged. **Test**: a catch-up test with a gap larger than `max_size_per_msg` and than 16,384 entries under the counting allocator, asserting the leader stays up and copies ≤ the sent bytes.

### Not proposed (by design, documented)
`Box<ReadyPhase>`/`Box<LightPhase>` per cycle (comment `persistence.rs:24-25`), `HandlerFuture` boxes (object-safe trait), one `tokio` task per QUIC stream, `Box` per `NativeObject` in wire pages (keeps the enum small), the child-budget `Arc` (one per session).

## 6a. Corrections (2026-09-29, the audit's F54)

The audit (`docs/audit/2026-09-29_audit.md` F54) found four places where this record
said more than its instrumentation measured. They stand corrected here; the figures
above are left as written, dated.

- **"Bytes moved by realloc" was an upper bound, not an observation.** The counter
  added `min(old, new)` on every reallocation whether or not the allocator moved the
  block. The allocator now counts a reallocation as a copy only when it returned a
  different pointer (`realloc_moved`, `realloc_moved_bytes`) and counts in-place growth
  apart (`realloc_in_place`); what an allocator does inside an in-place growth is not
  visible to the program. Every "moved" figure in §2 is the old upper bound.
- **Requested bytes are not live memory.** `bytes/op` (now labelled `requested/op`) is
  allocation requests plus the new size of every reallocation; it says nothing about
  resident set size or retained memory. Peak and live growth are per phase, between
  `Meter::start` and `finish`, and include what the phase allocated outside its gated
  operations.
- **The page size of this host is 16,384 bytes, not 4,096.** §3 converted minor faults
  to "fresh pages" at 4 KiB: "0.67 faults (2.7 MB of fresh pages) per committed claim"
  is 0.67 faults × 16 KiB ≈ 10.7 KB of first-touched pages per claim — which is what the
  same paragraph measured as RSS growth per claim (10.9 KB); the 4 KiB arithmetic was
  wrong by four and the agreement was hidden by it. A fault count is events, and a
  fault does not say how many bytes of the page were new; "0 major faults" means no
  fault needed I/O, not that there was no memory pressure, compression or reclaim.
  `getconf PAGESIZE` is recorded with every process-level figure from now on.
- **Counting perturbs.** The counters' atomics and the sampler's backtraces change
  timing, residency and faults even with sampling off; counted runs attribute, and
  uninstrumented release runs measure speed and residency (the 2026-09-29 performance
  record was measured uninstrumented).

## 7. Observations and instrumentation limits
- `benches/replicate.rs` delivers newest-first (`at: net.len() − 1`) and therefore measures a reject/re-probe path on every round; in-order delivery is 24 allocations per entry cheaper for both cores and changes the batch=16 comparison (§2.5). Worth a note in the qualification doc when its numbers are next refreshed.
- `focal schema example claim.submit` produces a document `submit claim` rejects (`evidence_schemas` unknown; then `deadline` missing) — the manual's "self-targeted handoff example, suitable for trying the local protocol" does not currently work as written.
- The sampled site tables are estimates; realloc counts per site are exact because every reallocation is sampled. Sample budgets were raised for the second core run (the first exhausted its budget in the first half).
- Wall-clock in `report-*.json` (17 ops/s writes, 74k ops/s reads) was taken on a loaded machine and is not a qualification number.

## 8. Files touched (all test/bench-only; nothing shipped changes)

New:
- `crates/focal-memory/benches/support/alloc_count.rs` — the counting allocator, sampling, site table, `Meter`, share/report helpers (`#![allow(unsafe_code, …)]` as the adversarial tests do; `check-contracts.py` scopes the unsafe ban to `src/`).
- `crates/focal-memory/benches/allocs.rs`, `crates/focal-wire/benches/allocs.rs`, `crates/focal-log/benches/allocs.rs`, `crates/focal-core/benches/allocs.rs`, `crates/focal-raft/benches/allocs.rs`, `tools/load/benches/allocs.rs`.

Edited (manifests only, one `[[bench]] name = "allocs" harness = false` entry each; no dependency changes):
- `crates/focal-memory/Cargo.toml`, `crates/focal-wire/Cargo.toml`, `crates/focal-log/Cargo.toml`, `crates/focal-core/Cargo.toml`, `crates/focal-raft/Cargo.toml`, `tools/load/Cargo.toml`.

Not mine: `git status` at the end also shows `crates/focal-timing/src/round.rs`, `crates/focal-wire/src/round.rs` and `docs/archictecutre/27-consensus-roadmap-and-slates-port.md` modified — another agent's in-progress work, untouched by this audit (they were not modified when the audit started).

Gates run on the touched crates: `cargo fmt --all --check` clean for the files above (`rustfmt --check` on the seven files exits 0; the workspace-wide `cargo fmt --all --check` currently fails on the other agent's in-progress `crates/focal-node/tests` — "failed to resolve mod `support`: tests/support.rs does not exist" — which is unrelated to this audit); `python3 scripts/check-contracts.py` clean; `cargo clippy -p focal-memory -p focal-wire -p focal-log -p focal-core -p focal-raft -p focal-load --all-targets --locked -- -D warnings`: clean (exit 0 after the final edits; the first pass flagged two `manual_is_multiple_of` lints in the allocator, fixed). The whole-workspace gates were not run (another agent owns the build).

Run: `CARGO_PROFILE_RELEASE_DEBUG=line-tables-only cargo build --release --bench allocs -p <crate>` then `target/release/deps/allocs-<hash>` (env: `FOCAL_BENCH_ITERS`, `FOCAL_ALLOC_SAMPLE`, `FOCAL_ALLOC_REALLOC_SAMPLE`, `FOCAL_ALLOC_SAMPLE_BUDGET`; `FOCAL_RAFT_FIFO=1`; `FOCAL_LOAD_CLAIMS`/`FOCAL_LOAD_READS`/`FOCAL_LOAD_SEED`/`FOCAL_LOAD_TOP`). `cargo bench -p <crate> --bench allocs` works too (bench profile).
