<!-- Qualification record: slates delta catalog, 2026-09-29, macOS arm64 (Apple M5 Max, 18 CPUs, 128 GiB), focal at 6b03c4a with the slates-port batch in the tree. Written by the session's subagent; file:line citations are of that tree. -->

# slates → focal: the delta since the last port (2026-09-10 … 2026-09-29)

Status: analysis only, 2026-09-29. Nothing in either tree was changed. Every claim about
focal cites a focal `path:line` that was read for this document; every claim about slates
cites a slates commit (short hash, date) and file. Where a claim is a reading rather than a
measurement it says so. Where I could not establish a fact it is marked **unclear**.

Sources read in full or in the parts cited: focal
`docs/archictecutre/27-consensus-roadmap-and-slates-port.md` (§1–§8), doc 09 entries
2026-09-27 … 2026-09-29 (`09-implementation-status.md:10456-11525`),
`crates/focal-wire/src/{transport.rs,congestion.rs,round.rs,peers.rs,admission.rs}`,
`crates/focal-timing/src/{lib.rs,round.rs,progress.rs}`,
`crates/focal-raft/src/{raft.rs,progress.rs,quorum.rs,log.rs (parts),track.rs (parts)}`,
`crates/focal-consensus/src/lib.rs` (parts), `crates/focal-node/src/{liveness/driver.rs
(parts),liveness/suspicion.rs,liveness/health.rs,pace.rs,leader_return.rs (head),
fleet.rs (parts),control_host.rs (parts),replication.rs,network_service.rs (parts),
main.rs (parts),network_controller.rs (parts),placement_controller.rs (parts)}`; slates
`git log --since=2026-09-10` over `crates/{transport,rt,cluster,server,fleet,lane}` (269
commits, bodies read for every transport/rt/cluster/server commit since 2026-09-13),
`crates/transport/src/{congestion/copa.rs,congestion/mod.rs,congestion/filter.rs,
reorder.rs,pmtud.rs,pacer.rs,rtt.rs,flow.rs,connection.rs (parts),endpoint.rs (parts),
streams.rs (head),demux.rs (head),flight.rs (head)}`, `crates/cluster/src/{raft.rs
(1–3140),lib.rs (150–430),timing.rs,progress.rs,detector.rs (1–600),coordinates.rs
(head),fold.rs (head),multilog.rs (head)}`, `crates/server/src/{fleet.rs (parts),daemon.rs
(1155–1215),lease.rs (grep)}`, `crates/rt/src/timer.rs`, `docs/wip/BENCHMARKS.md`
(490–560), `docs/bugs/2026-09-28-copa-froze-an-overshot-window.md`; and quinn-proto
0.11.18 (the version `Cargo.lock` pins, `crates/focal-wire/Cargo.toml:19-21`) at
`~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/quinn-proto-0.11.18/src/`:
`config/transport.rs`, `connection/{mod.rs (parts),pacing.rs,mtud.rs (grep),spaces.rs
(parts),paths.rs (grep),assembler.rs (grep),streams/recv.rs (grep)}`, `congestion.rs`.

---

## 0. How to read the verdicts

- **Already had** — focal has the mechanism, cited.
- **quinn provides** — the mechanism is inside quinn-proto; the row says whether focal enables it and at what value.
- **Port** — worth taking; §4 ranks these.
- **Defect candidate** — slates fixed a bug whose analogue is present or plausible in focal; §5 grades it.
- **Not applicable** — a fix for a defect of slates' own transport/runtime that quinn/tokio do not have (doc 27 §3.2, §8.2), or a design focal does not share.
- **Reject** — considered and declined, with the reason.

The house rule on constants applies: a row that would need a number chosen without a derivation is flagged **[constant]**.

---

## 1. The delta table

Index first, then one block per row with the required fields. IDs: T = transport, R = runtime, C = consensus/fleet.

| ID | slates enhancement | slates commit | Verdict | Wall-clock gain at fleet scale |
|---|---|---|---|---|
| T1 | Clocked connection: RFC 9002 time-threshold loss, PTO | 50287e6 (09-27) | quinn provides, enabled | none (had) |
| T2 | Probes as copies; bounded probe copies | 50287e6, 4a3f6d7 | quinn provides | none |
| T3 | Persistent congestion | 50287e6 | quinn provides; Copa reacts | none (had) |
| T4 | Delivery-rate sampling | 50287e6 → deleted 42ebbbb | Not applicable | — |
| T5 | Pacing at the law's rate (Copa 2·cwnd/RTTstanding) | 50287e6, f50c027 | quinn provides its own rule; Copa's not settable | unmeasured; needs a quinn patch |
| T6 | Receive-window auto-tuning to a derived ceiling | 50287e6 | Not applicable (no quinn API); memory note | — |
| T7 | Copa window-shrink fix | 8907c6f (09-27) | Already had | none |
| T8 | 1,200-byte packet floor budget | 2e60c2f (09-27) | quinn provides | none |
| T9 | Adaptive reordering tolerance (RFC 9002 §6.1.1, RFC 8985) | 797fde3 (09-28) | Measure first; not portable inside quinn | only on reordering paths |
| T10 | Ordered stream reassembly | 8700a7f (09-28) | quinn provides | none |
| T11 | PMTUD (RFC 8899) and the raise recheck | 8b55cda, fd4f0ef (09-28) | quinn provides; **enable a derived upper bound** [constant] | CPU/goodput on jumbo paths |
| T12 | Transport parameters in the handshake | 3f2bcf6 (09-28) | quinn provides (QUIC TPs) | none |
| T13 | Don't-fragment; one lent 64 KiB receive buffer per shard | ed613fe (09-28) | Not applicable (quinn-udp; not verified) | none |
| T14 | Stream credit rides acks (idle-peer ack inflated RTT) | 0def3b4 (09-28) | Not applicable (quinn subtracts ack delay) | none |
| T15 | Concurrent prioritized exchanges, strict priority | 4a3f6d7 (09-27), 42ebbbb | Already had (classes, doc 27 §7) | none |
| T16 | Congestion bake-off; Copa selected; losers deleted | f50c027, 42ebbbb | Already had (doc 27 §7) | none |
| T17 | Handshake flight fragmentation / dedup / pending flight | d0513b8, 42d5178, 8bf7509 | quinn provides (CRYPTO frames) | none |
| T18 | forget_stream reply-in-flight fix | 837142a (09-27) | Not applicable (bidi streams) | none |
| T19 | Connection-id demux | 4dd4e6e (09-10) | quinn provides | none |
| T20 | Packet fill of several frames | ae3eab1 (09-13) | quinn provides | none |
| T21 | Fresh stream id per exchange | f7651dc (09-13) | Already had (`open_bi` per request) | none |
| T22 | Duplicate packet-number discard | 62ffd49 (09-10) | quinn provides (`Dedup`) | none |
| T23 | RTT sampled on the runtime clock; first 1-RTT datagram served | 2b9ec09 (09-14) | quinn provides | none |
| T24 | Bake-off harness: 1,000 pings, warm-up, per-run stall bound | 3a0d86e (09-27) | Already had (own harness) | none |
| R1 | Timer wheel jumps to the next occupied slot | 837142a (09-27) | Not applicable (tokio) | none |
| R2 | Shard drains one batch per step; io_uring/epoll/Windows driver fixes; registry reclaim | 3c5a25f, fa94928, 9f03d8e, de11563, 55d6b35, 2b15429 | Not applicable (tokio) | none |
| R3 | Loom models of control flag, doorbell, timed park | f108433, 7cb3079, e339bd8 | Not applicable (no custom runtime) | none |
| C1 | Pre-vote lease forgotten at the minimum election timeout | cf76129 (09-29) | Already had | none |
| C2 | Priority = measured quorum RTT; table in appends; timer yields per rank; leader hands off to an outranking voter | f02ed5e (09-28) | Reject as replacement; hand-off already had | none |
| C3 | Campaign waits for a voter's session held by its link or lent | 38c987e (09-29) | **Defect candidate** (focal shape: vote refused `Busy` on the 2-permit lane, dropped, unobserved) | one election timeout per refused vote |
| C4 | `DispatchWait` stopped a dispatch with nothing gathered at ¾ of its deadline | b036a0e (09-29) | **Defect present** in `focal_timing::round` | placement rounds on WAN |
| C5 | Late pre-vote grants dropped (counted) | b036a0e, 3f8733e | Absent by construction | none |
| C6 | Leader pipelining; per-period window ⌈2×tail/heartbeat⌉ batches | 8119f07, 90d560a (09-29) | Pipelining had; derived depth is a **Port** candidate; the real bottleneck is elsewhere | throughput under loss; bytes |
| C7 | O(1) commit rule; index of configuration entries | 8119f07 (09-29) | Already had | none |
| C8 | Compaction by the thesis size rule; hint-guided bounded batches | 84b218a (09-28) | Hints had; size rule is a **Port** candidate | memory/disk bound |
| C9 | Learners caught up before voting | 3316fc0 (09-28) | Already had (shell rule) | none |
| C10 | Leader hands off on SIGTERM / planned stop | 4e38d3e (09-28), 95f79ba | **Port** | one election timeout per led group per restart |
| C11 | Election jitter independent per node and attempt | b3b6712 (09-28) | Already had | none |
| C12 | Fast track built, kept closed; crossover measured on Azure | 462b63d, b20c5cd, e09a2c7, 1bc85f1 | Same decision as focal (doc 27 §4.6) | none |
| C13 | Leader fills holes at stalled indices with no-ops | 462b63d (09-29) | **Unclear**: schedule test to write | none today (no owner takes the track) |
| C14 | MLRaft measured; both groups keep one log | cf76129 (09-29) | Already had the design (independent ledgers) | none |
| C15 | Symmetric-partition heal probe (idle probe, RFC 6298 backoff to 6 s) | 5836a8c (09-29) | Absent by construction; constants flagged | none |
| C16 | Lone owner's lease needs only the intersection bound | 3f8733e (09-29) | Not applicable (no per-object lease) | — |
| C17 | Serve sockets bound before the daemon; anchor holds ports | 42ebbbb, 0fe5160 (09-28) | Not applicable (no supervisor; UDP) | — |
| C18 | Gossiped seed death no longer strands an unreached peer | f108433 (09-28) | Absent by identity model | — |
| C19 | Retain warm voters; council seated by incumbency and liveness; confirmed stable death | f50e939, fb220e2, d8f482c | Already had (P4, seats) | none |
| C20 | SWIM probe deadline from RTT; suspect stays probed; buddy system; Lifeguard cadence | 484768a, 78547ef (09-13) | Already had; constants flagged | none |
| C21 | Timing law and round budget from measured paths | 0e7a0cd, 328c4d4, 77b23c3, a0ef0ee (09-14) | Already had (P2; focal's estimator is the better one) | none |
| C22 | Election state in status (term, priority, rank, lease, pre-vote refusals by reason) | b036a0e (09-29) | **Port** (small, diagnostic) | diagnosis, not latency |
| C23 | A member that missed its promotion refused every election | 2026-09-29 bug (raft.rs) | Absent | none |
| C24 | A late append below the commit landed compacted entries | 84b218a sibling | Absent | none |
| C25 | Progress-aware fan-out, stragglers, late replies folded | 66e9204, 68f3d1a (09-12/13) | Already had (P1) | none |

### T1 — Clocked session connection, RFC 9002 loss detection
- **Mechanism.** The connection runs on a clock: time-threshold loss (9/8 × max(srtt, latest)), packet threshold 3, PTO = srtt + max(4·rttvar, 1 ms) backed off per expiry (slates `crates/transport/src/rtt.rs:147-160,97-108`, `connection.rs:1400-1418`).
- **slates evidence.** 50287e6 (2026-09-27): "the session connection runs on a clock … fleet 50/50".
- **quinn.** `detect_lost_packets` with `time_threshold` 9/8 and `packet_threshold` 3 (`connection/mod.rs:1695-1760`; defaults `config/transport.rs:381-382`), PTO `pto_time_and_space` (`mod.rs:1834`). focal takes the defaults (`crates/focal-wire/src/transport.rs:189`).
- **focal.** Already had.
- **Verdict.** quinn provides, enabled. Nothing to measure.

### T2 — Probes as copies; bounded probe copies
- **Mechanism.** A PTO probe carries a copy of the oldest in-flight packet's frames (RFC 9002 §6.2.4) so a tail loss is seen; only the two most recent copies stay tracked (`PROBE_COPIES_KEPT`, slates `connection.rs:100-108,1471-1491`).
- **slates evidence.** 4a3f6d7: "probe copies toward a silent peer grew one tracked packet per PTO (136,106 after 90,640 virtual s; now originals + 2 copies)".
- **quinn.** `maybe_queue_probe` moves the oldest in-flight packet's retransmittable frames into `pending` and removes them from the old packet so they are not retransmitted twice (`connection/spaces.rs:115-145`); no per-probe tracking growth.
- **Verdict.** quinn provides. Not applicable.

### T3 — Persistent congestion
- **quinn.** `persistent_congestion_threshold` 3 (`config/transport.rs:390`); computed in `detect_lost_packets` (`mod.rs:1710-1749`) and passed to `on_congestion_event(is_persistent_congestion)`.
- **focal.** `CopaController::on_congestion_event` → `Copa::on_loss(.., persistent)` collapses to the minimum window and leaves slow start (`crates/focal-wire/src/congestion.rs:503-512,421-434`); test `a_loss_is_no_signal_unless_it_persists` (`congestion.rs:626-637`).
- **Verdict.** Already had.

### T4 — Delivery-rate sampling
- **slates evidence.** Built in 50287e6 for BBRv3; deleted in 42ebbbb (2026-09-28): "NewReno, CUBIC, BBRv3, Copa-Meta, the round-robin and weighted schedulers and the delivery-rate sampler deleted after the 2026-09-28 bake-offs".
- **Verdict.** Not applicable: neither Copa needs it, and slates removed it.

### T5 — Pacing at the law's rate
- **Mechanism.** slates paces at the controller's rate: Copa's `2·cwnd/RTTstanding` (`congestion/copa.rs:37-38,136-143`), a token bucket whose quantum is one millisecond of the rate clamped to [2 datagrams, 64 KiB] (`congestion/mod.rs:119-132`, `pacer.rs:25-59`).
- **quinn.** Its own pacer: tokens refill at `1.25 × window / srtt` (`connection/pacing.rs:91`), burst capacity `window × 2 ms / rtt` clamped to [10, 256] × mtu (`pacing.rs:128-151`). The `Controller` trait has no pacing hook — `on_sent`, `on_ack`, `on_end_acks`, `on_congestion_event`, `on_mtu_update`, `window`, `metrics`, `initial_window` only (`congestion.rs:17-84`); `ControllerMetrics::pacing_rate` is reported in path stats (`connection/paths.rs:204`), not used by the pacer.
- **focal.** Says so: "what quinn does not let a law decide is the pacing, which stays quinn's" (doc 27 §7, `27-…md:479-480`; `congestion.rs:26-29`). focal's grid was measured under quinn's pacer, so its Copa result already includes it.
- **Verdict.** quinn provides a pacer; Copa's rate is not settable without a quinn patch. Where focal's Copa is worse than CUBIC — 1 Mbit/s 20 ms, p99 134 vs 78 ms (`27-…md:480-483`) — the standing queue is the law's, not the pacer's. No port; an upstream change if a thin-link measurement ever shows the 1.25 rule bursting a Copa window.

### T6 — Receive-window auto-tuning to a derived ceiling
- **Mechanism.** The advertised window doubles when a whole window was consumed within two round trips, up to a ceiling the daemon derives from its memory budget (Chromium's rule; slates `flow.rs:22-29,62-81`; connection shape `connection.rs:183-202`). It was 4.8 kB before (50287e6).
- **quinn.** No auto-tune: `stream_receive_window` and `receive_window` are static configuration (`config/transport.rs:31-32,376-377`; defaults 1.25 MB per stream and unbounded per connection).
- **focal.** Sets the stream window to `min(max_frame_bytes + 16, 1 MiB)` and the connection window to that × (streams + 2): with `WireLimits::default()` (`crates/focal-wire/src/message.rs:773-784`: 1 MiB frames, 16 streams) and the control host's 10 MiB frames (`crates/focal-node/src/control_host.rs:466-472`), the stream window is 1 MiB (`STREAM_WINDOW_CEILING`, `transport.rs:58`) and the connection window 18 MiB; `send_window` the same (`transport.rs:177-194`). Chosen by measurement for the 1,024-gap bound (doc 27 §7, `27-…md:452-461`).
- **Memory note.** The ceiling is configuration, not a memory derivation: up to 128 pooled connections (`peers.rs:52`) × 18 MiB of quinn receive buffering that `MemoryBudget` does not see (the listener's child budget at `network_service.rs:660` charges focal's frames, not quinn's assembler; not verified further). Doc 27 §8.4 lists the stream count as open.
- **Verdict.** Not applicable as a port (no API). Record the memory bound as an open item: quinn's per-connection buffers are outside the budget.

### T7 — Copa shrinks a window it is not filling
- **slates evidence.** 8907c6f (2026-09-27): frozen 1.5 MB window (six BDPs), 92,500 queue drops per run; p99 at 100 Mbit/s/20 ms 103 → 20.5 ms; goodput 82 → 88 % (`docs/bugs/2026-09-28-copa-froze-an-overshot-window.md`).
- **focal.** `if increase && !window_limited { return; }` guards increases only; a decrease still runs (`congestion.rs:303-307`, comment "It still shrinks (slates, 2026-09-28)"); test `a_window_the_sender_does_not_fill_shrinks_and_never_grows` (`congestion.rs:588-601`).
- **Verdict.** Already had.

### T8 — Every packet fits the 1,200-byte floor
- **slates evidence.** 2e60c2f: ack and credit frames grew packets to 2,048 bytes, truncated and read as losses (17 % goodput on a lossless 100 Mbit/s path); `MAX_PACKET_PAYLOAD = 1171` (`endpoint.rs:81-86`).
- **quinn.** Packets are built to the current MTU (`initial_mtu`/`min_mtu` 1,200, `config/transport.rs:384-385`; `connection/packet_builder.rs`).
- **Verdict.** quinn provides. Not applicable.

### T9 — Adaptive reordering tolerance
- **Mechanism.** A loss declared and then acknowledged is spurious; the packet threshold rises to its reordering distance + 1 (capped by the window in packets) and the time threshold gains ¼ min_rtt per round trip that saw one (capped at srtt); both reset after 16 quiet recoveries; declarations remembered for two srtts, bounded by count (slates `reorder.rs:1-17,56-158`; `connection.rs:1150-1178`).
- **slates evidence.** 797fde3 (2026-09-28): a reordering path (10 Mbit/s, 20 ms, 8 ms jitter) carried 0.254 of the link, 0.540 after; "neutral elsewhere within the noise band"; 27,016 spurious retransmissions before.
- **quinn.** Fixed thresholds only (`config/transport.rs:381-382`); no spurious-loss detection (grep `spurious` in `connection/*.rs`: comments at `mod.rs:1191,1483,1511,4077` only).
- **focal.** Not adaptable from outside quinn. focal's Copa takes a loss as no signal unless competitive or persistent (`congestion.rs:421-434`), so the cost on a reordering path is the retransmitted bytes and in-flight accounting, not a window collapse — the same cost slates paid under Copa. focal's simulator can already reorder (`crates/focal-sim/src/path.rs:134,164-168` `Path::reordering`), and `tests/congestion.rs` has no reordering scenario (the grid's scenarios are rate × rtt × loss, bursts, queue, `congestion.rs:98-110`; doc 09 table `09-…md:11075-11194`).
- **Verdict.** Measure first: add a reordering scenario to `crates/focal-wire/tests/congestion.rs` (8 ms jitter at 10 Mbit/s/20 ms, as slates ran) and read `PathStats.lost_packets` against the fabric's zero drops. If the carried fraction falls as slates' did, the fix is an upstream quinn-proto change (RACK-style widening), or a static `packet_threshold`/`time_threshold` derived from the measured reordering distance **[constant]**. Gain only on reordering paths (ECMP/multipath WANs).

### T10 — Ordered stream reassembly
- **slates evidence.** 8700a7f: 4,000,000 examinations for 4,000 holed arrivals → ~16,000; "Found by the focal cross-check".
- **quinn.** `Assembler` holds spans in a `BinaryHeap<Buffer>` with `defragment` and `MAX_CHUNKS = 1024` (`connection/assembler.rs:13-15,105-108,361`) — the 1,024-gap close focal already met and bounded with `STREAM_WINDOW_CEILING` (doc 27 §7 `27-…md:451-461`; quinn-proto 0.11.18 merges full-datagram spans, `Cargo.lock`).
- **Verdict.** quinn provides. Not applicable.

### T11 — Path MTU discovery and the raise recheck
- **Mechanism.** RFC 8899 search from the 1,200-byte floor by binary search to the peer's declared maximum, `MAX_PROBES` 3, black hole after 3 consecutive above-floor losses, re-search after 600 s that rechecks the last failed size alone (slates `pmtud.rs:26-42,131-167,190-247`).
- **slates evidence.** 8b55cda: a session finds a 9,000-byte path (8,989 confirmed) and falls back through a shrink to 1,500 with no data lost. fd4f0ef: 4.8× goodput on real loopback (1,625 → 7,840 Mbit/s, 9,209-byte packets, 12 probes) and neutral on floor paths; restarting the whole search cost ~36 lost probes per raise, +22 % p99 at 64 kbit/s/300 ms.
- **quinn.** MTUD on by default: `mtu_discovery_config: Some(MtuDiscoveryConfig::default())` (`config/transport.rs:386`) with `interval 600 s`, **`upper_bound 1452`**, `black_hole_cooldown 60 s`, `minimum_change 20` (`config/transport.rs:745-753`); `MAX_PROBE_RETRANSMITS 3`, `BLACK_HOLE_THRESHOLD 3` (`connection/mtud.rs:524-528`); clamped by the peer's `max_udp_payload_size` (`mtud.rs:719-733` tests); a raise restarts the binary search from the current MTU as the lower bound (`mtud.rs:662-683`); GSO on (`enable_segmentation_offload: true`, `config/transport.rs:401`).
- **focal.** Takes the defaults (`transport.rs:189-207` sets no `mtu_discovery_config`, `initial_mtu`, `min_mtu`); Copa follows `on_mtu_update` (`congestion.rs:513-515,258-264`). So focal's connections never send more than 1,452 bytes, on a 9,000-byte data-centre or loopback path included.
- **Verdict.** quinn provides; **enable** by raising `MtuDiscoveryConfig::upper_bound`. **[constant]**: the bound must be derived — the interface MTU (Linux `IP_MTU` after connect; not read here) or the UDP maximum 65,527 with the search bounded to 3 × ⌈log₂(65,527 − 1,200)⌉ ≈ 48 probes per 600 s. The raise-recheck cost slates measured (36 probes per raise on a floor path) applies to quinn's restarted binary search on any path narrower than the bound: measure the ping p99 on the thin-link scenarios of `tests/congestion.rs` (1 Mbit/s) with the bound raised before deciding.
- **Expected gain.** CPU and syscalls on LAN/loopback bulk (custody, seeds): fewer packets per megabyte; smaller than slates' 4.8× because quinn already batches with GSO and slates' runtime had no offload (uncertain: unmeasured in focal). Nothing on the WAN grid, whose paths are at the floor. Memory: none.
- **How to measure.** `crates/focal-wire/tests/congestion.rs` with `Path::with_mtu(9_000)` (`focal-sim/src/path.rs:173-176`) at 100 Mbit/s 1 ms, carried fraction and the number of datagrams sent; and a real-loopback transfer through `PeerConnectionPool` counting `udp_tx.datagrams`.

### T12 — Session transport parameters in the handshake
- **slates evidence.** 3f2bcf6: a dialect version and the largest UDP payload each end reads, TLV, checked at handshake completion.
- **quinn.** Standard QUIC transport parameters (`transport_parameters.rs`; `max_udp_payload_size` clamps MTUD).
- **Verdict.** quinn provides. Not applicable.

### T13 — Don't-fragment on every datagram; one lent 64 KiB receive buffer per shard
- **slates evidence.** ed613fe: DF via `IP_PMTUDISC_PROBE`/`IP_DONTFRAG`/`IP_DONTFRAGMENT` (`rt/src/netsys.rs:76-93,262-278`); a per-shard lent buffer instead of 2 KiB stack buffers (`transport/src/receive.rs:1-10`).
- **quinn.** Socket options are quinn-udp's (not read for this document; **unclear** whether DF is set on macOS/Windows). Receive buffering is quinn-udp's `recvmmsg`/GRO path; `datagram_receive_buffer_size` (`config/transport.rs:394`) is the DATAGRAM-frame buffer, unrelated.
- **Verdict.** Not applicable; the DF question is an upstream fact to confirm if T11 is taken (a search without DF measures nothing).

### T14 — Stream credit rides acknowledgements
- **slates evidence.** 0def3b4: an idle peer acknowledged an ack-eliciting `MaxStreams` only on waking, splitting RTT samples between 100 ms and 3 s; PTO reached 6.6 s; ping p99 at 64 kbit/s/100 ms/1 % 5.5–7.1 s → 0.62–0.79 s.
- **quinn.** RTT samples subtract the peer's reported `ack_delay`, bounded by its `max_ack_delay` (`connection/mod.rs:1541-1551`), and are taken only for the largest newly acked ack-eliciting packet; an idle peer's delayed acknowledgement cannot inflate the estimate by seconds.
- **Verdict.** Not applicable.

### T15 — Concurrent prioritized exchanges; strict priority scheduler
- **slates evidence.** 4a3f6d7: a control ping through a full 1 Mbit/s queue waits one RTT + one queue drain, not the whole transfer (~2 s); 42ebbbb: strict priority chosen (control p99 1.023× best; round-robin 4.8× at worst, `BENCHMARKS.md:539-543`).
- **focal.** Streams per request (`transport.rs:765-769`), `SendStream::set_priority` from `Operation::class()` (`transport.rs:494,770-771`); measured in doc 27 §7 (`27-…md:569-581`: 1 Mbit/s 100 ms beside eight transfers 260.3 → 170.9 ms p99).
- **Verdict.** Already had.

### T16 — The congestion bake-off and Copa
- **slates evidence.** `BENCHMARKS.md:504-520`: 57 scenarios, Copa ping p99 1.268× best (worst 3.05× at 1 M/20 ms/5 %), goodput shortfall geomean 1.068 (≈ 0.936 of best), selected; NewReno/CUBIC stalled at 100 M with 1 % and 5 % loss; BBRv3 stalled at 100 M/300 ms/5 %.
- **focal.** Own grid (doc 27 §7 `27-…md:463-472`; doc 09 `09-…md:11075-11201`): Copa p99 1.082× best, carried 0.998 of best, stalled nowhere. The grids differ (scenarios, harness, 3 seeds vs 1), so the numbers corroborate the choice and are not comparable line by line.
- **Verdict.** Already had.

### T17 — Handshake flight fragmentation, dedup, pending flight across establish calls
- **slates evidence.** d0513b8 (09-10), 42d5178 (09-14: a 120-SAN certificate crosses as 3 fragments), 8bf7509 (09-14: a dialer starved past one budget resends instead of waiting in silence), d94d1cc (a roster-sized flight truncated at 2,048 bytes).
- **quinn.** CRYPTO frames with `crypto_buffer_size` 16 KiB (`config/transport.rs:397`), handshake retransmission on PTO; rustls in focal sends no roster hints beyond the sponsor CA (`transport.rs:217-222`, one root store).
- **Verdict.** quinn provides (doc 27 §3.2). Not applicable.

### T18 — A late request copy forgot the reply in flight
- **slates evidence.** 837142a: `forget_stream` dropped both halves of a shared id; a 64 kbit/s 5 %-loss run ran to 152,000 virtual seconds.
- **focal.** Bidirectional streams with independent halves; a reply is reset only when its delivery was not acknowledged (`transport.rs:515-526`).
- **Verdict.** Not applicable.

### T19–T23 — Connection-id demux, packet fill, fresh stream id, duplicate discard, RTT on the runtime clock
- **quinn.** CID routing (`cid_state.rs`, endpoint), packet building (`packet_builder.rs`), fresh stream ids per `open_bi`, `Dedup` sliding window of 129 packet numbers (`connection/spaces.rs:459-482`), RTT from `largest_acked_packet_sent` (`mod.rs:1550`).
- **focal.** `open_bi` per request (`transport.rs:765`).
- **Verdict.** quinn provides / Already had. Not applicable.

### T24 — Bake-off harness fixes
- **slates evidence.** 3a0d86e: 1,000 steady-state pings per run at 1 % of link load (the first grid's ~150 made the p99 the second-worst sample), warm-up max(20 RTT, 5 s), worst-scenario p99 beside the geometric mean; 4a3f6d7: a run is recorded stalled at 100× its ideal duration.
- **focal.** `tests/congestion.rs` asks 200 bytes every 50 ms for 30 virtual s; the doc 09 table reports 450 asked per run (`09-…md:11075`), i.e. 22.5 s after a warm-up (the warm-up rule is in the harness body, not read to the line). A p99 over 450 samples is the fifth-worst sample — coarser than slates' 1,000.
- **Verdict.** Already had; if the grid is rerun for T9/T11, lengthen the measured window so the p99 is not one of the five worst samples.

### R1–R3 — Runtime
- **slates evidence.** 837142a: the timer wheel walked every idle tick after a firing (66,595 visits to reach one timer, now ≤ 12; `rt/src/timer.rs:147-187`); 3c5a25f: a drain takes one batch (82,245-task fill 95 s → 12.6 s); fa94928/9f03d8e/de11563: io_uring readiness, epoll `EPOLL_CTL_MOD`, Windows kick; 55d6b35/2b15429: registry and context reclaim (1,025 runtimes; 56,016 KiB leak per 32 cycles); f108433/7cb3079/e339bd8: loom models (a `swap/swap` control flag: 5,204 interleavings clean, the old halves deadlock at 1 and 207).
- **focal.** tokio (doc 27 §8.2 "Its executor … Not taken"). focal's own rule of never holding a lock across an await is stated where it matters (`peers.rs:159-161`).
- **Verdict.** Not applicable.

### C1 — The pre-vote lease lapses at the minimum election timeout (thesis §4.2.3)
- **Mechanism.** A follower that heard no leader for the *base* election timeout forgets its leader and grants pre-votes even while it yields its own jittered timeout (slates `timing.rs:268-282,322-351`, `raft.rs:1054-1064,1085-1097`).
- **slates evidence.** cf76129 (2026-09-29): before, the lease lasted until the node's own campaign; in a three-region group the outranked region won 195 leader losses of 200 at a 6,766 ms median; after, the most central survivor wins all 200 at its first campaign (3,322 ms; p99 12,109 → 3,680 ms).
- **focal.** `in_lease = check_quorum && leader_id != 0 && election_elapsed < config.election_tick` (`crates/focal-raft/src/raft.rs:1278-1286`): the lease is the base tick, not the randomized timeout; `election_elapsed` resets on a leader's append/heartbeat/snapshot (`raft.rs:1737,1744,1752`) and on a granted vote (`raft.rs:1374`); patience extends only the campaign (`raft.rs:1023-1026`), not the lease.
- **Verdict.** Already had (raft-rs's rule). Nothing to measure; `sim_election_tests::the_preferred_member_leads_after_a_loss_over_a_regional_path` covers the preferred-successor case (doc 09 `09-…md:10628`).

### C2 — Priority as the measured quorum round trip
- **Mechanism.** Each voter's priority is the ⌊n/2⌋-th smallest measured round trip to the other voters with its spread; intervals that overlap tie (`timing.rs:114-145`, `raft.rs:376-399`); followers report it in every append reply and the leader returns the table in every append (`raft.rs:516-518,553-554,2195-2206,2304-2306`); the timer yields one timeout per live voter that outranks it (`timing.rs:303-351`); a leader hands off after two windows to the voter that outranks it most, once per leadership (`raft.rs:2710-2743`).
- **slates evidence.** f02ed5e (2026-09-28): on Microsoft's published five-region matrix the fastest-committing region led every seed (14 of 20 before; median commit 189 → 171 ms) and took leadership back after an outage (0 of 20 before); b036a0e: on KIND pods (80 ms and 20 ms egress) the central pod is handed leadership 6 of 6 (median 3.08 s) where the unfixed daemon hands it to the outranked pod 6 of 6 (median 6.47 s).
- **focal.** Priorities are configuration: preferred leader 3, its zone 2, other voters 1, from the committed placement (`crates/focal-node/src/fleet.rs:449-451,1598-1606`); the vote rule `priority_in_force <= priority_of(message)` unless the log is more current (`raft.rs:1352-1360`, `Precedence::Log`); leadership returns to the preferred leader by `leader_return` (`crates/focal-node/src/leader_return.rs:1-40`: fit 2 timeouts, rest 4, doubling ≤ 6), spread by the planner and moved by the balancer (doc 27 §5). Doc 27 §5: "Priorities are configuration, never liveness."
- **Verdict.** Reject as a replacement: focal's rank encodes placement policy (home region, zone), which a pure latency rank cannot, and its hand-off is already there. The one piece focal lacks is the *yielding* timer (a lower-ranked voter waits one timeout per higher-ranked live voter before campaigning); in focal a lower-ranked voter campaigns at its timeout and is refused by the higher-ranked ones without a term spent (pre-vote), and the preferred one campaigns at its own draw — so no time is lost and no term. No port. If the placement ever wants a latency tiebreak inside one rank, `PeerConnectionPool::path` (`peers.rs:417-423`) already measures it.

### C3 — A campaign waits for a voter's session that is out for a moment
- **Mechanism.** slates' campaigns borrow one session per peer; a session lent to a dispatch or held by its link's discovery page made the round ask no one. Now the campaign waits, paced at the poll interval, until the round's base deadline, counting voters awaited and those left unasked by reason (`server/src/fleet.rs:4837-4913`).
- **slates evidence.** 38c987e (2026-09-29): 5 voters unasked in 4 of 10 leader losses on KIND, each costing a whole election timeout; median successor 2.77 → 1.91 s, none unasked, 9 of 10 at the first pre-election.
- **focal.** No session is lent: every request is a stream of its own (`transport.rs:765-769`). But focal has a bound of the same shape. Each connection admits two Raft exchanges at once (`RemoteCapacity.control: Semaphore::new(2)`, `transport.rs:700-701`) and the pool admits `per_peer_inflight` = 2 exchanges per peer *node* (`peers.rs:53,933`); both are `try_acquire`d and refused `Busy` at once (`transport.rs:754-759` → `WireError::Limit` → `peers.rs:860`; `peers.rs:798-808`). Every group a node hosts shares that lane to each peer, and a Raft exchange holds its permit until the peer answers `PeerAccepted`, which the peer sends after it has *persisted* the message (`fleet.rs:2966` `WaitingFor::PeerPersistence => Response::PeerAccepted`). The replication driver drops a refused frame, counting it `saturated` (`crates/focal-node/src/replication.rs:126-146`); the count is not exported (grep `saturated` in `crates/focal-node/src`: `replication.rs:15,144` only, nothing in `metrics.rs`), the core is not told (`Frame::complete` reports snapshot status only, `replication.rs:80-85`; `report_unreachable` is never called from the driver — grep), and a vote request is re-sent only at the next election timeout (`raft.rs:1021-1034`). So on a node hosting many session groups, a pre-vote or vote to a voter node B is refused whenever two appends of other groups to B are outstanding (an RTT plus B's fsync), and that election waits another timeout, invisibly.
- **Verdict.** **Defect candidate** — present in focal's shape; frequency unmeasured. Failing test first: a pool with a peer that holds two Raft exchanges open (a handler that persists slowly), then a third Raft request of class Consensus — assert it is delivered within one round budget, not refused. Fix at the cause: consensus messages are never refused for the lane (a lane of their own as probes have: `peers.rs:157-158,935` `probes: Semaphore::new(1)`; or an awaited acquire bounded by the round budget for `TrafficClass::Consensus`, `peers.rs:807`), export `saturated`/`lost` as `focal_replication_*` metrics, and report a lost append as unreachable so the leader probes instead of waiting out its window (`raft.rs:1436-1443`, `focal-consensus/src/lib.rs:845-846`).
- **Expected gain.** Wall clock: one election timeout per refused vote — 1 s at the floor (`focal-consensus/src/lib.rs:109-110` × 100 ms) and up to the pace ceiling × election ticks across the planet. Memory: none. Correctness: observability of a silent drop.
- **How to measure.** `crates/focal-wire/tests/peer_registry.rs`-style pool test over real QUIC; `focal-consensus::sim_election_tests` extended with a second group sharing the pool; on real processes `tests/leader_balance.rs` (three processes, sessions spread) with a stalled follower's fsync injected (`FaultSite`).

### C4 — A dispatch with nothing gathered stopped at three quarters of its deadline
- **Mechanism.** slates' `DispatchWait::judge` now returns "keep waiting" when nothing was gathered and the deadline has not passed; a round with nothing gathered is never extended but is not stopped before its deadline (`cluster/src/lib.rs:303-320`).
- **slates evidence.** b036a0e (2026-09-29): the first KIND runs took 13.4 and 25.4 s because a candidate whose one live voter answered in the last quarter failed every pre-election; over 40 WAN seeds the maximum election fell from 11.9 to 9.3 s.
- **focal.** The same rule, unfixed: `DeadlineExtender::evaluate` returns `Expire` at the lookahead when the witness is not progressing (`crates/focal-timing/src/round.rs:153-165`), a witness that never advanced is not progressing (`round.rs:109-114`), the derived lookahead is ¾ (`round.rs:72`), and `gather` ends the round on `Expire` (`crates/focal-wire/src/round.rs:101-106`). The test `a_round_with_no_answer_ends_at_its_lookahead` pins it: two peers that never answer end a 100 ms round at 75 ms (`focal-wire/src/round.rs:192-200`). The deadline is `max(period, tail)` with `tail = smoothed + 4·rttvar` of the exchanges the peer answered (`round.rs:58-66`; `focal-timing/src/lib.rs:207-215`): with a smoothed exchange of 100 ms and a deviation of 5 ms the tail is 120 ms and the lookahead 90 ms, so an answer that takes the *mean* plus a little is cut off. Each cut-off exchange is counted abandoned and doubles the peer's expected tail (`peers.rs:257-267,427-433`), so it corrects itself after a few rounds — at one wasted round each.
- **Where it bites in focal.** `gather` has one production caller: the session-fact signature round of the placement agent (`crates/focal-node/src/placement_agent.rs:2710-2715`). Raft votes do not go through `gather` (they are per-peer sends through the replication driver, `replication.rs:111-150`), so elections are not affected; placement passes are.
- **Verdict.** **Defect present.** Port slates' rule into `RoundWait::judge` or `DeadlineExtender::evaluate`: `Expire` past the lookahead only if something was gathered or the deadline itself has passed. Failing tests first: change `a_round_with_no_answer_ends_at_its_lookahead` to end at the deadline (100 ms, `Expired`), and add `an_answer_in_the_last_quarter_is_collected` (one peer at 90 ms, deadline 100 ms → `Enough`). Doc 27 §3.1 P1 and doc 09 2026-09-28 ("no answer ends the round at its lookahead (75 ms)", `09-…md:10697-10698`) state the old rule and need the correction.
- **Expected gain.** Wall clock: one placement round per cut-off on WAN placements; correctness of the tail estimate (no spurious doubling). Memory: none.

### C5 — Late pre-vote grants
- **slates.** A late pre-vote reply is dropped by design and counted (`ELECTION_LATE_PRE_VOTE_GRANT`, `server/src/fleet.rs:3866-3870,4553-4563`; `timing.rs:33-36`); b036a0e found 4 in 10 successor pre-elections dropped one; 38c987e's campaign wait made it 0 in 20 trials.
- **focal.** A pre-vote reply is its own inbound message and is stepped whenever it arrives; the core accepts it while it is still a pre-candidate of that term (`raft.rs:1692-1707`, term checks `raft.rs:1270-1319`).
- **Verdict.** Absent by construction.

### C6 — Leader pipelining with a derived window
- **Mechanism.** A confirmed follower is sent the next batch before the last is acknowledged as far as its window holds, whenever a resend would not carry the whole backlog (`raft.rs:2147-2284`); a follower buffers the leader's out-of-order entries in its window and absorbs them when the hole fills (`raft.rs:2370-2445`); the window is one batch per period a lost batch takes to repair, ⌈2 × broadcast tail / heartbeat⌉ batches (`timing.rs:54-57,210-222`; `REPAIR_ROUND_TRIPS = 2`), derived each period from the measured paths.
- **slates evidence.** 8119f07 (2026-09-29): across five Azure regions a window of one batch keeps a group committing 2,000 proposals/s at a 172 ms median where no window is overloaded at 7.3 s; 14 % fewer bytes at 1,000/s. 90d560a: the derived window equals the best fixed window in every simulated case (at 2,000/s with 1 % loss 201/321 ms median/p99 against one batch's 260/458).
- **focal.** raft-rs pipelining: `ProgressState::Replicate` with an `Inflights` window (`crates/focal-raft/src/progress.rs:10-101,224-246`), `send_append_all` fills the window (`raft.rs:769-776,464-473`), the window is `max_inflight_msgs` = 128 messages (`focal-consensus/src/lib.rs:113,574`; doc 27 §1 table) of up to `max_size_per_msg` = 4 MiB + 1 KiB each (`lib.rs:111,573`); a follower rejects an out-of-order append (no buffering) and the leader backs up by the conflict hint (`raft.rs:1548-1573`). Nothing derives the depth from the path. And the depth the core sets is not what reaches the wire: the pool admits two Raft exchanges per peer (C3) and the driver `max_inflight` tasks (`replication.rs:116`); the rest queue in the bounded `outbound` channel and are dropped, counted, when it is full (`fleet.rs:2932-2934`, `control_host.rs:1641-1652`) — while the core counts them sent (`progress.rs:232-237`) and waits for a heartbeat response to free the window (`raft.rs:1616-1619`) or a refusal.
- **Verdict.** Pipelining Already had. A derived depth is a **Port** candidate, but the binding constraint in focal is the two-permit lane and the persistence-coupled reply (C3), not Raft's 128. Take C3 first; then derive the per-peer depth from the same tail slates uses (focal has it: `PeerConnectionPool::path`/`exchange_tail`, `peers.rs:417-433`; the pace's `broadcast_tail_ns`, `focal-timing/src/lib.rs:220-228`) **[constant: REPAIR_ROUND_TRIPS = 2 is derived in slates as "the refusal back and the resend out"]**.
- **Expected gain.** Throughput and bytes under WAN loss (slates: 14 % fewer bytes; p99 458 → 321 ms at 1 % loss); memory: a window bounded by the path instead of 128 × 4 MiB per follower of queued frames (each charged to the budget at `fleet.rs:2898-2907`, so bounded, but refused under pressure).
- **How to measure.** `crates/focal-raft/benches/replicate.rs` (core cost, unchanged); a new `sim_election_tests` case with a proposal stream at 1,000/s over `focal_sim::path` at 1 % loss and 80 ms, reading commit latency and bytes on the fabric (`FabricStats`, `path.rs:230-239`).

### C7 — O(1) commit rule and a configuration index
- **slates evidence.** 8119f07: the commit rule tried every index of the backlog and every configuration lookup scanned the log — 20 ms per proposal at a 5,000-entry backlog, now 61–102 ns at any backlog up to 50,000 (`raft.rs:3082-3128,739-742`).
- **focal.** `Tracker::quorum_index` orders the members' matched indexes once (`progress.rs:319-341`, `quorum.rs:94-115`) and `Log::maybe_commit` checks one term (`log.rs:460-467`) — cost in members, not backlog; the configuration in force is the tracker's, with `pending_conf_index` for the one change in flight (`raft.rs:240,1457-1481`) and a scan only over applied…committed when a candidate hups (`raft.rs:1192-1207`). Measured: 1,721 ns per entry committed by every member, 3 members 16 at a time (doc 09 `09-…md:10877-10886`).
- **Verdict.** Already had.

### C8 — Compaction by the thesis's size rule; hint-guided bounded batches
- **Mechanism.** Snapshot once the applied entries' wire bytes exceed the last snapshot's size (factor one); the leader waits for its followers up to twice that, so the retained state is at most three snapshots and the tail (`cluster/src/fold.rs:11-41`); appends carry a byte budget of entries derived from the first receive window (`raft_wire.rs:115-125`); a refusal carries the conflict term and its first index (`raft.rs:527-555,3054-3075`).
- **slates evidence.** 84b218a (2026-09-28): 4,000 changes 0.6 µs each instead of 25.5 µs; the log no longer grows with the fleet's life; an empty follower found in 1 refusal instead of 20; a late append landed compacted entries (fixed).
- **focal.** Conflict hints: `find_conflict_by_term` on both sides (`raft.rs:1548-1559,1854-1860`; `log.rs:275-291`); an empty follower answers `reject_hint = 0` and the leader probes at 1 (`raft.rs:1854-1860,1549-1573`; `progress.rs:214-217`) — one refusal. Compaction trigger: `checkpoint_interval` = 1,024 applied entries above the floor (`crates/focal-node/src/control_host.rs:36-51,1748-1762`) plus a refresh when a membership change committed above the floor (`control_host.rs:1737-1777`); sessions checkpoint by their own `checkpoint_if_due` (`fleet.rs:1703`; rule not read to the line).
- **Verdict.** Hints Already had. The trigger is a count **[constant: 1,024]** where slates' is derived from the state's own size. **Port** the rule for the control log: checkpoint once the WAL bytes above the floor exceed the last snapshot's bytes (the snapshot size is known: `memory::snapshot_bytes`, `lib.rs:563`), with the follower wait slates added (a follower one round behind is otherwise sent the whole snapshot at every compaction — slates measured it on the third voter of a council).
- **Expected gain.** Memory/disk: the WAL held above the floor becomes proportional to the state (1,024 entries of up to 4 MiB is 4 GiB against a snapshot of a few MiB); wall clock: fewer snapshot installs for a lagging follower. 
- **How to measure.** `focal_root_snapshot_index` and `focal_root_peer_pending_snapshot` metrics (memory file, doc 09) under `tests/control_host.rs` with a slow follower; disk bytes of the WAL between checkpoints.

### C9 — Learners caught up before voting (thesis §4.2.1)
- **slates evidence.** 3316fc0 (2026-09-28): replaying Figure 4.4(a), a group losing a voter after adding an empty one commits in 1 round instead of 21; staging in rounds, caught up once a round completes within one CheckQuorum window, aborted after one window without progress (`raft.rs:237-286,2598-2659`).
- **focal.** New voters join as learners and are promoted when installed and custody-verified (`placement_controller.rs:946-968,1023-1057`), and the shell refuses `AddNode` unless the member's `matched >= committed` (`focal-consensus/src/lib.rs:1112-1123` `LearnerBehind`); the agent retries on `Behind` (`placement_controller.rs:887-889`).
- **Verdict.** Already had, in the shell. **Unclear**: whether a learner that never catches up ends the plan (slates aborts staging after `STALLED_WINDOWS`); the retry at `placement_controller.rs:887` is bounded by the plan's own deadlines, not read here.

### C10 — A planned stop hands leadership off
- **Mechanism.** On SIGTERM (or the anchor's stop request) the daemon transfers each group it leads to the most caught-up voter, declares a stop deadline of two CheckQuorum intervals of the slower group plus one period, waits until the successor is in office (not merely until it stepped down), then exits (`server/src/daemon.rs:1155-1215`; `raft.rs:2825-2842,2853-2884`).
- **slates evidence.** 4e38d3e (2026-09-28): a council leader sent SIGTERM is succeeded in 0.106–0.141 s by real processes (five runs) against a 1 s election timeout the old SIGKILL made survivors wait out; 95f79ba: council hand-off 0.103 s against a 1.316 s leader-loss election (medians of five).
- **focal.** `shutdown_signal` handles SIGTERM/ctrl-c (`crates/focal-node/src/main.rs:572-593`) and the cleanup calls `host.stop()` under `SHUTDOWN_DEADLINE` (30 s, `main.rs:440-453`, `network_service.rs:108-115`); `begin_stop` marks the owner stopping with a deadline in its periods (`fleet.rs:1993-2003`) and the owner finishes once nothing is ready (`fleet.rs:1704-1730`); `leader_return` holds while stopping (`fleet.rs:1666`). No transfer on stop: `transfer_leader` is called by the admin/controller path (`fleet.rs:850-862`) and by `leader_return` (`fleet.rs:1675,1826`) only; none in `network_service.rs`/`main.rs` (grep). The controller transfers before it *removes* a draining voter and `cluster nodes remove` does (doc 27 §5), so only an explicit drain avoids the election; a rolling restart, an upgrade or a pod eviction costs every group the node leads a full election timeout.
- **Verdict.** **Port.** On stop, each owner that leads asks its core to transfer to the voter with the highest `matched` among live voters (focal has `handle_transfer_leader`, `raft.rs:1650-1675`, and the heir rule in `post_conf_change`, `raft.rs:1942-1954`, to lift into a `most_caught_up_voter`), waits at most two election timeouts at the group's pace (`TickPeriod`, `pace.rs:78-81`) for a successor to be known (`leader_id != self`, `raft.rs:565-567`), then stops as today; the wait is inside `SHUTDOWN_DEADLINE`. Failing test first, on real processes as `tests/drain_leader.rs` does: SIGTERM the leader of three sessions, assert each has a new leader before one election timeout of its pace, and that the stop's own bound holds.
- **Expected gain.** Wall clock: one election timeout per led group per restart — 1 s at the floor, and `election_tick × period` at the pace (up to 10 × 2 s ceiling = 20 s across the planet, `TickPace::derive` clamps to `tick_ceiling` 2 s, doc 09 `09-…md:10531-10532`); with leaders spread one session each over N nodes (doc 27 §5 table), a rolling restart pays it N × (sessions per node) times. Memory: none. Correctness: proposals refused `NotReady` during the gap are avoided.
- **How to measure.** New binary suite beside `tests/drain_leader.rs`; `fleet_stop_tests.rs` for the owner rule; `focal_session_leader_returns_total` afterwards to see leadership return to the restarted node.

### C11 — Election jitter independent per node and attempt
- **slates evidence.** b3b6712 (2026-09-28): `(id + attempt) mod span` kept a congruent pair congruent forever — a split vote every round for 19 s; now `splitmix64(id ^ attempt·γ) mod span` (`timing.rs:236-259`).
- **focal.** `reset_randomized_election_timeout` draws splitmix64 from a per-node stream that advances by γ on every reset (`raft.rs:2014-2028`), seeded from `getrandom` (fallback: a hash of node, group and cluster ids) (`focal-consensus/src/lib.rs:1481-1492`); a candidate redraws at every term change (`raft.rs:875-881,1079`). Caveat: `become_pre_candidate` does not redraw (`raft.rs:1084-1096`), so a pre-candidate that hears nothing keeps its draw — but two pre-candidates that collide become candidates and redraw independently, so no collision persists.
- **Verdict.** Already had.

### C12 — The fast track's crossover
- **slates evidence.** 462b63d (2026-09-29): on five Azure regions the fast track commits up to 37 % sooner for a proposer far from the leader below 4 % loss (Southeast Asia 435 → 275 ms) but later for one beside it (East US 156 → 201 ms: a fast quorum of four is larger than a classic three) and later for everyone at 10 %; both groups keep it closed. b20c5cd/e09a2c7/1bc85f1: exhaustive models found the published recovery unsafe and fixed the design.
- **focal.** Built, modelled (`docs/models/FastTrack.tla`), measured in simulation at 0.75× for a non-leader proposer from 0 to 10 % loss (doc 09 `09-…md:10954-10985`), no owner takes it (doc 27 §4.6, §8.1). focal's leader takes the first entry it hears of, so it does not pay the extra round above 5 % loss that the published algorithm and slates' vote-counting leader do (`27-…md:230-238`).
- **Verdict.** Same decision. No port. slates' real-region numbers corroborate that the track pays only for proposers away from the leader.

### C13 — Holes left by lost fast votes
- **slates evidence.** 462b63d: an index whose votes fell short of a classic quorum held up every index behind it for good — at 4 % loss a proposer lost up to half its commands (348 of ~780); the leader now proposes a no-op at a stalled index (`raft.rs:1949-1983`).
- **focal.** The leader takes, for its next index, whatever it has heard of as soon as one vote is there (`crates/focal-raft/src/track.rs:359-393` `decide` → `votes.most(index)`), so an index with any vote never stalls. An index *below* a held vote with no vote of its own — a proposer whose uncommitted tail was longer than the new leader's log proposes above a gap — is filled only by a later proposal or the leader's own entries; the schedule harness's `settles` checks that a fresh proposal commits (`crates/focal-raft/tests/support/cluster.rs:459-484`), not that every earlier fast proposal was applied or reported displaced.
- **Verdict.** **Unclear.** Write the schedule in `crates/focal-raft/tests/fast.rs`: a follower with an uncommitted tail two entries longer than the new leader's log proposes on the fast track; assert the proposal is applied or reported displaced within the settle budget. No owner takes the track today, so no wall-clock gain; a correctness fact for stage E's record.

### C14 — MLRaft
- **slates evidence.** cf76129: a keyed command proposed as one of five regions is lost expects 1,036 ms with one log and 1,301–1,399 ms with two to five; a lost leader of any log but the designated one stalls every log's keyed commands (3.7–6.3 s); both groups keep one log.
- **focal.** Many independent ledgers, no cross-log order (doc 27 §5 `27-…md:365-373`); MLRaft's balancing mechanisms are in place (`leading`, `leader_move`, `leader_balancer`).
- **Verdict.** Already had the design; slates' measurement is corroboration.

### C15 — A symmetric partition heals
- **Mechanism.** An idle probe task reaches out to a peer it believes dead one suspicion window after retirement, doubling to a cap of 6 s (RFC 6298 backoff under `ELECTION_MARGIN` × the Lifeguard-capped window) with one ping whose gossip drives the peer's own refutation (`server/src/fleet.rs:143-213,2243-2272`).
- **slates evidence.** 5836a8c (2026-09-29): on KIND a council leader cut off for 15 s had not rejoined 180 s after the heal; now 4.95–6.30 s after it over 19 trials.
- **focal.** The liveness driver's round is drawn from every member whatever its status (`crates/focal-node/src/liveness/driver.rs:768-794`: `members.keys()`, skipping only a member with a probe in flight); a Dead member that answers is Revived (`driver.rs:1428-1431`); its route stays while its contact is committed (`network_controller.rs:1480-1513` builds routes from every authorized contact). A probe to a dead peer fails fast inside the pool's `unreachable_cooldown` (2 s, `peers.rs:59,956-958`) and dials again after it, on the probe lane (`max_probe_inflight` 16, `peers.rs:54`).
- **Verdict.** Absent by construction: both halves keep probing. **[constant]**: focal's period (1 s, `driver.rs:81`) and cooldown (2 s) are set where slates derives the first reach-out (one suspicion window) and the cap (10 × the Lifeguard-capped window); the cost is one dial every 2 s per dead peer per node, bounded by the probe lane. No port.

### C16 — A lone owner's lease
- **slates evidence.** 3f8733e: the lease needs `others − f` confirmations (`server/src/lease.rs:204-216`); a lone owner refused its own volume from 0.51 s after formation for good.
- **focal.** No per-object owner lease; a session is a consensus group. Not applicable.

### C17 — Serve sockets bound before the daemon; the anchor holds serve ports
- **slates evidence.** 42ebbbb: the fixture's bind-release-rebind race froze 1 run in 550–1,040; 0fe5160: the anchor adopts the node's ports once and hands them to every daemon; 5836a8c records one unexplained CI `EADDRINUSE` on a restart.
- **focal.** The service binds its UDP socket in `start` unless one is handed in (`network_service.rs:652-655`); there is no supervisor process; test ports are claimed across processes outside the ephemeral range (`tests/support/ports.rs`, doc 27 §8.2 "Had it"). UDP has no TIME_WAIT; a restarted process binds once the old one has exited.
- **Verdict.** Not applicable; absent.

### C18 — A gossiped seed death no longer strands an unreached peer
- **slates evidence.** f108433: a node whose first dials failed received the peer's manifest-seed death by gossip, idled its probe and never dialed again; KIND scale 10/10 after (was 2 of 5 failing).
- **focal.** Members are the directory's committed nodes, not manifest seeds; a never-reached member is suspected after `unconfirmed_patience` rounds (`driver.rs:1356-1361`), a Dead gossip is folded (`driver.rs:1512-1518`), and probing continues (C15) so the first answer revives it.
- **Verdict.** Absent by the identity model.

### C19 / C20 / C21 / C25 — Already ported classes
- Warm voters, seats by incumbency and liveness, a death held for one window (f50e939, fb220e2, d8f482c): doc 27 §3.1 P4, §5 seats, §8.2 "Taken".
- SWIM probe deadline from RTT with backoff, suspect stays probed, Lifeguard buddy system and cadence (484768a, 78547ef): focal's `LivenessConfig` timeout `clamp(300 ms, 3 × rtt_ucb, 2 s) × LHM` (`driver.rs:52-93`), Lifeguard score with 0.25 weight to 3× (`liveness/health.rs:6-41`), confirmation-shrunk suspicion with bounded extensions (`liveness/suspicion.rs:7-8,137-163`), indirect probes (`driver.rs:992-1010`), Vivaldi coordinates (`driver.rs:1315-1319`). **[constant]**: `base_timeout_ms 300`, `timeout_cap_ms 2_000`, `period 1 s`, `suspicion_factor 3`, `suspicion_spread 6` (`driver.rs:78-93`) are set; slates derives its probe deadline from the measured round trip floored at the heartbeat (484768a). **Unclear** whether focal dilates the probe *cadence* by the health multiplier as slates does (`driver.rs:342` mentions it; not read to the line).
- Timing law and round budget (0e7a0cd, 328c4d4, 77b23c3, a0ef0ee): doc 27 §3.1 P2; focal's `PathRtt` is the median and MAD of the last sixteen probes (`focal-timing/src/lib.rs:53-157`), which focal found necessary where slates' RFC 9002 smoothing carried a starting peer's late answer for seconds (doc 09 `09-…md:10771-10779`). slates still smooths (`timing.rs:66-112`).
- Progress-aware fan-out and stragglers (66e9204, 68f3d1a): doc 27 §3.1 P1.

### C22 — Election state in status
- **slates evidence.** b036a0e: every node's status reports its groups' term, priority, rank, lease, campaigns, pre-vote replies and refusals by reason (`raft.rs:401-462 ElectionView, PreVoteTally`); "which is how the runs were read" — the lease bug (cf76129) and the unasked-voter bug (38c987e) were found from these counters.
- **focal.** `cluster replicas diagnostics` reports `leader_returns`/`leader_returns_failed` (doc 27 §5); no pre-vote refusal counters in the core (grep `refused_leased|pre_vote_refus|refusals` in `focal-raft/src/raft.rs`, `focal-consensus/src/lib.rs`: none).
- **Verdict.** **Port** (small): count refusals by reason in `step_vote` (`raft.rs:1335-1392`: lease `1278-1286`, term, log, priority) and grants/refusals of the member's own campaign in `poll` (`raft.rs:1172-1191`), expose through `DurableNode` diagnostics and `focal_*_pre_votes_refused_total{reason}`. Gain: diagnosis; it is what would have shown C3's dropped votes.

### C23 / C24 — Two slates core bugs of 2026-09-28/29
- A member that missed its promotion refused every election (slates `raft.rs:1160-1188` comment): focal's `step_vote` never consults its own configuration (`raft.rs:1335-1392`); `hup` refuses only a non-promotable *campaign* (`raft.rs:1208-1216`); a candidate counts only voters (`progress.rs:342-360`). Absent.
- A late append below the commit landed compacted entries (84b218a): focal answers an append anchored below its commit with its commit index (`raft.rs:1826-1830`, etcd's rule). Absent.

---

## 2. focal's Copa against slates' Copa, rule by rule

Read side by side: `crates/focal-wire/src/congestion.rs` (focal) and `crates/transport/src/congestion/{copa.rs,mod.rs,filter.rs}` (slates, at HEAD 38c987e; the law last changed in 8907c6f).

| Rule | slates | focal | Differs |
|---|---|---|---|
| Target and increase test `cwnd·d_q ≤ (1/δ)·smss·RTTstanding`, empty queue always increases | `copa.rs:161-166` | `congestion.rs:291-302` (u128, saturating) | no |
| RFC 9002 §7.8 guard: an unfilled window does not grow but still shrinks | `copa.rs:167-173` | `congestion.rs:303-307` | no (T7) |
| Slow-start doubling cadence | once per srtt by the clock: `now − last_double > srtt` (`copa.rs:174-184`) | only when the acked packet was **sent** after the last doubling plus a srtt: `sent > then + srtt` (`congestion.rs:308-327`) | **yes** — focal's change; doc 27 §7 table (`27-…md:489-492`): 69 % → 96.9 % of 10 Mbit/s at 300 ms, window 3.07 MB → within the path |
| Slow-start exit | first decrease (`copa.rs:210`) | first decrease (`congestion.rs:358`) | no |
| Velocity: doubles after three srtts in one direction, resets on reversal, judged once per srtt | `copa.rs:218-244` | `congestion.rs:364-396` | no |
| Velocity cap | `cwnd/smss/(1/δ)` packets (`copa.rs:240`) | that divided by `stride` = 2 (`congestion.rs:386-393`, `DEFAULT_STRIDE` `:61`) | **yes** — focal's change; stride 2 p99 1.060 vs 1.149 at stride 1, carried 0.998 vs 0.951 (`27-…md:498-504`) |
| Sudden reversal at high velocity resets | `copa.rs:191-197` | `congestion.rs:334-339` | no |
| Step `acked·smss·v·(1/δ)/cwnd` with a byte remainder | `copa.rs:198-212` | `congestion.rs:340-360` | no |
| δ = ½ (`1/δ = 2`) | `mod.rs:58-59` | `congestion.rs:50-51` | no |
| Mode test: nearly empty within a tenth of the 4-srtt spread above RTTmin; competitive +1 per loss-free srtt | `copa.rs:246-273` | `congestion.rs:397-417` | no |
| Loss: no signal unless competitive (halve `1/δ`, ≥ default, once per srtt); persistent congestion → minimum window, leave slow start | `copa.rs:275-288` | `congestion.rs:418-434` | no |
| RTTmin over 10 s, RTTstanding over srtt/2, Nichols 3-sample filters | `copa.rs:29-33,152-153`, `filter.rs` | `congestion.rs:42-46,63-146,273-280` | no |
| Initial 10 datagrams, minimum 2, window follows the datagram size | `mod.rs:24-29`, `copa.rs:115-120` | `congestion.rs:38-41,258-264` | no |
| **Window-limited input** | `cwnd_limited = window_full_at > newest_acked`: the window was full since the newest acked packet left (`connection.rs:1129-1134`, draft-bbr §2.2) | `!app_limited`, quinn's flag **at ack time**: set when the last transmit found nothing to send and was not congestion-blocked (`connection/mod.rs:950`), passed unchanged to every per-packet `on_ack` (`mod.rs:1621-1627`) and to `on_end_acks` (`mod.rs:1533-1538`); focal reads it as `window_limited: !app_limited` (`congestion.rs:499`) | **yes** — semantics differ; effect unmeasured: a sender that just drained its buffer withholds growth on acks of packets it sent window-limited, and one that just refilled grows on acks of packets sent while idle |
| RTT sample fed to the law | one per ACK frame: the newest acked packet's (`connection.rs:1116-1122`); `newly_acked` = the frame's bytes | one `on_ack` per acked packet with that packet's own `now − sent` (`congestion.rs:485-502`) | minor — more samples into the filters; the step is linear in bytes so the sum is the same |
| Pacing | `2·cwnd/RTTstanding`, quantum 1 ms of rate in [2 datagrams, 64 KiB] (`copa.rs:136-143`, `mod.rs:119-132`) | quinn's `1.25·cwnd/srtt`, burst `window·2 ms/rtt` in [10, 256]·mtu (`pacing.rs:91,128-151`); not settable through `Controller` (`congestion.rs:17-84`) | **yes** — not focal's to change (T5) |
| Datagram size source | slates' own PMTUD (`connection.rs:1210-1215`) | quinn's `on_mtu_update` (`congestion.rs:513-515`), capped at 1,452 by default (T11) | by configuration |
| Delivery-rate | none | none | no |

**What each carries now.** focal: Copa 1.082× best p99, 0.998 of best carried, stalled nowhere; 96.9 % of 10 Mbit/s at 300 ms; 85.4 % of 100 Mbit/s at 300 ms (`27-…md:466-472,506-507`; grid rows `09-…md:11075-11201`). slates: Copa 1.268× best p99, goodput shortfall 1.068 (`BENCHMARKS.md:509-511`) on its own 57-scenario grid; its per-path fractions (doc 27 §8.3 cites 0.795 at 100 M/100 ms and 0.496 at 300 ms) are in `docs/wip/research/data/2026-09-28-congestion-grid-0def3b4.csv`, which I did not read, so I cannot state them at HEAD; since slates' law has not changed since 8907c6f and lacks focal's two rules, doc 27 §8.3's statement stands as written (**unverified at the row level**). The two grids are not the same paths or harness, so the geomeans are not comparable to each other.

---

## 3. focal's Raft against slates' Raft, rule by rule

| Rule (slates, commit) | slates | focal function | State |
|---|---|---|---|
| Pre-vote lease lapses at the minimum election timeout (cf76129) | `timing.rs:322-351`, `raft.rs:1059-1064` | `Raft::step` `in_lease` (`raft.rs:1278-1286`); resets `raft.rs:1737,1744,1752,1374` | had |
| Priority = measured quorum RTT with spread; tie on overlap (f02ed5e) | `timing.rs:120-145`, `raft.rs:376-399` | integer ranks from placement, `fleet.rs:449-451,1598-1606`; vote rule `raft.rs:1352-1360` | different by design (C2) |
| Priority table returned in appends | `raft.rs:2195-2206,2304-2306` | every member derives its own rank from the committed placement (`fleet.rs:1598`) | equivalent by another means |
| Timer yields one timeout per outranking live voter | `timing.rs:336-345` | absent (grep `rank` in `raft.rs`: none); a refused pre-vote costs no term | not needed (C2) |
| Leader hands off to an outranking voter after two windows, once (f02ed5e) | `raft.rs:2710-2743` | `leader_return` (`leader_return.rs:32-40,105-130`): fit 2, rest 4, doublings 6 | had |
| Leadership transfer: refuse proposals while transferring; invite when caught up; abort after ~one election timeout (95f79ba) | `raft.rs:2788-2884,235` | `handle_transfer_leader` (`raft.rs:1650-1675`), `propose` drops while transferring (`raft.rs:1453`), abort at `election_tick` (`raft.rs:1045-1049`), `MsgTimeoutNow` (`raft.rs:1597-1604,1709-1720,1763-1770`) | had |
| `most_caught_up_voter` for a drain | `raft.rs:2825-2842` | heir = a voter with `matched == last` in `post_conf_change` (`raft.rs:1942-1954`) — removal only | partial (C10) |
| Hand-off on SIGTERM / planned stop (4e38d3e) | `daemon.rs:1155-1215` | none on `stop` (`fleet.rs:1993-2003,1704-1730`; `main.rs:440-453`) | **absent → Port** |
| Campaign waits for a session out for a moment (38c987e) | `fleet.rs:4837-4866` | vote refused `Busy` on the 2-permit lane and dropped (`transport.rs:700-701,754-759`; `peers.rs:53,798-808,860`; `replication.rs:144`) | **defect candidate** (C3) |
| A dispatch with nothing gathered waits its whole deadline (b036a0e) | `lib.rs:311-320` | `DeadlineExtender::evaluate` expires at ¾ (`round.rs:153-165,109-114`) | **defect present** (C4) |
| Late pre-vote grants dropped, counted | `fleet.rs:3866-3870,4553-4563` | replies stepped on arrival (`raft.rs:1692-1707`) | absent by construction |
| Pipelining to a confirmed follower within a byte window; probing state after a refusal (8119f07) | `raft.rs:2163-2219,747-749` | `ProgressState::Replicate`/`Probe`, `Inflights` (`progress.rs:10-101`), `send_append_all` (`raft.rs:769-776`) | had (count window) |
| Window derived per period ⌈2×tail/heartbeat⌉ batches (90d560a) | `timing.rs:216-222` | `max_inflight_msgs` 128 fixed (`lib.rs:113,574`) | absent (C6) |
| Follower buffers out-of-order leader entries and absorbs them | `raft.rs:2370-2445` | rejects; leader backs up by hint | different by design (raft-rs) |
| Resend the whole backlog when one batch would carry it | `raft.rs:2238-2284` | `become_probe` on refusal then one message until answered (`progress.rs:152-161,224-230`) | equivalent in effect |
| O(1) commit: majority's match index; configuration index (8119f07) | `raft.rs:3082-3128,739-742` | `quorum_index` (`progress.rs:319-341`), `pending_conf_index` (`raft.rs:240`) | had |
| Recovery reads every report, not only its own window's reach (8119f07) | `raft.rs:1307-1376` | `recover(reports, last)` over all vote reports (`raft.rs:1127`; `track.rs:480`) | had (own design) |
| Compaction by the thesis size rule, factor 1, followers waited for up to 2× (84b218a) | `fold.rs:11-41` | `checkpoint_interval` 1,024 entries + membership refresh (`control_host.rs:36-51,1748-1777`) | absent (C8) |
| Conflict hints: term and first index of the run; empty follower in one refusal (84b218a) | `raft.rs:3054-3075,2489-2522` | `find_conflict_by_term` both sides (`raft.rs:1548-1573,1854-1860`; `log.rs:275-291`) | had |
| Append anchored inside the committed prefix takes the rest, never refused (84b218a) | `raft.rs:2308-2326` | answers with the commit index (`raft.rs:1826-1830`) | had (etcd's variant) |
| Learners staged in rounds, caught up within one window, aborted after a stalled window (3316fc0) | `raft.rs:237-286,2554-2659` | shell refuses `AddNode` below the commit (`lib.rs:1112-1123`); agent retries (`placement_controller.rs:887`) | had (shell); abort unclear |
| Voters vote without consulting their own configuration (2026-09-29) | `raft.rs:1160-1176` | `step_vote` (`raft.rs:1335-1392`) | had |
| Election jitter independent per node and attempt (b3b6712) | `timing.rs:236-259` | `reset_randomized_election_timeout` (`raft.rs:2014-2028`), seed `lib.rs:1481-1492` | had |
| Fast track: leader decides from votes once a classic quorum voted; fills a stalled index with a no-op (462b63d) | `raft.rs:2108-2145,1949-1983` | leader takes the first entry heard (`track.rs:359-410`); no stall at a voted index; hole below a vote unclear | different design (C13) |
| Fast track closed on an uncommitted or joint configuration; a transfer decides nothing | `raft.rs:1889-1903,2113-2118` | `fast_commit` refuses under pending conf or joint (`track.rs:413-421`); `decide` refuses while transferring (`track.rs:360-362`) | had |
| Commit index stays classic; fast choices applied by the leader only (b20c5cd) | `raft.rs:1583-1595,3095-3104` | classic commit plus `fast_commit` moves `committed` for fast-quorum-held indexes (`raft.rs:822-837`; `track.rs:413-452`); modelled in `FastTrack.tla` | different design, both modelled |
| MLRaft (cf76129) | `multilog.rs` | independent ledgers (doc 27 §5) | had (design) |
| CheckQuorum every base period; leader steps down without a majority heard | `raft.rs:2529-2549`, `timing.rs:361-369` | `tick_heartbeat` → `MsgCheckQuorum` (`raft.rs:1035-1050,1421-1427`), `quorum_recently_active` (`progress.rs:391`) | had |
| A removed leader steps down when its removal commits (fefb31f) | `raft.rs:1490-1506` | refused by the shell (`lib.rs:1103-1113` `LeaderLeaving`); applies → hands to the heir and follows (`raft.rs:1937-1957`) | had (stronger) |
| ReadIndex one round at a time | `raft.rs:2894-2942` | `read_index`/`ReadOnly` bounded by `pending_reads` (`raft.rs:1484-1547`) | had |
| Bounded queues (`Limits`) | window budget bytes | `Limits` (`raft.rs:31-69`) | had |
| Election state exposed (b036a0e) | `raft.rs:415-462` | none in the core (grep) | absent (C22) |

---

## 4. Ranked port list

Ordered by expected wall-clock gain at fleet scale, then memory. "Test first" names the failing test to write before the change. Doc 27 rows to touch are named.

1. **C10 — Hand leadership off on a planned stop.** Gain: one election timeout per led group per restart (1 s floor; up to `election_tick × tick_ceiling` = 20 s across the planet), multiplied by the sessions a node leads. Files: `crates/focal-node/src/fleet.rs` (`begin_stop`, the owner's stop branch at 1704–1730), `crates/focal-node/src/control_host.rs` (`stop`, 663), `crates/focal-node/src/host.rs:116`, `crates/focal-raft/src/raft.rs` (lift the heir rule at 1942–1954 into a `most_caught_up_voter`), `crates/focal-consensus/src/lib.rs` (`transfer_leader`, 827). Tests first: `tests/stop_handoff.rs` (real processes, SIGTERM the leader of three sessions; a successor before one election timeout at the group's pace; the stop within `SHUTDOWN_DEADLINE`); `fleet_stop_tests.rs` for the owner rule; a core test that a transfer started while stopping still aborts at `election_tick`. Doc 27: §3.3 new row "A planned stop costs an election", §5 "Leader transfer" paragraph, §8.4. Constants: the wait is two election timeouts at the pace — derived; none new.

2. **C3 — Consensus messages refused `Busy` on the per-peer lane, dropped and unobserved.** Gain: elections under load stop losing whole timeouts; a silent drop becomes a metric. Files: `crates/focal-wire/src/peers.rs` (lane rule at 798–808; a consensus lane beside `probes` at 157–158, 935; or an awaited acquire bounded by `round_budget` for `TrafficClass::Consensus`), `crates/focal-wire/src/transport.rs:700-701,754-759` (the connection's two control permits — derive from `streams_per_connection` rather than 2 **[constant: 2]**), `crates/focal-node/src/replication.rs` (export `saturated`/`lost`; `report_unreachable` on a lost append), `crates/focal-node/src/metrics.rs`. Tests first: pool test "a vote request is delivered while two appends to the peer are outstanding"; `sim_election_tests` with two groups on one pool and a follower whose persistence is slow. Doc 27: §3.1 P1 note on what a round drops; §8.4 new row.

3. **C4 — A round with nothing gathered waits its whole deadline.** Gain: WAN placement rounds stop cutting off answers between ¾ and 1 of the tail; tails stop being doubled for it. Files: `crates/focal-timing/src/round.rs:153-165` (or `RoundWait::judge` 186–194), `crates/focal-wire/src/round.rs` tests 192–200. Tests first: `a_round_with_no_answer_ends_at_its_deadline` (100 ms), `an_answer_in_the_last_quarter_is_collected` (90 ms → `Enough`). Doc 27: §3.1 P1 ("judged at three quarters" → "a round with nothing gathered is given its whole deadline"); doc 09 2026-09-28 entry's "75 ms" line. Constants: none.

4. **T11 — Raise quinn's MTU-discovery bound, derived.** Gain: CPU/syscalls per megabyte on jumbo LAN and loopback bulk (custody, seeds); none on WAN. Files: `crates/focal-wire/src/transport.rs:175-207` (`mtu_discovery_config`, `MtuDiscoveryConfig::upper_bound`; keep `interval`/`black_hole_cooldown` defaults). **[constant]**: derive the bound from the interface MTU or use the UDP maximum with the probe cost stated. Tests first: `tests/congestion.rs` scenario with `Path::with_mtu(9_000)` asserting datagrams sent per megabyte fall; a thin-link scenario (1 Mbit/s) asserting the ping p99 does not rise with the raise's rechecks; confirm quinn-udp sets DF on all three platforms (T13). Doc 27: §7 "The transport as measured" gains a row; §8.2 "path MTU search … Not taken" → enabled.

5. **C6 — Derive the pipelining depth per peer from the measured tail** (after 2). Gain: bytes and tail latency under WAN loss (slates: 14 % fewer bytes; p99 458 → 321 ms at 1 % loss); memory: queued frames bounded by the path. Files: `crates/focal-consensus/src/lib.rs` (`max_inflight_messages` per group → set from the pace each period, as `set_patience` is, `lib.rs:814-817`), `crates/focal-raft/src/progress.rs` (`Inflights::new(cap)` resized on a pace change, 30–37), `crates/focal-node/src/fleet.rs`/`control_host.rs` pace publish. **[constant: REPAIR_ROUND_TRIPS = 2]**, derived in slates as the refusal back plus the resend out — carry the derivation. Tests first: `sim_election_tests` proposal stream at 1,000/s, 80 ms, 1 % loss: bytes on the fabric and commit p99 before/after. Doc 27: §1 table "Pipelining, inflight window: 128" row; §8.4.

6. **C8 — Compaction by the thesis size rule.** Gain: memory/disk: the WAL above the floor proportional to the state; fewer snapshot installs for a lagging follower. Files: `crates/focal-node/src/control_host.rs:1748-1777` (`maybe_checkpoint`), `crates/focal-control/src/replica.rs` (snapshot size accessor), the session `checkpoint_if_due` (`fleet.rs:1703`, rule to read first). Tests first: `tests/control_host.rs` with 4 MiB entries — a checkpoint after one snapshot's worth of bytes, not 1,024 entries; a follower one round behind is not sent a snapshot. Doc 27: §8.4 new row. Constants: factor 1 and "held for followers 2" are the thesis's and slates' derivations — cite both.

7. **C22 — Election diagnostics.** Gain: diagnosis (it is how slates found C1, C3 and C4). Files: `crates/focal-raft/src/raft.rs` (`step_vote` 1335–1392, `poll` 1172–1191), `crates/focal-consensus/src/lib.rs` diagnostics, `crates/focal-node/src/metrics.rs`, `fleet_diagnostics.rs`. Tests first: `raft_safety_tests` asserting the counters after a refused pre-vote of each reason. Doc 27: §5 diagnostics sentence.

8. **T9 — Measure reordering.** Gain: only on reordering paths; unknown until measured. Files: `crates/focal-wire/tests/congestion.rs` (a jitter/reordering scenario over `Path::reordering`). Decision after: upstream quinn-proto (adaptive), or a derived static threshold **[constant]**. Doc 27: §7 grid table.

9. **C13 — Fast-track hole schedule.** Gain: a correctness fact for stage E; nothing in service. Files: `crates/focal-raft/tests/fast.rs`, `tests/support/cluster.rs` (`settles` to assert every fast proposal applied or displaced). Doc 27: §4.6 if a rule changes.

10. **C15/C20 constants.** `unreachable_cooldown` 2 s, liveness `period` 1 s, `base_timeout_ms` 300, `timeout_cap_ms` 2,000 (`peers.rs:59`, `driver.rs:78-93`) are set where slates derives its reach-out and probe deadline from the measured window and round trip. No wall-clock gain expected on a healthy fleet; a derivation batch for the liveness suite, measured with `liveness_tests` under `focal_sim::path`.

Declined with reasons: C2 (rank is policy; hand-off had), T5 (not settable in quinn; measured under quinn's pacer already), T6 (no API; memory note recorded), C12/C14 (same decisions already taken), C16/C17/C18 (not focal's shapes), R1–R3, T1–T4, T8, T10, T12–T24 (quinn/tokio provide them; doc 27 §3.2, §8.2).

---

## 5. Defects in focal that slates' history suggests

| slates class (commit) | focal analogue | State | Evidence |
|---|---|---|---|
| A dispatch with nothing gathered stops at ¾ of its deadline (b036a0e) | `DeadlineExtender::evaluate` / `RoundWait::judge` | **Present**; scope: `gather` callers (placement facts) | `focal-timing/src/round.rs:153-165,109-114,72`; `focal-wire/src/round.rs:101-106`, test `:192-200`; caller `placement_agent.rs:2710-2715` |
| A campaign asks no one because the only live voter's session is out (38c987e) | a Raft message refused `Busy` on the 2-permit per-peer lane, dropped, uncounted in metrics, core not told | **Present in focal's shape**; frequency unmeasured | `transport.rs:700-701,754-759`; `peers.rs:53,798-808,860`; `replication.rs:126-146,80-85`; no metric (grep) |
| Late pre-vote grants dropped (b036a0e) | replies are inbound messages | Absent | `raft.rs:1692-1707` |
| Election jitter correlated across attempts (b3b6712) | per-node splitmix64 stream, `getrandom` seed | Absent | `raft.rs:2014-2028`; `lib.rs:1481-1492` |
| Pre-vote lease outlives the minimum election timeout (cf76129) | lease = base tick | Absent | `raft.rs:1278-1286` |
| Receive-window ceiling from a peer-count floor (~135 KB; doc 27 §8.3) | 1 MiB per stream, 18 MiB per connection, from configuration | Absent as such; **at risk**: quinn's buffers are outside `MemoryBudget` (128 connections × 18 MiB) | `transport.rs:58,177-194`; `peers.rs:52`; `network_service.rs:660` (budget covers frames, not quinn) |
| forget_stream with a reply in flight (837142a) | bidi streams; reset only on failed delivery | Absent | `transport.rs:515-526` |
| EADDRINUSE on restart; bind-release-rebind race (42ebbbb, 0fe5160, 5836a8c) | bind in `start`; ports claimed in tests | Absent | `network_service.rs:652-655`; doc 27 §8.2 |
| Copa freezes an overshot window (8907c6f) | growth guard only | Absent | `congestion.rs:303-307`, test `588-601` |
| Idle peer's ack-eliciting credit inflates RTT (0def3b4) | quinn subtracts ack delay | Absent | quinn `connection/mod.rs:1541-1551` |
| Symmetric partition never heals (5836a8c) | every member probed whatever its status; Dead → Revived on answer; route kept | Absent | `driver.rs:768-794,1428-1431`; `network_controller.rs:1480-1513` |
| Gossiped seed death strands an unreached peer (f108433) | committed node ids; unconfirmed patience | Absent by identity model | `driver.rs:1356-1361,1512-1518` |
| Commit rule scans the backlog; configuration lookup scans the log (8119f07) | quorum over members; `pending_conf_index` | Absent | `progress.rs:319-341`; `raft.rs:240,1192-1207` |
| Late append lands compacted entries (84b218a) | answers with the commit | Absent | `raft.rs:1826-1830` |
| A member that missed its promotion refuses every election (2026-09-29) | vote without own-config check | Absent | `raft.rs:1335-1392` |
| Priority vetoes a transfer (focal's own, 2026-09-28) | `transfer || ahead || priority` | Absent (fixed) | `raft.rs:1349-1359` |
| A recovery reads only its own window's reach (8119f07) | all reports read | Absent (own design) | `raft.rs:1127`; `track.rs:480` |
| Window slots dropped before a classic commit lose chosen values (b20c5cd) | released at commit; modelled | Absent (modelled in `FastTrack.tla`) | `raft.rs:832`; doc 27 §4.4 |
| A hole below a fast vote never filled (462b63d) | leader takes the first heard; a hole below a vote waits for a proposal | **Unclear** | `track.rs:359-393`; `tests/support/cluster.rs:459-484` |
| A learner that never catches up pins the change (3316fc0's abort) | agent retries `Behind` | **Unclear** (plan deadline not read) | `placement_controller.rs:887`; `lib.rs:1112-1123` |
| Probe copies grow one tracked packet per PTO (4a3f6d7) | quinn moves the frames | Absent | quinn `spaces.rs:135-141` |
| Packets past the 1,200-byte floor (2e60c2f) | quinn builds to the MTU | Absent | quinn `config/transport.rs:384-385` |
| Reassembly quadratic in holes (8700a7f) | quinn's heap; the 1,024-chunk bound handled by `STREAM_WINDOW_CEILING` | Absent | quinn `assembler.rs:13-15,361`; `transport.rs:45-58` |
| Handshake flight larger than a datagram (42d5178, d94d1cc) | CRYPTO frames | Absent | quinn `config/transport.rs:397` |
| Spurious loss on a reordering path (797fde3) | fixed thresholds inside quinn | **Unmeasured** in focal | quinn `config/transport.rs:381-382`; no reordering scenario in `tests/congestion.rs:98-110` |
| Lifeguard probe cadence not dilated (78547ef) | LHM multiplies timeouts; cadence dilation **unclear** | Unclear | `health.rs:39-41`; `driver.rs:342` |

---

## 6. Summary

1. Of 25 transport/runtime items slates landed since 2026-09-10, quinn/tokio already provide 18 and focal already had 5 (Copa, its shrink fix, classes, fresh streams, the bake-off); the one thing to enable is quinn's MTU discovery beyond its 1,452-byte default (T11), with a derived bound.
2. focal's Copa differs from slates' in two rules focal added by measurement (doubling judged by what was sent after the last doubling; a stride of 2) and in two inputs it does not control (quinn's `app_limited` at ack time instead of "window full since the acked packet left"; quinn's 1.25·cwnd/srtt pacer instead of 2·cwnd/RTTstanding). Everything else is the same law line for line.
3. **Defect present:** focal's `DeadlineExtender` ends a round that gathered nothing at three quarters of its deadline — slates' 2026-09-29 bug, pinned by focal's own test (`focal-wire/src/round.rs:192-200`). It affects placement fact rounds (`placement_agent.rs:2715`), not elections.
4. **Defect candidate:** a Raft vote or append is refused `Busy` whenever two Raft exchanges to that peer node are outstanding across all groups (two permits, `try_acquire`), the frame is dropped, the drop is not exported and the core is not told — focal's shape of slates' "campaign asked no one". Cost: an election timeout per refused vote.
5. **Highest-value port:** hand leadership off on SIGTERM/planned stop. focal transfers only on drain/remove; every restart costs each led group a full election timeout (1 s floor, up to 20 s at the pace ceiling), times the sessions a node leads.
6. Already had, with citations: pre-vote lease at the minimum timeout, independent jitter, late pre-vote replies never dropped, O(1) commit, conflict hints and one-refusal empty-follower probing, learners promoted only at the commit, transfer semantics, CheckQuorum, removed-leader step-down, MLRaft's design, symmetric-partition healing, seats.
7. Pipelining exists (128-message window) but its depth is not derived and the wire depth is really two exchanges per peer node with a persistence-coupled reply; fix 4 before deriving the window (C6).
8. Compaction triggers on a 1,024-entry count where slates uses the thesis's size rule; a derived rule bounds the WAL relative to the state (C8).
9. Priority as measured RTT is rejected as a replacement for placement ranks; the hand-off it buys is already `leader_return`.
10. The fast track: same decision on both sides (built, closed, measured); slates' Azure numbers corroborate focal's simulation; one schedule (a hole below a fast vote) is worth writing.
11. Adaptive reordering tolerance lives inside quinn-proto; measure a reordering scenario first, then decide upstream vs. a derived threshold.
12. Memory note: quinn's per-connection receive buffers (18 MiB × up to 128 connections) sit outside `MemoryBudget`.
13. Slates' remaining constants that focal shares as set values: liveness period 1 s, probe base 300 ms and cap 2 s, `unreachable_cooldown` 2 s, control permits 2 per connection, `checkpoint_interval` 1,024, `max_inflight_messages` 128.
14. Diagnostics: focal's core counts no pre-vote refusals by reason; slates found three of its bugs from those counters (C22).
15. Doc 27 rows to update if the ports land: §3.1 P1 (the ¾ rule), §1 table (inflight window), §3.3 (planned stop), §7 (MTU), §8.2 ("path MTU search: Not taken"), §8.4 (new open rows).
