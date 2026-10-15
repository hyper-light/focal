# hyper-transport: where it comes from

Step T-1 of mantle note 32 (§3.4, §5.2): the application layer over hyper-quic. Its generic core is
focal-wire's, read at focal `99191da` (branch `slates-port`), ported rather than depended on: focal-wire
is an async crate over quinn and tokio, coupled to focal's domain, and this crate is a sans-io state
machine over hyper-quic. slates' session plane (`slates/crates/transport/src`, `5cce86a`) is the other
source: its credit law (`flow.rs`, `connection.rs`) is taken here. Neither repository was changed.

## What each module took, and from where

| Module | Source | What changed in the port |
|---|---|---|
| `admission.rs` | focal `admission.rs` (27 §3.1 P5; the audit's F20) | One owner instead of `Arc<Mutex<State>>`; connections are keys, not quinn handles. The pending places, the replacement of an identity's idle connection used longest ago (refused `Busy` with none idle, focal's fix of 2026-10-09, ported 2026-10-10), max-min fairness at the connection bound, the connection bound met after the replacement rule, and the identity bound are focal's; idleness is counted from the exchanges and lane frames the endpoint sees, with the serve period as the quiet focal took from its request timeout. focal's fixed 4 per node and 16 per participant are excluded (note 32 T9): one configured `per_identity`. focal's per-identity ingress share (F03) is the budget's, through `Budget::reserve` before a head is read. |
| `frame.rs` | focal `frame.rs` (`read_frame_header`, F03, F52) | The fixed header read first and checked before any byte past it is reserved or read is focal's. The layout is new: a kind, a head and an optional streamed body, with CRC-32C over prefix and head and a body trailer (mantle CLAUDE.md §6), where focal carried one postcard payload under a magic. |
| `progress.rs` | focal `transport.rs` `carried`, `frame.rs` `read_payload_arriving` and `residency` (note 32 T39) | The same law, judged by the caller's clock: asking, a period that sends less than a datagram or that began with everything owed sent ends the exchange; answering, a period must bring a datagram or the body's end, and one that began with everything the peer declared of the body's class and the less urgent ones delivered ends it (focal's residency is gone; see below). focal's `Held` atomic is the endpoint's own sum over its exchanges. |
| `round.rs` | focal `round.rs` (27 §3.1 P1, note 32 T45) | `gather` over `FuturesUnordered` becomes `Round`, which the owner reports to and judges at its next judgement, over hyper-timing's `RoundWait`. focal's 1,024-peer bound is gone: the round holds counts, no table. |
| `timing.rs` | focal `peers.rs` (`Exchange`, `exchange_tail`, `spread`; T40, T41) | The estimator doubles per abandoned exchange without focal's cap of six (note 32 T40); `spread` is focal's F64 unchanged. |
| `credit.rs` | slates `connection.rs` `class_credit_reserve`, `stream_bytes_per_packet`; slates `flow.rs` autotune; focal `transport.rs` `STREAM_WINDOW_CEILING` (T16, T17, T33) | The reserve is slates' law over QUIC's own packet layout (RFC 9000 §17, §19.8; RFC 9001 §5.3). The window starts at RFC 9002's initial window and doubles by Chromium's rule, each growth reserved from the budget. focal's 1 MiB stream ceiling, set after its 10 MiB window closed connections, is replaced by its derivation from the assembler's span limit, now public in hyper-quic (patch Q5). |
| `endpoint.rs` | focal `transport.rs` (`QuicServer`, `QuicRemote`, `RouteConnections`), `peers.rs` (lanes, single-flight dial, retirement) | One endpoint owns everything: no `Arc`, `Mutex`, `Semaphore`, task or channel. focal's per-exchange tasks are exchanges in a generational table; its lane semaphores are QUIC's stream limits plus a waiting state judged by the exchange's deadline (T37); its dial cache is the peer table (T42); a retired route closes its connections and refuses their exchanges at once (T43). The class is decided by the kind and the sender's role (audit §13.3), never by a stream ID. New: strict priority is kept where credit is taken, not only in QUIC's send order (below). |
| `lane.rs` | node.md §3.2; focal `peers.rs` lanes (T37) | A lane is a long-lived unidirectional stream per (peer, lane) carrying frames in order, always read; a frame its class or the budget cannot take is skipped and counted. |
| `tls.rs` | focal `transport.rs` `server_tls`, `client_tls` (T49) | Mutual TLS 1.3, 0-RTT off, over hyper-tls's owned configurations and `&'static` provider. |

## Tests ported

| Test here | Origin |
|---|---|
| `admission::tests::pending_places_are_bounded_and_given_back` | focal `tests.rs` `admission::pending_places_are_bounded_and_given_back` |
| `admission::tests::a_full_endpoint_still_replaces_an_identity_s_own_connection` | focal `admission::a_full_listener_still_replaces_an_identity_s_own_connection` |
| `admission::tests::an_identity_past_its_bound_loses_the_connection_it_used_least` | focal `admission::an_identity_past_its_bound_loses_the_connection_it_used_least`, and `a_node_past_its_bound_replaces_its_oldest_connection` |
| `admission::tests::identities_are_bounded_and_one_identity_cannot_take_another_s_place` | focal `admission::identities_are_bounded_and_one_identity_cannot_take_anothers_place` |
| `tests/exchange.rs` `a_certificate_the_directory_does_not_know_is_refused_and_charged_to_no_one` | focal `admission::a_certificate_that_does_not_authenticate_is_charged_to_no_identity` |
| `tests/exchange.rs` `a_budget_that_cannot_fund_a_head_refuses_it` | focal `admission::a_body_is_permitted_before_it_is_allocated_within_the_identity_s_share` |
| `tests/exchange.rs` `a_message_past_its_class_bound_is_refused_from_its_prefix` | focal `tests/adversarial.rs` `oversized_and_malformed_frames_reject_before_allocating_the_declared_length` |
| `frame::tests::a_malformed_prefix_is_corrupt` | the same test's bad-magic and truncated cases |
| `round::tests::*` (9 tests) | focal `round.rs` tests, one for one, on the caller's clock instead of tokio's paused one |
| `timing::tests::a_peer_pause_is_spread_over_its_second_half` | focal `tests.rs` `a_peer_pause_is_spread_over_its_second_half` |
| `timing::tests::an_abandoned_exchange_doubles_what_the_peer_is_expected_to_take` | focal `peers.rs` `exchange_tail`'s doubling, without the cap |
| `progress::tests::a_slow_transfer_that_moves_is_never_cut_off` | focal `carried`'s documented law ("a megabyte is given eight seconds and more on a path that carries a megabit in a second") |
| `credit::tests::a_window_consumed_quickly_doubles_up_to_its_ceiling` | slates `flow.rs` autotune |
| `tests/exchange.rs` `a_class_reserve_keeps_control_moving_under_bulk_load` | slates bug 2026-09-30-bulk-spent-the-connection-credit-a-control-exchange-needed: fails with the reserve set to zero |
| `tests/exchange.rs` `exchanges_past_the_stream_limit_wait_their_turn_and_the_table_is_bounded` | focal `a_groups_message_waits_its_turn_on_the_lane_instead_of_being_refused` |
| `tests/exchange.rs` `frames_on_a_lane_arrive_in_order` | focal `the_lanes_of_a_connection_are_derived_from_the_consensus_window_and_the_path` (the window), node.md §3.2 (the order) |

## What is not ported, and why

- focal's domain layer (`handler.rs`, `native.rs`, `managed.rs`, `message.rs`, the registry and its
  grants): focal-wire keeps it over this crate (note 32 §3.4).
- focal's `congestion.rs` (Copa): it is congestion control, which belongs to hyper-quic's controller
  seam (note 32 Q2, T28, T29), not to the application layer.
- focal's fixed timeouts, reference path (1 Gbit/s × 100 ms) and 144 MiB windows: excluded by note 32
  (T46, T47, T48); every bound here is the owner's configuration or derived from the path and budget.
- focal's `RouteConnections` name resolution: an owner resolves names and calls `connect` with an
  address.
- The TCP fallback (T53) is not built yet.

## Departures found while porting

- **The peer's time to answer begins when it has the request** (2026-10-10). The asking phase ended
  a period after everything owed had been *sent*. On a slow path a bottleneck's queue holds the last
  window for seconds after it leaves, so the peer's period ran out before it had the request: focal's
  `a_peers_time_to_answer_begins_when_it_has_what_was_asked`, 32 KiB over 64 kbit/s answered 400 ms
  after arrival, was refused at 4.5 s, as it was in focal before its audit's F38. Once sent whole, a
  request is now charged with what the peer acknowledges (hyper-quic patch Q6), and the answer period
  begins when QUIC reports the stream acknowledged whole (`StreamEvent::Finished`;
  `a_peers_time_to_answer_begins_when_it_has_the_request`,
  `a_request_in_the_network_is_waited_on_while_the_peer_acknowledges_it`). Over focal's own shape —
  64 kbit/s each way, 32-datagram queues — that was not enough: the sender, held by the peer's credit,
  sent nothing for a period while the peer drained the queue, and under that load the round trip was
  longer than the 500 ms period, so no period could carry the peer's acknowledgements. A period is
  now charged what the peer acknowledged where that is more than what was sent
  (`a_request_held_by_the_peers_credit_moves_by_what_the_peer_acknowledges`), and a window is not
  judged sooner than the path's probe timeout (hyper-quic patch Q7;
  `a_window_is_judged_no_sooner_than_the_probe_timeout`).
- **A stream its peer does not read** (2026-10-10, F38's other half). One the peer accepted and
  never read took the peer's connection credit, and every exchange of its class on the connection
  waited for credit behind it and was refused (focal's
  `a_stream_its_peer_does_not_read_ends_whatever_else_its_connection_carries`); since the answer
  period began from the acknowledgement, the unread one itself lived as long as the others moved
  (over a lossy path, longer than the twenty minutes a run was given). Out of credit, a datagram of
  it and more sent and all of that acknowledged (hyper-quic patch Q8), it is now unread and ends a
  window later; an exchange waiting only for credit an unread one holds, or held in its window, is
  held, and goes on as the credit comes back
  (`a_stream_its_peer_never_reads_ends_by_its_own_bytes_and_the_others_go_on`).
- **What is sent into silence is not progress.** focal's `carried` charged a period with the bytes
  the connection sent and did not find lost. After a peer dies the sender still sends: the flight in
  the air, then the probe timeout's probes, a datagram or two each at a doubling backoff, so every
  period that held a probe kept the exchange alive and the end depended on where the backoff fell (a
  killed peer's upload ended after two periods on one run, four on another). A period now also has to
  have heard the peer (`progress.rs`; `what_is_sent_into_silence_is_not_progress`).

- **An answer is charged with what arrives, not priced by the path.** focal gave a payload its
  residency, its bytes at two datagrams of the least size a round trip (a probe timeout since focal's audit F36)
  , and a wait at least: a sender limited by its path, which holds where a payload is written
  whole and read on a stream of its own. Here the peer's owner writes a body as it has it and the
  exchanges share the connection, so a body that took longer than a period on a path of microseconds
  was refused while it moved: an 8 MiB bulk reply at 5.6 and 5.5 MB read (ubuntu-24.04 at
  `ac10f6f`, windows-11-arm at `35d35d8`, its sender CPU-bound on a loaded runner, 2.7 MB/s against
  the 6 MB/s that two datagrams per 395 µs assume), and a 64 KiB reply queued behind fifteen others
  at none read while its period brought 464 KB (macOS, three runs at once under background QoS). The
  wait is now charged with the bytes the connection delivered of its class and the less urgent ones,
  against what the peer declared of them, as the asking phase is charged with what it sent
  (`a_reply_written_slower_than_the_path_carries_is_not_refused_while_it_arrives`,
  `a_reply_queued_behind_the_peers_others_is_not_refused_while_they_arrive`,
  `a_body_the_peer_withholds_while_it_sends_others_is_given_up`).
- **Strict priority where credit is taken.** quinn orders what is buffered by stream priority, but
  connection credit is charged when an application writes. An owner that wrote its bulk body first
  starved its own requests of credit: the end-to-end run found request bodies stalled behind an 8 MiB
  bulk body (`tests/e2e.rs` `exchanges`). The first rule counted a more urgent exchange as waiting
  only once its owner had been refused a write, so an owner that wrote bulk first still let the bulk
  body take a whole window before its requests took a byte (30,332 bytes in
  `requests_behind_a_bulk_body_take_credit_first_from_a_slow_owner`). A class now takes only the
  credit left after the bytes every more urgent class has declared and not sent (`Core::demand_above`).
- **Writable means the stream can take more.** A body refused by its stream's own window, with
  connection credit to spare, was told `Writable` again at once, and an owner driven by queued
  events spun without reading the window update; it now waits for QUIC's `Writable` for the stream
  (`a_body_blocked_by_its_stream_window_hears_no_writable_until_quic_says_so`).
- **Received datagrams are cut from a bounded, charged pool** (`receive.rs`): at most
  `receive_chunks` chunks of 65,527 bytes, each reserved from the budget; past them a datagram is
  copied into a buffer of its own (`unread_datagrams_pin_no_more_receive_chunks_than_the_bound`).
- **This side's delays are not the peer's.** A period in which an exchange's own owner offered
  nothing the peer would take, or a more urgent class of this side took the credit, is held rather
  than judged (`Core::ours_to_move`; `an_owner_late_to_write_is_not_refused_by_its_own_side`).
