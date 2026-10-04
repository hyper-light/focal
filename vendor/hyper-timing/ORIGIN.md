# hyper-timing: origin

- **Source.** focal's `crates/focal-timing` at focal `a8e95f7` (`origin/slates-port`), brought in
  with its history by `git subtree`.
  - The imported tree hash equals focal's `a8e95f7:crates/focal-timing`
    (`01bdb7575584338d8b857535fc5ca68ae061e613`).
  - It holds the election-timing law (`TickPace`), both path estimators (`PathRtt`, the median and
    MAD of the latest 16; `ExchangeRtt`, RFC 9002 §5.3), and progress-charged waits
    (`ProgressDeadline`, `RoundBudget`, `DeadlineExtender`, `RoundWait`).

## Changes

1. **Package `hyper-timing`.** It inherits the workspace lint wall.
2. **No clock is read.**
   - `ProgressDeadline::begin` and `ProgressDeadline::check` read `Instant::now()` and are removed.
   - Callers use `begin_at` and `check_at` with their own clock, as the sans-io rule requires
     (CLAUDE.md §1). focal's call sites change when it takes the vendored copy.
3. **Documentation.** The 22 public items the union wall's `missing_docs` found are documented.
4. **Test opt-out.** The crate-root test block also allows `cognitive_complexity`. Four test
   functions exceed the shipped-code threshold, and no shipped function does.

5. **slates' timing law joins the crate** (`src/election.rs`, from slates
   `crates/cluster/src/timing.rs` at `c4e2c52`).
   - Ported: `ElectionTiming` (derive, floor, window_budget, timeout_periods), `ElectionTimer`,
     `FollowerStep`, `ElectionPriority` (from slates' `raft.rs`), `quorum_priority` and
     `REPAIR_ROUND_TRIPS`.
   - The derivations take any `PathEstimate` (`PathRtt` or `ExchangeRtt`), so the choice of
     estimator for election timing is a measured input, not a fork.
   - `ElectionTiming::derive` takes `durable_tail_ns`: the durable-acknowledgement time the path
     samples lack (mantle audit §11.7). It is added once a path is measured.
   - Local ids are `u64`.
   - slates' 15 tests are ported on `ExchangeRtt` and pass unchanged in their numbers. That
     confirms focal's `ExchangeRtt` and slates' `RttEstimator` compute the same RFC 9002 values.
   - New tests: the durable term, the law on either estimator (three late answers of 16 move the
     median-and-MAD base nowhere and the smoothed base past 100 periods), and
     `ElectionPriority::outranks`.
6. **One round-budget law.** `RoundBudget::derive(&RoundAnchors, tail_ns, ceiling_ns)` replaces
   focal's `derive(period, tail, ceiling)` and slates' `round_budget(anchors, tail)`.
   - From focal: the caller's ceiling bounds everything; an unmeasured round gets the whole ceiling,
     hard; and extensions were capped at `ELECTION_MARGIN` and at the room left under the ceiling
     (the first cap went in change 9).
   - From slates: the stall window is `stall_periods` periods (derived from the SWIM suspicion span,
     where focal fixed it at two), the lookahead and poll rate come from the anchors, and
     `RoundAnchors::poll_interval_ns` is kept.
   - focal's tests pass with its anchors (stall 2, lookahead 3/4).
   - slates' measured cases are identical under a ceiling of `max(heartbeat, tail) +
     ELECTION_MARGIN × heartbeat`.
   - slates' unmeasured case changes from one period to the whole ceiling. A round against a peer
     nothing is known about is never cut off before the caller's bound; slates' own WAN-round bug
     was such a cut-off.

7. **Reads are fields, and nothing allocates** (`CLAUDE.md` §1a; `docs/benchmarks.md`,
   "hyper-timing").
   - focal's `PathRtt` sorted its window on every read: `smoothed_ns` once, `variation_ns` twice,
     so a tail read sorted three times (73 to 88 ns) and the election timing, the priority and the
     tick pace each sorted every path several times. The window is now kept in order as samples
     arrive (the evicted sample out and the new one in, two shifts), the median is read by index,
     and the median absolute deviation is found by merging the two sides of the median outwards to
     the middle. Both are kept as fields, so a read is a load. Results are identical to sorting
     (a test compares them over 12,000 samples, ties included).
   - `quorum_priority` collected the measured paths into a vector and sorted it. It now counts each
     path's rank among the others, which allocates nothing; a test compares it with the sort over
     2,000 groups.

8. **The detector, new in hyper-raft** (`docs/timing.md` §2.2–§2.6, step L-1).
   - `src/qos.rs`: Theorem 7's bound, the configurator minimizing unavailability within the
     measured floors, the margin at a given interval, and the split-vote span.
   - `src/link.rs`: `LinkEstimator`, NFD-E's expected arrival over the window `min(n_G, n_A)`
     computed online under a drift bound derived from RFC 5905's `PHI`, the prediction errors'
     variance over the history, the Jeffreys loss, the unseen-delay term, freshness and suspicion.
   - `src/folds.rs`: the timer-lateness, flush and exposure folds.

9. **The election law from measurement** (`docs/timing.md` §2.3, "The election law"; L-1).
   - `ELECTION_MARGIN`, `PATH_WINDOW` and `GRANULARITY_NS` are gone.
   - `Ballot` measures a group's election from the voters' paths: the one-way latency (half the
     slowest mean round trip), the vote round (the quorum's mean round trip plus the mean flush)
     and the broadcast tail; `Ballot::span` is `election_span` on it.
   - `ElectionTiming::derive(period, &Detector, &Span, &Ballot)` replaces `derive(heartbeat_ns,
     durable_tail_ns, paths)`: the base is the configured detector's `η + α`, the span `W`, both in
     whole periods, with the detection bound, `T_E` and the broadcast tail beside them.
     `ElectionTiming::floor` is gone: nothing measured, no timing (`docs/timing.md` §3, item 10).
     `ElectionTiming::delay` draws the same jitter in time on `[0, W)`.
   - `TickPace::derive(configured, ceiling, election_tick, &ElectionTiming)` replaces the paths
     argument; its period covers the longer of the base and the span. `broadcast_tail_ns` becomes
     `covered`.
   - `PathRtt::new(correlation, interval)` derives the window, `2·max(1, ⌈T_c/p⌉) + 1`, and allocates
     its two buffers once; it is no longer `Copy` or `Default`. `PathRtt::mean_ns` is new.
   - `tail_ns`, `spread_ns` and `quorum_priority` take the measured granularity; `PathEstimate` has
     `mean_ns`.
   - `RoundBudget::derive` no longer caps extensions at `ELECTION_MARGIN`: the ceiling bounds them.
   - `quorum_priority` stops counting a rank once it passes the position, and ranks the paths on
     their spread without the granularity floor, applied once to the path chosen (the floored
     spread never falls as the unfloored one rises, so the choice is the same): even with the law
     before on the same samples (`docs/benchmarks.md`, "hyper-timing").
   - Tests: the window against sorting at every derived window and against a stall of `k` and
     `k + 1` (property), the ballot's latency, round and availability, its refusals, the span as the
     ballot's `election_span`, the base in whole periods and its rounding (property), the delay's
     range (property) and uniformity, the base lapsing at the period the link estimator suspects,
     the priority unmoved by a stall, slates' timer tests on the derived timing; and the allocation
     law over every operation of the law (`tests/alloc.rs`).

## Planned

- **The estimator that feeds the priority** stays the median (`docs/timing.md` §2.6, item 7); the
  base and span no longer read a path's tail. Whether `PathRtt` or `ExchangeRtt` feeds the ballot's
  means is for the timed simulation in note 32 §3.7, with the judged outputs fixed before the run.
