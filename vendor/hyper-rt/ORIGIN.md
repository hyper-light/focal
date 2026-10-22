# hyper-rt: where it comes from

slates' runtime, taken at slates `6b9ce5c` (2026-10-05) and designed onward for all three consumers
(docs/runtime.md). slates was not changed; it moves onto this crate on its own schedule.

## What was taken

| Here | From slates | What changed in the port |
|---|---|---|
| `src/*.rs` (runtime, shard, registry, parking, task, queue, timer, driver, kqueue, epoll, iocp, afd, netsys, udp, tcp, readiness, futures, sim, attribution, thread_clock, waker, control, error) | `crates/rt/src` | Paths renamed (`slates_mem` → `crate::mem`, `slates_machine` → `crate::machine`). `uring.rs` not taken: its io_uring driver runs in readiness mode only, a second path to epoll (docs/runtime.md §3.7). `RuntimeConfig::from_profile` became `from_calibration` (below). `UdpSocket::dont_fragment` added, so the don't-fragment tests read the option through the crate instead of an `unsafe` call of their own. |
| `src/mem/{error,handle,loom_bounds,mpsc,ring,segmented,slab}.rs` | `crates/mem/src` | Only what the runtime uses. The arenas, the buddy allocator, budgets, locked and shared regions and pre-faulting are slates' storage, not a runtime, and are not taken: the runtime locks and pre-faults nothing. |
| `src/machine/{bench,clock,derived,error,facts,placement,stats,wake}.rs` | `crates/machine/src` | `serde` derives and the JSON profile removed; the BLAKE3 identity hash removed (a consumer keys its stored calibration as it chooses, docs/runtime.md §10.2). `Placement::of` takes the consumer's reserved cores in place of slates' fixed one for control and the OS (§10.3); the wake probe's waker runs on the first reserved core, or a sibling shard's when none is reserved. |
| `src/machine/probes.rs` | `crates/machine/src/probes.rs` | Only the system-call probe and thread pinning. The fault, memcpy, hash, codec, lock-capacity and core-matrix probes size slates' storage and are not taken. |
| `src/machine/calibration.rs` | new, in place of slates' `profile.rs` | The three measurements the runtime needs and the constants derived from them under the consumer's policy (docs/runtime.md §10). |
| `tests/*.rs` | `crates/rt/tests` | Paths renamed. `timer_allocations.rs` counts through hyper-measure's counting allocator; slates' own check that its allocator does not attribute another thread's allocations is hyper-measure's to test, and is not carried. The `rt_bench` example and the callgrind bench are not taken; docs/runtime.md §12's benchmarks replace them. |

## The suite at the port

`cargo test -p hyper-rt` on macOS arm64, Rust 1.98.0, before any of docs/runtime.md's departures:
91 unit tests, 70 integration tests in 22 files, 6 doctests, all passing. The departures are made
against this suite.

| `src/udp/linux.rs`, `src/udp/macos.rs`, `src/udp/batched.rs` | hyper-tokio `src/sys/{linux,macos}.rs`, `src/socket.rs`, `src/clock.rs` | The batched calls and kernel stamps, moved so hyper-tokio and hyper-rt share one layer (docs/runtime.md §5.1, §14). `Batched` awaits hyper-rt's readiness instead of tokio's reactor, so a receive always asks the kernel (no `Through::Reactor`). Stamps land on the shard's clock by their age; macOS reads the age on `mach_absolute_time` (`Clock::now_ticks`), the stamp, in the kernel's ticks, carried into the process's at the kernel's rate first (`machine::clock::MachTicks`: a process Rosetta 2 translates is shown a timebase of 1/1 against the stamp's 24 MHz, docs/runtime.md §5.1). Astray reports are a seam outcome; EIO from a segmented send resends unsegmented instead of dropping. |

## Mantle's runtime work, taken here (2026-10-12)

Made in mantle's vendored copy (by the owner's direction and by Codex) and taken here from mantle's
commits as they were (`cf6e37c`, `6f05bd8`, `3743b25`, `f797bd9`, `bfdf9c2`, `bb0779e`, `bd7c0fd`,
replayed onto `8cd3604`, the revision mantle took, and merged as mantle merged them at `be8d04d`), then
merged with `main`:
- cooperative service admission and cancellation ownership (`spawn_service`; a service's cancellation
  is a level it reads, its future kept until its cleanup returns), nonblocking manual-step and readiness
  progression, typed driver-loss cleanup, context checks before each synchronization receive poll,
  checked channel slot and ticket layouts before construction, and cold, runtime-owned, bounded native
  retirement groups (`prepare_retirement`, `prepare_retirement_with_original`); TCP serving metadata
  owned from the first shard claim, accept futures keeping their guards across refused admission and
  cancellation (`docs/runtime.md` §4, §8);
- the loop under load: one kick a park, a tracking shard spinning as long as blocking costs it in CPU,
  the cheap clock read once a poll, and channels that hand values over with no lock (`docs/runtime.md`
  §3.9, mantle's `docs/design/event-loop.md` D1–D4);
- a step's poll budget covering the wakes its own polls make (hyper-raft PR #16, `ca14726`) and a
  thread's CPU account read only while attribution is armed (#15).

In the merge with `main`: the one-shot keeps mantle's lock-free state, with `main`'s
`OneshotSender::is_closed` read over it; a poll that unwound still returns its end, as mantle's step reads
it, and tells a waiting `block_on` its root unwound (`RtError::RootUnwound`, `main`'s); the service
unwind test now holds that a root's unwind ends its call refused, its peer service cancelled, released
and joined inside the call, where it held the panic passing out of `block_on`.

Made here with it, for `tests/service_resources.rs`, red in mantle since `6f05bd8`: the shard's control
queue is a ring held in its registry entry (`control.rs`, `docs/runtime.md` §8), not std's
`sync_channel`, whose lock on macOS is a pthread mutex boxed at its first lock, so a shard losing its
driver allocated as it closed the queue, inside the measured driver-loss round (backtraces:
`OnceBox::initialize` under `close_control` from `lose_driver`). The queue has no cell: hyper-rt's
channel there, the first try, put a call cycle through the registry (the entry's free dropping its
sender, whose drop wakes through the registry) that `scripts/check-recursion.py` refused on every
target (CI run 38282201776). The test's harness moved to hyper-rt's channels (std's also grew a blocked
receiver's waiter list, `mpmc::waker::Entry`); with both, no hot round allocates. One observer thread
serves every driver-loss round, as one serves every reuse round: a thread started per round began inside
the hot window it was started for, and its first stack pages counted (4 of 10 runs red, 1–2 faults; a
64 KiB stack touch at its start made it 20 of 20, 3–4 faults a round; with one observer 30 of 30 green,
and 20 of 20 with that touch).

Also made here with it, each found on CI or by widening a window the scheduler opens on a busy host:
- A receiver's drop closes its ring, a mark in the tail, so a send claims its place before the close or
  is refused `Closed` (`docs/runtime.md` §8). mantle's channel read a state bit before the push, and a
  receiver's drop could free a full ring's room between the read and the push (`tests/sync.rs`).
- `readiness_waits`' wait dropped on another shard takes the slot back once a later step has polled the
  task, where it took one yield for a control drain: a step polls a task that yields again within its
  batch (PR #16).
- `service_driverless_receipts` counted hyper-measure's Linux probe as the runtime's: the probe read
  `/proc` through `format!` and `read_to_string`; it reads into a buffer on the stack now.
- `service_resources`' observer yields once before any window: its loop yields only when the shard has
  not parked yet, and its first yield, in a random measured round, ran kernel32!SwitchToThread for the
  first time in the process, which Windows counts as a page fault (8 runs in 40 on windows-2025, the
  page named by Windows' working-set watch and cdb; 0 in 80 with the yield).

## Made here: long polls judged by step (2026-10-12)

- mantle's ranges bench found the shard reading its thread's CPU clock once at the end of every poll past
  the quantum and once at every step's start: a seek's poll ran a little past the 2.1–4.0 µs quantum, so
  727,739 reads in a million seeks, 2.9% of the shard's time in `__thread_selfusage`. From a long poll
  judged a run on the CPU, a step's long polls are judged together at one reading at its end
  (`attribution::Tracker::by_step`, `judge_in_window`): an overrun past the window's off-CPU time was
  spent on the CPU, the per-poll rule's verdict, and a window that leaves one undecided judges one at a
  time again. Reads fell to about one a step (8–9 times fewer), 0.5% of the shard's time
  (`docs/benchmarks.md`, "hyper-rt: long polls judged by step"). Tests: `tests/attribution_by_step.rs`
  (the reads beyond one a step paid for by undecided verdicts; it fails on the per-poll reading, 41
  reads for 32 long polls), two unit tests of the window and the modes; `tests/wake_estimate.rs`'s
  attribution counts unchanged.

