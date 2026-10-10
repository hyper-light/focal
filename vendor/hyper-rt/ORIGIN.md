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

| `src/udp/linux.rs`, `src/udp/macos.rs`, `src/udp/batched.rs` | hyper-tokio `src/sys/{linux,macos}.rs`, `src/socket.rs`, `src/clock.rs` | The batched calls and kernel stamps, moved so hyper-tokio and hyper-rt share one layer (docs/runtime.md §5.1, §14). `Batched` awaits hyper-rt's readiness instead of tokio's reactor, so a receive always asks the kernel (no `Through::Reactor`). Stamps land on the shard's clock by their age; macOS reads the age on `mach_absolute_time` (`Clock::now_ticks`). Astray reports are a seam outcome; EIO from a segmented send resends unsegmented instead of dropping. |
