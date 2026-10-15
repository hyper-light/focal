# hyper-block: origin

- **Source.** mantle's `crates/disk` (mantle-disk) at mantle `147f035`
  (`147f0355513e802fd4b8b3fa3717d8cc1138ded5`, the commit that adds the per-device issuer), brought in
  with its history by `git subtree`.
  - The imported tree hash equals mantle's `147f035:crates/disk`
    (`b6c839763715f05d5e4b648a2fbfb65885b2a267`).
  - It is the block layer mantle's log is written against (mantle note 32 §3.9): how bytes reach a
    device with the alignment and durability the device and OS guarantee.

## What was taken

- `block.rs`: `BlockFile`, the positional operations the log and the chunk store are written
  against.
- `file.rs` and `node/`: `DeviceFile`, a file or device node opened for direct I/O where the file
  system takes it (Linux `O_DIRECT`, macOS `F_NOCACHE`, Windows `FILE_FLAG_NO_BUFFERING`), with the
  platform's full flush (`fdatasync`, `F_FULLFSYNC`, `FlushFileBuffers`; on a macOS disk node,
  `fsync` and `DKIOCSYNCHRONIZE`) and `sync_dir`, the parent-directory flush after a create or rename.
- `buf.rs`: `Alignment`, `AlignedBuf`, `Pool`, `MAX_BUFFER`.
- `commit.rs`: `Anticipation`, group commit's wait for the submitters a batch answered.
- `issuer.rs`: the per-device issuer, and `UNDESCRIBED_QUEUE_DEPTH` from `calibrate.rs` with its
  citation.
- `threads/`: the process's thread budget and the OS's thread counts and ceilings.
- `scratch.rs`, `sim.rs` (the simulated device with power-loss semantics, feature `sim`), and
  `image.rs` (a macOS disk image attached as a node, for tests).

Not taken: `calibrate.rs`, `histogram.rs`, `identity.rs`, `measure.rs`, `probe/`, `rounds.rs` and
the examples that measure with them. Identifying and measuring a device stays in mantle-disk: a
caller hands in the alignment, queue and measured depth it found.

## Changes

1. **Package `hyper-block`**, inheriting the workspace's lint wall. Commit `5bd0699` (L-1).
2. **The zone refusal reads what it needs itself.** mantle's `DeviceFile::open` refused zoned
   storage from `probe::identify`; here, on Linux, a zonefs file is told by statfs(2)'s `f_type`
   (`ZONEFS_MAGIC`, include/uapi/linux/magic.h) and a host-managed node by its `queue/zoned` in
   sysfs (Documentation/ABI/stable/sysfs-block), its partition's one directory up. macOS and
   Windows expose no host-managed zones.
3. **One owner for a file** (commit `4e58930`, L-2):
   - `BlockFile` asks `Send`, not `Sync`. The `Arc<T>` impl is gone.
   - `SimFile` keeps its state in a `RefCell`, borrowed once per operation, a failed borrow refused
     as an error.
   - `Pool` is single-owner: `take` hands out an `AlignedBuf`, `give` takes it back; the guard
     `PoolBuf` and the `Mutex` are gone. Since `4b48295` its free buffers are one list ordered by
     capacity, which takes and gives with no allocation once grown, where a map of lists dropped and
     remade a list whenever a capacity emptied.
   - The thread budget is one atomic: a draw adds its threads and takes them back if they passed
     the ceiling, so no draw waits for another.
   - The issuer's workers each own a duplicate (`try_clone`) of every attached file, in an arena
     of their own, told of an attach or a detach in their own slot before any transfer for it,
     where they borrowed one arena under a `RwLock`. A detach returns once every duplicate is
     dropped. The issuer's tests count through statics and gate through atomics, on a real file,
     where they shared an `Arc<Mutex>`; every assertion is kept.
4. **The wall** (commit `af2679b`): every public item documented; the platform shims crate-private;
   the fs calls outside the sans-io rule (a scratch file's removal, Windows preallocation) carry
   `#[expect(clippy::disallowed_methods)]` with the reason, as the device layer that owns its
   files. The `unsafe` files (`node/macos.rs`, `node/windows.rs`, `threads/macos.rs`,
   `threads/windows.rs`) are listed in `scripts/check-contracts.py`.
5. **A record kept whole** (`record.rs`, new): what a node reads back after a crash, written to a
   temporary name, flushed, renamed over the record and its directory flushed, with its CRC-32C
   checked on read (`DiskError::Corrupt`); mantle keeps its node's records the same way
   (`crates/node/src/layout.rs`, `write_record`). Its first record is a node's run, the count its
   liveness stream orders runs by (`hyper_liveness::Settings::run`, `docs/timing.md` §2.8).
6. **The block a file is written in** (`file::preferred_block`, new): the size the system reports
   for a file, `st_blksize` on Unix and the volume's physical sector for performance on Windows
   (`GetFileInformationByHandleEx`'s `FILE_STORAGE_INFO`, bound in `node/windows.rs`), for a writer
   that writes one block a flush and has no device geometry of its own (hyper-liveness's process
   test, whose 4 KiB had been asserted of every device).

7. **Batches out at once** (`issuer.rs`): a submitter attaches for up to a stated number of batches
   (`Issuer::attach_deep`), hands each over and goes on (`Attached::submit`), and takes each answer,
   numbered by its batch, when it needs it (`answer`, `try_answer`); `attach` and `write` keep one
   batch out, as before. The issuer keeps each submitter's batches in a queue bounded by that
   number and refuses one past it. For mantle's engine, whose puts waited on its extent writes:
   at 10 M puts, uncached, a put's p99.9 went from 43.8–48.9 µs to 5.6–6.7 µs and p99.99 from
   72.4–73.8 µs to 17.5–20.6 µs with two batches out (mantle `docs/design/engine-structure.md`
   §6).
8. **An owned way to attach** (`issuer.rs`, `Issuer::attacher`, `Attacher`): the issuer's inbox
   and worker count, cloned out to a submitter that starts later on a thread of its own, which
   attaches through it as through the issuer. mantle's maintenance workers start lazily from a
   `'static` spawn and their shard only borrows the issuer; with no shared ownership allowed, they
   could not attach, and wrote their branches' pages on their own threads. It keeps nothing alive:
   an attach after the issuer stopped is refused, and the duplicates it made are dropped with the
   refusal.
9. **A batch's vector comes back with its answer** (`issuer.rs`): the answer is the batch's own
   `Transfers` (`Vec<(AlignedBuf, u64)>`, `aio::Transfers`), each buffer and offset where it was
   given and the allocation intact, as `AioFile` answers; it was a new `Vec<AlignedBuf>`. The issuer
   keeps a batch's slots in that vector, an empty buffer (`AlignedBuf::empty`) standing in for one
   on a worker, where it built a `Vec<Option<AlignedBuf>>` per batch. A submitter that pools its
   vectors makes no allocation a batch, where it made three (the vector it built, the issuer's
   slots, the answer's vector): `tests/issuer_allocs.rs` counts 0 across the process for a warm
   pass of five batches, 15 before. Mantle measured this round trip as about 88 % of its engine's
   steady-state allocations over a 3 M-put fill and drain. A failed batch still drops its vector
   and buffers, as before.
10. **The shared simulated device opens a handle on a queue the handle brings** (`sim::shared`):
   the link opening a handle makes its answer queue and sends the queue's sending end with the
   request, where the device made it and sent the receiving end back in its answer, so an answer
   held a queue of answers and its drop ran its own (mantle `CLAUDE.md` §10, no runtime
   recursion). The handles and the device's answers are as before.

11. **Reusable asynchronous device completion and terminal retirement** (`issuer.rs`,
    local development repair): each actual attachment owns a submission lane and reusable
    numbered answer channel bounded by its declared batches. Output credit includes queued,
    in-flight and unanswered batches; in-credit submission does not block on a shared data
    inbox. Retirement follows out0 on the same empty lane. A capacity-one dirty doorbell
    coalesces submissions/retirements, and publication wakes the existing dispatcher thread.
    There is no workers+batches cap on cold attachments. A canceled borrowed completion or
    retirement wait keeps its number and owner; foreign context refuses before publication.
    Completion closure follows all native worker duplicate Drops/joins even during concurrent
    issuer Stop. Pre-Stop accepted batches are answered with numbered refusal; owning Drop
    retains its blocking plain-thread contract.
    The warm allocation oracle (`tests/issuer_allocs.rs`) and its hyper-measure dev-dependency
    came with it. Made in mantle's copy (`6f05bd8`) and taken here (2026-10-12).

12. **Terminal ownership and typed lifecycle failures** (`issuer.rs`, development):
    each completion channel holds its declared B numbered results plus one physical
    terminal role; batch/data admission remains B. The slot/ticket and array layouts
    are checked before channel construction or file duplication, and cold batch storage
    is fallibly reserved. Retirement publication reserves its worker news and receipt
    storage before transferring Client ownership. Any entered-shard owning Drop closes
    its sole lane and notifies without a success acknowledgement; the broker retains
    accepted batches and completion senders through actual duplicate retirement and
    abnormal native joins. Guarded duplicate Drop and worker/broker lifecycle failures
    become terminal errors, never successful Detached. `is_retired` follows actual
    Detached or post-join completion closure, including error; context/admission and
    numbered I/O errors are not terminal facts. `retire_async` checks the actual task
    waker before each receive poll; canceled borrowed waits retain the same receipt.
    Cold `retire_blocking` shares that terminal state and returns lifecycle failures,
    while refusing any entered shard before mutation. Raw Drop remains unacknowledged
    cleanup. Made in mantle's copy (`6f05bd8`) and taken here (2026-10-12); in the merge with
    `main`, an unwound payload the issuer does not keep for a later disposal is dropped inside a
    boundary of its own (`unwind::contained`): a duplicate's drop that unwinds with a payload
    that unwinds again costs that drop alone, and the worker lives on
    (`tests/attachment_watch_native.rs`).

13. **A probe's answer kept while the kernel reclaims nothing** (`resident`, `file.rs`,
    `block.rs`, `issuer.rs`): on macOS a read from memory keeps, one bit an OS page, that
    `mincore(2)` found its pages resident, and reads them again without probing while the pageout
    scan's file-backed reclamation counts (`vm.pageout_freed_*`) stay unchanged; they are read
    again before free memory could reach the kernel's free target at the fastest fill a Mac has.
    `Residency` counts probes, reads on kept answers, epochs and changes, and in shadow mode the
    kept answers a probe found wrong (`BlockFile::residency`, `BlockFile::set_residency_shadow`,
    `Attached::residency`, `Attached::set_residency_shadow`). Mantle
    `docs/research/46-reclaim-epochs.md`. Made in mantle's copy (`a5f5cf3`) and taken here
    (2026-10-12).

14. **A file kept as segments** (`segmented.rs`, `gather.rs`, `block.rs`, `file.rs`, `resident`): a
    base file and `<base>.1`, `<base>.2` and on, read and written at offsets into their
    concatenation (`SegmentedFile`). A segment is created whole at its final length, allocated and
    its name made durable, and never changes length, so every handle finds the layout from names
    and lengths alone; a duplicate made before a segment was added opens it when a transfer first
    reaches it, and a handle holds slots for the segments it has, found by a binary search over the
    filled ones, not for its bound. A transfer lies within one segment; a flush covers the segments
    written since the last; the segments are at most a bound given at open.
    `BlockFile::grows_by_segments`, `BlockFile::grow` and `BlockFile::segment_end` let a store add
    one and find where one ends (a plain file grows by its writes, as before), and
    `BlockFile::write_whole` writes a new segment's blocks once, through a handle past the OS's
    cache (from a buffer aligned to the VM page, which macOS's cluster layer needs to write past its
    cache), so its zeros take no memory a read wants: in as few calls as the kernel takes, by
    vectored writes of one buffer (`DeviceFile::write_copies_at`, pwritev(2) on Linux and macOS) and
    by gather writes on Windows (`WriteFileGather`, `gather.rs`). Reads made from memory keep one
    reclaim epoch for the whole file (`resident::Epoch`, split from `Resident`, its generation 64
    bits so it never comes round again), and `Residency::plus` sums segments' counts. A file's own
    first writes hold up its other reads and writes, another file's do not: mantle
    `docs/research/48-first-writes.md`. Made in mantle's copy (`0d9dfb4`, `0906dfe`, `8dcf196`,
    `e44d9e5`, `f2f6a57`, `abe99c8`, `8384354`; mantle's `ORIGIN.md` change 14) and taken here
    (2026-10-12), with `gather.rs` listed among the files whose `unsafe` binds an OS interface
    (`scripts/check-contracts.py`).
15. **What a segment written whole leaves in memory, asked of the page cache** (`resident/linux.rs`,
    new, tests only; `segmented.rs`'s `a_segment_written_whole_holds_its_fill_and_takes_no_memory`).
    On Linux a resident read is no probe: `preadv2(2)` with `RWF_NOWAIT` on a page not in memory
    starts read-ahead of it and of the window after it before it answers, and reads what that
    brought in by the time it looks (Linux 6.8 `mm/filemap.c`, `filemap_get_pages` and
    `filemap_update_page`). The test read each piece of the segments it had written past the cache
    that way, so a piece's own read-ahead, or the one before it in the same segment, could leave it
    resident: once on ubuntu-24.04-arm (CI run 38348043505), and in Docker's Linux 6.12 under busy
    loops in 1 run of 300, where `mincore(2)` before and after each read showed both (a piece not in
    memory read by its own probe; the next piece in memory before its probe). On Linux the test now
    asks `mincore(2)` over a mapping of each piece, which starts no I/O; elsewhere the resident read
    stays its probe (`mincore(2)` on macOS; no read off Linux and macOS).
    `DeviceFile::read_resident_at` states what a true answer means on Linux: read without waiting,
    not in memory before.

16. **The simulated file's model in hyper-sim** (`sim.rs`, `sim/shared.rs`; `docs/sim.md` §16, step
    S-3): the device itself, its images, faults, generator and counts, is `hyper_sim::storage::Device`,
    and `SimFile` and `SimDevice` are adapters over it that add a direct-I/O file's refusals and map
    its refusals onto `DiskError`. `Fault`, `Crash` and `SimStats` are hyper-sim's (`Stats`), with
    its new faults: a lost write, a misdirected write, a block read as zeros or junk. Every seed's bytes
    are unchanged (hyper-log's equivalence hashes, identical); the `sim` feature takes hyper-sim.

## Planned

- The log's frame writes and flushes through the device's issuer, and the issuer's own reads (mantle
  `docs/design/node.md` §1.2); today the log has a device thread of its own (`hyper-log/ORIGIN.md`).
- io_uring on Linux and the completion port on Windows behind the issuer, as node.md §1.2 takes them.
