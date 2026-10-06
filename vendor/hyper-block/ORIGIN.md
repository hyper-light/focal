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

## Planned

- The log's frame writes and flushes through the device's issuer, and the issuer's own reads (mantle
  `docs/design/node.md` §1.2); today the log has a device thread of its own (`hyper-log/ORIGIN.md`).
- io_uring on Linux and the completion port on Windows behind the issuer, as node.md §1.2 takes them.
