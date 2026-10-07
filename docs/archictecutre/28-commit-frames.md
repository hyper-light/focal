# 28. Commit frames in the log

Status: decided 2026-10-06 (owner: "aim for one" sync, and "we NEVER weaken correctness or
robustness"); built in `focal-log`. A commit takes two flushes of the segment, the second a commit
frame alone; the fence's file write, rename and directory flush leave the commit path.

## 1. What it replaces

Until this change a group commit was durable after three flushes: the segment's data
(`sync_all`), then a new `CURRENT.tmp` written and flushed, then the atomic rename and a flush
of the directory. On macOS each is an `F_FULLFSYNC`, and the 2026-10-06 measurement put a
committed claim's p50 at about 12.8 ms whatever its CPU work. The fence existed to tell
recovery which prefix was acknowledged: every byte before it must validate, and only the suffix
after it may be discarded.

## 2. The design

**Commit frames.** Each group commit ends with a commit frame in the segment itself: a frame of
the same chain (length, sequence, predecessor checksum, checksum) whose length field carries the
commit flag (bit 31) and whose 28-byte payload is the durable base (segment, byte, sequence,
checksum). Recovery admits a batch only through its commit frame, so a batch is atomic, as it was
under the fence.

**The commit is ordered after its data.** The batch's frames are written and flushed
(`sync_data`); only then is the commit frame written and flushed (`sync_all`). A commit frame on
disk therefore proves its batch's bytes reached the disk before it. One flush would let a crash
persist the commit frame ahead of an earlier sector of its own batch (a drive orders nothing inside
one flush), and recovery could then not tell an unacknowledged torn write from an acknowledged write
the disk later lost. The two flushes are what make every acknowledged byte strictly checked. On macOS
both are `F_FULLFSYNC` (std's `sync_data` and `sync_all`): `F_BARRIERFSYNC` would order them more
cheaply, but no safe binding exists and `unsafe` is confined to `focal-platform`'s Windows file.

This is SQLite's WAL design (commit frames in the log, a cumulative checksum chain, recovery to
the last valid commit; <https://www.sqlite.org/fileformat2.html#walformat>) and the shape of
etcd's WAL (CRC-chained records, recovery to the last valid record).

**The fence becomes a strict lower bound, written rarely.** `CURRENT` is installed when a
generation begins (`rewrite_checkpoint`), when the base moves past a segment boundary (so the
segments behind it may be removed), and at open when recovery found a later commit than the
fence names. Every frame from the base through the fence must validate, as before. Past the
fence, recovery follows the chain through commit frames. A segment behind the base is removed
only after a fence names that base, so the segment any recovery starts from always exists.

**A torn tail is told from corruption** (Alagappan et al., *Protocol-Aware Recovery for
Consensus-Based Storage*, FAST 2018: truncating a corrupted, acknowledged entry as if it were a
crash violates a consensus protocol's safety; the two must be distinguished). Past the fence, at
the first frame that does not validate, the suffix is taken as a torn tail, and dropped with its
batch's uncommitted frames, only when all three hold:

1. the frame is torn in the way an interrupted write leaves it: its header is all zero, or it
   runs past the end of the file, or a 512-byte sector of it is all zero. This is etcd's rule
   (`isTornEntry`): a sector is written atomically, so a write cut short leaves whole unwritten
   sectors, which read as zero past the old end of the file; bit rot and misdirected writes
   leave non-zero garbage;
2. no valid commit frame with a later sequence follows it in the segment. A commit is written only
   after its batch's data is flushed, so a commit past the damage proves the damaged bytes had
   reached the disk: a lost or corrupted write of acknowledged data, which fails closed. A commit
   frame verifies on its own (its checksum covers its header and payload), so the search does not
   need the chain the damage broke;
3. no later segment exists. A segment is flushed entirely before the next is created, so damage
   in a segment followed by another is corruption of flushed data.

Anything else fails closed, as damage before the fence always does. No acknowledged byte is ever
taken for a torn tail: an acknowledged batch has its commit frame on disk, and that frame is a
later commit for any damage inside the batch. This is stricter than etcd, whose `isTornEntry`
accepts a zeroed sector in the last records as torn.

## 3. Compatibility

A segment header carries its format version: 1 for fence-strict segments, 2 for segments with
commit frames. A version-1 segment admits nothing past the fence, as before. A log opened at
version 1 rolls to a new version-2 segment before its first append, so no segment mixes formats
and an old log upgrades in place. A record is capped below 2³¹ bytes (the commit flag), far above
the configured `max_record_bytes` (16 MiB).

## 4. Evidence

`focal-log`'s tests cut the last batch at every byte and recover the previous commit, corrupt a
committed frame before the last commit and fail closed, zero a sector of the last acknowledged
batch and fail closed (its own commit follows it), write garbage into the tail and fail closed,
open a version-1 log and keep appending, and move the base across segments with removal only after
its fence. The fault points keep their names and their meaning: `AfterDataSync` falls between the
data's flush and the commit frame, so the batch is not durable there, as under the fence.
