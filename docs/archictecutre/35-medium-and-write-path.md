# 35 — The medium: what a node is told of its disk, and what its writes take from it

Terminating condition 10 asks that a node detect its medium (NVMe, SSD, HDD, and the file system)
and calibrate its write path for throughput, longevity and power, never assuming the medium. This
note says what a node asks of its operating system and how far each answer is trusted (§1), what
its write path takes from the medium and why (§2), what an operator sees (§3), what was measured
on which media (§4), and what is still open (§5). Its sources are mantle's device-class and OS
storage research (`../mantle/docs/research/29-device-classes.md`, `02-os-storage-apis.md`), cited
by section, and the platforms' own documentation where a constant comes from it.

## 1. What a node asks, and how far it trusts the answer

`focal_platform::medium` describes the device and file system under a path as the operating system
does. A description is the system's claim, never the node's assumption: what the system does not
say stays `unknown`, and the query that did not answer is noted (`notes`, fourteen at most, one for
each query the Linux probe makes; names are kept to forty bytes, the length of an NVMe or ATA
model number).

| Fact | Linux | macOS | Windows |
|---|---|---|---|
| File system | statfs(2) `f_type` (include/uapi/linux/magic.h), named by the mount table (`/proc/self/mountinfo` field 9) | statfs(2) `f_fstypename` | `GetVolumeInformationW` |
| Local or not | the file system's type; a device's NVMe transport (`tcp`, `rdma`, `fc` reach another host) | statfs(2) `MNT_LOCAL` | `GetDriveTypeW` (`DRIVE_REMOTE`, `DRIVE_RAMDISK`, `DRIVE_REMOVABLE`) |
| Device | statx(2) device number through `/sys/dev/block/MAJ:MIN`, a partition's whole disk, a mapped or RAID device's members (`slaves/`); for btrfs, whose subvolumes have anonymous device numbers, the device the mount names (statx `STATX_MNT_ID` to its exact mount-table line); for an overlay, its upper directory's | `f_mntfromname`'s whole disk (`diskN`), described by Disk Arbitration (`diskutil info -plist`) | the volume (`GetVolumeNameForVolumeMountPointW`), asked through a handle opened for no access |
| How it attaches | the kernel's name for the disk and where sysfs places it (under a USB host, a virtio or Hyper-V bus, a libata port) | `BusProtocol` | `STORAGE_DEVICE_DESCRIPTOR.BusType` |
| Rotating or not | `queue/rotational` | `SolidState` | `SSINFO_FLAGS_NO_SEEK_PENALTY` (`FileFsSectorSizeInformation`) |
| Block sizes | `queue/logical_block_size`, `physical_block_size`, `minimum_io_size` (an NVMe namespace's NPWG, 29 §1 finding 1), `optimal_io_size` | `DeviceBlockSize` | the sector sizes for addressing, atomicity and performance |
| Volatile cache, FUA | `queue/write_cache`, `queue/fua` (what decides whether the kernel flushes) | not reported | `STORAGE_WRITE_CACHE_PROPERTY` |
| Discard, removable, model | `queue/discard_granularity`, `removable`, `device/model` | `RemovableMedia`, `MediaName` | `SSINFO_FLAGS_TRIM_ENABLED`, `RemovableMedia`, the product string |

Classes follow from the answers, memory and other hosts' storage first: tmpfs, ramfs or a memory
device is `memory`; NFS, SMB, Ceph, 9P or a network transport is `network`; an NVMe or Apple fabric
device is `nvme`; otherwise the rotational flag says `hdd` or `ssd`. A virtual disk's rotational
flag is its driver's default, set unless the driver clears it (02 §7, "driver policy, bridges,
VMs"), and this machine's Docker VM disk, on flash, says it rotates: a virtual disk that says it
rotates is `unknown`, and one that says it does not is `ssd`.

Every read is bounded where the system does not bound it: a sysfs attribute to a page (the buffer
sysfs gives it), a mount-table line to the four paths it can matter for at `PATH_MAX`, escaped
(`MAX_LINE`), the table to the mounts a namespace may hold (`fs.mount-max`), the device walk to the
devices `/sys/block` lists, an overlay's upper directories to the kernel's stacking depth, and
`diskutil`'s description to what a pipe holds before its writer waits (`BIG_PIPE_SIZE`, 64 KiB).
No probe recurses. Windows' answers come from the one audited FFI file; Linux's and macOS's use no
`unsafe`.

On macOS the device's description is a system tool's, run once for the volume's whole disk (0.26
to 0.48 s on this machine), so a node starts it as it opens and goes on (`Probe`): the node's
metrics sampler polls it each round and holds the description from the round after it answers.
Elsewhere the first round holds it. A probe that ends before its tool ends the tool's run.

## 2. What the write path takes from the medium

Most of what the write path does on a medium it measures or asks of the file system itself, in the
shared device and log layers, and no class decides it: a class names what the system said, and a
decision made from a class would be an assumption the system might contradict. What the node does
with what it is told:

| Decision | Rule | Basis |
|---|---|---|
| Direct transfers | the log's file bypasses the page cache where the file system takes it (`O_DIRECT`, `F_NOCACHE`, `FILE_FLAG_NO_BUFFERING`), and is buffered where it refuses | hyper-block `DeviceFile::open` |
| Write unit | the log's file is laid out in the block the system reports as the size it writes whole (`st_blksize`; Windows' physical sector for performance) | hyper-block `preferred_block` |
| Confirmation | each frame's confirmation is made durable the cheaper way the device measured: a FUA write (`RWF_DSYNC` on a direct Linux file) or a write and a flush | hyper-log, 29 §1 findings 2 and 3 |
| Zeros ahead | new slots are written with zeros ahead of their first frames only on ext2-4 and XFS direct files, where it was measured to take the journal's flush out of every later write; never on btrfs, where it doubled the bytes for nothing | hyper-block `journals_allocation` (hyper-raft e05956a) |
| Copy on write | btrfs writes the log's file in place (`FS_NOCOW_FL`, asked as the file is made empty) | hyper-block `written_in_place` |
| Group commit | a batch waits for the submitters the last one answered where waiting is expected to lower total latency | hyper-log `Waits::Measured` |
| Idle | a node writes nothing for what did not change: its load report to the directory is due when what placement decides by changes, and its measured values refresh at most every five minutes, only where they changed (§4) | Kubernetes' `nodeStatusReportFrequency`; 29 §1 findings 8, 9 and 15 |
| Durability | where a flush cannot be taken to reach stable media (memory, another host's storage, a USB attachment), the node says so as it starts and in its storage view: what it writes is as durable as its replicas make it | 29 §1 findings 4 and 5 |

The idle rule is this note's change to the write path. A node's load report (its free memory, free
disk, sessions held, enrollment generation and capability level) is a committed directory write:
one frame the log makes durable, with two flushes when nothing follows it, on every replica of the
partition. It was refreshed every 30 seconds whether or not anything in it had changed, and it was
the only write an idle node made. On flash a request's energy is the rule (29 §1 finding 15, "bytes
per joule increases as request size increases"), and a drive Linux has idled into a low-power
state pays its exit latency on the next write (finding 9); on a disk, a write every 30 seconds keeps
the heads loaded or loads them again, where a disk rated for 600,000 load/unload cycles over a
5-year warranty is allowed 329 a day (finding 8). The planner places by the sessions a node holds
(its first key after home and incumbency) and by its eligibility (its generation, and its disk
clearing the 64 MiB headroom), and breaks ties by free memory and free disk. The sessions held, the
generation, the capability level and a disk falling below the headroom are reported as soon as they
change; the tie-breakers, and a disk that rose past the headroom again, refresh at most every five
minutes, Kubernetes' default for node status that has not changed ("Kubelet will ignore this
frequency and post node status immediately if any change is detected", KubeletConfiguration
v1beta1), and only where they changed, so a disk hovering at the headroom reports its fall at most
once an interval.
Kafka's controller logs a broker's registration, fencing and unfencing and takes its heartbeats as
requests (KIP-631); a node's liveness in focal is its failure detector's, never its load report's.

## 3. What an operator sees

`inspect node --storage` names the medium and the write path ([the operator guide](../cluster-admin.md)):
`medium` with every fact of §1 and `flushes_reach_stable_media`, and `write_path` with `direct`,
`layout_block`, `zero_fill`, `written_in_place` and the log's counts at the last metrics sample
(frames, updates, bytes and flushes, and how its confirmations were made durable). A node whose
flushes cannot reach stable media writes one line as its description completes.

## 4. Measured

One node in its laptop form (`focal start node` on a fresh data directory), the release binary of
this change: the node's own counts from its storage view, and the kernel's count of the bytes its
process wrote (macOS `proc_pid_rusage`'s `ri_diskio_byteswritten`, Linux `/proc/PID/io`'s
`write_bytes`). Idle is a window that starts 30 seconds after the node is ready; a load is
`focal-load` over the node's Unix socket, open loop at the rate offered, eight callers, each claim
created and left open. Both machines were shared with other work while measured (this Mac's
one-minute load 24 to 39 on 18 cores), so latencies are a busy machine's; the counts are not.

**This Mac** (Mac Studio, M-series, 2026-11-05): the data directory on its internal SSD.

| | |
|---|---|
| Described as | `nvme`, `apple-fabric`, `APPLE SSD AP8192Z`, 4,096-byte blocks, not removable, solid state; `apfs`; no notes |
| Write path | direct (`F_NOCACHE`), laid out in 4,096-byte blocks, no zeros ahead; every confirmation a write and an `F_FULLFSYNC` (`durable_fallbacks`: macOS has no FUA write) |
| Idle, 30-second load reports (before) | 10 frames, 20 flushes and 568 KiB written by the process in 300 s: 2,880 frames a day |
| Idle, this change | 2 frames, 4 flushes and 120 KiB in 600 s (one report each five minutes, its values changed): 288 a day |
| 3,000 claims at 100/s | all committed; 2.07 claims a frame, 6,663 log bytes and 0.74 flushes a claim, 24,419 bytes written by the process a claim; flush 8.3 ms mean; p50 76 ms, p99 242 ms |
| 20,000 claims offered at 1,000/s | all committed at 178/s, the log bound by its flushes (each frame 1.56 `F_FULLFSYNC`s of 7.0 ms mean, 4.7 ms median, 18.9 ms p99); 3.16 claims a frame, 5,529 log bytes and 0.49 flushes a claim, 18,527 bytes written a claim |

The interrupt wakeups of the idle node's process were 205 a second before and 193 after: the
change is to what it writes, not to its timers (§5).

## 5. Open

- **Disks, USB and network storage are not measured.** No machine this work could reach has a
  spinning disk, a USB drive or a network file system attached; their classification is tested
  from what each system says of them (sysfs places, Disk Arbitration's bus protocols, Windows' bus
  types), and the idle rule's benefit to a disk's heads is argued from the drive's rating, not
  measured on one.
- **A load report is still a durable write.** Kafka keeps a broker's heartbeat out of its metadata
  log; focal's planner plans from committed state and its proposals compare against the reports
  they saw, so a report off the log is a placement redesign, not a change to the write path. What
  this note changed is when a report is due.
- **The device's preferred write granularity is reported, not laid out by.** On Linux
  `minimum_io_size` is an NVMe namespace's NPWG, but on md RAID it is the stripe chunk (hundreds of
  KiB), a block that pads every small frame to it, and a log made on one device and opened on
  another would refuse a block it was not made with. No drive reachable here reports more than
  512 bytes there (fedora's two NVMe drives), so whether a QLC drive's 16 KiB unit pays as the log's
  block is unmeasured.
- **An idle node's CPU still wakes.** This Mac's idle node took 205 interrupt wakeups a second
  (every group's tick, the placement agent's pass, the sampler's round): a laptop's battery is the
  CPU's as well as the disk's, and an idle node's timers are a separate change from its writes.
- **Windows' device queries are its own to answer.** The bus type and the write cache come from
  `IOCTL_STORAGE_QUERY_PROPERTY` on a volume handle opened for no access, which 02 §4.3 leaves
  unverified for an unprivileged process; a refusal is noted and the facts stay `unknown`.
