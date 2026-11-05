//! The medium a node writes to: the device and file system under a path, as the operating system
//! describes them (mantle `docs/research/29-device-classes.md` §2, what every class is asked;
//! `02-os-storage-apis.md`, where each operating system answers). A description is the system's
//! claim, never an assumption the node makes: a virtual disk on flash may call itself rotational,
//! and a USB bridge may report a write cache it never flushes (29 §1, finding 5). What the system
//! does not say stays `Unknown`, with a note of the query that did not answer.
use std::path::Path;

/// What kind of medium holds the path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// Flash on NVMe, or Apple's internal SSD: no positioning cost, deep queues.
    Nvme,
    /// Solid state on another interconnect (SATA, SAS, USB, a virtual disk that says so).
    Ssd,
    /// A spinning disk, as the system reports it: seeks and rotation dominate small writes.
    Hdd,
    /// Memory (tmpfs, ramfs, zram): nothing survives a restart of the machine.
    Memory,
    /// Another host's storage (NFS, SMB, Ceph).
    Network,
    /// The system did not say.
    Unknown,
}

impl Class {
    /// The class's name as reports and labels show it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Nvme => "nvme",
            Self::Ssd => "ssd",
            Self::Hdd => "hdd",
            Self::Memory => "memory",
            Self::Network => "network",
            Self::Unknown => "unknown",
        }
    }
}

/// How the device attaches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Interconnect {
    Nvme,
    /// ATA or SATA.
    Sata,
    /// SCSI or SAS.
    Scsi,
    Usb,
    /// A paravirtual or emulated disk (virtio, Xen, a disk image).
    Virtual,
    /// An SD card or eMMC.
    Mmc,
    /// Apple's internal SSD fabric.
    AppleFabric,
    /// A device built over others (device-mapper, md RAID).
    Composite,
    Network,
    Memory,
    /// A loop device or another the probe does not name.
    Other,
    Unknown,
}

impl Interconnect {
    /// The interconnect's name as reports and labels show it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Nvme => "nvme",
            Self::Sata => "sata",
            Self::Scsi => "scsi",
            Self::Usb => "usb",
            Self::Virtual => "virtual",
            Self::Mmc => "mmc",
            Self::AppleFabric => "apple-fabric",
            Self::Composite => "composite",
            Self::Network => "network",
            Self::Memory => "memory",
            Self::Other => "other",
            Self::Unknown => "unknown",
        }
    }
}

/// Whether the device holds acknowledged writes in a volatile cache a flush empties.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteCache {
    /// Durability needs a flush (or a FUA write) to reach the medium.
    WriteBack,
    /// A write completes on stable media: no cache, or a power-loss-protected one, in which case
    /// an NVMe flush "shall have no effect" (NVMe 2.0 base specification §7.2; 29 §1, finding 2).
    WriteThrough,
    Unknown,
}

impl WriteCache {
    /// The cache's mode as reports show it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WriteBack => "write-back",
            Self::WriteThrough => "write-through",
            Self::Unknown => "unknown",
        }
    }
}

/// The file system holding the path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileSystem {
    /// ext2, ext3 or ext4: Linux gives the three one statfs(2) type, and the mount names which.
    Ext,
    Xfs,
    Btrfs,
    F2fs,
    Bcachefs,
    Zfs,
    Apfs,
    Hfs,
    Ntfs,
    Refs,
    Fat,
    ExFat,
    /// tmpfs or ramfs: memory.
    Tmpfs,
    Overlay,
    Nfs,
    Smb,
    /// Another host's file system that is neither NFS nor SMB (Ceph, 9P, AFS, AFP, WebDAV).
    OtherNetwork,
    /// A file system served by a process (FUSE, macFUSE, virtiofs): what holds its data is the
    /// server's.
    Fuse,
    Other,
    Unknown,
}

impl FileSystem {
    /// Whether the file system keeps nothing across a restart of the machine.
    pub fn is_memory(self) -> bool {
        self == Self::Tmpfs
    }
    /// Whether the data lives on another host.
    pub fn is_network(self) -> bool {
        matches!(self, Self::Nfs | Self::Smb | Self::OtherNetwork)
    }
    /// The kind a system's own name for a file system names, compared without case: Linux's
    /// mount table (`/proc/self/mountinfo` field 9, `type[.subtype]`), macOS's `f_fstypename`,
    /// Windows' `GetVolumeInformationW` file system name. `None` for a name it does not know.
    pub fn from_name(name: &str) -> Option<Self> {
        let name = name.to_ascii_lowercase();
        // A FUSE mount's type is `fuse` or `fuseblk`, then its subtype (`fuse.sshfs`).
        let base = name.split('.').next().unwrap_or_default();
        Some(match base {
            "ext2" | "ext3" | "ext4" => Self::Ext,
            "xfs" => Self::Xfs,
            "btrfs" => Self::Btrfs,
            "f2fs" => Self::F2fs,
            "bcachefs" => Self::Bcachefs,
            "zfs" => Self::Zfs,
            "apfs" => Self::Apfs,
            "hfs" | "hfsplus" => Self::Hfs,
            "ntfs" | "ntfs3" => Self::Ntfs,
            "refs" => Self::Refs,
            "vfat" | "msdos" | "fat" | "fat32" => Self::Fat,
            "exfat" => Self::ExFat,
            "tmpfs" | "ramfs" => Self::Tmpfs,
            "overlay" => Self::Overlay,
            "nfs" | "nfs4" => Self::Nfs,
            "cifs" | "smb3" | "smbfs" => Self::Smb,
            "ceph" | "9p" | "afs" | "afpfs" | "webdav" => Self::OtherNetwork,
            "fuse" | "fuseblk" | "virtiofs" | "macfuse" | "osxfuse" => Self::Fuse,
            _ => return None,
        })
    }
    /// The kind's name as reports and labels show it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ext => "ext",
            Self::Xfs => "xfs",
            Self::Btrfs => "btrfs",
            Self::F2fs => "f2fs",
            Self::Bcachefs => "bcachefs",
            Self::Zfs => "zfs",
            Self::Apfs => "apfs",
            Self::Hfs => "hfs",
            Self::Ntfs => "ntfs",
            Self::Refs => "refs",
            Self::Fat => "fat",
            Self::ExFat => "exfat",
            Self::Tmpfs => "tmpfs",
            Self::Overlay => "overlay",
            Self::Nfs => "nfs",
            Self::Smb => "smb",
            Self::OtherNetwork => "network",
            Self::Fuse => "fuse",
            Self::Other => "other",
            Self::Unknown => "unknown",
        }
    }
}

/// The most notes one description keeps, past which a query that does not answer is not noted:
/// one for each query of a probe's pass over a path, the most being Linux's (statfs, stat, the
/// mount table, the device's directory, its eight queue attributes, its removable flag and its
/// members). An overlay's upper directory takes passes of its own, whose further failures go
/// unnoted.
pub const MAX_NOTES: usize = 14;
/// The longest device or model name kept, in bytes: the model number an NVMe controller reports
/// is 40 bytes (Identify Controller, `MN`), as is an ATA device's (IDENTIFY DEVICE words 27-46).
pub const MAX_NAME_BYTES: usize = 40;

/// The device and file system under a path, as the operating system describes them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Medium {
    pub class: Class,
    pub interconnect: Interconnect,
    /// The system's rotational flag, where it reports one.
    pub rotational: Option<bool>,
    pub file_system: FileSystem,
    /// The system's own name for the file system (`ext4`, `apfs`, `NTFS`, `fuse.sshfs`), at most
    /// [`MAX_NAME_BYTES`].
    pub file_system_name: Option<String>,
    /// The smallest unit the device addresses.
    pub logical_block: Option<u32>,
    /// The unit the device writes internally; a smaller write costs a read-modify-write.
    pub physical_block: Option<u32>,
    /// The device's preferred write granularity: Linux puts an NVMe namespace's NPWG here
    /// (`queue/minimum_io_size`; 29 §1, finding 1), Windows the physical sector it performs best at.
    pub preferred_write: Option<u32>,
    /// The device's preferred transfer for sustained writes, where it states one.
    pub optimal_io: Option<u32>,
    pub write_cache: WriteCache,
    /// Whether a write can be made durable on its own (Forced Unit Access).
    pub fua: Option<bool>,
    /// Whether the device takes deallocation (TRIM, discard).
    pub discard: Option<bool>,
    pub removable: Option<bool>,
    /// The system's name for the device (`nvme0n1`, `disk3`), at most [`MAX_NAME_BYTES`].
    pub device: Option<String>,
    /// The device's model, at most [`MAX_NAME_BYTES`].
    pub model: Option<String>,
    /// The queries that did not answer, and why: at most [`MAX_NOTES`].
    pub notes: Vec<String>,
}

impl Medium {
    fn unknown(file_system: FileSystem) -> Self {
        Self {
            class: Class::Unknown,
            interconnect: Interconnect::Unknown,
            rotational: None,
            file_system,
            file_system_name: None,
            logical_block: None,
            physical_block: None,
            preferred_write: None,
            optimal_io: None,
            write_cache: WriteCache::Unknown,
            fua: None,
            discard: None,
            removable: None,
            device: None,
            model: None,
            notes: Vec::new(),
        }
    }
    /// Notes `query` failing for `why`, once a query and within [`MAX_NOTES`].
    fn note(&mut self, query: &str, why: impl std::fmt::Display) {
        if self.notes.len() < MAX_NOTES && self.notes.try_reserve(1).is_ok() {
            self.notes
                .push(bounded(&format!("{query}: {why}"), 4 * MAX_NAME_BYTES));
        }
    }
    /// The class from what the interconnect and the rotational flag say. A virtual disk's flag is
    /// its hypervisor's or its driver's choice (mantle 02 §7: "driver policy, bridges, VMs"), and
    /// a disk is taken to rotate unless its driver says otherwise, so a virtual disk that says it
    /// rotates is not taken at its word (this machine's Docker VM disk, on flash, says it does),
    /// while one that says it does not is flash.
    fn classify(&mut self) {
        self.class = if self.file_system.is_memory() || self.interconnect == Interconnect::Memory {
            Class::Memory
        } else if self.file_system.is_network() || self.interconnect == Interconnect::Network {
            Class::Network
        } else if matches!(
            self.interconnect,
            Interconnect::Nvme | Interconnect::AppleFabric
        ) {
            Class::Nvme
        } else {
            match (self.rotational, self.interconnect) {
                (Some(true), Interconnect::Virtual) => Class::Unknown,
                (Some(true), _) => Class::Hdd,
                (Some(false), _) => Class::Ssd,
                (None, _) => Class::Unknown,
            }
        };
    }
    /// The bytes the description holds on the heap: its names and notes, at their capacity.
    pub fn heap_bytes(&self) -> usize {
        let names = [&self.file_system_name, &self.device, &self.model]
            .into_iter()
            .flatten()
            .map(String::capacity)
            .fold(0usize, usize::saturating_add);
        let notes = self
            .notes
            .iter()
            .map(String::capacity)
            .fold(0usize, usize::saturating_add);
        names.saturating_add(notes).saturating_add(
            self.notes
                .capacity()
                .saturating_mul(std::mem::size_of::<String>()),
        )
    }
    /// Whether a flush this node issues can be taken to reach stable media here: not on memory,
    /// another host's storage, or a USB attachment, which Linux may give no cache flushes at all
    /// (`usb-storage` assumes a write-through cache when it cannot read the caching mode page;
    /// 29 §1, finding 5).
    pub fn flushes_reach_stable_media(&self) -> bool {
        !matches!(self.class, Class::Memory | Class::Network)
            && self.interconnect != Interconnect::Usb
    }
}

/// `text`, cut at a character boundary to at most `max` bytes.
fn bounded(text: &str, max: usize) -> String {
    let mut end = text.len().min(max);
    while end > 0 && !text.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    text.get(..end).unwrap_or_default().trim().to_owned()
}

/// The medium under `path`, as the operating system describes it, waited for: what the system
/// does not say is `Unknown`, and noted. For a caller with nothing else to do; a node starts a
/// [`Probe`] and goes on.
pub fn medium(path: &Path) -> Medium {
    Probe::start(path).wait()
}

/// A description being made. What the system answers at once is in it from the start; on macOS
/// the device's own description comes from a tool the probe runs (`medium_apple.rs`), and
/// completes once it has answered. A probe dropped before then ends the tool's run.
pub struct Probe {
    medium: Medium,
    pending: Option<Pending>,
}

impl Probe {
    /// Starts describing the medium under `path`.
    pub fn start(path: &Path) -> Self {
        let (medium, pending) = start(path);
        Self { medium, pending }
    }
    /// The description once every query has answered; `None` while one is out. Never waits.
    pub fn poll(&mut self) -> Option<&Medium> {
        if let Some(pending) = self.pending.as_mut() {
            if !poll(&mut self.medium, pending) {
                return None;
            }
            self.pending = None;
        }
        Some(&self.medium)
    }
    /// The description, waiting for the queries still out.
    pub fn wait(mut self) -> Medium {
        if let Some(pending) = self.pending.take() {
            wait(&mut self.medium, pending);
        }
        std::mem::replace(&mut self.medium, Medium::unknown(FileSystem::Unknown))
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        if let Some(pending) = self.pending.as_mut() {
            kill(pending);
        }
    }
}

impl std::fmt::Debug for Probe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Probe")
            .field("medium", &self.medium)
            .field("pending", &self.pending.is_some())
            .finish()
    }
}

#[cfg(target_os = "linux")]
#[path = "medium_linux.rs"]
mod linux;

#[cfg(target_vendor = "apple")]
#[path = "medium_apple.rs"]
mod apple;
#[cfg(target_vendor = "apple")]
use apple::{Pending, kill, poll, start, wait};

#[cfg(windows)]
#[path = "medium_windows.rs"]
mod windows;

#[cfg(not(target_vendor = "apple"))]
use immediate::{Pending, kill, poll, start, wait};

/// Where every query answers at once: nothing is ever out.
#[cfg(not(target_vendor = "apple"))]
mod immediate {
    use super::{Medium, Path};

    pub(super) enum Pending {}

    pub(super) fn start(path: &Path) -> (Medium, Option<Pending>) {
        (now(path), None)
    }
    pub(super) fn poll(_: &mut Medium, pending: &mut Pending) -> bool {
        match *pending {}
    }
    pub(super) fn wait(_: &mut Medium, pending: Pending) {
        match pending {}
    }
    pub(super) fn kill(pending: &mut Pending) {
        match *pending {}
    }

    #[cfg(target_os = "linux")]
    fn now(path: &Path) -> Medium {
        super::linux::medium(path)
    }
    #[cfg(windows)]
    fn now(path: &Path) -> Medium {
        super::windows::medium(path)
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    fn now(path: &Path) -> Medium {
        let mut medium = Medium::unknown(super::FileSystem::Unknown);
        medium.note(
            &format!("medium of {}", path.display()),
            "no probe on this system",
        );
        medium
    }
}

#[cfg(test)]
#[path = "medium_tests.rs"]
mod tests;
