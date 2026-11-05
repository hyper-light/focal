//! Linux's description of a medium (mantle `docs/research/02-os-storage-apis.md` §2, steps A to E):
//! the file system from statfs(2)'s `f_type` and the mount's own name for it
//! (`/proc/self/mountinfo`, proc_pid_mountinfo(5)), the device from the path's device number
//! through `/sys/dev/block` — a partition's whole disk, and the devices a mapped or RAID device is
//! built over — and what the block layer says of it in the device's `queue/` attributes
//! (Documentation/ABI/stable/sysfs-block). A file system whose files have no device number of
//! their own (btrfs gives each subvolume an anonymous one, major 0) is followed to the device its
//! mount names, and an overlay to its upper directory's. A stacked device's `queue/` attributes are
//! the kernel's own combination of its members' (block/blk-settings.c: features such as the write
//! cache, FUA and rotation OR-ed, block sizes the largest), so they are read from the device the
//! file system is on, and its members only say how they attach and what they are.
use std::collections::BTreeSet;
use std::io::{self, BufRead, Read};
use std::path::{Path, PathBuf};

use super::{FileSystem, Interconnect, MAX_NAME_BYTES, Medium, WriteCache, bounded};

/// statfs(2) `f_type`s and the kinds they name: include/uapi/linux/magic.h as linux-raw-sys 0.12
/// carries it (`EXT4_SUPER_MAGIC` is ext2's and ext3's too), and ZFS's from OpenZFS
/// `include/sys/fs/zfs.h` (`ZFS_SUPER_MAGIC`), which the kernel tree does not define.
const MAGICS: &[(u32, FileSystem)] = &[
    (0xEF53, FileSystem::Ext),
    (0x5846_5342, FileSystem::Xfs),
    (0x9123_683E, FileSystem::Btrfs),
    (0xF2F5_2010, FileSystem::F2fs),
    (0xCA45_1A4E, FileSystem::Bcachefs),
    (0x2FC1_2FC1, FileSystem::Zfs),
    (0x0102_1994, FileSystem::Tmpfs),
    (0x8584_58F6, FileSystem::Tmpfs),
    (0x794C_7630, FileSystem::Overlay),
    (0x6969, FileSystem::Nfs),
    (0x517B, FileSystem::Smb),
    (0xFF53_4D42, FileSystem::Smb),
    (0xFE53_4D42, FileSystem::Smb),
    (0x00C3_6400, FileSystem::OtherNetwork),
    (0x0102_1997, FileSystem::OtherNetwork),
    (0x5346_414F, FileSystem::OtherNetwork),
    (0x6573_5546, FileSystem::Fuse),
    (0x4D44, FileSystem::Fat),
    (0x2011_BAB0, FileSystem::ExFat),
];

/// The most file systems an overlay stacks (`FILESYSTEM_MAX_STACK_DEPTH`, include/linux/fs.h): an
/// overlay's upper directory is followed this many times at most.
const FILESYSTEM_MAX_STACK_DEPTH: usize = 2;
/// The longest path the kernel resolves, its NUL included (`PATH_MAX`, include/uapi/linux/limits.h).
const PATH_MAX: usize = 4096;
/// The bytes of one mount table line kept: the four paths that matter here (the mount's root, its
/// mount point, its source and an overlay's upper directory), each at most [`PATH_MAX`] and four
/// times that escaped (`\ooo`, fs/proc_namespace.c), and the short fields between them, which one
/// more path's length covers. A line past it is read to its end and cut.
const MAX_LINE: usize = (4 * 4 + 1) * PATH_MAX;
/// The mounts a mount namespace may hold by default (`sysctl_mount_max`, fs/namespace.c), the
/// bound on the mount table's lines when `/proc/sys/fs/mount-max` does not say.
const DEFAULT_MOUNT_MAX: u64 = 100_000;

/// The medium under `path`, as Linux describes it.
pub(super) fn medium(path: &Path) -> Medium {
    let mut medium = Medium::unknown(FileSystem::Unknown);
    match rustix::fs::statfs(path) {
        Ok(stat) => medium.file_system = of_magic(i128::from(stat.f_type)),
        Err(error) => medium.note("statfs", error),
    }
    let mut at = path.to_path_buf();
    let mut device = None;
    for pass in 0..=FILESYSTEM_MAX_STACK_DEPTH {
        let query = if pass == 0 {
            "stat"
        } else {
            "overlay upper directory"
        };
        let Some(place) = place_of(&at, query, &mut medium) else {
            break;
        };
        let mount = mount_of(&place, &mut medium);
        if pass == 0
            && let Some(mount) = &mount
        {
            medium.file_system_name = Some(bounded(&mount.kind, MAX_NAME_BYTES));
            medium.file_system = FileSystem::from_name(&mount.kind).unwrap_or(FileSystem::Other);
        }
        if place.major != 0 {
            device = Some((place.major, place.minor));
            break;
        }
        let Some(mount) = mount else {
            break;
        };
        if let Some(found) = source_device(&mount.source) {
            device = Some(found);
            break;
        }
        match upper_of(&mount) {
            Some(upper) if pass < FILESYSTEM_MAX_STACK_DEPTH => at = upper,
            Some(_) => {
                medium.note(
                    "overlay",
                    "stacked deeper than the kernel stacks file systems",
                );
                break;
            }
            None => break,
        }
    }
    match device {
        Some((major, minor)) => describe(major, minor, &mut medium),
        None if !medium.file_system.is_memory() && !medium.file_system.is_network() => {
            medium.note(
                "device",
                "the file system names no block device this probe can reach",
            );
        }
        None => {}
    }
    medium.classify();
    medium
}

/// The kind a statfs(2) `f_type` names: [`FileSystem::Other`] for a type not listed. A word the
/// architecture holds signed (32-bit `long`) is read as the unsigned magic it carries.
fn of_magic(f_type: i128) -> FileSystem {
    let word = if f_type < 0 {
        f_type.checked_add(0x1_0000_0000)
    } else {
        Some(f_type)
    };
    let Some(magic) = word.and_then(|word| u32::try_from(word).ok()) else {
        return FileSystem::Other;
    };
    MAGICS
        .iter()
        .find(|(listed, _)| *listed == magic)
        .map_or(FileSystem::Other, |(_, kind)| *kind)
}

/// Where a path is: its device number and, where statx(2) gives it, the mount that holds it.
struct Place {
    major: u32,
    minor: u32,
    /// The mount's ID (`STATX_MNT_ID`, Linux 5.8): the first field of its mount table line.
    mount: Option<u64>,
}

fn place_of(path: &Path, query: &str, medium: &mut Medium) -> Option<Place> {
    use rustix::fs::{AtFlags, CWD, StatxFlags};
    match rustix::fs::statx(CWD, path, AtFlags::empty(), StatxFlags::MNT_ID) {
        Ok(stat) => Some(Place {
            major: stat.stx_dev_major,
            minor: stat.stx_dev_minor,
            mount: StatxFlags::from_bits_retain(stat.stx_mask)
                .contains(StatxFlags::MNT_ID)
                .then_some(stat.stx_mnt_id),
        }),
        // A kernel before statx, or a sandbox that refuses it: stat(2)'s device number, and the
        // mount found by it.
        Err(_) => match rustix::fs::stat(path) {
            Ok(stat) => Some(Place {
                major: rustix::fs::major(stat.st_dev),
                minor: rustix::fs::minor(stat.st_dev),
                mount: None,
            }),
            Err(error) => {
                medium.note(query, error);
                None
            }
        },
    }
}

/// A mount, as its line of the mount table gives it.
struct Mount {
    /// Its file system type, `type[.subtype]` (field 9).
    kind: String,
    /// Its source (field 10), unescaped: a device node, a server's path, or a name.
    source: String,
    /// Its super block's options (field 11), escaped as the kernel prints them.
    options: String,
}

/// The mount holding `place`, from the mount table: by its ID where statx gave one, else the first
/// mount of its device number.
fn mount_of(place: &Place, medium: &mut Medium) -> Option<Mount> {
    let file = match std::fs::File::open("/proc/self/mountinfo") {
        Ok(file) => file,
        Err(error) => {
            medium.note("mountinfo", error);
            return None;
        }
    };
    let device = format!("{}:{}", place.major, place.minor);
    let mut reader = io::BufReader::new(file);
    let mut line = Vec::new();
    for _ in 0..mount_max() {
        match next_line(&mut reader, &mut line) {
            Ok(false) => return None,
            Ok(true) => {}
            Err(error) => {
                medium.note("mountinfo", error);
                return None;
            }
        }
        let Some((id, of, mount)) = std::str::from_utf8(&line).ok().and_then(parse_mount) else {
            continue;
        };
        let matches = match place.mount {
            Some(mount) => id == mount,
            None => of == device,
        };
        if matches {
            return Some(mount);
        }
    }
    medium.note("mountinfo", "more lines than the namespace may hold mounts");
    None
}

/// The mounts this namespace may hold: `/proc/sys/fs/mount-max`, else the kernel's default.
fn mount_max() -> u64 {
    std::fs::read_to_string("/proc/sys/fs/mount-max")
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .unwrap_or(DEFAULT_MOUNT_MAX)
}

/// The next line of `reader` in `line`, at most [`MAX_LINE`] bytes of it kept; `false` at the end.
fn next_line(reader: &mut impl BufRead, line: &mut Vec<u8>) -> io::Result<bool> {
    line.clear();
    let mut read_any = false;
    loop {
        let (ended, used) = {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                return Ok(read_any);
            }
            read_any = true;
            let (part, ended) = match buf.iter().position(|byte| *byte == b'\n') {
                Some(end) => (buf.get(..end).unwrap_or_default(), true),
                None => (buf, false),
            };
            let room = MAX_LINE.saturating_sub(line.len());
            line.extend_from_slice(part.get(..part.len().min(room)).unwrap_or_default());
            (ended, part.len().saturating_add(usize::from(ended)))
        };
        reader.consume(used);
        if ended {
            return Ok(true);
        }
    }
}

/// A mount table line's mount ID, device number and mount (proc_pid_mountinfo(5)): six fields,
/// optional fields up to a lone `-`, then the type, the source and the super options.
fn parse_mount(line: &str) -> Option<(u64, &str, Mount)> {
    let mut fields = line.split(' ');
    let id = fields.next()?.parse().ok()?;
    let _parent = fields.next()?;
    let device = fields.next()?;
    let mut rest = fields.skip(3);
    rest.by_ref().find(|field| *field == "-")?;
    let kind = rest.next()?.to_owned();
    let source = unescape(rest.next()?)?;
    let options = rest.next().unwrap_or_default().to_owned();
    Some((
        id,
        device,
        Mount {
            kind,
            source,
            options,
        },
    ))
}

/// A field of the mount table as it was before the kernel escaped its spaces, tabs, newlines and
/// backslashes as `\ooo` (fs/proc_namespace.c).
fn unescape(field: &str) -> Option<String> {
    let bytes = field.as_bytes();
    let mut out = Vec::new();
    out.try_reserve(bytes.len()).ok()?;
    let mut at = 0;
    while let Some(byte) = bytes.get(at).copied() {
        let octal = bytes
            .get(at.saturating_add(1)..at.saturating_add(4))
            .filter(|digits| byte == b'\\' && digits.iter().all(|d| (b'0'..=b'7').contains(d)))
            .and_then(|digits| std::str::from_utf8(digits).ok())
            .and_then(|digits| u8::from_str_radix(digits, 8).ok());
        match octal {
            Some(value) => {
                out.push(value);
                at = at.saturating_add(4);
            }
            None => {
                out.push(byte);
                at = at.saturating_add(1);
            }
        }
    }
    String::from_utf8(out).ok()
}

/// The device number of a mount's source when it is a block device's node (btrfs's `/dev/...`).
fn source_device(source: &str) -> Option<(u32, u32)> {
    use rustix::fs::{AtFlags, CWD, FileType, StatxFlags};
    if !source.starts_with("/dev/") {
        return None;
    }
    let stat = rustix::fs::statx(CWD, source, AtFlags::empty(), StatxFlags::TYPE).ok()?;
    (FileType::from_raw_mode(u32::from(stat.stx_mode)) == FileType::BlockDevice)
        .then_some((stat.stx_rdev_major, stat.stx_rdev_minor))
}

/// An overlay's upper directory, from its super options (`upperdir=`).
fn upper_of(mount: &Mount) -> Option<PathBuf> {
    if FileSystem::from_name(&mount.kind) != Some(FileSystem::Overlay) {
        return None;
    }
    mount
        .options
        .split(',')
        .find_map(|option| option.strip_prefix("upperdir="))
        .and_then(unescape)
        .map(PathBuf::from)
}

/// What sysfs says of the block device `major:minor`, its whole disk when it is a partition.
fn describe(major: u32, minor: u32, medium: &mut Medium) {
    let disk = match std::fs::canonicalize(format!("/sys/dev/block/{major}:{minor}")) {
        Ok(dir) => whole_disk(dir),
        Err(error) => {
            medium.note("sysfs device", error);
            return;
        }
    };
    medium.device = disk
        .file_name()
        .map(|name| bounded(&name.to_string_lossy(), MAX_NAME_BYTES));
    let queue = disk.join("queue");
    let read = |name: &str, medium: &mut Medium| match attribute(&queue, name) {
        Ok(text) => Some(text),
        Err(error) => {
            medium.note(name, error);
            None
        }
    };
    medium.rotational = read("rotational", medium).and_then(|text| flag(&text));
    medium.logical_block = read("logical_block_size", medium).and_then(|text| text.parse().ok());
    medium.physical_block = read("physical_block_size", medium).and_then(|text| text.parse().ok());
    medium.preferred_write = read("minimum_io_size", medium).and_then(|text| text.parse().ok());
    // "If no optimal I/O size is reported this file contains 0."
    medium.optimal_io = read("optimal_io_size", medium)
        .and_then(|text| text.parse().ok())
        .filter(|bytes: &u32| *bytes != 0);
    medium.write_cache = match read("write_cache", medium).as_deref() {
        Some("write back") => WriteCache::WriteBack,
        Some("write through") => WriteCache::WriteThrough,
        _ => WriteCache::Unknown,
    };
    medium.fua = read("fua", medium).and_then(|text| flag(&text));
    // "A discard_granularity of 0 means that the device does not support discard functionality."
    medium.discard = read("discard_granularity", medium)
        .and_then(|text| text.parse::<u64>().ok())
        .map(|bytes| bytes != 0);
    medium.removable = match attribute(&disk, "removable") {
        Ok(text) => flag(&text),
        Err(error) => {
            medium.note("removable", error);
            None
        }
    };
    let leaves = leaves(&disk, medium);
    let mut kinds = leaves.iter().map(|leaf| interconnect_of(leaf));
    medium.interconnect = match kinds.next() {
        None => Interconnect::Unknown,
        Some(first) if kinds.all(|kind| kind == first) => first,
        Some(_) => Interconnect::Composite,
    };
    medium.model = match leaves.as_slice() {
        [leaf] => attribute(&leaf.join("device"), "model")
            .ok()
            .filter(|model| !model.is_empty())
            .map(|model| bounded(&model, MAX_NAME_BYTES)),
        _ => None,
    };
}

/// A partition's whole disk is its parent (Documentation/ABI/stable/sysfs-block:
/// `/sys/block/<disk>/<partition>`); a partition's directory has a `partition` attribute.
fn whole_disk(dir: PathBuf) -> PathBuf {
    if dir.join("partition").is_file()
        && let Some(parent) = dir.parent()
    {
        return parent.to_path_buf();
    }
    dir
}

/// The whole disks `top` is built over: followed down each device's members (`slaves/`,
/// block/holder.c) to those built over nothing else; `top` itself when it has none. The walk
/// visits each device once, so it ends within the block devices the system lists (`/sys/block`).
fn leaves(top: &Path, medium: &mut Medium) -> Vec<PathBuf> {
    let devices = std::fs::read_dir("/sys/block").map_or(1, Iterator::count);
    let mut seen = BTreeSet::new();
    let mut stack = vec![top.to_path_buf()];
    let mut leaves = Vec::new();
    while let Some(dir) = stack.pop() {
        if seen.contains(&dir) {
            continue;
        }
        if seen.len() >= devices {
            medium.note("members", "more devices than the system lists");
            break;
        }
        let members: Vec<PathBuf> = std::fs::read_dir(dir.join("slaves"))
            .map(|entries| {
                entries
                    .filter_map(|entry| std::fs::canonicalize(entry.ok()?.path()).ok())
                    .map(whole_disk)
                    .collect()
            })
            .unwrap_or_default();
        if members.is_empty() {
            leaves.push(dir.clone());
        } else {
            stack.extend(members);
        }
        seen.insert(dir);
    }
    leaves
}

/// How a whole disk attaches, from its kernel name (Documentation/admin-guide/devices.txt) and
/// where sysfs places it: under a USB host, a virtio or Hyper-V bus, or a libata port.
fn interconnect_of(disk: &Path) -> Interconnect {
    let name = disk
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let place = disk.to_string_lossy().to_ascii_lowercase();
    if name.starts_with("nvme") {
        // An NVMe controller's transport: "pcie", "tcp", "rdma", "fc" or "loop"
        // (Documentation/ABI/testing/sysfs-class-nvme); the last three but loop reach another host.
        return match attribute(&disk.join("device"), "transport").as_deref() {
            Ok("tcp" | "rdma" | "fc") => Interconnect::Network,
            _ => Interconnect::Nvme,
        };
    }
    if name.starts_with("mmcblk") {
        Interconnect::Mmc
    } else if name.starts_with("vd") || name.starts_with("xvd") {
        Interconnect::Virtual
    } else if name.starts_with("sd") {
        if place.contains("/usb") {
            Interconnect::Usb
        } else if place.contains("/virtio") || place.contains("/vmbus") {
            Interconnect::Virtual
        } else if place.contains("/ata") {
            Interconnect::Sata
        } else {
            Interconnect::Scsi
        }
    } else if name.starts_with("zram") || name.starts_with("ram") {
        Interconnect::Memory
    } else if name.starts_with("nbd") || name.starts_with("rbd") {
        Interconnect::Network
    } else {
        Interconnect::Other
    }
}

/// A sysfs attribute's text, trimmed: at most a page, the buffer sysfs gives an attribute
/// (Documentation/filesystems/sysfs.rst: "sysfs allocates a buffer of size (PAGE_SIZE)").
fn attribute(dir: &Path, name: &str) -> io::Result<String> {
    let page = u64::try_from(rustix::param::page_size()).unwrap_or(u64::MAX);
    let mut text = String::new();
    std::fs::File::open(dir.join(name))?
        .take(page)
        .read_to_string(&mut text)?;
    Ok(text.trim().to_owned())
}

/// A sysfs flag: `1` or `0`.
fn flag(text: &str) -> Option<bool> {
    match text {
        "1" => Some(true),
        "0" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
#[path = "medium_linux_tests.rs"]
mod tests;
