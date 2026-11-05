use super::*;
use crate::medium::{Class, Interconnect};

/// proc_pid_mountinfo(5)'s own example line reads as its fields say: mount 36, device 98:0, ext3
/// mounted from `/dev/root`, past an optional field.
#[test]
fn a_mount_table_line_reads_as_its_fields_say() {
    let line = "36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue";
    let (id, device, mount) = parse_mount(line).unwrap();
    assert_eq!((id, device), (36, "98:0"));
    assert_eq!(mount.kind, "ext3");
    assert_eq!(mount.source, "/dev/root");
    assert_eq!(mount.options, "rw,errors=continue");
    // No optional fields, and a source the kernel escaped.
    let line = r"29 1 0:26 / /data rw - btrfs /dev/disk\040one rw,ssd,subvol=/data";
    let (id, device, mount) = parse_mount(line).unwrap();
    assert_eq!((id, device), (29, "0:26"));
    assert_eq!(mount.kind, "btrfs");
    assert_eq!(mount.source, "/dev/disk one");
    // A line cut before its separator names no mount.
    assert!(parse_mount("36 35 98:0 /mnt1 /mnt2 rw,noatime master:1").is_none());
}

#[test]
fn an_escaped_field_reads_as_it_was() {
    assert_eq!(
        unescape(r"a\040b\011c\012d\134e").as_deref(),
        Some("a b\tc\nd\\e")
    );
    // Not an escape: fewer than three octal digits, or past a byte.
    assert_eq!(unescape(r"a\04").as_deref(), Some(r"a\04"));
    assert_eq!(unescape(r"a\777").as_deref(), Some(r"a\777"));
    assert_eq!(unescape("plain").as_deref(), Some("plain"));
}

/// A statfs(2) type names its kind whether the architecture holds it signed or not; a type not
/// listed is another file system's.
#[test]
fn a_file_system_type_names_its_kind() {
    assert_eq!(of_magic(0xEF53), FileSystem::Ext);
    assert_eq!(of_magic(0x9123_683E), FileSystem::Btrfs);
    // btrfs's magic in a 32-bit signed word.
    assert_eq!(of_magic(0x9123_683E - 0x1_0000_0000), FileSystem::Btrfs);
    assert_eq!(of_magic(0x0102_1994), FileSystem::Tmpfs);
    assert_eq!(of_magic(0x7371_7368), FileSystem::Other);
    assert_eq!(of_magic(-0x1_0000_0001), FileSystem::Other);
}

#[test]
fn an_overlay_s_upper_directory_is_read_from_its_options() {
    let mount = Mount {
        kind: "overlay".into(),
        source: "overlay".into(),
        options: r"rw,lowerdir=/l1:/l2,upperdir=/var/lib/upper\040dir,workdir=/var/lib/work".into(),
    };
    assert_eq!(upper_of(&mount), Some(PathBuf::from("/var/lib/upper dir")));
    let mount = Mount {
        kind: "ext4".into(),
        source: "/dev/sda1".into(),
        options: "rw,upperdir=/x".into(),
    };
    assert_eq!(upper_of(&mount), None);
}

/// A mount table line past its bound is cut and the next line read whole.
#[test]
fn a_line_past_its_bound_is_cut_and_the_next_read_whole() {
    let mut text = "x".repeat(MAX_LINE.saturating_add(100));
    text.push('\n');
    text.push_str("short\nlast");
    let mut reader = io::BufReader::with_capacity(64, text.as_bytes());
    let mut line = Vec::new();
    assert!(next_line(&mut reader, &mut line).unwrap());
    assert_eq!(line.len(), MAX_LINE);
    assert!(next_line(&mut reader, &mut line).unwrap());
    assert_eq!(line, b"short");
    assert!(next_line(&mut reader, &mut line).unwrap());
    assert_eq!(line, b"last");
    assert!(!next_line(&mut reader, &mut line).unwrap());
}

/// How a disk attaches is read from its name and where sysfs places it.
#[test]
fn a_disk_s_interconnect_is_read_from_its_name_and_place() {
    for (place, interconnect) in [
        (
            "/sys/devices/pci0000:00/0000:00:14.0/usb2/2-1/2-1:1.0/host6/target6:0:0/6:0:0:0/block/sdb",
            Interconnect::Usb,
        ),
        (
            "/sys/devices/pci0000:00/0000:00:17.0/ata1/host0/target0:0:0/0:0:0:0/block/sda",
            Interconnect::Sata,
        ),
        (
            "/sys/devices/LNXSYSTM:00/LNXSYBUS:00/ACPI0004:00/VMBUS:00/f8b3781b-1e82-4818-a1c3-63d806ec15bb/host0/target0:0:0/0:0:0:0/block/sda",
            Interconnect::Virtual,
        ),
        (
            "/sys/devices/pci0000:00/0000:00:03.0/virtio0/host0/target0:0:0/0:0:0:0/block/sda",
            Interconnect::Virtual,
        ),
        (
            "/sys/devices/pci0000:00/0000:00:01.0/0000:01:00.0/host0/port-0:0/end_device-0:0/target0:0:0/0:0:0:0/block/sda",
            Interconnect::Scsi,
        ),
        (
            "/sys/devices/pci0000:00/0000:00:05.0/virtio2/block/vda",
            Interconnect::Virtual,
        ),
        ("/sys/devices/vbd-51712/block/xvda", Interconnect::Virtual),
        (
            "/sys/devices/platform/soc/mmc0/mmc0:0001/block/mmcblk0",
            Interconnect::Mmc,
        ),
        ("/sys/devices/virtual/block/zram0", Interconnect::Memory),
        ("/sys/devices/virtual/block/nbd0", Interconnect::Network),
        ("/sys/devices/virtual/block/loop0", Interconnect::Other),
        (
            // A namespace whose controller says nothing of its transport: NVMe, as it is named.
            "/nonexistent/pci0000:00/0000:00:01.1/0000:01:00.0/nvme/nvme0/nvme0n1",
            Interconnect::Nvme,
        ),
    ] {
        assert_eq!(interconnect_of(Path::new(place)), interconnect, "{place}");
    }
}

/// A directory in memory is memory, whatever the machine's disks are, and its flushes reach no
/// stable media. Do: describe a directory under `/dev/shm`, the tmpfs every Linux system mounts
/// for POSIX shared memory. Expect: tmpfs, class memory, no stable media.
#[test]
fn a_directory_in_memory_is_memory() {
    let dir = tempfile::tempdir_in("/dev/shm").unwrap();
    let medium = medium(dir.path());
    eprintln!("{medium:?}");
    assert_eq!(medium.file_system, FileSystem::Tmpfs, "{medium:?}");
    assert_eq!(medium.class, Class::Memory, "{medium:?}");
    assert!(!medium.flushes_reach_stable_media());
}
