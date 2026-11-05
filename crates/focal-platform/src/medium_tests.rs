use super::*;

/// What a probe of a directory on this machine says, as the record of each platform CI runs on
/// shows it: the file system named, the notes and names within their bounds. Do: describe a fresh
/// temporary directory. Expect: a known file system kind and the system's name for it; at most
/// `MAX_NOTES` notes; every name at most `MAX_NAME_BYTES`.
#[test]
fn a_directory_s_medium_names_its_file_system_within_its_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let medium = medium(dir.path());
    eprintln!("medium of {}: {medium:?}", dir.path().display());
    assert_ne!(medium.file_system, FileSystem::Unknown, "{medium:?}");
    assert!(medium.file_system_name.is_some(), "{medium:?}");
    assert!(medium.notes.len() <= MAX_NOTES, "{medium:?}");
    for name in [&medium.device, &medium.model, &medium.file_system_name]
        .into_iter()
        .flatten()
    {
        assert!(name.len() <= MAX_NAME_BYTES, "{name:?}");
    }
    #[cfg(target_vendor = "apple")]
    assert_eq!(medium.file_system, FileSystem::Apfs);
    #[cfg(windows)]
    assert_eq!(medium.file_system, FileSystem::Ntfs);
}

/// A probe answers once every query has, and never before: on macOS the device's description is
/// out until its tool exits. Do: start a probe; wait for it; start another and poll it until it
/// answers, each poll returning at once. Expect: the waited and the polled descriptions agree.
#[test]
fn a_probe_answers_what_waiting_for_it_answers() {
    let dir = tempfile::tempdir().unwrap();
    let waited = Probe::start(dir.path()).wait();
    let mut probe = Probe::start(dir.path());
    // The poll's wait is the tool's own run: each poll returns at once, and the probe answers
    // when the tool has exited, as `wait` above saw it do.
    let polled = loop {
        if let Some(medium) = probe.poll() {
            break medium.clone();
        }
        std::thread::yield_now();
    };
    assert_eq!(polled, waited);
    assert_eq!(probe.poll(), Some(&waited), "an answer stays");
}

#[test]
fn a_file_system_kind_is_named_by_any_system_s_name_for_it() {
    for (name, kind) in [
        ("ext4", FileSystem::Ext),
        ("ext2", FileSystem::Ext),
        ("xfs", FileSystem::Xfs),
        ("btrfs", FileSystem::Btrfs),
        ("apfs", FileSystem::Apfs),
        ("NTFS", FileSystem::Ntfs),
        ("ReFS", FileSystem::Refs),
        ("FAT32", FileSystem::Fat),
        ("exFAT", FileSystem::ExFat),
        ("tmpfs", FileSystem::Tmpfs),
        ("ramfs", FileSystem::Tmpfs),
        ("nfs4", FileSystem::Nfs),
        ("smbfs", FileSystem::Smb),
        ("cifs", FileSystem::Smb),
        ("9p", FileSystem::OtherNetwork),
        ("fuse.sshfs", FileSystem::Fuse),
        ("fuseblk", FileSystem::Fuse),
        ("overlay", FileSystem::Overlay),
    ] {
        assert_eq!(FileSystem::from_name(name), Some(kind), "{name}");
    }
    assert_eq!(FileSystem::from_name("squashfs"), None);
}

/// The class follows what the system says, memory and another host's storage first: a tmpfs on
/// an NVMe machine is memory, an NFS mount is the network's, an NVMe or Apple fabric device is
/// NVMe, and otherwise the rotational flag decides, unknown where the system gave none.
#[test]
fn the_class_follows_what_the_system_says() {
    let classed = |file_system, interconnect, rotational| {
        let mut medium = Medium::unknown(file_system);
        medium.interconnect = interconnect;
        medium.rotational = rotational;
        medium.classify();
        medium.class
    };
    assert_eq!(
        classed(FileSystem::Tmpfs, Interconnect::Nvme, Some(false)),
        Class::Memory
    );
    assert_eq!(
        classed(FileSystem::Nfs, Interconnect::Unknown, None),
        Class::Network
    );
    assert_eq!(
        classed(FileSystem::Ext, Interconnect::Network, Some(false)),
        Class::Network
    );
    assert_eq!(
        classed(FileSystem::Ext, Interconnect::Memory, Some(false)),
        Class::Memory
    );
    assert_eq!(
        classed(FileSystem::Btrfs, Interconnect::Nvme, Some(false)),
        Class::Nvme
    );
    assert_eq!(
        classed(FileSystem::Apfs, Interconnect::AppleFabric, Some(false)),
        Class::Nvme
    );
    assert_eq!(
        classed(FileSystem::Xfs, Interconnect::Sata, Some(true)),
        Class::Hdd
    );
    assert_eq!(
        classed(FileSystem::Xfs, Interconnect::Usb, Some(false)),
        Class::Ssd
    );
    assert_eq!(
        classed(FileSystem::Ext, Interconnect::Virtual, None),
        Class::Unknown
    );
    // A virtual disk that says it rotates is not taken at its word; one that says it does not is
    // flash.
    assert_eq!(
        classed(FileSystem::Ext, Interconnect::Virtual, Some(true)),
        Class::Unknown
    );
    assert_eq!(
        classed(FileSystem::Ext, Interconnect::Virtual, Some(false)),
        Class::Ssd
    );
}

/// A flush is taken to reach stable media only where it can: not on memory, not on another host's
/// storage, not through USB, which Linux may send no flushes at all (29 §1, finding 5).
#[test]
fn flushes_reach_stable_media_only_where_they_can() {
    let medium = |file_system, interconnect, rotational| {
        let mut medium = Medium::unknown(file_system);
        medium.interconnect = interconnect;
        medium.rotational = rotational;
        medium.classify();
        medium
    };
    assert!(
        medium(FileSystem::Btrfs, Interconnect::Nvme, Some(false)).flushes_reach_stable_media()
    );
    assert!(medium(FileSystem::Xfs, Interconnect::Sata, Some(true)).flushes_reach_stable_media());
    assert!(!medium(FileSystem::Tmpfs, Interconnect::Unknown, None).flushes_reach_stable_media());
    assert!(!medium(FileSystem::Smb, Interconnect::Unknown, None).flushes_reach_stable_media());
    assert!(
        !medium(FileSystem::ExFat, Interconnect::Usb, Some(false)).flushes_reach_stable_media()
    );
}

/// Notes and names stay within their bounds: a note past `MAX_NOTES` is dropped, a name is cut at
/// a character boundary.
#[test]
fn notes_and_names_stay_within_their_bounds() {
    let mut medium = Medium::unknown(FileSystem::Unknown);
    for query in 0..MAX_NOTES.saturating_mul(2) {
        medium.note(&format!("query {query}"), "no answer");
    }
    assert_eq!(medium.notes.len(), MAX_NOTES);
    let name = "é".repeat(MAX_NAME_BYTES);
    let cut = bounded(&name, MAX_NAME_BYTES);
    assert!(cut.len() <= MAX_NAME_BYTES && name.starts_with(&cut));
    assert_eq!(cut.chars().count(), MAX_NAME_BYTES / 2);
}

/// diskutil's description of this machine's internal SSD (2026-11-05, an APFS container's whole
/// disk, trimmed to the keys the probe reads and two it must pass over), as the probe reads it.
#[cfg(target_vendor = "apple")]
const APPLE_SSD: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>APFSPhysicalStores</key>
	<array>
		<dict>
			<key>APFSPhysicalStore</key>
			<string>disk0s2</string>
		</dict>
	</array>
	<key>BusProtocol</key>
	<string>Apple Fabric</string>
	<key>DeviceBlockSize</key>
	<integer>4096</integer>
	<key>DeviceIdentifier</key>
	<string>disk3</string>
	<key>Internal</key>
	<true/>
	<key>MediaName</key>
	<string>APPLE SSD AP8192Z</string>
	<key>Removable</key>
	<false/>
	<key>RemovableMedia</key>
	<false/>
	<key>SMARTDeviceSpecificKeysMayVaryNotGuaranteed</key>
	<dict>
		<key>DATA_UNITS_WRITTEN_0</key>
		<integer>1223748357</integer>
		<key>MediaName</key>
		<string>a nested key the probe passes over</string>
	</dict>
	<key>SolidState</key>
	<true/>
</dict>
</plist>
"#;

#[cfg(target_vendor = "apple")]
#[test]
fn diskutil_s_description_of_an_internal_ssd_reads_as_nvme_flash() {
    let mut medium = Medium::unknown(FileSystem::Apfs);
    apple::describe(APPLE_SSD, &mut medium);
    medium.classify();
    assert_eq!(medium.interconnect, Interconnect::AppleFabric);
    assert_eq!(medium.class, Class::Nvme);
    assert_eq!(medium.rotational, Some(false));
    assert_eq!(medium.logical_block, Some(4096));
    assert_eq!(medium.model.as_deref(), Some("APPLE SSD AP8192Z"));
    assert_eq!(medium.removable, Some(false));
    assert!(medium.notes.is_empty(), "{medium:?}");
}

#[cfg(target_vendor = "apple")]
#[test]
fn diskutil_names_the_whole_disk_of_a_slice() {
    assert_eq!(apple::whole_disk("/dev/disk3s1s1"), Some("disk3"));
    assert_eq!(apple::whole_disk("/dev/disk10s2"), Some("disk10"));
    assert_eq!(apple::whole_disk("/dev/disk4"), Some("disk4"));
    assert_eq!(apple::whole_disk("devfs"), None);
    assert_eq!(apple::whole_disk("/dev/diskX"), None);
    assert_eq!(apple::whole_disk("map auto_home"), None);
}
